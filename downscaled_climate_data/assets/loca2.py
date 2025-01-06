from tempfile import TemporaryDirectory

import intake_esm.cat
import requests
import s3fs
import xarray as xr
from dagster import AssetExecutionContext, AssetIn, Config, EnvVar, asset
from dagster_aws.s3 import S3Resource

import downscaled_climate_data


class Loca2Config(Config):
    s3_key: str
    url: str = "https://cirrus.ucsd.edu/~pierce/LOCA2/CONUS_regions_split/ACCESS-CM2/cent/0p0625deg/r2i1p1f1/historical/tasmax/tasmax.ACCESS-CM2.historical.r2i1p1f1.1950-2014.LOCA_16thdeg_v20220413.cent.nc"  # NOQA E501


@asset(
    name="loca2_raw_netcdf",
    description="Raw LOCA2 data downloaded from the web",
    code_version=downscaled_climate_data.__version__,
    group_name="loca2"
)
def loca2_raw_netcdf(context: AssetExecutionContext,
                     config: Loca2Config,
                     s3: S3Resource) -> dict[str, str]:

    destination_bucket = EnvVar("LOCA2_BUCKET").get_value()
    destination_path_root = EnvVar("LOCA2_RAW_PATH_ROOT").get_value()

    with requests.get(config.url, stream=True) as response:
        # Raise an exception for bad HTTP responses
        response.raise_for_status()

        # Get total file size from headers if available
        total_size = int(response.headers.get('content-length', 0)) / (1024 ** 3)
        context.log.info(f"Downloading {total_size:.2f} GB of data")

        # Upload directly using S3 client's upload_fileobj method
        s3.get_client().upload_fileobj(
            response.raw,
            destination_bucket,
            destination_path_root + config.s3_key
        )

    context.log.info(f"Downloading data to {config.s3_key}")
    return {
        "bucket": destination_bucket,
        "s3_key": config.s3_key,
    }


@asset(
    name="loca2_zarr",
    ins={
        "loca2_raw_netcdf": AssetIn()
    },
    group_name="loca2",
    description="LOCA2 data converted to Zarr format",
    code_version=downscaled_climate_data.__version__)
def loca2_zarr(context,
               loca2_raw_netcdf,
               s3: S3Resource):
    context.log.info(f"Converting {loca2_raw_netcdf['s3_key']} to zarr")

    # Initialize s3fs with the same credentials as the S3Resource
    fs = s3fs.S3FileSystem(
        key=s3.aws_access_key_id,
        secret=s3.aws_secret_access_key,
        endpoint_url=s3.endpoint_url
    )

    raw_root = EnvVar("LOCA2_RAW_PATH_ROOT").get_value()
    zarr_root = EnvVar("LOCA2_ZARR_PATH_ROOT").get_value()
    # Construct S3 paths
    input_path = f"s3://{loca2_raw_netcdf['bucket']}/{raw_root}{loca2_raw_netcdf['s3_key']}"  # NOQA E501
    context.log.info(f"Reading from {input_path}")

    zarr_key = loca2_raw_netcdf['s3_key'].replace('.nc', '.zarr')
    output_path = f"s3://{loca2_raw_netcdf['bucket']}/{zarr_root}{zarr_key}"
    context.log.info(f"Writing to {output_path}")

    # Read NetCDF file from S3
    with fs.open(input_path, 'rb') as f:
        ds = xr.open_dataset(f)
        context.log.info(f"Dataset keys: {ds.keys()}")

        # Create a zarr store using the same s3fs instance
        store = s3fs.S3Map(
            root=output_path,
            s3=fs,
            check=False  # Don't check if the store exists
        )

        # Write to Zarr format
        ds.to_zarr(
            store=store,
            mode='w',  # Overwrite if exists
            consolidated=True  # Write metadata to a single consolidated file
        )

        # Close the dataset to free memory
        ds.close()


class ESMCatalogConfig(Config):
    data_format: str = "netcdf"
    id: str = "loca2_raw_netcdf_monthly_esm_catalog"
    description: str = "LOCA2 raw data catalog"

    def is_zarr(self):
        return self.data_format == "zarr"


def parse_key(relative_path: str, bucket: str, full_key: str) -> dict[str, str]:
    # Split the relative path into parts
    # Filter out empty strings that occur when there are consecutive slashes
    path_parts = [part for part in relative_path.split('/') if part]

    model = path_parts[0]
    scheme = path_parts[1]

    file_parts = path_parts[-1].split('.')
    variable = file_parts[0]
    experiment_id = file_parts[3]
    time_range = file_parts[4]

    uri = f"s3://{bucket}/{full_key}"
    return {
        "variable": variable,
        "model": model,
        "scheme": scheme,
        "experiment_id": experiment_id,
        "time_range": time_range,
        "path": uri
    }


@asset(
    name="loca2_esm_catalog",
    group_name="loca2",
    description="Generate an Intake-ESM Catalog for LOCA2 datasets",
    code_version=downscaled_climate_data.__version__)
def loca2_esm_catalog(context: AssetExecutionContext,
                      config: ESMCatalogConfig,
                      s3: S3Resource):

    bucket = EnvVar("LOCA2_BUCKET").get_value()

    if config.is_zarr():
        prefix = EnvVar("LOCA2_ZARR_PATH_ROOT").get_value()
    else:
        prefix = EnvVar("LOCA2_RAW_PATH_ROOT").get_value()

    catalog_metadata = intake_esm.cat.ESMCatalogModel(
        esmcat_version="0.1.0",
        id=config.id,
        description=config.description,
        catalog_file=f"s3://{bucket}/{config.id}.csv",
        attributes=[
            intake_esm.cat.Attribute(column_name="variable"),
            intake_esm.cat.Attribute(column_name="model"),
            intake_esm.cat.Attribute(column_name="scheme"),
            intake_esm.cat.Attribute(column_name="experiment_id"),
            intake_esm.cat.Attribute(column_name="time_range"),
            intake_esm.cat.Attribute(column_name="path")
        ],
        assets=intake_esm.cat.Assets(
            column_name='path',
            format=intake_esm.cat.DataFormat.zarr
            if config.is_zarr() else intake_esm.cat.DataFormat.netcdf,
        )
    )
    context.log.info(catalog_metadata.model_dump_json())

    s3_client = s3.get_client()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    # We use a set to keep track of unique keys since the zarr keys are only
    # directories with many files in them, so they show up as a number of
    # keys in the S3 bucket
    keys = set()

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                full_key = obj['Key']

                # If we are cataloging Zarr stores, we need to identify the directory
                # that holds all the files for a single dataset. These directories end
                # with "cent.zarr" and are the base path for the dataset
                if config.is_zarr():
                    if "monthly.cent.zarr" in full_key:
                        base_path = (full_key.rsplit("monthly.cent.zarr", 1)[0]
                                     + "monthly.cent.zarr")
                    elif "cent.monthly.zarr" in full_key:
                        base_path = (full_key.rsplit("cent.monthly.zarr", 1)[0]
                                     + "cent.monthly.zarr")
                else:
                    base_path = full_key
                keys.add(base_path)

    context.log.info(f"Found {len(keys)} unique base paths")

    with TemporaryDirectory() as temp_dir:
        collection_spec_path = f"{temp_dir}/{config.id}.json"
        with open(collection_spec_path, "w") as json_file:
            json_file.write(catalog_metadata.model_dump_json(indent=4))

        catalog_path = f"{temp_dir}/{config.id}.csv"

        with open(catalog_path, 'w') as f:
            f.write("variable,model,scheme,experiment_id,time_range,path\n")

            # Now that we have the unique base paths, we can write the catalog
            for full_key in keys:
                relative_path = full_key[len(prefix):] if full_key.startswith(prefix) \
                    else full_key
                try:
                    parsed = parse_key(relative_path, bucket, full_key)
                    f.write(f"{parsed['variable']},{parsed['model']},{parsed['scheme']},{parsed['experiment_id']},{parsed['time_range']},{parsed['path']}\n")   # NOQA E501
                except IndexError as e:
                    context.log.error(f"Error processing {full_key}: {e}")

        s3_client.upload_file(Filename=catalog_path,
                              Bucket=bucket,
                              Key=f"{config.id}.csv")

        s3_client.upload_file(Filename=collection_spec_path,
                              Bucket=bucket,
                              Key=f"{config.id}.json")
