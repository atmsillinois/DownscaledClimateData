import requests
from dagster import AssetExecutionContext, Config, asset, EnvVar, AssetIn
from dagster_aws.s3 import S3Resource
import s3fs
import xarray as xr

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
