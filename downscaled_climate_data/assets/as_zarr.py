import boto3
import s3fs
import xarray as xr

from dagster import AssetIn, asset, EnvVar
from dagster_aws.s3 import S3Resource


@asset(
    name="AsZarr",
    ins={
        "RawLOCA2": AssetIn()
    })
def as_zarr(context,
            RawLOCA2,
            s3: S3Resource):
    context.log.info(f"Converting {RawLOCA2['s3_key']} to zarr")

    # Initialize s3fs with the same credentials as the S3Resource
    fs = s3fs.S3FileSystem(
        key=s3.aws_access_key_id,
        secret=s3.aws_secret_access_key,
        endpoint_url=s3.endpoint_url
    )

    raw_root = EnvVar("LOCA2_RAW_PATH_ROOT").get_value()
    zarr_root = EnvVar("LOCA2_ZARR_PATH_ROOT").get_value()
    # Construct S3 paths
    input_path = f"s3://{RawLOCA2['bucket']}/{raw_root}{RawLOCA2['s3_key']}"
    context.log.info(f"Reading from {input_path}")

    zarr_key = RawLOCA2['s3_key'].split('/')[-1].replace('.nc', '.zarr')
    output_path = f"s3://{RawLOCA2['bucket']}/{zarr_root}/{zarr_key}"
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
