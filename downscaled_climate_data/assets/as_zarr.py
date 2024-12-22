import s3fs
import xarray as xr

from dagster import AssetIn, asset
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
    # Get S3 client from resource
    s3_client = s3.get_client()

    # Initialize s3fs with the same credentials as the S3Resource
    fs = s3fs.S3FileSystem(client_kwargs={"session": s3_client._session})

    # Construct S3 paths
    input_path = f"s3://{RawLOCA2['bucket']}/{RawLOCA2['s3_key']}"
    zarr_key = RawLOCA2['s3_key'].replace('.nc', '.zarr')
    output_path = f"s3://{RawLOCA2['bucket']}/{zarr_key}"

    # Read NetCDF file from S3
    with fs.open(input_path, 'rb') as f:
        ds = xr.open_dataset(f)

    # Write to Zarr format
    ds.to_zarr(
        store=s3fs.S3Map(root=output_path, s3=fs),
        mode='w'  # Overwrite if exists
    )

    # Close the dataset to free memory
    ds.close()
