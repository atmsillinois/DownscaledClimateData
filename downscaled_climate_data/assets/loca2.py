import requests
from dagster import asset, Config, AssetExecutionContext
from dagster_aws.s3 import S3Resource


class Loca2Config(Config):
    bucket: str = "loca2-data"
    s3_key: str
    url: str = "https://cirrus.ucsd.edu/~pierce/LOCA2/CONUS_regions_split/ACCESS-CM2/cent/0p0625deg/r2i1p1f1/historical/tasmax/tasmax.ACCESS-CM2.historical.r2i1p1f1.1950-2014.LOCA_16thdeg_v20220413.cent.nc"


@asset(
    name="RawLOCA2",
    description="Raw LOCA2 data downloaded from the web",
)
def loca2_raw(context: AssetExecutionContext, config: Loca2Config, s3: S3Resource) -> None:

    with requests.get(config.url, stream=True) as response:
        # Raise an exception for bad HTTP responses
        response.raise_for_status()

        # Upload directly using S3 client's upload_fileobj method
        s3.get_client().upload_fileobj(
            response.raw,
            config.bucket,
            config.s3_key
        )

    context.log.info(f"Downloading data to {config.s3_key}")


