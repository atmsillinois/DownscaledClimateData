from dagster import Definitions, EnvVar
from dagster_aws.s3 import S3Resource

from downscaled_climate_data.assets.loca2 import loca2_zarr, loca2_raw_netcdf
from downscaled_climate_data.assets.loca2 import loca2_esm_catalog
from downscaled_climate_data.sensors.loca2_models import Loca2Models
from downscaled_climate_data.sensors.loca2_sensor import (loca2_sensor_monthly_pr,
                                                          loca2_sensor_monthly_tasmin,
                                                          loca2_sensor_pr,
                                                          loca2_sensor_tasmax,
                                                          Loca2Datasets,
                                                          loca2_sensor_monthly_tasmax,
                                                          loca2_sensor_tasmin)

defs = Definitions(
    assets=[loca2_raw_netcdf, loca2_zarr, loca2_esm_catalog],
    sensors=[loca2_sensor_tasmax,
             loca2_sensor_tasmin,
             loca2_sensor_pr,
             loca2_sensor_monthly_tasmax,
             loca2_sensor_monthly_tasmin,
             loca2_sensor_monthly_pr],
    resources={
        "loca2_models": Loca2Models(),
        "loca2_datasets_tasmax": Loca2Datasets(variable="tasmax"),
        "loca2_datasets_tasmin": Loca2Datasets(variable="tasmin"),
        "loca2_datasets_pr": Loca2Datasets(variable="pr"),
        "s3": S3Resource(endpoint_url=EnvVar("S3_ENDPOINT_URL"),
                         aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
                         aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"))
    },
)
