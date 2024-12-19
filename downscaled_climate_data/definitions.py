from dagster import Definitions, EnvVar
from dagster_aws.s3 import S3Resource

from downscaled_climate_data.assets.loca2 import loca2_raw
from downscaled_climate_data.sensors.loca2_models import Loca2Models
from downscaled_climate_data.sensors.loca2_sensor import (loca2_sensor, Loca2Datasets)

defs = Definitions(
    assets=[loca2_raw],
    sensors=[loca2_sensor],
    resources={
        "loca2_models": Loca2Models(),
        "loca2_datasets": Loca2Datasets(variable="tasmax"),
        "s3": S3Resource(endpoint_url=EnvVar("S3_ENDPOINT_URL"),
                         aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
                         aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"))
    },
)
