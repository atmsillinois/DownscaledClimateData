from downscaled_climate_data.processors.era5_processor import era5_processing
import pandas as pd
import dask_geopandas as dgpd
from shapely.geometry import Point
from dask.distributed import Client

import s3fs

from htcdaskgateway import HTCGateway
from dask_gateway.auth import BasicAuth
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

os.environ['CONDOR_BIN_DIR'] = "/opt/conda/envs/pangeo/bin"
gateway = HTCGateway(address="https://dask.software-dev.ncsa.illinois.edu",
                     proxy_address=8786,
                     auth = BasicAuth(
                         username=None, 
                         password=os.environ['DASK_GATEWAY_PASSWORD'])
                    )
cluster = gateway.new_cluster(image="bengal1/pangeo-ncsa:dev", 
                              container_image="/u/bengal1/condor/pangeo.sif")
cluster.scale(200)
client = cluster.get_client()
print(cluster.dashboard_link)



fs = s3fs.S3FileSystem(
    endpoint_url=os.environ['S3_ENDPOINT_URL'],
    key=os.environ['AWS_ACCESS_KEY_ID'],
    secret=os.environ['AWS_SECRET_ACCESS_KEY'],
    config_kwargs={
        'signature_version': 's3v4',
        's3': {
            'addressing_style': 'path'
        }
    })

try:
    era5 = era5_processing({'2m_temperature',
                            'total_precipitation',
                            "sfcWind",
                            "relative_humidity",
                            "vapor_pressure",
                            "surface_pressure"},
                           2024, 2025, 'analysis_ready')
    df = era5.to_dask_dataframe()
    
    era5_gdf = dgpd.from_dask_dataframe(
        df, 
        geometry=dgpd.points_from_xy(df, 'lon', 'lat')) \
    .drop(columns=['lat', 'lon']) \
    .repartition(partition_size="100MB")

    
    era5_gdf.to_parquet('s3://ees240146/analysis/era5.parquet', 
                        filesystem=fs,
                        write_metadata_file=True,
                        schema="infer")

finally:
    cluster.close()
