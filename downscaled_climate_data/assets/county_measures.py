from downscaled_climate_data.processors.era5_processor import era5_processing
import pandas as pd
import dask_geopandas as dgpd
from shapely.geometry import Point
from dask.distributed import Client
from dask.distributed import wait

import time
import s3fs

from htcdaskgateway import HTCGateway
from dask_gateway.auth import BasicAuth
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

os.environ['CONDOR_BIN_DIR'] = "/u/bengal1/.conda/envs/downscaled_climate_data/bin"
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
    start_time = time.time()

    era5_processing_start = time.time()
    era5 = era5_processing({'2m_temperature',
                            'total_precipitation',
                            "sfcWind",
                            "vapor_pressure",
                            "surface_pressure"},
                           1990, 2025, 'analysis_ready', chunks=500).persist()
    wait(era5)
    print(f"era processing {time.time() - era5_processing_start:.2f} seconds")
    print(era5)

    to_tabular_start = time.time()
    df = era5.to_dask_dataframe()
    del era5
    df = df.repartition(partition_size='200MB').persist()  # Target 200MB per partition
    wait(df)
    print(df)
    print(f"to tabular {time.time() - to_tabular_start:.2f} seconds")


    era5_gdf = dgpd.from_dask_dataframe(
        df, 
        geometry=dgpd.points_from_xy(df, 'lon', 'lat')) \
    .drop(columns=['lat', 'lon'])
    
    era5_gdf.to_parquet('s3://ees240146/analysis/era5.parquet', 
                        filesystem=fs,
                        write_metadata_file=True,
                        schema="infer")
    print(f"To Tabular {time.time() - to_tabular_start:.2f} seconds")
    print(f"TOTAL TIME {time.time() - start_time:.2f} seconds")

    info = client.scheduler_info()
    num_workers = len(info['workers'])
    print(f"Number of workers: {num_workers}")



finally:
    cluster.close()
