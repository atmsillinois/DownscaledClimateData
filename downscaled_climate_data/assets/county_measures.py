from downscaled_climate_data.processors.era5_processor import era5_processing
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from dask.distributed import Client


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
print(client)

t2m = era5_processing(['2m_temperature', 'total_precipitation'], 2024, 2025, 'analysis_ready')
df = t2m.to_dataframe()

print(df.head())

if client is not None:
    client.close()
