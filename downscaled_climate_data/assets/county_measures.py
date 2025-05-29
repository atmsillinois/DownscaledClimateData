from downscaled_climate_data.processors.era5_processor import era5_processing
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from dask.distributed import Client


t2m = era5_processing(['2m_temperature', 'total_precipitation'], 2024, 2025, 'analysis_ready')
df = t2m.to_dataframe()

print(df.head())

# Get the current Dask client and shut it down
client = Client.current()
if client is not None:
    client.close()
