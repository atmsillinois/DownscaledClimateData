from downscaled_climate_data.processors.era5_processor import era5_processing
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


t2m = era5_processing('2m_temperature', 2024, 2025, 'analysis_ready')
df = t2m.to_dataframe(name='temperature_2m').reset_index()

print(df.head())
