import xarray as xr
import scipy.spatial
import numpy as np
import argparse
import time
from datetime import date
from downscaled_climate_data.calculations.calculations import vapor_pressure
from downscaled_climate_data.calculations.calculations import wind_mag
from downscaled_climate_data.calculations.calculations import rel_hum

calc_dict = {
    'vapor_pressure': {
        'analysis_variables': ['2m_dewpoint_temperature'],
        'calculator': vapor_pressure
    },
    'sfcWind': {
        'analysis_variables': ['10m_u_component_of_wind', '10m_v_component_of_wind'],
        'calculator': wind_mag
    },
    'relative_humidity': {
        'analysis_variables': ['2m_temperature', '2m_dewpoint_temperature'],
        'calculator': rel_hum
    }
}

# Using code from https://github.com/google-research/arco-era5/blob/main/docs/0-Surface-Reanalysis-Walkthrough.ipynb 

def build_triangulation(x, y):
    """
    Creates a Delaunay tesselation
    
    """
    grid = np.stack([x, y], axis=1)
    return scipy.spatial.Delaunay(grid)

def interpolate(data, tri, mesh):
    """
    Interpolates the ERA5 grid using the Delaunay tesselation
    
    """
    indices = tri.find_simplex(mesh)
    ndim = tri.transform.shape[-1]
    T_inv = tri.transform[indices, :ndim, :]
    r = tri.transform[indices, ndim, :]
    c = np.einsum('...ij,...j', T_inv, mesh - r)
    c = np.concatenate([c, 1 - c.sum(axis=-1, keepdims=True)], axis=-1)
    result = np.einsum('...i,...i', data[:, tri.simplices[indices]], c)
    return np.where(indices == -1, np.nan, result)

def era5_processing(variables:set[str], year_start:int, year_end:int, dataset:str, chunks:int=48):
    """
    Code to process ERA5 Data over the state of Illinois
    https://github.com/google-research/arco-era5 
    
    Inputs:
        - variable (str) - Variable name to call, using ERA5 data names
                           User can also call "vapor_pressure" (mb), "sfcWind" (m/s) and "relative_humidity" (decimal)
        - year_start (int) - First year you'd like to request
        - year_end (int) - Last year you'd like to request (inclusive)
        - dataset (str) - Either use the "raw" or "analysis_ready" dataset from ARCO-ERA5 (for specifics, see 
                            Github link)
        - chunks (int) - Number of time chunks to use for the xArray
    Outputs:
        - fin_array (Dataarray) - Dataarray with appropriate ERA5 data for the variable and timeframe chosen,
                                   interpolated using Delaunay triangulation
        
    """
    start_time = time.time()
    
    analyis_variables = set()
    calculations = set()
    for variable in variables:
        if variable in calc_dict:
            analyis_variables.update(calc_dict[variable]['analysis_variables'])
            calculations.update(calc_dict[variable]['calculator'])
        else:
            analyis_variables.add(variable)

        
    print(f"Opening dataset: {dataset}...")
    dataset_start = time.time()
    if dataset == 'raw':
        # Opening dataset with zarr
        reanalysis = xr.open_zarr(
            'gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr', 
            chunks={'time': chunks},
            consolidated=True,
            )
    
    if dataset == 'analysis_ready':
        # Opening dataset with zarr
        reanalysis = xr.open_zarr(
            'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3', 
            chunks={'time': chunks},
            consolidated=True,
            )
    print(f"Dataset opened in {time.time() - dataset_start:.2f} seconds")

    # Dates
    i_date = str(year_start) + '-01-01'
    f_date = str(year_end)   + '-12-31'
    
    print("Selecting time range...")
    time_select_start = time.time()
    recent_an = reanalysis.sel(time=slice(i_date, f_date))
    print(f"Time range selection completed in {time.time() - time_select_start:.2f} seconds")

    era5_var = recent_an[list(analyis_variables)]
    
    lon_min = 267.2
    lon_max = 274
    lat_min = 36
    lat_max = 43.5
    
    print("Filtering for Illinois region...")
    region_start = time.time()
    illinois_ds = era5_var.where(
    (recent_an.longitude > lon_min) & (recent_an.latitude > lat_min) &
    (recent_an.longitude < lon_max) & (recent_an.latitude < lat_max),
    drop=True)
    print(f"Illinois region filtering completed in {time.time() - region_start:.2f} seconds")
    
    if dataset == 'raw':
        print("Performing interpolation...")
        interp_start = time.time()
        tri = build_triangulation(illinois_ds.longitude, illinois_ds.latitude)
        longitude = np.linspace(lon_min, lon_max, num=round(lon_max-lon_min)*4+1)
        latitude = np.linspace(lat_min, lat_max, num=round(lat_max-lat_min)*4+1)
            
        mesh = np.stack(np.meshgrid(longitude, latitude, indexing='ij'), axis=-1)
        mesh_int = interpolate(illinois_ds[variable].values, tri, mesh)
        
        fin_array = xr.DataArray(mesh_int, 
                             coords=[('time', illinois_ds.time.data), ('longitude', longitude), ('latitude', latitude)])
        print(f"Interpolation completed in {time.time() - interp_start:.2f} seconds")
    else:
        fin_array = illinois_ds
        
    fin_array = fin_array.rename({'longitude':'lon', 'latitude':'lat'})
    print(fin_array)
    
    # Calculations
    for calc in calculations:
        print(f"Performing {calc} calculation...")
        fin_array = calc(fin_array)
    
    print(f"Total processing time: {time.time() - start_time:.2f} seconds")
    return fin_array

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--variable", required=True, type=str)
    parser.add_argument("--year_start", required=True, type=int)
    parser.add_argument("--year_end", required=True, type=int)
    parser.add_argument("--out_path", required=True, type=str)
    parser.add_argument("--dataset", required=True, type=str)
    args = parser.parse_args()

    variable = args.variable
    year_start = args.year_start
    year_end = args.year_end
    out_path = args.out_path
    dataset = args.dataset
    
    dataarray = era5_processing(variable, year_start, year_end, dataset)
    
    # Saving the dataset
    output_file = (out_path + '/ERA5_IL_' + variable + '_' + str(year_start) + '-' + str(year_end) + '_' + 
                   str(date.today()) + '.nc')
    dataarray.to_netcdf(output_file)
    print('Dataset saved to ' + output_file)