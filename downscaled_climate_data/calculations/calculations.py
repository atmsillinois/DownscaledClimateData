import xarray as xr
import numpy as np

"""
Calculations for Downscaled Climate Data
This module contains functions for calculating various climate indices and statistics
Code authored by Maile Sasaki UIUC, 
originally from https://github.com/mailesasaki/climate_map/blob/main/calculations/calculations.py
"""

def stats(dataset):
    """    
    Calculates statistics across the models for all variables
    
    Input:
        - dataset (Dataset or Dataarray) - Needs a dimension called "model"
    Output:
        - data_stats (Dataset) - Contains calculations of the mean, standard deviation, and variance
            of the dataset. All statistics stored in a coordinate called "stats"
            
    """
    
    mean = dataset.mean('model')
    mean['stats'] = 'mean'
    
    stdev = dataset.std('model')
    stdev['stats'] = 'stdev'
    
    variance = dataset.var('model')
    variance['stats'] = 'variance'
    
    data_stats = xr.concat([mean, stdev, variance], 'stats')
    return data_stats



def heat_index(dataset):
    """
    https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml

    Calculates heat index for an array
    
    Inputs:
        RH (DataArray) - Should be in decimal format
        t2m  (DataArray) - Should be in Kelvins
    Outputs:
        hi_alone (DataArray) - Heat index array (in K)
        
    """
    t2m = dataset['2m_temperature']
    RH = dataset['relative_humidity']
    # Convert to Fahrenheit
    T_F = ((t2m - 273.15) * 1.8) + 32

    # Convert to relative humidity
    RH_p = RH * 100
    RH_p = RH_p.rename('relative_humidity')
    
    # Standard heat index
    heat_index = 0.5 * (T_F + 61.0 + ((T_F-68.0)*1.2) + (RH_p*0.094))
    heat_index = heat_index.rename('heat_index')

    # Combining temperature, relative humidity, and heat index into a dataset
    hi_set = xr.combine_by_coords((heat_index,T_F,RH_p))
        
    # Heat index for heat index above 80
    heat_index_80 = (-42.379 + 2.04901523*T_F + 10.14333127*RH_p - 0.22475541*T_F*RH_p 
          - 6.83783e-3*T_F**2 - 5.481717e-2*RH_p**2 + 1.22874e-3*T_F**2*RH_p 
          + 8.5282e-4*T_F*RH_p**2 - 1.99e-6*T_F**2*RH_p**2)
    hi_set['heat_index>80'] = heat_index_80
    
    # Replacing heat indices above 80 with the new equation
    hi_set['heat_index'] = xr.where(hi_set['heat_index']>80,
                                    hi_set['heat_index>80'],
                                    hi_set['heat_index']
                                    )
    
    # Heat index for relative humidity under 13% and temps between 80 and 112 F
    heat_index_13 = heat_index_80 - ((13-RH_p)/4) * np.sqrt((17 - abs(T_F - 95))/17)
    hi_set['heat_index_RH<13'] = heat_index_13
    
    hi_set['heat_index'] = xr.where(((hi_set['relative_humidity']<13) & 
                                         (hi_set['2m_temperature']>80) & 
                                         (hi_set['2m_temperature']<112)),
                                    hi_set['heat_index_RH<13'],
                                    hi_set['heat_index'])
    
    # Heat index for relative humidity over 85% and temps between 80 and 87 F
    heat_index_85 = heat_index_80 + ((RH_p-85)/10) * ((87-T_F)/5)
    hi_set['heat_index_RH>85'] = heat_index_85
    hi_set['heat_index'] = xr.where(((hi_set['relative_humidity']>85) & 
                                         (hi_set['2m_temperature']>80) & 
                                         (hi_set['2m_temperature']<87)),
                                    hi_set['heat_index_RH>85'],
                                    hi_set['heat_index'])
    
    # Picking out the heat index dataarray alone
    hi_alone = hi_set['heat_index']
    hi_alone = ((hi_alone - 32) / 1.8) + 273.15 # Fahrenheit to Kelvin

    return hi_alone



def wind_tot(uwind, vwind):
    """
    Calculates wind magnitude and angle
    
    Inputs:
        uwind (DataArray) - E-W wind component (m/s)
        vwind (DataArray) - N-S wind component (m/s)
    Outputs:
        wind_mag (DataArray) - Wind magnitude (m/s)
        wind_dir (DataArray) - Wind angle (deg)
        
    """
    wind_mag = np.sqrt(vwind**2 + uwind**2)
    wind_dir = np.arctan2(vwind/wind_mag, uwind/wind_mag)
    wind_dir = wind_dir * 180/np.pi
    
    return wind_mag, wind_dir

def wind_mag(uwind, vwind):
    """
    Calculates wind magnitude and angle
    
    Inputs:
        uwind (DataArray) - E-W wind component (m/s)
        vwind (DataArray) - N-S wind component (m/s)
    Outputs:
        wind_mag (DataArray) - Wind magnitude (m/s)
        
    """
    return np.sqrt(vwind**2 + uwind**2)
    


def wind_chill(t2m, wind):
    """
    https://www.weather.gov/safety/cold-wind-chill-chart
    
    Calculates wind chill in array format
    
    Inputs:
        t2m (DataArray) - Temperature in Kelvin format
        wind (DataArray) - Surface Wind in m/s
    Output:
        wind_chill_alone (DataArray) - Wind Chill array (in K) 
        
    """
    T_F = t2m * 9/5 - 459.67 # Convert to Fahrenheit
    sfcWind_mph = wind/0.44704  # Convert to mph
    sfcWind_mph = sfcWind_mph.rename('surface_wind')
    
    # Calculate wind chill
    wind_chill = 35.74 + 0.6215*T_F - 35.75*(sfcWind_mph**0.16) + 0.4275*T_F*(sfcWind_mph**0.16)
    wind_chill = wind_chill.rename('wind_chill')
    
    # Combining into one dataset
    wind_chill_set = xr.combine_by_coords((wind_chill,T_F,sfcWind_mph))
    
   # Note: The Wind Chill Temperature is defined only for temperatures at or below 50°F and wind speeds above 3 mph.
    wind_chill_set['wind_chill'] = xr.where(((wind_chill_set['2m_temperature']<50) & 
                                                  (wind_chill_set['surface_wind']>3)),
                                              wind_chill_set['wind_chill'],
                                              np.nan)    

    wind_chill_alone = wind_chill_set['wind_chill']
    wind_chill_alone = ((wind_chill_alone - 32) / 1.8) + 273.15 # Convert Fahrenheit to Kelvin
    
    return wind_chill_alone



def apparent_temperature(t2m, vp, wind):
    """
    https://confluence.ecmwf.int/display/FCST/New+parameters%3A+heat+and+cold+indices%2C+mean+radiant+temperature+and+globe+temperature
    
    Inputs:
        t2m - (DataArray) 2m temperature (K)
        vp - (DataArray) 2m vapor pressure (hPa)
        wind - (DataArray) 10 m wind speed (m/s)
    Outputs: 
        apparent_temperature - (DataArray) Apparent temperature (in K)
        
    """
    
    t2m_C = t2m - 273.15 # Kelvin to Celsius
    
    apparent_temperature = t2m_C + 0.33*vp - 0.7*wind - 4.0
    
    apparent_temperature += 273.15 # Celsius to Kelvin
    
    return apparent_temperature



def vapor_pressure(dewpoint):
    """
    https://www.weather.gov/epz/wxcalc_vaporpressure
    
    Returns vapor pressure in units of hPa/mb
    
    Input:
        dewpoint - (DataArray) 2m dewpoint temperature (K)
    Output:
        e - (DataArray) Vapor pressure (mb)
        
    """
    dewpoint_C = dewpoint - 273.15 # Kelvin to Celsius
    
    e = 6.11 * 10**((7.5*dewpoint_C)/(237.3+dewpoint_C))
    
    return e



def normal_effective_temperature(t2m, RH, wind):
    """
    Calculates normal effective temperature for a DataArray
    
    Inputs:
        t2m - (DataArray) 2m air temperature (K)
        RH - (DataArray) 2m relative humidity (decimal)
        wind - (DataArray) wind speed at 1.2 m above the ground (m/s)
    Output:
        net - (DataArray) normal effective temperature (K)
        
    """
    
    t2m_C = t2m - 273.15 # Kelvin to Celsius
    RH_p = RH*100
    
    net = (37 - 
           ((37-t2m_C)/(0.68-(0.0014*RH_p)+(1/(1.76+(1.4*wind**0.75)))))
           - (0.29*t2m_C*(1-(0.01*RH_p))))
    
    net += 273.15 # Celsius to Kelvin
    
    return net



def humidex(t2m, vp):
    """
    Calculate humidex for a DataArray
    
    Inputs:
        t2m (DataArray) - 2m air temperature in K
        vp (DataArray) - vapor pressure in hPa
    Output:
        humidex (DataArray) - Humidex in K
        
    """
    
    t2m_C = t2m - 273.15 # Kelvin to Celsius
    
    humidex = t2m_C + 0.5555*(vp - 10) 
    
    humidex += 273.15 # Celsius to Kelvin
    
    return humidex

def rel_hum(dewpoint, t2m):
    """
    Calculate relative humidity from dewpoint temperature

    Input:
        dewpoint (DataArray) - 2m dewpoint temperature in K
        t2m (DataArray) - 2m air temperature in K
    Output:
        relative_humidity (DataArray) - Relative humidity in decimals

    """
    vp_s = vapor_pressure(t2m) # Saturation vapor pressure
    vp = vapor_pressure(dewpoint)      # Vapor pressure

    relative_humidity = vp / vp_s

    return relative_humidity
        