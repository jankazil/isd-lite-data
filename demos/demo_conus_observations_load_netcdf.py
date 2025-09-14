'''
This script demonstrates how to load ISD-Lite station metadata and observations 
from a previously created NetCDF file. The NetCDF file must have been generated 
using demo_conus_observations_create_netcdf.py.

Workflow:
1. Defines the data directory where the NetCDF file is stored.
2. Constructs the NetCDF file name based on the start and end year.
3. Loads both station metadata and observations from the NetCDF file into a 
   Stations object.
   - Metadata are loaded into the `Stations.metadata` Pandas DataFrame.
   - Observations are loaded into the `Stations.observations` xarray Dataset.

Input:
    - A NetCDF file named:
        conus_stations.<start_year>-<end_year>.nc
      located in the data directory.

Output:
    - A fully populated Stations object with both metadata and observations 
      available for further analysis.

Example:
    Run this script after creating the NetCDF file with 
    demo_conus_observations_create_netcdf.py. The resulting `conus_stations` 
    object can then be used for data inspection, visualization, or analysis.
'''

from datetime import datetime
from pathlib import Path

from isd_lite_data import stations

#
# Directory where data are located/will be placed
#

data_dir = Path('..') / 'data'

#
# Load metadata
#

start_year = 2020
end_year = 2020

start_date = datetime(start_year, 1, 1)
end_date = datetime(end_year, 12, 31)

#
# Load metadata and observations from the netCDF file
#

nc_file_name = 'conus_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.nc'

nc_file_path = data_dir / nc_file_name

conus_stations = stations.Stations.from_netcdf(nc_file_path)
