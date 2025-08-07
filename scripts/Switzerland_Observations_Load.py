"""
Demonstrates loading ISD Lite station observations for a set of stations and a specified year range
from local gzipped NCEI ISD Lite files. The observations are loaded into the Stations.observations
xarray dataset, and saved to a netCDF file
"""

from datetime import datetime
from pathlib import Path

import xarray as xr

from isd_lite_data import ncei
from isd_lite_data import stations

#
# Directory where data is located/will be placed
#

data_dir = Path('..') / 'data'

#
# Load metadata
#

start_year = 2020
end_year = 2023

start_date = datetime(start_year, 1, 1)
end_date = datetime(end_year, 12, 31)

file_name = 'switzerland_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.txt'

file_path = data_dir / file_name

switzerland_stations = stations.Stations.from_file(file_path)

#
# Load observations from local files (must have been previously downloaded)
#

switzerland_stations.load_observations(data_dir,start_year,end_year,verbose=True)

#
# Save observations as netCDF file
#

nc_file_name = 'switzerland_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.nc'

nc_file_path = data_dir / nc_file_name

switzerland_stations.write_observations2netcdf(nc_file_path)
