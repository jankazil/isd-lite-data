#!/usr/bin/env python

"""
Demonstrates loading ISD Lite station metadata and observations from a local netCDF file.

The file must have been previously created with CONUS_Observations_Load.py

The metadata are loaded into the Stations.metadata Pandas Dataframe, the observations
into the Stations.observations xarray Dataset.
"""

from datetime import datetime
from pathlib import Path

from isd_lite_data import stations

#
# Directory where data is located/will be placed
#

data_dir = Path('..') / 'data'

#
# Load metadata
#

start_year = 2020
end_year = 2025

start_date = datetime(start_year, 1, 1)
end_date = datetime(end_year, 12, 31)

#
# Load metadata and observations from the netCDF
#

nc_file_name = 'conus_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.nc'

nc_file_path = data_dir / nc_file_name

conus_stations = stations.Stations.from_netcdf(nc_file_path)
