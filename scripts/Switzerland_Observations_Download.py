#!/usr/bin/env python

"""

Downloads IDSLite station observations for stations in a given stations database file.

"""

from datetime import datetime
from pathlib import Path

from isd_lite_data import ncei, stations

#
# Directory where data is located/will be placed
#

data_dir = Path('..') / 'data'

#
# Download data from stations in Switzerland with data for a given period
#

# Period of interest

start_year = 2020
end_year = 2023

start_date = datetime(start_year, 1, 1)
end_date = datetime(end_year, 12, 31)

# Load stations Switzerland for the period of interest

file_name = 'switzerland_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.txt'

file_path = data_dir / file_name

switzerland_stations = stations.Stations.from_file(file_path)

# Download observations

local_files = ncei.download_many(
    start_date.year,
    end_date.year,
    switzerland_stations.id(),
    data_dir,
    n_jobs=1, # njobs > 1 accelerates downloads significantly, but can result in network errors / the server refusing to cooperate due
    refresh=False,
    verbose=True,
)
