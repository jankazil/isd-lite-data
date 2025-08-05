"""

Downloads IDSLite station observations for stations in a given stations database file.

"""

from pathlib import Path
from datetime import datetime
from isd_lite_data import ncei
from isd_lite_data import stations

#
# Download data from stations in the contiguous US (CONUS) with data for a given period
#

# Data directory

data_dir = Path('..') / 'data'

# Period of interest

start_date = datetime(2020, 12, 3)
end_date = datetime(2025, 7, 31)

# Load stations in the contiguous US (CONUS)

file_name = 'CONUS.' + str(start_date) + '-' + str(end_date) + '.txt'

file_path = data_dir / file_name

conus_stations = stations.Stations.from_file(file_path)

# Download data

local_files = ncei.download_many(
    start_date.year,
    end_date.year,
    conus_stations.id(),
    data_dir,
    n_jobs=32,
    refresh = False,
    verbose=True)
