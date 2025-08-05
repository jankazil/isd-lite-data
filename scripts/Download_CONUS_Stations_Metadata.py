"""

Downloads IDSLite station metadata and filters the stations to obtain stations
in the contiguous US which have observations available for download in a specified
time period.

"""

from pathlib import Path
from datetime import datetime
from isd_lite_data import ncei
from isd_lite_data import stations

# Data directory

data_dir = Path('..') / 'data'

#
# Stations in the contiguous US (CONUS)
#

# Identify contiguous US (CONUS) stations

all_stations = stations.Stations.from_url()

conus_stations = all_stations.filter_by_country(['US'])

min_lat = 25
max_lat = 50
min_lon = -125
max_lon = -65

# Save the stations in the contiguous US (CONUS)

file_name = 'conus_stations.txt'

file_path = data_dir / file_name

conus_stations.save('Stations in the contiguous US (CONUS)',file_path)

#
# Stations in the contiguous US (CONUS) with data for a given period
#

# Period of interest

start_date = datetime(2020, 12, 3)
end_date = datetime(2025, 7, 31)

# Load the stations in the contiguous US (CONUS)

file_name = 'conus_stations.txt'

file_path = data_dir / file_name

conus_stations = stations.Stations.from_file(file_path)

# Filter the stations by whether observations are available for download for the given period

conus_stations = conus_stations.filter_by_data_availability(start_date,end_date,n_jobs=32,verbose=True)

# Save the stations in the contiguous US (CONUS) that have observations are available for download for the given period

file_name = 'CONUS.' + str(start_date) + '-' + str(end_date) + '.txt'

file_path = data_dir / file_name

conus_stations.save('Stations in the contiguous US (CONUS), with observations available for download in the period ' + str(start_date) + ' - ' + str(end_date),file_path)

print('Saved file with stations meeting criteria: ',file_path)

