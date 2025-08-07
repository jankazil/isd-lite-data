"""

Downloads IDSLite station metadata and filters the stations to obtain stations
in Switzerland which have observations available for download in a specified
time period.

"""

from pathlib import Path
from datetime import datetime
from isd_lite_data import ncei
from isd_lite_data import stations

#
# Directory where data is located/will be placed
#

data_dir = Path('..') / 'data'

#
# Get metadata for all stations
#

all_stations = stations.Stations.from_url()

# Filter by country and coordinates

switzerland_and_china_stations = all_stations.filter_by_country(['CH'])

min_lat = 40
max_lat = 50
min_lon = 0
max_lon = 15

switzerland_stations = switzerland_and_china_stations.filter_by_coordinates(min_lat,max_lat,min_lon,max_lon)

# Filter by period of interest

start_year = 2020
end_year = 2023

start_date = datetime(start_year, 1, 1)
end_date = datetime(end_year, 12, 31)

switzerland_stations = switzerland_stations.filter_by_period(start_date,end_date)

# Filter the stations by whether observations are available for download for the given period

switzerland_stations = switzerland_stations.filter_by_data_availability(start_date,end_date,n_jobs=32,verbose=True)

#
# Save the stations metadata
#

file_name = 'switzerland_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.txt'

file_path = data_dir / file_name

switzerland_stations.save('Stations in Switzerland with observations ' + str(start_date.year) + ' - ' + str(end_date.year),file_path)

print()
print('Saved station metadata as',file_name)
print()
