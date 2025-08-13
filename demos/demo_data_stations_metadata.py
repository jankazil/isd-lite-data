"""
Demonstrates country filtering, geographic filtering, time filtering, and how station metadata can be listed.
"""

from datetime import datetime
from pathlib import Path

from isd_lite_data import stations

# Directory where data is located/will be placed

data_dir = Path('..') / 'data'

# Get metadata for all stations

all_stations = stations.Stations.from_url()

# Filter by country and coordinates

switzerland_and_china_stations = all_stations.filter_by_country(['CH'])

min_lat = 40
max_lat = 50
min_lon = 0
max_lon = 15

switzerland_stations = switzerland_and_china_stations.filter_by_coordinates(
    min_lat, max_lat, min_lon, max_lon
)

# Filter by period of interest

start_date = datetime(2020, 1, 1)
end_date = datetime(2023, 12, 31)

switzerland_2020_to_2023_stations = switzerland_stations.filter_by_period(start_date, end_date)

#
# Print select station metadata
#

for name, elevation, coordinates in zip(
    switzerland_2020_to_2023_stations.name(),
    switzerland_2020_to_2023_stations.elevation(),
    switzerland_2020_to_2023_stations.coordinates(),
    strict=False,
):
    print(name, coordinates[0], coordinates[1], elevation)
