"""

Tests code that filters IDS station metadata by country.

In the IDS station database, countries are identified with
a two-letter country code. This code is not unambiguous.
For example, 'CH' is the country code for Switzerland and
China. Hence when providing the country code 'CH", one will
obtain station metadata for both countries. One can then
use the geographic filter to separate the two.

"""

from pathlib import Path
import pandas as pd
from isd_lite_data import data

data_dir = Path('..') / 'data'

all_stations = data.Stations.from_url()

switzerland_and_china_stations = all_stations.filter_by_country(['CH'])

min_lat = 40 
max_lat = 50
min_lon = 0
max_lon = 15

switzerland_stations = switzerland_and_china_stations.filter_by_coordinates(min_lat,max_lat,min_lon,max_lon)

with pd.option_context('display.max_rows', None):
  print(switzerland_stations.station_metadata)
