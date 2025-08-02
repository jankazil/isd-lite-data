"""

Tests code that filters IDS station metadata by US state.

"""

from pathlib import Path
import pandas as pd
from isd_lite_data import data

data_dir = Path('..') / 'data'

all_stations = data.Stations.from_url()

colorado_stations = all_stations.filter_by_us_state(['CO'])

with pd.option_context('display.max_rows', None):
  print(colorado_stations.station_metadata)
