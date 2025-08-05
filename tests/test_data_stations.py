"""

Tests code that initialize the station metadata database
and prints all available country and US state codes.

"""

from pathlib import Path
from isd_lite_data import stations

data_dir = Path('..') / 'data'

all_stations = stations.Stations.from_url()

all_stations.print_countries()

all_stations.print_us_states()
