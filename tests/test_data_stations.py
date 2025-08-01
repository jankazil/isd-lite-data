"""

Tests code that initialize the station metadata database
and prints all available country and US state codes.

"""

from pathlib import Path
from isd_lite_data import data

data_dir = Path('..') / 'data'

all_stations = data.Stations.from_file(data_dir)

all_stations.print_countries()

all_stations.print_us_states()
