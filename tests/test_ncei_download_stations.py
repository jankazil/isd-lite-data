"""
Test code for ncei.download_stations.
"""

from pathlib import Path
from isd_lite_data import ncei

local_dir = Path('..') / 'data'

local_file = ncei.download_stations(local_dir)

print()
print('Downloaded the file',str(local_file))
print()
