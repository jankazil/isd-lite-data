"""
Test code for ncei.download_stations.
"""

from pathlib import Path

from isd_lite_data import ncei

local_file = Path('..') / 'data/isd-history.txt'

ncei.download_stations(local_file)

print()
print('Downloaded the file', str(local_file))
print()
