from pathlib import Path

from isd_lite_data import ncei

local_dir = Path('..') / 'data'

start_year = 2020
end_year = 2025

# List of two-element lists, each of which holds the USAF and WBAN station ID

ids = [['723940', '23273'], ['010010', '99999']]

# Download
local_files = ncei.download_many(
    start_year, end_year, ids, local_dir, n_jobs=8, refresh=True, verbose=True
)

# Print local files

print()
print('Downloaded the files')
print()
for local_file in local_files:
    print(str(local_file))
print()
