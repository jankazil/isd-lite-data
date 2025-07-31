from pathlib import Path
from isd_lite_data import ncei

local_dir = Path('..') / 'data'

start_year = 2020
end_year = 2025

usaf_id = '010010'
wban_id = '99999'

local_files = ncei.download_year_range(start_year,end_year,usaf_id,wban_id,local_dir)

print()
print('Downloaded the files')
for local_file in local_files:
  print(str(local_file))
print()
