from pathlib import Path
from isd_lite_data import ncei

local_dir = Path('..') / 'data'

year = 2020
usaf_id = '010010'
wban_id = '99999'

local_file = ncei.download_one(2020,usaf_id,wban_id,local_dir,refresh=True,verbose=True)

print()
print('Downloaded the file',str(local_file))
print()
