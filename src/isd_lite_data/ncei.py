"""
Tools for download of ISDLite data from National Centers for Environmental Information (NCEI), https://www.ncei.noaa.gov. 
"""

from pathlib import Path
import hashlib

import requests

# ISDLite data URL
isd_lite_data_url = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-lite'

# ISD Station History (station meta data) file
isd_lite_stations_url = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt'

def download(year: int,usaf_id: str,wban_id: str,local_dir: Path,refresh: bool = False) -> Path:
    
    """
    
    Downloads an ISDLite data file for a given year and station, unless it already exists locally.
    
    Args:
        year (int): Gregorian year of the data
        usaf_id (str): Air Force station ID. May contain a letter in the first position.
        wban_id (str): NCDC WBAN number
    
    Returns:
        Path: Local path of the downloaded file.
    
    """
    
    # Construct data URL
    
    url = isd_lite_data_url + '/' + str(year) + '/' + usaf_id + '-' + wban_id + '-' + str(year) + '.gz'
    
    response = requests.head(url)
    
    etag = response.headers.get("ETag")
    
    if etag is None:
      message = '\n' \
              + 'ETag not found of file at URL ' + url + '\n' \
              + 'This could mean the file does not exist at this URL.'
      raise Exception(message)
    
    # Construct local file path
    
    local_file_name = usaf_id + '-' + wban_id + '-' + str(year) + '.gz'
    
    local_file_path = local_dir / local_file_name
    
    # Construct local etag file path
    
    etag_file_name = local_file_name + '.etag'
    
    etag_file_path = local_dir / etag_file_name
    
    # Check if file already exists and its ETag is the same as the ETag of the file requested for download
    
    if not refresh and local_file_path.exists() and etag_file_path.exists():
        
        # Read local ETag
        
        with open(etag_file_path, "r") as f:
            local_etag = f.read().strip()
        
        # Compare ETags
        
        if local_etag == etag:
            print(url, 'already available locally as ', str(local_file_path), '. Skipping download.')
            return local_file_path
    
    # Download file
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    f.write(chunk)
    
    # Save ETag
    
    with open(etag_file_path, "w") as f:
        f.write(etag)
    
    return local_file_path

