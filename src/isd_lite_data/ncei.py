"""
Tools for download of ISDLite data from National Centers for Environmental Information (NCEI), https://www.ncei.noaa.gov. 
"""

from pathlib import Path
import hashlib

import requests

# ISDLite data URL
isd_lite_url = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-lite'

# Integrated Surface Database (ISD) Station History (station meta data) file
isd_lite_stations_url = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt'

def isdlite_data_url(year: int,usaf_id: str,wban_id: str) -> str:
    
    """
    
    Constructs the URL of a NCEI ISDLite station data file.
    
    Args:
        year (int): Gregorian year of the data
        usaf_id (str): Air Force station ID. May contain a letter in the first position.
        wban_id (str): NCDC WBAN number
    
    Returns:
        str: URL of a NCEI ISDLite station data file
    
    """
    
    url = isd_lite_url + '/' + str(year) + '/' + usaf_id + '-' + wban_id + '-' + str(year) + '.gz'
    
    return url
    
def download_year_range(
    start_year: int,
    end_year: int,
    usaf_id: str,
    wban_id: str,
    local_dir: Path,
    refresh: bool = False,
    verbose : bool = False
    ) -> list[Path]:
    
    """
    
    Downloads ISDLite data files for a given year range (inclusive) and station,
    except for files that already exists locally.
    
    Why an inclusive listing? Because that is the canonical definition of a range of years -
    it is not meant to exclude the last one.
    
    Args:
        start_year (int): Gregorian year of the first data file to be downloaded
        end_year (int): Gregorian year of the last data file to be downloaded
        usaf_id (str): Air Force station ID. May contain a letter in the first position.
        local_dir (Path): Local directory where the downloaded files will be saved.
        refresh (bool, optional): If True, download even if the file already exists. Defaults to False.
        verbose (bool): If True, print information. Defaults to False.
    
    Returns:
        list[Path]: List of local paths of the downloaded files.
    
    """
    
    local_file_paths = []
    
    for year in range(start_year,end_year+1):
        local_file_path = download(year,usaf_id,wban_id,local_dir,refresh,verbose)
        local_file_paths.append(local_file_path)
    
    return local_file_paths
    
def download(year: int,usaf_id: str,wban_id: str,local_dir: Path,refresh: bool = False,verbose : bool = False) -> Path:
    
    """
    
    Downloads an ISDLite data file for a given year and station, unless it already exists locally.
    
    Args:
        year (int): Gregorian year of the data
        usaf_id (str): Air Force station ID. May contain a letter in the first position.
        wban_id (str): NCDC WBAN number
        local_dir (Path): Local directory where the downloaded files will be saved.
        refresh (bool, optional): If True, download even if the file already exists. Defaults to False.
        verbose (bool): If True, print information. Defaults to False.
    
    Returns:
        Path: Local path of the downloaded file.
    
    """
    
    # Construct data URL and get ETag
    
    url = isdlite_data_url(year,usaf_id,wban_id)
    
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
            if verbose:
              print(url, 'already available locally as ', str(local_file_path), '. Skipping download.')
            return local_file_path
    
    # Download file
    
    with requests.get(url, stream=True) as r:
        if verbose:
            print('Downloading',url)
        r.raise_for_status()
        with open(local_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    f.write(chunk)
    
    # Save ETag
    
    with open(etag_file_path, "w") as f:
        f.write(etag)
    
    return local_file_path

def download_stations(local_file: Path):
    
    """
    
    Downloads the Integrated Surface Database (ISD) Station History (station meta data) file
    
    Args:
        local_file (Path): Local file where the downloaded file will be saved.
    
    Returns:
        
    
    """
    
    # Download file
    
    with requests.get(isd_lite_stations_url, stream=True) as r:
        r.raise_for_status()
        with open(local_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    f.write(chunk)
    
    return

def url_exists(url: str) -> bool:
    """
    Checks if a file exists at a given URL.
    
    Args:
        url (str): uniform resource locator
        
    Returns:
        bool: True if file exists at given URL, False othewise
    """
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        return response.status_code == 200 # A status code of 200 means the file exists
    except requests.RequestException:
        return False
