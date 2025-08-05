"""
Tools for download of ISDLite data from National Centers for Environmental Information (NCEI), https://www.ncei.noaa.gov. 
"""

from pathlib import Path
import hashlib

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        wban_id (str): Weather Bureau Army Navy station ID.
    
    Returns:
        str: URL of a NCEI ISDLite station data file
    
    """
    
    url = isd_lite_url + '/' + str(year) + '/' + ISDLite_data_file_name(year,usaf_id,wban_id)
    
    return url
    
def ISDLite_data_file_name(year: int,usaf_id: str,wban_id: str) -> str:
    
    """
    
    Constructs the name of a ISDLite data file stored locally.
    
    Args:
      year (int): Gregorian year of the data
        usaf_id (str): Air Force station ID. May contain a letter in the first position.
        wban_id (str): Weather Bureau Army Navy station ID.
    
    Returns:
        str: Local file name.
    
    """
    
    file_name = usaf_id + '-' + wban_id + '-' + str(year) + '.gz'
    
    return file_name
    
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

def download_one(
    year: int,
    usaf_id: str,
    wban_id: str,
    local_dir: Path,
    refresh: bool = False,
    verbose : bool = False) -> Path:
    
    """
    
    Downloads an ISDLite data file for a given year and station.
    
    Args:
        year (int): Gregorian year of the data
        usaf_id (str): Air Force station ID. May contain a letter in the first position.
        wban_id (str): Weather Bureau Army Navy station ID.
        local_dir (Path): Local directory where the downloaded files will be saved.
        refresh (bool, optional): If True, download even if the file already exists. Defaults to False.
        verbose (bool): If True, print information. Defaults to False.
    
    Returns:
        Path: Local path of the downloaded file.
    
    """
    
    # Construct data URL and get ETag
    
    url = isdlite_data_url(year,usaf_id,wban_id)
    
    # Construct local file path
    
    local_file_path = local_dir / ISDLite_data_file_name(year,usaf_id,wban_id)
    
    # Download file
    
    download_file(url,local_file_path,refresh=refresh,verbose=verbose)
    
    return local_file_path

def download_many(
    start_year: int,
    end_year: int,
    ids: list[list[str]],
    local_dir: Path,
    n_jobs: int = 8,
    refresh: bool = False,
    verbose : bool = False
    ) -> list[Path]:
    
    """
    
    Downloads ISDLite data files for a given year range (inclusive) and given station IDs,
    even for files that already exists locally. A given number of parallel threads is used
    to accelerate download. The routine is parallelized over stations, but not over the 
    year range.
    
    Why an inclusive listing? Because that is the canonical definition of a range of years -
    it is not meant to exclude the last one.
    
    Args:
        start_year (int): Gregorian year of the first data file to be downloaded
        end_year (int): Gregorian year of the last data file to be downloaded
        list[list[str]]: A list of 2-element lists. Each inner list contains:
                         - First element : USAF = Air Force station ID. May contain a letter in the first position.  (str)
                         - Second element: WBAN = NCDC WBAN number (str)
        local_dir (Path): Local directory where the downloaded files will be saved.
        refresh (bool, optional): If True, download even if the file already exists. Defaults to False.
        verbose (bool): If True, print information. Defaults to False.
    
    Returns:
        list[Path]: List of local paths of the downloaded files.
    
    """
    
    # Construct URLs and local file paths 
    
    all_local_file_paths = []
    
    for year in range(start_year,end_year+1):
        
        urls = []
        
        local_file_paths = []
        
        for id in ids:
          
          usaf_id = id[0]
          wban_id = id[1]
          
          urls.append(isdlite_data_url(year,usaf_id,wban_id))
          
          local_file_paths.append(local_dir / ISDLite_data_file_name(year,usaf_id,wban_id))
          
        download_threaded(urls,local_file_paths,n_jobs=n_jobs,refresh=refresh,verbose=verbose)
        
        all_local_file_paths = all_local_file_paths + local_file_paths
        
    return all_local_file_paths
    
def check_station(
    start_year: int,
    end_year: int,
    usaf_id: str,
    wban_id: str,
    ) -> bool:
    
    """
    
    Checks whether observation files exist online for the given year range (inclusive) for a given station.
    
    For the given station, this function constructs URLs for each year 
    in the specified date range and checks if data are available at those URLs. 
    If all required files are present, the station is marked as available.
    
    Args:
        start_year (int): Gregorian year of the first data file to be checked.
        end_year (int): Gregorian year of the last data file to be checked.
        usaf_id (str): Air Force station ID. May contain a letter in the first position.
        wban_id (str): Weather Bureau Army Navy station ID.
    
    Returns:
        bool: True if data are available for all years in the date range, False otherwise.
    
    """
    
    for year in range(start_year, end_year + 1):
        url = isdlite_data_url(year, usaf_id, wban_id)
        if not url_exists(url):
            return False
    
    return True

def download_threaded(urls: list[str],paths: list[Path],n_jobs = 8,refresh: bool = False,verbose: bool = False):
    
    """
    
    Downloads a given number of files from given URLs to given local paths, in parallel.
    
    Args:
        urls (list[str]): List of URLs of files to download
        paths (list[Path]): List of local paths of downloaded files
        n_jobs (int): Maximum number of parallel downloads
        refresh (bool, optional): If True, download even if the file already exists. Defaults to False.
        verbose (bool): If True, print information. Defaults to False.
    
    Returns:
        
    
    """
    
    if len(urls) != len(paths):
        raise ValueError("The number of URLs must match the number of local paths.")
    
    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        futures = [
            executor.submit(download_file, url, path, refresh, verbose)
            for url, path in zip(urls,paths)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Download generated an exception: {exc}")

def download_file(url: str,local_file_path: Path,refresh: bool = False, verbose: bool = False):
    
    """
    
    Downloads a file from a given URL to a given local path.
    
        Args:
        url (str): URL of file to download
        local_file_path (Path): Local paths of downloaded files
        refresh (bool, optional): If True, download even if the file already exists. Defaults to False.
                                  When False:
                                  - if the local ETag of the file matches its ETag online, then the file will not be downloaded.
                                  - if the local ETag of the file differs from its ETag online, then the file will be downloaded.
        verbose (bool): If True, print information. Defaults to False.
    
    Returns:
        
    """
    
    # Get ETag
    
    response = requests.head(url)
    
    etag = response.headers.get("ETag")
    
    if etag is None:
        message = '\n' \
              + 'ETag not found of file at URL ' + url + '\n' \
              + 'This could mean the file does not exist at this URL.'
        raise Exception(message)
    
    # Check if file already exists and its ETag is the same as the ETag of the file requested for download
    
    etag_file_path = local_file_path.with_name(local_file_path.name + '.etag')
    
    if not refresh and local_file_path.exists() and etag_file_path.exists():
        
        # Read local ETag
        
        with open(etag_file_path, "r") as f:
            local_etag = f.read().strip()
        
        # Compare ETags
        
        if local_etag == etag:
            if verbose:
                print(url, 'available locally as', str(local_file_path), 'and ETag matches ETag online. Skipping download.')
            return
        else:
            if verbose:
                print(url, 'available locally as', str(local_file_path), 'and ETag differs from ETag online. Proceeding to download.')
    
    with requests.get(url, stream=True) as r:
        
        r.raise_for_status()
        
        with open(local_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    
    if verbose:
        print(url,'downloaded.')
    
    # Save ETag
    
    with open(etag_file_path, "w") as f:
        f.write(etag)
    
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
        response = requests.head(url, allow_redirects=True, timeout=20)
        return response.status_code == 200 # A status code of 200 means the file exists
    except requests.RequestException:
        return False

