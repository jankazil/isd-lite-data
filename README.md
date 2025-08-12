# ISD-lite-data

**ISD-lite-data** is a Python toolkit for accessing, downloading, and processing [ISDLite](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite) data. ISDLite is a version of the Integrated Surface Database (ISD), provided online by NOAA's National Centers for Environmental Information (NCEI).

ISDLite is a product derived from [ISD](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database), which is easier to work with for general research and scientific purposes. It is a subset of the full ISD containing eight common surface parameters in a fixed-width format free of duplicate values, sub-hourly data, and complicated flags.

## Overview

The project consists of the following modules:

- **`ncei`**  
  Functions for:
  - Downloading ISDLite station metadata (`isd-history.txt`)
  - Downloading ISDLite data files
  - Downloading multiple ISDLite data files in parallel using threading

- **`stations`**  
  Classes and functions for:
  - Loading ISDLite station metadata from the web or local file
  - Saving ISDLite station metadata to file
  - Listing available countries and US states
  - Filtering stations by:
    - country
    - US state
    - geographic bounding box (latitude and longitude)
    - time period for which a station has observations (nominally)
    - time period for which a station has observations (actually available for download)
  - Listing selected station metadata:
    - station USAF and WBAN ID
    - station name
    - station coordinates
    - station elevation
    - period with observations
  - Loading station observations from ISDLite data files
  - Saving station observations and station metadata as a netCDF file
  - Reading station observations and station metadata from a netCDF file

## Public API

The list of modules, classes, and functions is documented in [public-api.md](docs/public-api.md).

## Dependencies

- Python ≥ 3.10
- Required Python packages:
  - `requests`
  - `pandas`
  - `numpy`
  - `xarray`

## Usage

All test scripts demonstrate usage and serve as functional examples. See:

- `test_ncei_download_stations.py`  
  Download the ISDLite station metadata file (`isd-history.txt`) and save it locally.

- `test_data_stations.py`  
  Initialize the station metadata database and print all available country and US state codes.

- `test_data_stations_filter_by_country.py`  
  Filter station metadata by country code (e.g., 'CH' for Switzerland/China), then further by geographic coordinates.

- `test_data_stations_filter_by_US_state.py`  
  Filter station metadata by US state code (e.g., 'CO' for Colorado).

- `test_data_stations_metadata.py`  
  Filter station metadata by country, geographic bounding box, and time period. Print selected station metadata.

- `test_ncei_download.py`  
  Download a sample ISDLite data file for a given year and station.

- `test_ncei_download_year_range.py`  
  Download ISDLite data files for a given range of years for a given list of stations.

### Regional Downloads - Switzerland

Scripts are intended to be executed in the following sequence:

- `scripts/Switzerland_Metadata_Download.py`  
  Download the ISDLite station metadata file and produce:
  - `data/switzerland_stations.2020-2023.txt`:  
     station metadata of Swiss stations for which observations are available for download between 2020-12-03 and 2023-12-31.
- `scripts/Switzerland_Observations_Download.py`  
  Download the ISDLite station observations and produce:
  - `data/USAF_ID-WBAN_ID-YYYY.gz`:  
     files with observations of Swiss stations for which observations are available for download between 2020-12-03 and 2023-12-31. 
- `scripts/Switzerland_Observations_Load.py`  
  Load the ISDLite station observationsand produce:
  - `data/switzerland_stations.2020-2023.nc`:  
     netCDF file with observations of Swiss stations for which observations are available for download between 2020-12-03 and 2023-12-31. 
- `scripts/Switzerland_Observations_Load_netCDF.py`  
  Load the ISDLite Swiss station observations from the netCDF file `data/switzerland_stations.2020-2023.nc`

### Regional Downloads - Contiguous United States (CONUS)

Scripts are intended to be executed in the following sequence:

- `scripts/CONUS_Metadata_Download.py`  
  Download the ISDLite station metadata file and produce:
  - `data/conus_stations.2020-2025.txt`:  
     station metadata of CONUS stations for which observations are available for download between 2020-12-03 and 2025-07-31.
- `scripts/CONUS_Observations_Download.py`  
  Download the ISDLite station observations and produce:
  - `data/USAF_ID-WBAN_ID-YYYY.gz`:  
     files with observations at CONUS stations for which observations are available for download between 2020-12-03 and 2025-07-31. 
- `scripts/CONUS_Observations_Load.py`  
  Load the ISDLite station observations and produce:
  - `data/conus_stations.2020-2025.nc`:  
     netCDF file with observations at CONUS stations for which observations are available for download between 2020-12-03 and 2025-07-31.
- `scripts/CONUS_Observations_Load_netCDF.py`  
  Load the ISDLite CONUS station observations the netCDF file `data/conus_stations.2020-2025.nc`

## Notes

- Download operations are read-only and anonymous; no credentials are required.
- Download operations can be parallelized for speed with a user-specified number > 1 of concurrent download requests.
- Download operations will check if local ISDLite metadata files and observation files are present; if they are and their ETag matches their ETag online, they will not be downloaded, unless instructed with refresh = True.
- Loading of thousands of observation data files from disk can be slow.
- Country codes in the ISDLite metadata are not always unambiguous. For example, `'CH'` is used for both Switzerland and China. Use geographic filtering to disambiguate.
- The CONUS and Switzerland scripts demonstrate a practical pipeline for identifiying, downloading, loading to memory, and saving as netCDF files regional ISDLite observation data. The Switzerland contain fewer stations than the CONUS scripts and are therefore faster.

## Disclaimer

The ISDLite data accessed by this software are publicly available from NOAA's National Centers for Environmental Information and are subject to their terms of use. This project is not affiliated with or endorsed by NOAA.

## Author

Jan Kazil - jan.kazil.dev@gmail.com - [jankazil.com](https://jankazil.com)

## License

BSD 3-clause
