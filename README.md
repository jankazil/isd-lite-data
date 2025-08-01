# ISD-lite-data

**ISD-lite-data** is a Python toolkit for accessing, downloading, and processing [ISDLite](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite) data. ISDLite is a version of the Integrated Surface Database (ISD), provided online by NOAA's National Centers for Environmental Information (NCEI).

ISDLite is a product derived from [ISD](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database), which is easier to work with for general research and scientific purposes. It is a subset of the full ISD containing eight common surface parameters in a fixed-width format free of duplicate values, sub-hourly data, and complicated flags.

## Overview

The project consists of the following modules:

- **`ncei.py`**  
  Functions for:
  - Downloading ISDLite data (unless present as local files).

- **`data.py`**  
  Classes and functions for:
  - Loading ISD station metadata from file (with download if needed).
  - Listing available countries and US states.
  - Filtering station metadata by country, US state, or geographic bounding box (latitude and longitude).
  - Returning filtered station metadata as a new object.

## Dependencies

- Python ≥ 3.10
- Required Python packages:
  - `requests`
  - `pandas`

## Usage

All test scripts demonstrate usage and serve as functional examples. See:

- `test_ncei_download.py`  
  Download a sample ISD Lite file for a given year and station.
- `test_ncei_download_year_range.py`  
  Download ISD Lite files for a given range of years for a given station.

- `test_data_stations.py`  
  Initialize the station metadata database and print all available country and US state codes.

- `test_data_stations_filter_by_country.py`  
  Filter the station metadata by country code (e.g., 'CH' for Switzerland/China), then further by geographic coordinates.

- `test_data_stations_filter_by_US_state.py`  
  Filter the station metadata by US state code (e.g., 'CO' for Colorado).

## Notes

- Download operations are read-only and anonymous; no credentials are required.
- Country codes in the ISD metadata are not always unambiguous. For example, the code `'CH'` is used for both Switzerland and China. Use the geographic filtering methods to disambiguate as needed.

## Disclaimer

The ISDLite data accessed by this software are publicly available from NOAA's National Centers for Environmental Information and are subject to their terms of use. This project is not affiliated with or endorsed by NOAA.

## Author

Jan Kazil - jan.kazil.dev@gmail.com - [jankazil.com](https://jankazil.com)

## License

BSD 3-clause
