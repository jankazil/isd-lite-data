# ISD-lite-data

**ISD-lite-data** is a Python toolkit for accessing, downloading, and processing [ISDLite](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite) data. ISDLite is a version of the Integrated Surface Database (ISD), provided online by NOAA's National Centers for Environmental Information (NCEI).

ISDLite is product derived from [ISD](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database), which is easier to work with for general research and scientific purposes. It is a subset of the full ISD containing eight common surface parameters in a fixed-width format free of duplicate values, sub-hourly data, and complicated flags.

## Overview

The project consists of the following modules:

- **`ncei.py`**: Functions for
  - Downloading ISDLite data

## Dependencies

- Python ≥ 3.10
- Required Python packages:
  - `requests`

## Usage

All test scripts demonstrate usage and serve as functional examples. See:

- `test_ncei_download.py` for downloading a sample ISD Lite file for a given year and station 
- `test_ncei_download_year_range.py` for downloading ISD Lite files for a given range of years, for a given station 

## Notes

- Download operations are read-only and anonymous; no credentials are required.

## Disclaimer

The ISDLite data accessed by this software are publicly available from NOAA's National Centers for Environmental Information and are subject to their terms of use. This project is not affiliated with or endorsed by NOAA.

## Author
Jan Kazil - jan.kazil.dev@gmail.com - [jankazil.com](https://jankazil.com)

## License

BSD 3-clause
