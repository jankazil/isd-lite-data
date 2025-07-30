# ISD-lite-data

**ISD-lite-data** is a Python toolkit for accessing, downloading, and processing [ISDLite](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/) version data of the [Integrated Surface Database (ISD)](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) from NOAA.

ISDLite is product derived from ISD that makes it easier to work with for general research and scientific purposes. It is a subset of the full ISD containing eight common surface parameters in a fixed-width format free of duplicate values, sub-hourly data, and complicated flags.[^1]

## Overview

The project consists of two main modules:

- **`ncei.py`**: Functions for
  - Downloading ISDLite data

## Dependencies

- Python ≥ 3.10
- Required Python packages:
  - `requests`

## Usage

All test scripts demonstrate usage and serve as functional examples. See:

- `test_ncei_download.py` for downloading a sample ISD Lite file

## Notes

- Download operations are read-only and anonymous; no credentials are required.

## Disclaimer

The ISDLite data accessed by this software are publicly available from NOAA and are subject to their terms of use. This project is not affiliated with or endorsed by NOAA.

## Author
Jan Kazil - jan.kazil.dev@gmail.com - [jankazil.com](https://jankazil.com)

## License

BSD 3-clause

[^1]: [NOAA ISD](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database)
