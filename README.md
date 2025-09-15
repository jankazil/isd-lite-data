# ISD-lite-data

**ISD-lite-data** is a Python toolkit for accessing, downloading, and processing [ISDLite](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite) data. ISDLite is a version of the Integrated Surface Database (ISD), provided online by NOAA's National Centers for Environmental Information (NCEI).

ISDLite is a product derived from [ISD](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database), which is easier to work with for general research and scientific purposes. It is a subset of the full ISD containing eight common surface parameters in a fixed-width format free of duplicate values, sub-hourly data, and complicated flags.

## Installation (Linux / macOS)

```bash
pip install git+https://github.com/jankazil/isd-lite-data
```

## Usage

This repository provides the following top-level scripts for downloading and pre-processing ISDLite data for the contiguous United States and Switzerland. The workflow is

1. Download ISD-Lite station metadata file
2. Download ISD-Lite station observations

### Contiguous United States

**1. CONUS_Metadata_Download.py**

Downloads IDS-Lite station metadata and filters it to produce a list of stations located in the contiguous United States (CONUS) that have observations available within a user-specified date range. The resulting metadata file is saved.

**Usage:**  

```bash
CONUS_Metadata_Download.py <start_year> <start_month> <start_day> <end_year> <end_month> <end_day> <data_dir>
```

**Example:**  

```bash
CONUS_Metadata_Download.py 2020 1 1 2020 12 31 output_data
```

**2. CONUS_Observations_Download.py**

Downloads IDS-Lite station observations for all stations listed in the previously generated CONUS metadata file. Supports parallel downloads for faster retrieval.

**Usage:**  

```bash
CONUS_Observations_Download.py <start_year> <start_month> <start_day> <end_year> <end_month> <end_day> <data_dir> [-n <n_parallel>]
```

**Example:**  

```bash
CONUS_Observations_Download.py 2020 1 1 2020 12 31 output_data -n 4
```

### Switzerland

**1. Switzerland_Metadata_Download.py**

Downloads IDS-Lite station metadata and filters it to produce a list of stations located in Switzerland that have observations available within a user-specified date range. The resulting metadata file is saved.

**Usage:**  

```bash
Switzerland_Metadata_Download.py <start_year> <start_month> <start_day> <end_year> <end_month> <end_day> <data_dir>
```

**Example:**  

```bash
Switzerland_Metadata_Download.py 2020 1 1 2020 12 31 output_data
```

**2. Switzerland_Observations_Download.py**

Downloads IDS-Lite station observations for all stations listed in the previously generated Switzerland metadata file. Supports parallel downloads for faster retrieval.

**Usage:**  

```bash
Switzerland_Observations_Download.py <start_year> <start_month> <start_day> <end_year> <end_month> <end_day> <data_dir> [-n <n_parallel>]
```

**Example:**  

```bash
Switzerland_Observations_Download.py 2020 1 1 2020 12 31 output_data -n 4
```

## Demo Scripts

This repository provides example scripts demonstrating downloading and pre-processing IDS-Lite data in the directory demos.

## Public API

The list of modules, classes, and functions is documented in [public-api.md](docs/public-api.md).

### Modules Overview

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

## Development

### Code Quality and Testing Commands

- `make fmt` - Runs ruff format, which automatically reformats Python files according to the style rules in `pyproject.toml`
- `make lint` - Runs ruff check --fix, which lints the code (checks for style errors, bugs, outdated patterns, etc.) and auto-fixes what it can.
- `make check` - Runs fmt and lint.
- `make type` - Runs mypy, the static type checker, using the strictness settings from `pyproject.toml`. Mypy is a static type checker for Python, a dynamically typed language. Because static analysis cannot account for all dynamic runtime behaviors, mypy may report false positives which do no reflect actual runtime issues. The usefulness of mypy is therefore limited, unless the developer compensates with extra work for the choices that were made when Python was originally designed.
- `make test` - Runs pytest with coverage reporting (configured in `pyproject.toml`).

## Notes

- Download operations are read-only and anonymous; no credentials are required.
- Download operations can be parallelized for speed with a user-specified number > 1 of concurrent download requests. This may increase network load or trigger rate limits.
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
