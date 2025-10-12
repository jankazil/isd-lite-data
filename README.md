# isd-lite-data

**isd-lite-data** is a Python toolkit for downloading, filtering, and processing [Integrated Surface Dataset Lite (ISD‑Lite)](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite) observations from NOAA/NCEI.  

It provides:

- A top level command-line tool that
    
  - automates the download of ISD‑Lite station observations for
    - countries
    - U.S. states and territories
    - Regional Transmission Organization (RTO) / Independent System Operator (ISO) regions
    - individual stations
    
  - saves the full-hourly UTC time series of
    - sea level pressure
    - temperature at 2 m
    - dew point temperature at 2 m
    - relative humidity at 2 m
    - wind direction
    - wind speed at 10 m
    - 1 h accumulated precipitation
    - 6 h accumulated precipitation
    - sky condition
  
    for the stations in the selected country/state/territory/region, or for the selected station, for a user-specified time range, as a netCDF file.
  
- Modules for processing ISD‑Lite station observations.

## Installation (Linux / macOS)

```bash
mamba install -c jan.kazil -c conda-forge isd-lite-data
```

## Overview

The package provides a command‑line tool that selects stations by geography (a country, a U.S. state or territory, an RTO/ISO regions, the special region CONUS representing the contiguous United States, or single station by USAF/WBAN identifier), checks data availability, downloads ISD‑Lite observation files for a given year range, and writes a NetCDF file with full‑hourly UTC time series.

Geospatial region selection is based on The World Bank Official Boundaries, U.S. Energy Information Administration definitions of RTO/ISO footprints, and U.S. Census Bureau state/territory boundaries, included with the package.

The USAF/WBAN station identifiers are available in the [ISD Lite stations list](https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt).

## Workflow

The following describes the internal workflow performed by the command-line tool:

1. Load the region geometry if a region is specified; skip this step if a station ID is provided.
2. Retrieve the ISD station list from NCEI and either filter it spatially by region or select the specified station. Save the list as a text file.
3. Filter the stations by data availability for the requested year range, either online by probing NCEI directory listings or offline by checking local files.
4. Save the filtered station list for reference.
5. Download ISD‑Lite observation files from NCEI for the selected stations and years, skipping files already present that match by ETag; supports parallel downloads.
6. Load observations and construct a full‑hourly UTC time series dataset with variables such as temperature, dew point, sea‑level pressure, wind direction/speed, sky condition, and precipitation.
7. Save the full‑hourly UTC time series as a NetCDF file for the given station or the stations in the state/region.

## Command‑line interface (CLI)

The CLI is exposed as `"build-isd-lite-dataset"` when installed:

```bash
# Provide usage information, a list of two‑letter U.S. state/territory codes, and a list of RTO/ISO region names:
build-isd-lite-dataset --help

# ERCOT region:
build-isd-lite-dataset 2022 2022 ERCOT /path/to/data -n 32

# Colorado, work offline with previously downloaded data files:
build-isd-lite-dataset 2021 2021 CO /path/to/data --offline

# Individual station indentified with its USAF and WBAN IDs (space‑separated inside quotes):
build-isd-lite-dataset 2020 2025 "725650 03017" /path/to/data
```

**Positional arguments**

- `start_year` and `end_year`: Inclusive range of years.
- `region_name`:
  - a three-letter ISO 3166-1 alpha-3 country code (e.g., `CHE`, `USA`)
  - a two-letter U.S. state or territory code (e.g., `CA`, `PR`)
  - the special region `CONUS`
  - an RTO/ISO region code (`ERCOT`,`CAISO`,`ISONE`,`NYISO`,`MISO`,`SPP`,`PJM`)
  - or a double-quoted string containting the USAF station ID and and the WBAN station ID separated by a space, as in the [ISD Lite stations list](https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt)

- `data_dir`: Destination directory for station lists, downloads, and outputs.

**Options**

- `-n, --n INT`: Maximum number of parallel downloads.
- `-o, --offline`: Work offline; expect required files to be present in `data_dir`.

## Public API

### Modules

#### `isd_lite_data.ncei`
Utilities for ISD‑Lite station metadata and observation downloads.

- URLs for the ISD‑Lite station list and data files.
- `isd_lite_data_url(year, usaf_id, wban_id)`: Build the absolute URL to an ISD‑Lite observation file.
- `isd_lite_data_urls(start_year, end_year)`: Probe NCEI year directories and list available files.
- `isd_lite_data_file_name(year, usaf_id, wban_id)`: Construct the canonical ISD‑Lite file name.
- `isd_lite_data_file_paths(start_year, end_year, ids, local_dir)`: Build local paths for expected files.
- `download_stations(local_file)`: Download the ISD station list file.
- `download_one(...)`, `download_many(...)`, `download_threaded(...)`: Robust downloads with optional refresh behavior and parallelism.
- `download_file(url, local_path, refresh=False, verbose=False)`: Download with ETag checking and retries.

#### `isd_lite_data.rto_iso`
Helpers to work with RTO/ISO region polygons.

- `regions(rto_iso_geojson)`: Read GeoJSON and return a GeoDataFrame with merged geometries for each region.
- `region(rto_iso_geojson, region_name)`: Return a GeoDataFrame for a requested region.

#### `isd_lite_data.region_codes`
Provides

  - `isd_lite_data.region_codes.countries`: Three-letter ISO 3166-1 alpha-3 country codes
  - `isd_lite_data.region_codes.us_states_territories`: Two-letter U.S. state or territory codes
  - `isd_lite_data.region_codes.conus`: The special `CONUS` region code
  - `isd_lite_data.region_codes.rto_iso_regions`: RTO/ISO region codes

#### `isd_lite_data.stations`
Station catalog handling, filtering, reading, and writing.

- `Stations.from_url()` / `Stations.from_file(path)`: Build the station catalog from the ISD station list.
- Spatial selection by region geometry with `filter_by_region(region_gdf)` and by bounding box with `filter_by_coordinates(...)`.
- Availability filters: `filter_by_data_availability_online(start_time, end_time, verbose)` and `filter_by_data_availability_offline(data_dir, start_time, end_time, verbose)`.
- Station utilities: `filter_by_id(usaf_id, wban_id)`, `ids()`, `save_station_list(title, path)`.
- Observation handling: `load_observations(...)` to assemble an xarray dataset across stations and time; `write_observations2netcdf(path)` to save NetCDF.
- Per‑station parser: `read_station_observations(...)` for reading gzipped ISD‑Lite files.

## Development

### Code Quality and Testing Commands

- `make fmt` - Runs ruff format, which automatically reformats Python files according to the style rules in `pyproject.toml`
- `make lint` - Runs ruff check - -fix, which checks for style errors, bugs, outdated patterns, etc., and auto-fixes what it can.
- `make check` - Runs fmt and lint.
- `make type` - Currently disabled. Runs mypy, the static type checker, using the strictness settings from `pyproject.toml`. Mypy is a static type checker for Python, a dynamically typed language. Because static analysis cannot account for all dynamic runtime behaviors, mypy may report false positives which do no reflect actual runtime issues.
- `make test` - Runs pytest with reporting (configured in `pyproject.toml`).

## Notes

- Download operations are read‑only and anonymous.
- Downloads can be parallelized with a user‑specified number of threads greater than 1; this may increase network load or trigger rate limits.
- Files already present locally are skipped when their stored ETag matches the ETag online, unless `refresh=True` is used.
- Loading and aggregating large numbers of per‑station files can be slow.
- The ISD‑Lite station list sometimes contains ambiguous country codes; consider geographic filtering for disambiguation.

## Disclaimer

ISD‑Lite data are publicly available from NOAA's National Centers for Environmental Information and are subject to their terms of use. This project is not affiliated with or endorsed by NOAA.

This software uses data from the U.S. Census Bureau, the U.S. Energy Information Administration, and The World Bank Group, but is neither endorsed nor certified by these organizations. It is impermissible to represent or imply that they have sponsored, approved, or endorsed its use.

## Author

Jan Kazil – jan.kazil.dev@gmail.com – https://jankazil.com

## License

BSD 3‑Clause
