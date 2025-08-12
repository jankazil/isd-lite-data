# Public API

This document declares the **public API**&nbsp; for this project as of **v1.1.5**.

---

## Module: `ncei`

Utilities for downloading ISD‑Lite station metadata and observations from NCEI.

### `download_stations(local_file: pathlib.Path) -> None`

Download the Integrated Surface Database (ISD) **Station History**&nbsp; (station metadata) file and write it to `local_file`.

### `download_one(year: int, usaf_id: str, wban_id: str, local_dir: pathlib.Path, refresh: bool = False, verbose: bool = False) -> pathlib.Path`

Download a single gzipped ISD‑Lite file for one station and year.

### `download_many(start_year: int, end_year: int, ids: list[list[str]], local_dir: pathlib.Path, n_jobs: int = 8, refresh: bool = False, verbose: bool = False) -> list[pathlib.Path]`

Download multiple gzipped ISD‑Lite files for stations specified by their station IDs, and a given range of years.

---

## Module: `stations`

Tools for selecting stations, loading ISD‑Lite observations, and exporting to NetCDF.

> **Instantiation:**&nbsp; The stable way to create a `Stations` instance is via the class methods listed below.

### Class: `Stations`

Represents a collection of ISD stations and metadata and observations in the `xarray.Dataset` `self.observations`.

#### Constructors

* `@classmethod from_url(cls) -> Stations`  
  Download the ISD Station History (station metadata) file from NCEI and build an instance that has populated metadata, but not yet populated observations.  

* `@classmethod from_file(cls, file_path: pathlib.Path) -> Stations`  
  Read a local ISD Station History–formatted (station metadata) file and build an instance that has populated metadata, but not yet populated observations.  

* `@classmethod from_netcdf(cls, file_path: pathlib.Path) -> Stations`  
 Load station metadata and observations from a NetCDF file (e.g. created by `write_observations2netcdf`), and build an instance that has populated metadata and observations. See section 'I/O, Persistence'.  
  
* `@classmethod from_dataset(cls, fds: xarray.Dataset) -> Stations`  
 Initialize station metadata and observations from an xarray.Dataset and build an instance that has populated metadata and observations. The xarray.Dataset must have the same structure as `self.observations`.
  
#### I/O, Persistence

* `save(self, title_line: str, file_path: pathlib.Path) -> Stations`  
  Write the current station metadata to a Station History–formatted text file.

* `load_observations(self, data_dir: pathlib.Path, start_year: int, end_year: int, verbose: bool = False) -> None`  
  Read observation data from gzipped ISD-Lite files for all stations in the current metadata and the given year range. The ISD-Lite files contain hourly records but may have gaps; a common time dimension is built from the union of all station timestamps, with `NaN` where values are missing. Stores the result in `self.observations: xarray.Dataset`:

  * **Dims:**&nbsp; `time`, `station`  
  * **Data variables:**&nbsp; `T, TD, SLP, WD, WS, SKY, PREC1H, PREC6H`  
  * **Per‑station variables:**&nbsp; `lat, lon, elevation, station_name, station_id, country, us_state`  
  * **Attrs:**&nbsp; `title`, `source`, `URL`  
    **Requirements:**&nbsp; Required `.gz` files must already exist in `data_dir` (e.g., via `ncei.download_many`).  

* `write_observations2netcdf(self, file_path: pathlib.Path) -> None`  
  Write `self.observations` to NetCDF with float32 data variables and CF‑style time encoding.  

* `@classmethod from_netcdf(cls, file_path: pathlib.Path) -> Stations`  
  Load station metadata and observations from a NetCDF file. Creates and populates both `self.meta_data` (as a Pandas DataFrame) and `self.observations` (as an `xarray.Dataset`) with the same structure and attributes as when originally written. The NetCDF file must have been created by `write_observations2netcdf` or follow the encoding and variable naming conventions produced by `write_observations2netcdf`. Returns a fully populated `Stations` instance ready for further filtering, analysis, or re-export.

#### Inspection / printing

* `print_countries(self) -> None`  
  Print two‑letter country codes present in the station metadata.

* `print_us_states(self) -> None`  
  Print two‑letter US state codes present in the station metadata.

#### Filtering (return new `Stations`)

All filters return a new `Stations` with filtered stations and metadata but without observation data; original is unchanged.

* `filter_by_country(self, countries: list[str]) -> Stations`  
  Returns a new `Stations` instance containing only stations and metadata whose country code is in the given list.

* `filter_by_us_state(self, us_states: list[str]) -> Stations`  
  Returns a new `Stations` instance containing only stations and metadata whose US state code is in the given list.

* `filter_by_coordinates(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> Stations`  
  Returns a new `Stations` instance containing only stations and metadata within the specified latitude/longitude range.

* `filter_by_period(self, start_time: datetime.datetime, end_time: datetime.datetime) -> Stations`  
  Returns a new `Stations` instance containing only stations and metadata whose nominal data period fully covers the given date range.

* `filter_by_data_availability(self, start_time: datetime.datetime, end_time: datetime.datetime, n_jobs: int = 8, verbose: bool = False) -> Stations`  
  Returns a new `Stations` instance containing only stations and metadata with downloadable observation files for all years in the inclusive date range.

#### Metadata accessors

Return simple Python collections derived from the current metadata.

* `id(self) -> list[list[str]]`  
  Returns the USAF and WBAN identifiers for each station as a list of `[USAF, WBAN]` string pairs.

* `coordinates(self) -> list[list[float]]`  
  Returns the latitude and longitude for each station as a list of `[lat, lon]` floats, with `NaN` where values are missing.

* `name(self) -> list[str]`  
  Returns the station names as a list of strings.

* `elevation(self) -> list[float]`  
  Returns the elevation above sea level for each station in meters, as floats with `NaN` where values are missing.

* `data_period(self) -> list[list[datetime.datetime]]`  
  Returns the nominal data coverage period for each station as a list of `[BEGIN, END]` datetimes.

* `meta_data(self) -> list[list]`  
  Returns the complete station metadata table as a nested list, preserving all 11 canonical columns.

---

## Behavioral notes

* **Time ranges are inclusive**&nbsp; of both `start_year` and `end_year`.
* **ETag caching**: `ncei.download_one`/`download_many` create and use `*.etag` files for skip‑download decisions when `refresh=False`.

---

*End of public API.*
