# Agent Development Guide

## Project Scope and Isolation

**The project root is the directory containing this `AGENTS.md` (`.` relative to this file). These instructions apply throughout that repository, including after it is moved.**

- Treat the repository root as the exclusive project boundary. Read, search, analyze, and modify project files only inside it. Run commands from the root unless an in-repository script requires another directory within the root.
- Do not inspect parent directories, sibling projects, unrelated workspaces, global configuration, installed dependency source, or other external locations for project context. Do not import assumptions from other projects or previous sessions.
- Check path containment before following symbolic links, Git worktree/submodule references, object alternates, configuration includes, or paths that may escape through `..`. Do not follow references outside the root.
- External URLs, paths, and credentials mentioned in repository files describe interfaces or dependencies; they do not authorize accessing resources outside the root. Report any required outside-root access instead of performing it.
- Keep development inputs, generated outputs, caches, fixtures, and temporary project files inside the root. Inspect scripts for filesystem, environment, network, or deployment effects before running them. Do not run workflows that cross the project boundary under these instructions.

## General Agent Instructions

- Base project-specific decisions on verified repository contents. Read the files relevant to the task; distinguish implementation facts, documentation claims, test results, and unverified assumptions.
- Before editing, inspect the relevant code and working-tree state. Make focused changes, preserve unrelated work, and retain established interfaces and data contracts unless the task requires otherwise.
- Use relevant checks that are safe in the available environment. Do not confuse a passing placeholder test, build, or lint check with end-to-end or scientific correctness. Report the checks actually run, failures, and verification gaps.
- Do not expose credentials, or perform publishing, deployment, destructive environment setup, or other unrelated side effects as routine development or validation.

## Project Overview and Repository Map

This Python package downloads NOAA/NCEI Integrated Surface Dataset Lite
(ISD-Lite) station observations, selects stations geographically or by identifier,
and writes station metadata and UTC time-series NetCDF datasets. See
[README.md](README.md) for the public API, CLI arguments, and examples.

| Path | Responsibility |
| --- | --- |
| `src/isd_lite_data/build_isd_lite_dataset.py` | Argument parsing, packaged geometry loading, and CLI orchestration. |
| `src/isd_lite_data/ncei.py` | NOAA URLs, filenames, directory listings, threaded downloads, retries, and ETag sidecars. |
| `src/isd_lite_data/stations.py` | `Stations` metadata container, filtering, gzip parsing, xarray assembly, and NetCDF reading/writing. |
| `src/isd_lite_data/region_codes.py` | Country, U.S. state/territory, CONUS, and RTO/ISO selectors. |
| `src/isd_lite_data/rto_iso.py` | EIA polygon selection and merging into seven RTO/ISO regions. |
| `src/isd_lite_data/data/` | Bundled EIA GeoJSON and Census Bureau/World Bank shapefiles with provenance documents. |
| `src/isd_lite_data/__init__.py` | Distribution version lookup, with a local fallback. |
| `tests/test_test.py`, `src/isd_lite_data/test.py` | One placeholder test of an integer addition helper. |
| `pyproject.toml`, `environment.yml` | Packaging, dependencies, Python compatibility, and tool settings. |
| `Makefile`, `scripts/`, `.github/workflows/ci.yml` | Development commands, environment setup, conda publishing, and CI. |

## Architecture and Data Contracts

1. The CLI resolves a country, U.S. state/territory, CONUS, RTO/ISO code, or quoted
   `"USAF WBAN"` pair. Geometry resources are loaded via `importlib.resources`;
   individual station selection skips geometry loading.
2. `Stations` reads the fixed-width station catalog, cleans metadata, and selects
   stations by geometry or identifier. Spatial filtering reprojects regions to
   EPSG:4326 and uses `within`, which excludes points on polygon boundaries.
3. Availability filtering requires an observation file for **every year** in the
   inclusive requested range. Online mode examines NOAA year directory listings;
   offline mode checks local files.
4. Online mode downloads station/year gzip files through `ncei`. The CLI saves
   `isd-history.txt`, `<selector>.<start>-<end>.txt`, and
   `<selector>.<start>-<end>.nc` in the supplied `data_dir`; observation filenames
   are `<USAF>-<WBAN>-<year>.gz`, with `.etag` sidecars.
5. `stations.py` parses observations and builds an xarray dataset with
   `time` and `station` dimensions and station metadata variables, then writes
   NetCDF. Filters return new `Stations` objects; loading observations mutates the
   object's `observations`.

Preserve station identifiers as strings, including leading zeros and possible
letters. The parser treats `-9999` as missing, converts trace precipitation
`-1` to zero, and scales temperature, dew point, pressure, wind speed, and
precipitation by 0.1. Keep variable names, units, station metadata, and UTC
semantics compatible unless a task explicitly changes the output contract.
The writer encodes time as float64 epoch seconds with a proleptic Gregorian
calendar, floating data variables as float32, and drops object-valued data
variables, including the auxiliary `UTC` variable.

## Environment and Setup

[pyproject.toml](pyproject.toml) declares Python `>=3.11,<=3.12.9`, setuptools
`>=77`, and wheel. Runtime dependencies are GeoPandas, Matplotlib, NumPy,
netCDF4, pandas, requests, Shapely, and xarray. Development dependencies include
pytest, pytest-cov, Ruff, and pre-commit. See [environment.yml](environment.yml)
for the conda-forge environment and additional Jupyter tools.

## Development Commands and Validation

Run commands from the root in an already provisioned compatible environment.
These are repository-defined workflows; dependency availability and successful
execution must be verified separately.

| Command | Effect / caveat |
| --- | --- |
| `pip install -e ".[dev]"` | Editable installation used by CI; installs dependencies and writes installation metadata. Requires provisioning beyond a read-only analysis. |
| `make setup-dev-env` | Runs `scripts/setup-dev-env.sh`; requires conda, uses mamba if available, and generates `environment-dev.yml`. Deletes/recreates `isd-lite-data-dev`, replaces its user Jupyter kernel, installs the package and Git hooks. Crosses the isolation boundary. |
| `python -m ruff check .` | CI lint command, without automatic fixes; Ruff may write its cache. |
| `make test` / `pytest` | Runs the configured `tests/` suite; may create caches. |
| `make fmt` | Runs `python -m ruff format`, modifying files. |
| `make lint` | Runs `python -m ruff check --fix`, modifying files. |
| `make check` | Runs formatting and lint fixes; does not run tests. |
| `build-isd-lite-dataset --help` | Installed CLI usage; correct executable spelling comes from `[project.scripts]`. |
| `make upload-anaconda` | Builds and uploads through `scripts/upload2anaconda.sh`; uses external environments, temporary directories, credentials, and anaconda.org. |
| `make clean` | Removes caches, coverage output, `dist/`, and `build/`. |

CI installs the development extra, runs Ruff and pytest on Ubuntu with Python
3.11, and runs on pull requests and pushes to `main`. No standalone wheel/sdist
build target is defined in the Makefile. The README mentions `make type` and
`make upload-pypi`, but neither target exists; mypy configuration is commented
out. Do not assume those commands work.

The CI test suite is intentionally limited to `tests/test_test.py`, which asserts `hrrr_ml.test.add(2, 2) == 4`. Do not add further CI tests unless explicitly requested by the user. Passing this placeholder does not establish numerical, model, netCDF, or operational correctness. For behavioral changes, use relevant safe checks with small in-repository inputs and report what was actually verified. Avoid live network calls and expensive retraining as default validation. Keep temporary verification files inside the repository and report validation limits explicitly.

## Coding Conventions

- Follow [.editorconfig](.editorconfig): UTF-8, LF, final newline, four-space
  indentation; YAML/JSON/TOML use two spaces.
- Follow Ruff settings in `pyproject.toml`: target Python 3.11, line length 100,
  preserved quote style, and lint groups E/F/I/UP/B/SIM with E501 and SIM108
  ignored. Match nearby docstrings and typing rather than imposing a new style.
- Keep orchestration in the CLI, HTTP/file naming in `ncei.py`, and metadata
  and observation processing in `stations.py`. Existing code uses pathlib,
  pandas, GeoPandas, and xarray rather than custom data containers.
- Preserve packaged resource paths and shapefile companion files. Update
  setuptools package-data patterns and `MANIFEST.in` when changing asset layout.
  Keep region lists and geometry mappings consistent.
- Read the data-provider README files under `src/isd_lite_data/data/` before
  changing reference data. Retain provenance, attribution, and licensing notices;
  the bundled datasets have terms separate from [LICENSE](LICENSE).
  World Bank data documentation records local FRA/NOR country-code corrections.

## Configuration and External Interfaces

The application is configured through CLI arguments; no runtime environment
variable interface is defined in the Python sources. `--offline` requires the
local station catalog and all needed gzip files, skips HTTP operations, and
still writes station lists and NetCDF output.

`ncei.py` defines anonymous HTTPS endpoints under
`https://www.ncei.noaa.gov/pub/data/noaa/`: `isd-history.txt` and
`isd-lite/<year>/<USAF>-<WBAN>-<year>.gz`. Cached downloads still issue HEAD
requests and require an ETag; `refresh=True` forces downloading.

The publishing script requires `ANACONDA_API_TOKEN` and supports
`ANACONDA_USER_NAME`, `LICENSE_ID`, `PYPROJECT`, `IMPORT_NAME_OVERRIDE`,
`BUILD_ENV_PREFIX`, `BUILD_ENV_NAME`, and `TMPDIR` overrides. Consult the script
for defaults and behavior; do not inspect external credential stores or run
publishing as routine validation.

## Change and Review Workflow

- Read the relevant implementation and repository documentation first. Keep
  changes focused and preserve unrelated working-tree edits.
- For functional changes, add focused pytest coverage under `tests/` for the
  affected behavior. Use small synthetic inputs and mocked HTTP responses; keep
  validation independent of live NOAA availability.
- Validate with CI's Ruff and pytest commands when the environment is available
  and execution fits the boundary. For parsing, geometry, or serialization
  changes, verify identifiers, units, missing values, coordinates, and relevant
  round trips. Report unavailable dependencies and checks not performed.
- Review the diff for output/API compatibility, resource packaging, accidental
  generated data, and documentation drift. Update README examples when behavior
  changes. Report what changed, checks performed, and unresolved limitations.
- **Maintain `AGENTS.md`:** When making changes that affect the project's
  architecture, interfaces, dependencies, workflows, conventions, or documented
  constraints, update the relevant sections of `AGENTS.md` in the same change.
  Keep revisions focused and based on verified repository contents. Do not
  modify `AGENTS.md` for changes that do not affect its accuracy or usefulness.

## Known Limitations and Verification Gaps

- The current test suite verifies only `add(2, 2) == 4`; a passing suite does not
  validate downloading, geography, parsing, CLI behavior, or NetCDF round trips.
- README promises relative humidity, but `Stations.var_names` contains only
  `T, TD, SLP, WD, WS, SKY, PREC1H, PREC6H`; no humidity calculation is present.
  Confirm the intended feature before documenting or implementing it.
- The documented full-hourly series currently uses the union of observed
  timestamps; it does not generate a complete hourly index. Confirm gap-filling
  requirements before changing this behavior.
- Download HEAD and GET loops each allow 1,200 attempts with three-second retry
  delays. Threaded worker exceptions are printed rather than propagated, and
  directory-listing failures can be skipped. Verify actual file availability;
  downloader completion alone does not establish success.
- NetCDF output overwrites an existing target. Large station/year selections
  materialize arrays in memory; use small fixtures for validation.
- No dependency lockfile or end-to-end fixture set is present. CI covers only
  Python 3.11; compatibility across the full declared Python range, geospatial
  dependency versions, external endpoint behavior, and release execution require
  separate verification within the isolation policy.
