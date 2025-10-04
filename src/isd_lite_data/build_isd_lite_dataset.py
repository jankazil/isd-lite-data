#!/usr/bin/env python

'''
Builds an Integrated Surface Dataset Lite (ISD-Lite) station metadata file and an hourly UTC
time-series dataset for a selected country, U.S. state or territory, the contiguous United
States (CONUS), an RTO/ISO region, or an individual station. Downloads ISD-Lite observations
from NOAA NCEI for an inclusive range of years and writes a NetCDF file with a full-hourly
UTC time series.

Workflow:

1) Load the requested region geometry (RTO/ISO polygons or U.S. state boundaries) if a region is specified.
   If an individual station is specified, geometry loading is skipped.
2) Retrieve the ISD-Lite station catalog and either spatially filter stations to the region or select
   the specified station by USAF and WBAN identifiers (as listed in isd-history.txt).
3) Filter the resulting stations by data availability over [start_year, end_year].
4) Save a text file with the resulting station list to the data directory (for both region and single-station modes).
5) Download ISD-Lite observation files for the filtered station IDs. Files already present in the download
   directory and unchanged on the NOAA NCEI server are not re-downloaded.
   - Parallel downloads can be enabled with -n.
   - The --offline flag disables network access and expects all required files to be present locally.
6) Load observations and save the full-hourly UTC time series as a NetCDF file.

Inputs are provided via command-line arguments. Outputs include:
- A text file listing the stations used.
- The downloaded ISD-Lite data files (unless offline mode is selected).
- A NetCDF file containing the full-hourly UTC time series.

Assumptions:
- Network access to NOAA NCEI is available unless --offline is specified.

Example usage:

    build-isdlite-dataset 2022 2022 CHE /path/to/data -n 32
    build-isdlite-dataset 2021 2021 CO /path/to/data --offline
    build-isdlite-dataset 2022 2022 ERCOT /path/to/data -n 32
    build-isdlite-dataset 2020 2025 "725650 03017" /path/to/data
'''

import argparse
import os
import sys
from datetime import datetime
from importlib.resources import as_file, files
from pathlib import Path

import geopandas as gpd

from isd_lite_data import ncei, region_codes, rto_iso, stations


def arg_parse(argv=None):
    '''

    Command line argument parser.

    Parses command-line arguments and returns normalized values used by the script.

    Parameters
    ----------
    argv : list[str] or None
        Sequence of argument tokens to parse (excluding the program name). If None,
        arguments are taken from sys.argv[1:].

    Returns
    -------
    tuple[datetime, datetime, str, Path, Path | None, int | None, bool]
        start_date : datetime
            Inclusive start date constructed from `start_year` (January 1).

        end_date : datetime
            Inclusive end date constructed from `end_year` (December 31).

        region_code : str
            Region selector. One of
            - a three-letter ISO 3166-1 alpha-3 country code (e.g., 'CHE', 'USA')
            - a two-letter U.S. state or territory code (e.g., 'CA', 'PR')
            - the special region 'CONUS'
            - an RTO/ISO region code {'ERCOT','CAISO','ISONE','NYISO','MISO','SPP','PJM'}
            - or a double-quoted string containting the USAF station ID and
              and the WBAN station ID separated by a space, as in the ISD Lite stations file

                https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt

              e.g., "724670 03017"

        data_dir : Path
            Destination directory into which the station list, downloaded ISD-Lite files,
            and outputs will be written.

        n_jobs : int | None
            Maximum number of parallel download workers. If None, downloads run
            single-threaded.

        offline : bool
            If True, work offline and expect all required inputs to be present in data_dir.

    Raises
    ------
    SystemExit
        If the provided arguments fail validation performed by argparse.
    '''

    code_description = (
        "Download NOAA NCEI Integrated Surface Dataset Lite (ISD-Lite) observations for stations located "
        "within a selected country, U.S. state or territory, the contiguous United States (CONUS), an RTO/ISO region, "
        "or for an individual station by USAF and WBAN identifiers (as listed in isd-history.txt), over an inclusive range of years. "
        "The script filters stations spatially and by data availability (for regions) or selects the given station, "
        "saves the station list, downloads observations, and writes the full-hourly UTC time series to a NetCDF file.\n\n"
        "Valid country, region or station arguments:\n\n"
        f"  - Countries: {', '.join(region_codes.countries)}\n\n"
        f"  - US states/territories: {', '.join(region_codes.us_states_territories)}\n\n"
        f"  - Special region: {region_codes.conus}\n\n"
        f"  - RTO/ISO regions: {', '.join(region_codes.rto_iso_regions)}\n\n"
        "  - Individual station: provide a quoted USAF and WBAN pair, e.g. \"724670 03017\".\n\n"
        "Parallel downloads can be enabled with -n.\n\n"
        "ISD-Lite observation files already present in the download directory and unchanged on the NOAA NCEI server are not re-downloaded."
    )

    parser = argparse.ArgumentParser(
        description=code_description, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Mandatory arguments

    parser.add_argument('start_year', type=int, help='Start year of time range.')

    parser.add_argument('end_year', type=int, help='End year of time range (inclusive).')

    parser.add_argument(
        'region_code',
        type=str,
        help=(
            "Region or station selector. Use a three-letter country code, two-letter "
            "U.S. state or territory code, 'CONUS', or one of the RTO/ISO region codes."
        ),
    )

    parser.add_argument(
        'data_dir', type=str, help='Directory path into which the data will be downloaded.'
    )

    # Optional arguments

    parser.add_argument(
        '-n',
        '--n',
        type=int,
        help=(
            'Number of parallel download processes. n > 1 accelerates downloads significantly, '
            'but can result in network errors or in the server refusing to cooperate.'
        ),
    )

    parser.add_argument(
        '-o',
        '--offline',
        action='store_true',
        help=(
            'Work offline. All required files must have been downloaded to data_dir in a previous call without this flag.'
        ),
    )

    args = parser.parse_args(argv)

    start_date = datetime(year=args.start_year, month=1, day=1)
    end_date = datetime(year=args.end_year, month=12, day=31)
    region_code = args.region_code
    data_dir = Path(args.data_dir)

    n_jobs = args.n

    offline = args.offline

    return (start_date, end_date, region_code, data_dir, n_jobs, offline)


def main(argv=None):
    '''
    CLI entry point.
    '''

    (start_date, end_date, region_code, data_dir, n_jobs, offline) = arg_parse(
        argv if argv is not None else sys.argv[1:]
    )

    if offline:
        print(
            'Working offline. All required files must have been downloaded to '
            + str(data_dir)
            + ' in a previous call.'
        )
    else:
        print(
            'Working online. Will download files to '
            + str(data_dir)
            + ' unless they are already present and identical with their version on the NCEI server.'
        )

    # Create data directory unless it exists
    data_dir.mkdir(parents=True, exist_ok=True)

    #
    # Identify stations in the selected country, US state, territory, region, or handle an individual station
    #

    if len(region_code) == 3:
        assert region_code in region_codes.countries, (
            'Country code ' + region_code + ' is not available.'
        )
        working_on_region = True
        # Load countries shapefile directory from installed distribution using importlib.resources
        world_dir_res = files('isd_lite_data') / 'data' / 'WorldBank' / 'WB_countries_Admin0_10m'
        with as_file(world_dir_res) as world_dir_path:
            world_shp_file = world_dir_path / 'WB_countries_Admin0_10m.shp'
            countries_gdf = gpd.read_file(world_shp_file)
        region_gdf = countries_gdf[countries_gdf['ISO_A3'].isin([region_code])]
        region_name = region_gdf['NAME_EN'].values[0]

    elif len(region_code) == 2:
        assert region_code in region_codes.us_states_territories, (
            'US state/territory code ' + region_code + ' is not available.'
        )
        working_on_region = True
        # Load US states shapefile directory from installed distribution using importlib.resources
        us_states_dir_res = files('isd_lite_data') / 'data' / 'CensusBureau' / 'US_states'
        with as_file(us_states_dir_res) as us_states_dir_path:
            us_states_shp_file = us_states_dir_path / 'tl_2024_us_state.shp'
            us_gdf = gpd.read_file(us_states_shp_file)
        region_gdf = us_gdf[us_gdf['STUSPS'].isin([region_code])]
        region_name = region_code

    elif region_code in region_codes.rto_iso_regions:
        working_on_region = True
        # Load RTO/ISO region GeoJSON from installed distribution using importlib.resources
        rto_iso_geojson_res = files('isd_lite_data') / 'data' / 'EIA' / 'RTO_ISO_regions.geojson'
        with as_file(rto_iso_geojson_res) as rto_iso_geojson_path:
            region_gdf = rto_iso.region(rto_iso_geojson_path, region_code)
        region_name = region_code

    elif region_code == region_codes.conus:
        working_on_region = True
        # Load US states shapefile directory from installed distribution using importlib.resources
        us_states_dir_res = files('isd_lite_data') / 'data' / 'CensusBureau' / 'US_states'
        with as_file(us_states_dir_res) as us_states_dir_path:
            us_states_shp_file = us_states_dir_path / 'tl_2024_us_state.shp'
            us_gdf = gpd.read_file(us_states_shp_file)
        exclude_codes = ['AK', 'HI', 'PR', 'GU', 'VI', 'AS', 'MP']
        region_gdf = us_gdf[~us_gdf['STUSPS'].isin(exclude_codes)]
        region_name = region_code

    else:
        working_on_region = False
        usaf_id, wban_id = region_code.split(' ')
        region_name = region_code

    # File with list of all stations and metadata

    all_stations_file = data_dir / Path(os.path.basename(ncei.isd_lite_stations_url))

    if offline:
        # Load the file from disk - it must have been downloaded previously to the data directory
        all_stations = stations.Stations.from_file(all_stations_file)
    else:
        # Load the file from the NCEI server and save it for later use
        all_stations = stations.Stations.from_url()
        all_stations.save_station_list('ISD Lite stations', all_stations_file)

    # Determine if we are working on a region or an individual station

    if working_on_region:
        region_stations = all_stations.filter_by_region(region_gdf)
    else:
        region_stations = all_stations.filter_by_id(usaf_id, wban_id)

    # Filter by data availability

    if offline:
        region_stations = region_stations.filter_by_data_availability_offline(
            data_dir, start_date, end_date, verbose=True
        )
    else:
        region_stations = region_stations.filter_by_data_availability_online(
            start_date, end_date, verbose=True
        )

    # Save the metadata file for these stations

    region_stations_file = data_dir / Path(
        region_code + '.' + str(start_date.year) + '-' + str(end_date.year) + '.txt'
    )

    region_stations.save_station_list('Region/station: ' + region_name, region_stations_file)

    # Download ISD-Lite station data

    if not offline:
        _ = ncei.download_many(
            start_date.year,
            end_date.year,
            region_stations.ids(),
            data_dir,
            n_jobs=n_jobs,
            refresh=False,
            verbose=True,
        )

    # Load the full-hourly UTC time series from the ISD-Lite station data

    region_stations.load_observations(data_dir, start_date.year, end_date.year, verbose=True)

    region_stations.observations.attrs['region'] = region_name

    # Save full-hourly UTC time series as a netCDF file

    isd_lite_netcdf_file = data_dir / Path(
        region_code + '.' + str(start_date.year) + '-' + str(end_date.year) + '.nc'
    )

    region_stations.write_observations2netcdf(isd_lite_netcdf_file)


if __name__ == '__main__':
    main()
