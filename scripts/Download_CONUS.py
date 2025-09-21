#!/usr/bin/env python

'''
Download_CONUS.py

This script automates the retrieval of Integrated Surface Dataset Lite (ISD-Lite) 
observations for the contiguous United States from NOAA's NCEI servers.

Workflow:
---------
1. Parses user-provided command-line arguments:
   - Start and end years (inclusive) defining the period of interest.
   - Path to a local data directory to download the data to
   - Optional number of parallel download processes for faster execution.

2. Loads the full station metadata from NCEI, then applies successive filters:
   - Country filter: restricts stations to the US (ISO country code 'US').
   - Coordinate filter: further restricts stations to latitudes 24 to 50°N and 
     longitudes 125 to 65°W.
   - Period filter: selects stations that have observations within the requested 
     date range.
   - Availability filter: retains only those stations with actual downloadable 
     data for the given period (with verbose output).

3. Saves the resulting station list to a text file named 
   `conus_stations.<start_year>-<end_year>.txt` in the data directory.

4. Downloads ISD-Lite observation files for the selected stations and years,
   optionally in parallel if the `-n`/`--n` argument is provided.

Usage:
------
    python Download_CONUS.py <start_year> <end_year> <data_dir> [-n <parallel_jobs>]

Example:
--------
    python Download_CONUS.py 2020 2020 ./data -n 4

This will retrieve all available contiguous US (CONUS) station data for the year 2020
and save both metadata and downloaded observations under `./data`.

'''

import argparse
import sys
from datetime import datetime
from pathlib import Path

from isd_lite_data import ncei, stations


def arg_parse(argv=None):
    '''
    Argument parser which returns the parsed values given as arguments.
    '''

    code_description = (
    'Download ISD-Lite weather observation data for contiguous US (CONUS) stations from NOAA NCEI '
    'for a specified range of years. The script filters stations by country, '
    'coordinates, period of interest, and data availability, saves the resulting '
    'station list, and downloads the observations into the specified data directory.'
    )
    
    parser = argparse.ArgumentParser(description=code_description)

    # Mandatory arguments
    parser.add_argument('start_year', type=int, help='Start year of time range.')
    parser.add_argument('end_year', type=int, help='End year of time range (inclusive).')
    parser.add_argument(
        'data_dir',
        type=str,
        help='Directory path into which the data will be downloaded.',
    )

    # Optional arguments
    parser.add_argument(
        '-n',
        '--n',
        type=int,
        help='Number of parallel download processes. n > 1 accelerates downloads significantly, but can result in network errors or in the server refusing to cooperate.',
    )

    args = parser.parse_args()

    start_date = datetime(year=args.start_year, month=1, day=1)
    end_date = datetime(year=args.end_year, month=12, day=31)
    data_dir = Path(args.data_dir)
    n_jobs = args.n

    return (start_date, end_date, data_dir, n_jobs)

(start_date, end_date, data_dir, n_jobs) = arg_parse(sys.argv[1:])

#
# Get metadata for all stations
#

all_stations = stations.Stations.from_url()

# Filter by country and coordinates

conus_stations = all_stations.filter_by_country(['US'])

conus_stations = conus_stations.filter_by_coordinates(24, 50, -125, -65)

# Filter the stations by whether observations are available for download for the given period

conus_stations = conus_stations.filter_by_data_availability(start_date, end_date, verbose=True)

# Save the stations metadata

file_name = 'conus_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.txt'

file_path = data_dir / file_name

conus_stations.save('Stations in the contiguous US with observations '+ str(start_date.year)+ ' - '+ str(end_date.year),file_path)

# Load the stations metadata file

conus_stations = stations.Stations.from_file(file_path)

#
# Download observations for these stations
#

local_files = ncei.download_many(start_date.year,end_date.year,conus_stations.id(),data_dir,n_jobs=n_jobs,refresh=False,verbose=True)
