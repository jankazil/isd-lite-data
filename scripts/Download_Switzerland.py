#!/usr/bin/env python

'''
Download_Switzerland.py

This script automates the retrieval of Integrated Surface Dataset Lite (ISD-Lite) 
observations for Switzerland from NOAA's NCEI servers.

Workflow:
---------
1. Parses user-provided command-line arguments:
   - Start and end years (inclusive) defining the period of interest.
   - Path to a local data directory to download the data to
   - Optional number of parallel download processes for faster execution.

2. Loads the full station metadata from NCEI, then applies successive filters:
   - Country filter: restricts stations to Switzerland (ISO country code 'CH').
   - Coordinate filter: further restricts stations to latitudes 40–50°N and 
     longitudes 0–15°E.
   - Period filter: selects stations that have observations within the requested 
     date range.
   - Availability filter: retains only those stations with actual downloadable 
     data for the given period (with verbose output).

3. Saves the resulting station list to a text file named 
   `switzerland_stations.<start_year>-<end_year>.txt` in the data directory.

4. Downloads ISD-Lite observation files for the selected stations and years,
   optionally in parallel if the `-n`/`--n` argument is provided.

Usage:
------
    python Download_Switzerland.py <start_year> <end_year> <data_dir> [-n <parallel_jobs>]

Example:
--------
    python Download_Switzerland.py 2020 2020 ./data -n 4

This will retrieve all available Swiss station data for the year 2020
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
    'Download ISD-Lite weather observation data for Swiss stations from NOAA NCEI '
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

switzerland_and_china_stations = all_stations.filter_by_country(['CH'])

switzerland_stations = switzerland_and_china_stations.filter_by_coordinates(40, 50, 0, 15)

# Filter the stations by whether observations are available for download for the given period

switzerland_stations = switzerland_stations.filter_by_data_availability(start_date, end_date, verbose=True)

# Save the stations metadata

file_name = 'switzerland_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.txt'

file_path = data_dir / file_name

switzerland_stations.save('Stations in Switzerland with observations '+ str(start_date.year)+ ' - '+ str(end_date.year),file_path)

# Load the stations metadata file

switzerland_stations = stations.Stations.from_file(file_path)

#
# Download observations for these stations
#

local_files = ncei.download_many(start_date.year,end_date.year,switzerland_stations.id(),data_dir,n_jobs=n_jobs,refresh=False,verbose=True)
