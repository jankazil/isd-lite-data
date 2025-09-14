#!/usr/bin/env python

'''
This script downloads IDS-Lite station observations for all stations listed in a
previously generated station metadata file. It is designed to work with station
files produced by CONUS_Metadata_Download.py.

The script performs the following steps:
1. Parses command-line arguments for start and end dates, as well as a directory
   that must contain the station metadata file.
2. Constructs the expected metadata file name based on the date range and verifies
   that the file exists.
3. Loads the list of stations from the metadata file.
4. Downloads all available observations for these stations within the specified
   date range.
5. Optionally uses multiple parallel download processes if the user specifies the
   `-n` argument, which can accelerate downloads but may increase the chance of
   network errors or rate limiting by the server.

Usage:
    python CONUS_Metadata_Download.py <start_year> <start_month> <start_day> <end_year> <end_month> <end_day> <data_dir>

Command line arguments:
    start_year (int): Start year of the time range.
    start_month (int): Start month of the time range.
    start_day (int): Start day of the time range.
    end_year (int): End year of the time range.
    end_month (int): End month of the time range.
    end_day (int): End day of the time range.
    data_dir (str): Directory containing the station metadata file. Downloaded
                    observations will be stored here.
    -n, --n (int, optional): Number of parallel download processes. Defaults to 1
                             if not provided.

Output:
    Downloaded IDS-Lite observation files for all stations in the metadata file,
    saved in the specified data directory.

Example:
    python CONUS_Observations_Download.py 2020 1 1 2020 12 31 ./output_data -n 4
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
        'Download IDSLite observations for all stations listed in a previously '
        'generated station metadata file. The script requires start and end dates '
        'defining the period of interest, as well as the directory containing the '
        'station metadata file (created using CONUS_Metadata_Download.py). It loads '
        'the station list, verifies that the file exists, and downloads all '
        'available observations for those stations during the specified date range. '
        'An optional argument allows for parallel downloads to speed up data retrieval.'
    )

    parser = argparse.ArgumentParser(description=code_description)

    # Mandatory arguments
    parser.add_argument('start_year', type=int, help='Start year of time range.')
    parser.add_argument('start_month', type=int, help='Start month of time range.')
    parser.add_argument('start_day', type=int, help='Start day of time range.')
    parser.add_argument('end_year', type=int, help='End year of time range.')
    parser.add_argument('end_month', type=int, help='End month of time range.')
    parser.add_argument('end_day', type=int, help='End day of time range.')
    parser.add_argument(
        'data_dir',
        type=str,
        help='Directory path into which the data will be downloaded. Must contain station metadata file downloaded previously with CONUS_Metadata_Download.py.',
    )

    # Optional arguments
    parser.add_argument(
        '-n',
        '--n',
        type=int,
        help='Number of parallel download processes. n > 1 accelerates downloads significantly, but can result in network errors or in the server refusing to cooperate.',
    )

    args = parser.parse_args()

    start_date = datetime(year=args.start_year, month=args.start_month, day=args.start_day)
    end_date = datetime(year=args.end_year, month=args.end_month, day=args.end_day)
    data_dir = Path(args.data_dir)
    n_jobs = args.n

    return (start_date, end_date, data_dir, n_jobs)


(start_date, end_date, data_dir, n_jobs) = arg_parse(sys.argv[1:])

#
# Load stations in the contiguous US from the metadata file
#

file_name = 'conus_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.txt'

file_path = data_dir / file_name

assert file_path.is_file(), f'File does not exist: {file_path}.'

conus_stations = stations.Stations.from_file(file_path)

#
# Download observations for these stations
#

local_files = ncei.download_many(
    start_date.year,
    end_date.year,
    conus_stations.id(),
    data_dir,
    n_jobs=n_jobs,
    refresh=False,
    verbose=True,
)
