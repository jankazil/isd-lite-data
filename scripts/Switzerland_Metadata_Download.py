#!/usr/bin/env python

'''
This script downloads IDS-Lite station metadata and filters it to produce a list of
stations located in Switzerland that have observations  available within a
user-specified date range.

The script performs the following steps:
1. Parses command-line arguments for start and end dates, as well as a target
   directory for saving results.
2. Downloads metadata for all available stations from the IDS-Lite data source.
3. Filters the stations by:
   - Country (only Swiss stations are retained).
   - Geographic coordinates (restricted to Switzerland).
   - Availability of observations within the specified date range.
4. Saves the filtered station metadata to a text file in the specified directory.

Usage:
    python Switzerland_Metadata_Download.py <start_year> <start_month> <start_day> <end_year> <end_month> <end_day> <data_dir>

Command line arguments:
    start_year (int)   Start year of the time range.
    start_month (int)  Start month of the time range.
    start_day (int)    Start day of the time range.
    end_year (int)     End year of the time range.
    end_month (int)    End month of the time range.
    end_day (int)      End day of the time range.
    data_dir (str)     Directory path where the station metadata file will be saved.
                       The directory will be created if it does not exist.

Output:
    Downloaded IDS-Lite station metadata file for stations in the requested
    region, in the requested time range, for which observations are available
    for download.

Example:
    python Switzerland_Metadata_Download.py 2020 1 1 2020 12 31 ./output_data
'''

import argparse
import sys
from datetime import datetime
from pathlib import Path

from isd_lite_data import stations


def arg_parse(argv=None):
    '''
    Argument parser which returns the parsed values given as arguments.
    '''

    code_description = (
        'Download IDS-Lite station metadata and filter it to obtain stations'
        'in Switzerland with available observations within a specified time'
        'period. The script requires start and end dates as input, as well'
        'as a target directory for storing the resulting metadata. The'
        'metadata is filtered by country, geographic coordinates, period of'
        'interest, and data availability, and then saved to a text file in'
        'the specified directory.'
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
        help='Directory path into which the data will be downloaded. Will be created if it does not exist.',
    )

    # Optional arguments
    # parser.add_argument('-x','--xxx', type=str, help='HELP STRING HERE')

    args = parser.parse_args()

    start_date = datetime(year=args.start_year, month=args.start_month, day=args.start_day)
    end_date = datetime(year=args.end_year, month=args.end_month, day=args.end_day)
    data_dir = Path(args.data_dir)

    return (start_date, end_date, data_dir)


(start_date, end_date, data_dir) = arg_parse(sys.argv[1:])

#
# Get metadata for all stations
#

all_stations = stations.Stations.from_url()

#
# Filter by country and coordinates
#

switzerland_and_china_stations = all_stations.filter_by_country(['CH'])

min_lat = 40
max_lat = 50
min_lon = 0
max_lon = 15

switzerland_stations = switzerland_and_china_stations.filter_by_coordinates(
    min_lat, max_lat, min_lon, max_lon
)

#
# Filter by period of interest
#

switzerland_stations = switzerland_stations.filter_by_period(start_date, end_date)

#
# Filter the stations by whether observations are available for download for the given period
#

switzerland_stations = switzerland_stations.filter_by_data_availability(
    start_date, end_date, verbose=True
)

#
# Save the stations metadata
#

file_name = 'switzerland_stations.' + str(start_date.year) + '-' + str(end_date.year) + '.txt'

file_path = data_dir / file_name

switzerland_stations.save(
    'Stations in Switzerland with observations '
    + str(start_date.year)
    + ' - '
    + str(end_date.year),
    file_path,
)

print()
print('Saved station metadata as', file_path)
print()
