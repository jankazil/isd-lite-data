"""
Classes to hold ISDLit data and perform operations on them.
"""

from typing import Self
from pathlib import Path
import pandas as pd
from isd_lite_data import ncei

class Stations():
    
    """
    
    Holds
    
      - ISD station metadata
      - a list of countries where stations are located
      - a list of US states where stations are located
    
    The ISD station metadata data is initialized from file, if the ISD
    station file exists locally. Otherwise, the file will be downloaded.
    
    """
    
    # Type hints for instance variables (good practice to hint all instance variables
    # in one place, as otherwise, clarity and re-usability goes down the drain)
    
    countries: list
    us_states: list
    
    station_metadata: pd.DataFrame
    
    def __init__(self,station_metadata: pd.DataFrame):
        
        """
        
        Default constructor.
        
        Args:
            station_metadata (pd.DataFrame): A Pandas dataframe with station metadata
        
        """
        
        self.station_metadata = station_metadata.copy()
        
        # Create lists of countries and US states
        
        self.countries = sorted(list(self.station_metadata['CTRY'].dropna().unique()))
        self.us_states = sorted(list(self.station_metadata['ST'].dropna().unique()))
        
        return
    
    @classmethod
    def from_file(cls,data_dir: Path) -> Self:
        
        """
        
        Alternative constructor, initializes the ISD station metadata from an "ISD history file",
        called isd-history.txt, if the file exists. If it does not, it is downloaded first.
        
        Args:
            data_dir (Path): Path to directory with the ISD history file "isd-history.txt"
        
        Returns:
            Stations: An instance of Stations initialized with data from the file.
        
        """
        
        # Download the station database if not present locally
        
        stations_file = ncei.download_stations(data_dir)
        
        # Find the header length
        
        with open(stations_file) as f:
            for i, line in enumerate(f):
                if line.startswith('USAF   WBAN  STATION NAME'):
                    header_n = i
                    break
        
        # Read station database from file into a Pandas dataframe
        
        tmp_data = pd.read_fwf(stations_file,skiprows=header_n,header=0, dtype={"USAF": str, "WBAN": str})
        
        # Drop any lines with
        # - missing LAT or LON data
        # - LAT and LON are both zero
        
        station_metadata = tmp_data.dropna(subset=["LAT", "LON"]).query("not (LON == 0 and LAT == 0)")
        
        # Reset row index
        
        station_metadata = station_metadata.reset_index(drop=True)
        
        return cls(station_metadata)
    
    def print_countries(self):
        
        """
        Print the two-letter country codes of the ISD stations
        """
        
        print()
        print('Two-letter country codes of the ISD stations')
        print()
        
        for ii in range(0,len(self.countries),10):
            print(' '.join(self.countries[ii:ii+10]))
        
        return
        
    def print_us_states(self):
        
        """
        Print two-letter US states codes of the ISDLite stations
        """
        
        print()
        print('Two-letter US states codes of the ISD stations')
        print()
        
        for ii in range(0,len(self.us_states),10):
            print(' '.join(self.us_states[ii:ii+10]))
        
        return
    
    def filter_by_country(self,countries: list[str]) -> Self:
        
        """
        
        Filters the station metadata by country.
        
        Args:
            countries (list[str]): A list of two-letter country codes of ISD stations
        
        Returns:
            Stations: An instance of Stations holding the ISD station metadata for the given list of countries.
        
        """
        
        # Filter by country
        
        station_metadata = self.station_metadata[self.station_metadata["CTRY"].isin(countries)]
        
        # Reset row index
        
        station_metadata = station_metadata.reset_index(drop=True)
        
        return Stations(station_metadata)

    def filter_by_us_state(self,us_states: list[str]) -> Self:
        
        """
        
        Filters the station metadata by US state.
        
        Args:
            us_states (list[str]): A list of two-letter US state codes of ISD stations
        
        Returns:
            Stations: An instance of Stations holding the ISD station metadata for the given list of US states.
        
        """
        
        # Filter by country
        
        station_metadata = self.station_metadata[self.station_metadata["ST"].isin(us_states)]
        
        # Reset row index
        
        station_metadata = station_metadata.reset_index(drop=True)
        
        return Stations(station_metadata)

    def filter_by_coordinates(self,min_lat: float,max_lat: float,min_lon: float,max_lon: float) -> Self:
        
        """
        
        Filters the station metadata by latitude and longitude.
        
        
        Args:
            min_lat (float): Minimum latitude bounding the latitude and longitude box, [-90,90]
            max_lat (float): Maximum latitude bounding the latitude and longitude box, [-90,90]
            min_lon (float): Minimum longitude bounding the latitude and longitude box, [-180,180]
            max_lon (float): Maximum longitude bounding the latitude and longitude box, [-180,180]
        
        Returns:
            Stations: An instance of Stations holding the ISD station metadata for the stations
                      within the given latitude/longitude box (inclusive).
        
        """
        
        # Filter by latitude and longitude
        
        station_metadata = self.station_metadata[ \
                          (self.station_metadata['LAT'] >= min_lat) & \
                          (self.station_metadata['LAT'] <= max_lat) & \
                          (self.station_metadata['LON'] >= min_lon) & \
                          (self.station_metadata['LON'] <= max_lon)   \
                          ]
        
        # Reset row index
        
        station_metadata = station_metadata.reset_index(drop=True)
        
        return Stations(station_metadata)

