"""
Classes to hold ISDLite data and perform operations on them.
"""

from typing import Self
from pathlib import Path
from datetime import datetime
import tempfile

import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed

from isd_lite_data import ncei

class Stations():
    
    """
    
    Holds
    
      - ISD station metadata
      - a list of countries where stations are located
      - a list of US states where stations are located
    
    The ISD station metadata data is initialized from a file or from the
    Integrated Surface Database (ISD) Station History file, available online.
    
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
    def from_file(cls,file_path: Path) -> Self:
        
        """
        
        Alternative constructor, initializes the ISD station metadata from a file, which
        must have the same structure as the Integrated Surface Database (ISD) Station History
        file, available online.
        
        Args:
            file_path (Path): Path to file with station metadata
        
        Returns:
            Stations: An instance of Stations initialized with data from the file.
        
        """
        
        # Define column widths and names
        widths = [6,6,30,3,5,5,9,9,8,9,9]
        column_names = ['USAF', 'WBAN', 'STATION NAME', 'CTRY', 'ST', 'CALL','LAT', 'LON', 'ELEV(M)', 'BEGIN', 'END']
        
        # Read the data rows from the file
        station_metadata = pd.read_fwf(file_path,widths=widths,skiprows=22,names=column_names,dtype=str)
        
        # Strip whitespace
        station_metadata = station_metadata.map(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Convert LAT, LON, ELEV(M) to float if possible; empty strings become NaN
        for col in ['LAT', 'LON', 'ELEV(M)']:
            station_metadata[col] = pd.to_numeric(station_metadata[col], errors='coerce')
        
        # Drop rows where both LAT and LON are NaN
        station_metadata = station_metadata.dropna(subset=['LAT', 'LON'], how='all')
        
        # Drop rows where the station name contains 'bogus'
        station_metadata = station_metadata[~station_metadata['STATION NAME'].str.contains('bogus', case=False, na=False)]
        
        # Convert the 'BEGIN' and 'END' columns from date strings to datetime objects
        for col in ['BEGIN', 'END']:
            station_metadata[col] = pd.to_datetime(station_metadata[col], format="%Y%m%d", errors='coerce')
        
        # Reset index
        station_metadata = station_metadata.reset_index(drop=True)
        
        return cls(station_metadata)
    
    @classmethod
    def from_url(cls) -> Self:
        
        """
        
        Alternative constructor, initializes the ISD station metadata from the
        Integrated Surface Database (ISD) Station History file, available online.
        
        Args:
            
        
        Returns:
            Stations: An instance of Stations initialized with data obtained online.
        
        """
        
        # Download the station database file
        
        tmpfile = Path(tempfile.NamedTemporaryFile(delete=False).name)
        
        ncei.download_stations(tmpfile)
        
        stations = cls.from_file(tmpfile)
        
        tmpfile.unlink()
        
        return stations
    
    def save(self,title_line: str, file_path: Path) -> Self:
        
        """
        
        Saves ISD station metadata to a file which has the same structure as the
        Integrated Surface Database (ISD) Station History file, available online.
        
        Args:
            title_line (str): First line in the file, can be used to describe specifics
                              of the stations in the file, e.g., "Stations in Texas between 2002-2012 for which data are available for download"
            file_path (Path): Path to file with station metadata
        
        Returns:
        
        """
        
        # Header
        
        header = """
 USAF = Air Force station ID. May contain a letter in the first position.
 WBAN = NCDC WBAN number
 CTRY = FIPS country ID
   ST = State for US stations
 ICAO = ICAO ID
  LAT = Latitude in thousandths of decimal degrees
  LON = Longitude in thousandths of decimal degrees
 ELEV = Elevation in meters
BEGIN = Beginning Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.
  END = Ending Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.

Notes:
- Missing station name, etc indicate the metadata are not currently available.
- The term "bogus" indicates that the station name, etc are not available.
- For a small % of the station entries in this list, climatic data are not 
  available. To determine data availability for each location, see the 
  'isd-inventory.txt' or 'isd-inventory.csv' file. 
"""

        columns_title = 'USAF   WBAN  STATION NAME                  CTRY ST CALL  LAT     LON      ELEV(M) BEGIN    END'
        
        with open(file_path, 'w') as f:
            f.write(title_line + '\n')
            f.write('\n')
            f.write(header.lstrip('\n'))
            f.write('\n')
            f.write(columns_title + '\n')
            f.write('\n')
            for _, row in self.station_metadata.iterrows():
                row_strs = [
                f"{str(row['USAF']):<6}",
                f"{str(row['WBAN']):>6}",
                f"{' ' + str(row['STATION NAME']):<30}",
                f"{str(row['CTRY']):>3}",
                f"{str(row['ST']):>5}",
                f"{str(row['CALL']):>5}",
                f"{float(row['LAT']):+9.3f}",
                f"{float(row['LON']):+9.3f}",
                f"{float(row['ELEV(M)']):+8.1f}",
                f"{row['BEGIN'].strftime('%Y%m%d'):>9}",
                f"{row['END'].strftime('%Y%m%d'):>9}"
                ]
                f.write("".join(row_strs) + '\n')
        
        return
    
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
    
    def filter_by_period(self,start_time: datetime,end_time: datetime) -> Self:
        
        """
        
        Filters the station metadata by data period.
        
        
        Args:
            start_time (datetime): Start time of period for which observations are nominally available
            end_time (datetime): End time of period for which station observations are nominally available
        
        Returns:
            Stations: An instance of Stations holding the ISD station metadata for the stations
                      whose data period fully covers the period given by the start and end time.
        
        """
        
        # Filter by time period
        
        station_metadata = self.station_metadata[
                          (self.station_metadata['BEGIN'] <= start_time) &
                          (self.station_metadata['END'] >= end_time)
                          ]
        
        # Reset row index
        
        station_metadata = station_metadata.reset_index(drop=True)
        
        return Stations(station_metadata)
    
    def filter_by_data_availability(self,start_time: datetime,end_time: datetime,n_jobs: int = 8,verbose: bool = False) -> Self:
        
        """
        
        Filters the station metadata by whether files with observations are available for download
        for the given period. This filter is needed because some files with observations are not available
        for download even though the station metadata may indicate that data exist for the given period.
        
        This can take a while, depending on the responsiveness of the NCEI server.
        
        Args:
            start_time (datetime): Start time of period for which files with observations must are available
            end_time (datetime): End time of period for which files with observations must are available
            n_jobs (int): Maximum number of parallel URL requests
            verbose (bool): If True, print information. Defaults to False.
        
        Returns:
            Stations: An instance of Stations holding the ISD station metadata for the stations
                      for which files with observations are available for download for the period given
                      by the start and end time.
        
        """
        
        #
        # Construct a new dataframe
        #
        
        if verbose:
            print()
            print('Filtering out stations for which not all files with observations are available in the period of interest',str(start_time),'to',str(end_time))
            print()
        
        
        filtered_rows = []
        
        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            
            future_to_row = {
                executor.submit(ncei.check_station, start_time.year, end_time.year, row['USAF'], row['WBAN']): row
                for _, row in self.station_metadata.iterrows()
            }
            
            for future in as_completed(future_to_row):
                row = future_to_row[future]
                observations_files_available = future.result()
        
                if observations_files_available:
                    filtered_rows.append(row)
                    if verbose:
                        print('Including station', row['USAF'], row['WBAN'], row['STATION NAME'])
                else:
                    if verbose:
                        print('Excluding station', row['USAF'], row['WBAN'], row['STATION NAME'], '(not all files with observations are available for download)')
        
        # Construct a new DataFrame from the selected rows
        station_metadata = pd.DataFrame(filtered_rows, columns=self.station_metadata.columns)
        
        # Reset row index
        
        station_metadata = station_metadata.reset_index(drop=True)
        
        return Stations(station_metadata)
    
    def id(self) -> list[list[str]]:
        
        """
        
        Returns a list of 2-element lists containing station USAF and WBAN IDs as strings.
        
        Returns:
            list[list[str]]: A list of 2-element lists. Each inner list contains:
                             - First element : USAF = Air Force station ID. May contain a letter in the first position.  (str)
                             - Second element: WBAN = NCDC WBAN number (str)
        
        """
        
        # Selects all rows and the first two columns in the dataframe
        subset = self.station_metadata.iloc[:, 0:2]
        
        # Convert to nested list
        result = subset.values.tolist()
        
        return result
        
    def coordinates(self) -> list[list[str]]:
        
        """
        
        Returns a list of 2-element lists containing station latitude and longitude as floating point numbers.
        
        Returns:
            list[list[float]]: A list of 2-element lists. Each inner list contains:
                               - First element: station latitude  (float)
                               - Second element: station longitude (float)
        
        """
        
        # Selects all rows and the 6th and 7th columns in the dataframe
        subset = self.station_metadata.iloc[:, 6:8]
        
        # Convert to nested list
        result = subset.values.tolist()
        
        return result
        
    def name(self) -> list[str]:
        
        """
        
        Returns a list station names as strings.
        
        Returns:
            list[str]: A list of station names.
        
        """
        
        # Selects all rows and the 2nd column in the dataframe
        subset = self.station_metadata.iloc[:,2]
        
        # Convert to list
        result = subset.values.tolist()
        
        return result
        
    def elevation(self) -> list[list[float]]:
        
        """
        
        Returns a list station elevations.
        
        Returns:
            list[float]: List of station elevation as floating point numbers (m).
        
        """
        
        # Selects all rows and the 8th column in the dataframe
        subset = self.station_metadata.iloc[:,8]
        
        # Convert to list
        result = subset.values.tolist()
        
        return result
        
    def data_period(self) -> list[list[datetime]]:
        
        """
        
        Returns a list of 2-element lists containing the start and end dates of the period with data for each station.
        
        Returns:
            list[list[datetime]]: A list of 2-element lists. Each inner list contains:
                                  - First element: station data period start date (datetime)
                                  - Second element: station data period end date (datetime)
        
        """
        
        # Selects all rows and the 9th and 10th columns in the dataframe
        subset = self.station_metadata.iloc[:, 9:11]
        
        # Convert to nested list
        result = subset.values.tolist()
        
        return result
        
    def meta_data(self) -> list[list]:
        
        """
        
        Returns a list of 11-element lists containing the station metadata
        
        Returns:
            list[list]: A list of 11-element lists. Each inner list contains:
                        - USAF = Air Force station ID. May contain a letter in the first position (str)
                        - WBAN = NCDC WBAN number (str)
                        - STATION NAME = Location name (str)
                        - CTRY = FIPS country ID (str)
                        - ST = State for US stations (str)
                        - ICAO = ICAO ID (str)
                        - LAT = Latitude in thousandths of decimal degrees (float)
                        - LON = Longitude in thousandths of decimal degrees (float)
                        - ELEV = Elevation in meters (float)
                        - BEGIN = Station data period start date (datetime)
                        - END = Station data period end date (datetime)
        
        """
        
        # Selects all rows and columns in the dataframe
        subset = self.station_metadata.iloc[:,:]
        
        # Convert to nested list
        result = subset.values.tolist()
        
        return result
        
