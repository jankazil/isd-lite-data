"""
Classes to hold ISD Lite data and perform operations on them.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import gzip
from pathlib import Path
import tempfile
from typing import Self

import numpy as np
import pandas as pd
import xarray as xr

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
    
    # ISD station metadata
    
    meta_data: pd.DataFrame
    
    # ISD Lite observations
    
    observations: xr.Dataset
    
    # Observation variable names, long names, and units
    
    var_names = ['T', 'TD', 'SLP', 'WD', 'WS', 'SKY', 'PREC1H', 'PREC6H']
    
    var_long_names = [
    'Air temperature at 2 m above ground',
    'Dew point temperature at 2 m above ground',
    'Sea level pressure',
    'Wind direction',
    'Wind speed at 10 m above ground',
    'Sky condition',
    '1 h accumulated precipitation',
    '6 h accumulated precipitation'
    ]
    
    var_units = ['C','C','hPa','angular degrees','m s-1','','mm','mm']
        
    def __init__(self,meta_data: pd.DataFrame):
        
        """
        
        Default constructor.
        
        Args:
            meta_data (pd.DataFrame): A Pandas dataframe with station metadata
        
        """
        
        self.meta_data = meta_data.copy()
        
        # Create lists of countries and US states
        
        self.countries = sorted(list(self.meta_data['CTRY'].dropna().unique()))
        self.us_states = sorted(list(self.meta_data['ST'].dropna().unique()))
        
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
        column_names = ['USAF', 'WBAN', 'STATION_NAME', 'CTRY', 'ST', 'CALL', 'LAT', 'LON', 'ELEV', 'BEGIN', 'END']
        
        # Read the data rows from the file
        meta_data = pd.read_fwf(file_path,widths=widths,skiprows=22,names=column_names,dtype=str)
        
        # Strip whitespace
        meta_data = meta_data.map(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Convert LAT, LON, ELEV to float if possible; empty strings become NaN
        for col in ['LAT', 'LON', 'ELEV']:
            meta_data[col] = pd.to_numeric(meta_data[col], errors='coerce')
        
        # Drop rows where both LAT and LON are NaN
        meta_data = meta_data.dropna(subset=['LAT', 'LON'], how='all')
        
        # Drop rows where the station name contains 'bogus'
        meta_data = meta_data[~meta_data['STATION_NAME'].str.contains('bogus', case=False, na=False)]
        
        # Convert the 'BEGIN' and 'END' columns from date strings to datetime objects
        for col in ['BEGIN', 'END']:
            meta_data[col] = pd.to_datetime(meta_data[col], format="%Y%m%d", errors='coerce')
        
        # Reset index
        meta_data = meta_data.reset_index(drop=True)
        
        return cls(meta_data)
    
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

        columns_title = 'USAF   WBAN  STATION NAME                  CTRY ST CALL  LAT     LON      ELEV    BEGIN    END'
        
        with open(file_path, 'w') as f:
            f.write(title_line + '\n')
            f.write('\n')
            f.write(header.lstrip('\n'))
            f.write('\n')
            f.write(columns_title + '\n')
            f.write('\n')
            for _, row in self.meta_data.iterrows():
                row_strs = [
                f"{str(row['USAF']):<6}",
                f"{str(row['WBAN']):>6}",
                f"{' ' + str(row['STATION_NAME']):<30}",
                f"{str(row['CTRY']):>3}",
                f"{str(row['ST']):>5}",
                f"{str(row['CALL']):>5}",
                f"{float(row['LAT']):+9.3f}",
                f"{float(row['LON']):+9.3f}",
                f"{float(row['ELEV']):+8.1f}",
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
        Print two-letter US states codes of the ISD Lite stations
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
        
        meta_data = self.meta_data[self.meta_data["CTRY"].isin(countries)]
        
        # Reset row index
        
        meta_data = meta_data.reset_index(drop=True)
        
        return Stations(meta_data)

    def filter_by_us_state(self,us_states: list[str]) -> Self:
        
        """
        
        Filters the station metadata by US state.
        
        Args:
            us_states (list[str]): A list of two-letter US state codes of ISD stations
        
        Returns:
            Stations: An instance of Stations holding the ISD station metadata for the given list of US states.
        
        """
        
        # Filter by country
        
        meta_data = self.meta_data[self.meta_data["ST"].isin(us_states)]
        
        # Reset row index
        
        meta_data = meta_data.reset_index(drop=True)
        
        return Stations(meta_data)

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
        
        meta_data = self.meta_data[ \
                          (self.meta_data['LAT'] >= min_lat) & \
                          (self.meta_data['LAT'] <= max_lat) & \
                          (self.meta_data['LON'] >= min_lon) & \
                          (self.meta_data['LON'] <= max_lon)   \
                          ]
        
        # Reset row index
        
        meta_data = meta_data.reset_index(drop=True)
        
        return Stations(meta_data)
    
    def filter_by_period(self,start_time: datetime,end_time: datetime) -> Self:
        
        """
        
        Filters the station metadata by time period. Stations that have nominally observations
        that cover the entire requested time period are kept, those that don't are filtered out.
        
        
        Args:
            start_time (datetime): Start time of period for which observations are required
            end_time (datetime): End time of period for which station observations are required
        
        Returns:
            Stations: An instance of Stations holding the ISD station metadata for the stations
                      whose data period fully covers, nominally, the period given by the start and end time.
        
        """
        
        # Filter by time period
        
        meta_data = self.meta_data[
                          (self.meta_data['BEGIN'] <= start_time) &
                          (self.meta_data['END'] >= end_time)
                          ]
        
        # Reset row index
        
        meta_data = meta_data.reset_index(drop=True)
        
        return Stations(meta_data)
    
    def filter_by_data_availability(self,start_time: datetime,end_time: datetime,n_jobs: int = 8,verbose: bool = False) -> Self:
        
        """
        
        Filters the station metadata by whether files with observations are available for download
        for the given period. This filter is needed because some files with observations are not available
        for download even though the station metadata may indicate that data exist for the given period.
        
        This can take a while, depending on the responsiveness of the NCEI server.
        
        Args:
            start_time (datetime): Start time of period for which files with observations must be available for download
            end_time (datetime): End time of period for which files with observations must be available for download
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
                for _, row in self.meta_data.iterrows()
            }
            
            for future in as_completed(future_to_row):
                row = future_to_row[future]
                observations_files_available = future.result()
        
                if observations_files_available:
                    filtered_rows.append(row)
                    if verbose:
                        print('Including station', row['USAF'], row['WBAN'], row['STATION_NAME'])
                else:
                    if verbose:
                        print('Excluding station', row['USAF'], row['WBAN'], row['STATION_NAME'], '(not all files with observations are available for download)')
        
        # Construct a new DataFrame from the selected rows
        meta_data = pd.DataFrame(filtered_rows, columns=self.meta_data.columns)
        
        # Reset row index
        
        meta_data = meta_data.reset_index(drop=True)
        
        return Stations(meta_data)
    
    def id(self) -> list[list[str]]:
        
        """
        
        Returns a list of 2-element lists containing station USAF and WBAN IDs as strings.
        
        Returns:
            list[list[str]]: A list of 2-element lists. Each inner list contains:
                             - First element : USAF = Air Force station ID. May contain a letter in the first position.  (str)
                             - Second element: WBAN = NCDC WBAN number (str)
        
        """
        
        # Selects all rows and the first two columns in the dataframe
        subset = self.meta_data.iloc[:, 0:2]
        
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
        subset = self.meta_data.iloc[:, 6:8]
        
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
        subset = self.meta_data.iloc[:,2]
        
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
        subset = self.meta_data.iloc[:,8]
        
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
        subset = self.meta_data.iloc[:, 9:11]
        
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
        subset = self.meta_data.iloc[:,:]
        
        # Convert to nested list
        result = subset.values.tolist()
        
        return result
    
    def load_observations(self,data_dir: Path, start_year: int, end_year: int,verbose: bool = False):
        
        """
        Loads ISD Lite station observations for a given year range (inclusive)
        from (gzipped) NCEI ISD Lite data files into the xarray object self.observations.
        
        The files must already exist in the specified directory,
        having been previously downloaded from the web.
        
        Args:
            data_dir (pathlib.Path): Local directory containing ISD-Lite data files.
            start_year (int): Gregorian year of the first data file to be read
            end_year (int): Gregorian year of the last data file to be read
            verbose (bool): If True, print information. Defaults to False.
        """
        
        #
        # Load data for each station and the year range
        #
        
        dfs = []
        
        usaf_ids = []
        wban_ids = []
        station_ids = []
        station_names = []
        ctrys = []
        ussts = []
        lats = []
        lons = []
        elevs = []
        
        for row in self.meta_data.itertuples():
            
            if verbose:
                print('Loading observations for station',row.STATION_NAME)
            
            df = self.read_station_observations(data_dir,start_year,end_year,row.USAF,row.WBAN)
            
            dfs.append(df)
            
            usaf_ids.append(row.USAF)
            wban_ids.append(row.WBAN)
            station_ids.append(row.USAF + '-' + row.WBAN)
            station_names.append(row.STATION_NAME)
            ctrys.append(row.CTRY)
            ussts.append(row.ST)
            lats.append(row.LAT)
            lons.append(row.LON)
            elevs.append(row.ELEV)
        
        #
        # Create common times
        #
        
        # Union of all time values
        
        all_times = pd.Index([])
        for df in dfs:
            all_times = all_times.union(df.index)
        
        all_times = all_times.sort_values()
        
        # Convert to numpy array of dtype datetime64[ns]
        all_times = pd.to_datetime(all_times).values
        
        #
        # Construct a dictionary, which for each variable, holds
        # a numpy array with the data as a function of time and station
        #
        
        # Collect all columns containing variables (that means exclude time)
        columns = [col for col in dfs[0].columns if col != 'time']
        
        data_vars = {}
        for var in columns:
            # Numpy array with the dimensions all times, stations
            arr = np.full((len(all_times), len(dfs)), np.nan)
            for i, df in enumerate(dfs):
                times = df.index
                s = pd.Series(df[var].values, index=times)
                arr[:, i] = s.reindex(all_times).values
            data_vars[var] = (('time', 'station'), arr)
        
        #
        # Create an xarray dataset that holds the data for each variable
        # as a function of time and station
        #
        
        ds = xr.Dataset(data_vars,coords={'time': all_times, 'station': station_ids})
        
        ds['time'].attrs['long_name'] = 'UTC'
        
        # Set long names and units
        
        for var_name, var_long_name, var_unit in zip(self.var_names,self.var_long_names,self.var_units):
            ds[var_name].attrs['long_name'] = var_long_name
            ds[var_name].attrs['units'] = var_unit
        
        #
        # Add variables that are a function of station only (as data variables)
        #
        
        ds = ds.assign(
            lat=("station", lats),
            lon=("station", lons),
            elevation=("station", elevs),
            station_name=("station", station_names),
            station_id=("station", station_ids),
            country=("station", ctrys),
            us_state=("station", ussts)
        )
        
        # Set long names and units
        
        ds['lat'].attrs['long_name'] = 'Latitude'
        ds['lat'].attrs['units'] = 'degrees north'
        
        ds['lon'].attrs['long_name'] = 'Longitude'
        ds['lon'].attrs['units'] = 'degrees east'
        
        ds['elevation'].attrs['long_name'] = 'Elevation above sea level'
        ds['elevation'].attrs['units'] = 'm'
        
        ds['station_name'].attrs['long_name'] = 'Station name'
        ds['station_name'].attrs['units'] = ''
        
        ds['station_id'].attrs['long_name'] = 'Station USAF and WBAN id'
        ds['station_id'].attrs['units'] = ''
        
        ds['country'].attrs['long_name'] = 'Country code'
        ds['country'].attrs['units'] = ''
        
        ds['us_state'].attrs['long_name'] = 'US state'
        ds['us_state'].attrs['units'] = ''
        
        #
        # Add global attributes
        #
        
        ds.attrs['title'] = 'ISD-Lite station observations'
        ds.attrs['source'] = 'National Centers for Environmental Information (NCEI)'
        ds.attrs['URL'] = ncei.isd_lite_url
        
        #
        # Assign to instance variable
        #
        
        self.observations = ds
        
        return
    
    def write_observations2netcdf(self,file_path : Path):
        
        """
        Writes the xarray self.observations into a netCDF file.
        
        Args:
            file_path (Path): Path to the netCDF file. The file will be overwritten if it exists.
        """
        
        encoding = {
            "time": {
                "dtype": "float64",
                "units": "seconds since 1970-01-01T00:00:00Z",
                "calendar": "proleptic_gregorian"
            },
            **{
                var: {"dtype": "float32"} for var in self.observations.data_vars if np.issubdtype(self.observations[var].dtype, np.floating)
            }
        }
        
        self.observations.to_netcdf(file_path,encoding=encoding)
        
        return
    
    def read_station_observations(
        self,
        data_dir: Path,
        start_year: int,
        end_year: int,
        usaf_id: str,
        wban_id: str,
        missing_value=-9999) -> pd.DataFrame:
        
        """
        Reads ISD Lite observations from (gzipped) NCEI ISD Lite data files
        for the given range of years (inclusive), for the given station.
        
        The files must already exist in the specified directory, having been previously downloaded from the web.
        
        The data are read and processed based on their description in
        
        https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/isd-lite-format.pdf
        https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/isd-lite-technical-document.pdf
        
        Args:
            data_dir (Path): Path to directory holding (gzipped) NCEI ISD Lite data files.
            start_year (int): Gregorian year of the first data file to be read
            end_year (int): Gregorian year of the last data file to be read
            usaf_id (str): Air Force station ID. May contain a letter in the first position.
            wban_id (str): Weather Bureau Army Navy station ID.
            missing_value: Integer or floating point number corresponding to missing data values in the file.
        Returns:
            pandas.Dataframe : A pandas dataframe holding variable names, times, and obsevations
                               as a function of time for the given station and the given year range.
        """
        
        dfs = []
        
        for year in range(start_year,end_year+1):
            
            file_path = data_dir / ncei.ISDLite_data_file_name(year,usaf_id,wban_id)
            
            if not file_path.exists():
                raise ValueError(str(file_path) + ' does not exist.')
            
            # Read the gzipped file as text
            
            with gzip.open(file_path, 'rt') as f:
                # Read all lines, split by whitespace
                df = pd.read_csv( 
                f,
                sep=r'\s+',
                header=None,
                na_values=missing_value,
                dtype = {0: int,
                         1: int,
                         2: int,
                         3: int,
                         4: np.float32,
                         5: np.float32,
                         6: np.float32,
                         7: np.float32,
                         8: np.float32,
                         9: np.float32,
                         10: np.float32,
                         11: np.float32})
            
            # Remove trace precipitation values
            
            for jj in range(10,12):
              col = df.iloc[:,jj]
              mask = (col == -1)
              df.iloc[mask,jj] = 0
            
            # Multiply colums with appropriate scaling factors
            
            cols = [4,5,6,8,10,11]
            df.iloc[:, cols] = 0.1 * df.iloc[:, cols]
            
            # Extract the time columns and convert them to datetime objects
            df['time'] = pd.to_datetime(df.iloc[:,0:4].astype(str).agg(' '.join,axis=1), format='%Y %m %d %H')
            
            # Cut the time columns
            df = df.drop(columns=[0,1,2,3])
            
            dfs.append(df)
            
        df_merged = pd.concat(dfs, ignore_index=True)
        
        # Set variable names
        df_merged.columns = self.var_names + ['time']
        
        # Set time as index
        df_merged = df_merged.set_index('time')
        
        return df_merged
