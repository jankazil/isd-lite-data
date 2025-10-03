'''
Provides information and functionality on Regional Transmission Organizations (RTOs) and
Independent System Operators (ISOs)
'''

from pathlib import Path

import geopandas as gpd

# Names of current RTO/ISO regions

REGION_NAMES = ['CAISO', 'ERCOT', 'ISONE', 'NYISO', 'MISO', 'PJM', 'SPP']


def regions(rto_iso_geojson: Path) -> gpd.GeoDataFrame:
    '''
    Args:
        rto_iso_json_file : a pathlib Path to a file in GeoJSON format holding RTO/ISO region information,
                            e.g. from https://atlas.eia.gov/datasets/rto-regions

    Returns:
        gdf : a Geopandas GeoDataFrame holding the names and geometries of the RTO/ISO regions.
    '''

    # Read file

    regions_raw_gdf = gpd.read_file(rto_iso_geojson)

    # Identify RTO/ISO regions

    names = []
    regions = []

    name = 'CAISO'
    names.append(name)
    mask = (regions_raw_gdf['RTO_ISO'] == name) & (regions_raw_gdf['LOC_TYPE'] == 'REG')
    region = regions_raw_gdf.loc[mask, 'geometry'].union_all()
    regions.append(region)

    name = 'ERCOT'
    names.append(name)
    mask = (regions_raw_gdf['RTO_ISO'] == name) & (regions_raw_gdf['LOC_TYPE'] == 'REG')
    region = regions_raw_gdf.loc[mask, 'geometry'].union_all()
    regions.append(region)

    name = 'ISONE'
    names.append(name)
    mask = (regions_raw_gdf['RTO_ISO'] == name) & (regions_raw_gdf['LOC_TYPE'] == 'REG')
    region = regions_raw_gdf.loc[mask, 'geometry'].union_all()
    regions.append(region)

    name = 'NYISO'
    names.append(name)
    mask = (regions_raw_gdf['RTO_ISO'] == name) & (regions_raw_gdf['LOC_TYPE'] == 'REG')
    region = regions_raw_gdf.loc[mask, 'geometry'].union_all()
    regions.append(region)

    name = 'MISO'
    names.append(name)
    mask = (regions_raw_gdf['RTO_ISO'] == name) & (regions_raw_gdf['LOC_TYPE'] == 'REG')
    region = regions_raw_gdf.loc[mask, 'geometry'].union_all()
    regions.append(region)

    name = 'PJM'
    names.append(name)
    mask = regions_raw_gdf['RTO_ISO'] == name
    region = regions_raw_gdf.loc[mask, 'geometry'].union_all()
    regions.append(region)

    name = 'SPP'
    names.append(name)
    mask = (regions_raw_gdf['RTO_ISO'] == name) & (regions_raw_gdf['LOC_TYPE'] == 'REG')
    region = regions_raw_gdf.loc[mask, 'geometry'].union_all()
    regions.append(region)

    # Create Geopandas dataframe

    regions_gdf = gpd.GeoDataFrame({'name': names, 'geometry': regions}, crs=regions_raw_gdf.crs)

    # Ensure the coordinate reference system is EPSG:4326 (latitude/longitude)

    regions_gdf = regions_gdf.to_crs('EPSG:4326')

    return regions_gdf


def region(rto_iso_geojson: Path, region_name: str) -> gpd.GeoDataFrame:
    '''
    Args:
        rto_iso_json_file : a pathlib Path to a file in GeoJSON format holding RTO/ISO region information,
                            e.g. from https://atlas.eia.gov/datasets/rto-regions
        region_name : A string containing the name of the RTO/ISO region - currently one of
                      'CAISO','ERCOT','ISONE','NYISO','MISO','PJM','SPP'

    Returns:
        gdf : a Geopandas GeoDataFrame holding the name and geometry of the selected RTO/ISO region.
    '''

    assert region_name in REGION_NAMES, (
        'Region name must be one of ' + ', '.join(REGION_NAMES) + '.'
    )

    regions_gdf = regions(rto_iso_geojson)

    mask = regions_gdf['name'] == region_name

    region_gdf = regions_gdf[mask]

    return region_gdf
