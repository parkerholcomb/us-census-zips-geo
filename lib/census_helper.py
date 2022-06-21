# ok I want with confidence, all zip codes reported by census, and their lat/lngs
import pandas as pd
import plotly.express as px
from IPython.display import display
from scipy.stats import zscore


def _features_only(df): return df[[name for name in df.columns if name.startswith("_") ]]

def _load_census_zips(features_only=True) -> pd.DataFrame:
    # todo put the census.gov link
    df = pd.read_csv("lib/data/DECENNIALSF12010.P1_2022-06-09T164557/DECENNIALSF12010.P1_data_with_overlays_2022-04-27T100124.csv")
    df['_census_total'] = df['Total'].apply(lambda x: int(x))
    df['_zip'] = df['Geographic Area Name'].apply(lambda x: x.split(" ")[-1])
    assert(type(df['_zip'][0]) == str)
    return _features_only(df) if features_only else df

def _load_zip_lat_lng(features_only = True, force=False) -> pd.DataFrame:
    # https://www.census.gov/cgi-bin/geo/shapefiles/index.php
    # this takes about 16 seconds from root, so going to cache (<1s load)
    if force:
        import geopandas as gpd
        shapefile = gpd.read_file("lib/data/tl_2021_us_zcta520/tl_2021_us_zcta520.shp")
        df = shapefile
        df['_zip'] = df['ZCTA5CE20']
        df['_lat'] = df['INTPTLAT20'].apply(lambda x: float(x))
        df['_lng'] = df['INTPTLON20'].apply(lambda x: float(x))
    else:
        df = pd.read_feather("lib/data/_zip_lat_lng.feather")
    return _features_only(df) if features_only else df

def load_sweetened_zips():
    zip_lat_lng = _load_zip_lat_lng(force=False)
    # display(zip_lat_lng.head())
    census_zips = _load_census_zips()
    # display(census_zips.head())
    df = zip_lat_lng.merge(census_zips)
    return df
