import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPolygon, Point
import sys
import os

taxi_zones = pd.read_csv('../processed_data/taxi_zones.csv')
g_taxi_zones = gpd.GeoDataFrame(taxi_zones)
g_taxi_zones['geometry'] = gpd.GeoSeries.from_wkt(g_taxi_zones['the_geom'])


subway_stns = pd.read_csv('../processed_data/subway_stns.csv')
g_sub_stns = gpd.GeoDataFrame(subway_stns)
g_sub_stns['geometry'] = gpd.GeoSeries.from_wkt(g_sub_stns['the_geom'])
g_sub_stn_new = g_sub_stns[['geometry']]

for j in range(len(g_sub_stn_new)):
    addr = g_taxi_zones['geometry'].contains(g_sub_stn_new['geometry'][j])
    for i in range(len(addr)):
        if addr[i] == True:
            g_sub_stn_new.loc[j,'ID'] = g_taxi_zones.loc[i]['LocationID']
            g_sub_stn_new.loc[j,'zone'] = g_taxi_zones.loc[i]['zone']
            g_sub_stn_new.loc[j,'borough'] = g_taxi_zones.loc[i]['borough']
            break

g_sub_stn_new = g_sub_stn_new.drop(labels = 'geometry',axis = 1)
g_sub_stn_new.to_parquet('../output/substn_with_location.parquet', index=False)

