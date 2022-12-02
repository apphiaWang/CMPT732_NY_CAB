import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPolygon, Point
import sys
import os

taxi_zones = pd.read_csv('../processed_data/taxi_zones.csv')
g_taxi_zones = gpd.GeoDataFrame(taxi_zones)
g_taxi_zones['geometry'] = gpd.GeoSeries.from_wkt(g_taxi_zones['the_geom'])


bus_stops = pd.read_csv('../processed_data/Bus_Stop_Shelter.csv')
g_bus_stops = gpd.GeoDataFrame(bus_stops)
g_bus_stops['geometry'] = gpd.GeoSeries.from_wkt(g_bus_stops['the_geom'])
g_bus_stops_new = g_bus_stops[['geometry']]

for j in range(len(g_bus_stops_new)):
    addr = g_taxi_zones['geometry'].contains(g_bus_stops_new['geometry'][j])
    for i in range(len(addr)):
        if addr[i] == True:
            g_bus_stops_new.loc[j,'ID'] = g_taxi_zones.loc[i]['LocationID']
            g_bus_stops_new.loc[j,'zone'] = g_taxi_zones.loc[i]['zone']
            g_bus_stops_new.loc[j,'borough'] = g_taxi_zones.loc[i]['borough']
            break

g_bus_stops_new = g_bus_stops_new.drop(labels = 'geometry',axis = 1)
g_bus_stops_new.to_parquet('../output/bus_stops_with_location.parquet', index=False)

