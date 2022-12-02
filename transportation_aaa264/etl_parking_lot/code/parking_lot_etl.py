import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPolygon

taxi_zones = pd.read_csv('../processed_data/taxi_zones.csv')
g_taxi_zones = gpd.GeoDataFrame(taxi_zones)
g_taxi_zones['geometry'] = gpd.GeoSeries.from_wkt(g_taxi_zones['the_geom'])


lots = pd.read_csv('../processed_data/DOITT_PARKING_LOT.csv')
g_lots = gpd.GeoDataFrame(lots)
g_lots['geometry'] = gpd.GeoSeries.from_wkt(g_lots['the_geom'])
g_lots = g_lots.drop(labels = ['the_geom','SOURCE_ID','FEAT_CODE','SUB_CODE','STATUS','SHAPE_Leng'],axis=1)

for j in range(len(g_lots)):
    addr = g_taxi_zones['geometry'].intersects(g_lots['geometry'][j])
    for i in range(len(addr)):
        if addr[i] == True:
            g_lots.loc[j,'ID'] = g_taxi_zones.loc[i]['LocationID']
            g_lots.loc[j,'zone'] = g_taxi_zones.loc[i]['zone']
            g_lots.loc[j,'borough'] = g_taxi_zones.loc[i]['borough']
            break
        
g_lots = g_lots.drop(labels = 'geometry',axis = 1)
g_lots.to_parquet('../output/lots_with_location.parquet', index=False)

