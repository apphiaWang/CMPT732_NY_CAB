import geopandas 
import geoplot
from shapely import wkt
import pandas as pd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import sys
import os

def main(output):
    if os.path.exists(output):
        pass
    else :
        os.mkdir(output)
    lot_nums = pd.read_parquet('../processed_data/nums_of_lots.parquet')
    lot_sizes = pd.read_parquet('../processed_data/sizes_of_lots.parquet')
    taxi_zone = pd.read_csv('../processed_data/taxi_zones.csv')

    for i in range(len(lot_nums)):
        for j in range(len(taxi_zone)):
            if(taxi_zone.loc[j]['LocationID'] == lot_nums.loc[i]['ID']):
                lot_nums.loc[i,'geometry'] = taxi_zone.loc[j]['the_geom']
    
    for i in range(len(lot_sizes)):
        for j in range(len(taxi_zone)):
            if(taxi_zone.loc[j]['LocationID'] == lot_sizes.loc[i]['ID']):
                        lot_sizes.loc[i,'geometry'] = taxi_zone.loc[j]['the_geom']

    lot_nums = lot_nums.dropna()
    lot_sizes = lot_sizes.dropna()
    geometry = lot_nums['geometry'].apply(wkt.loads)
    lot_nums = geopandas.GeoDataFrame(lot_nums, crs="EPSG:4326", geometry=geometry)
    geometry = lot_sizes['geometry'].apply(wkt.loads)
    lot_sizes = geopandas.GeoDataFrame(lot_sizes, crs="EPSG:4326", geometry=geometry)

    parking_lot_nums = lot_nums.plot(column='lot_nums', cmap='coolwarm', legend=False,figsize=(50,50),alpha=1)
    plt.axis('off')
    parking_lot_nums.figure.savefig(output+'/parking_lot_nums.png',transparent=True)

    parking_lot_sizes = lot_sizes.plot(column='lot_area', cmap='coolwarm', legend=False,figsize=(50,50),alpha=1)
    plt.axis('off')
    parking_lot_sizes.figure.savefig(output+'/parking_lot_sizes.png',transparent=True)

if __name__ == '__main__':
    output = sys.argv[1]
    main(output)

