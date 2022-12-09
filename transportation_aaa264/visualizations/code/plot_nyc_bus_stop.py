import geopandas 
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
    bus_stop_nums = pd.read_parquet('../processed_data/nums_of_bus_stop.parquet')
    taxi_zone = pd.read_csv('../processed_data/taxi_zones.csv')

    for i in range(len(bus_stop_nums)):
        for j in range(len(taxi_zone)):
            if(taxi_zone.loc[j]['LocationID'] == bus_stop_nums.loc[i]['ID']):
                bus_stop_nums.loc[i,'geometry'] = taxi_zone.loc[j]['the_geom']

    bus_stop_nums = bus_stop_nums.dropna()
    geometry = bus_stop_nums['geometry'].apply(wkt.loads)
    bus_stop_nums = geopandas.GeoDataFrame(bus_stop_nums, crs="EPSG:4326", geometry=geometry)

    parking_bus_stop_nums = bus_stop_nums.plot(column='bus_stop_nums', cmap='coolwarm', legend=False,figsize=(50,50),alpha=1)
    plt.axis('off')
    parking_bus_stop_nums.figure.savefig(output+'/parking_bus_stop_nums.png',transparent=True)

if __name__ == '__main__':
    output = sys.argv[1]
    main(output)

