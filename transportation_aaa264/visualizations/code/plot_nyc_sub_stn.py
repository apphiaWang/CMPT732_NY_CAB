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
    sub_stn_nums = pd.read_parquet('../processed_data/nums_of_sub_stn.parquet')
    taxi_zone = pd.read_csv('../processed_data/taxi_zones.csv')

    for i in range(len(sub_stn_nums)):
        for j in range(len(taxi_zone)):
            if(taxi_zone.loc[j]['LocationID'] == sub_stn_nums.loc[i]['ID']):
                sub_stn_nums.loc[i,'geometry'] = taxi_zone.loc[j]['the_geom']

    sub_stn_nums = sub_stn_nums.dropna()
    geometry = sub_stn_nums['geometry'].apply(wkt.loads)
    sub_stn_nums = geopandas.GeoDataFrame(sub_stn_nums, crs="EPSG:4326", geometry=geometry)

    g_sub_stn_nums = sub_stn_nums.plot(column='sub_stn_nums', cmap='coolwarm', legend=False,figsize=(50,50),alpha=1)
    plt.axis('off')
    g_sub_stn_nums.figure.savefig(output+'/sub_stn_nums.png',transparent=True)

if __name__ == '__main__':
    output = sys.argv[1]
    main(output)

