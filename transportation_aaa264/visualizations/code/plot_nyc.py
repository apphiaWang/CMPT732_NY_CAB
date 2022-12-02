import geopandas 
import geoplot
from shapely import wkt
import pandas as pd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import numpy as np
import sys
import os


def main(input,output):
    if os.path.exists(output):
        pass
    else :
        os.mkdir(output)
    taxi_zone = pd.read_csv('../processed_data/taxi_zones.csv')
    for root, _, files in os.walk(input):
        for f in files:
            if f[-8:] == ".parquet":
                if f[0:2] == "DO":
                    data = pd.read_parquet(os.path.join(root,f))
                    for i in range(len(data)):
                        for j in range(len(taxi_zone)):
                            if(taxi_zone.loc[j]['LocationID'] == data.loc[i]['DOLocationID']):
                                data.loc[i,'geometry'] = taxi_zone.loc[j]['the_geom']

                    data = data.dropna()
                    geometry = data['geometry'].apply(wkt.loads)
                    data['log_amount'] = np.log(data['DOID_count'])
                    data = geopandas.GeoDataFrame(data, crs="EPSG:4326", geometry=geometry)

                    DO_graph=data.plot(column='DOID_count', cmap='coolwarm', legend=False,figsize=(50,50),alpha=1)
                    plt.axis('off')
                    DO_graph_log=data.plot(column='log_amount', cmap='coolwarm', legend=False,figsize=(50,50),alpha=1)
                    plt.axis('off')

                    DO_graph.figure.savefig(output+'/'+f[:-8]+'.png',transparent=True)
                    DO_graph_log.figure.savefig(output+'/'+'log_'+f[:-8]+'.png',transparent=True)
                elif f[0:2] == "PU":
                    data = pd.read_parquet(os.path.join(root,f))
                    for i in range(len(data)):
                        for j in range(len(taxi_zone)):
                            if(taxi_zone.loc[j]['LocationID'] == data.loc[i]['PULocationID']):
                                data.loc[i,'geometry'] = taxi_zone.loc[j]['the_geom']

                    data = data.dropna()
                    geometry = data['geometry'].apply(wkt.loads)
                    data['log_amount'] = np.log(data['PUID_count'])
                    data = geopandas.GeoDataFrame(data, crs="EPSG:4326", geometry=geometry)

                    PU_graph=data.plot(column='PUID_count', cmap='coolwarm', legend=False,figsize=(50,50),alpha=1)
                    plt.axis('off')
                    PU_graph_log=data.plot(column='log_amount', cmap='coolwarm', legend=False,figsize=(50,50),alpha=1)
                    plt.axis('off')

                    PU_graph.figure.savefig(output+'/'+f[:-8]+'.png',transparent=True)
                    PU_graph_log.figure.savefig(output+'/'+'log_'+f[:-8]+'.png',transparent=True)


if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input,output)

