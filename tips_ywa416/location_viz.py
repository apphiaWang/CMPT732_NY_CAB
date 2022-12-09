import pandas as pd
import geopandas
import sys
import os

# load taxi zone file and shape file
taxi_zone =  os.path.join(os.path.dirname(__file__), 'data/taxi_zone/taxi+_zone_lookup.csv')
shape_file = os.path.join(os.path.dirname(__file__), 'data/taxi_zone/taxi_zones.shp')
lookup_zone = pd.read_csv(taxi_zone)
shp_df = geopandas.GeoDataFrame.from_file(shape_file)

'''
    Visualize gemoetric heatmap of corresponding data and column
'''
def draw_map(fig_name, df, col, index='locationID'):
    data = shp_df.set_index('LocationID').join(df.set_index(index))
    ax = data.plot(column=col, cmap='coolwarm', legend=True,figsize=(20,20),alpha=1)
    fig=ax.get_figure()
    fig.savefig(fig_name)

def join_data(spark_dir, output_path, index = 'LocationID'):
    filepath = "" # tailored for reading spark output
    for f in os.listdir(spark_dir):
        if f.startswith("part"):
            filepath = os.path.join(spark_dir, f)
            break
    if not os.path.isdir(output_path):
        os.makedirs(output_path) 
    df = pd.read_csv(filepath)
    joined = df.join(lookup_zone, lsuffix=index, rsuffix='LocationID')
    joined.to_csv('%s/full.csv'%output_path, index=False)
    joined = joined[joined["count"]> count_threshold] 
    joined.to_csv('%s/>%i.csv'%(output_path,count_threshold), index=False)
    joined.head(20).to_csv('%s/top20.csv'%output_path, index=False)
    return df

def main(input, output):
    # the 
    spark_dirs = ["dropoff", "pickup", "total", "petty", "generous"]
    indexes = ['DOLocationID', 'PULocationID', 'locationID', 'locationID', 'locationID']
    cols = ["avg", "avg", "avg", "0_tip_ratio", "count"]
    fig_names = ["mean_tip_dropoff", "mean_tip_pickup", "mean_tip_ny", "0_tip_ratio", "0_tip_ny"]
    for i in range(len(spark_dirs)):
        df = join_data(os.path.join(input, spark_dirs[i]), os.path.join(output, spark_dirs[i]))
        draw_map(os.path.join(output, 'figures', fig_names[i]), df, cols[i], indexes[i])
    return

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    count_threshold = int(sys.argv[2]) if len(sys.argv) > 3 else 1000
    if not os.path.isdir(output):
        os.makedirs(output) 
        os.makedirs(os.path.join(output, 'figures')) 
    main(input, output)