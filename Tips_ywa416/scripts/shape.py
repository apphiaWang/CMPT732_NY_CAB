import pandas as pd
import geopandas
import sys

'''
Run Command
python3  multipolygon.py ../figure/test_gen_output

Function
Draw nyc heatmap with each zone's average speed
'''

def draw_map(filepath, fig_name, col, shp_df):
    speed_data = pd.read_csv(filepath)
    data = shp_df.set_index('LocationID').join(speed_data.set_index('LocationID'))
    ax = data.plot(column=col, cmap='coolwarm', legend=True,figsize=(20,20),alpha=1)
    fig=ax.get_figure()
    fig.savefig(fig_name)
    
def main(input, output):
    shp_df = geopandas.GeoDataFrame.from_file("data/taxi_zone/taxi_zones.shp")
    files = ["location/full.csv", "petty/full.csv", "petty/full.csv"]
    cols = ["avg", "0_tip_ratio", "count"]
    fig_names = ["mean_tip_ny", "0_tip_ratio", "0_tip_ny"]
    for i in range(len(files)):
        filepath = '{input}/{file}'.format(input=input, file=files[i])
        draw_map(filepath, fig_names[i], cols[i], shp_df)
     

    

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)