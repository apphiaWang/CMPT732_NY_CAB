import pandas as pd
import geopandas
import sys

'''
Run Command
python3  multipolygon.py ../figure/test_gen_output

Function
Draw nyc heatmap with each zone's average speed
'''

def main(output):
	shp_df = geopandas.GeoDataFrame.from_file("../data/taxi_zone/taxi_zones.shp")
	speed_data = pd.read_csv('../../speed_aws_ywa422/aws_gen_query_data/boxplot/box_plot.csv')
	data = shp_df.set_index('LocationID').join(speed_data.set_index('PULocationID'))
	ax = data.plot(column='median', cmap='coolwarm', legend=True,figsize=(20,20),alpha=1)
	fig=ax.get_figure()
	fig.savefig(output + '/speed_map')

if __name__ == '__main__':
	output = sys.argv[1]
	main(output)