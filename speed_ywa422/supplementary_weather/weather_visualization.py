import pandas as pd
import sys
import matplotlib.pyplot as plt

'''
Run Command
python3 weather_visualization.py weather_ETL_data/weather_data.csv figure_dir

Function
Draw nyc 2019 temperature plot
'''

def averaget(data, output):
	data.plot()
	plt.savefig( output + '/avgt')

def main(inputs, output):
	data = pd.read_csv(inputs)
	averaget(data, output)

if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)