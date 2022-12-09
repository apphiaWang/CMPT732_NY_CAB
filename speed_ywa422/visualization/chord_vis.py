import numpy as np
import matplotlib as mpl
import sys
import matplotlib.pyplot as plt
import pandas as pd
import holoviews as hv
from holoviews import opts, dim

'''
Run Command
python3 chord_vis.py ../data

Function
This part of code read the commute frequncy of each Borough and generate a chord graph.
Graph won't show in the Linux environment.
'''

hv.extension('bokeh')
hv.output(size=200)

def map(str):
	if(str == 'Brooklyn'):
		return 0
	elif(str == 'Queens'):
		return 1
	elif(str == 'Manhattan'):
		return 2
	elif(str == 'Bronx'):
		return 3
	elif(str == 'EWR'):
		return 4
	else:
		return 5

def main(input):
	data = pd.read_csv(input + '/five_year_query_data/borough_speed/borough_speed.csv')
	data['source'] = data['PUborough'].map(lambda a : map(a))
	data['target'] = data['DOborough'].map(lambda a: map(a))
	link = data[['source', 'target', 'avg(speed)']]
	link = link.rename(columns={'avg(speed)': 'value'})
	hv.Chord(link)

if __name__ == '__main__':		
	inputs = sys.argv[1]
	main(inputs)