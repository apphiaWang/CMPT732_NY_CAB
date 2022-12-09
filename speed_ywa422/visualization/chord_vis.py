import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import holoviews as hv
from holoviews import opts, dim

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

data = pd.read_csv('speed_ywa422/data/five_year_query_data/borough_speed/borough_speed.csv')
data['source'] = data['PUborough'].map(lambda a : map(a))
data['target'] = data['DOborough'].map(lambda a: map(a))
link = data[['source', 'target', 'avg(speed)']]
link = link.rename(columns={'avg(speed)': 'value'})
hv.Chord(link)
