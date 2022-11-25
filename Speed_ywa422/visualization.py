from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import pandas as pd
import numpy as np

import os
import sys

import shapefile
from shapely.geometry import Polygon
from descartes.patch import PolygonPatch
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

from ETL import read_ETL

'''
Plot the speed distribution to confirm the valid speed range for ETL
'''
def speed_distribution_plot(data):
	data = data.select(data["speed"]).filter(data["speed"]>0)# there are negative outliars
	data = data.toPandas()
	data.hist(column='speed',bins = 200)
	plt.ylabel('frequency')
	plt.savefig(output+'/speed')

'''
Plot the duration distribution to confirm the valid duration range for ETL
'''
def duration_distribution_plot(data):
	data = data.select(data["duration"]/60)
	data = data.withColumnRenamed("(duration / 60)", "duration")
	data = data.filter(data["duration"]>0).filter(data['duration']<600) # there are negative outliars
	data = data.toPandas()
	data.hist(column='duration',bins = 200)
	plt.ylabel('frequency')
	plt.xlabel('duration/min')
	plt.savefig(output+'/duration')

'''
This part of code came from https://chih-ling-hsu.github.io/2018/05/14/NYC#location-data [line 42 to line 206]
Use this part of code to generate NYC region and zone indication map for better understanding of the geometric information
'''
def NYC_region_zone_map(data):
	sf = shapefile.Reader("taxi_zone/taxi_zones.shp")
	fields_name = [field[0] for field in sf.fields[1:]]
	shp_dic = dict(zip(fields_name, list(range(len(fields_name)))))
	attributes = sf.records()
	shp_attr = [dict(zip(fields_name, attr)) for attr in attributes]

	fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(15,8))
	ax = plt.subplot(1, 2, 1)
	ax.set_title("Boroughs in NYC")
	draw_region_map(ax = ax, sf = sf, shp_dic = shp_dic)
	ax = plt.subplot(1, 2, 2)
	ax.set_title("Zones in NYC")
	draw_zone_map(ax = ax, sf = sf, shp_dic = shp_dic)

def get_boundaries(sf):
	lat, lon = [], []
	for shape in list(sf.iterShapes()):
		lat.extend([shape.bbox[0], shape.bbox[2]])
		lon.extend([shape.bbox[1], shape.bbox[3]])

	margin = 0.01 # buffer to add to the range
	lat_min = min(lat) - margin
	lat_max = max(lat) + margin
	lon_min = min(lon) - margin
	lon_max = max(lon) + margin

	return lat_min, lat_max, lon_min, lon_max

def draw_region_map(ax, sf, shp_dic,heat={}):
	continent = [235/256, 151/256, 78/256]
	ocean = (89/256, 171/256, 227/256)    
	
	reg_list={'Staten Island':1, 'Queens':2, 'Bronx':3, 'Manhattan':4, 'EWR':5, 'Brooklyn':6}
	reg_x = {'Staten Island':[], 'Queens':[], 'Bronx':[], 'Manhattan':[], 'EWR':[], 'Brooklyn':[]}
	reg_y = {'Staten Island':[], 'Queens':[], 'Bronx':[], 'Manhattan':[], 'EWR':[], 'Brooklyn':[]}
	
	# colorbar
	if len(heat) != 0:
		norm = mpl.colors.Normalize(vmin=math.sqrt(min(heat.values())), vmax=math.sqrt(max(heat.values()))) #norm = mpl.colors.LogNorm(vmin=1,vmax=max(heat))
		cm=plt.get_cmap('Reds')
		#sm = plt.cm.ScalarMappable(cmap=cm, norm=norm)
		#sm.set_array([])
		#plt.colorbar(sm, ticks=np.linspace(min(heat.values()),max(heat.values()),8), \
		#             boundaries=np.arange(min(heat.values())-10,max(heat.values())+10,.1))
	
	ax.set_facecolor(ocean)
	for sr in sf.shapeRecords():
		shape = sr.shape
		rec = sr.record
		reg_name = rec[shp_dic['borough']]
		
		if len(heat) == 0:
			norm = mpl.colors.Normalize(vmin=1,vmax=6) #norm = mpl.colors.LogNorm(vmin=1,vmax=max(heat))
			cm=plt.get_cmap('Pastel1')
			R,G,B,A = cm(norm(reg_list[reg_name]))
			col = [R,G,B]
		else:
			R,G,B,A = cm(norm(math.sqrt(heat[reg_name])))
			col = [R,G,B]
			
		# check number of parts (could use MultiPolygon class of shapely?)
		nparts = len(shape.parts) # total parts
		if nparts == 1:
			polygon = Polygon(shape.points)
			patch = PolygonPatch(polygon, facecolor=col, alpha=1.0, zorder=2)
			ax.add_patch(patch)
		else: # loop over parts of each shape, plot separately
			for ip in range(nparts): # loop over parts, plot separately
				i0 = shape.parts[ip]
				if ip < nparts-1:
					i1 = shape.parts[ip+1]-1
				else:
					i1 = len(shape.points)

				polygon = Polygon(shape.points[i0:i1+1])
				patch = PolygonPatch(polygon, facecolor=col, alpha=1.0, zorder=2)
				ax.add_patch(patch)
				
		reg_x[reg_name].append((shape.bbox[0]+shape.bbox[2])/2)
		reg_y[reg_name].append((shape.bbox[1]+shape.bbox[3])/2)
		
	for k in reg_list:
		if len(heat)==0:
			plt.text(np.mean(reg_x[k]), np.mean(reg_y[k]), k, horizontalalignment='center', verticalalignment='center',
						bbox=dict(facecolor='black', alpha=0.5), color="white", fontsize=12)     
		else:
			plt.text(np.mean(reg_x[k]), np.mean(reg_y[k]), "{}\n({}K)".format(k, heat[k]/1000), horizontalalignment='center', 
					verticalalignment='center',bbox=dict(facecolor='black', alpha=0.5), color="white", fontsize=12)       

	# display
	limits = get_boundaries(sf)
	plt.xlim(limits[0], limits[1])
	plt.ylim(limits[2], limits[3])

def draw_zone_map(ax, sf, shp_dic, heat={}, text=[], arrows=[]):
	continent = [235/256, 151/256, 78/256]
	ocean = (89/256, 171/256, 227/256)
	theta = np.linspace(0, 2*np.pi, len(text)+1).tolist()
	ax.set_facecolor(ocean)
	
	# colorbar
	if len(heat) != 0:
		norm = mpl.colors.Normalize(vmin=min(heat.values()),vmax=max(heat.values())) #norm = mpl.colors.LogNorm(vmin=1,vmax=max(heat))
		cm=plt.get_cmap('Reds')
		sm = plt.cm.ScalarMappable(cmap=cm, norm=norm)
		sm.set_array([])
		plt.colorbar(sm, ticks=np.linspace(min(heat.values()),max(heat.values()),8),
					boundaries=np.arange(min(heat.values())-10,max(heat.values())+10,.1))
	
	for sr in sf.shapeRecords():
		shape = sr.shape
		rec = sr.record
		loc_id = rec[shp_dic['LocationID']]
		zone = rec[shp_dic['zone']]
		
		if len(heat) == 0:
			col = continent
		else:
			if loc_id not in heat:
				R,G,B,A = cm(norm(0))
			else:
				R,G,B,A = cm(norm(heat[loc_id]))
			col = [R,G,B]

		# check number of parts (could use MultiPolygon class of shapely?)
		nparts = len(shape.parts) # total parts
		if nparts == 1:
			polygon = Polygon(shape.points)
			patch = PolygonPatch(polygon, facecolor=col, alpha=1.0, zorder=2)
			ax.add_patch(patch)
		else: # loop over parts of each shape, plot separately
			for ip in range(nparts): # loop over parts, plot separately
				i0 = shape.parts[ip]
				if ip < nparts-1:
					i1 = shape.parts[ip+1]-1
				else:
					i1 = len(shape.points)

				polygon = Polygon(shape.points[i0:i1+1])
				patch = PolygonPatch(polygon, facecolor=col, alpha=1.0, zorder=2)
				ax.add_patch(patch)
		
		x = (shape.bbox[0]+shape.bbox[2])/2
		y = (shape.bbox[1]+shape.bbox[3])/2
		if (len(text) == 0 and rec[shp_dic['Shape_Area']] > 0.0001):
			plt.text(x, y, str(loc_id), horizontalalignment='center', verticalalignment='center')            
		elif len(text) != 0 and loc_id in text:
			#plt.text(x+0.01, y-0.01, str(loc_id), fontsize=12, color="white", bbox=dict(facecolor='black', alpha=0.5))
			eta_x = 0.05*np.cos(theta[text.index(loc_id)])
			eta_y = 0.05*np.sin(theta[text.index(loc_id)])
			ax.annotate("[{}] {}".format(loc_id, zone), xy=(x, y), xytext=(x+eta_x, y+eta_y),
						bbox=dict(facecolor='black', alpha=0.5), color="white", fontsize=12,
						arrowprops=dict(facecolor='black', width=3, shrink=0.05))
	if len(arrows)!=0:
		for arr in arrows:
			ax.annotate('', xy = arr['dest'], xytext = arr['src'], size = arr['cnt'],
					arrowprops=dict(arrowstyle="fancy", fc="0.6", ec="none"))
	
	# display
	limits = get_boundaries(sf)
	plt.xlim(limits[0], limits[1])
	plt.ylim(limits[2], limits[3])
	plt.savefig('figure/zone')

'''
To see the dynamic of the city in every weekday of 24 hours
'''
def weekday_24hour(path):
	files= os.listdir(path)
	for file in files:
		_, suffix = os.path.splitext(file)
		if(suffix == '.csv'):
			data = pd.read_csv(path+'/'+file)
			dt = pd.pivot_table(data, index=['weekday'], columns=['hour'], values=['average'])
			new_col = dt.columns.levels[1]
			dt.columns = new_col

			dt.sort_index(axis=0, ascending=True, inplace=True)
			dt.sort_index(axis=1, ascending=True, inplace=True)  

			plt.figure(figsize=(8, 6))
			g = sns.heatmap(dt, vmin=0.0, annot=True, fmt='.2g', cmap='Blues', cbar=True)
			plt.savefig('figure/24hour_heatmap')

def main(inputs, output):
	data, _ = read_ETL(inputs,output)
	# speed_distribution_plot(data)
	# duration_distribution_plot(data)
	# NYC_region_zone_map(data)
	weekday_24hour('24hour_speed')




if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('visualization and Data Cleansing').getOrCreate()
	spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(inputs, output)