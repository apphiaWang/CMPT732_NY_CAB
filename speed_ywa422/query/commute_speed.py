from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import sys
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
'''
Run command
spark-submit commute_speed.py ../data ../../speed_aws_ywa422/aws_gen_query_data/borough_speed/borough_speed.csv ../data/test_gen_data
'''

etl_path = os.path.join(os.path.dirname(__file__) , "../ETL")
sys.path.append(etl_path) #absolute path of ETL package
from ETL import read_ETL

#map borough name to index
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

def gen_comute_avg_speed(data, output):
	data = data.groupBy(["PUborough", "DOborough"]).agg(F.avg("speed"))
	data = data.toPandas()
	data.to_csv(output + '/borough_speed')
		
def gen_commute_total_freq(data, output):
	data = data.groupBy(["PUborough", "DOborough"]).count()
	data = data.toPandas()
	data.to_csv(output + '/borough_count')

#hepler function to convert string boroughs to index pair
def pd_func(a,b):
	return str( str(map(a)) + '-' + str(map(b)))

def histo_graph(input2):
	df = pd.read_csv(input2)
	df["start_end"] = df.apply(lambda x: pd_func(x.PUborough, x.DOborough), axis = 1)
	df = df.sort_values(by=['avg(speed)'])
	x = np.arange(0,360,10)
	y = df["avg(speed)"].values.tolist()
	x_tic = df["start_end"].values.tolist()
	
	plt.plot(x,y)
	plt.xticks(x,x_tic)
	plt.tick_params(labelsize = 5)
	plt.xlabel = "from zone1 to zone 2"
	plt.ylabel = "average speed km/h"
	plt.text(10,10, "0-Brooklyn, 1-Queens, 2-Manhattan, 3-Bronx, 4-EWR, 5-Staten Island",fontsize = 5)
	plt.savefig('../figure/test_gen_output/hist')

def main(inputs, input2, output):
	#generate pickup and dropoff borough column
	data, loc_data = read_ETL(inputs, output)
	loc_data = loc_data.select("LocationID", "borough")
	data = data.alias('a').join(loc_data.alias('b'), data["PULocationID"] == loc_data["LocationID"])
	data = data.withColumnRenamed("borough", "PUborough")
	data = data.join(loc_data.alias('c'), F.col("a.DOLocationID") == F.col("c.LocationID"))
	data = data.withColumnRenamed("borough", "DOborough")
	
	gen_comute_avg_speed(data, output)
	gen_commute_total_freq(data, output)
	histo_graph(input2)


	

if __name__ == '__main__':
	inputs = sys.argv[1] #data directory
	input2 = sys.argv[2] #historgram data input
	output = sys.argv[3]
	spark = SparkSession.builder.appName('explore commute times and speed relationship').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	if not os.path.isdir(output):
		os.makedirs(output) 
	main(inputs, input2, output)