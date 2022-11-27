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
          speed      hour
speed  1.000000  0.216297
hour   0.216297  1.000000
'''
def hour_speed(data):
	data = data.sample(True, 0.05)
	data = data.withColumn("hour", F.hour(data['pickup_datetime']))
	data = data.select("speed", "hour")
	data = data.toPandas()
	print(data.corr())
	data.plot.scatter(x="hour", y = "speed")
	plt.savefig(output+'/hour_speed')

'''
            speed   weekday
speed    1.000000 -0.051392
weekday -0.051392  1.000000
'''
def weekday_speed(data):
	data = data.sample(True, 0.05)
	data = data.select("speed", "weekday")
	data = data.toPandas()
	print(data.corr())
	data.plot.scatter(x="weekday", y = "speed")
	plt.savefig(output+'/weekday_speed')

'''
                   speed  tip_percentage
speed           1.000000       -0.054848
tip_percentage -0.054848        1.000000
'''
def tipper_speed(data):
	data = data.sample(True, 0.05)
	data = data.select("speed", "tip_percentage")
	data = data.toPandas()
	print(data.corr())
	data.plot.scatter(x="tip_percentage", y = "speed")
	plt.savefig(output+'/tipper_speed')

def main(inputs, output):
	data, _ = read_ETL(inputs,output)
	# hour_speed(data)
	# weekday_speed(data)
	tipper_speed(data)

if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('visualization and Data Cleansing').getOrCreate()
	spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(inputs, output)