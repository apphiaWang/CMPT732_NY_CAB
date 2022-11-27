from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import os
import sys
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from weather_ETL import weather_ETL

def averaget(data):
	data = data.toPandas()
	data.plot()
	plt.savefig(output + '/avgt')

def main(inputs, output):
	data = weather_ETL(inputs, output)
	averaget(data)

if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('weather ETL').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(inputs, output)