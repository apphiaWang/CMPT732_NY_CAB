from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
import pandas as pd
import sys
import os

def has_snow(new_snow, snow_depth):
	if(new_snow!='0' and new_snow!='T') or (snow_depth!='0' and snow_depth!='T'):
		return 1
	else:
		return 0

def has_precipitation(precipitation):
	if(precipitation!='0' or precipitation!='T'):
		return 1
	else:
		return 0



def main(inputs, output):
	'''
	|-- date: string (nullable = true)
	|-- tmax: long (nullable = true)
	|-- tmin: long (nullable = true)
	|-- tavg: double (nullable = true)
	|-- departure: double (nullable = true)
	|-- HDD: long (nullable = true)
	|-- CDD: long (nullable = true)
	|-- precipitation: string (nullable = true)
	|-- new_snow: string (nullable = true)
	|-- snow_depth: string (nullable = true)
	'''
	data = pd.read_csv(inputs+'/nyc_temperature.csv')
	data = spark.createDataFrame(data)

	
	'''
	|-- convert Fahrenheit to Celsius
	|-- convert string date to date format
	|-- add has_snow boolean feature
	|-- add has_rain boolean feature
	|-- leave only date, average temperature, has_snow, has_precipitation colmns
	'''
	data = data.withColumn("date", F.to_date(data["date"]))
	# data = data.withColumn("has_snow", )
	data = data.withColumn("tavg", (data["tavg"]-32)/1.8)
	# data = data.withColumn("has_rain", rainUDF(data["precipitation"]))
	data.show()
	return data

if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('ETL').getOrCreate()
	spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(inputs, output)