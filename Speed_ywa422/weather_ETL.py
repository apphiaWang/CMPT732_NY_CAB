from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import functions as F
import pandas as pd
import sys
import os

def uniform_date(date):
	#alter the date format to 'yyyy-mm-dd'
	day, month, year = date.split('/')
	if(len(day)<2):
		day = '0'+ day
	if(len(month)<2):
		month = '0'+month
	year = '20'+year
	return year + '-' + month + '-' + day

def has_snow(new_snow, snow_depth):
	if(new_snow!='0' and new_snow!='T') or (snow_depth!='0' and snow_depth!='T'):
		return 1
	else:
		return 0

def has_precipitation(precipitation):
	if(precipitation == '0' or precipitation == 'T'):
		return 0
	else:
		return 1

#weather dataset is small so udf is okay here
rainUDF = F.udf(lambda z: has_precipitation(z), IntegerType())
snowUDF = F.udf(lambda z1,z2: has_snow(z1,z2), IntegerType())
dateUDF = F.udf(lambda z: uniform_date(z), StringType())

inputs = sys.argv[1]
output = sys.argv[2]
spark = SparkSession.builder.appName('ETL').getOrCreate()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
assert spark.version >= '3.0' # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

'''
	Weather Schema
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
	|-- convert string type date to date type date
	|-- convert Fahrenheit to Celsius
	|-- add has_snow boolean feature
	|-- add has_rain boolean feature
	|-- leave only date, average temperature, has_snow, has_precipitation colmns
'''
data = data.withColumn("date", dateUDF(data["date"]))
data = data.withColumn("new_date", F.to_date(data["date"], 'yyyy-mm-dd')) #to date has to assign to a new column name or the month will all becom '01' (why?)
data = data.withColumn("has_snow", snowUDF(data["new_snow"],data["snow_depth"]))
data = data.withColumn("tavg", (data["tavg"]-32)/1.8)
data = data.withColumn("has_rain", rainUDF(data["precipitation"]))
data = data.select("date","tavg","has_snow","has_rain")


data.repartition(1).write.mode("overwrite").option("header", "true").csv(output)