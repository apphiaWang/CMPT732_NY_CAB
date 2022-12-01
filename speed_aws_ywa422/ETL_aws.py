from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import sys
import os

spark = SparkSession.builder.appName('ETL').getOrCreate()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

assert sys.version_info >= (3, 5) 
assert spark.version >= '3.0'

'''
The green and yellow cab has different pickup time dropoff time. It needs to be uniformed.
This function is only needed when using the initial ETL version of data.
'''
def merge_yellow_green(inputs):
	data = None

	filepath = inputs + '/'
	files= os.listdir(inputs)
	print(inputs)
	for file in files: 
		if not os.path.isdir(file):
			path = filepath+str(file)
			file_data = spark.read.parquet(path)
			if 'lpep_pickup_datetime' in file_data.columns:
				file_data = file_data.withColumnRenamed('lpep_pickup_datetime','pickup_datetime').withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')
			if 'tpep_pickup_datetime' in file_data.columns:
				file_data = file_data.withColumnRenamed('tpep_pickup_datetime','pickup_datetime').withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')
			file_data = file_data.select("pickup_datetime","dropoff_datetime", "payment_type", "PULocationID", "DOLocationID", "trip_distance", "total_amount","tip_amount")
			if data == None:
				data = file_data
			else:
				data.union(file_data)
				file_data.printSchema()
	return data

def read_ETL(inputs, output):
	'''
	The schema of original data set
	|-- VendorID: long (nullable = true)
	|-- tpep_pickup_datetime: timestamp (nullable = true)
	|-- tpep_dropoff_datetime: timestamp (nullable = true)
	|-- store_and_fwd_flag: string (nullable = true)
	|-- RatecodeID: long (nullable = true)
	|-- PULocationID: long (nullable = true)
	|-- DOLocationID: long (nullable = true)
	|-- passenger_count: long (nullable = true)
	|-- trip_distance: double (nullable = true)
	|-- fare_amount: double (nullable = true)
	|-- extra: double (nullable = true)
	|-- mta_tax: double (nullable = true)
	|-- tip_amount: double (nullable = true)
	|-- tolls_amount: double (nullable = true)
	|-- ehail_fee: integer (nullable = true)
	|-- improvement_surcharge: double (nullable = true)
	|-- total_amount: double (nullable = true)
	|-- payment_type: long (nullable = true)
	|-- trip_type: long (nullable = true)
	|-- congestion_surcharge: integer (nullable = true)
	'''
	
	# data = merge_yellow_green(inputs)
	data = spark.read.parquet(inputs)
	'''
	|-- delete data with total amount less than 2.5 dollars
	|-- delete data with trip distance 0
	|-- delete data with unknown zone (LocationID = 264, 265)
	|-- delete data with year other than 2017-2021
	|-- generate duration column and delete duration less than 0 and longer than 180min (3h, 10800sec)
	|-- generate speed column with trip_distance/duration adn delete speed less than 0 and larger than 100km/h
	|-- generate tip_precentage column with tip_amount/total_amount
	|-- generate weekday column
	|-- generate hour column
	'''
	data = data.filter(data['total_amount']>2.5)
	data = data.filter(data['trip_distance']>0)
	data = data.filter(data['PULocationID'] != 264).filter(data['DOLocationID'] != 265).\
		filter(data['PULocationID'] != 264).filter(data['DOLocationID'] != 265)
	data = data.filter(F.year(data["pickup_datetime"]) >= 2017)
	data = data.filter(F.year(data["pickup_datetime"]) <= 2021)
	data = data.filter(F.year(data["dropoff_datetime"]) >= 2017)
	data = data.filter(F.year(data["dropoff_datetime"]) <= 2021)
	data = data.withColumn("duration", data['dropoff_datetime'].cast("long")-data['pickup_datetime'].cast("long"))
	data = data.filter(data["duration"]>0).filter(data["duration"]<10800)
	data = data.withColumn("speed", data['trip_distance']/(data['duration']/3600))
	data = data.filter(data['speed']<100).filter(data["speed"]>0)
	data = data.withColumn("weekday", F.dayofweek(data['pickup_datetime']))
	data = data.withColumn("tip_percentage", data['tip_amount']/data['total_amount'])
	data =data.withColumn("hour", F.hour("pickup_datetime"))

	return data

if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('ETL').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	read_ETL(inputs, output)