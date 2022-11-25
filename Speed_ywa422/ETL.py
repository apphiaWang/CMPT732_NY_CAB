from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import sys
import os
import shapefile
import zipfile
import pandas as pd

assert sys.version_info >= (3, 5) 

'''
The green and yellow cab has different pickup time dropoff time. It needs to be uniformed.
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

'''
Get the Exact longtitude and Latitude of each zone
'''
def get_lat_lon(sf,shp_dic):
	content = []
	for sr in sf.shapeRecords():
		shape = sr.shape
		rec = sr.record
		loc_id = rec[shp_dic['LocationID']]

		x = (shape.bbox[0]+shape.bbox[2])/2
		y = (shape.bbox[1]+shape.bbox[3])/2

		content.append((loc_id, x, y))
	return pd.DataFrame(content, columns=["LocationID", "longitude", "latitude"])

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
	
	data = merge_yellow_green(inputs)

	'''
	|-- delete data with total amount less than 2.5 dollars
	|-- delete data with trip distance 0
	|-- generate speed colum with trip_distance/duration
	|-- generate tip_precentage column with tip_amount/total_amount
	|-- generate weekday column
	'''
	data = data.filter(data['total_amount']>2.5)
	data = data.filter(data['trip_distance']>0)
	data = data.withColumn("duration", data['dropoff_datetime'].cast("long")-data['pickup_datetime'].cast("long"))
	data = data.withColumn("speed", data['trip_distance']*1000/data['duration'])
	data = data.withColumn("weekday", F.dayofweek(data['pickup_datetime']))
	data = data.withColumn("tip_percentage", data['tip_amount']/data['total_amount']).cache()


	'''
	Get the Latitue and Longtitude of dataset and save it to the file
	'''
	sf = shapefile.Reader("taxi_zone/taxi_zones.shp")
	fields_name = [field[0] for field in sf.fields[1:]]
	shp_dic = dict(zip(fields_name, list(range(len(fields_name)))))
	attributes = sf.records()
	shp_attr = [dict(zip(fields_name, attr)) for attr in attributes]

	df_loc = pd.DataFrame(shp_attr).join(get_lat_lon(sf,shp_dic).set_index("LocationID"), on="LocationID")

	return data, df_loc

if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('ETL').getOrCreate()
	spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	read_ETL(inputs, output)