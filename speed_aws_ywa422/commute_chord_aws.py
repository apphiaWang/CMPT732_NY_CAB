import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
spark = SparkSession.builder.appName('speed prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

def main(input1, input2 ,output):
	'''
	Read data and perform ETL
	'''
	data = spark.read.parquet(input1)
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
	data = data.withColumn("hour", F.hour("pickup_datetime"))

	loc_data = spark.read.option("header", True).csv(path = input2)
	loc_data = loc_data.select("LocationID", "borough")
	data = data.alias('a').join(loc_data.alias('b'), data["PULocationID"] == loc_data["LocationID"])
	data = data.withColumnRenamed("borough", "PUborough")
	data = data.join(loc_data.alias('c'), F.col("a.DOLocationID") == F.col("c.LocationID"))
	data = data.withColumnRenamed("borough", "DOborough")
	speed_data = data.groupBy(["PUborough", "DOborough"]).agg(F.avg("speed"))
	freq_data = data.groupBy(["PUborough", "DOborough"]).count()
	
	speed_data.show()
	freq_data.show()
	speed_data.repartition(1).write.csv(path = output + '/borough_speed',header=True)
	freq_data.repartition(1).write.csv(path = output + '/borough_freq',header=True)

if __name__ == '__main__':		
	input1 = sys.argv[1]#nyc taxi input
	input2 = sys.argv[2]#location input
	output = sys.argv[3]#model saving path file
	spark = SparkSession.builder.appName('speed prediction aws').getOrCreate()
	spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(input1, input2, output)