from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F

import sys

#aws cannot refer to another python file in the same S3 directory? 

'''
Get each zone's max, min ,median, Q2, Q4 speed
Pickup location and Dropoff location contribute to the calculation of each zone 
'''
def box_plot(data):
	merge_data = data.withColumnRenamed("PULocationID","tmp")
	merge_data = merge_data.withColumnRenamed("DOLocationID", "PULocationID")
	merge_data = merge_data.withColumnRenamed("tmp", "DOLocationID")
	data = data.union(merge_data)
	med = F.expr('percentile_approx(speed, 0.5)')
	Q2 = F.expr('percentile_approx(speed, 0.25)')
	Q4 = F.expr('percentile_approx(speed, 0,75)')
	box_data = data.groupBy(["PULocationID"]).agg(F.min("speed").alias("min"),\
					F.max("speed").alias("max"),\
					med.alias('median'),Q2.alias('Q2'), Q4.alias('Q4'))
	box_data.show()
	box_data.repartition(1).write.mode('overwrite').option("header", True).csv(path = 'box_plot/')

'''
Get each weekays's 24 hour average speed
'''
def day_speed(data):
	data = data.groupBy(["hour","weekday"]).agg(F.avg("speed").alias("average")).orderBy("average")
	data.repartition(1).write.csv(path = '24hour_speed',header=True)

'''
Get top 10 and bottom 10 destination destination
'''
def best_and_worst_destination(data):
	med = F.expr('percentile_approx(speed, 0.5)')
	data = data.groupBy("DOLocationID").agg(med.alias("median"))
	worst10 = data.orderBy("median").limit(10)
	best10 = data.orderBy(data["median"].desc()).limit(10)
	worst10.repartition(1).write.option("header", True).csv(path = 'worst10')
	best10.repartition(1).write.option("header", True).csv(path = 'best10')

def main(inputs, output):

	data = spark.read.parquet(inputs)
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

	box_plot(data)
	day_speed(data)
	best_and_worst_destination(data)

if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]

	spark = SparkSession.builder.appName('speed exloration').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(inputs, output)