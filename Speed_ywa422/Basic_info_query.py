from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F


import sys
from ETL import read_ETL

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
	box_data.repartition(1).write.mode('overwrite').csv(path = 'box_plot/')

'''
Get each weekays's 24 hour average speed
'''
def day_speed(data):
	day_data = data.withColumn("hour", F.hour(data['pickup_datetime']))
	day_data = day_data.groupBy(["hour","weekday"]).agg(F.avg("speed").alias("average")).orderBy("average")
	day_data.repartition(1).write.csv(path = '24hour_speed',header=True)

'''
Get top 10 and bottom 10 destination destination
'''
def best_and_worst_destination(data):
	med = F.expr('percentile_approx(speed, 0.5)')
	data = data.groupBy("DOLocationID").agg(med.alias("median"))
	worst10 = data.orderBy("median").limit(10)
	best10 = data.orderBy(data["median"].desc()).limit(10)
	worst10.repartition(1).write.csv(path = 'worst10', header = True)
	best10.repartition(1).write.csv(path = 'best10', header = True)

def main(inputs, output):

	data, loc_data = read_ETL(inputs, output)
	
	# box_plot(data)
	day_speed(data)
	# best_and_worst_destination(data)

if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('speed exloration').getOrCreate()
	spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(inputs, output)