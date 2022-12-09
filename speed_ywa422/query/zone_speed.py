from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt

etl_path = os.path.join(os.path.dirname(__file__) , "../ETL")
sys.path.append(etl_path) #absolute path of ETL package
from ETL import read_ETL

'''
Run command
spark-submit zone_speed.py ../data ../data/test_gen_data 

Function
This part aims to generate new feature for the speed prediction part.
Find out the commute situation between each zone i.e. the frequency from one zone to
another during a specific hour in a specific weekday.
'''

'''
Generate each zone's commute average speed in each hour of each weekday.
'''
def hour_day_zone_avgspeed(data, output):
	data = data.groupBy(["hour", "weekday", "PULocationID", "DOLocationID"]).agg(F.avg("speed").alias("average_speed"))
	data.repartition(1).write.mode('overwrite').option("header", True).csv(path = output + '/hour_day_zone_avgspeed')
	return data

def hour_day_zone_freq(data, output):
	data = data.groupBy(["hour", "weekday", "PULocationID", "DOLocationID"]).count()
	data.repartition(1).write.mode('overwrite').option("header", True).csv(path = output + '/hour_day_zone_freq')

'''
Generate each hour and day zone commute freq
'''
def hour_freq(data, output):
	data = data.groupBy(["hour", "weekday"]).count()
	data.repartition(1).write.mode('overwrite').option("header",True).csv(path = output + '/hour_day_commute_freq')


'''
Generate each zone's commute total times
'''
def zone_freq(data, output):
	data = data.groupBy(["PULocationID", "DOLocationID"]).count()
	data.repartition(1).write.mode('overwrite').option("header", True).csv(path = output + '/zone_commute_freq')

'''
Generate each zone's total commute times in each hour of each weekday.
'''
def count_freq(data, output):
	data = data.groupBy(["hour", "weekday", "PULocationID", "DOLocationID"]).count()
	data.repartition(1).write.mode('overwrite').option("header", True).csv(path = output + '/hour_day_zone_commute_freq')
	return data

'''
To see if speed is correlated to the frequncy of commute traffic between each zone and each hour in the week
               average_speed     count
average_speed       1.000000 -0.209363
count              -0.209363  1.000000
The results show that there is negative correlation
'''
def see_connection(data, output):
	speed_data = hour_day_zone_avgspeed(data,output)
	freq_data = count_freq(data, output)

	data = speed_data.alias("a").join(freq_data.alias("b"), [speed_data["hour"] == freq_data["hour"] , speed_data["weekday"] == freq_data["weekday"] ,\
		speed_data["PULocationID"] == freq_data["PULocationID"] , speed_data["DOLocationID"] == freq_data["DOLocationID"]])
	data = data.select("a.hour", "a.weekday", "a.PULocationID", "a.DOLocationID", "average_speed", "count")
	data = data.select("average_speed","count")
	data = data.withColumn("average_speed", data["average_speed"].cast("double")).\
		withColumn("count", data["count"].cast("double"))

	data = data.toPandas()
	print(data.corr())
	data.plot.scatter(y="average_speed", x = "count")
	plt.savefig('../figure/test_gen_output')

def main(inputs, output):
	data, _ = read_ETL(inputs,output)

	hour_day_zone_avgspeed(data, output)
	hour_day_zone_freq(data, output)
	hour_freq(data, output)
	zone_freq(data, output)
	count_freq(data, output)
	see_connection(data,output)

if __name__ == '__main__':		
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('explore zone and speed').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(inputs, output)