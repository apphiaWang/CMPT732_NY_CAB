import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions, types

'''
Get trip count of every year
'''
def yearly_trip_count(trip):
    trip0 = trip.groupBy(trip['year']).count()
    trip0 = trip0.orderBy('year')
    trip0.write.mode('overwrite').option("header", True).csv(path = outputs + '/year')


'''
Get trip count of every month
'''
def monthly_trip_count(trip):
    trip1 = trip.groupBy(trip['month']).count()
    trip1 = trip1.orderBy('month')
    trip1.write.mode('overwrite').option("header", True).csv(path = outputs + '/month')


'''
Get trip count of every weekday
'''
def weekday_trip_count(trip):   
    trip2 = trip.groupBy(trip['weekday']).count()
    trip2 = trip2.orderBy('weekday')
    trip2.write.mode('overwrite').option("header", True).csv(path = outputs + '/weekday')


'''
Get trip count of every day
'''
def daily_trip_count(trip):   
    trip3 = trip.groupBy(trip['date']).count()
    trip3 = trip3.orderBy('date')
    trip3.write.mode('overwrite').option("header", True).csv(path = outputs + '/date')


'''
Get trip count of every hour
'''
def hourly_trip_count(trip):
    trip4 = trip.groupBy(trip['hour']).count()
    trip4 = trip4.orderBy('hour')
    trip4.write.mode('overwrite').option("header", True).csv(path = outputs + '/hour')


def main(inputs, outputs):
    # main logic starts here
    trip = spark.read.parquet(inputs)

    # trip.show()
    yearly_trip_count(trip)
    monthly_trip_count(trip)
    weekday_trip_count(trip)
    daily_trip_count(trip)
    hourly_trip_count(trip)

    #trip.write.csv(outputs, mode='overwrite')
    #trip.write.option("header",True).csv(outputs, mode='overwrite')
  

   
                    

if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('analyse time').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, outputs)