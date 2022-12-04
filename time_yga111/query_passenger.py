import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions, types




def main(inputs, outputs):
    # main logic starts here
    trip = spark.read.parquet(inputs)
    '''
    Get average passenger count of every weekday
    '''   
   
    # Sunday-1, Saturday-7
    passenger = trip.groupby(trip['weekday']).agg(functions.avg(trip['passenger_count']).alias('passenger_avg'))
    
    passenger = passenger.orderBy('weekday')
    passenger.repartition(1).write.mode('overwrite').option("header", True).csv(path = outputs + '/passenger')


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