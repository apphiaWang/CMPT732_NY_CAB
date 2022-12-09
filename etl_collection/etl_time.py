import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary
name = "{col}_tripdata_20{y}-{m}.parquet"
color = ['yellow','green']
mon = range(1,13)
year = range(17,22) 
output_name = "/{col}_tripdata_20{y}-{m}.parquet"

def write_parquet_with_specific_file_name(sc, df, path, filename):
    df.repartition(1).write.option("header", "true").option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").parquet(path, mode='append')
    try:
        sc_uri = sc._gateway.jvm.java.net.URI
        sc_path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        file_system = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
        fs = file_system.get(sc_uri("172.29.89.207"), configuration())#need adaptation on different machines
        src_path = None
        status = fs.listStatus(sc_path(path))
        for fileStatus in status:
            temp = fileStatus.getPath().toString()
            if "part" in temp:
                src_path = sc_path(temp)
        dest_path = sc_path(path + filename)
        if fs.exists(src_path) and fs.isFile(src_path):
            fs.rename(src_path, dest_path)
            fs.delete(src_path, True)
    except Exception as e:
        raise Exception("Error renaming the part file to {}:".format(filename, e))

def main(input, output):
    # main logic starts here
    for i in range(len(color)):
        for k in range(len(year)):
            for j in range(len(mon)):
                file = name.format(col = color[i], m = "%02d"%mon[j], y = year[k])
                myoutput = output_name.format(col = color[i],m = "%02d"%mon[j], y = year[k])
                file = os.path.join(input, file)
                if os.path.exists(file):

                    # rename the column of pickup_time and dropoff_time because of difference in naming of yellow and green cabs dataset.
                    data = spark.read.parquet(file)
                    if 'lpep_pickup_datetime' in data.columns:
                        data = data.withColumnRenamed('lpep_pickup_datetime','pickup_datetime').withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')
                    if 'tpep_pickup_datetime' in data.columns:
                        data = data.withColumnRenamed('tpep_pickup_datetime','pickup_datetime').withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')

                    # add columns needed for analysis
                    data = data.withColumn('year', functions.year(data['pickup_datetime']))
                    data = data.withColumn('month', functions.month(data['pickup_datetime']))
                    data = data.withColumn('monthday', functions.dayofmonth(data['pickup_datetime']))
                    data = data.withColumn('weekday', functions.dayofweek(data['pickup_datetime'])) # Sunday-1, Saturday-7
                    data = data.withColumn('hour', functions.hour(data['pickup_datetime']))
                    data = data.withColumn('date', functions.to_date(data['pickup_datetime'])) 

                    # Delete records with a year less than 2017 or greater than 2021.
                    # Delete records with a total amount less than or equal to 2.5.
                    # Delete records with LocationID = 264 or 265, which indicates an unknown location.
                    # add duration column and Delete records with a duration less than 0min and greater than or equal to 100min
                    # add speed column with trip_distance/duration and Delete records with speeds less than or equal to 0 and greater than or equal to 100km/h
                    # data = data.filter(data['year'] >= 2017)
                    # data = data.filter(data['year'] <= 2021)
                    data = data.filter(data['year'] == (2000 + year[k]))
                    data = data.filter(data['total_amount'] > 2.5)
                    data = data.filter(data['PULocationID'] != 264).filter(data['DOLocationID'] != 265).\
                        filter(data['PULocationID'] != 264).filter(data['DOLocationID'] != 265)
                    data = data.withColumn('duration', data['dropoff_datetime'].cast("long") - data['pickup_datetime'].cast("long"))
                    data = data.filter(data['duration'] > 0).filter(data['duration'] < 6000) # in 100 min
                    data = data.withColumn('speed', data['trip_distance'] / (data['duration'] / 3600))
                    data = data.filter(data['speed'] < 100).filter(data['speed'] > 0)
                    
                    # drop columns not needed for analysis
                    if 'trip_type' in data.columns:
                        data = data.drop(data['trip_type'])
                    if 'congestion_surcharge' in data.columns:
                        data = data.drop(data['congestion_surcharge']).drop(data['airport_fee'])
                    del_useless = data.drop(data['RatecodeID']).drop(data['PULocationID']).drop(data['DOLocationID'])\
                    .drop(data['fare_amount']).drop(data['extra']).drop(data['total_amount']).drop(data['trip_distance'])\
                    .drop(data['mta_tax']).drop(data['tip_amount']).drop(data['tolls_amount']).drop(data['speed'])\
                    .drop(data['payment_type']).drop(data['improvement_surcharge'])
                    
                    write_parquet_with_specific_file_name(spark.sparkContext, del_useless, output, myoutput)
                    

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Time Etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)