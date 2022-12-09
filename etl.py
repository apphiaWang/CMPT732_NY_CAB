import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions, types

import numpy as np

# add more functions as necessary
name = "{col}_tripdata_20{y}-{m}.parquet"
color = ['yellow','green']
mon = range(1,13)
year = range(17,22)
output_name = "/{col}_tripdata_20{y}-{m}.parquet.gzip"

def write_parquet_with_specific_file_name(sc, df, path, filename):
    df.repartition(1).write.option("header", "true").option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")\
        .parquet(path, compression='gzip', mode='append')
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

def main(inputs, outputs):
    # main logic starts here
    for i in range(len(color)):
        for k in range(len(year)):
            for j in range(len(mon)):
                
                file = name.format(col = color[i], m = "%02d"%mon[j], y = year[k])
                print(file)
                myoutput = output_name.format(col = color[i],m = "%02d"%mon[j], y = year[k])
                path = inputs+"/"+file
                if os.path.exists(path):
                    raw_data = spark.read.parquet(path)
                    raw_data = raw_data.drop(raw_data['store_and_fwd_flag'])
                    if 'ehail_fee' in raw_data.columns:
                        raw_data = raw_data.drop(raw_data['ehail_fee']).drop(raw_data['congestion_surcharge'])
                    if 'lpep_pickup_datetime' in raw_data.columns:
                        raw_data = raw_data.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")\
                                           .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
                    if 'tpep_pickup_datetime' in raw_data.columns:
                        raw_data = raw_data.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")\
                                           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
                    valid_data = raw_data.filter(raw_data['payment_type'] < 2).filter(raw_data['trip_distance'] > 0).filter(raw_data['fare_amount'] >= 2.5)                                     
                    valid_data = valid_data.select('VendorID','pickup_datetime','dropoff_datetime','trip_distance',\
                                        # "Extra", "MTA_tax", "Improvement_surcharge", "Tolls_amount",\
                                        # "Congestion_Surcharge", "Airport_fee",\
                                        'PULocationID', 'DOLocationID', 'fare_amount','tip_amount','total_amount',\
                                            "passenger_count",
                                        valid_data['payment_type'].cast('int').alias("payment_type"))
                    write_parquet_with_specific_file_name(spark.sparkContext, valid_data, outputs, myoutput)
                   
if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('Initial ETL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, outputs)