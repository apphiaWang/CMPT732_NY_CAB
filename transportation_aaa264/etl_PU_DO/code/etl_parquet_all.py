import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions, types

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
        fs = file_system.get(sc_uri("10.0.0.148"), configuration())#need adaptation on different machines
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

def main(input,output):
    for i in range(len(color)):
        for k in range(len(year)):
            for j in range(len(mon)):
                file = name.format(col = color[i], m = "%02d"%mon[j], y = year[k])
                myoutput = output_name.format(col = color[i],m = "%02d"%mon[j], y = year[k])
                filepath = input+'/'+file
                if os.path.exists(filepath):
                    parquetFile = spark.read.parquet(filepath)
                    del_nons = parquetFile.drop(parquetFile['VendorID']).drop(parquetFile['store_and_fwd_flag'])
                    if 'ehail_fee' in del_nons.columns:
                        del_nons = del_nons.drop(parquetFile['ehail_fee']).drop(parquetFile['congestion_surcharge'])
                    after_filt = del_nons.filter((del_nons['payment_type'] == 1) | (del_nons['payment_type'] == 2))
                    after_filt = after_filt.filter(after_filt['trip_distance'] > 0).filter(after_filt['total_amount'] > 0)
                    after_filt = after_filt.select('PULocationID','DOLocationID').cache()

                    PU = after_filt.groupby(after_filt['PULocationID']).count().orderBy('count',ascending = False).withColumnRenamed('count','PUID_count')
                    PU_out =  myoutput[:1]+"/PU_"+myoutput[1:]
                    write_parquet_with_specific_file_name(spark.sparkContext, PU, output, PU_out)
                    DO = after_filt.groupby(after_filt['DOLocationID']).count().orderBy('count',ascending = False).withColumnRenamed('count','DOID_count')
                    DO_out =  myoutput[:1]+"/DO_"+myoutput[1:]
                    write_parquet_with_specific_file_name(spark.sparkContext, DO, output, DO_out)

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input,output)