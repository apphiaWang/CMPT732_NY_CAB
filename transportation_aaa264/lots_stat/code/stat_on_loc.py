import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary
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

def main(inputs, output):
    # main logic starts here
    
    lots = spark.read.parquet(inputs).cache() 
    nums = lots.groupby(lots['ID']).count().orderBy('count',ascending = False).withColumnRenamed('count','lot_nums')
    nums_of_lots = "/nums_of_lots.parquet"
    sizes = lots.groupby(lots['ID']).sum('SHAPE_Area').withColumnRenamed('sum(SHAPE_Area)','lot_area').orderBy('lot_area',ascending = False)
    sizes_of_lots = "/sizes_of_lots.parquet"
    write_parquet_with_specific_file_name(spark.sparkContext, nums, output, nums_of_lots)
    write_parquet_with_specific_file_name(spark.sparkContext, sizes, output, sizes_of_lots)
    #nums.write.parquet(output, mode='overwrite')
    #sizes.write.parquet(output, mode='overwrite')
    #joined.write.parquet(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)

