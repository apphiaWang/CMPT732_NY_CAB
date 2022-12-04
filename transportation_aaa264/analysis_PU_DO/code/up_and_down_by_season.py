import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary
name = "{DOPU}_{col}_tripdata_20{y}-{m}.parquet"
dopu = ['DO','PU']
color = ['yellow','green']
season = [3,6,9,12]
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

    # main logic starts here
    doschema = types.StructType([
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("DOID_count", types.IntegerType(), True)])
    puschema = types.StructType([
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("PUID_count", types.IntegerType(), True)])
    l = [(-1, -1)]
    rdd = sc.parallelize(l)
    
    for j in range(len(season)):
        doform = spark.createDataFrame(rdd,doschema)
        puform = spark.createDataFrame(rdd,puschema)
        for k in range(len(year)):
            for i in range(len(color)):
                for dp in range(len(dopu)):
                    #for j in range(len(mon)):
                    for bias in range(0,3):
                        file = name.format(col = color[i], m = "%02d"%(season[j]+bias), y = year[k], DOPU = dopu[dp])
                        file = input + '/' + file
                        if os.path.exists(file):
                            parquetFile = spark.read.parquet(file)
                            if dp == 0:
                                doform = doform.union(parquetFile)
                            else:
                                puform = puform.union(parquetFile)

        doform = doform.groupby(doform['DOLocationID']).sum('DOID_count').drop(doform['DOID_count']).withColumnRenamed('sum(DOID_count)','DOID_count').filter("DOID_count > 0").orderBy('DOID_count',ascending = False)
        write_parquet_with_specific_file_name(spark.sparkContext, doform, output, "/DO_season_{}.parquet".format(int(season[j]/3)))
        puform = puform.groupby(puform['PULocationID']).sum('PUID_count').drop(puform['PUID_count']).withColumnRenamed('sum(PUID_count)','PUID_count').filter("PUID_count > 0").orderBy('PUID_count',ascending = False)               
        write_parquet_with_specific_file_name(spark.sparkContext, puform, output, "/PU_season_{}.parquet".format(int(season[j]/3)))
                

if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('D_U_season').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input,output)