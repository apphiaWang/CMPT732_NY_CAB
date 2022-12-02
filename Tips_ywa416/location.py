import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions, types

import numpy as np


@functions.udf(returnType=types.StringType())
def get_filename(s):
    return s.split("/")[-1].split(".")[0]

def combine_location(df="data"):
    """
        this function takes df as the input dataframe tempview name
            the dataframe is generated in the main function
        produces a table of year, month, locationID, the location's of the year and month
             average tip_ratio, count of trip, and max tip amount given
        ***
        if a ride starts or ends at a location, we say the trip occurs at the location.
    """
    spark.sql("""
        SELECT pu.year, pu.month, pu.PULocationID as locationID, (pu.avg * pu.n + do.avg * do.n - same.n * same.avg) / (pu.n + do.n - same.n) as avg, 
                    (pu.n + do.n -same.n) as count, CASE WHEN pu.max > do.max THEN pu.max ELSE do.max END AS max
            FROM  (SELECT year, month, PULocationID, mean(tip_ratio) as avg, count(*) as n, max(tip_amount) as max FROM {df} GROUP BY year, month, PULocationID) as pu, 
                (SELECT year, month, DOLocationID, mean(tip_ratio) as avg, count(*) as n, max(tip_amount) as max FROM {df} GROUP BY year, month, DOLocationID) as do, 
                (SELECT year, month, PULocationID, mean(tip_ratio) as avg, count(*) as n FROM {df} WHERE PULocationID = DOLocationID GROUP BY year, month, PULocationID) as same
            WHERE pu.year = do.year and pu.month = do.month and pu.PULocationID = do.DOLocationID 
                and pu.year = same.year and pu.month = same.month and pu.PULocationID = same.PULocationID 
    """.format(df=df)).createOrReplaceTempView("total")

def main(inputs, outputs):
    spark.read.parquet(inputs).select('VendorID','pickup_datetime','dropoff_datetime','trip_distance',\
        'PULocationID', 'DOLocationID', 'fare_amount','tip_amount','total_amount', 'payment_type')\
        .createOrReplaceTempView("data")
    spark.sql("""
        SELECT *, tip_amount/(total_amount - tip_amount) as tip_ratio, month(pickup_datetime) as month, year(pickup_datetime) as year
        FROM data 
        WHERE BIGINT(dropoff_datetime - pickup_datetime)/60 <= 180
            AND payment_type = 1
            AND fare_amount >= 2.5
            AND trip_distance > 0
            AND trip_distance < 180
            AND year(pickup_datetime) < 2022 AND year(pickup_datetime) > 2016
            AND VendorID < 3
    """).createOrReplaceTempView("data")
    combine_location()

    monthly_location = spark.sql("""
        with tb as (SELECT *,
            mean(count) OVER(PARTITION BY year, month) AS avg_count  FROM total)
        SELECT year, month, locationID, avg, max, count, avg_count  FROM tb where count > avg_count ORDER BY year, month, avg DESC
    """)
    
    # percentile_approx(count, 0.5) OVER(PARTITION BY year, month) AS median_count

    monthly_location.write.option("header",True).csv('%s/monthly_location'%outputs, mode='overwrite')
    total_location = spark.sql("""
        SELECT locationID, sum(avg*count)/sum(count) as avg, max(max) as max, sum(count) as count  
        FROM total 
        GROUP BY locationID
        ORDER BY 2
    """)
    total_location.write.option("header",True).csv('%s/total'%outputs, mode='overwrite')
    total_location.createOrReplaceTempView("total_location")
    spark.sql("""
        SELECT * FROM data WHERE tip_amount = 0
    """).createOrReplaceTempView("petty")
    combine_location("petty")
    petty_location = spark.sql("""
        WITH petty_location AS (SELECT locationID, sum(count) as count FROM total GROUP BY locationID)
        SELECT p.locationID, p.count as count, p.count / t.count as 0_tip_ratio
        FROM petty_location as p, total_location as t
        WHERE p.locationID = t.locationID
        ORDER BY p.count DESC
    """)
    petty_location.write.option("header",True).csv('%s/petty_location'%outputs, mode='overwrite')
    
    spark.sql("""
        SELECT * FROM data WHERE tip_ratio >= 0.4
    """).createOrReplaceTempView("generous")
    combine_location("generous")
    petty_location = spark.sql("""
        WITH generous_location AS (SELECT locationID, sum(count) as count FROM total GROUP BY locationID)
        SELECT p.locationID, p.count as count, p.count / t.count as 0_tip_ratio
        FROM generous_location as p, total_location as t
        WHERE p.locationID = t.locationID
        ORDER BY p.count DESC
    """)
    petty_location.write.option("header",True).csv('%s/generous'%outputs, mode='overwrite')
   
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('Tips and Location').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, outputs)