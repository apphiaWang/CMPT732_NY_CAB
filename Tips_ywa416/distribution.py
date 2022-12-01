import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession

def main(inputs, outputs):
    spark.read.parquet(inputs).select('VendorID','pickup_datetime','dropoff_datetime','trip_distance',\
        'PULocationID', 'DOLocationID', 'fare_amount','tip_amount','total_amount', 'payment_type')\
        .createOrReplaceTempView("data")
    spark.sql("""
        SELECT *, tip_amount/fare_amount as tip_ratio, ceil(20*tip_amount/fare_amount) as tip_range_index, to_date(pickup_datetime) as date,
            month(pickup_datetime) as month, year(pickup_datetime) as year
        FROM data 
        WHERE BIGINT(dropoff_datetime - pickup_datetime)/60 <= 180
            AND payment_type = 1
            AND fare_amount >= 2.5
            AND trip_distance > 0
            AND year(pickup_datetime) < 2022 AND year(pickup_datetime) > 2016
            AND VendorID < 3
    """).createOrReplaceTempView("data")
    distribution = spark.sql("""
        with tb as (SELECT tip_ratio, year,
                CASE WHEN tip_range_index <= 8 THEN tip_range_index * 5
                    ELSE 100 END  as tip_range_index
                FROM data)
        SELECT year, tip_range_index, count(*) as count FROM tb group by year, tip_range_index ORDER BY 1, 2
    """)
    distribution.write.option("header",True).csv('%s/distribution'%outputs, mode='overwrite')
    monthly = spark.sql("""
        SELECT year, month, mean(tip_ratio)*100 as mean_percent, percentile_approx(tip_ratio, 0.5)*100 as median_percent,
                max(tip_amount) as max_tip, count(*) as count
        FROM data 
        GROUP BY year, month
        order by 1, 2
    """)
    monthly.write.option("header",True).csv('%s/monthly'%outputs, mode='overwrite')
    daily = spark.sql("""
        SELECT year, date, mean(tip_ratio)*100 as mean_percent, percentile_approx(tip_ratio, 0.5)*100 as median_percent, 
                max(tip_amount) as max_tip, count(*) as count
        FROM data 
        GROUP BY year, date
        order by 1, 2
    """)
    daily.write.partitionBy("year").option("header",True).csv('%s/daily'%outputs, mode='overwrite')

if __name__ == '__main__':  
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('Tips Distribution Analysis').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, outputs)