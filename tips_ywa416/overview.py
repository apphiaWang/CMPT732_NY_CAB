import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os
from pyspark.sql import SparkSession

def main(inputs, outputs):
    spark.read.parquet(inputs).select('VendorID','pickup_datetime','dropoff_datetime','trip_distance',\
        'PULocationID', 'DOLocationID', 'fare_amount','tip_amount','total_amount', 'payment_type')\
        .createOrReplaceTempView("data")

    # filter unwated records and generate wanted features
    spark.sql("""
        WITH tb AS (SELECT *, tip_amount/(total_amount - tip_amount) as tip_ratio, 
                ceil(20*tip_amount/(total_amount - tip_amount)) as tip_index,
                (total_amount-tip_amount-fare_amount)/(total_amount - tip_amount) as other_fare_ratio,
                to_date(pickup_datetime) as date,
                bigint(dropoff_datetime) - bigint(pickup_datetime)/60 as duration,
                month(pickup_datetime) as month, year(pickup_datetime) as year
            FROM data 
            WHERE BIGINT(dropoff_datetime - pickup_datetime)/60 <= 180
                AND payment_type = 1
                AND fare_amount BETWEEN 2.5 + 2 * trip_distance AND 2.5 + 3.5 * trip_distance
                AND trip_distance > 0 AND trip_distance < 180
                AND year(pickup_datetime) < 2022 AND year(pickup_datetime) > 2016
                AND VendorID < 3
            )
        SELECT tip_ratio, tip_amount, other_fare_ratio, year, month, date, duration,
            trip_distance, PULocationID, DOLocationID,
            CASE WHEN tip_index <= 8 THEN tip_index
                    ELSE 9 END  as tip_range_index
        FROM tb
    """).createOrReplaceTempView("data")

    # print the yearly mean and median
    spark.sql("""
        SELECT year, mean(tip_ratio) * 100 as mean_percent, percentile_approx(tip_ratio, 0.5)*100 as median_percent FROM data
        GROUP BY year ORDER BY year    """).show()

    # distribution of tips over year
    spark.sql("""
        SELECT year, tip_range_index, count(*) as count FROM data group by year, tip_range_index
    """).createOrReplaceTempView("distribution")
    distribution = spark.sql("""
        SELECT year, tip_range_index, count, count/total as ratio_of_year FROM 
         (SELECT year, tip_range_index, count, SUM(count) OVER(PARTITION BY year) AS total FROM distribution)
        ORDER BY 1, 2
    """)
    distribution.write.option("header",True).csv('%s/distribution'%outputs, mode='overwrite')
    
    # this part is omitted when running in AWS 
    tip_dict = os.path.join(os.path.dirname(__file__), 'data/tip_range_dict.csv')
    spark.read.option("header",True).csv(tip_dict).createOrReplaceTempView("tip_range")
    spark.sql("""
    SELECT t.year, r.range, t.count, t.ratio_of_year
    FROM(   SELECT year, tip_range_index, count, count/total as ratio_of_year FROM 
         (SELECT year, tip_range_index, count, SUM(count) OVER(PARTITION BY year) AS total FROM distribution)
         ) AS t, tip_range as r
    WHERE t.tip_range_index = r.tip_range_index
    ORDER BY t.year, t.tip_range_index
    """).write.option("header",True).csv('%s/distribution-rangemapped'%outputs, mode='overwrite')

    # mean, median, max of dates
    daily = spark.sql("""
        SELECT year, date, mean(tip_ratio)*100 as mean_percent, percentile_approx(tip_ratio, 0.5)*100 as median_percent, 
                max(tip_amount) as max_amount, count(*) as count
        FROM data 
        GROUP BY year, date
        order by 1, 2
    """)
    daily.write.partitionBy("year").option("header",True).csv('%s/daily'%outputs, mode='overwrite')

if __name__ == '__main__':  
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('Overview Analysis of Tip').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, outputs)