import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession

def main(inputs, outputs):
    spark.read.parquet(inputs).select('passenger_count', 'VendorID','pickup_datetime','dropoff_datetime','trip_distance',\
        'PULocationID', 'DOLocationID', 'fare_amount','tip_amount','total_amount', 'payment_type')\
        .createOrReplaceTempView("data")
    spark.sql("""
        SELECT *, tip_amount/(total_amount - tip_amount) as tip_ratio, total_amount - tip_amount - fare_amount as other_amount,
            ceil(20*tip_amount/(total_amount - tip_amount)) as tip_range_index,
            to_date(pickup_datetime) as date,
            month(pickup_datetime) as month, year(pickup_datetime) as year
        FROM data 
        WHERE BIGINT(dropoff_datetime - pickup_datetime)/60 <= 180
            AND payment_type = 1
            AND fare_amount >= 2.5
            AND trip_distance > 0
            AND year(pickup_datetime) < 2022 AND year(pickup_datetime) > 2016
            AND VendorID < 3
            AND Trip_distance < 180
    """).createOrReplaceTempView("data")

    other_amount = spark.sql("""
        with tb as (SELECT tip_ratio, other_amount/(total_amount - tip_amount) as other_amount,
                CASE WHEN tip_range_index <= 8 THEN tip_range_index * 5
                    ELSE 100 END  as tip_range_index
                FROM data)
        SELECT tip_range_index, min(other_amount) as min, max(other_amount) as max, percentile_approx(other_amount, 0.25) as q1, mean(other_amount) as mean, percentile_approx(other_amount, 0.5) as median,  percentile_approx(other_amount, 0.75) as q3
         FROM tb group by tip_range_index ORDER BY 1, 2
    """)
    other_amount.write.option("header",True).csv('%s/other_amount'%outputs, mode='overwrite')
    passenger_count = spark.sql("""
        with tb as (SELECT tip_ratio, passenger_count,
                CASE WHEN tip_range_index <= 8 THEN tip_range_index * 5
                    ELSE 100 END  as tip_range_index
                FROM data)
        SELECT tip_range_index, min(passenger_count) as min, max(passenger_count) as max, percentile_approx(passenger_count, 0.25) as q1, mean(passenger_count) as mean, percentile_approx(passenger_count, 0.5) as median,  percentile_approx(passenger_count, 0.75) as q3
         FROM tb group by tip_range_index ORDER BY 1, 2
    """)
    passenger_count.write.option("header",True).csv('%s/passenger_count'%outputs, mode='overwrite')


if __name__ == '__main__':  
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('Tips features Analysis').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, outputs)