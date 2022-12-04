import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
import seaborn as sns

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
            hour(pickup_datetime) as hour, dayofweek(pickup_datetime) as day
        FROM data 
        WHERE BIGINT(dropoff_datetime - pickup_datetime)/60 <= 180
            AND payment_type = 1
            AND fare_amount >= 2.5
            AND trip_distance > 0 AND trip_distance < 180
            AND year(pickup_datetime) < 2022 AND year(pickup_datetime) > 2016
            AND VendorID < 3
            AND fare_amount BETWEEN 2.5 + 2 * trip_distance AND 2.5 + 3.5 * trip_distance 
        )
        SELECT tip_ratio, tip_amount, other_fare_ratio, trip_distance, fare_amount, total_amount,
            day, hour,
            CASE WHEN tip_index <= 8 THEN tip_index
                    ELSE 9 END  as tip_range_index
        FROM tb
    """).createOrReplaceTempView("data")

    df = spark.sql("""
        SELECT tip_range_index, tip_ratio, fare_amount, trip_distance, total_amount - tip_amount as total_fare, 
            total_amount - tip_amount - fare_amount as other_amount
        FROM data WHERE total_amount <= 120 AND trip_distance <= 20 
            AND total_amount - tip_amount - fare_amount <= 40 AND tip_ratio < 1 
    """).toPandas()
    ranges = range(0, 10)
    sns.relplot(data=df, x='total_fare', y='tip_ratio')
    plt.savefig(outputs+'/fare_tip.png')
    # sns.relplot(data=df, x='trip_distance', y='tip_ratio')
    # plt.savefig(outputs+'/distance_tip.png')
    sns.relplot(data=df, x='other_amount', y='tip_ratio')
    plt.savefig(outputs+'/other_tip.png')
    # sns.relplot(data=df, x='total_fare', y='trip_distance', hue='tip_range_index', hue_order=ranges, aspect=1.61)
    # plt.savefig(outputs+'/tip_fare_distance.png')

    df.hist(column="other_amount")
    plt.savefig(outputs+'/other.png')
    sns.relplot(data=df, x='total_fare', y='other_amount', hue='tip_range_index', hue_order=ranges, aspect=1.61)
    plt.savefig(outputs+'/tip_fare_other.png')

    # heatmap = spark.sql("""
    #     SELECT day, hour, mean(tip_ratio)*100 as mean_percent FROM data GROUP BY day, hour ORDER BY 1, 2
    # """).toPandas()
    # day_hour = heatmap.pivot(index='day', columns='hour', values='mean_percent')
    # sns.heatmap(day_hour, fmt="g", cmap='Blues')
    # plt.savefig(outputs+'/day_hour.png')


if __name__ == '__main__':  
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('Only a month').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    if not os.path.isdir(outputs):
        os.makedirs(outputs) 
    main(inputs, outputs)