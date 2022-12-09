import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession

def write_boxplot_data(outputs, feature):
    '''
    Generate data for drawing boxplot https://en.wikipedia.org/wiki/Box_plot
    '''
    box = spark.sql("""
        SELECT tip_range_index, min({feature}) as min, percentile_approx({feature}, 0.25) as q1, mean({feature}) as mean, 
            percentile_approx({feature}, 0.5) as median,  percentile_approx({feature}, 0.75) as q3, max({feature}) as max
         FROM data group by tip_range_index ORDER BY 1, 2
    """.format(feature=feature))
    box.write.option("header",True).csv('%s/%s'%(outputs, feature), mode='overwrite')

def main(inputs, tip):
    # spark.read.option("header",True).csv(inputs).createOrReplaceTempView("data")
    
    spark.read.parquet(inputs).createOrReplaceTempView("data")
    spark.sql("""
        SELECT * FROM data 
        WHERE tip_amount > {tip}
    """.format(tip = tip)).show()
    # spark.sql("""
    #     select year(date) as year, sum(mean_percent*count)/sum(count) as mean_tip_percent from data group by 1
    # """).show()
    return
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
            AND fare_amount >= 2.5
            AND trip_distance > 0 AND trip_distance < 180
            AND year(pickup_datetime) < 2022 AND year(pickup_datetime) > 2016
            AND VendorID < 3)
        SELECT tip_ratio, tip_amount, other_fare_ratio, year, month, date, duration,
            trip_distance, PULocationID, DOLocationID,
            CASE WHEN tip_index <= 8 THEN tip_index
                    ELSE 9 END  as tip_range_index
        FROM tb
    """).createOrReplaceTempView("data")
    spark.sql("""
        SELECT mean(tip_ratio) FROM data
    """).show()
    
    return


if __name__ == '__main__':  
    inputs = sys.argv[1]
    tip = sys.argv[2]
    spark = SparkSession.builder.appName('Tips Distribution Analysis').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, tip)