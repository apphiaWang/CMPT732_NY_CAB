import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types, functions
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main(inputs, model_file):
    # prepare data
    data = spark.read.parquet(inputs)
    train, validation = data.randomSplit([0.75, 0.25])
    # create model 
    day_transformer = SQLTransformer(
        statement="""WITH data as (SELECT passenger_count, PULocationID, DOLocationID, tip_amount/(total_amount-tip_amount)*100 as tip_ratio,
                            ceil(10*tip_amount/(total_amount - tip_amount)) as tip_range_index,
                            bigint(dropoff_datetime) - bigint(pickup_datetime)/60 as duration,
                            dayofweek(pickup_datetime) as day,
                            hour(pickup_datetime) as hour, month(pickup_datetime) as month, year(pickup_datetime) as year,
                            trip_distance/(bigint(dropoff_datetime) - bigint(pickup_datetime))*60*60 as avg_speed,
                            trip_distance, fare_amount, total_amount-fare_amount-tip_amount as other_amount
                    FROM __THIS__
                    WHERE BIGINT(dropoff_datetime - pickup_datetime)/60 <= 180
                        AND payment_type = 1
                        AND fare_amount >= 2.5
                        AND trip_distance > 0
                        AND year(pickup_datetime) < 2022 AND year(pickup_datetime) > 2016
                        AND VendorID < 3
                        AND Trip_distance < 180
                        AND tip_amount/(total_amount-tip_amount) < 0.4)
                    SELECT tip_ratio, PULocationID, DOLocationID, other_amount, fare_amount, hour, day, duration, passenger_count, trip_distance,
                        CASE WHEN tip_range_index <= 8 THEN int(tip_range_index)
                            ELSE 10 END  as tip_range_index
                        FROM data
                    """
    )
    test = day_transformer.transform(train)
    test.show(1)
    feature_assembler = VectorAssembler(outputCol="features").setHandleInvalid("skip")
    # feature_assembler.setInputCols([ "hour","PULocationID", "DOLocationID", "Congestion_Surcharge", "Airport_fee", "Extra", "MTA_tax", "Improvement_surcharge", \
    #     "Tolls_amount", "fare_amount"])
    feature_assembler.setInputCols([ "passenger_count", "day", "hour", "PULocationID", "DOLocationID",\
         "trip_distance", "duration", "other_amount", "fare_amount"])
    estimator = MultilayerPerceptronClassifier(featuresCol="features", labelCol="tip_range_index", layers=[9, 10])
    # estimator = RandomForestClassifier(featuresCol="features", labelCol="tip_range_index")
    estimator.setMaxIter(500)
    pipeline = Pipeline(stages=[day_transformer, feature_assembler, estimator])
    model = pipeline.fit(train)
    # evaluate performance
    evaluator = MulticlassClassificationEvaluator(labelCol="tip_range_index")
    train_result = model.transform(train)
    train_rmse = evaluator.evaluate(train_result)
    print('Training score for GBT model:')
    # print('r2 =', train_r2)
    print('rmse =', train_rmse)
    prediction = model.transform(validation)
    val_rmse = evaluator.evaluate(prediction)
    # val_r2 = evaluator.evaluate(prediction, {evaluator.metricName: "r2"})
    print('Validation score for GBT model:')
    # print('r2 =', val_r2)
    print('rmse =', val_rmse)
    print(model.stages[-1].featureImportances)
    # output model
    model.write().overwrite().save(model_file)

if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    spark = SparkSession.builder.appName('Predict Tips') \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, model_file)
