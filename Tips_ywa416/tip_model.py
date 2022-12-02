import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types, functions
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor, DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def main(inputs, model_file):
    # prepare data
    data = spark.read.parquet(inputs)
    train, validation = data.randomSplit([0.75, 0.25])
    # create model 
    day_transformer = SQLTransformer(
        statement="""SELECT PULocationID, DOLocationID, tip_amount/(total_amount-tip_amount)*100 as tip_ratio,
                            ceil(20*tip_amount/(total_amount - tip_amount)) as tip_range_index,
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
                        AND tip_amount/(total_amount-tip_amount) < 0.5
                    """
    )
    test = day_transformer.transform(train)

    feature_assembler = VectorAssembler(outputCol="features").setHandleInvalid("skip")
    # feature_assembler.setInputCols([ "hour","PULocationID", "DOLocationID", "Congestion_Surcharge", "Airport_fee", "Extra", "MTA_tax", "Improvement_surcharge", \
    #     "Tolls_amount", "fare_amount"])
    feature_assembler.setInputCols([ "month", "day", "hour", "PULocationID", "DOLocationID", "trip_distance", "duration", "other_amount", "fare_amount"])
    estimator = DecisionTreeRegressor(featuresCol="features", labelCol="tip_ratio")
    pipeline = Pipeline(stages=[day_transformer, feature_assembler, estimator])
    model = pipeline.fit(train)
    # evaluate performance
    evaluator = RegressionEvaluator(labelCol="tip_ratio")
    train_result = model.transform(train)
    train_rmse = evaluator.evaluate(train_result)
    train_r2 = evaluator.evaluate(train_result, {evaluator.metricName: "r2"})
    print('Training score for GBT model:')
    print('r2 =', train_r2)
    print('rmse =', train_rmse)
    prediction = model.transform(validation)
    val_rmse = evaluator.evaluate(prediction)
    val_r2 = evaluator.evaluate(prediction, {evaluator.metricName: "r2"})
    print('Validation score for GBT model:')
    print('r2 =', val_r2)
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
