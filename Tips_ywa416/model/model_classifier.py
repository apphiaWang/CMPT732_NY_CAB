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
        statement="""WITH data as (SELECT PULocationID, DOLocationID, tip_amount/(total_amount-tip_amount)*100 as tip_ratio,
                            ceil(10*tip_amount/(total_amount - tip_amount)) as tip_range_index,
                            bigint(dropoff_datetime) - bigint(pickup_datetime)/60 as duration,
                            dayofweek(pickup_datetime) as day,
                            hour(pickup_datetime) as hour, month(pickup_datetime) as month, year(pickup_datetime) as year,
                            trip_distance/(bigint(dropoff_datetime) - bigint(pickup_datetime))*60*60 as avg_speed,
                            trip_distance, fare_amount, (total_amount-fare_amount-tip_amount)/total_amount as other_amount
                    FROM __THIS__
                    WHERE BIGINT(dropoff_datetime - pickup_datetime)/60 <= 180
                        AND payment_type = 1
                        AND year(pickup_datetime) < 2022 AND year(pickup_datetime) > 2016
                        AND VendorID < 3
                        AND tip_amount/(total_amount-tip_amount) <= 0.4
                        AND fare_amount BETWEEN 2.5 + 2 * trip_distance AND 2.5 + 3.5 * trip_distance
                        AND total_amount <= 120 AND trip_distance <= 20 
                        AND total_amount - tip_amount - fare_amount <= 20
                    )
                    SELECT tip_ratio, PULocationID, DOLocationID, other_amount, fare_amount, hour, day, duration, trip_distance,
                        CASE WHEN tip_range_index <= 2 THEN int(tip_range_index)
                            ELSE 3 END  as tip_range_index
                        FROM data
                    """
    )
    feature_assembler = VectorAssembler(outputCol="features").setHandleInvalid("skip")
    feature_assembler.setInputCols([  "day", "hour", "PULocationID", "DOLocationID",\
         "trip_distance", "duration", "other_amount", "fare_amount"])
    estimator = RandomForestClassifier(featuresCol="features", labelCol="tip_range_index")
    pipeline = Pipeline(stages=[day_transformer, feature_assembler, estimator])
    model = pipeline.fit(train)
    # evaluate performance
    evaluator = MulticlassClassificationEvaluator(labelCol="tip_range_index")
    train_result = model.transform(train)
    train_rmse = evaluator.evaluate(train_result)
    print('Training score for classifier:')
    print('score =', train_rmse)
    prediction = model.transform(validation)
    val_rmse = evaluator.evaluate(prediction)
    print('Validation score for classifier:')
    print('rmse =', val_rmse)
    print(model.stages[-1].featureImportances)
    # output model
    model.write().overwrite().save(model_file)

if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    spark = SparkSession.builder.appName('Predict Tips Range') \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, model_file)
