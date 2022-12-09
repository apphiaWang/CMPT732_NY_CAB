import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
spark = SparkSession.builder.appName('speed prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+
import os
'''
Run Command
spark-submit speed_prediction.py ../data test_model_output

Function
Train Model with only information in the original dataset with toy dataset.
'''
etl_path = os.path.join(os.path.dirname(__file__) , "../ETL")
sys.path.append(etl_path) #absolute path of ETL package
from ETL import read_ETL

def main(inputs, output):
	data, loc_data = read_ETL(inputs, output)
	data = data.sample(True, 0.05)

	'''
	Feature Engineering
	|--Add specific pickup and dropoff location features 
	'''
	loc_data = loc_data.select("LocationID", "longitude", "latitude")
	data = data.join(loc_data, data["PULocationID"] == loc_data["LocationID"], "inner")
	data = data.withColumnRenamed("longitude", "pickup_longitude").withColumnRenamed("latitude", "pickup_latitude")
	data.drop("LocationID")
	data = data.alias("a").join(loc_data.alias("b"), F.col("a.DOLocationID") == F.col("b.LocationID"), "inner")
	data = data.withColumnRenamed("longitude", "dropoff_longitude").withColumnRenamed("latitude", "dropoff_latitude")
	data = data.withColumn("hour", F.hour(data['pickup_datetime']))

	'''
	Use GBTRegression Tree Model to predict the speed
	Use the lr and max_depth list to perform grid search for the best hyperparameters
	'''
	train, validation = data.randomSplit([0.75, 0.25])
	train = train

	lr = [0.3, 0.4, 0.5]
	max_depth = [7,8]

	for stepsize in lr:
		for depth in max_depth:
			print("step size: ", stepsize, "max depth: ", depth)
			assembler = VectorAssembler(outputCol ="features",\
				inputCols = ["weekday", "hour", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude"])

			predictor = GBTRegressor(featuresCol="features", labelCol = "speed",maxDepth = depth, stepSize = stepsize)
			speed_pipeline = Pipeline(stages = [assembler, predictor])
			speed_model = speed_pipeline.fit(train)

			speed_model.write().overwrite().save(output + '/' +str(stepsize)+'-' + str(depth))

			val_pred = speed_model.transform(validation)
			pred = val_pred.select("speed", "prediction")
			pred.show()

			r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='speed',metricName='r2')
			r2 = r2_evaluator.evaluate(val_pred)
			
			rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='speed',metricName='rmse')
			rmse = rmse_evaluator.evaluate(val_pred)

			print("r2: ", r2)
			print("rmse: ", rmse)

			print(speed_model.stages[-1].featureImportances)

if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)
