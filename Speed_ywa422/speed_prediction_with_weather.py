import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType # has to assign specific type name or "not define" error will occur
from pyspark.sql import functions as F
spark = SparkSession.builder.appName('speed prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

from ETL import read_ETL

def main(input1,input2, output):
	data, loc_data = read_ETL(input1, output)
	data = data.sample(True, 0.05)
	weather_data = spark.read.option("header","true").csv(input2) #when typing dataframe head, only use comma with no space!

	'''
	Feature Engineering
	|-- Add specific pickup and dropoff location features 
	|-- Add hour of day feature
	|-- Add weather features (average temperature, has_snow, has_rain)
	|-- Alter data type
	|-- Preserve hour, weekday, pickup and dropoff longitude and latitude and weather info
	'''
	loc_data = loc_data.select("LocationID", "longitude", "latitude")
	data = data.join(loc_data, data["PULocationID"] == loc_data["LocationID"], "inner")
	data = data.withColumnRenamed("longitude", "pickup_longitude").withColumnRenamed("latitude", "pickup_latitude")
	data.drop("LocationID")
	data = data.alias("a").join(loc_data.alias("b"), F.col("a.DOLocationID") == F.col("b.LocationID"), "inner")
	data = data.withColumnRenamed("longitude", "dropoff_longitude").withColumnRenamed("latitude", "dropoff_latitude")
	data = data.withColumn("hour", F.hour(data['pickup_datetime']))
	data = data.select("pickup_datetime", "hour", "weekday", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude","speed")

	data = data.withColumn("date", F.to_date(data["pickup_datetime"]))
	data = data.join(weather_data, data["date"] == weather_data["date"])
	# data.show()
	data = data.withColumn("has_snow", data["has_snow"].cast(IntegerType())).withColumn("has_rain", data["has_rain"].cast(IntegerType()))\
		.withColumn("tavg", data["tavg"].cast(DoubleType()))

	data = data.select("hour", "weekday", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude","tavg", "has_snow", "has_rain", "speed")
	# data.show()

	'''
	Use GBTRegression Tree Model to predict the speed
	use the lr and max_depth list to do the grid search for the best hyperparameters
	'''
	lr = [0.3]
	max_depth = [8]

	train, validation = data.randomSplit([0.75, 0.25])
	train = train
	for stepsize in lr:
		for depth in max_depth:
			print("step size: ", stepsize, " max depth: ", depth)
			assembler = VectorAssembler(outputCol ="features",\
				inputCols = ["weekday", "hour", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude", "tavg", "has_snow", "has_rain"])
			# assembler = VectorAssembler(outputCol ="features",\
			# 	inputCols = ["weekday", "hour", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude"])
			predictor = GBTRegressor(featuresCol="features", labelCol = "speed",maxDepth = depth, stepSize = stepsize)
			speed_pipeline = Pipeline(stages = [assembler, predictor])
			speed_model = speed_pipeline.fit(train)

			speed_model.write().overwrite().save(output)

			val_pred = speed_model.transform(validation)
			pred = val_pred.select("speed", "prediction")
			# pred.show()
			r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='speed',metricName='r2')
			r2 = r2_evaluator.evaluate(val_pred)
			
			rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='speed',metricName='rmse')
			rmse = rmse_evaluator.evaluate(val_pred)

			print("r2: ", r2)
			print("rmse: ", rmse)

			print(speed_model.stages[-1].featureImportances)

if __name__ == '__main__':
	input1 = sys.argv[1]
	input2 = sys.argv[2]
	output = sys.argv[3]
	main(input1,input2, output)