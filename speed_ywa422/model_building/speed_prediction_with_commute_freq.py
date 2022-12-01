import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
spark = SparkSession.builder.appName('speed prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

'''
Run Command
spark-submit speed_prediction_with_commute_freq.py ../data ../data/query_gen_data/hour_day_zone_commute_freq.csv  test_model_output

Function
Train model with information from original dataset with additional traffic flow feature
'''

home_dir = '/home/yiwenw/CMPT732_NY_CAB/' #alter your directory here
sys.path.append(home_dir + "speed/ETL") #absolute path of ETL package
from ETL import read_ETL

def main(inputs, input2, output):
	data, loc_data = read_ETL(inputs, output)
	data = data.sample(True, 0.1)
	freq_data = spark.read.option("header", True).csv(input2)
	freq_data = freq_data.withColumn("hour", freq_data["hour"].cast("int")).withColumn("count", freq_data["count"].cast("int")).\
		withColumn("weekday", freq_data["weekday"].cast("int")).withColumn("PULocationID", freq_data["PULocationID"].cast("long")).\
		withColumn("DOLocationID", freq_data["DOLocationID"].cast("long"))
	'''
	Feature Engineering
	|--Add specific pickup and dropoff location features 
	|--Add hour of day feature
	|--Add traffic crowded feature
	'''
	data = data.alias('a').join(freq_data.alias('c'), [data["PULocationID"] == freq_data["PULocationID"], data["DOLocationID"] == freq_data["DOLocationID"],\
		data["hour"] == freq_data["hour"], data["weekday"] == freq_data["weekday"]], "inner")
	data = data.drop("c.hour", "c.PULocationID", "c.DULocationID", "c.weekday")

	loc_data = loc_data.select("LocationID", "longitude", "latitude")
	data = data.join(loc_data, data["a.PULocationID"] == loc_data["LocationID"], "inner")
	data = data.withColumnRenamed("longitude", "pickup_longitude").withColumnRenamed("latitude", "pickup_latitude")
	data.drop("LocationID")
	data = data.join(loc_data.alias("b"), F.col("a.DOLocationID") == F.col("b.LocationID"), "inner")
	data = data.withColumnRenamed("longitude", "dropoff_longitude").withColumnRenamed("latitude", "dropoff_latitude")

	data = data.select("a.weekday", "a.hour", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude", "count", "speed")
	data.show()
	# '''
	# Use GBTRegression Tree Model to predict the speed
	# Use the lr and max_depth list to perform grid search for the best hyperparameters
	# '''
	train, validation = data.randomSplit([0.75, 0.25])
	train = train

	lr = [0.3, 0.4, 0.5]
	max_depth = [6,7,8]

	'''
	lr =0.3 depth = 6
	with commute freq: r2:  0.674579556756322 rmse:  3.6138341882549794
	without commute freq: r2:  0.6837200267929897 rmse:  3.583850496204484
	'''
	for stepsize in lr:
		for depth in max_depth:
			print("step size: ", stepsize, "max depth: ", depth)
			assembler = VectorAssembler(outputCol ="features",\
				inputCols = ["weekday", "hour", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude", "count"])
			# assembler = VectorAssembler(outputCol ="features",\
			# 	inputCols = ["weekday", "hour", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude"])
			predictor = GBTRegressor(featuresCol="features", labelCol = "speed",maxDepth = depth, stepSize = stepsize)
			speed_pipeline = Pipeline(stages = [assembler, predictor])
			speed_model = speed_pipeline.fit(train)

			speed_model.write().overwrite().save(output + '/' + str(stepsize) + '-' + str(depth))

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
	inputs = sys.argv[1] # data directory
	input2 = sys.argv[2] # commute total times data directory
	output = sys.argv[3] # model saving path
	main(inputs, input2, output)
