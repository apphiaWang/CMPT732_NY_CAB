import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
spark = SparkSession.builder.appName('speed prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def main(input1, input2, input3, input4, output):
	'''
	Read data and perform ETL
	'''
	data = spark.read.parquet(input1)
	data = data.filter(data['total_amount']>2.5)
	data = data.filter(data['trip_distance']>0)
	data = data.filter(data['PULocationID'] != 264).filter(data['DOLocationID'] != 265).\
		filter(data['PULocationID'] != 264).filter(data['DOLocationID'] != 265)
	data = data.filter(F.year(data["pickup_datetime"]) >= 2017)
	data = data.filter(F.year(data["pickup_datetime"]) <= 2021)
	data = data.filter(F.year(data["dropoff_datetime"]) >= 2017)
	data = data.filter(F.year(data["dropoff_datetime"]) <= 2021)
	data = data.withColumn("duration", data['dropoff_datetime'].cast("long")-data['pickup_datetime'].cast("long"))
	data = data.filter(data["duration"]>0).filter(data["duration"]<10800)
	data = data.withColumn("speed", data['trip_distance']/(data['duration']/3600))
	data = data.filter(data['speed']<100).filter(data["speed"]>0)
	data = data.withColumn("weekday", F.dayofweek(data['pickup_datetime']))
	data = data.withColumn("tip_percentage", data['tip_amount']/data['total_amount'])
	data = data.withColumn("hour", F.hour("pickup_datetime"))
	
	#read supportive dataset and ETL
	#read commute freq data and join
	freq_data = spark.read.option("header", True).csv(path = input4)
	freq_data = freq_data.withColumn("hour", freq_data["hour"].cast("int")).withColumn("count", freq_data["count"].cast("int")).\
		withColumn("weekday", freq_data["weekday"].cast("int")).withColumn("PULocationID", freq_data["PULocationID"].cast("long")).\
		withColumn("DOLocationID", freq_data["DOLocationID"].cast("long"))
	data = data.alias('a').join(freq_data.alias('c'), [data["PULocationID"] == freq_data["PULocationID"], data["DOLocationID"] == freq_data["DOLocationID"],\
		data["hour"] == freq_data["hour"], data["weekday"] == freq_data["weekday"]], "inner")
	data = data.drop("c.hour", "c.PULocationID", "c.DULocationID", "c.weekday")

	#read location dataset and join
	loc_data = spark.read.option("header", True).csv(path = input2)
	loc_data = loc_data.select("LocationID", "longitude", "latitude")
	data = data.join(loc_data, data["a.PULocationID"] == loc_data["LocationID"], "inner")
	data = data.withColumnRenamed("longitude", "pickup_longitude").withColumnRenamed("latitude", "pickup_latitude")
	data.drop("LocationID")
	data = data.join(loc_data.alias("b"), F.col("a.DOLocationID") == F.col("b.LocationID"), "inner")
	data = data.withColumnRenamed("longitude", "dropoff_longitude").withColumnRenamed("latitude", "dropoff_latitude")
	data  =data. withColumn("pickup_longitude", data["pickup_longitude"].cast("double")).withColumn("pickup_latitude", data["pickup_latitude"].cast("double")).\
		withColumn("dropoff_longitude", data["dropoff_longitude"].cast("double")).withColumn("dropoff_latitude", data["dropoff_latitude"].cast("double"))

	#read weather input and join
	weather_data = spark.read.option("header","true").csv(input3)
	data = data.withColumn("date", F.to_date(data["pickup_datetime"]))
	data = data.join(weather_data, data["date"] == weather_data["date"])
	# data.show()
	data = data.withColumn("has_snow", data["has_snow"].cast(types.IntegerType())).withColumn("has_rain", data["has_rain"].cast(types.IntegerType()))\
		.withColumn("tavg", data["tavg"].cast(types.DoubleType()))

	data = data.select("a.weekday", "a.hour", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude", "count", "tavg", "has_snow", "has_rain","speed")

	'''
	Train GBT model
	'''

	train, validation = data.randomSplit([0.75, 0.25])
	train = train

	lr = [0.3, 0.4, 0.5]
	max_depth = [6,7,8]

	for stepsize in lr:
		for depth in max_depth:
			print("step size: ", stepsize, "max depth: ", depth)
			assembler = VectorAssembler(outputCol ="features",\
				inputCols = ["weekday", "hour", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude", "count", "tavg", "has_rain", "has_snow"])
			predictor = GBTRegressor(featuresCol="features", labelCol = "speed",maxDepth = depth, stepSize = stepsize)
			speed_pipeline = Pipeline(stages = [assembler, predictor])
			speed_model = speed_pipeline.fit(train)

			speed_model.write().overwrite().save(output + '/' + str(stepsize) + '-' + str(depth))

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
	input1 = sys.argv[1]#nyc taxi input
	input2 = sys.argv[2]#location data input
	input3 = sys.argv[3]#weather input
	input4 = sys.argv[4]#commute freq input
	output = sys.argv[5]#model saving path file
	spark = SparkSession.builder.appName('speed prediction aws').getOrCreate()
	spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(input1, input2, input3, input4, output)