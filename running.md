# Additional Required Packages
Please install these packages on your environment for testing the corresponding section: 

Package  | Transportation | Time | Tips | Speed | installed on cluster?
-|-|-|-|-| -
**matplotlib** | o | o |o |o|o
**seaborn** | o | - |o | o|-
**geopandas** |o|-|o|o|-
**holoviews** |- |- |-| o|-
**shapely**|o|-|-|-|-
**pyarrow**|o|-|-|-|-
**fastparquet**|o|-|-|-|-

# Get the data
The input files can be directly downloaded from [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) or [s3](https://s3.console.aws.amazon.com/s3/buckets/nyc-tlc?region=us-east-1&tab=objects).

# Initial ETL

> Each section contain its processed toy dataset for testing. You may skip this part.

Perform initial etl on the raw data. The etl process files one by one because the raw data has inconsistent schemas.

Change this line of code to adapt to your machine.
```python
fs = file_system.get(sc_uri("172.29.89.207"), configuration())#need adaptation on different machines
```

The ETL program takes two arguments, a directory containing all raw data files, and an output directory.
```sh
spark-submit etl_collection/etl_general.py raw_data data
```

# Transportations

> Please run all the commands of transportation analysis under the directory containing the corresponding program file.

0. Please find all programs below under `$project_root/transportation_aaa264`

> Step 1 to 4 are all doing etl work. Processed data is ready for analysis. You may skip the 4 steps.
1. **etl_PU_DO/code/etl_parquet_all.py**
Usage:
```
spark-submit etl_parquet_all.py $raw_data_path ../output_all
```
*[REMINDER] raw_data can be downloaded from NYC open data*

Description:
This program will yield the parquet files after the initial ETL.

2. **etl_bus_stop/code/bus_stop_etl.py**

Usage:
```
python bus_stop_etl.py 
```

Description:
This program will yield the bus stop's geographic information after the ETL process.

3. **etl_subway_stn/code/subway_stn_etl.py**

Usage:
```
python subway_stn_etl.py 
```

Description:
This program will yield the subway station's geographic information after the ETL process.

4. **etl_parking_lot/code/parking_lot_etl.py**

Usage:
```
python parking_lot_etl.py 
```

Description:
This program will yield the parking lot's geographic information after the ETL process.

5. **analysis_PU_DO/code/up_and_down_by_color.py**

Usage:
```
spark-submit up_and_down_by_color.py  ../processed_data ../output_by_color
```

Description:
This program aggregates the data of pick-up and drop-off orders according to the color of the cab.

**up_and_down_by_season.py**

Usage:
```
spark-submit up_and_down_by_season.py ../processed_data ../output_by_season
```

Description:
This program aggregates the data of pick-up and drop-off orders according to the season.

**up_and_down_by_year.py**

Usage:
```
spark-submit up_and_down_by_year.py ../processed_data ../output_by_year
```

Description:
This program aggregates the data of pick-up and drop-off orders according to the year.

**up_and_down_total.py**

Usage:
```
spark-submit up_and_down_total.py ../processed_data ../output_total
```

Description:
This program aggregates the statistics of drop-off and pick-up orders for five years.

6. **analysis_bus_stop/code/stat_on_bus_stop.py**

Usage:
```
spark-submit stat_on_bus_stop.py ../processed_data ../output
```

Description:
This program produces data on the distribution of bus stops according to each zone.

7. **analysis_lots/code/stat_on_loc.py**

Usage:
```
spark-submit stat_on_loc.py ../../etl_parking_lot/output/lots_with_location.parquet ../data
```

Description:
This program produces data on the distribution of parking lots according to each zone.

8. **analysis_subway_stn/code/stat_on_sub.py**

Usage:
```
spark-submit stat_on_sub.py ../processed_data ../output
```

Description:
This program produces data on the distribution of subway stations according to each zone.

8. **visualizations/code/**

**plot_nyc.py**

Usage:
```
spark-submit plot_nyc.py ../../analysis_PU_DO/output_by_color ../output_color
spark-submit plot_nyc.py ../../analysis_PU_DO/output_by_season ../output_season
spark-submit plot_nyc.py ../../analysis_PU_DO/output_total ../output_total
spark-submit plot_nyc.py ../../analysis_PU_DO/output_by_year ../output_year
```

Description:
This program visualizes the data related to the distribution of taxi orders.

**plot_nyc_bus_stop.py**

Usage:
```
spark-submit plot_nyc_bus_stop.py ../output_bus_stop
```

Description:
This program visualizes the data of the distribution of bus stops.

**plot_nyc_parking.py**

Usage:
```
spark-submit plot_nyc_parking.py ../output_parking_lot
```

Description:
This program visualizes the data of the distribution of parking lots.

**plot_nyc_sub_stn.py**

Usage:
```
spark-submit plot_nyc_sub_stn.py ../output_sub_stn
```

Description:
This program visualizes the data of the distribution of subway stations.

# Time Analysis
> Please run all the commands of time analysis under `$project_root/time_yga111`

**1. Specialized ETL for time**

> A processed toy dataset ready at `$project_root/time_yga111/time_input`. You may skip this part.

Perform a ETL job to get the data needed for time analysis. 

Change this line of code to adapt to your machine.
```sh
fs = file_system.get(sc_uri("172.29.89.207"), configuration())#need adaptation on different machines
```

The time ETL program takes two arguments, the input directory containing the raw data, and the output directory for ETL results.
```sh
spark-submit etl_time.py ../raw_data time_input
```

**2. Query**

Divide the parquet directory into five directories by index of year(2017-2021) as the input data files.

The query_time.py does the queries about time on these five years' data respectively.

Make sure everytime just run one of these five functions. 

```sh
    #yearly_trip_count(trip)
    monthly_trip_count(trip)
    #weekday_trip_count(trip)
    #daily_trip_count(trip)
    #hourly_trip_count(trip)
```

This query_time.py program takes two arguments, the input data path and the output file path. The demo output csv file can be found under `$project_root/time_yga111/data/month/month-2017.csv`. 

```sh
spark-submit query_time.py time_input output
```

The query_passenger.py does the queries about passengers on these five years' data respectively, too.

This query_passenger.py program takes two arguments, the input data path and the output file path. The demo output csv file can be found under `$project_root/time_yga111/data/passenger/passenger-2017.csv`. 

```sh
spark-submit query_passenger.py time_input output
```

**3. Visualization**

    The plot_time.py visualizes the relation between time and order numbers on these five years' data altogether.

    Make sure everytime just run one of these five functions. 

    ```sh
        plot_day(inputs)
        #plot_month(inputs)
        #plot_hour(inputs)
        #plot_week(inputs)
        #plot_year(inputs)
    ```

    This plot_time.py program takes one argument, the input data path. The demo output png file can be found under `$project_root/time_yga111/figure/day/`.

    ```sh
    spark-submit plot_time.py data/day/
    ```

    The plot_passenger.py visualizes the relation between weekday and average passenger number per order on these five years' data altogether.

    This plot_passenger.py program takes one argument, the input data path. The demo output png file can be found under `$project_root/time_yga111/figure/passenger/`.

    ```sh
    spark-submit plot_passenger.py data/passenger/
    ```

# Speed Analysis

> Please run all the commands of speed analysis under the directory containing the corresponding program file.

1. #### ETL

  This part of the code will perform ETL for speed analysis, considering the size of the data, this part of the code will function as a package each time in other data-requiring queries, model training, and visualization codes. No need to run this part of the code independently, it will be called in other files.

2. #### query

   Three Python files under this file perform different basic queries to the toy dataset. The program has 2 or 3 arguments. The first argument is always the data directory, where the toy dataset is kept. If there are 3 arguments, then the second argument would be the path of the supplementary data. The last argument is the output path, it can be either a data directory or a figure directory.

   + ##### basic_info_query.py

     ```python
     spark-submit basic_info_query.py ../data ../data/test_gen_data
     ```

   + ##### commute_speed.py

     ```python
     spark-submit commute_speed.py ../data ../../speed_aws_ywa422/aws_gen_query_data/borough_speed/borough_speed.csv ../data/test_gen_data
     ```

   + ##### zone_speed.py

     ```python
     spark-submit zone_speed.py ../data ../data/test_gen_data 
     ```

3. #### supplementary_weather

   Two Python files under this file perform ETL and visualization of the NYC 2019 weather data.

   + ##### weather_ETL.py

     The first argument is the path of the original data, and the second argument is the output path of the post-ETL data.

     ```python
     spark-submit weather_ETL.py weather19 post_ETL_data
     ```

   + ##### weather_visualization.py

     This code generates the average temperature of 2019 NYC. The first argument is the path of the post-ETL data, the second argument is the output figure directory.

     ```python
     python3 weather_visualization.py weather_ETL_data/weather_data.csv weather_figure
     ```

4. #### visualization

   This part of code use matplotlib, seaborn, holoview to perform visualization of queried data.

   + ##### chord_vis.py

     This code is will not show the generated figure while running in Linux environment, I generated the chord figure in the Windows environment. 

     ```python
     python3 chord_vis.py ../data
     ```

   + ##### explore_visualization.py

     This part of the code generates speed distribution, duration distribution, NYC region map and 24-hour average speed heatmap. The first argument is the data directory and the second argument is the output directory of figures. [The output directory contains the figure I generated, so I set up a `test_gen_output` in case of overwriting and other hazards.]

     ```python
     spark-submit explore_visualization.py ../data ../figure/test_gen_output
     ```

   + ##### multipolygon.py

     This part of the code generates a heatmap of the average taxi speed of each zone in NYC. The only argument is the output directory of the figure.

     ```python
     python3  multipolygon.py ../figure/test_gen_output
     ```

5. #### model_building

    This part of the code is to build and train a GBT model to predict speed.

+ ##### correlation_analysis.py

    This part of the code prints out the Pearson correlation value between speed and features, and also generates scatter plots. The first argument is the data directory, and the second argument is the output directory of figures.

    ```python
    spark-submit correlation_analysis.py ../data ../figure/test_gen_output
    ```

+ ##### speed_prediction.py

    This part of code trained GBT model with only original information from the NYC taxi data set. The only argument is the output directory to save the trained models.

    ```python
    spark-submit speed_prediction.py ../data test_model_output
    ```

+ ##### speed_prediction_with_weather.py

    This part of the code trained the GBT model with the original features with additional weather features. The first argument is the path of the supplementary weather data set, and the second argument is the output directory to save the trained models.

    ```
    spark-submit speed_prediction_with_weather.py ../data ../supplementary_weather/weather_ETL_data/weather_data.csv  test_model_output
    ```

+ ##### speed_prediction_with_commute_freq.py

    This part of the code trained the GBT model with original features with additional traffic flow feature. The first argument is the path of the supplementary traffic flow data set, and the second argument is the output directory to save the trained models.

    ```python
    spark-submit speed_prediction_with_commute_freq.py ../data ../data/query_gen_data/hour_day_zone_commute_freq.csv  test_model_output
    ```

6. #### AWS scripts

    The Python files under the directory `/speed_aws_ywa422` contain all runnable scripts that perform some of the functions above. So basically nothing new in this directory, only necessary alternations for large-scale data are done.


# Tip Analysis

> Please run all the commands of tips analysis under `$project_root/tips_ywa416`

## bash script to test tip related programs
```sh
spark-submit general.py test_data your_output/etl_figures
spark-submit overview.py test_data your_output
spark-submit location.py test_data your_output
python3 location_viz.py your_output your_output/location_result
spark-submit model_regressor.py test_data your_output/model_regressor
spark-submit model_classifier.py test_data your_output/model_classifier
```
All results will be written to `$project_root/tips_ywa416/your_output`.

## detailed explanation

0. `test_data`
Contains the 2 ETLed trip data of yellow cab and green cab of Sep 2019.

1. general query a month's data
This program will run some general queries about tips and fares on one month of data, looking at abnormal data and if some features are related to tipping, in order to determine the final ETL for tip analysis on 5 years of data. 

The program takes two arguments, the input data path and the output graph path. 
```sh
spark-submit general.py test_data your_output
```
Detailed explanantion are written in the program comments. The demo output figures can be found under `figures/etl`.

2. tipping overview
This program will 1. get the distribution of tipping over the 5 years, 2. get daily mean, max, median, trip amount of the 5 year. The organized results of 5-year data can be found under `data/overivew`.

> Please notice that this program does not include group by green/yellow cabs considering reducing shuffle. If you want to compare yellow cabs with green cabs, please run separately on the two data files.

The program takes two arguments, the input data path and the output data path.
```sh
spark-submit overview.py test_data your_output
```

The visualization is done by Echarts, you may find the demo figures under `figures/overview`. 

3. tipping by location

This step contains 1. a spark program analyzing tips by location and 2. a python program joins the data with taxi zone lookup file and visualizes results.
 
The spark program takes two arguments, the input data path and the output data path. The program will generate 5 results:
- pickup: the mean and median tip_ratio, and trip count by pickup location
- dropoff: the mean and median tip_ratio, and trip count by dropoff location
- total: the mean tip_ratio, max tip_amount, and trip count by location
- petty: the count of trip with 0-tip (considered as petty tipper), and the ratio of petty count to total count by location
- generous: the count of trip with tip_ratio > 0.4(considered as generous tipper), and the ratio of generous count to total count by location

The python program takes three arguments, an input (the output data path by pyspark), and an output path for new results and figures, and the threshold count number for filtering valid data. The third arg is conditional, if not given, the program will use 1000 to filter valid data. The program will generate 3 files for each result produced by the spark program:
- A full table of the result joined with taxi zone.
- A top 20 table of the corresponding attribute by the spark result, for the first three is average tip ratio, and the last three is petty/generous trip count.
- A >count_threshold table. The record with small count are filtered because they might be seriously affected by outliers.
The program will also visualize the 5 full results. The demo figures can be found under `figures/location`.
```sh
spark-submit location.py test_data your_output
python3 location_viz.py your_output result
```
> The python program is designed for reading spark output structure directly. Please provide the exactly same spark output directory for the first argument
4. tipping models

The program takes two arguments, the input data path and the output model path. The program will print out the evaluation score of test and validation, also the feature importances of tree-based methods. The default model is GBT for regressor, and RandomForest for classifier.
```sh
spark-submit model_regressor.py test_data your_model_output
spark-submit model_classifier.py test_data your_model_output
```
> Please notice that these programs have bad performance on the dataset. They are mainly used give inspiration of important features.
