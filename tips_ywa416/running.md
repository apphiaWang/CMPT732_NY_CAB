## Additional Required Packages
Please install these packages on your environment: 
- **matplotlib**
- **seaborn**
- **geopandas**: for visualizing geometric heatmap map of New York


## Initial ETL (can be skipped)

The program read all files under the input directory and write the processed data into the output directory. The input files can be directly downloaded from [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) or [s3](https://s3.console.aws.amazon.com/s3/buckets/nyc-tlc?region=us-east-1&tab=objects).
```sh
spark-submit etl.py input-rawdata output
```

## Test Dataset

The test data can be found under `test_data`. It contains the trip records of _**yellow cab and green cab of Sep 2019**_. The data has been processed with initial etl.


# Time Analysis

**1. ETL**

    Perform two ETL jobs to get the data needed for analysis. 

    Change this line of code to adapt to your machine.

    ```sh
    fs = file_system.get(sc_uri("172.29.89.207"), configuration())#need adaptation on different machines
    ```

    Put the etl_parquet.py file under the raw data directory. The initial ETL program takes one argument, the output parquet file path.

    ```sh
    spark-submit time_yga111/etl_parquet.py your_output
    ```

    Put the etl_time.py file under the data directory. The second ETL program takes one argument, the output parquet file path. We can get a directory containing all the parquet files.

    ```sh
    spark-submit time_yga111/etl_time.py your_output
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

This query_time.py program takes two arguments, the input data path and the output file path. The demo output csv file can be found under `time_yga111/data/month/month-2017.csv`. 

```sh
spark-submit time_yga111/query_time.py 2017_input 2017_output
```

The query_passenger.py does the queries about passengers on these five years' data respectively, too.

This query_passenger.py program takes two arguments, the input data path and the output file path. The demo output csv file can be found under `time_yga111/data/passenger/passenger-2017.csv`. 

```sh
spark-submit time_yga111/query_passenger.py 2017_input 2017_output
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

    This plot_time.py program takes one argument, the input data path. The demo output png file can be found under `time_yga111/figure/day/`.

    ```sh
    spark-submit time_yga111/plot_time.py time_yga111/data/day/
    ```

    The plot_passenger.py visualizes the relation between weekday and average passenger number per order on these five years' data altogether.

    This plot_passenger.py program takes one argument, the input data path. The demo output png file can be found under `time_yga111/figure/passenger/`.

    ```sh
    spark-submit time_yga111/plot_passenger.py time_yga111/data/passenger/
    ```

# Speed Analysis

0. #### Home Directory

   To run the python scripts mentioned below smoothly, all the `home_dir` needs to be altered accordingly.

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


## Tip Analysis

1. general query a month's data
This program will run some general queries about tips and fares on one month of data, looking at abnormal data and if some features are related to tipping, in order to determine the final ETL for tip analysis on 5 years of data. 

The program takes two arguments, the input data path and the output graph path. 
```sh
spark-submit tips_ywa416/general.py test_data your_output
```
Detailed explanantion are written in the program comments. The demo output figures can be found under `tips_ywa416/figures/etl`.

2. tipping overview
This program will 1. get the distribution of tipping over the 5 years, 2. get daily mean, max, median, trip amount of the 5 year. The organized output files of yellow cab can be found under `tips_ywa416/data/overivew`.

> Please notice that this program does not include group by green/yellow cabs considering reducing shuffle. If you want to compare yellow cabs with green cabs, please run separately on the two dataset.

The program takes two arguments, the input data path and the output data path.
```sh
spark-submit tips_ywa416/overview.py test_data your_output
```

The visualization is done by Echarts, you may find the demo figures under `tips_ywa416/figures/overview`. 

3. tipping by location
This step contains a spark program analyzing the data and a python program for further and visualizing results.

This spark program will 1. get the distribution of tipping over the 5 years, 2. get daily mean, max, median, trip amount of the 5 year. 
The spark program takes two arguments, the input data path and the output data path.
```sh
spark-submit tips_ywa416/location.py test_data your_output
python3 
```
> Please notice that 

4. tipping models

The program takes two arguments, the input data path and the output model path. The program will print out the evaluation score of test and validation, also the feature importances of tree-based methods.
```sh
spark-submit tips_ywa416/model_regressor.py test_data your_model_output
spark-submit tips_ywa416/model_classifier.py test_data your_model_output
```
