import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions, types
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime

year = range(2017, 2022) 


def main(inputs):
    # main logic starts here
    '''
    Get weekly average passenger count in year 2017-2021
    Sunday-1, Saturday-7
    '''
    for k in range(len(year)):
        file_name = inputs + '/passenger-' + str(year[k]) + '.csv'
        data = pd.read_csv(file_name)
        plt.figure(figsize = (20, 10))
        weekday = data.iloc[:, 0]
        avg_cnt = [float(i) for i in data.iloc[:, 1]]
        plt.plot(weekday, avg_cnt, 'o-')
        x_lable = 'week in ' + str(year[k])
        plt.xlabel(x_lable)
        y_lable = 'average number of passengers per trip'
        plt.ylabel(y_lable)
        plt.grid(ls = '--')
        pic_name = 'passenger_' + str(year[k])
        plt.savefig(pic_name)   


          

if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('analyse time').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)