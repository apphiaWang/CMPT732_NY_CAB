import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os

from pyspark.sql import SparkSession, functions, types
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime

year = range(2017, 2022) 

'''
Get daily trip count in year 2017-2021
'''
def plot_day(inputs):
    for k in range(len(year)):
        # date_count_schema = types.StructType([
        # types.StructField('date', types.StringType()),
        # types.StructField('count', types.LongType()),
        # ])
        file_name = inputs + '/date-' + str(year[k]) + '.csv'
        data = pd.read_csv(file_name)
        plt.figure(figsize = (20, 10))
        date = data.iloc[:, 0]
        date = [datetime.datetime.strptime(i, '%Y-%m-%d') for i in date]
        count = [int(i) for i in data.iloc[:, 1]]
        plt.plot(date, count, 'o-')
        x_lable = 'day in ' + str(year[k])
        plt.xlabel(x_lable)
        y_lable = 'number of daily trips in ' + str(year[k])
        plt.ylabel(y_lable)

        if year[k] == 2017:
            plt.annotate('[2017-03-14 Blizzard]', xy = (datetime.datetime.strptime(('2017/03/14'), '%Y/%m/%d'), 104210),
            xytext = (datetime.datetime.strptime(('2017/04/14'), '%Y/%m/%d'), 144210),
            arrowprops = dict(color = 'black', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
            plt.annotate('[2017 Christmas]', xy = (datetime.datetime.strptime(('2017/12/25'), '%Y/%m/%d'), 157005),
            xytext = (datetime.datetime.strptime(('2017/10/25'), '%Y/%m/%d'), 107005),
            arrowprops = dict(color = 'orange', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
        elif year[k] == 2018:
            plt.annotate('[2018-01-04 Blizzard]', xy = (datetime.datetime.strptime(('2018/01/04'), '%Y/%m/%d'), 125059),
            xytext = (datetime.datetime.strptime(('2018/02/12'), '%Y/%m/%d'), 154210),
            arrowprops = dict(color = 'black', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
            plt.annotate('[2018 Christmas]', xy = (datetime.datetime.strptime(('2018/12/25'), '%Y/%m/%d'), 131211),
            xytext = (datetime.datetime.strptime(('2018/10/12'), '%Y/%m/%d'), 154210),
            arrowprops = dict(color = 'orange', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
        elif year[k] == 2019:
            plt.annotate('[2019-07-04 Fireworks Display]', xy = (datetime.datetime.strptime(('2019/07/04'), '%Y/%m/%d'), 124791),
            xytext = (datetime.datetime.strptime(('2019/04/01'), '%Y/%m/%d'), 124791),
            arrowprops = dict(color = 'green', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
            plt.annotate('[2019 Labour Day]', xy = (datetime.datetime.strptime(('2019/09/02'), '%Y/%m/%d'), 136500),
            xytext = (datetime.datetime.strptime(('2019/08/02'), '%Y/%m/%d'), 100791),
            arrowprops = dict(color = 'orange', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
            plt.annotate('[2019 Thanksgiving Day]', xy = (datetime.datetime.strptime(('2019/11/28'), '%Y/%m/%d'), 131728),
            xytext = (datetime.datetime.strptime(('2019/9/26'), '%Y/%m/%d'), 161728),
            arrowprops = dict(color = 'orange', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
            plt.annotate('[2019 Christmas]', xy = (datetime.datetime.strptime(('2019/12/25'), '%Y/%m/%d'), 100271),
            xytext = (datetime.datetime.strptime(('2019/10/22'), '%Y/%m/%d'), 100271),
            arrowprops = dict(color = 'orange', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
        elif year[k] == 2020:
            plt.annotate('[2020-03-11 COVID–19 Pandemic]', xy = (datetime.datetime.strptime(('2020/03/11'), '%Y/%m/%d'), 184036),
            xytext = (datetime.datetime.strptime(('2020/03/26'), '%Y/%m/%d'), 214036),
            arrowprops = dict(color = 'red', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
            plt.annotate('[2020-03-16 School Closed]', xy = (datetime.datetime.strptime(('2020/03/16'), '%Y/%m/%d'), 63483),
            xytext = (datetime.datetime.strptime(('2020/01/01'), '%Y/%m/%d'), 63483),
            arrowprops = dict(color = 'red', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
            plt.annotate('[2020-03-20 Business Closed]', xy = (datetime.datetime.strptime(('2020/03/20'), '%Y/%m/%d'), 27681),
            xytext = (datetime.datetime.strptime(('2020/04/10'), '%Y/%m/%d'), 59791),
            arrowprops = dict(color = 'red', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
        else:
            plt.annotate('[2021-02-01 Blizzard]', xy = (datetime.datetime.strptime(('2021/02/01'), '%Y/%m/%d'), 5583),
            xytext = (datetime.datetime.strptime(('2021/02/20'), '%Y/%m/%d'), 24210),
            arrowprops = dict(color = 'black', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))
            plt.annotate('[2021 Christmas]', xy = (datetime.datetime.strptime(('2021/12/25'), '%Y/%m/%d'), 37637),
            xytext = (datetime.datetime.strptime(('2021/10/28'), '%Y/%m/%d'), 54210),
            arrowprops = dict(color = 'orange', shrink = 0.05, width = 6, headwidth = 16, headlength = 16))

        plt.grid(ls = '--')
        pic_name = 'day_' + str(year[k])
        plt.savefig(pic_name)

'''
Get monthly trip count in year 2017-2021
'''
def plot_month(inputs):
    for k in range(len(year)):
        file_name = inputs + '/month-' + str(year[k]) + '.csv'
        data = pd.read_csv(file_name)
        plt.figure(figsize = (25, 10))
        month = data.iloc[:, 0]
        count = [int(i) for i in data.iloc[:, 1]]
        plt.plot(month, count, 'o-')
        x_lable = 'month in ' + str(year[k])
        plt.xlabel(x_lable)
        y_lable = 'number of monthly trips in ' + str(year[k])
        plt.ylabel(y_lable)
        plt.grid(ls = '--')
        pic_name = 'month_' + str(year[k])
        plt.savefig(pic_name)


'''
Get hourly trip count in year 2017-2021
'''
def plot_hour(inputs):
    for k in range(len(year)):
        file_name = inputs + '/hour-' + str(year[k]) + '.csv'
        data = pd.read_csv(file_name)
        plt.figure(figsize = (20, 10))
        hour = data.iloc[:, 0]
        count = [int(i) for i in data.iloc[:, 1]]
        plt.plot(hour, count, 'o-')
        x_lable = 'hour in ' + str(year[k])
        plt.xlabel(x_lable)
        y_lable = 'number of hourly trips in ' + str(year[k])
        plt.ylabel(y_lable)
        plt.grid(ls = '--')
        pic_name = 'hour_' + str(year[k])
        plt.savefig(pic_name)

'''
Get weekly trip count in year 2017-2021
Sunday-1, Saturday-7
'''
def plot_week(inputs):
    for k in range(len(year)):
        file_name = inputs + '/weekday-' + str(year[k]) + '.csv'
        data = pd.read_csv(file_name)
        plt.figure(figsize = (20, 10))
        weekday = data.iloc[:, 0]
        count = [int(i) for i in data.iloc[:, 1]]
        plt.plot(weekday, count, 'o-')
        x_lable = 'week in ' + str(year[k])
        plt.xlabel(x_lable)
        y_lable = 'number of weekly trips in ' + str(year[k])
        plt.ylabel(y_lable)
        plt.grid(ls = '--')
        pic_name = 'week_' + str(year[k])
        plt.savefig(pic_name)

'''
Get yearly trip count in year 2017-2021
'''
def plot_year(inputs):
    file_name = inputs + '/year.csv'
    data = pd.read_csv(file_name)
    plt.figure(figsize = (16, 10))
    year = data.iloc[:, 0]
    count = [int(i) for i in data.iloc[:, 1]]
    plt.plot(year, count, 'o-')
    x_lable = 'year'
    plt.xlabel(x_lable)
    y_lable = 'number of yearly trips'
    plt.ylabel(y_lable)
    plt.grid(ls = '--')
    pic_name = 'year'
    plt.savefig(pic_name)    



def main(inputs):
    # main logic starts here
    
    plot_day(inputs)
    #plot_month(inputs)
    #plot_hour(inputs)
    #plot_week(inputs)
    #plot_year(inputs)


          

if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('analyse time').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)