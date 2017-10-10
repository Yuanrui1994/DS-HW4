from __future__ import print_function

import sys
import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import date

list_all = [ list() for x in range(366)]
if __name__ == "__main__":
    def process(filename):
        df = spark.read.csv(DIR_PATH + filename, header=True, inferSchema=True, mode="DROPMALFORMED")
        name = filename.replace('.csv', '')
        date_gain_list = df.rdd.map(lambda x: get_gain(x,name)).collect()
        return (name, date_gain_list)

    def get_gain(df,name):
        gain = (df['Close']-df['Open'])/df['Open']
        return (df['Date'], (gain, name))
        
    def flatten(files):
        return files[1]

    def sortUsingKey(t):
        date = t[0].date().strftime("%Y-%m-%d")
        after_sort = sorted(list(t[1]), reverse=True)[0:5]
        name_list = list()
        for item in after_sort:
            name_list.append(item[1])
        return (date, name_list)


    DIR_PATH = "examples/src/main/python/hw4/NASDAQ100/"
    os.chdir(DIR_PATH)
    name_list = list()
    for file in glob.glob('*.csv'):
        name_list.append(file)

    spark = SparkSession.builder \
        .master("local") \
        .appName("CSV read test") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    result = map(process, name_list)
    sc = spark.sparkContext.parallelize(result).flatMap(flatten).groupByKey().map(sortUsingKey).collect()
    sorted_result = sorted(sc)
    
    output_file = open("output_2.txt",'w')
    for x in sorted_result: 
        output_file.write(str(x[0]) + ',' + str(x[1])+'\n')
    output_file.close()
   

    
