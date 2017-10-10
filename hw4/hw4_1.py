from __future__ import print_function

import sys
import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import date

list_all = [ list() for x in range(366)]
if __name__ == "__main__":
    def get_count(filename):
        df = spark.read.csv(DIR_PATH + filename, header=True, inferSchema=True, mode="DROPMALFORMED")
        name = filename.replace('.csv', '')
        count = df.filter((df['Close']-df['Open'])/df['Open']>0.01).count()
        return (name, count)


    DIR_PATH = "examples/src/main/python/hw4/NASDAQ100/"
    os.chdir(DIR_PATH)
    name_list = list()
    for file in glob.glob('*.csv'):
        name_list.append(file)

    spark = SparkSession.builder \
        .master("local") \
        .appName("CSV read and count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    result = map(get_count, name_list)
    output_file = open("output_1.txt",'w')
    for x in result:
    	output_file.write(str(x[0]) + ',' + str(x[1])+'\n')
    output_file.close()
   

    
