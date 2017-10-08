"""SimpleApp.py"""
from pyspark.sql import SparkSession

logFile = "/Users/Yuanrui/Downloads/spark-2.2.0-bin-hadoop2.7/README.md"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").master('local[4]').getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
