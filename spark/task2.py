# Task 2
import sys
from csv import reader
from operator import add
from pyspark import SparkContext
# Set Spark Context
sc=SparkContext()
# Importing the parking-violations lines and the desired features
park_lines = sc.textFile(sys.argv[1], 1)\
               .mapPartitions(lambda x: list(reader(x)))\
               .map(lambda x: (x[2], 1))
# RDD with difference of park and intersection in the desired format
out = park_lines.reduceByKey(add)\
             .map(lambda x: str(str(x[0])+'\t'+str(x[1])))
#print(len(out.collect()))
# Saving the out RDD
out.saveAsTextFile('task2.out')
# Stopping the Spark Context
sc.stop()
