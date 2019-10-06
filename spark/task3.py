# Task 3
import sys
from csv import reader
from operator import add
from pyspark import SparkContext
# Set Spark Context
sc=SparkContext()
# Importing the parking-violations lines and the desired features
park_lines = sc.textFile(sys.argv[1], 1)\
               .mapPartitions(lambda x: list(reader(x)))\
               .map(lambda x: (x[2], (float(x[12]), 1)))
# RDD with totals and averages
out = park_lines.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))\
             .map(lambda x: str(str(x[0])+'\t'+'{:0.2f}'.format(x[1][0])+', '+'{:0.2f}'.format(x[1][0]/x[1][1])))
# Saving the out RDD
out.saveAsTextFile('task3.out')
# Stopping the Spark Context
sc.stop()
