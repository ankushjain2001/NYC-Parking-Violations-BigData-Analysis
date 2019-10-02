# Task 5
import sys
from csv import reader
from operator import add
from pyspark import SparkContext
# Set Spark Context
sc = SparkContext()
# Importing the parking-violations lines and the desired features
park_lines = sc.textFile(sys.argv[1], 1)\
               .mapPartitions(lambda x: list(reader(x)))\
               .map(lambda x: ((x[14], x[16]), 1))\
               .reduceByKey(add)
# Obtaining the max val
gr =[tuple(park_lines.max(lambda x: x[1]))]
out = sc.parallelize(gr)
# Mapping the max to the required format
out =  out.map(lambda x: str(x[0][0])+', '+str(x[0][1])+'\t'+str(x[1]))
# Saving the out RDD
out.saveAsTextFile('hw1/task5_test.txt')
# Stopping the Spark Context
sc.stop()