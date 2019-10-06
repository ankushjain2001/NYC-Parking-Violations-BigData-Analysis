# Task 4
import sys
from csv import reader
from operator import add
from pyspark import SparkContext
# Set Spark Context
sc = SparkContext()
# Importing the parking-violations lines and the desired features
park_lines = sc.textFile(sys.argv[1], 1)\
               .mapPartitions(lambda x: list(reader(x)))\
               .map(lambda x: (x[16]))
# RDD with parking key and count
park_cnt = park_lines.map(lambda x: (x, 1) if x.upper()=='NY' else ('Other', 1))\
                    .reduceByKey(add)\
                    .map(lambda x:str(x[0]+'\t'+str(x[1])))
#print(len(out.collect()))
# Saving the out RDD
park_cnt.saveAsTextFile('task4.out')
# Stopping the Spark Context
sc.stop()
