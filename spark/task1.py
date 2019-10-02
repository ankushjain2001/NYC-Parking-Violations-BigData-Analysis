# Task 1
import sys
from csv import reader
from operator import add
from pyspark import SparkContext
# Set Spark Context
sc=SparkContext()
# Importing the parking-violations lines and the desired features
park_lines = sc.textFile(sys.argv[1], 1)\
               .mapPartitions(lambda x: list(reader(x)))\
               .map(lambda x: (x[0], (x[14], x[6], x[2], x[1])))
# RDD with parking key and count
park_sn = park_lines.map(lambda x: (x[0], 1))
# Importing the open-violations and the desired features
open_lines = sc.textFile(sys.argv[2], 1)\
               .mapPartitions(lambda x:list(reader(x)))
# Intersection of park and open
intersection_sn = park_sn.keys().intersection(open_lines.keys())\
                         .map(lambda x:(x, 1))
#print(len(intersection_sn.collect()))
# RDD with difference of park and intersection in the desired format
out = park_sn.union(intersection_sn)\
             .reduceByKey(add)\
             .filter(lambda x: x[1]==1)\
             .leftOuterJoin(park_lines)\
             .map(lambda x:str(x[0]+'\t'+x[1][1][0]+', '+x[1][1][1]+', '+x[1][1][2]+', '+x[1][1][3]))
#print(len(out.collect()))
# Saving the out RDD
out.saveAsTextFile('hw1/task1_test.txt')
# Stopping the Spark Context
sc.stop()
