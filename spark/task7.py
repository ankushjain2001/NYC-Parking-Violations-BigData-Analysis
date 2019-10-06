# Task 7
import sys
from csv import reader
from operator import add
from pyspark import SparkContext
from datetime import datetime
# Set Spark Context
sc = SparkContext()
# Day Checker
def isWeekend(d):
    dt = datetime.strptime(d, '%Y-%m-%d')
    if dt.weekday() in [5, 6]:
        return True
    else:
        return False
# Day CountMapping Function
def dayCountMapper(x):
    if isWeekend(x[1]):
        return (x[2], (1, 0, 1))
    else:
        return (x[2], (0, 1, 1))
# Importing the parking-violations lines and the desired features
park_lines = sc.textFile(sys.argv[1], 1)\
               .mapPartitions(lambda x: list(reader(x)))\
               .map(lambda x: dayCountMapper(x))\
               .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
# Calculating the averages and mapping in desired format
park_lines = park_lines.map(lambda x: str(str(x[0]) + '\t'  + '{:0.2f}'.format(x[1][0]/8) + ', ' + '{:0.2f}'.format (x[1][1]/23)))
# Saving the out RDD
park_lines.saveAsTextFile('task7.out')
# Stopping the Spark Context
sc.stop()
