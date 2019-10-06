# Task 2 - SQL
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
# Initiating the SparkSession
spark = SparkSession.builder.getOrCreate()
# Importing the required datasets
parkDF = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
parkDF.createOrReplaceTempView("parkDF")
# SQL query
queryDF = spark.sql('SELECT p.violation_code, count(p.violation_code) as `number of violations` FROM parkDF p GROUP BY p.violation_code')
# queryDF.show()
# Saving the query output
queryDF.select(format_string('%s\t%s', queryDF.violation_code, queryDF['number of violations'])).write.save("task2-sql.out",format="text")
