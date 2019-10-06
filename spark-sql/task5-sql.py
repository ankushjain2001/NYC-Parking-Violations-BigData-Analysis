# Task 5 - SQL
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
# Initiating the SparkSession
spark = SparkSession.builder.getOrCreate()
# Importing the required datasets
parkDF = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
parkDF.createOrReplaceTempView("parkDF")
# SQL query
queryDF = spark.sql('SELECT p.plate_id, p.registration_state, COUNT(p.plate_id, p.registration_state) as total FROM parkDF p GROUP BY p.plate_id, p.registration_state ORDER BY total DESC limit 1')
# queryDF.show()
# Saving the query output
queryDF.select(format_string('%s, %s\t%d', queryDF.plate_id, queryDF.registration_state, queryDF['total'])).write.save("task5-sql.out",format="text")
