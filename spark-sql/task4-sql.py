# Task 4 - SQL
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
# Initiating the SparkSession
spark = SparkSession.builder.getOrCreate()
# Importing the required datasets
parkDF = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
parkDF.createOrReplaceTempView("parkDF")
# SQL query
queryDF = spark.sql('SELECT CASE WHEN p.registration_state="NY" THEN "NY" ELSE "Other" END as state, count(*) as total FROM parkDF p GROUP BY state')
# queryDF.show()
# Saving the query output
queryDF.select(format_string('%s\t%s', queryDF.state, queryDF.total)).write.save("task4-sql.out",format="text")
