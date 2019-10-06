# Task 3 - SQL
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
# Initiating the SparkSession
spark = SparkSession.builder.getOrCreate()
# Importing the required datasets
openDF = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
openDF.createOrReplaceTempView("openDF")
# SQL query
queryDF = spark.sql("SELECT license_type, CAST (round(sum(amount_due), 2) as DECIMAL(20,2)) as total, CAST (round(avg(amount_due), 2) as DECIMAL(20,2)) as average FROM openDF GROUP BY license_type")
# queryDF.show()
# Saving the query output
queryDF.select(format_string('%s\t%s, %s', queryDF.license_type, queryDF.total, queryDF.average)).write.save("task3-sql.out",format="text")

