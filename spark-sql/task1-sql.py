# Task 1 - SQL
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
# Initiating the SparkSession
spark = SparkSession.builder.getOrCreate()
# Importing the required datasets
parkDF = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
parkDF.createOrReplaceTempView("parkDF")
openDF = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
openDF.createOrReplaceTempView("openDF")
# SQL query
queryDF = spark.sql('SELECT p.summons_number, p.plate_id, p.violation_precinct, p.violation_code, SUBSTRING(CAST(p.issue_date as string), 1, 10) as issue_date FROM parkDF p WHERE NOT EXISTS (SELECT NULL FROM openDF o WHERE p.summons_number = o.summons_number)')
# queryDF.show()
# Saving the query output
queryDF.select(format_string('%s\t%s, %s, %s, %s', queryDF.summons_number, queryDF.plate_id, queryDF.violation_precinct, queryDF.violation_code, queryDF.issue_date)).write.save("task1-sql.out",format="text")
