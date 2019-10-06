# Task 7 - SQL
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
# Initiating the SparkSession
spark = SparkSession.builder.getOrCreate()
# Importing the required datasets
parkDF = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
parkDF.createOrReplaceTempView("parkDF")
# SQL query
queryDF = spark.sql('SELECT p.violation_code, CAST(round(COUNT(IF(date_format(p.issue_date, "EEEE") in ("Sunday","Saturday"), 1, NULL))/8, 2) as DECIMAL(20,2)) as weekend_average, CAST(round(COUNT(IF(date_format(p.issue_date, "EEEE") not in ("Sunday", "Saturday"), 1, NULL))/23, 2) as DECIMAL(20,2)) as weekday_average FROM parkDF p GROUP BY p.violation_code')
# queryDF.show()
# Saving the query output
queryDF.select(format_string('%s\t%s, %s', queryDF.violation_code, queryDF.weekend_average, queryDF.weekday_average)).write.save("task7-sql.out",format="text")
