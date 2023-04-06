from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.show(truncate=False)
#PySpark explode function can be used to explode an Array of Array (nested Array) columns to rows
df.select(df.name,explode(df.subjects)).show(truncate=False)

#flatten function to flatten the array
df.select(df.name,flatten(df.subjects)).show(truncate=False)