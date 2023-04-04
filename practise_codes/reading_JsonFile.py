from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("reading_Json_file").getOrCreate()
df=spark.createDataFrame([],StructType([]))
df=spark.read.option("multiline",True).json("../resource_practise_code/sample1.json")
df.show()
