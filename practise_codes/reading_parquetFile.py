from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("reading_parquet_file").getOrCreate()
df=spark.createDataFrame([],StructType([]))
df=spark.read.parquet("../resource_practise_code/userdata1.parquet")
df.show()
