from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
transactionSchema = StructType([
        StructField("transaction_id",IntegerType(),True),
        StructField("product_id",IntegerType(),True),
        StructField("userid",IntegerType(),True),
        StructField("price",IntegerType(),True),
        StructField("product_description",StringType(),True)
 ])
df=spark.read.csv("../resource/transaction.csv",header='true',inferSchema="true")
df.show()
df.filter(df.price >= '1000').show()
collectData=df.collect()
for row in collectData:
        print(row["transaction_id"] )
