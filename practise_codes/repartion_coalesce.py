from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
rdd=spark.sparkContext.parallelize([1,2,3,4,5,6,7,8])
print("number of partitions : ",rdd.getNumPartitions())
rdd1=rdd.repartition(6)
print(rdd.glom().collect())
print("number of partitions after repartition ")
print(rdd1.getNumPartitions())
#we can see how data is partitioned using glom() method
print(rdd1.glom().collect())
#repartititon based on column name
# df = spark.createDataFrame([("Alex", 20), ("Bob", 30), ("Cathy", 40), ("Alex", 50)], ["name", "age"])
# # df.show()
# df_new = df.repartition(2, "name")


rdd5=spark.sparkContext.parallelize([1,2,3,4,5,6,7,8])
print("number of partitions  : ",rdd5.getNumPartitions())
rdd6=rdd5.coalesce(2)
print(rdd5.glom().collect())
print("number of partitions after coalesce ")
print(rdd6.getNumPartitions())
#we can see how data is partitioned using glom() method
print(rdd6.glom().collect())
