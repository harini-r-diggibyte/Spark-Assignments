from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
kv_pair=spark.sparkContext.parallelize([("spark",4),("pyspark",3),("python",5),("spark",5),("pyspark",4),("big data",4)])
countKey=kv_pair.countByKey().items()
print(countKey)
#to print key and values separately we use as
for k,v in countKey:
    print (k,v)