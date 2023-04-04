from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
#reduce is a aggragation usinf function.
spark=SparkSession.builder.getOrCreate()
rdd=spark.sparkContext.parallelize([1,2,3,4,5,6,7,8])
print(rdd.reduce(lambda a,b : a+b))
