from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
num_rdd=spark.sparkContext.parallelize([2,4,6,8,10])
square_num=num_rdd.map(lambda n:n*n )
listnum=square_num.collect()

for i in listnum:
    print(i)
