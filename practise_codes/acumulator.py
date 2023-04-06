# Accumulator variables are used for aggregating the information through associative and commutative operations.
# For example, you can use an accumulator for a sum operation or counters.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()

#an accumulator variable “accum” of type int and using it to sum all values in an RDD.
accum=spark.sparkContext.accumulator(0)
#accumulator with initial value as 0.
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x:accum.add(x))
print(accum.value)


accumCount=spark.sparkContext.accumulator(0)
#accumulators to do counters
#this program counts the element in container
rdd2=spark.sparkContext.parallelize([1,2,3,4,5])
rdd2.foreach(lambda x:accumCount.add(1))
print(accumCount.value)