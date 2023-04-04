
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
kv_pair=spark.sparkContext.parallelize([("spark",4),("pyspark",3),("python",5),("spark",5),("pyspark",4),("big data",4)])
group_by_rdd=kv_pair.groupByKey().collect()
for k,v in group_by_rdd:
    print(k,"=",list(v))
sort_rdd=kv_pair.sortByKey().collect()
print(sort_rdd)
#summing up the values under similar keys using reduceKey()
#so we take any two keys a and b, if similar then add values a+b
list1=kv_pair.reduceByKey(lambda a,b:a+b).collect()
print(list1)
