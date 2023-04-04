from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
lines_rdd=spark.sparkContext.textFile("../resource/textFile.txt")
words_list_rdd=lines_rdd.flatMap(lambda line: line.split(" "))
list1=words_list_rdd.collect()
for i in list1:
    print (i)

