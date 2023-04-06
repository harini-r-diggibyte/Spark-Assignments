from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)

#pivot transposes the rows to columns
pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")

pivotDF.show(truncate=False)
