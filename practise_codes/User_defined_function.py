from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()

schema = StructType([
        StructField("Name",StringType(),True),
        StructField("Id",IntegerType(),True),
        StructField("Designation",StringType(),True)])
data=[("aaaa",101,"data engineer"),
      ("bbbb",102,"full stack"),
      ("cccc",103,"manager")]

df=spark.createDataFrame(data=data, schema=schema)
def convert_name(str):
    result=""
    name_split=str.split(" ")
    for i in name_split:
        result=result+i[0:1].upper()+i[1:len(i)]
    return result

def convert_designation(str):
     return str.upper()

convert_name_udf=udf(lambda x:convert_name(x))
convert_designation_udf=udf(lambda y:convert_designation(y))

df.select(convert_name_udf(col("Name")).alias("NAME"),col("Id"),\
               convert_designation_udf(col("Designation")).alias("DESIGNATION")).show()
