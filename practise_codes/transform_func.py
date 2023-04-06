from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark=SparkSession.builder.getOrCreate()

#transform function in pyspark.sql.Dataframe
simpleData = (("Java",4000,5), \
            ("Python", 4600,10), \
            ("Scala", 4100,15),   \
            ("Scala", 4500,15),   \
            ("PHP", 3000,20))
columns= ["CourseName", "fee", "discount"]

#creating a dataframe
df = spark.createDataFrame(data = simpleData, schema = columns)
df.show(truncate=False)

#function to convert coursename column to uppercase completely
def convert_Coursename(df):
    return df.withColumn("new_Course_name",upper(df.CourseName))

#function to calculate discount fee
def disc_fee(df):
    return df.withColumn( "discount", df.fee - (df.fee *df.discount)/100)

#transform function applies custom transformation and returns new dataframe
df1=df.transform(disc_fee).transform(convert_Coursename)
df1.show()

#transform function under pyspark.sql.functions
#This function applies the specified transformation on every element of the array and returns an object of ArrayType.

data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"]),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"]),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"])
]
df = spark.createDataFrame(data=data,schema=["Name","Languages1","Languages2"])
df1=df.select(transform("Languages1", lambda x: upper(x)).alias("languages1"))
df1.show()