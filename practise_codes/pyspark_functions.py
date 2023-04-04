from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("reading_avro_file").getOrCreate()
df=spark.createDataFrame([],StructType([]))
df=spark.read.option("header",True).csv("../resource/user.csv")
df.show()

#map() transformation
#map is a transformation that is used to apply the transformation to each element and returns a new RDD
#map is a RDD transaformation and we cant apply map to DataFrame
data = ["Project","Gutenberg’s","Alice’s","Adventures",
"in","Wonderland","Project","Gutenberg’s","Adventures",
"in","Wonderland","Project","Gutenberg’s"]

#map function eg1
rdd=spark.sparkContext.parallelize(data)
rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)

data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62),
]

#map function eg2
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
rdd2=df.rdd.map(lambda x : (x.firstname+","+x.lastname,x.gender,x.salary*2) )
df2=rdd2.toDF(["name" , "gender" , "new_salary"])
df2.show()

#flatMap()  transformation
#flatMap() is transfromation that flattens the RDD/dataframe after applying function on every element and returns new RDD/dataframe.
data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)

rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)

#to apply flatmap in dataframe we have explode
arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]
df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df2 = df.select(df.name,explode(df.knownLanguages))
df2.show()

#lit() function
#lit() are used to add a new column to DataFrame by assigning a literal or constant value
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("111",50000),("222",60000),("333",40000)]
columns= ["EmpId","Salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()
df1=df.withColumn("lit_value",when (col("Salary") <= 50000,lit("100")).otherwise(lit("200")))
df1.show()

#split()
data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62),
]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
rdd2=df.rdd.map(lambda x : (x.firstname+","+x.lastname,x.gender,x.salary*2) )
df2=rdd2.toDF(["name" , "gender" , "new_salary"])
df2.show()

df3=df2.select(split(col("name"),",").alias("name_separated"))
df3.show()

#concat_ws(sep,columns)
#this converts array to a string with the specified separator
columns = ["name","languagesAtSchool","currentState"]
data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
    ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
    ("Robert,,Williams",["CSharp","VB"],"NV")]

df = spark.createDataFrame(data=data,schema=columns)
df.show()

df2 = df.withColumn("languagesAtSchool",
   concat_ws(",",col("languagesAtSchool")))
df2.show()

#substring
#the substring() function is used to extract the substring from string column
# substring(str, pos, len)

data = [(1,"20200828"),(2,"20180525")]
columns=["id","date"]
df=spark.createDataFrame(data,columns)
df.withColumn('year', substring('date', 1,4))\
    .withColumn('month', substring('date', 5,2))\
    .withColumn('day', substring('date', 7,2)).show()

#regexp_replace() to replace a column value with a string for another string/substring
address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]
df =spark.createDataFrame(address,["id","address","state"])
df.withColumn('address', regexp_replace('address', 'Rd', 'Road')) \
  .show(truncate=False)
df.show()

# to replace column values conditionally (with when)
# df.withColumn('address',
#     when(df.address.endswith('Rd'),regexp_replace(df.address,'Rd','Road')) \
#    .when(df.address.endswith('St'),regexp_replace(df.address,'St','Street')) \
#    .when(df.address.endswith('Ave'),regexp_replace(df.address,'Ave','Avenue')) \
#    .otherwise(df.address)) \
#    .show(truncate=False)

#overlay() function
#col2 part is replaced in col1 value.
#replacement of col2 is starting from position 7
df = spark.createDataFrame([("ABCDE_XYZ", "FGH")], ("col1", "col2"))
df.select(overlay("col1", "col2", 7).alias("overlayed")).show()


#translate()
#replace character by character of dataframe column value

address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]
df =spark.createDataFrame(address,["id","address","state"])
df.withColumn('address', translate('address', '123', 'ABC')).show()

