from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()

data = [("James", "Sales", 3000), \
        ("James","Sales",3000),\
        ("Michael", "Sales", 4600), \
        ("Robert", "Sales", 4100), \
        ("Maria", "Finance", 3000), \
        ("James","Sales",2000),\
        ("Saif", "Sales", 4100) ]

columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)
#distinct retrns non dupliacte records by comparing all column values.
#(i.e) 3 column values of the two rows should be same, then it takes any one row in result
#if 2nd column value of those two rows differs, it considers both rows as distinct
distinctDF = df.distinct()
distinctDF.show()

#drop_duplicates is like distinct function is been applied to specific column
#rows with same values dept&salary will not be seen in result.
dropDisDF = df.dropDuplicates(["department","salary"])
dropDisDF.show()

#to change the dattype of the column
df1=df.withColumn("salary",col("salary").cast(FloatType()))
df1.printSchema()