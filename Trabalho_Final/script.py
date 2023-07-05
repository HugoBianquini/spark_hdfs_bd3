# IMPLEMENTAR SCRIPT
from pyspark import RDD
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import *

sc = SparkContext("local","PySpark Word Count")
spark = SparkSession.builder.getOrCreate()

for i in range(1,13):
    if i > 9:
        df_temp = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-"+str(i)+".parquet")
    else:
        df_temp = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-0"+str(i)+".parquet")
        
    if i > 1:
        df.union(df_temp)
    else:
        df = df_temp
    
df.select('*').where("VendorID != 1 AND VendorID != 2").groupBy("VendorId").count().show()
    