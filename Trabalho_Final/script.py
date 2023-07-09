# IMPLEMENTAR SCRIPT
from pyspark import RDD
from pyspark.sql.functions import col, lit, when, abs
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import *

sc = SparkContext("local","PySpark Word Count")
spark = SparkSession.builder.getOrCreate()

df_jan = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-01.parquet").withColumn("month", lit("january"))
df_feb = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-02.parquet").withColumn("month", lit("february"))
df_mar = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-03.parquet").withColumn("month", lit("march"))
df_apr = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-04.parquet").withColumn("month", lit("april"))
df_may = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-05.parquet").withColumn("month", lit("may"))
df_jun = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-06.parquet").withColumn("month", lit("june"))
df_jul = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-07.parquet").withColumn("month", lit("july"))
df_aug = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-08.parquet").withColumn("month", lit("august"))
df_sep = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-09.parquet").withColumn("month", lit("september"))
df_oct = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-10.parquet").withColumn("month", lit("october"))
df_nov = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-11.parquet").withColumn("month", lit("november"))
df_dec = spark.read.parquet("yellow_tripdatas/yellow_tripdata_2022-12.parquet").withColumn("month", lit("december"))

df = df_jan.union(df_feb).union(df_mar).union(df_apr) \
        .union(df_may).union(df_jun).union(df_jul) \
        .union(df_aug).union(df_sep).union(df_oct) \
        .union(df_nov).union(df_dec)
        
#transformando valores negativos do Total_amount para positivo
df = df.withColumn("Total_amount", when(col("Total_amount") < 0, abs(col("Total_amount"))).otherwise(col("Total_amount")))

# invertendo a data de embarque com a data de desembarque, quando o desembarque aconteceu antes do embarque
df = df.withColumn("dropoff_save", when(col("tpep_pickup_datetime") > col("tpep_dropoff_datetime"), col("tpep_dropoff_datetime")))\
        .withColumn("tpep_dropoff_datetime", when(col("tpep_pickup_datetime") > col("tpep_dropoff_datetime"), col("tpep_pickup_datetime")).otherwise(col("tpep_pickup_datetime")))\
        .withColumn("tpep_pickup_datetime", when(col("tpep_pickup_datetime") > col("dropoff_save"), col("dropoff_save")).otherwise(col("tpep_dropoff_datetime")))\
        .drop(col("dropff_save"))

#df.select(to_date(col("tpep_pickup_datetime"), "yyyy-MM").alias("DatePick")).groupBy("DatePick").count().show()
#df_sum = df.groupBy("VendorId").sum("Total_amount", "Trip_distance")\
    #.withColumnRenamed("sum(Total_amount)", "GainsPerVendor")\
    #.withColumnRenamed("sum(Trip_distance)", "DistanceTraveled")
#df_sum.withColumn("AverageGains", col("GainsPerVendor")/col("DistanceTraveled")).show()