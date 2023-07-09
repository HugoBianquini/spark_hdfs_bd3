# IMPLEMENTAR SCRIPT
from pyspark import RDD
from pyspark.sql.functions import col, lit, when, abs
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import *

sc = SparkContext("local", "PySpark Word Count")
spark = SparkSession.builder.getOrCreate()

df_jan = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-01.parquet").withColumn("Month", lit("january"))
df_feb = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-02.parquet").withColumn("Month", lit("february"))
df_mar = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-03.parquet").withColumn("Month", lit("march"))
df_apr = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-04.parquet").withColumn("Month", lit("april"))
df_may = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-05.parquet").withColumn("Month", lit("may"))
df_jun = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-06.parquet").withColumn("Month", lit("june"))
df_jul = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-07.parquet").withColumn("Month", lit("july"))
df_aug = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-08.parquet").withColumn("Month", lit("august"))
df_sep = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-09.parquet").withColumn("Month", lit("september"))
df_oct = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-10.parquet").withColumn("Month", lit("october"))
df_nov = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-11.parquet").withColumn("Month", lit("november"))
df_dec = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-12.parquet").withColumn("Month", lit("december"))

df = df_jan.union(df_feb).union(df_mar).union(df_apr) \
    .union(df_may).union(df_jun).union(df_jul) \
    .union(df_aug).union(df_sep).union(df_oct) \
    .union(df_nov).union(df_dec)

# trocando o tipo de pagamento de 0 para 2(cash)
df = df.withColumn("Payment_type", when(col("Payment_type") == 0, lit(2)).otherwise(col("Payment_type")))

# alterando os valores de custo para 0 quando o tipo de pagamento for 3(no charge)
df = df.withColumn("Total_amount", when(col("Payment_type")==3, lit(0.0)).otherwise(col("Total_amount")))

# transformando valores negativos do Total_amount para positivo
df = df.withColumn("Total_amount", when(col("Total_amount") < 0, abs(col("Total_amount"))).otherwise(col("Total_amount")))

# invertendo a data de embarque com a data de desembarque, quando o desembarque aconteceu antes do embarque
df = df.withColumn("dropoff_save", when(col("tpep_pickup_datetime") > col("tpep_dropoff_datetime"), col("tpep_dropoff_datetime")))\
    .withColumn("tpep_dropoff_datetime", when(col("tpep_pickup_datetime") > col("tpep_dropoff_datetime"), col("tpep_pickup_datetime")).otherwise(col("tpep_pickup_datetime")))\
    .withColumn("tpep_pickup_datetime", when(col("tpep_pickup_datetime") > col("dropoff_save"), col("dropoff_save")).otherwise(col("tpep_dropoff_datetime")))\
    .drop(col("dropff_save"))

# resolvendo inconsistencias das corridas canceladas
df = df.withColumn("Trip_distance", when(col("tpep_pickup_datetime") == col("tpep_dropoff_datetime"), lit(0)).otherwise(col("Trip_distance")))\
    .withColumn("DOLocationID", when(col("tpep_pickup_datetime") == col("tpep_dropoff_datetime"), col("PULocationID")).otherwise(col("DOLocationID")))