# IMPLEMENTAR SCRIPT
from pyspark import RDD
from pyspark.sql.functions import udf, col, to_date, lit, year
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import *

sc = SparkContext("local", "PySpark Word Count")
spark = SparkSession.builder.getOrCreate()

df_feb = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-02.parquet").withColumn("Month", lit("february"))
df_sep = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-09.parquet").withColumn("Month", lit("september"))

original_df = df_feb.union(df_sep)

# ------------------------------------------- #
# Parte 1: Ganho médio por milha por mês por VendorId

df_sum: DataFrame = original_df.groupBy("VendorId", "Month").sum("Total_amount", "Trip_distance")\
    .withColumnRenamed("sum(Total_amount)", "GainsPerVendor")\
    .withColumnRenamed("sum(Trip_distance)", "DistanceTraveled")


df_sum = df_sum.withColumn(
    "AverageGains", col("GainsPerVendor")/col("DistanceTraveled"))

df_sum.select(["VendorId", "Month", "AverageGains"]).orderBy(
    ['VendorId', 'Month'], ascending=True).show()

# ------------------------------------------- #
# Parte 2: Taxa de corridas canceladas por mês
# para os taxistas que possuem o ganho médio por milha superior a média geral

# GroupBy vazio para pegar media geral
col_total_avg = df_sum.groupBy().avg("AverageGains")

# GroupBy para pegar media anual por VendorId
df_sum = df_sum.groupBy("VendorId").avg(
    "AverageGains").withColumnRenamed("avg(AverageGains)", "AnualAvgPerVendor")

# Adicionar coluna de media geral anual
df_with_avg = df_sum.withColumn("TotalAvg", lit(
    col_total_avg.first()[0])).select(['VendorId', 'AnualAvgPerVendor', 'TotalAvg'])

# Join para manter apenas dados de Vendors que possuem media maior que a media geral
df_with_avg = original_df.join(df_with_avg.where(
    col('AnualAvgPerVendor') > col('TotalAvg')), "VendorId")

df_with_avg.groupBy('VendorId').count().show()

# Próximos passos são manter apenas viagens canceladas para extrair a taxa de viagens canceladas por mês


# ------------------------------------------- #
# Parte 3: A fazer