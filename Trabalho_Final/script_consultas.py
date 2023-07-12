from pyspark import RDD
from pyspark.sql.functions import udf, col, to_date, lit, round, when, asc, desc
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import *


def earnings_per_month(original_df: DataFrame):
    df_sum: DataFrame = original_df.where("Trip_distance > 0").groupBy("VendorId", "Month").sum("Total_amount", "Trip_distance")\
        .withColumnRenamed("sum(Total_amount)", "GainsPerVendor")\
        .withColumnRenamed("sum(Trip_distance)", "DistanceTraveled")

    df_sum = df_sum.withColumn(
        "AverageGains", col("GainsPerVendor")/col("DistanceTraveled"))

    return df_sum


def canceled_trips_per_month(df_sum: DataFrame, original_df: DataFrame):
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

    # Filtrar apenas as viagens canceladas e agrupar por mês
    df_canceled_trips: DataFrame = df_with_avg.filter((df_with_avg.tpep_pickup_datetime == df_with_avg.tpep_dropoff_datetime)
                                                      & (df_with_avg.trip_distance == 0)
                                                      & (df_with_avg.PULocationID == df_with_avg.DOLocationID))\
        .groupBy('VendorId', 'Month').count().withColumnRenamed('count', 'CanceledTripsPerMonth')

    return df_canceled_trips


def top_ten_per_year(original_df: DataFrame):
    df_top_dist_year = original_df.groupBy("VendorId").sum("Total_amount", "Trip_distance")\
        .withColumnRenamed("sum(Trip_distance)", "DistanceTraveled")\
        .withColumnRenamed("sum(Total_amount)", "GainsPerVendor")\
        .orderBy("DistanceTraveled", ascending=False)

    df_top_gain_year = df_top_dist_year.withColumn("AverageGains", col(
        "GainsPerVendor")/col("DistanceTraveled")).orderBy('AverageGains', ascending=False)

    return df_top_gain_year


def top_ten_per_month(original_df: DataFrame):
    df_top_dist_month = original_df.groupBy("VendorId", "Month").sum("Total_amount", "Trip_distance")\
        .withColumnRenamed("sum(Total_amount)", "GainsPerVendor")\
        .withColumnRenamed("sum(Trip_distance)", "DistanceTraveled")

    df_top_gain_month = df_top_dist_month .withColumn("AverageGains", col(
        "GainsPerVendor")/col("DistanceTraveled")).orderBy(asc("Month"), desc("AverageGains"))

    return df_top_gain_month


# MAIN PROGRAM
sc = SparkContext("local", "PySpark Word Count")
spark = SparkSession.builder.getOrCreate()

df_feb = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-02.parquet").withColumn("Month", lit("february"))
df_sep = spark.read.parquet(
    "yellow_tripdatas/yellow_tripdata_2022-09.parquet").withColumn("Month", lit("september"))

original_df = df_feb.union(df_sep)

# ------------------------------------------- #
# PARTE 1: Ganho médio por milha por mês por VendorId

print("\nGanho médio por milha por mês:\n")

df_sum = earnings_per_month(original_df)

df_sum.select("VendorId", "Month", "AverageGains").orderBy(
    'VendorId', 'Month', ascending=True).show()


# ------------------------------------------- #
# PARTE 2: Taxa de corridas canceladas por mês
# para os taxistas que possuem o ganho médio por milha superior a média geral


print("\nTaxa de corridas canceladas por mês:\n")

df_canceled_trips = canceled_trips_per_month(df_sum, original_df)

df_canceled_trips.show()

print("\nMédia de corridas canceladas no ano:\n")

df_canceled_trips.groupBy('VendorId').avg("CanceledTripsPerMonth").withColumnRenamed(
    'avg(CanceledTripsPerMonth)', 'CanceledTripsPerYear').show()

# ------------------------------------------- #
# PARTE 3: Ganho médio dos top 10 taxistas que mais rodaram no ano de 2022

print("\nGanho médio dos top 10 no ano:\n")

df_top_gain_year = top_ten_per_year(original_df)

df_top_gain_year.select("VendorId", "AverageGains").show()


# ------------------------------------------- #
# Ganho médio dos top 10 taxistas que mais rodaram por mes no ano de 2022

print("\nGanho médio dos top 10 por mes:\n")

df_top_gain_month = top_ten_per_month(original_df)

df_top_gain_month.select("VendorId", "Month", "AverageGains").show()


# ------------------------------------------- #
# PARTE 4: Adicionar +2% nos valores de viagens pagas com cartão de crédito

df_2 = original_df.withColumn("Total_amount", when(col("Payment_type") == 1, round(col(
    "Total_amount") * 1.02, 2)).otherwise(col("Total_amount")))


#################################### REFAZENDO CONSULTAS COM DATAFRAME ATUALIZADO ####################################


# PARTE 5: Refazer tudo com o dataframe atualizado (df_2)

print("\nRefazendo tudo com a adição da taxa de 2% para viagens pagas no cartão:\n")

# Ganho médio por milha por mês por VendorId

print("\nGanho médio por milha por mês (com 2%):\n")

df_sum_2 = earnings_per_month(df_2)

df_sum_2.select("VendorId", "Month", "AverageGains").orderBy(
    'VendorId', 'Month', ascending=True).show()

print("\nTaxa de corridas canceladas por mês (com 2%):\n")

df_canceled_trips_2 = canceled_trips_per_month(df_sum_2, df_2)

df_canceled_trips_2.show()

print("\nMédia de corridas canceladas no ano (com 2%):\n")

df_canceled_trips_2.groupBy('VendorId').avg("CanceledTripsPerMonth").withColumnRenamed(
    'avg(CanceledTripsPerMonth)', 'CanceledTripsPerYear').show()

print("\nGanho médio dos top 10 no ano (com 2%):\n")

df_top_gain_year_2 = top_ten_per_year(df_2)

df_top_gain_year_2.select("VendorId", "AverageGains").show()

print("\nGanho médio dos top 10 por mes (com 2%):\n")

df_top_gain_month_2 = top_ten_per_month(df_2)

df_top_gain_month_2.select("VendorId", "Month", "AverageGains").show()
