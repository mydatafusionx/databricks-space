# Databricks notebook source
# MAGIC %md
# # Processamento e Limpeza (Silver)
# MAGIC 
# MAGIC Limpeza e transformação dos dados bronze para silver.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ProcessamentoSilver").getOrCreate()

# Ler dados bronze
df = spark.read.parquet("/Volumes/workspace/bronze/data/bronze/")

# Exemplo de limpeza: remover nulos
df_silver = df.dropna()

# Exemplo de transformação: converter coluna para inteiro (ajuste conforme seu dado)
# df_silver = df_silver.withColumn("coluna", col("coluna").cast("int"))

df_silver.write.mode("overwrite").parquet("/Volumes/workspace/bronze/data/silver/")
