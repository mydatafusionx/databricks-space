# Databricks notebook source
# MAGIC %md
# # Análise e Enriquecimento (Gold)
# MAGIC 
# MAGIC Agregações e análises exploratórias.

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

spark = SparkSession.builder.appName("AnaliseGold").getOrCreate()

df = spark.read.parquet("/Volumes/workspace/bronze/data/silver/")

# Exemplo de agregação
# Ajuste os nomes das colunas conforme seu dado
# resultado = df.groupBy("coluna_categoria").agg(count("*").alias("contagem"), avg("coluna_valor").alias("media"))
# resultado.show()
