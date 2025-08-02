# Databricks notebook source
# MAGIC %md
# # Ingestão de Dados (Bronze)
# MAGIC 
# MAGIC Este notebook lê um arquivo CSV e salva como tabela bronze no Databricks.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IngestaoBronze").getOrCreate()

# Caminho do arquivo CSV (pode ser alterado para DBFS ou S3)
input_path = "data/exemplo.csv"  # coloque um CSV de exemplo aqui

df = spark.read.option("header", True).csv(input_path)
df.printSchema()
df.show(5)

# Salvar como bronze (parquet)
df.write.mode("overwrite").parquet("data/bronze/")
