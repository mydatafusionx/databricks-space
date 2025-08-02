# Databricks notebook source
# MAGIC %md
# # Ingestão de Dados (Bronze)
# MAGIC 
# MAGIC Este notebook lê um arquivo CSV e salva como tabela bronze no Databricks.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IngestaoBronze").getOrCreate()

# Caminho do arquivo CSV (pode ser alterado para DBFS ou S3)
# Caminho para uso local:
# input_path = "data/exemplo.csv"
# Caminho para uso no Databricks (ajuste <usuario> para seu usuário ou e-mail):
input_path = "/dbfs/user/<usuario>/data/exemplo.csv"  # Exemplo: /dbfs/user/leonardonunes/data/exemplo.csv

# Dica: faça upload do arquivo pelo menu lateral do Databricks (Data > Upload File) para /user/<usuario>/data/


df = spark.read.option("header", True).csv(input_path)
df.printSchema()
df.show(5)

# Salvar como bronze (parquet)
# Caminho para uso local:
# df.write.mode("overwrite").parquet("data/bronze/")
# Caminho para uso no Databricks:
df.write.mode("overwrite").parquet("dbfs:/user/<usuario>/data/bronze/")  # Exemplo: dbfs:/user/leonardonunes/data/bronze/

