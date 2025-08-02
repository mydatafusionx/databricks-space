import pytest
from pyspark.sql import SparkSession

def test_leitura_csv():
    spark = SparkSession.builder.master("local[1]").appName("TestIngestao").getOrCreate()
    data = [(1, "Alice"), (2, "Bob")]
    df = spark.createDataFrame(data, ["id", "nome"])
    assert df.count() == 2
    assert set(df.columns) == {"id", "nome"}
