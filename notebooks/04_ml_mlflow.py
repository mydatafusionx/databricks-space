# Databricks notebook source
# MAGIC %md
# # Machine Learning com MLflow
# MAGIC 
# MAGIC Exemplo de pipeline de ML rastreado pelo MLflow.

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pandas as pd

# Carregar dados de exemplo (ajuste para seu dado real)
df = pd.read_parquet("data/silver/")

# Ajuste as colunas conforme seu dado
# X = df[["feature1", "feature2"]]
# y = df["target"]

# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# with mlflow.start_run():
#     model = RandomForestClassifier()
#     model.fit(X_train, y_train)
#     preds = model.predict(X_test)
#     acc = accuracy_score(y_test, preds)
#     mlflow.log_metric("accuracy", acc)
#     mlflow.sklearn.log_model(model, "model")
#     print(f"Acurácia: {acc}")
