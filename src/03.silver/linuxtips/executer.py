# Databricks notebook source
def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()

# COMMAND ----------

table_name = dbutils.widgets.get("table_name")
query = import_query(f"etl/{table_name}.sql")
spark.sql(query)
