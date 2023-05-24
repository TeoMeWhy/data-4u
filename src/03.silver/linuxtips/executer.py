# Databricks notebook source
def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()
    
def execute_many(query, spark):
    queries = query.split(";")[:-1]
    for q in queries:
        spark.sql(q)

# COMMAND ----------

table_name = dbutils.widgets.get("table_name")
query = import_query(f"etl/{table_name}.sql")
execute_many(query, spark)
