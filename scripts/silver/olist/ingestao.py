# Databricks notebook source
import os

def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query


def ingestion_table(table_name):

    query = import_query(f"sql/{table_name}.sql")

    (spark.sql(query)
     .coalesce(1)     
     .write
     .mode("overwrite")
     .format("delta")
     .option("overwriteSchema", "true")
     .saveAsTable(f"silver_olist.{table_name}"))

# COMMAND ----------

table_names = [i.replace(".sql", "") for i in os.listdir("sql")]
table_names

for t in table_names:
    print(t)
    ingestion_table(t)