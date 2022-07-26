# Databricks notebook source
import os

databases = [ i for i in os.listdir() if os.path.isdir(i)]

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("Database", "olist", databases)

# COMMAND ----------

def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query
  
def ingestion(database, table):
    query = import_query(f"{database}/{table}.sql")
    df = spark.sql(query)
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .format("delta")
       .option("overwriteSchema","true")
       .saveAsTable(f"silver_{database}.{table}"))
    
def ingestion_many(database):
    tables = [i.split(".")[0] for i in os.listdir(database)]
    for t in tables:
        print(t)
        ingestion(database, table)

# COMMAND ----------

database = dbutils.widgets.get("Database")
ingestion_many(database)
