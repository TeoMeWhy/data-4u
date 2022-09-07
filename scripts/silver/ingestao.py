# Databricks notebook source
import os

databases = [ i for i in os.listdir() if os.path.isdir(i)]

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("Database", "olist", databases)

particoes = ["ANO_ELEICAO", "None"]
dbutils.widgets.dropdown("Particao", "None", particoes)

# COMMAND ----------

def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query
  
def ingestion(database, table, partition="None"):
    query = import_query(f"{database}/{table}.sql")
    df = spark.sql(query)
    
    writer = (df.coalesce(1)
                .write
                .mode("overwrite")
                .format("delta")
                .option("overwriteSchema","true"))
    
    if partition != "None":
        writer = writer.partitionBy(partition)
    
    writer.saveAsTable(f"silver_{database}.{table}")
    
def ingestion_many(database, partition):
    tables = [i.split(".")[0] for i in os.listdir(database) if i.endswith(".sql")]
    for t in tables:
        print(t)
        ingestion(database, t, partition)

# COMMAND ----------

database = dbutils.widgets.get("Database")
partition = dbutils.widgets.get("Particao")

ingestion_many(database, partition)
