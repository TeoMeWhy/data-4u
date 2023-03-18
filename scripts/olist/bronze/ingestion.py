# Databricks notebook source
from pyspark.sql.types import StructType
import json
import os

def read_schema(path):
    with open(path, "r") as open_file:
        data = json.load(open_file)
    return StructType.fromJson(data)

def read_data(table):
    
    schema = read_schema(f"schemas/{table}.json")
    
    df = (spark.read
               .schema(schema)
               .format("csv")
               .option("header", "true")
               .option("multiLine", "true")
               .option("sep", ",")
               .load(f"/mnt/datalake/raw/olist/{table}.csv"))
    return df

def save_data(df, table):
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .format("delta")
       .option("overwriteSchema", "true")
       .saveAsTable(f"bronze.olist.{table}"))

# COMMAND ----------

tables = [i.replace(".json", "") for i in os.listdir("schemas") if i.endswith(".json")]

for t in tables:
    df = read_data(t)
    save_data(df, t)
