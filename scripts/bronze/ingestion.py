# Databricks notebook source
import json
import os

from pyspark.sql.types import *

config = json.load(open("tables.json", 'r'))

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("Datasource", "censo_escolar", config.keys())

# COMMAND ----------

datasource = dbutils.widgets.get("Datasource")

config = config[datasource]

database = f"bronze_{datasource}"
path = config["raw_path"]

print(f"Database: {database}")

# COMMAND ----------

def read_schema(path):
    if os.path.exists(path):
        schema = json.load(open(path,'r'))
        return StructType.fromJson(schema)
    else:
        return None

def get_file_tables(path):
    return [(i.path, i.name.split(".")[0]) for i in dbutils.fs.ls(path)]

def ingestion(file_table, database, datasource, config):
    path, table = file_table
    database_table = f"{database}.{table}"

    schema = read_schema(f"schemas/{datasource}/{table}.json".lower())
    
    df = prepare_read(config, schema).load(path)
    
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .format("delta")
       .saveAsTable(database_table))
    
    return df
  
def prepare_read(config, schema=None):
    
    reader = spark.read.format(config["format"])
    
    if schema != None:
        reader = reader.schema(schema)
    
    if config["format"] in ["csv", "txt"]:
        reader = reader.option("header", "true")
    
    if "sep" in config:
        reader = reader.option("sep", config["sep"])
    
    if "encoding" in config:
        reader = reader.option("encoding", config["encoding"])
        
    if "multiline" in config:
        reader = reader.option("multiline", "true")

    return reader

# COMMAND ----------

file_tables = get_file_tables(path)

for f in file_tables:
    print(f)
    ingestion(file_table=f, database=database, datasource=datasource, config=config)
