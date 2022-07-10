# Databricks notebook source
config = json.load(open("tables.json", 'r'))

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("Datasource", "censo_escolar", config.keys())

# COMMAND ----------

import json

# COMMAND ----------

datasource = dbutils.widgets.get("Datasource")

config = config[datasource]

database = f"bronze_{datasource}"
path = config["raw_path"]

print(f"Database: {database}")

# COMMAND ----------

def get_file_tables(path):
    return [(i.path, i.name.split(".")[0]) for i in dbutils.fs.ls(path)]

def ingestion(file_table, database, config):
    path, table = file_table
    database_table = f"{database}.{table}"
    
    df = prepare_read(config).load(path)
    
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .format("delta")
       .saveAsTable(database_table))
    
    return df
  
def prepare_read(config):
    
    reader = spark.read.format(config["format"])
    
    if config["format"] in ["csv", "txt"]:
        reader = reader.option("header", "true")
    
    if "sep" in config:
        reader = reader.option("sep", config["sep"])
    
    if "encoding" in config:
        reader = reader.option("encoding", config["encoding"])
    
    return reader

# COMMAND ----------

file_tables = get_file_tables(path)

for f in file_tables:
    ingestion(file_table=f, database=database, config=config)
