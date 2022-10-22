# Databricks notebook source
dbutils.widgets.dropdown(name="table", defaultValue="pagamento_auxilio", choices=['pagamento_auxilio'])
table = dbutils.widgets.get("table")

# COMMAND ----------

# DBTITLE 1,Setup
import json
import os
from pyspark.sql import types

def read_json(path):
    with open(path, 'r') as open_file:
        schema = json.load(open_file)
    return schema

  
config = read_json("tables.json")['ministerio_cidadania']

schema = read_json(f"schemas/ministerio_cidadania/{table}.json")
schema = types.StructType.fromJson(schema)

# COMMAND ----------

# DBTITLE 1,Read Full Load
path = os.path.join(config['raw_path'], table)

df = spark.read.format(config['format']).schema(schema).load(path)
df.display()

# COMMAND ----------

# DBTITLE 1,Save Full-load
df.coalesce(1).write.format("delta").mode("overwrite").saveAsTable("bronze_ministerio_cidadania.pagamento_auxilio")
