# Databricks notebook source
from pyspark.sql import types

import sys

sys.path.insert(0, '../../lib/')

import ingestors
import dbtools

# COMMAND ----------

path_full_load = "/mnt/datalake/dota/matches_details"
path_incremental = "/mnt/datalake/dota/matches_details"
file_format = "json"
table_name = dbutils.widgets.get("table_name")
database_name = "bronze.dota"
id_fields = dbutils.widgets.get("id_fields").split(",")
timestamp_field = "start_time"
partition_fields = []
read_options = {}

# COMMAND ----------

ing = ingestors.IngestaoBronze(
    path_full_load = path_full_load,
    path_incremental = path_incremental,
    file_format = file_format,
    table_name = table_name,
    database_name = database_name,
    id_fields = id_fields,
    timestamp_field = timestamp_field,
    partition_fields = partition_fields,
    read_options = read_options,
    spark = spark,
)

# COMMAND ----------

if not dbtools.table_exists(spark, database_name, table_name):
    print("Criando a tabela")
    df_null = spark.createDataFrame(data=[], schema=ing.schema)
    ing.save_full(df_null)
    dbutils.fs.rm(ing.checkpoint_path, True)

else:
    print("Tabela já existente")

# COMMAND ----------

stream = ing.process_stream()
