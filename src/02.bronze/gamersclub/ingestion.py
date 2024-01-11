# Databricks notebook source
import sys

sys.path.insert(0, '../../lib/')

from ingestors import IngestaoBronze
import dbtools

# COMMAND ----------

database_name = 'bronze.gamersclub'
table_name = dbutils.widgets.get('table_name')
file_format = 'csv'
id_fields = []
timestamp_field = ''
partition_fields = []

path_full_load = f"/mnt/datalake/gc/{table_name}.csv"
path_incremental = ''

read_options = {
        "header": "true",
        "multiLine": "true",
        "sep": ",",
    }

# COMMAND ----------


ingest = IngestaoBronze(
            path_full_load = path_full_load,
            path_incremental = path_incremental,
            file_format = file_format,
            table_name = table_name,
            database_name = database_name,
            id_fields = id_fields,
            timestamp_field = timestamp_field,
            partition_fields = partition_fields,
            read_options=read_options,
            spark = spark,
            )

# COMMAND ----------

if not dbtools.table_exists(spark, database_name, table_name):
    print("Tabela não existente, realizando primeira carga...")
    ingest.process_full()
    print("Ok")

