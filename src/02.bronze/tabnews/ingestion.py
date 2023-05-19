# Databricks notebook source
import sys

sys.path.insert(0, '../../lib/')

from ingestors import IngestaoBronze
import dbtools

# COMMAND ----------

# DBTITLE 1,Setup
database_name = 'bronze.tabnews'
table_name = dbutils.widgets.get('table_name')
file_format = dbutils.widgets.get('file_format')
id_fields = dbutils.widgets.get('id_fields').split(',')
timestamp_field = dbutils.widgets.get('timestamp_field')
partition_fields = dbutils.widgets.get('partition_fields').split(',')

path_full_load = f"/mnt/datalake/tabnews/{table_name}/"
path_incremental = f"/mnt/datalake/tabnews/{table_name}/"

# COMMAND ----------

# DBTITLE 1,Instanciando ingestor

ingest = IngestaoBronze(
            path_full_load = path_full_load,
            path_incremental = path_incremental,
            file_format = file_format,
            table_name = table_name,
            database_name = database_name,
            id_fields = id_fields,
            timestamp_field = timestamp_field,
            partition_fields = partition_fields,
            spark = spark,
            )

# COMMAND ----------

# DBTITLE 1,Carga full-load
if not dbtools.table_exists(spark, 'bronze.tabnews', 'contents'):
    print("Tabela não existente, realizando primeira carga...")
    ingest.process_full()
    print("Ok")

# COMMAND ----------

# DBTITLE 1,Stream
stream = ingest.process_stream()
