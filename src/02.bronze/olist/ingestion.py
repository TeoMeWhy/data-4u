# Databricks notebook source
import sys

sys.path.insert(0, '../../lib/')

from ingestors import IngestaoBronze
import dbtools

# COMMAND ----------

# DBTITLE 1,Setup
database_name = 'bronze.olist'
table_name = dbutils.widgets.get('table_name')
file_format = 'csv'
id_fields = []
timestamp_field = ''
partition_fields = ''

path_full_load = f"/mnt/datalake/olist/{table_name}.csv"
path_incremental = ''

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
if not dbtools.table_exists(spark, database_name, table_name):
    print("Tabela não existente, realizando primeira carga...")

    options = {
        "header": "true",
        "multiLine": "true",
        "sep": ",",
    }
    
    ingest.process_full(**options)
    print("Ok")

