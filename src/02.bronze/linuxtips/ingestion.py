# Databricks notebook source
import sys

sys.path.insert(0, '../../lib')

from ingestors import IngestaoBronze

# COMMAND ----------

# dbutils.widgets.dropdown('table_name', 'forms', choices=['forms', 'produtos'])

# COMMAND ----------

table_origin = dbutils.widgets.get('table_name')

path_full_load = f'/mnt/datalake/linuxtips/pizza_query_{table_origin}'
path_incremental = f'/mnt/datalake/linuxtips/pizza_query_{table_origin}'
file_format = 'csv'
table_name = f'pizza_query_{table_origin}'
database_name = 'bronze.linuxtips'
id_fields = ['id']
timestamp_field = 'updated_at'
partition_fields = ''

options = {'header':'true', 'sep':';', 'multiLine': 'true'}

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
            read_options = options,
            spark = spark,
            )

# COMMAND ----------

ingest.process_full()
