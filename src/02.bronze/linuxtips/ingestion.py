# Databricks notebook source
import sys

sys.path.insert(0, '../../lib')

from ingestors import IngestaoBronze

# COMMAND ----------


path_full_load = '/mnt/datalake/linuxtips/pizza_query_forms'
path_incremental = '/mnt/datalake/linuxtips/pizza_query_forms'
file_format = 'csv'
table_name = 'pizza_query_forms'
database_name = 'bronze.linuxtips'
id_fields = ['id']
timestamp_field = 'updated_at'
partition_fields = ''

options = {'header':'true', 'sep':';'}

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
