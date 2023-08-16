# Databricks notebook source
# DBTITLE 1,Imports
import sys

sys.path.insert(0, '../../lib')

import dbtools

# COMMAND ----------

# DBTITLE 1,Setup
table = dbutils.widgets.get("table")

database = 'silver.dota'
table_full_name = f'{database}.{table}'

# COMMAND ----------

# DBTITLE 1,Execução
query = dbtools.import_query(f'etl/{table}.sql')

(spark.sql(query)
      .write
      .format('delta')
      .mode('overwrite')
      .option('overwriteSchema', 'true')
      .saveAsTable(table_full_name))
