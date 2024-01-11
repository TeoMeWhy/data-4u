# Databricks notebook source
import sys

sys.path.insert(0, '../../lib')

import dbtools

# COMMAND ----------

database = "silver.gamersclub"
table_name = dbutils.widgets.get("table_name")

query = dbtools.import_query(f"sql/{table_name}.sql")

# COMMAND ----------

(spark.sql(query)
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("delta")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{database}.{table_name}")
)
