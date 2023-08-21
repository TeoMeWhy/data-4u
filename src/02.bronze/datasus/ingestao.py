# Databricks notebook source
# DBTITLE 1,Imports
import sys

sys.path.insert(0, '../../lib')

from ingestors import IngestaoBronze
import dbtools
import delta
import json

# COMMAND ----------

# DBTITLE 1,Setup
table_name="sinasc"
database_name='bronze.datasus'

with open("sources.json", "r") as open_file:
    config = json.load(open_file)[table_name]


ingestao = IngestaoBronze(
            table_name=table_name,
            database_name=database_name,
            spark=spark,
            **config,
)

# COMMAND ----------

# DBTITLE 1,Criação da tabela
if not dbtools.table_exists(spark, database_name, table):
    df_null = spark.createDataFrame(data=[], schema=ingestao.schema)
    ingestao.save_full(df_null)
    dbutils.fs.rm(ingestao.checkpoint_path, True)

# COMMAND ----------

# DBTITLE 1,Ingestão por streaming
stream = ingestao.process_stream()

# COMMAND ----------

stream.awaitTermination()

# COMMAND ----------

table = delta.DeltaTable.forName(spark, f"{database_name}.{table_name}")
table.vacuum()
