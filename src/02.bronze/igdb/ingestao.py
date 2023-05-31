# Databricks notebook source
import sys

sys.path.insert(0, '../../lib')

from ingestors import IngestaoBronze
import dbtools

# COMMAND ----------

table = 'games'
path_full_load=f'/mnt/datalake/igdb/{table}'
path_incremental=f'/mnt/datalake/igdb/{table}'
file_format='json'
table_name=table
database_name='bronze.igdb'
id_fields=['id']
timestamp_field='updated_at'
partition_fields=[]
read_options = {'multiLine': 'true'}

# COMMAND ----------

ingestao = IngestaoBronze(
            path_full_load=path_full_load,
            path_incremental=path_incremental,
            file_format=file_format,
            table_name=table_name,
            database_name=database_name,
            id_fields=id_fields,
            timestamp_field=timestamp_field,
            partition_fields=partition_fields,
            read_options=read_options,
            spark=spark,
)

# COMMAND ----------

# DBTITLE 1,Full Load
if not dbtools.table_exists(spark, database_name, table):
    df_null = spark.createDataFrame(data=[], schema=ingestao.schema)
    dbutils.fs.rm(ingestao.checkpoint_path, True)

# COMMAND ----------

# DBTITLE 1,Stream
ingestao.process_stream()
