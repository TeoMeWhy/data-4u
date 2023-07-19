# Databricks notebook source
# DBTITLE 1,Imports
import sys

sys.path.insert(0, '../../lib')

from ingestors import IngestaoBronze
import dbtools
import delta

# COMMAND ----------

# DBTITLE 1,Setup
table = "rd_sih"
path_full_load=f'/mnt/datalake/datasus/rd/csv'
path_incremental=f'/mnt/datalake/datasus/rd/csv'
file_format='csv'
table_name=table
database_name='bronze.datasus'
id_fields= ["N_AIH", "DT_SAIDA", "IDENT"]
timestamp_field='DT_SAIDA'
partition_fields=[]
read_options = {'sep': ';','header': "true"}

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
