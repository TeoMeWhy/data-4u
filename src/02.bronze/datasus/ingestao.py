# Databricks notebook source
import sys

sys.path.insert(0, '../../lib')

from ingestors import IngestaoBronze
import dbtools

# COMMAND ----------

table = "rd_sih"
path_full_load=f'/mnt/datalake/datasus/rd/csv'
path_incremental=f'/mnt/datalake/datasus/rd/csv'
file_format='csv'
table_name=table
database_name='bronze.datasus'
id_fields= ["N_AIH", "DT_SAIDA", "IDENT"]
timestamp_field='DT_SAIDA'
partition_fields=["ANO_CMPT","MES_CMPT"]
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

if not dbtools.table_exists(spark, database_name, table):
    df_null = spark.createDataFrame(data=[], schema=ingestao.schema)
    ingestao.save_full(df_null)
    dbutils.fs.rm(ingestao.checkpoint_path, True)

# COMMAND ----------

ingestao.process_stream()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 53834305
# MAGIC -- 53834305
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bronze.datasus.rd_sih

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*),
# MAGIC        count(distinct N_AIH, IDENT),
# MAGIC        count(distinct N_AIH, DT_SAIDA, IDENT)
# MAGIC FROM bronze.datasus.sih
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with tb_group As (
# MAGIC   SELECT 
# MAGIC         N_AIH, DT_SAIDA,
# MAGIC         COUNT(*)
# MAGIC   FROM bronze.datasus.sih
# MAGIC
# MAGIC   GROUP BY N_AIH, DT_SAIDA
# MAGIC
# MAGIC   having COUNT(*) > 1
# MAGIC
# MAGIC   order by 3 desc
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM bronze.datasus.sih
# MAGIC WHERE N_AIH IN (select N_AIH from tb_group)
# MAGIC and DT_SAIDA in (select DT_SAIDA from tb_group)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM TB_SUS WHERE N_AIH = 2708103094323
