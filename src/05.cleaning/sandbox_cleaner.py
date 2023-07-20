# Databricks notebook source
import sys

sys.path.insert(0, "../lib")

import dbtools

# COMMAND ----------

catalogo = 'sandbox'

databases = dbtools.get_schemas_from_catalog(spark, catalogo, ["information_schema"])

dfs = []
for i in databases:
    tables = dbtools.get_tables_from_database(spark, f"{catalogo}.{i}")
    df = pd.DataFrame({"table": tables})
    df['database'] = i
    df['name'] = catalogo + "." + df['database'] + "." + df['table']
    dfs.append(df)

df_full = pd.concat(dfs)

print(df_full)

# COMMAND ----------

for i in df_full['name']:
    spark.sql(f'DROP TABLE IF EXISTS {i}')
