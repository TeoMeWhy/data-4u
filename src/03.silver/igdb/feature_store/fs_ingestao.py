# Databricks notebook source
import sys

sys.path.insert(0, '../../../lib/')

import dbtools
import dttools
from databricks import feature_store

table = dbutils.widgets.get('table')                                  # parametro
fs_name = f'igdb_{table}'
database = 'feature_store'
fs_full_name = f'{database}.{fs_name}'
description = dbutils.widgets.get('description')                      # parametro
id_fields = dbutils.widgets.get('id_fields').split(',')               # parametro
partition_fields = dbutils.widgets.get('partition_fields').split(',') # parametro

date_start = dbutils.widgets.get('date_start')                        # parametro
date_stop = dbutils.widgets.get('date_stop')                          # parametro
period = 'monthly' if dbutils.widgets.get('monthly') == 'True' else 'daily'


# parametro
dates = dttools.date_range(date_start, date_stop, period=period)

query = dbtools.import_query(f'etl/{table}.sql')

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

fl_table_exists = dbtools.table_exists(spark=spark, database=database, table=fs_name)

for i in dates:
    print(i)
    df = spark.sql(query.format(date=i))

    if not fl_table_exists:
        fs.create_table(name=fs_full_name,
                        primary_keys=id_fields,
                        partition_columns=partition_fields,
                        description=description,
                        schema=df.schema)
        
        fl_table_exists = True

    fs.write_table(name=fs_full_name, df=df, mode='merge')
