# Databricks notebook source
import datetime
import pandas as pd

# COMMAND ----------

path = dbutils.widgets.get("path")

files = dbutils.fs.ls(path)

times = [datetime.datetime.fromtimestamp(int(str(i.modificationTime)[:-3])).date() for i in files]

df_files = pd.DataFrame( {"files": [i.path for i in files],
                          "data":times})

to_remove = df_files[df_files['data']<df_files['data'].max()]

# COMMAND ----------

toremove_list = to_remove['files'].tolist()
for i in toremove_list:
    dbutils.fs.rm(i)
