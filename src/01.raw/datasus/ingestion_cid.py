# Databricks notebook source
# MAGIC %pip install lxml

# COMMAND ----------

import pandas as pd

url = 'http://tabnet.datasus.gov.br/cgi/sih/mxcid10lm.htm'

dfs = pd.read_html(url)
df = dfs[2]
df.columns = df.columns.levels[1]

# COMMAND ----------

df.to_csv("/dbfs/mnt/datalake/datasus/cid/volume1.csv", index=False, sep=";")

# COMMAND ----------

df
