# Databricks notebook source
# MAGIC %pip install gspread

# COMMAND ----------

import gspread
import pandas as pd

url = dbutils.widgets.get('url')
sheet_id = int(dbutils.widgets.get('sheet_id'))

gc = gspread.service_account('credentials.json')
sheet = gc.open_by_url(url)

worksheet = sheet.get_worksheet_by_id(sheet_id)
data = worksheet.get()

# COMMAND ----------

pdf = pd.DataFrame(data[1:], columns=data[0]).reset_index()
sdf = spark.createDataFrame(pdf)
(sdf.write
    .format('csv')
    .mode('append')
    .option('header', 'true')
    .option('sep', ';')
    .save('/mnt/datalake/linuxtips/pizza_query_forms'))
