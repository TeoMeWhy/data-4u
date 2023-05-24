# Databricks notebook source
# MAGIC %pip install gspread

# COMMAND ----------

dbutils.widgets.dropdown('sheetID', 'forms', choices=['forms', 'produtos'])

# COMMAND ----------

import gspread
import pandas as pd

url = 'https://docs.google.com/spreadsheets/d/1NKx9GxiYC5ExDiQJg9OPgdiKktri6Sjn-FHZQQ6RoKM/edit?resourcekey#gid=1571995799'

sheets = {
    'forms': 1571995799,
    'produtos': 789410206
}

sheet_name = dbutils.widgets.get('sheetID')
sheet_id = sheets[sheet_name]

gc = gspread.service_account('credentials.json')
sheet = gc.open_by_url(url)

worksheet = sheet.get_worksheet_by_id(sheet_id)
data = worksheet.get()

# COMMAND ----------

pdf = pd.DataFrame(data[1:], columns=data[0]).reset_index()
sdf = spark.createDataFrame(pdf)
(sdf.write
    .format('csv')
    .mode('overwrite')
    .option('header', 'true')
    .option('sep', ';')
    .save(f'/mnt/datalake/linuxtips/pizza_query_{sheet_name}'))
