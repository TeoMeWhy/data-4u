# Databricks notebook source
import json
import os
import requests
from zipfile import *
import shutil

def read_config(path):
    with open(path, 'r') as open_file:
        config = json.load(open_file)
    return config

def download_data(url, filename, **kwargs):
    response = requests.get(url, **kwargs)

    with open(filename, 'wb') as open_file:
        open_file.write(response.content)
    return True

def unzip_files(filename, target_path):
    with ZipFile(filename, "r") as zipObj:
        zipObj.extractall(path=target_path)

# COMMAND ----------

# contexto = dbutils.widgets.get("contexto")
# ano = dbutils.widgets.get("ano")

contexto = 'candidatura'
ano = '2022'

config = read_config("config.json")

url = config['source'][contexto][ano]
cookies = config['cookies']
headers = config['headers']

raw_db = "/dbfs/mnt/datalake/raw/tse/"
path_raw = f"/dbfs/mnt/datalake/raw/tse/{contexto}/{ano}"

if not os.path.exists( os.path.dirname(path_raw)):
    os.mkdir(os.path.dirname(path_raw))
    
if not os.path.exists(path_raw):
    os.mkdir(path_raw)

# COMMAND ----------

# DBTITLE 1,Download Data
filename = os.path.join(path_raw, "data.zip")

download_data(url, filename, headers=headers, cookies=cookies)

# COMMAND ----------

# DBTITLE 1,Unzip
unzip_files(filename, path_raw)

# COMMAND ----------

# DBTITLE 1,Copiando dado para tse
file_brasil = [i for i in os.listdir(path_raw) if "BRASIL" in i][0]

path_brasil_origin = os.path.join(path_raw, file_brasil)
path_brasil_target = os.path.join(raw_db, file_brasil)

shutil.copyfile(path_brasil_origin, path_brasil_target)
