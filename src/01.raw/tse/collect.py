# Databricks notebook source
import requests
import json
import os
import zipfile
from multiprocessing import Pool

# COMMAND ----------

with open("datasources.json", "r") as open_file:
    datasources = json.load(open_file)

dbutils.widgets.dropdown("tipo_coleta",
                         defaultValue=list(datasources.keys())[0], 
                         choices=datasources.keys(),
                         label="Tipo Coleta")

# COMMAND ----------

datasource = dbutils.widgets.get("tipo_coleta")

url = datasources[datasource]['url']
base_path = datasources[datasource]['path']
if not os.path.exists(base_path):
    os.mkdir(base_path)

anos_start = datasources[datasource]['anos']['start']
anos_stop = datasources[datasource]['anos']['stop']+1
anos_step = datasources[datasource]['anos']['step']
anos = list(range(anos_start, anos_stop, anos_step))

download_path = os.path.join(base_path, "download.zip")

sufix_not_remove = datasources[datasource]['sufix_not_remove']
sufix_manage = datasources[datasource]['sufix_manage']

ufs = [
       'AC', 'AL', 'AP', 'AM',
       'BA',
       'CE',
       'ES',
       'GO',
       'MA', 'MT', 'MS', 'MG',
       'PA', 'PB', 'PR', 'PE', 'PI', 'RJ',
       'RN', 'RS', 'RO', 'RR',
       'SC', 'SP', 'SE', 'TO'
       ]

# COMMAND ----------

def check_sufix(text, sufixes):
    for s in sufixes:
        if text.endswith(s):
            return True
    return False


def get_data(url, download_path):
    response = requests.get(url)
    if response.status_code == 404:
        return response
    with open(download_path, "wb") as open_file:
        open_file.write(response.content)
    return response


def unzip_download(download_path, unzip_path):
    with zipfile.ZipFile(download_path, 'r') as zip_ref:
        zip_ref.extractall(unzip_path)


def remove_files(path, sufix_not_remove=["BR.csv", "BRASIL.csv"]):
    all_files = dbutils.fs.ls(path.replace("/dbfs", ""))
    to_remove = [i.path for i in all_files if not check_sufix(i.name, sufix_not_remove)]
    for i in to_remove:
        dbutils.fs.rm(i)


def manage_files(path, sufix_to_move=['csv', 'txt']):
    db_path = path.replace("/dbfs", "")
    all_files = dbutils.fs.ls(db_path)
    for s in sufix_to_move:
        s_files = [i.path for i in all_files if i.name.endswith(s)]
        dbutils.fs.mkdirs(os.path.join(db_path, s))
        for file in s_files:
            dbutils.fs.mv(file, os.path.join(db_path, s))


def get_years(url, anos, uf, download_path, base_path):
    for i in anos:
        print(f"{uf}-{i}")
        resp = get_data(url.format(ano=i, uf=uf), download_path)
        if resp.status_code != 200:
            print(f"Ignorado, razão: Código {resp.status_code}.")
            continue
        try:
            unzip_download(download_path, base_path)
        except:
            print("erro ao tentar extrair os dados:", i, "-", uf)


# COMMAND ----------

if datasource.endswith("uf"):
    print("Coletando dados de eleições por estado")
    
    data = [(url, anos, uf, download_path.replace("download", f"download_{uf}"), base_path) for uf in ufs]
    with Pool(1) as p:
        p.starmap(get_years, data)

else:
    print("Coletando dados de eleições nível nacional")
    get_years(url, anos, "BR", download_path, base_path)

print("Movendo arquivos...")
manage_files(base_path)

for i in sufix_manage:
    remove_path = os.path.join(base_path.replace("/dbfs", ""), i)
    remove_files(remove_path, sufix_not_remove)
