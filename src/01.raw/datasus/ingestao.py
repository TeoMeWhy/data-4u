# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import urllib.request

from tqdm import tqdm

def get_data_uf_ano_mes(uf, ano, mes):
    url = f"ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD{uf}{ano}{mes}.dbc"

    file_path = f"/dbfs/mnt/datalake/datasus/rd/dbc/RD{uf}{ano}{mes}.dbc" 

    resp = urllib.request.urlretrieve(url, file_path)

def get_data_uf(uf, datas):
    for i in tqdm(datas):
        ano, mes, dia = i.split("-")
        ano = ano[-2:]
        get_data_uf_ano_mes(uf, ano, mes)

ufs = ["RO", "AC", "AM", "RR","PA",
       "AP", "TO", "MA", "PI", "CE",
       "RN", "PB", "PE", "AL", "SE",
       "BA", "MG", "ES", "RJ", "SP",
       "PR", "SC", "RS", "MS", "MT",
       "GO","DF"]

datas = ['2023-01-01']

for uf in ufs:
    print(uf)
    get_data_uf(uf, datas)
