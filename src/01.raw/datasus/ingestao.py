# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import urllib.request
from multiprocessing import Pool
from tqdm import tqdm

import datetime
from dateutil.relativedelta import relativedelta

import sys
sys.path.insert(0,"../../lib/")

import dttools

def get_data_uf_ano_mes(uf, ano, mes):
    url = f"ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD{uf}{ano}{mes}.dbc"

    file_path = f"/dbfs/mnt/datalake/datasus/rd/dbc/landing/RD{uf}{ano}{mes}.dbc" 

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


# COMMAND ----------

dt_start = dbutils.widgets.get("dt_start")
dt_stop = dbutils.widgets.get("dt_stop")
delay = int(dbutils.widgets.get("delay"))

dt_start = (datetime.datetime.strptime(dt_start, "%Y-%m-%d") - relativedelta(months=delay)).strftime("%Y-%m-01")

datas = dttools.date_range(dt_start, dt_stop, monthly=True)
to_download = [(uf, datas) for uf in ufs]

with Pool(10) as pool:
    pool.starmap(get_data_uf, to_download)
