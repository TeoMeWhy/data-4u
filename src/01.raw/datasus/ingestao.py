# Databricks notebook source

# COMMAND ----------

import urllib.request
from multiprocessing import Pool
from tqdm import tqdm
import json

import datetime
from dateutil.relativedelta import relativedelta

import sys
sys.path.insert(0,"../../lib/")

import dttools

class IngestionRawSUS:

    def __init__(self, ufs, date_range, source, n_jobs=2):
        self.ufs = ufs
        self.n_jobs = n_jobs

        with open('datasources.json', 'r') as open_file:
            datasources = json.load(open_file)

        self.origin = datasources[source]['origin']
        self.target = datasources[source]['target']
        self.period = datasources[source]['period']

        self.dates=[]
        self.set_dates(date_range)

    def set_dates(self, date_range):
        self.dates = dttools.date_range(date_range[0], date_range[-1], period=self.period)


    def get_data_uf_ano_mes(self, uf, ano, mes):
        
        url = self.origin.format(uf=uf, ano=ano, mes=mes)
        file_path = self.target.format(uf=uf, ano=ano, mes=mes)

        try:
            resp = urllib.request.urlretrieve(url, file_path)

        except:
            print(f"Não foi possível coletar o arquivo.  {uf} | {ano}-{mes}-01")

    def get_data_uf(self, uf):
        for i in tqdm(self.dates):
            ano, mes, dia = i.split("-")
            
            if self.period == 'monthly':
                ano = ano[-2:]

            self.get_data_uf_ano_mes(uf, ano, mes)


    def auto_execute(self):
        with Pool(self.n_jobs) as pool:
            pool.map(self.get_data_uf, self.ufs)


# COMMAND ----------

datasource = dbutils.widgets.get("datasource")
dt_start = dbutils.widgets.get("dt_start")
dt_stop = dbutils.widgets.get("dt_stop")
delay = int(dbutils.widgets.get("delay"))

dt_start = (datetime.datetime.strptime(dt_start, "%Y-%m-%d") - relativedelta(months=delay)).strftime("%Y-%m-01")


ufs = ["RO", "AC", "AM", "RR","PA",
       "AP", "TO", "MA", "PI", "CE",
       "RN", "PB", "PE", "AL", "SE",
       "BA", "MG", "ES", "RJ", "SP",
       "PR", "SC", "RS", "MS", "MT",
       "GO", "DF"]

ufs.sort(reverse=True)


ing = IngestionRawSUS(ufs=ufs,
                      date_range=[dt_start, dt_stop],
                      source=datasource,
                      n_jobs=10)

# COMMAND ----------

ing.auto_execute()
