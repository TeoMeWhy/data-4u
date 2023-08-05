# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import requests
import json
from multiprocessing import Pool

from tqdm import tqdm

from pyspark.sql import functions as F
from pyspark.sql import types


class IngestorMatchDetails:

    def __init__(self, url:str, api_key, pool_size:int):
        self.url = url
        self.api_key = api_key
        self.pool_size = pool_size

    def get_resp(self, match_id):
        url = self.url + match_id if self.url.endswith("/") else self.url + "/" + str(match_id)
        resp = requests.get(url, params={"api_key":self.api_key})
        return resp

    def save_data(self, data):
        try:
            path = f"/dbfs/mnt/datalake/dota/matches_details/{data['match_id']}.json"
            with open(path, "w") as open_file:
                json.dump(data, open_file)

        except KeyError as err:
            print("Id da partida não encontrada")
            print("A API pode ter estourado o rate limit")
            print(data)
    
    def get_match_list(self):
        df_pro_matches = spark.read.format("parquet").load("/mnt/datalake/dota/pro_matches/")
        df_collect = spark.read.format("delta").load("/mnt/datalake/dota/collect/")

        ids = (df_pro_matches.join(df_collect,
                                   df_pro_matches.match_id==df_collect.match_id,
                                   how='left')
                               .filter(df_collect.match_id.isNull())
                               .select(df_pro_matches.match_id)
                               .toPandas()['match_id']
                               .tolist())
        return ids
    
    def get_and_save(self, match_id):
        resp = self.get_resp(match_id)
        data = resp.json()
        self.save_data(data)

    def auto_execute(self):
        print("Obtendo lista de partidas a serem coletadas...")
        ids = self.get_match_list()
        
        print(f"Iniciando coleta de dados de forma paralela. {len(ids)} partidas...")

        with Pool(self.pool_size) as p:
            p.map(self.get_and_save, ids)

# COMMAND ----------

try:
    api_key = dbutils.secrets.get("dota", "opendota_api")
    print("API KEY importada com sucesso!")
except:
    api_key = None
    print("API KEY não foi importada!")

pool_size = int(dbutils.widgets.get("pool_size"))

url = "https://api.opendota.com/api/matches"
ing = IngestorMatchDetails(url, api_key, pool_size)
ing.auto_execute()
