# Databricks notebook source
# DBTITLE 1,Funções
import datetime
import json
import os
import requests

def get_twitch_token(client_secret, client_id):

    params = {
        "client_secret" : client_secret,
        "client_id" : client_id,
        "grant_type" : "client_credentials",
        }

    url = "https://id.twitch.tv/oauth2/token"
    resp = requests.post(url, params=params)
    data = resp.json()

    token = data['access_token']
    return token


class Ingestor:

    def __init__(self, token, client_id, delay, path) -> None:
        self.headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
        }
        self.base_url = 'https://api.igdb.com/v4/{sufix}'
        self.delay = delay
        self.delay_timestamp = datetime.datetime.now() - datetime.timedelta(days=delay)
        self.delay_timestamp = int(self.delay_timestamp.timestamp())
        self.path = path

    def get_data(self, sufix, params={}):

        url = self.base_url.format(sufix=sufix)
        data = requests.get(url, headers=self.headers, params=params)
        return data.json()
    
    def save_data(self, data, sufix):

        name = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")

        with open(f'{self.path}/{sufix}/{name}.json', 'w') as open_file:
            json.dump(data, open_file)
        return True

    def get_and_save(self, sufix, params):
        data = self.get_data(sufix, params)
        self.save_data(data, sufix)
        return data

    def process(self, sufix, **params):
        default = {
            'fields': '*',
            'limit': 500,
            'offset' : 0,
            'order': 'updated_at:desc',
        }

        default.update(params)
        
        print("Iniciando loop...")
        while True:
        
            print("Obtendo dados...")
            data = self.get_and_save(sufix, default)
            updated_timestamp = int(data[-1]['updated_at'])
            print(updated_timestamp, "... Ok.")

            if len(data) < 500 or updated_timestamp < self.delay_timestamp:
                print("Finalizando loop...")
                return True
        
            default['offset'] += default['limit']
        
def collect(endpoint, delay, path, **params):
    client_secret = dbutils.secrets.get('twitch', 'CLIENT_SECRET')
    client_id = dbutils.secrets.get('twitch', 'CLIENT_ID')

    if not os.path.exists(f'{path}/{endpoint}'):
        os.mkdir(f'{path}/{endpoint}')

    print('Obtendo token da twitch...')
    token = get_twitch_token(client_secret, client_id)
    print('Ok.\n')

    print('Criando classe de ingestão...')
    ingestor = Ingestor(token, client_id, delay, path)
    print('Ok.\n')

    print('Iniciando o processo...')
    ingestor.process(endpoint, **params)
    print('Ok.\n')

# COMMAND ----------

# DBTITLE 1,Setup
endpoint = dbutils.widgets.get('endpoint')
delay = int(dbutils.widgets.get('delay'))

path = '/dbfs/mnt/datalake/igdb'

# COMMAND ----------

# DBTITLE 1,Execução
print('\n############################################')
print('Executando para endpoint:', endpoint)
collect(endpoint=endpoint, delay=delay, path=path)
