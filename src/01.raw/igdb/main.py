import argparse
import datetime
import json
import multiprocessing
import os
import requests

import boto3
import dotenv
from tqdm import tqdm

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

    def __init__(self, token, client_id, delay) -> None:
        self.headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
        }
        self.base_url = 'https://api.igdb.com/v4/{sufix}'
        self.delay = delay
        self.delay_timestamp = int((datetime.datetime.now() - datetime.timedelta(days=delay)).timestamp())

    def get_data(self, sufix, params={}):

        url = self.base_url.format(sufix=sufix)
        data = requests.get(url, headers=self.headers, params=params)
        return data.json()
    
    def save_data(self, data, sufix):

        name = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")

        with open(f'data/{sufix}/{name}.json', 'w') as open_file:
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
        

def collect(endpoint, delay, **params):
    client_secret = os.getenv("CLIENT_SECRET")
    client_id = os.getenv("CLIENT_ID")

    if not os.path.exists(f"data/{endpoint}"):
        os.mkdir(f"data/{endpoint}")

    print("Obtendo token da twitch...")
    token = get_twitch_token(client_secret, client_id)
    print("Ok.\n")

    print("Criando classe de ingestão...")
    ingestor = Ingestor(token, client_id, delay)
    print("Ok.\n")

    print("Iniciando o processo...")
    ingestor.process(endpoint, **params)
    print("Ok.\n")
    
def file_to_s3(filepath, s3_client, bucket_name):

    *_, endpoint, filename = filepath.strip("/").split("/")

    s3_path = f'igdb/{endpoint}/{filename}'

    with open(filepath) as open_file:
            data = json.dumps(json.load(open_file), indent=3)
            s3_client.put_object(Body=data, Bucket=bucket_name, Key=s3_path)
    os.remove(filepath)


def files_to_s3(filepaths):

    bucket_name = os.getenv("BUCKET_NAME")

    session = boto3.Session(profile_name=PROFILE_AWS)

    client = session.client('s3')
    for i in tqdm(filepaths):
        file_to_s3(i, client, bucket_name)

def export(endpoint, n_jobs=1):

    filespaths = [f"data/{endpoint}/{i}"  for i in os.listdir(f"data/{endpoint}/")]
    slices = [filespaths[i-1::n_jobs] for i in range(1, n_jobs+1)]
    with multiprocessing.Pool(n_jobs) as pool:
        pool.map(files_to_s3, slices)


parser = argparse.ArgumentParser()
parser.add_argument('--endpoint', type=str)
parser.add_argument('--mode', type=str, choices=['collect', 'export', 'all'])
parser.add_argument('--delay', type=int, default=1)
parser.add_argument('--n_jobs', type=int, default=1)
parser.add_argument('--profile_aws', type=str)

args = parser.parse_args()

PROFILE_AWS = args.profile_aws

dotenv.load_dotenv(dotenv.find_dotenv('.env'))

print("\n############################################")
print("Executando para endpoint:", args.endpoint)

if args.mode == 'collect':
    collect(args.endpoint, delay=args.delay)

elif args.mode == 'export':
    export(args.endpoint, args.n_jobs)

elif args.mode == 'all':
    collect(args.endpoint, delay=args.delay)
    export(args.endpoint, args.n_jobs)
