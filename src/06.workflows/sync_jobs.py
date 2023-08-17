# %%

import json
import dotenv
import os
import requests
import pandas as pd
from tqdm import tqdm

# %%

dotenv.load_dotenv(dotenv.find_dotenv(".env"))
DATABRICKS_WORKSPACE_TOKEN = os.getenv("DATABRICKS_WORKSPACE_TOKEN")
DATABRICKS_WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL")

class JobAPI:
    def __init__(self, url, token):
        self.base_url = f"{url}/api/2.0/jobs"
        self.token = token
        self.headers = {"Authorization": f"Bearer {token}"}

    def list_jobs(self):
        url = f"{self.base_url}/list"
        resp = requests.get(url, headers=self.headers)
        return resp

    def create_job(self, job_settings):
        url = f"{self.base_url}/create"
        resp = requests.post(url, headers=self.headers, json=job_settings)
        return resp

    def update_job(self, job_settings):
        url = f"{self.base_url}/update"
        resp = requests.post(url, headers=self.headers, json=job_settings)
        return resp

    def reset_job(self, job_settings):
        url = f"{self.base_url}/reset"
        resp = requests.post(url, headers=self.headers, json=job_settings)
        return resp


def import_json(path):
    with open(path, "r") as open_file:
        return json.load(open_file)


def jobs_to_pandas(data):
    df_jobs = pd.DataFrame(data["jobs"])
    df_jobs["name"] = df_jobs["settings"].apply(lambda x: x["name"])
    return df_jobs[["job_id", "name"]]


def create_or_update_job(
    job_name: str,
    job_settings: dict,
    df_jobs: pd.DataFrame,
    job_client: JobAPI,
):
    if job_name in df_jobs["name"].tolist():
        job_id = df_jobs[df_jobs["name"] == job_name]["job_id"].iloc[0]
        job_id = int(job_id)
        data = {"job_id": job_id, "new_settings": job_settings}
        return job_client.reset_job(data)

    return job_client.create_job(job_settings)



job_client = JobAPI(DATABRICKS_WORKSPACE_URL, DATABRICKS_WORKSPACE_TOKEN)
data = job_client.list_jobs().json()

df_jobs = jobs_to_pandas(data)
jobs_files = [i for i in os.listdir(".") if i.endswith(".json")]

for i in tqdm(jobs_files):
    job_settings = import_json(i)
    job_name = i.split(".")[0]
    resp = create_or_update_job(job_name, job_settings, df_jobs, job_client)

    if resp.status_code != 200:
        print(i, resp.text)
