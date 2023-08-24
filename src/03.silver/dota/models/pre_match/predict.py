# Databricks notebook source
import datetime
import pandas as pd
import mlflow
from databricks import feature_store

from pyspark.sql import functions as F

import sys

sys.path.insert(0, '../../../../lib/')

import dbtools

model = mlflow.sklearn.load_model("models:/dota_pre_match/production")

# COMMAND ----------

dbutils.widgets.text(label="Radiant", name="Radiant", defaultValue="")
dbutils.widgets.text(label="Dire", name="Dire", defaultValue="")

radiant_name = dbutils.widgets.get("Radiant")
dire_name = dbutils.widgets.get("Dire")

print(radiant_name)
print(dire_name)

# COMMAND ----------

df_teams = (spark.table("silver.dota.team_last_seen")
                 .select("idTeam", "descTeamName")
                 .toPandas())

try:
    radiant_id = ""
    radiant_id = df_teams[df_teams['descTeamName'] == radiant_name]['idTeam'].iloc[0]
except IndexError as err:
    print("Verifique o nome do time dos Iluminados")

try:
    dire_id = ""
    dire_id = df_teams[df_teams['descTeamName'] == dire_name]['idTeam'].iloc[0]
except IndexError as err:
    print("Verifique o nome do time dos Temidos")

text = f"Radiant: {radiant_name}({radiant_id})  x  {dire_name}({dire_id}) :Dire"

print(text)

# COMMAND ----------

dt_reference = datetime.datetime.now().strftime("%Y-%m-%d")

df = spark.createDataFrame(
    pd.DataFrame({
        "dtReference": [dt_reference],
        "idTeamDire":[dire_id],
        "idTeamRadiant":[radiant_id],     
    })
)

df_teams_fs_dt = (spark.table('feature_store.dota_teams_0')
                       .filter(f"dtReference = '{dt_reference}'")
                       .drop(F.col("idTeamRadiant"),
                             F.col("descTeamNameRadiant"),
                             F.col("descTeamTagRadiant"),
                             F.col("idTeamDire"),
                             F.col("descTeamNameDire"),
                             F.col("descTeamTagDire")))

df_radiant_fs = (df_teams_fs_dt.pandas_api()
                               .rename(columns= {i:f"{i}Radiant" for i in df_teams_fs_dt.columns} )
                               .to_spark())

df_dire_fs = (df_teams_fs_dt.pandas_api()
                               .rename(columns= {i:f"{i}Dire" for i in df_teams_fs_dt.columns} )
                               .to_spark())


df_predict = (df.join( df_radiant_fs.alias("radiant"),
                       df.idTeamRadiant==df_radiant_fs.idTeamRadiant,
                       "left")
                .join(df_dire_fs.alias("dire"),
                      df.idTeamDire==df_dire_fs.idTeamDire,
                      "left")
                .drop(F.col("radiant.dtReferenceRadiant"),F.col("radiant.idTeamRadiant"))
                .drop(F.col("dire.dtReferenceDire"),F.col("dire.idTeamDire"))
                .toPandas()
)

# COMMAND ----------

radiant_prob, dire_prob = model.predict_proba(df_predict[df_predict.columns[3:]])[0]*100

df_dashboard = spark.createDataFrame(
    pd.DataFrame(
        {
            "descRadiantTeam": [radiant_name],
            "probRadiant": [radiant_prob],
            "descDireTeam": [dire_name],
            "probDire": [dire_prob],
        }
    )
)

df_dashboard.display()
