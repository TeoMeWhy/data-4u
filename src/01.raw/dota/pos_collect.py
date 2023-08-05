# Databricks notebook source
from pyspark.sql import types
from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Consolida collect
files = [i.name.split(".")[0] for i in dbutils.fs.ls("/mnt/datalake/dota/matches_details")]

df = spark.createDataFrame(pd.DataFrame({"match_id":files}))

(df.write
   .format("delta")
   .mode("overwrite")
   .save("/mnt/datalake/dota/collect"))

# COMMAND ----------

# DBTITLE 1,Consolida Pro Matches
df_macthes = (spark.read
                   .format("parquet")
                   .load("/mnt/datalake/dota/pro_matches")
                   .collect())

df_matches_save = spark.createDataFrame(df_macthes)

(df_matches_save.write
                .format("parquet")
                .mode("overwrite")
                .save("/mnt/datalake/dota/pro_matches"))

# COMMAND ----------

df_pro_matches = spark.read.parquet("/mnt/datalake/dota/pro_matches")
df_pro_matches.select(F.count_distinct("match_id"), F.count("match_id")).display()

df_collect = spark.read.format("delta").load("/mnt/datalake/dota/collect")
df_collect.select(F.count_distinct("match_id")).display()
