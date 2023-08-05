# Databricks notebook source
from pyspark.sql import types

# COMMAND ----------

# DBTITLE 1,Consolida collect
schema = types.StructType(fields=[
            types.StructField(name="match_id",
                              dataType=types.StringType())
])

df = (spark.read
           .format("json")
           .schema(schema) 
           .load("/mnt/datalake/dota/matches_details"))

(df.write
   .format("delta")
   .mode("overwrite")
   .save("/mnt/datalake/dota/collect"))

# COMMAND ----------

# DBTITLE 1,Consolida Pro Matches
df_macthes = (spark.read
                .format("parquet")
                .load("/mnt/datalake/dota/pro_matches")
                .collect()
           )

df_matches_save = spark.createDataFrame(df_macthes)

(df_matches_save.write
                .format("parquet")
                .mode("overwrite")
                .save("/mnt/datalake/dota/pro_matches"))
