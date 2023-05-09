# Databricks notebook source
tables = (spark.sql("SHOW TABLES FROM silver.olist")
               .select("tableName")
               .toPandas()['tableName']
               .tolist())

# COMMAND ----------

for i in tables:
    print(i)
    df = spark.table(f"silver.olist.{i}")
    (df.coalesce(1)
       .write
       .format("parquet")
       .mode("overwrite")
       .save(f"/mnt/datalake/export/olist/{i}"))
    
