# Databricks notebook source
path = "/mnt/datalake/ibge/municipios_brasileiros.csv"

df = (spark.read
           .csv(path, header=True, sep=";"))

(df.write
   .format("delta")
   .mode("overwrite")
   .saveAsTable("bronze.ibge.municipios_brasileiros"))
