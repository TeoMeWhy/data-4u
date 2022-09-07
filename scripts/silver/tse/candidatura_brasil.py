# Databricks notebook source
df_1 = spark.table("bronze_tse.consulta_cand_2018_brasil")
df_2 = spark.table("bronze_tse.consulta_cand_2020_brasil")
df_3 = spark.table("bronze_tse.consulta_cand_2022_brasil")

df_full = (df_1.unionByName(df_2, allowMissingColumns=True)
               .unionByName(df_3, allowMissingColumns=True))

(df_full.coalesce(1)
        .write
        .mode("overwrite")
        .format("delta") 
        .option("overwriteSchema","true")
        .partitionBy("ANO_ELEICAO")
        .saveAsTable("silver_tse.candidatura_brasil"))
