# Databricks notebook source
import sys

sys.path.insert(0, "../../lib/")

import ingestors as ing
import dateutil
import dbtools

import json

# COMMAND ----------

with open("datasources.json", "r") as open_file:
    datasources = json.load(open_file)

dbutils.widgets.combobox("table",
                         list(datasources.keys())[0],
                         list(datasources.keys()),
                         "Tabela",
                         )

# COMMAND ----------

table = dbutils.widgets.get("table")

database = "bronze.tse"

id_fields = datasources[table]["id_fields"]
partition_fields = datasources[table]["partition_fields"]
file_format = datasources[table]["file_format"]
path = datasources[table]["path"].format(file_format=file_format)
read_options = datasources[table]["read_options"]
timestamp_field = datasources[table]["timestamp_field"]

# COMMAND ----------

ingestor = ing.IngestaoBronze(
    path_full_load = path,
    path_incremental = path,
    file_format = file_format,
    table_name = table,
    database_name = database,
    id_fields = id_fields,
    timestamp_field = timestamp_field,
    partition_fields = partition_fields,
    read_options = read_options,
    spark=spark
)

# COMMAND ----------

if not dbtools.table_exists(spark, database, table):
    print("Criando tabela em delta...")
    df = ingestor.load_full().createOrReplaceTempView(table)
    df_transformed = ingestor.transform(table)
    
    (spark.createDataFrame(data=[], schema=df_transformed.schema)
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy(*partition_fields)
        .option("overwriteSchema", "true")
        .saveAsTable(f"{database}.{table}")
    )
    print("")

# COMMAND ----------

stream = ingestor.process_stream()
