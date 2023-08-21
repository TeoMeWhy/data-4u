# Databricks notebook source
# DBTITLE 1,Instalação de pacotes
install.packages("read.dbc")
install.packages("doParallel")
install.packages("jsonlite")

# COMMAND ----------

# DBTITLE 1,Setup
library(read.dbc)
library(foreach)
library(doParallel)
library(jsonlite)
library(SparkR)

date = format(Sys.time(), "%Y%m%d")

datasource <- dbutils.widgets.get("datasource")
datasources <- fromJSON("datasources.json")

path = datasources[datasource][[1]]['target'][[1]]
partes <- unlist(strsplit(path, "/"))
partes <- partes[-length(partes)]
dbc_folder <- paste(partes, collapse = "/")
parquet_folder <- sub('/dbc/landing', '/parquet/', dbc_folder)
parquet_folder <- sub('/dbfs', '', parquet_folder)


# COMMAND ----------

# DBTITLE 1,Funções
etl <- function(f) {
    df <- createDataFrame(read.dbc(f))
    write.parquet(df, parquet_folder, mode='append')
    file.rename(from=f, to=gsub("landing", "proceeded", f))
    return
}

# COMMAND ----------

# DBTITLE 1,Execução
files <- list.files(dbc_folder, full.names=TRUE)

for (i in files){
  print(i)
  etl(i)
}
