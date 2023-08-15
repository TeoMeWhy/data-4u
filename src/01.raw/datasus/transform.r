# Databricks notebook source
install.packages("read.dbc")
install.packages("doParallel")
install.packages("jsonlite")

# COMMAND ----------

library(read.dbc)
library(foreach)
library(doParallel)
library(jsonlite)

date = format(Sys.time(), "%Y%m%d")

datasource <- dbutils.widgets.get("datasource")
datasources <- fromJSON("datasources.json")

path = datasources[datasource][[1]]['target'][[1]]
partes <- unlist(strsplit(path, "/"))
partes <- partes[-length(partes)]
dbc_folder <- paste(partes, collapse = "/")
csv_folder <- sub('/dbc/landing', '/csv', dbc_folder)

files <- list.files(dbc_folder, full.names=TRUE)

# COMMAND ----------

etl <- function(f) {
    print(f)
    df= read.dbc(f)
    lista = strsplit(f, "/")[[1]]
    file = gsub(".dbc", paste(date, "csv", sep="."), lista[length(lista)])
    write.csv2(df, paste(csv_folder, file, sep="/"), row.names=FALSE)
    file.rename(from=f, to=gsub("landing", "proceeded", f))
    return
}

# COMMAND ----------

registerDoParallel(8)
while (sum(is.na(files)) != length(files)) {
  batch = files[1:min(8, length(files))]
  files = files[1+min(8, length(files)):length(files)]
  foreach (i=batch) %dopar% {
    print(i)
    if (is.na(i) == FALSE) {
      etl(i)
    }
  }
}
