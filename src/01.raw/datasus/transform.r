# Databricks notebook source
install.packages("read.dbc")
install.packages("doParallel")
install.packages("jsonlite")

# COMMAND ----------

library(read.dbc)
library(foreach)
library(doParallel)
library(jsonlite)
library(SparkR)

date = format(Sys.time(), "%Y%m%d")

# datasource <- dbutils.widgets.get("datasource")
datasource <- 'sinasc'
datasources <- fromJSON("datasources.json")

path = datasources[datasource][[1]]['target'][[1]]
partes <- unlist(strsplit(path, "/"))
partes <- partes[-length(partes)]
dbc_folder <- paste(partes, collapse = "/")
parquet_folder <- sub('/dbc/landing', '/parquet/', dbc_folder)
parquet_folder <- sub('/dbfs', '', parquet_folder)

files <- list.files(dbc_folder, full.names=TRUE)

# COMMAND ----------

etl <- function(f) {
    df <- createDataFrame(read.dbc(f))
    write.parquet(df, parquet_folder, mode='append')
    file.rename(from=f, to=gsub("landing", "proceeded", f))
    return
}

# COMMAND ----------

for (i in files){
  print(i)
  etl(i)
}

# COMMAND ----------

# registerDoParallel(8)
# while (sum(is.na(files)) != length(files)) {
#   batch = files[1:min(8, length(files))]
#   files = files[1+min(8, length(files)):length(files)]
#   foreach (i=batch) %dopar% {
#     print(i)
#     if (is.na(i) == FALSE) {
#       etl(i)
#     }
#   }
# }

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC dbutils.fs.mv("/mnt/datalake/datasus/sinasc/dbc/landind/", "/mnt/datalake/datasus/sinasc/dbc/landing/", True)
