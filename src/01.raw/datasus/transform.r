# Databricks notebook source
install.packages("read.dbc")
install.packages("doParallel")

# COMMAND ----------

library(read.dbc)
library(foreach)
library(doParallel)

date = format(Sys.time(), "%Y%m%d")

dbc_folder <- "/dbfs/mnt/datalake/datasus/rd/dbc/landing"
csv_folder <- "/dbfs/mnt/datalake/datasus/rd/csv"

files <- list.files(dbc_folder, full.names=TRUE)
print(files)

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
