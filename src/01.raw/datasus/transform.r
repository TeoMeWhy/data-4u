# Databricks notebook source
install.packages("read.dbc")

# COMMAND ----------

library(read.dbc)

date = format(Sys.time(), "%Y%m%d")

dbc_folder <- "/dbfs/mnt/datalake/datasus/rd/dbc/landing"
csv_folder <- "/dbfs/mnt/datalake/datasus/rd/csv"

files <- list.files(dbc_folder, full.names=TRUE)
for(f in files) {
    print(f)
    df= read.dbc(f)
    lista = strsplit(f, "/")[[1]]

    file = gsub(".dbc", paste(date, "csv", sep="."), lista[length(lista)])
    write.csv2(df, paste(csv_folder, file, sep="/"), row.names=FALSE)
    file.rename(from=f, to=gsub("landing", "proceeded", f))
}
