# Databricks notebook source
import pandas as pd
import string

def range_letter(x, stop_number=None):
    letter = x[0]
    start_number = int(x.split("-")[0][1:])
    stop_number = int(x.split("-")[1][1:]) if stop_number == None else stop_number

    values = [ f'{letter}{i:02d}' for i in range(start_number, stop_number+1) ]
    return values


def make_range(x):

    x = x.strip(" ")

    try:
        start, stop = x.split("-")
    except ValueError as err:
        return x

    letter_start = start[0]
    letter_start_pos = string.ascii_uppercase.find(letter_start)
    number_start = int(float(start[1:]))

    letter_stop = stop[0]
    letter_stop_pos = string.ascii_uppercase.find(letter_stop)
    number_stop = int(float(stop[1:]))

    values = []
    letter_pos = letter_start_pos
    while letter_pos < letter_stop_pos:

        letter = string.ascii_uppercase[letter_pos]

        values.extend(range_letter(f'{letter}{number_start:02d}-{letter}99'))

        letter_pos += 1
        number_start = 1
    
    values.extend(range_letter(f'{letter_stop}{number_start}-{letter_stop}{number_stop}'))

    return values

# COMMAND ----------

df = pd.read_csv("/dbfs/mnt/datalake/datasus/cid/volume1.csv", sep=";")

# COMMAND ----------

df['DescricaoLista'] = df['Descrição'].fillna("").apply(lambda x: x.split(","))

df_explode = df.explode("DescricaoLista").iloc[:-1]
df_explode = df_explode[df_explode["Código"] != '-']

df_explode["DescricaoListaRange"] = df_explode["DescricaoLista"].apply(make_range)
df_completa = df_explode.explode("DescricaoListaRange")

columns = {
    "Capítulo": "descCapituloCID" ,
    "Código":  "codCID",
    "Códigos da CID-10": "descCID" ,
    "Descrição": "codCID10" ,
    "DescricaoListaRange":  "codCID10dst",
}

df_completa = df_completa.rename(columns=columns)[columns.values()]
sdf = spark.createDataFrame(df_completa)

# COMMAND ----------

sdf.write.format("delta").mode("overwrite").saveAsTable("bronze.datasus.cid")
