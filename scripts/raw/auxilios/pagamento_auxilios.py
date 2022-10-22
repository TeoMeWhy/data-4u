# Databricks notebook source
dbutils.widgets.removeAll()

dbutils.widgets.dropdown("ano_inicio", "2004", choices=list(str(i) for i in range(2004,2023)), label="Ano Início" )

dbutils.widgets.dropdown("ano_fim", "2021", choices=list(str(i) for i in range(2004,2023)), label="Ano Fim" )

ano_inicio = int(dbutils.widgets.get("ano_inicio"))
ano_fim = int(dbutils.widgets.get("ano_fim"))

print(ano_inicio, ano_fim)

# COMMAND ----------

import requests
import json


def get_data(ano):
    url = f"https://aplicacoes.mds.gov.br/sagi/servicos/misocial?fq=anomes_s:{ano}*&fq=tipo_s:mes_mu&wt=json&q=*&fl=ibge:codigo_ibge,anomes:anomes_s,qtd_familias_beneficiarias_bolsa_familia,valor_repassado_bolsa_familia,pab_qtd_fam_benef_i,pab_valor_pago_d,pab_valor_medio_d,pab_extraordinario_qtd_fam_i,pab_extraordinario_valor_pago_d,pab_extraordinario_valor_medio_d,pab_qtd_beneficios_primeira_infancia_i,pab_qtd_beneficios_comp_familiar_crianca_i,pab_qtd_beneficios_comp_familiar_adolescente_i,pab_qtd_beneficios_comp_familiar_jovem_i,pab_qtd_beneficios_comp_familiar_gestante_i,pab_qtd_beneficios_superacao_extr_pobreza_i,pab_qtd_beneficios_compensatorio_transitorio_i&rows=10000000&sort=anomes_s%20asc,%20codigo_ibge%20asc"
    data = requests.get(url).json()
    return data

def save_data(data, ano):
    path = f"/dbfs/mnt/datalake/raw/ministerio_cidadania/pagamento_auxilio/data_{ano}.json"
    with open(path, 'w') as open_file:
        json.dump(data, open_file)
    
    return True

def transform(data):
    return data['response']['docs']
        
for ano in range(ano_inicio, ano_fim+1):
    print(ano)
    data = get_data(ano)
    data = transform(data)
    save_data(data, ano)

# COMMAND ----------

df.schema.json()
