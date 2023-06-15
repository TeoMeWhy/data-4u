# Databricks notebook source
# DBTITLE 1,Imports
import mlflow
from databricks import feature_store

# COMMAND ----------

# DBTITLE 1,Setup
fs = feature_store.FeatureStoreClient()

model_uri = 'models:/IGDB_company_success/production'
model = mlflow.sklearn.load_model(model_uri)
features = model.feature_names_in_

query = '''
    SELECT idCompany,
           dtRef
    FROM feature_store.igdb_company_general
    WHERE dtRef = (SELECT MAX(dtRef) FROM feature_store.igdb_company_general)
'''

to_predict = spark.sql(query)

## LOOKUPS
df_fs = (spark.sql("SHOW TABLES FROM feature_store")
              .filter("STARTSWITH(tableName, 'igdb')")
              .toPandas())

df_fs['feature_store'] = df_fs['database'] + '.' + df_fs['tableName']

fs_lookups = []
for i in df_fs['feature_store'].tolist():
    fs_lookups.append( feature_store.FeatureLookup(table_name=i,
                                                    lookup_key=['idCompany', 'dtRef']))
    
predict_set = fs.create_training_set(to_predict,
                                     feature_lookups=fs_lookups,
                                     label=None)

predict_sdf = predict_set.load_df()
predict_pdf = predict_df.toPandas()

# COMMAND ----------

# DBTITLE 1,Predictions
predict_probas = model.predict_proba(predict_pdf[features])
predict_pdf['proba_sucess_game'] = predict_probas[:,1]

(spark.createDataFrame(predict_pdf[['idCompany', 'proba_sucess_game']])
      .createOrReplaceTempView('company_sucess_predict'))

# COMMAND ----------

# DBTITLE 1,Resultados
# MAGIC %sql
# MAGIC
# MAGIC SELECT t1.idCompany,
# MAGIC        t2.descName,
# MAGIC        round(t1.proba_sucess_game * 100, 2)  AS vlProbaSuccessGame,
# MAGIC        row_number() OVER (PARTITION BY 1 ORDER BY proba_sucess_game DESC) AS vlSuccessRank
# MAGIC
# MAGIC FROM company_sucess_predict AS t1
# MAGIC
# MAGIC LEFT JOIN silver.igdb.companies AS t2
# MAGIC ON t1.idCompany = t2.idCompany
# MAGIC
# MAGIC ORDER BY proba_sucess_game DESC
