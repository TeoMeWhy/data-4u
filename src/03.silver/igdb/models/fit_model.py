# Databricks notebook source
# DBTITLE 1,Imports
import sys
from databricks import feature_store

sys.path.insert(0, "../../../lib")

import dbtools

import pandas as pd

from sklearn import model_selection
from sklearn import metrics
from sklearn import ensemble
from sklearn import tree
from sklearn import pipeline

from feature_engine import imputation
from feature_engine import selection

import mlflow

# COMMAND ----------

# DBTITLE 1,Criação da variável resposta
query = dbtools.import_query('etl_sucess_game.sql')
df_target = spark.sql(query)
df_target.display()

# COMMAND ----------

# DBTITLE 1,Definição de Lookups
df_fs = (spark.sql("SHOW TABLES FROM feature_store")
              .filter("STARTSWITH(tableName, 'igdb')")
              .toPandas())

df_fs['feature_store'] = df_fs['database'] + '.' + df_fs['tableName']

fs_lookups = []
for i in df_fs['feature_store'].tolist():
    fs_lookups.append(feature_store.FeatureLookup(table_name=i,
                                                  lookup_key=['idCompany', 'dtRef']))

# COMMAND ----------

# DBTITLE 1,Criação da ABT
fs_client = feature_store.FeatureStoreClient()
training_set = fs_client.create_training_set(df=df_target,
                                             feature_lookups=fs_lookups,
                                             label='flSuccessGame')

df_training = training_set.load_df()
df_training

# COMMAND ----------

# DBTITLE 1,Sample
pdf = df_training.toPandas()

target = 'flSuccessGame'
id_columns = ['idCompany', 'dtRef']
features = list(set(pdf.columns.tolist()) - set(id_columns + [target]))

X_train, X_test, y_train, y_test = model_selection.train_test_split(pdf[features],
                                                                    pdf[target],
                                                                    train_size=0.8,
                                                                    random_state=42,
                                                                    stratify=pdf[target])

print('Taxa da variável resposta em train:', y_train.mean())
print('Taxa da variável resposta em test:', y_test.mean())

# COMMAND ----------

# DBTITLE 1,Seleção de variáveis
imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0, variables=features)

tree_model = tree.DecisionTreeClassifier(min_samples_leaf=250, max_depth=8)

model_pipeline = pipeline.Pipeline( [('Imputer', imputer_0),
                                     ("Tree Model", tree_model)])

model_pipeline.fit(X_train, y_train)

# COMMAND ----------

feature_importance = pd.Series(tree_model.feature_importances_,
                               index=model_pipeline[:-1].transform(X_train.head(1)).columns)

best_features = (feature_importance.sort_values(ascending=False)
                                   .cumsum()
                                   .head(50)
                                   .index.tolist())
