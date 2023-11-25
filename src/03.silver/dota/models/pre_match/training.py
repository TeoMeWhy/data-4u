# Databricks notebook source
# DBTITLE 1,Imports

from databricks import feature_store

import sys

sys.path.insert(0, '../../../../lib/')

import dbtools

import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 1000)

from sklearn import model_selection
from sklearn import ensemble
from sklearn import pipeline
from sklearn import tree
from sklearn import metrics
from sklearn import preprocessing

from feature_engine import encoding
from feature_engine import imputation
from feature_engine import creation
from feature_engine import selection

import lightgbm as lgb

import mlflow

import scikitplot
import matplotlib.pyplot as plt

# COMMAND ----------

# DBTITLE 1,Lookups e Target
query = dbtools.import_query("target.sql")
df = spark.sql(query)

features_lookup = spark.table('feature_store.dota_teams_0').columns[8:]

lookups = [
    feature_store.FeatureLookup(
      table_name = 'feature_store.dota_teams_0',
      feature_names = features_lookup,
      lookup_key = ['dtReference', 'idTeamRadiant'],
      rename_outputs = {i:f'{i}Radiant' for i in features_lookup}
    ),
    feature_store.FeatureLookup(
      table_name = 'feature_store.dota_teams_0',
      feature_names = features_lookup,
      lookup_key = ['dtReference', 'idTeamDire'],
      rename_outputs = {i:f'{i}Dire' for i in features_lookup}
    )
]

# COMMAND ----------

# DBTITLE 1,ABT

fs_client = feature_store.FeatureStoreClient()
training_set = fs_client.create_training_set(
    df=df,
    feature_lookups=lookups,
    label="flRadiantWin",
    exclude_columns=['descTeamNameRadiant', 'descTeamTagRadiant', 'descTeamTagDire', 'descTeamNameDire']
)

training_df = (training_set.load_df()
                           .filter('avgFrequency180Radiant > 10 and avgFrequency180Dire > 10')
                           .filter('minFrequency30Radiant > 0 and minFrequency30Dire > 0')
                           .toPandas())

# COMMAND ----------

# DBTITLE 1,Modelagem
features = training_df.columns[4:-1]
target = 'flRadiantWin'

X_train, X_test, y_train, y_test = model_selection.train_test_split(training_df[features],
                                                                    training_df[target],
                                                                    test_size=0.2,
                                                                    random_state=42)

# COMMAND ----------

print("Tamanho base de treino:", X_train.shape[0], "| Taxa resposta:", y_train.mean())
print("Tamanho base de teste:", X_test.shape[0], "| Taxa resposta:", y_test.mean())

# COMMAND ----------

mlflow.set_experiment("/Users/teomewhy@gmail.com/dota_pre_match")

with mlflow.start_run():

    mlflow.sklearn.autolog()

    missing_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                                    variables=X_train.columns.tolist())

    min_max = preprocessing.MinMaxScaler(feature_range=(1,2)).set_output(transform="pandas")

    model = lgb.LGBMClassifier(n_jobs=-1, random_state=42)

    params = {
                "learning_rate":[0.1, 0.01],
                "n_estimators":[500,1000],
                "min_child_samples":[250,400,800],
                "num_leaves": [10,20,30,50,100,200,500]
            }

    grid = model_selection.GridSearchCV(model,
                                        cv=3,
                                        param_grid=params,
                                        scoring='roc_auc',
                                        verbose=3,
                                        n_jobs=1)

    model_pipe = pipeline.Pipeline(
        [('imputer', missing_0),
         ('model', grid)]
    )

    model_pipe.fit(X_train, y_train)

    pred_test =  model_pipe.predict(X_test)
    proba_test =  model_pipe.predict_proba(X_test)[:,1]

    acc_test = metrics.accuracy_score(y_test, pred_test)
    auc_test = metrics.roc_auc_score(y_test, proba_test)

    mlflow.log_metrics({"test_roc_auc": auc_test,
                        "test_accuracy_score":acc_test})

# COMMAND ----------

pd.DataFrame(grid.cv_results_).sort_values(by='rank_test_score')

# COMMAND ----------

# DBTITLE 1,Predict Test
pred_train =  model_pipe.predict(X_train)
proba_train =  model_pipe.predict_proba(X_train)

pred_test =  model_pipe.predict(X_test)
proba_test =  model_pipe.predict_proba(X_test)

# COMMAND ----------

scikitplot.metrics.plot_roc(y_true=y_test,
                            y_probas=proba_test,
                            plot_micro=False,
                            plot_macro=False )
plt.show()

# COMMAND ----------

scikitplot.metrics.plot_ks_statistic(y_true=y_test, y_probas=proba_test)
plt.show()

# COMMAND ----------

scikitplot.metrics.plot_lift_curve(y_true=y_test, y_probas=proba_test)
plt.show()

# COMMAND ----------


