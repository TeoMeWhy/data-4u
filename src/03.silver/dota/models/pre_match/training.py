# Databricks notebook source
# DBTITLE 1,Imports
from databricks import feature_store

import sys

sys.path.insert(0, '../../../../lib/')

import dbtools

import pandas as pd

from sklearn import model_selection
from sklearn import ensemble
from sklearn import pipeline
from sklearn import tree
from sklearn import metrics

from feature_engine import encoding
from feature_engine import imputation

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
                           .filter('nrFrequency180Radiant > 10 and nrFrequency180Dire > 10')
                           .toPandas())

# COMMAND ----------

# DBTITLE 1,Modelagem
to_remove = set(['descTeamNameRadiant', 'descTeamTagRadiant',
                 'descTeamTagDire','descTeamNameDire'])

features = list(set(training_df.columns[4:-1]) - to_remove)
target = 'flRadiantWin'

X_train, X_test, y_train, y_test = model_selection.train_test_split(training_df[features],
                                                                    training_df[target],
                                                                    test_size=0.2,
                                                                    random_state=42)

# COMMAND ----------

print("Tamanho base de treino:", X_train.shape[0])
print("Tamanho base de teste:", X_test.shape[0])

# COMMAND ----------

mlflow.set_experiment("/Users/teomewhy@gmail.com/dota_pre_match")

with mlflow.start_run():

    mlflow.sklearn.autolog()

    missing_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                                  variables=X_test.columns.tolist())

    model = lgb.LGBMClassifier(n_jobs=-1, random_state=42)

    params = {"min_child_samples":[900,1000],
              "learning_rate":[0.01],
              "n_estimators":[1000],
              "subsample":[0.9],
              "max_depth":[15]}

    grid = model_selection.GridSearchCV(model, cv=3, param_grid=params, scoring='roc_auc', verbose=3)

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
