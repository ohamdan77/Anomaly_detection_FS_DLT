# Databricks notebook source
# MAGIC %md
# MAGIC ## Model Training and regitration

# COMMAND ----------

# DBTITLE 1,Import needed Libraries and functions
from databricks.feature_store import FeatureLookup
from databricks.feature_store import FeatureStoreClient
import mlflow
from mlflow.tracking import MlflowClient

import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import FeatureUnion
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline, make_union
from sklearn.metrics import classification_report, precision_recall_fscore_support
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np


# COMMAND ----------

# MAGIC %md
# MAGIC %md Instantiate a FeatureStoreClient instance

# COMMAND ----------

fs = FeatureStoreClient()

# COMMAND ----------

# MAGIC %md Create an MLflow experiment

# COMMAND ----------

# DBTITLE 1,Create an MLFlow experiment to be used for training
def get_or_create_experiment(experiment_location: str) -> None:
 
  if not mlflow.get_experiment_by_name(experiment_location):
    print("Experiment does not exist. Creating experiment")
    
    mlflow.create_experiment(experiment_location)
    
  mlflow.set_experiment(experiment_location)


experiment_location = '/Shared/anomaly_feature_store_experiment'
get_or_create_experiment(experiment_location)

mlflow.set_experiment(experiment_location)

# COMMAND ----------

# MAGIC %md Specify the feature table names, columns, and join keys

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.oh_anomaly_detection

# COMMAND ----------

columns = spark.read.table("transaction_features").columns
remove = [ 'transaction_id']
names = [item for item in columns if item not in remove]

# COMMAND ----------

names

# COMMAND ----------

feature_lookups = [
    FeatureLookup(
      table_name = 'transaction_features',
      feature_names = names,
      lookup_key = 'transaction_id',
      # timestamp_lookup_key= 'timestamp'
    )
  ]

# COMMAND ----------

# MAGIC %md Join the features to form the training dataset

# COMMAND ----------

transactions_and_labels = spark.table('card_transaction_labels') 

training_set = fs.create_training_set(df = transactions_and_labels,
                                      feature_lookups = feature_lookups,
                                      label = 'fraud',
                                      exclude_columns = ['transaction_id'])

training_df = training_set.load_df()

display(training_df)

# COMMAND ----------

# MAGIC %md Fit a scikit-learn pipeline model to the features. After fitting the model, local the model run in the Mlflow Tracking Server. Promote the model to the Model Registry. Local the model in the Registry and change its "Stage" to "Production"  
# MAGIC
# MAGIC See https://www.mlflow.org/docs/latest/model-registry.html#registering-a-model for instructions.

# COMMAND ----------

# Convert to Pandas for scikit-learn training
data = training_df.toPandas()

# Split into training and test datasets
label = 'fraud'
features = [col for col in data.columns if col not in [label, 'transaction_id']]

X_train, X_test, y_train, y_test = train_test_split(data[features], data[label], test_size=0.25, random_state=123, shuffle=True)

# Categorize columns by data type
# categorical_vars = ['NamePrefix', 'Sex', 'CabinChar', 'CabinMulti', 'Embarked', 'Parch', 'Pclass', 'SibSp']
numeric_vars = names
# binary_vars = ['NameMultiple']

# Create the a pre-processing and modleing pipeline
# binary_transform = make_pipeline(SimpleImputer(strategy = 'constant', fill_value = 'missing'))

numeric_transform = make_pipeline(SimpleImputer(strategy = 'most_frequent'))

# categorical_transform = make_pipeline(SimpleImputer(missing_values = None, strategy = 'constant', fill_value = 'missing'), 
                                      # OneHotEncoder(handle_unknown="ignore"))
  
transformer = ColumnTransformer([('numeric_vars', numeric_transform, numeric_vars)],
                                  remainder = 'drop')

# Specify the model
# See Hyperopt for hyperparameter tuning: https://docs.databricks.com/applications/machine-learning/automl-hyperparam-tuning/index.html
model = xgb.XGBClassifier(n_estimators = 50, use_label_encoder=False)

classification_pipeline = Pipeline([("preprocess", transformer), ("classifier", model)])

# Fit the model, collect statistics, and log the model
with mlflow.start_run() as run:
  
  run_id = run.info.run_id
  #mlflow.xgboost.autolog()
    
  # Fit model
  classification_pipeline.fit(X_train, y_train)
  
  train_pred = classification_pipeline.predict(X_train)
  test_pred = classification_pipeline.predict(X_test)
  
  # Calculate validation statistics
  precision_train, recall_train, f1_train, _ = precision_recall_fscore_support(y_train, train_pred, average='weighted')
  precision_test, recall_test, f1_test, _ = precision_recall_fscore_support(y_test, test_pred, average='weighted')
  
  decimals = 2
  validation_statistics = {"precision_training": round(precision_train, decimals),
                           "precision_testing": round(precision_test, decimals),
                           "recall_training": round(recall_train, decimals),
                           "recall_testing": round(recall_test, decimals),
                           "f1_training": round(f1_train, decimals),
                           "f1_testing": round(f1_test, decimals)}
  
  # Log the validation statistics
  mlflow.log_metrics(validation_statistics)
  
  # Fit final model
  final_model = classification_pipeline.fit(data[features], data[label])
  
  # Log the model and training data metadata
  fs.log_model(
    final_model,
    artifact_path="model",
    flavor = mlflow.sklearn, 
    training_set=training_set
  )

# COMMAND ----------

# MAGIC %md Register the model in the Model Registry

# COMMAND ----------

client = MlflowClient()

# COMMAND ----------

# Create a Model Registry entry for the model if one does not exist
model_registry_name = 'anomaly_detection_model'
try:
  client.get_registered_model(model_registry_name)
  print(" Registered model already exists")
except:
  client.create_registered_model(model_registry_name)

# COMMAND ----------

# Get model run id and artifact path
model_info = client.get_run(run_id).to_dictionary()
artifact_uri = model_info['info']['artifact_uri']


# Register the model
registered_model = client.create_model_version(
                     name = model_registry_name,
                     source = artifact_uri + "/model",
                     run_id = run_id
                    )

# COMMAND ----------

# MAGIC %md Promote model to the Production stage

# COMMAND ----------

promote_to_prod = client.transition_model_version_stage(name=model_registry_name,
                                                        version = int(registered_model.version),
                                                        stage="Production",
                                                        archive_existing_versions=True)

# COMMAND ----------

# model_name = "anomaly_detection_model"
# def get_run_id(model_name, stage='Production'):
#   """Get production model id from Model Registry"""
  
#   prod_run = [run for run in client.search_model_versions(f"name='{model_name}'") 
#                   if run.current_stage == stage][0]
  
#   return prod_run.run_id


# # Replace the first parameter with your model's name
# run_id = get_run_id('anomaly_detection_model', stage='Production')
# run_id

# COMMAND ----------

# new_tr_df = spark.readStream.table("card_transaction_labels").drop("fraud")

# COMMAND ----------

# model_uri = f'runs:/{run_id}/model'

# with_predictions = fs.score_batch(model_uri, new_tr_df)

# display(with_predictions)
