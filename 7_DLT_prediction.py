# Databricks notebook source
# MAGIC
# MAGIC %pip install mlflow cloudpickle==1.6.0 pandas==1.2.4 psutil==5.8.0 scikit-learn==0.24.1 typing-extensions==3.7.4.3 xgboost==1.5.2 databricks-feature-store

# COMMAND ----------

checkpoint_location = "FileStore/OH/anomaly_pred/checlpoint"
dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------


import mlflow
from pyspark.sql.functions import *
from typing import Iterator, Tuple
import pandas as pd
from databricks.feature_store import FeatureStoreClient
from mlflow.tracking import MlflowClient

client = MlflowClient()
fs = FeatureStoreClient()

# COMMAND ----------

model_name = 'anomaly_detection_model'
def get_run_id(model_name, stage='Production'):
  """Get production model id from Model Registry"""
  
  prod_run = [run for run in client.search_model_versions(f"name='{model_name}'") 
                  if run.current_stage == stage][0]
  
  return prod_run.run_id


# Replace the first parameter with your model's name
run_id = get_run_id('anomaly_detection_model', stage='Production')

# COMMAND ----------


model_uri = f'runs:/{run_id}/model'
client = mlflow.tracking.MlflowClient()

logged_model = client.get_latest_versions(model_name, stages=["Production"])[0].source

# Load model as a Spark UDF. Override result_type if the model does not return double values.
# predict_anomaly = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')

# @pandas_udf("double")
# def udf_predict(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
#   model_name = 'anomaly_detection_model'
#   logged_model = client.get_latest_versions(model_name, stages=["Production"])[0].source
#   model = mlflow.pyfunc.load_model(logged_model)
  
#   for features in iterator:
#     predict_df = pd.concat(features, axis=1)
#     yield pd.Series(model.predict(predict_df))
    
# spark.udf.register("detect_anomaly", udf_predict)

# features = ['transaction_id', 'timestamp']
input_df = spark.readStream.table(f"hive_metastore.oh_anomaly_detection.card_transaction_labels_j").drop("fraud")

# @dlt.create_table(
#   comment="anomaly detection model for identifying OOD data",  
#   table_properties={
#     "quality": "gold"
#   }    
# )
# def anomaly_predictions():
#   return fs.score_batch(model_uri, input_df("card_transaction_labels"))
pred_df = fs.score_batch(model_uri, input_df)


# COMMAND ----------

pred_df.writeStream.option("checkpointLocation", checkpoint_location).table("hive_metastore.oh_anomaly_detection.anomaly_prediction")
