# Databricks notebook source
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import feature_table
import mlflow
from mlflow.tracking import MlflowClient

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.oh_anomaly_detection

# COMMAND ----------

client = MlflowClient()
fs = FeatureStoreClient()
fs.drop_table("transaction_features")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP DATABASE IF EXISTS oh_anomaly_detection CASCADE;

# COMMAND ----------

experiment_name = '/Shared/anomaly_feature_store_experiment'
exp = mlflow.get_experiment_by_name(experiment_name)
exp_id = exp.experiment_id
mlflow.delete_experiment(exp_id)

# COMMAND ----------

model_registry_name = 'anomaly_detection_model'
client.transition_model_version_stage(model_registry_name, version=1, stage='Archived')
client.delete_registered_model(model_registry_name)

# COMMAND ----------

json_landing_stream = "/FileStore/OH/transaction_landing_stream_dir"
dbutils.fs.rm(json_landing_stream, True)
# dbutils.fs.mkdirs(json_landing_stream)

# COMMAND ----------

json_landing_stream_fs = "/FileStore/OH/transaction_landing_stream_dir_fs"
dbutils.fs.rm(json_landing_stream_fs, True)
# dbutils.fs.mkdirs(json_landing_stream_fs)

# COMMAND ----------

checkppoint = "/FileStore/OH/stream/checkpoint/"
dbutils.fs.rm(checkppoint, True)
