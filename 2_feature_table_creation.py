# Databricks notebook source
# MAGIC %md ### Feature engineering logic for demographic features

# COMMAND ----------

from pyspark.sql.functions import col
import pyspark.sql.functions as func
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import feature_table

# COMMAND ----------

# MAGIC %md Instatiate feature store client

# COMMAND ----------

fs = FeatureStoreClient()

# COMMAND ----------

fs.drop_table("transaction_features")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Put all the "V" Features in a feature store table. The column *time* is not needed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE DATABASE oh_anomaly_feat_db;
# MAGIC USE hive_metastore.oh_anomaly_feat_db

# COMMAND ----------

feature_df = spark.read.table("hive_metastore.oh_anomaly_detection.card_transaction_features")

# COMMAND ----------

feature_table_name = "transaction_features"

try:
  fs.get_table(feature_table_name)
  print("Feature table entry already exists")
  pass
  
except Exception:
  fs.create_table(name = feature_table_name,
                          primary_keys = 'transaction_id',
                          # timestamp_keys= 'timestamp',
                          schema = feature_df.schema,
                          description = 'credit card transactions features')

# COMMAND ----------

fs.write_table(
  
  name= feature_table_name,
  df = feature_df,
  mode = 'merge'
  
  )
