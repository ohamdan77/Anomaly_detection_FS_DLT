# Databricks notebook source
# MAGIC %md ### Feature engineering logic for demographic features

# COMMAND ----------

# DBTITLE 1,Import the needed Libraries and functions
from pyspark.sql.functions import col
import pyspark.sql.functions as func
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import feature_table

# COMMAND ----------

# MAGIC %md Instatiate feature store client

# COMMAND ----------

fs = FeatureStoreClient()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.oh_anomaly_detection

# COMMAND ----------

feature_df = spark.read.table("card_transaction_features")

# COMMAND ----------

# DBTITLE 1,Create the Feature Store table
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

# DBTITLE 1,Write the Features in the feature store table
fs.write_table(
  
  name= feature_table_name,
  df = feature_df,
  mode = 'merge'
  
  )
