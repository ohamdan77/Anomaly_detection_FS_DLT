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

# MAGIC %md
# MAGIC
# MAGIC Put all the "V" Features in a feature store table. The column *time* is not needed

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE hive_metastore.oh_anomaly_detection

# COMMAND ----------


json_landing_stream_fs = "/FileStore/OH/transaction_landing_stream_dir_fs"
schema = spark.read.json(json_landing_stream_fs).schema
feature_df = spark.readStream.format("json").schema(schema).load(json_landing_stream_fs)

# COMMAND ----------

feature_table_name = "transaction_features"

# try:
#   fs.get_table(feature_table_name)
#   print("Feature table entry already exists")
#   pass
  
# except Exception:
#   fs.create_table(name = feature_table_name,
#                           primary_keys = 'transaction_id',
#                           timestamp_keys= 'timestamp',
#                           schema = feature_df.schema,
#                           description = 'credit card transactions features')

# COMMAND ----------

fs.write_table(
  
  name= feature_table_name,
  df = feature_df,
  mode = 'merge'
  
  )
