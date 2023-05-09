# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

import dlt
import mlflow
from pyspark.sql.functions import *
from typing import Iterator, Tuple
import pandas as pd
from databricks.feature_store import FeatureStoreClient
from mlflow.tracking import MlflowClient

client = MlflowClient()
fs = FeatureStoreClient()

# COMMAND ----------

json_path = "/FileStore/OH/transaction_landing_dir"
schema_location = "/FileStore/OH/transaction_landing_dir/schema"
@dlt.table(
  comment = "Bronze table to collect transaction data"
)
def bronze_transaction_j():
  return (
    spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "json")
     .option("cloudFiles.schemaLocation", schema_location)
     .option("cloudFiles.inferColumnTypes", "true")
     .option("cloudFiles.schemaEvolutionMode", "none")
     .option("maxFilesPerTrigger", 1)
     .load(json_path)
    #  .withColumn("timestamp", current_timestamp())
  )

# COMMAND ----------

@dlt.table(
  comment = "transaction features silver table"
)
def card_transaction_features_j():
  return(
    dlt.readStream("bronze_transaction_j")
     .drop("class")
  )

# COMMAND ----------

@dlt.table(
  comment = "transaction labels silver table"
)
def card_transaction_labels_j():
  return(
    dlt.readStream("bronze_transaction_j")
     .select("transaction_id", col("class").alias("fraud"))
  )

# COMMAND ----------

# dlt.create_feature_table(
#   name = "transaction_features",
#   path = "dbfs:/user/hive/warehouse/oh_anomaly_detection.db/transaction_features",
#   primary_keys= "transaction_id",
#   timestamp_key= "timestamp"
# )

# COMMAND ----------

# dlt.apply_changes(
#   target = "transaction_features",
#   source = "card_transaction_features_j",
#   keys = ["transaction_id"],
#   sequence_by = "timestamp",
#   stored_as_scd_type = "2"
# )
