# Databricks notebook source
# MAGIC %sql
# MAGIC USE oh_anomaly_detection

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

json_landing = "s3://files.training.databricks.com/ohamdan/json_landing/"
landing_df = (
  spark.table('anomaly_bronze')
       .where(col('transaction_id') >= 142403)
       .write
       .format("json")
       .option("maxRecordsPerFile", 100)
       .save(json_landing)
)
