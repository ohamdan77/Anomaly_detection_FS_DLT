# Databricks notebook source

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC For demo purposes clear the streaming landing location by removing all files

# COMMAND ----------

json_landing_stream = "/FileStore/OH/transaction_landing_stream_dir"
dbutils.fs.rm(json_landing_stream, True)
dbutils.fs.mkdirs(json_landing_stream)

# COMMAND ----------

# DBTITLE 1,Get the schema from the json landing folder
json_landing = "/FileStore/OH/transaction_landing_dir"
schema = spark.read.json(json_landing).schema

# COMMAND ----------

(spark.readStream
      .format("json")
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .load(json_landing)
      # .withColumn("timestamp", current_timestamp())
      .drop("class")
      .writeStream
      .format("json")
      .option("checkpointLocation", "/FileStore/OH/transaction_landing_stream_dir/checkpoint/")
      .trigger(once=True)
      .start(json_landing_stream)
      )
