# Databricks notebook source

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Get the schema from the json landing folder
json_landing_stream = "/FileStore/OH/transaction_landing_stream_dir"
json_landing_stream_fs = "/FileStore/OH/transaction_landing_stream_dir_fs"
json_landing = "/FileStore/OH/transaction_landing_dir"
schema = spark.read.json(json_landing).schema

# COMMAND ----------

def write_to_multiple_sinks(dataframe, batchId):
    dataframe.cache()
    dataframe.write.format("json").mode("append").save(json_landing_stream)
    dataframe.write.format("json").mode("append").save(json_landing_stream_fs)
    dataframe.unpersist()

# COMMAND ----------

df = (spark.readStream
      .format("json")
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .load(json_landing)
      # .withColumn("timestamp", current_timestamp())
      # .drop("class")
)

# COMMAND ----------

(df.writeStream
      .foreachBatch(write_to_multiple_sinks)
      .option("checkpointLocation", "/FileStore/OH/stream/checkpoint/")
      # .trigger(once=True)
      .start()
      # .awaitTermination()
      )
