# Databricks notebook source
import time
import pandas as pd
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Read in the credit card data into a spark 
# MAGIC dataframe

# COMMAND ----------

file_location = "s3://files.training.databricks.com/ohamdan/creditcard.csv"
file_type = 'csv'

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# COMMAND ----------

# The applied options are for CSV files. For other file types, these will be ignored.
# df = (spark.read.format(file_type)
#   .option("inferSchema", infer_schema)
#   .option("header", first_row_is_header)
#   .option("sep", delimiter)
#   .load(file_location)
#   .withColumn("transaction_id", monotonically_increasing_id()))
#   # .toPandas())



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE oh_anomaly_detection

# COMMAND ----------

df = spark.read.table('anomaly_bronze').where(col('transaction_id') >= 142403)

# COMMAND ----------

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC For demo purposes clear the landing location by removing all files

# COMMAND ----------

#Do a streaming read then a streaming write 
json_landing = "/FileStore/OH/transaction_landing_dir"
dbutils.fs.rm(json_landing, True)
dbutils.fs.mkdirs(json_landing)

# COMMAND ----------

# MAGIC %md
# MAGIC Add json records row by row into the landing location specified

# COMMAND ----------

(df.write.mode("overwrite")
   .option("maxRecordsPerFile", 100)
   .json(json_landing))

# COMMAND ----------

json_landing_stream = "/FileStore/OH/transaction_landing_stream_dir"
dbutils.fs.rm(json_landing_stream, True)
dbutils.fs.mkdirs(json_landing_stream)

# COMMAND ----------

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
