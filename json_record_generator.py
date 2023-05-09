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

df = spark.read.table('oh_anomaly_detection.anomaly_bronze').where(col('transaction_id') >= 142403)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename id column to cust_id and drop amount and class columns so this is identical to what the model was trained on

# COMMAND ----------

# df.reset_index(inplace=True)
# df.rename(columns={'index':'transaction_id'}, inplace=True)
# df.drop(columns=df.columns[-1], 
#         axis=1, 
#         inplace=True)


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

len(dbutils.fs.ls("/FileStore/OH/transaction_landing_dir"))

# COMMAND ----------

display(spark.read.json("/FileStore/OH/transaction_landing_dir"))

# COMMAND ----------

# MAGIC %md
# MAGIC Add json records row by row into the landing location specified

# COMMAND ----------

(df.write.mode("overwrite")
   .option("maxRecordsPerFile", 100)
   .json(json_landing))

# COMMAND ----------

dbutils.fs.rm("/FileStore/OH/transaction_landing_timestamp_dir", True)

# COMMAND ----------

json_landing_stream = "/FileStore/OH/transaction_landing_stream_dir"
dbutils.fs.rm(json_landing_stream, True)
dbutils.fs.mkdirs(json_landing_stream)

# COMMAND ----------

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
      .start(json_landing_with_time)
      )

# COMMAND ----------

display(spark.read.json(json_landing_with_time))

# COMMAND ----------

dbutils.fs.ls(json_landing_with_time)

# COMMAND ----------

#Parametrize this at the notebook level with a widget so it ...

# COMMAND ----------

#Add timestamp for the time in which this was generated, but for the sake of calling out for it 

# COMMAND ----------

import time 
import random 

i = 0
for json_dict in df.to_dict(orient='records'):
  dbutils.fs.put("{}/row{}.json".format(json_landing,i), str(json_dict))
  i += 1 
  #time.sleep(random.random())


# COMMAND ----------

# MAGIC
# MAGIC %fs
# MAGIC rm -r "/FileStore/OH/transaction_landing_dir"
