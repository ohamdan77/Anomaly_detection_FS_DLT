# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Read the data in *s3://files.training.databricks.com/classes/ohamdan/creditcard.csv* and create from it twoa Delta table following the below requirements:
# MAGIC * Create a Database to store the Delta tables meta data
# MAGIC * Add a column to the table calling it *transcation_id* as an increasing index
# MAGIC * select the *transaction_id* and the *class* columns and put them in a first Delta table *card_transactions_labels*
# MAGIC * drop the class column from that table and put all the remaining columns in a  second Delta table *crad_transaction_features*
# MAGIC * Create a Json Landing Dir and put half of the data in it

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS oh_anomaly_detection;
# MAGIC USE oh_anomaly_detection

# COMMAND ----------

raw_df = (spark.read
               .format("csv")
               .option("inferSchema", True)
               .option("header", True)
               .load("s3://files.training.databricks.com/classes/ohamdan/creditcard.csv")
               .coalesce(1)
               .withColumn("transaction_id", monotonically_increasing_id()))
              #  .withColumn("timestamp", current_timestamp()))

# COMMAND ----------

display(raw_df.select(max('transaction_id')))

# COMMAND ----------

# Create the bronze table from half of the transactions
raw_df.where(col('transaction_id') < 142403).write.saveAsTable("anomaly_bronze")

# COMMAND ----------

#Define the first Silver table with the transaction IDs and the Label
label_df = spark.table('anomaly_bronze').select("transaction_id", col("class").alias("fraud"))

# COMMAND ----------

label_df.write.saveAsTable("card_transaction_labels")

# COMMAND ----------

#Define the second Silver table with all the features and without the Label
feature_df = spark.table('anomaly_bronze').drop("class")

# COMMAND ----------

feature_df.write.saveAsTable("card_transaction_features")

# COMMAND ----------

#Create the json DF from the remaining half of the data
json_df = raw_df.where(col('transaction_id') >= 142403)

# COMMAND ----------

#Define the json Landing Dir
json_landing = "/FileStore/OH/transaction_landing_dir"
dbutils.fs.rm(json_landing, True)
dbutils.fs.mkdirs(json_landing)

# COMMAND ----------

#Write the json DF to json landing dir in files of 100 records each
(json_df.write.mode("overwrite")
   .option("maxRecordsPerFile", 100)
   .json(json_landing))
