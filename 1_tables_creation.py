# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Read the data in *s3://files.training.databricks.com/ohamdan/creditcard.csv* and create from it twoa Delta table following the below requirements:
# MAGIC * Create a Database to store the Delta tables meta data
# MAGIC * Add a column to the table calling it *transcation_id* as an increasing index
# MAGIC * Add another column calling it *timestamp* equal to the current timestamp
# MAGIC * select the *transaction_id* and the *class* columns and put them in a first Delta table *card_transactions_labels*
# MAGIC * drop the class column from that table and put all the remaining columns in a  second Delta table *crad_transaction_features*

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP DATABASE IF EXISTS oh_anomaly_detection CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS oh_anomaly_detection;
# MAGIC USE oh_anomaly_detection

# COMMAND ----------

raw_df = (spark.read
               .format("csv")
               .option("inferSchema", True)
               .option("header", True)
               .load("s3://files.training.databricks.com/ohamdan/creditcard.csv")
               .coalesce(1)
               .withColumn("transaction_id", monotonically_increasing_id()))
              #  .withColumn("timestamp", current_timestamp()))

# COMMAND ----------

display(raw_df.select(max('transaction_id')))

# COMMAND ----------

raw_df.write.saveAsTable("anomaly_bronze")

# COMMAND ----------

label_df = spark.table('anomaly_bronze').where(col('transaction_id') < 142403).select("transaction_id", col("class").alias("fraud"))

# COMMAND ----------

label_df.write.saveAsTable("card_transaction_labels")

# COMMAND ----------

feature_df = spark.table('anomaly_bronze').where(col('transaction_id') < 142403).drop("class")

# COMMAND ----------

feature_df.write.saveAsTable("card_transaction_features")
