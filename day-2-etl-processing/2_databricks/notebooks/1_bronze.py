# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Reading Files from Data Lake Gen2 [Bronze Zone]
# MAGIC > *access the raw data from your microservices and transform into medallion architecture*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="1800px" src ='https://owshqblobstg.blob.core.windows.net/stgfiles/png_files/cassi_develop_standards_1_bronze_datapipeline.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,List Landing folders
# list landing zone 
# fed by etl or elt processes
# databricks file system (dbfs)
display(dbutils.fs.ls("/mnt/landing-trn-bg-azure"))

# list folders to be used for the processing
# investigate the files and their types
# json is the predominant for majority of the apps
display(dbutils.fs.ls("/mnt/landing-trn-bg-azure/user"))
display(dbutils.fs.ls("/mnt/landing-trn-bg-azure/credit_card"))
display(dbutils.fs.ls("/mnt/landing-trn-bg-azure/subscription"))
display(dbutils.fs.ls("/mnt/landing-trn-bg-azure/commerce"))
display(dbutils.fs.ls("/mnt/landing-trn-bg-azure/bank"))

# COMMAND ----------

# DBTITLE 1,Loading Data into DataFrames using [PySpark]
# using pyspark to load data using the [latest api]
# inspect blob storage landing location ~ json files arriving constantly
# read files from mount point ~ storage
# load data into dataframe (table in-memory) spread across the cluster
df_user = spark.read.json("/mnt/landing-trn-bg-azure/user/*.json")
df_credit_card = spark.read.json("/mnt/landing-trn-bg-azure/credit_card/*.json")
df_subscription = spark.read.json("/mnt/landing-trn-bg-azure/subscription/*.json")
df_commerce = spark.read.json("/mnt/landing-trn-bg-azure/commerce/*.json")
df_bank = spark.read.json("/mnt/landing-trn-bg-azure/bank/*.json")


# COMMAND ----------

# DBTITLE 1,Count Dataframe values
df_user.count()
df_credit_card.count()
df_subscription.count()
df_commerce.count()
df_bank.count()

# COMMAND ----------

# DBTITLE 1,Delta Lake ~ [Bronze]
# landing zone is cleaned after data processing of apache spark
# if not possible, merge or reading differential based on partitions 
# the most common approach is to remove everything from landing after conversion
# that guarantees that you don't have to read full cycles of data into storage

# set write mode = complete, append and overwrite
# append = atomically add bew data into existing delta table
write_delta_mode = "append"

# set processing zone location = dbfs:/mnt/owshq/delta/bronze
delta_processing_store_zone = "/mnt/delta-trn-bg-azure/bronze"

# write into delta format
df_user.write.mode(write_delta_mode).format("delta").save(delta_processing_store_zone + "/user/")
df_credit_card.write.mode(write_delta_mode).format("delta").save(delta_processing_store_zone + "/credit_card/")
df_subscription.write.mode(write_delta_mode).format("delta").save(delta_processing_store_zone + "/subscription/")
df_commerce.write.mode(write_delta_mode).format("delta").save(delta_processing_store_zone + "/commerce/")
df_bank.write.mode(write_delta_mode).format("delta").save(delta_processing_store_zone + "/bank/")

# COMMAND ----------

# DBTITLE 1,Creating Database for Delta Tables using SQL [Bronze]
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS owshq

# COMMAND ----------

# DBTITLE 1,Creating delta bronze tables
# MAGIC %sql
# MAGIC use owshq;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS user_bronze;
# MAGIC DROP TABLE IF EXISTS credit_card_bronze;
# MAGIC DROP TABLE IF EXISTS subscription_bronze;
# MAGIC DROP TABLE IF EXISTS commerce_bronze;
# MAGIC DROP TABLE IF EXISTS bank_bronze;
# MAGIC 
# MAGIC -- bronze table user
# MAGIC CREATE TABLE user_bronze AS
# MAGIC SELECT * FROM delta.`/mnt/delta-trn-bg-azure/bronze/user/`;
# MAGIC 
# MAGIC 
# MAGIC -- bronze table credit_card
# MAGIC CREATE TABLE credit_card_bronze AS
# MAGIC SELECT * FROM delta.`/mnt/delta-trn-bg-azure/bronze/credit_card/`;
# MAGIC 
# MAGIC -- bronze table subscription
# MAGIC CREATE TABLE subscription_bronze AS
# MAGIC SELECT * FROM delta.`/mnt/delta-trn-bg-azure/bronze/subscription/`;
# MAGIC 
# MAGIC -- bronze table commerce
# MAGIC CREATE TABLE commerce_bronze AS
# MAGIC SELECT * FROM delta.`/mnt/delta-trn-bg-azure/bronze/commerce/`;
# MAGIC 
# MAGIC -- bronze table bank
# MAGIC CREATE TABLE bank_bronze AS
# MAGIC SELECT * FROM delta.`/mnt/delta-trn-bg-azure/bronze/bank/`;

# COMMAND ----------

# DBTITLE 1,Count users_bronze delta table
# MAGIC %sql
# MAGIC 
# MAGIC select * from user_bronze;

# COMMAND ----------

# DBTITLE 1,Count credit_card_bronze delta table
# MAGIC %sql
# MAGIC select * from credit_card_bronze;

# COMMAND ----------

# DBTITLE 1,Count subscription_bronze delta table
# MAGIC %sql
# MAGIC select * from subscription_bronze;

# COMMAND ----------

# DBTITLE 1,Count commerce_bronze delta table
# MAGIC %sql
# MAGIC select * from commerce_bronze;

# COMMAND ----------

# DBTITLE 1,Count bank_bronze delta table
# MAGIC %sql
# MAGIC select * from bank_bronze;
