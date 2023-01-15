# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="1800px" src ='https://owshqblobstg.blob.core.windows.net/stgfiles/png_files/cassi_develop_standards_2_silver_datapipeline.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Delta Lake ~ [Silver]
# import libraries
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql.functions import expr, col


# init read from delta to a dataframe
# get schema of the bronze delta table
df_silver_user = spark.read.format("delta").load("/mnt/delta-trn-bg-azure/bronze/user")
df_silver_credit_card = spark.read.format("delta").load("/mnt/delta-trn-bg-azure/bronze/credit_card")
df_silver_subscription = spark.read.format("delta").load("/mnt/delta-trn-bg-azure/bronze/subscription")
df_silver_commerce = spark.read.format("delta").load("/mnt/delta-trn-bg-azure/bronze/commerce")
df_silver_bank = spark.read.format("delta").load("/mnt/delta-trn-bg-azure/bronze/bank")



# using [pyspark] to perform the data plumbing
# start to massage the data to deliver to [silver] table
# process to filter, clean and augment data

# [user]
# concat columns
# access struct fields
# rename columns for better understanding
transf_silver_user = df_silver_user.select(
  col("user_id").alias("id"),
  concat_ws(" ", col("first_name"), col("last_name")).alias("name"),
  col("gender").alias("gender"),
  col("address.country").alias("country"),
  col("address.state").alias("state"),
  col("employment.title").alias("title")
)

# transformed dataframes
# ~ transf_silver_user for [user]


# COMMAND ----------

# DBTITLE 1,Show data inside dataframe
# user data
display(transf_silver_user)

# COMMAND ----------

# DBTITLE 1,Create materialized views
# create materialized views from these dataframes

transf_silver_user.createOrReplaceTempView("vw_silver_user")
df_silver_credit_card.createOrReplaceTempView("vw_silver_credit_card")
df_silver_subscription.createOrReplaceTempView("vw_silver_subscription")
df_silver_commerce.createOrReplaceTempView("vw_silver_commerce")
df_silver_bank.createOrReplaceTempView("vw_silver_bank")



# COMMAND ----------

# DBTITLE 1,Create Domain table [user]
df_user_domain = spark.sql(""" 
SELECT user.id
    ,user.name
    ,user.gender 
    ,user.country 
    ,user.state
    ,credit.credit_card_expiry_date
    ,credit.credit_card_number
    ,credit.credit_card_type
    ,credit.user_id as credit_card_user
    ,sub.payment_method
    ,sub.payment_term
    ,sub.plan
    ,sub.status as subscription_status
    ,sub.user_id as sub_user_id
    ,bank.account_number
    ,bank.bank_name
FROM vw_silver_user AS user
INNER JOIN vw_silver_credit_card AS credit
ON user.id = credit.user_id
INNER JOIN vw_silver_subscription as sub
ON credit.user_id = sub.user_id
INNER JOIN vw_silver_bank as bank
ON credit.user_id = bank.user_id
Limit 1000000
""")




# COMMAND ----------

# DBTITLE 1,Show data inside dataframe [Domain User]
display(df_user_domain)

# COMMAND ----------

# DBTITLE 1,Count Domain Table User
# count
# 5.357.100.288 Full
# limit 10000

df_user_domain.count()

# COMMAND ----------

# DBTITLE 1,Create database [SQL]
# MAGIC %sql
# MAGIC  
# MAGIC CREATE DATABASE IF NOT EXISTS owshq;

# COMMAND ----------

# landing zone is cleaned after data processing of apache spark
# if not possible, merge or reading differential based on partitions 
# the most common approach is to remove everything from landing after conversion
# that guarantees that you don't have to read full cycles of data into storage

# set write mode = complete, append and overwrite
# append = atomically add bew data into existing delta table
write_delta_mode = "append"

# set processing zone location = dbfs:/mnt/owshq/delta/bronze
delta_processing_store_zone = "/mnt/delta-trn-bg-azure/silver"

# write into delta format
df_user_domain.write.mode(write_delta_mode).format("delta").save(delta_processing_store_zone + "/user/")

# COMMAND ----------

# DBTITLE 1,Create delta tables [SQL]
# MAGIC %sql
# MAGIC use owshq;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS user_silver;
# MAGIC 
# MAGIC 
# MAGIC -- silver table user
# MAGIC CREATE TABLE user_silver AS
# MAGIC SELECT * FROM delta.`/mnt/delta-trn-bg-azure/silver/user/`;

# COMMAND ----------

# DBTITLE 1,Query silver table [User]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT count (*) FROM user_silver;
