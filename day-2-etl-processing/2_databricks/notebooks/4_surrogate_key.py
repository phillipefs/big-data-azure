# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="2000px" src ='https://owshqblobstg.blob.core.windows.net/stgfiles/png_files/cassi_develop_standards_4_surrogate_datapipeline.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Delta lake solution for Surrogate creation
# delta lake surrogate key available
# https://www.databricks.com/blog/2022/08/08/identity-columns-to-generate-surrogate-keys-are-now-available-in-a-lakehouse-near-you.html

# COMMAND ----------

# DBTITLE 1,Create delta table with surrogate field
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE credit_card_sk (
# MAGIC    id_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC    credit_card_expiry_date string
# MAGIC   ,credit_card_number string
# MAGIC   ,credit_card_type string
# MAGIC   ,dt_current_timestamp long
# MAGIC   ,id long
# MAGIC   ,uid string
# MAGIC   ,user_id long
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Verify the table
# MAGIC %sql
# MAGIC 
# MAGIC show tables;

# COMMAND ----------

# DBTITLE 1,Loading Data into DataFrames using [PySpark]
# read the landing files
df_credit_card = spark.read.json("/mnt/landing-trn-bg-azure/credit_card/*.json")

# write mode
write_delta_mode = "append"

# insert into the delta table
df_credit_card.write.mode(write_delta_mode).format("delta").saveAsTable("owshq.credit_card_sk")


# COMMAND ----------

# DBTITLE 1,Query delta lake with Surrogate Key
# MAGIC %sql
# MAGIC 
# MAGIC select * from credit_card_sk
# MAGIC order by 1 asc

# COMMAND ----------

# DBTITLE 1,Delta Table to Pyspark
df_credit_card = spark.table('credit_card_sk')
display(df_credit_card)
