# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="1700px" src ='https://owshqblobstg.blob.core.windows.net/stgfiles/png_files/cassi_develop_standards_5_scd_datapipeline.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Row that will be use in the update
# updating last_name to show 

user_json_event = {
  "id": 1164,
  "uid": "c8247b2e-149e-455a-9385-ab58ced59edc",
  "password": "bUfMxFXGdr",
  "first_name": "Barbie",
  "last_name": "Joseph",
  "username": "barbie.feeney",
  "email": "barbie.feeney@email.com",
  "avatar": "https://robohash.org/inciduntipsaa.png?size=300x300&set=set1",
  "gender": "Agender",
  "phone_number": "+1-758 246-740-4093 x759",
  "social_insurance_number": "718877087",
  "date_of_birth": "1966-06-30",
  "employment": {
    "title": "Forward Accounting Agent",
    "key_skill": "Communication"
  },
  "address": {
    "city": "Port Kimberlymouth",
    "street_name": "Berry Summit",
    "street_address": "3514 Langworth Extension",
    "zip_code": "51591-8361",
    "state": "Montana",
    "country": "United States",
    "coordinates": {
      "lat": 37.9026779139,
      "lng": 135.8106315086
    }
  },
  "credit_card": {
    "cc_number": "4712-0503-4623-1224"
  },
  "subscription": {
    "plan": "Student",
    "status": "Idle",
    "payment_method": "Credit card",
    "term": "Annual"
  },
  "user_id": 474,
  "dt_current_timestamp": 1635284656153
}

get_user_event = spark.read.json(sc.parallelize([user_json_event]))
display(get_user_event)

# COMMAND ----------

# DBTITLE 1,Loading Data into DataFrames using [PySpark]
df_user = spark.read.json("/mnt/delta-trn-bg-azure/landing/user/*.json")
display(df_user)

# COMMAND ----------

# DBTITLE 1,Drop Tables if exists [testing]
# MAGIC %sql
# MAGIC USE owshq;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS owshq.users_scd;
# MAGIC DROP TABLE IF EXISTS owshq.updates;

# COMMAND ----------

# DBTITLE 1,Creating Database for Delta Tables using SQL [users]
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE users_scd (
# MAGIC    id_sk BIGINT GENERATED ALWAYS AS IDENTITY
# MAGIC    ,user_id long
# MAGIC    ,uid string
# MAGIC    ,id long
# MAGIC    ,first_name string
# MAGIC    ,last_name string
# MAGIC    ,old_last_name string
# MAGIC    ,date_of_birth string
# MAGIC    ,gender string
# MAGIC    ,dt_current_timestamp bigint
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Writing landing data into delta table users [users_scd]
select_columns_users = df_user.select("user_id","uid","id","first_name","last_name","date_of_birth","gender","dt_current_timestamp").write.format("delta").mode("overwrite").saveAsTable("users_scd")

# COMMAND ----------

# DBTITLE 1,Writing landing data into delta table users [updates]
update_columns_users = get_user_event.select("user_id","uid","id","first_name","last_name","date_of_birth","gender","dt_current_timestamp").write.format("delta").mode("overwrite").saveAsTable("updates")

# COMMAND ----------

# DBTITLE 1,Count data in delta table
# MAGIC %sql
# MAGIC 
# MAGIC SELECT user_id, COUNT(*) AS amt
# MAGIC FROM owshq.users_scd 
# MAGIC GROUP BY user_id
# MAGIC HAVING COUNT(*) < 2

# COMMAND ----------

# DBTITLE 1,Merge using Type 3 [SCD]
# MAGIC %sql 
# MAGIC  
# MAGIC -- merge available on dbr 5.1 
# MAGIC 
# MAGIC MERGE INTO owshq.users_scd
# MAGIC USING owshq.updates
# MAGIC ON updates.user_id = users_scd.user_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET users_scd.old_last_name = users_scd.last_name,
# MAGIC              users_scd.last_name = updates.last_name        
# MAGIC WHEN NOT MATCHED            
# MAGIC   THEN INSERT (user_id,uid,id,first_name,last_name,date_of_birth,gender,dt_current_timestamp) 
# MAGIC        VALUES (user_id,uid,id,first_name,last_name,date_of_birth,gender,dt_current_timestamp)
# MAGIC 
# MAGIC -- new value = "joseph"

# COMMAND ----------

# DBTITLE 1,Verify change
# MAGIC %sql
# MAGIC -- must have data in column old_last_name
# MAGIC SELECT * 
# MAGIC FROM owshq.users_scd
# MAGIC WHERE user_id = 474

# COMMAND ----------

# DBTITLE 1,Validate rows
# MAGIC %sql
# MAGIC -- not new rows was created
# MAGIC SELECT count(user_id),user_id FROM owshq.users_scd
# MAGIC group by user_id
# MAGIC order by 1 asc
# MAGIC --where user_id = 3781
