# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="1800px" src ='https://owshqblobstg.blob.core.windows.net/stgfiles/png_files/cassi_develop_standards_3_gold_datapipeline.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# MAGIC %sql
# MAGIC use owshq;
# MAGIC 
# MAGIC SELECT * FROM user_silver;

# COMMAND ----------

# time taken = 1.20 [minutes] to ingest events

# execute sql to fetch all data from [silver tables] ~ data is being written into it constantly
# performing join of [user] and [vehicle]
# creating a dataframe to be inserted into delta table
# spark sql feature enables to write a sql query and save into a dataframe (language) agnostic
df_gold_user_plan = spark.sql(""" 
SELECT count (plan) as qty_plan, plan  FROM delta.`/mnt/delta-trn-bg-azure/silver/user` AS user
group by plan
order by 1 desc;
""")



# COMMAND ----------

display(df_gold_user_plan)

# COMMAND ----------

# writing into delta table
# gold table = user_plan
# important note = verify data lake storage (blob storage)
# curated zone (aggregations & data store)
df_gold_user_plan.write.format("delta").mode("overwrite").save("/mnt/delta-trn-bg-azure/gold/plan_analysis")

# for the sake of compatibility and integration surface you can write 
# into different file format such as orc, parquet, avro & json
# used for olap and dw

# COMMAND ----------

# MAGIC %sql
# MAGIC use owshq;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS gold_users_plan;
# MAGIC 
# MAGIC 
# MAGIC -- silver table user
# MAGIC CREATE TABLE gold_users_plan AS
# MAGIC SELECT * FROM delta.`/mnt/delta-trn-bg-azure/gold/plan_analysis`;

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC USE OwsHQ;
# MAGIC  
# MAGIC -- list tables on catalog
# MAGIC -- spark's metadata engine [hive]
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // reading table using [spark.table] function
# MAGIC // loading into a dataframe
# MAGIC // amount of rows = 14
# MAGIC val df_gold_users_plan_synapse_dedicated = spark.table("gold_users_plan")
# MAGIC df_gold_users_plan_synapse_dedicated.count()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // get the class of databricks & synapse
# MAGIC Class.forName("com.databricks.spark.sqldw.DefaultSource")
# MAGIC 
# MAGIC // mount string to connect into blob storage
# MAGIC // used as temporary storage to load using polybase
# MAGIC val blobStorage = "owshqblobstg.blob.core.windows.net"
# MAGIC val blobContainer = "cassi"
# MAGIC val blobAccessKey =  dbutils.secrets.get(scope = "storage", key = "blobstorage")
# MAGIC 
# MAGIC val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/synapse"
# MAGIC 
# MAGIC // set accounts to spark configuration
# MAGIC val acntInfo = "fs.azure.account.key."+ blobStorage
# MAGIC sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // setting up configurations to access the dedicated pool
# MAGIC // can use either secrets of azure key vault or databricks
# MAGIC val sqlDwUrl = dbutils.secrets.get(scope = "storage", key = "dedicatedpool")
# MAGIC 
# MAGIC spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")

# COMMAND ----------

# MAGIC %scala
# MAGIC // ds_gold_plan_users
# MAGIC // time taken = 17.39 minutes
# MAGIC // amount of rows = 434.170.658
# MAGIC // dwu = 300c
# MAGIC df_gold_users_plan_synapse_dedicated.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrl).option("dbtable","ds_gold_plan_users").option("forward_spark_azure_storage_credentials","True").option("tempdir", tempDir).mode("overwrite").save()
