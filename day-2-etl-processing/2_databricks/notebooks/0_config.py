# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="1500px" src ='https://owshqblobstg.blob.core.windows.net/stgfiles/png_files/cassi_develop_standards_0_config.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="2200px" src ='https://owshqblobstg.blob.core.windows.net/stgfiles/png_files/cassi_develop_standards_0_config_datalake.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Only for demonstration purposes
dbutils.fs.unmount("/mnt/landing-trn-bg-azure")
dbutils.fs.unmount("/mnt/delta-trn-bg-azure")

# COMMAND ----------

# DBTITLE 1,Mount Landing Zone
# mounting point are abstractions from the data lake endpoint
# development and productiong don't need to know where the files are sitting
try:
    dbutils.fs.mount(
      source = "wasbs://landing@owshqblobstg.blob.core.windows.net/",
      mount_point = "/mnt/landing-trn-bg-azure",
      extra_configs = {"fs.azure.account.key.owshqblobstg.blob.core.windows.net": dbutils.secrets.get(scope = "storage", key = "blobstorage")})
except Exception as e:
  print("already mounted. Try to unmount first")


# COMMAND ----------

# DBTITLE 1,Mount Delta Zone
# mounting point are abstractions from the data lake endpoint
# development and productiong don't need to know where the files are sitting

try:
    dbutils.fs.mount(
      source = "wasbs://cassi@owshqblobstg.blob.core.windows.net",
      mount_point = "/mnt/delta-trn-bg-azure",
      extra_configs = {"fs.azure.account.key.owshqblobstg.blob.core.windows.net": dbutils.secrets.get(scope = "storage", key = "blobstorage")})
except Exception as e:
  print("already mounted. Try to unmount first")

# COMMAND ----------

# DBTITLE 1,Mount Synapse temp folder
# mounting point are abstractions from the data lake endpoint
# development and productiong don't need to know where the files are sitting

try:
    dbutils.fs.mount(
      source = "wasbs://cassi@owshqblobstg.blob.core.windows.net",
      mount_point = "/mnt/synapse-trn-bg-azure",
      extra_configs = {"fs.azure.account.key.owshqblobstg.blob.core.windows.net": dbutils.secrets.get(scope = "storage", key = "blobstorage")})
except Exception as e:
  print("already mounted. Try to unmount first")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/delta-trn-bg-azure"))
display(dbutils.fs.ls("/mnt/landing-trn-bg-azure"))
