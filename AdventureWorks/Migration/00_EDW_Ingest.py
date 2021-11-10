# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # EDW Ingestion - Regular Data Migration from the Data Warehouse   
# MAGIC 
# MAGIC 
# MAGIC In this example, we will maintain checkpoint files for each table in our database recording the last time we extracted data and brought it into our bronze ingestion zone. This allows us to pull all the data if there is no checkpoint file, or to only pull newly created data from our tables using the `ModifiedDate` field.    
# MAGIC 
# MAGIC 
# MAGIC This notebook is parameterized to receive the name of a table and pull the appropriate data, which allows me to write a single notebook that can extract data for all 74 tables. Often I will use a seperate orchestration tool to trigger this notebook, but instead I will simply call this notebook from a parent notebook using notebook workflows. Note that notebook workflows are not recommended for long running tasks.   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT DatabaseNamePrefix DEFAULT '';
# MAGIC CREATE WIDGET TEXT UserName DEFAULT '';
# MAGIC CREATE WIDGET TEXT TableName Default '';

# COMMAND ----------

# MAGIC %sql
# MAGIC SET var.database_name_prefix = $DatabaseNamePrefix ; 
# MAGIC SET var.user_name = $UserName ; 
# MAGIC SET var.table_name = $TableName
# MAGIC USE ${var.database_name_prefix}_adventureworks_metadata ;

# COMMAND ----------

import os 
import datetime
from pyspark.sql.functions import col, max, current_timestamp

# COMMAND ----------

database_name_prefix = dbutils.widgets.get("DatabaseNamePrefix")
user_name = dbutils.widgets.get("UserName")
table_name = (dbutils.widgets.get("TableName")).lower()

# COMMAND ----------

database_name = spark.sql("SELECT * FROM {}_adventureworks_metadata.table_metadata WHERE lower(table_name) = 'address'".format(database_name_prefix, table_name)).collect()[0][0].lower()
database_name

# COMMAND ----------

database_location = spark.sql("describe database {}".format(database_name)).filter(col("database_description_item") == "Location").select(col("database_description_value")).collect()[0][0]
print("Database Location: '{}'".format(database_location))

# COMMAND ----------

dbutils.fs.ls("/users/ryan.chynoweth@databricks.com/databases")

# COMMAND ----------

dbutils.fs.ls(database_location)

# COMMAND ----------

table_location = f'{database_location}/{table_name}'

## "_" before a directory or file name will make it hidden from spark for data operations 
checkpoint_location = f'/dbfs/{table_location}/_ingest_checkpoint.txt'

# COMMAND ----------

def checkpoint_read():
  """ Read the checkpoint file """
  if os.path.exists(checkpoint_location.replace('/dbfs:', '')):
    # READ CHECKPOINT IF EXISTS 
    with open(checkpoint_location.replace('/dbfs:', ''), 'r') as f:
      ckpt = f.readline()
    return ckpt
    
  else :
    # RETURN VERSION 0 if Checkoint does not exist 
    return '1900-01-01'
  
  
def checkpoint_update(timestamp):
  """ Update the checkpoint file """
  # WRITE CHECKPOINT 
  with open(checkpoint_location.replace('/dbfs:', ''), 'w') as f:
    f.write(str(timestamp))
  

# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "rac_scope", key = "azuresqluser")
jdbcPassword = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlpassword")
jdbcHostname = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlserver")
jdbcPort = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlport")
jdbcDatabase = dbutils.secrets.get(scope = "rac_scope", key = "azuresqldatabase")

# COMMAND ----------

url = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={jdbcUsername}@{jdbcDatabase};password={jdbcPassword};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

checkpoint_date = checkpoint_read()
checkpoint_date

# COMMAND ----------

df = (spark.read
        .format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("dbtable", "(SELECT TOP 1000 * FROM edw_migration.{} WHERE ModifiedDate >= '{}' ORDER BY ModifiedDate ASC) as data".format(table_name, checkpoint_date)) 
        .load()
       
       )

display(df)

# COMMAND ----------

df = df.withColumn("bronze_date", current_timestamp())

df.write.mode("append").option("mergeSchema", True).saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

display(spark.sql("SELECT * FROM {}.{}".format(database_name, table_name)))

# COMMAND ----------

new_checkpoint = df.select(max(col("ModifiedDate"))).collect()[0][0]
new_checkpoint

# COMMAND ----------

checkpoint_update(new_checkpoint)

# COMMAND ----------

checkpoint_date = checkpoint_read()
checkpoint_date

# COMMAND ----------

dbutils.notebook.exit("Table Ingestion Completed Successfully.")

# COMMAND ----------


