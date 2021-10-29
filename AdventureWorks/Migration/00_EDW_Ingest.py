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
# MAGIC CREATE WIDGET TEXT DatabaseName DEFAULT '';
# MAGIC CREATE WIDGET TEXT UserName DEFAULT '';
# MAGIC CREATE WIDGET TEXT TableName Default '';

# COMMAND ----------

# MAGIC %sql
# MAGIC SET var.database_name = $DatabaseName ; 
# MAGIC SET var.user_name = $UserName ; 
# MAGIC SET var.table_location = '/users/${var.user_name}/databases/${var.database_name}' ;
# MAGIC CREATE DATABASE IF NOT EXISTS ${var.database_name} LOCATION ${var.table_location} ;
# MAGIC USE ${var.database_name} ;

# COMMAND ----------

import os 
import datetime
from pyspark.sql.functions import col, max, current_timestamp

# COMMAND ----------

database_name = dbutils.widgets.get("DatabaseName")
user_name = dbutils.widgets.get("UserName")
table_name = (dbutils.widgets.get("TableName")).lower()

table_location = f'/users/{user_name}/databases/{database_name}/{table_name}'

## "_" before a directory or file name will make it hidden from spark for data operations 
checkpoint_location = f'/dbfs/users/{user_name}/databases/{database_name}/{table_name}/_ingest_checkpoint.txt'


# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "rac_scope", key = "azuresqluser")
jdbcPassword = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlpassword")
jdbcHostname = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlserver")
jdbcPort = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlport")
jdbcDatabase = dbutils.secrets.get(scope = "rac_scope", key = "azuresqldatabase")

# COMMAND ----------

url = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={jdbcUsername}@{jdbcDatabase};password={jdbcPassword};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

checkpoint_date = '1900-01-01 00:00:00'
if os.path.exists(f"/dbfs/{checkpoint_location}"):
  check_file = open(checkpoint_location,"r") 
  checkpoint_date = (check_file.readline())
  check_file.close()

# COMMAND ----------

df = (spark.read
        .format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("dbtable", "(SELECT TOP 1000 * FROM edw_migration.{} WHERE ModifiedDate >= '{}' ORDER BY ModifiedDate ASC) as data".format(table_name, checkpoint_date)) 
        .load()
       
       )

# COMMAND ----------

df = df.withColumn("bronze_date", current_timestamp())

df.write.mode("append").option("mergeSchema", True).saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from $TableName limit 100

# COMMAND ----------

new_checkpoint = df.select(max(col("ModifiedDate"))).collect()[0][0]
new_checkpoint

# COMMAND ----------

check_file = open(checkpoint_location,"w") 
check_file.write(str(new_checkpoint))
check_file.close()

# COMMAND ----------

dbutils.notebook.exit("Table Ingestion Completed Successfully.")
