# Databricks notebook source
# MAGIC %md ## Notebook Set Up

# COMMAND ----------

dbutils.widgets.text("database_name", "")
dbutils.widgets.text("scope_name", "")
dbutils.widgets.text("tenant_id", "")
dbutils.widgets.text("client_id_key", "")
dbutils.widgets.text("client_secret_key", "")
dbutils.widgets.text("user_name", "")
dbutils.widgets.text("source_storage_account", "")
dbutils.widgets.text("source_container", "")

# COMMAND ----------

database_name = dbutils.widgets.get("database_name")
client_id_key = dbutils.widgets.get("client_id_key")
client_secret_key = dbutils.widgets.get("client_secret_key")
scope_name = dbutils.widgets.get("scope_name")
user_name = dbutils.widgets.get("user_name")
source_storage_account = dbutils.widgets.get("source_storage_account")
source_container = dbutils.widgets.get("source_container")

# COMMAND ----------

client_id = dbutils.secrets.get(scope_name, client_id_key)
client_secret = dbutils.secrets.get(scope_name, client_secret_key)

# COMMAND ----------

source_location = f"abfss://{source_container}@{source_storage_account}.dfs.core.windows.net/"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name} LOCATION '{source_location}'")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

dbutils.fs.ls(source_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE store_returns

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE customer_address

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE customer_demographics

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Tables

# COMMAND ----------

table_names = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer_address", "customer_demographics", "customer", 
                       "date_dim", "household_demographics", "income_band", "inventory", "item", "promotion", "reason", "ship_mode", 
                      "store_returns", "store_sales", "store", "time_dim", "warehouse", "web_page", "web_returns"] 


#, "web_sales", "web_site"

# COMMAND ----------

for t in table_names:
  spark.sql("""
  CREATE TABLE {}
  USING DELTA 
  LOCATION 'abfss://tpc@racadlsgen2.dfs.core.windows.net/{}'
  """.format(t, t))

# COMMAND ----------

for t in table_names:
  cnt = spark.read.table(t).rdd.countApprox(timeout=60)
  print("---- {} | {}".format(t, cnt))

# COMMAND ----------

# ---- call_center | 60
# ---- catalog_page | 46000
# ---- catalog_returns | 4320001270
# ---- catalog_sales | 43200005594
# ---- customer_address | 40,000,000 *
# ---- customer_demographics | 1,920,800 *
# ---- customer | 79,999,999
# ---- date_dim | 73049
# ---- household_demographics | 7200
# ---- income_band | 20
# ---- inventory | 1,627,857,000
# ---- item | 462000
# ---- promotion | 2300
# ---- reason | 72
# ---- ship_mode | 20
# ---- store_returns | 8,640,063,794 * 

# COMMAND ----------

from pyspark.sql.functions import input_file_name

display(spark.read.json(f"abfss://{source_container}@{source_storage_account}.dfs.core.windows.net/catalog_page/_delta_log/*.crc").withColumn("file_name", input_file_name()))

# COMMAND ----------



# COMMAND ----------


