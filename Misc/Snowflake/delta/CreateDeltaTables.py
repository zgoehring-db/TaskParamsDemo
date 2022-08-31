# Databricks notebook source
# MAGIC %md
# MAGIC # Create Delta Tables
# MAGIC 
# MAGIC This notebook creates two separate Delta tables to test the integration between Delta Lake and Snowflake.  

# COMMAND ----------

dbutils.widgets.text("delta_path","") # ex. abfss://snowflakedemo@racadlsgen2.dfs.core.windows.net/delta_table
dbutils.widgets.text("delta_path_manifest","") # ex. abfss://snowflakedemo@racadlsgen2.dfs.core.windows.net/delta_table_manifest
dbutils.widgets.text("schema_name", "")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}".format(dbutils.widgets.get('schema_name')))

# COMMAND ----------

spark.sql("USE {}".format(dbutils.widgets.get('schema_name')))

# COMMAND ----------

dbutils.fs.ls("abfss://snowflakedemo@racadlsgen2.dfs.core.windows.net/")

# COMMAND ----------

source_json = "/databricks-datasets/structured-streaming/events"
delta_path = dbutils.widgets.get("delta_path")
delta_path_manifest = dbutils.widgets.get("delta_path_manifest")

# COMMAND ----------

df = spark.read.json(source_json)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("json_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TABLE delta_table 
  (
    action string,
    time long
  )
  LOCATION '{}'

""".format(delta_path))

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO delta_table 
# MAGIC SELECT * FROM json_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifest Table

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TABLE delta_table_manifest
  (
    action string,
    time long
  )
  LOCATION '{}'

""".format(delta_path_manifest))

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO delta_table_manifest 
# MAGIC SELECT * FROM json_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_table_manifest

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.symlinkFormatManifest.fileSystemCheck.enabled = false
# MAGIC -- if this is not set then the next command will fail.
# MAGIC -- supposedly this works on AWS only. We will see if Azure works before moving clouds. 

# COMMAND ----------

# Generate manifests of a Delta table using Databricks Runtime
from delta import *
deltaTable = DeltaTable.forPath(spark, delta_path_manifest)
deltaTable.generate("symlink_format_manifest")

# COMMAND ----------

dbutils.fs.ls(delta_path_manifest)

# COMMAND ----------


