# Databricks notebook source


# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print("Using: {}.{}".format(catalog_name, schema_name))

# COMMAND ----------

user_name = spark.sql("SELECT current_user()").collect()[0][0]
print(user_name)

# COMMAND ----------

spark.sql("USE CATALOG {}".format(catalog_name))

# COMMAND ----------

spark.sql("USE {}".format(schema_name))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


