# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest JSON Data 
# MAGIC 
# MAGIC In this demo we are calling an API to ingest JSON data and bring it into Databricks Delta. 
# MAGIC 
# MAGIC There are many ways that one can deploy this ingestion process, however, in this example I will be using a Databricks notebook and relying mostly on the [Python requests package](https://pypi.org/project/requests/). Since this process really does not require distributing my data I will be using a single node Databricks cluster to reduce costs. One benefit of writing this ingestion process in Databricks is that [Unity Catalog offers Data Lineage](https://www.databricks.com/blog/2022/06/08/announcing-the-availability-of-data-lineage-with-unity-catalog.html.     
# MAGIC 
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/SingleNodeCluster.png" />
# MAGIC 
# MAGIC Examples of other ingestion options: 
# MAGIC - Cloud Functions (Azure functions, AWS Lambda etc.)
# MAGIC - Azure Data Factory 
# MAGIC - [FiveTran](https://fivetran.com/docs/functions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Options  
# MAGIC 
# MAGIC **Option 1** - Request data from API and save directly to delta   
# MAGIC **Option 2** - Request data from API, save as JSON files to raw directory, then load into Delta tables using AutoLoader (my preferred route)   
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Option 2 is preferred in my opinion because it will track unaltered versions of your data as JSON files which is considered a best practice. If you write directly to delta there could be some transformations that occur prior to writing to the table. Please note that in this case option 2 does have a double read cost (API + AutoLoader) but the raw data being stored is extremely valuable and worth it.   

# COMMAND ----------

import requests
import os
import datetime 
import time
import pandas as pd 
import json

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

user_name = spark.sql("SELECT current_user()").collect()[0][0]
print(user_name)

# COMMAND ----------

dbutils.widgets.text("api_key", "") ### NOTE - this should be stored as a secret. But for demo purposes it is a widget
dbutils.widgets.text("database_name", "") ### Note - this can be a widget or an environment variable  
dbutils.widgets.text

api_key = dbutils.widgets.get("api_key")
database_name = dbutils.widgets.get("database_name")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))

# COMMAND ----------

spark.sql("USE {}".format(database_name))

# COMMAND ----------

# seattle, Texarkana, AR   , Pleasanton, Boise
city_list = [(47.6, -122.3, 'Seattle'), (33.44, -94.04, 'Texarkana'), (37.6, -121.8, 'Pleasanton'), (43.6, -116.2, 'Boise')]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1 - Save directly to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_weather_api

# COMMAND ----------

def request_data(lat, long, key):
  api_url = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&exclude=hourly,daily&appid={}".format(lat, long, key)
  data = requests.get(api_url)

  return json.loads(data.content.decode("utf-8"))


# COMMAND ----------

stop_time = datetime.datetime.utcnow() + datetime.timedelta(minutes = 1)
while datetime.datetime.utcnow() < stop_time: 
  for c in city_list:
    pdf = pd.DataFrame([request_data(c[0], c[1], api_key)])

    (spark
     .createDataFrame(pdf)
     .write
     .option("overwriteSchema", "true")
     .mode("append")
     .saveAsTable("bronze_weather_api")
    )
  time.sleep(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_weather_api

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Option 2 - Raw files with AutoLoader 

# COMMAND ----------

raw_data_directory = "/dbfs/Users/{}/api_weather_demo/raw".format(user_name)
raw_schema = "/Users/{}/api_weather_demo/raw/_schema".format(user_name)
raw_checkpoint = "/Users/{}/api_weather_demo/_checkpoint".format(user_name)
bronze_location = "/Users/{}/api_weather_demo/bronze/weather_data".format(user_name)

dbutils.fs.mkdirs(raw_data_directory.replace("/dbfs", ""))

# COMMAND ----------

#####  
##  Request JSON Data and Save to files  
#####  

stop_time = datetime.datetime.utcnow() + datetime.timedelta(minutes = 1)
while datetime.datetime.utcnow() < stop_time: 
  for c in city_list:
    data = request_data(c[0], c[1], api_key) 
    
    data_path = "{}/{}_{}.json".format(raw_data_directory, c[2], datetime.datetime.utcnow().strftime("%d%m%Y%H%M%S"))
    with open(data_path, 'w') as f:
      json.dump(test_d, f)
    
    
  time.sleep(10)

# COMMAND ----------

dbutils.fs.ls(raw_data_directory.replace("/dbfs", ""))

# COMMAND ----------



# COMMAND ----------

#####  
##  Load data with Auto Loader and Trigger Once into Delta
##      - 
#####  


display(spark.readStream.format("cloudFiles") 
  .option("cloudFiles.format", "json") 
  .option("cloudFiles.schemaLocation", raw_schema) 
  .load(raw_data_directory.replace("/dbfs", "")) + "/*.json")
#   .writeStream \
#   .option("mergeSchema", "true") \
#   .option("checkpointLocation", raw_checkpoint) \
#   .trigger(once=True)
#   .start(bronze_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_weather_delta
# MAGIC AS 
# MAGIC SELECT * 
# MAGIC FROM delta.``""
