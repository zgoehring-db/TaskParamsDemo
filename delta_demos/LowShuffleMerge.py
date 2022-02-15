# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Low Shuffle Merge
# MAGIC 
# MAGIC A Spark Shuffle is the process of redistributing or re-partitioning data between the executors in your cluster. The shuffle operation can be very expensive and should be avoided when possible, however, most complex operations require shuffling to some degree. 
# MAGIC 
# MAGIC Merging data is the process of updating, inserting, and deleting data based on a source dataset into a target dataset. Under the hood, when you do a merge in Databricks using Delta we do two Scans. 
# MAGIC 1. A scan is completed to do an inner join between the target and source to figure out which files have matches.   
# MAGIC 1. A second scan is done to do an outer join between the selected files in the target and source to write the updated/deleted/inserted data as needed.  
# MAGIC 
# MAGIC After we do our scans we have now identified which files we are going to be working with. At this point we will complete 
# MAGIC 
# MAGIC Using our Low Shuffle Merge...
# MAGIC 
# MAGIC 
# MAGIC **When to use Low Shuffle Merge?**   
# MAGIC 
# MAGIC When merging data, Databricks needs to work on a per file basis to complete the operations. This includes processing all the rows that are stored in the file regardless to whether or not they are being updated. The goal of low shuffle merge optimizes the rows that do not need to be updated. Previously, the unmatched rows were being updated the same as the updated rows which required passing them through multiple stages and being shuffled. Now with the low shuffle merge we are able to process the unchanged rows without shuffles which reduces the overall cost of the operation.  
# MAGIC 
# MAGIC *It is best to use low shuffle merge when there are a small amount of records being updated*, however, you will see improvements in performance regardless due to the fact that we are able to better optimize the lay out of the data. The new merge functionality preserves the existing layout of the unmodified records which means that your performance will not degrade after a merge command and does not necessarily require an `optimize` each time. 
# MAGIC 
# MAGIC 
# MAGIC **How to enable Low Shuffle Merge?**
# MAGIC 
# MAGIC Enabling low shuffle merge is done on the session or cluster level using spark configuration. Run the following command in the notebook:
# MAGIC ```
# MAGIC %python
# MAGIC spark.sql("SET spark.databricks.delta.merge.enableLowShuffle = true")
# MAGIC ```
# MAGIC Or simple put the following into the spark configuration on your cluster
# MAGIC ```
# MAGIC spark.databricks.delta.merge.enableLowShuffle true
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC **Resources**: 
# MAGIC - Tech Talk - [DML Internals: Delete, Update, Merge](https://databricks.com/discover/diving-into-delta-lake-talks/how-delete-update-merge-work)
# MAGIC - Merge Into [Documentation](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-merge-into.html)
# MAGIC - Low Shuffle Merge [Blog](https://databricks.com/blog/2021/09/08/announcing-public-preview-of-low-shuffle-merge.html)
# MAGIC - Low Shuffle Merge [Documentation](https://docs.databricks.com/delta/optimizations/low-shuffle-merge.html)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd 

# COMMAND ----------

dbutils.widgets.text('DatabaseName', '')

# COMMAND ----------

db_name = dbutils.widgets.get('DatabaseName')
spark.sql('USE {}'.format(db_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preparation

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS low_shuffle_merge_nyctaxi') 
spark.sql('DROP TABLE IF EXISTS merge_nyctaxi') 
spark.sql('DROP TABLE IF EXISTS nyc_merge_data') 

# COMMAND ----------

data_files = [f.path for f in dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/green")]

sc = StructType(
[
  StructField('VendorID', IntegerType()),
  StructField('pickup_datetime',StringType()),
  StructField('dropoff_datetime',StringType()),
  StructField('Store_and_fwd_flag',StringType()),
  StructField('RateCodeID',IntegerType()),
  StructField('Pickup_longitude',DoubleType()),
  StructField('Pickup_latitude',DoubleType()),
  StructField('Dropoff_longitude',DoubleType()),
  StructField('Dropoff_latitude',DoubleType()),
  StructField('Passenger_count',IntegerType()),
  StructField('Trip_distance',DoubleType()),
  StructField('Fare_amount',DoubleType()),
  StructField('Extra',DoubleType()),
  StructField('MTA_tax',DoubleType()),
  StructField('Tip_amount',DoubleType()),
  StructField('Tolls_amount',DoubleType()),
  StructField('Ehail_fee',StringType()),
  StructField('Total_amount',DoubleType()),
  StructField('Payment_type',IntegerType()),
  StructField('Trip_type' ,StringType())
]  )
  

df = (spark.read
 .format("csv")
 .schema(sc)
 .option("header", True)
 .load(data_files)
)
      
display(df)

# COMMAND ----------

(df.withColumn('id', monotonically_increasing_id())
 .write
 .mode("overwrite")
 .format("delta")
 .saveAsTable("low_shuffle_merge_nyctaxi"))

# COMMAND ----------

(df.withColumn('id', monotonically_increasing_id())
 .write
 .mode("overwrite")
 .format("delta")
 .saveAsTable("merge_nyctaxi"))

# COMMAND ----------

spark.sql("OPTIMIZE low_shuffle_merge_nyctaxi")

# COMMAND ----------

spark.sql("OPTIMIZE merge_nyctaxi")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM merge_nyctaxi LIMIT 1000 

# COMMAND ----------

# DBTITLE 1,Create a dataset to merge into the previous tables
merge_df = spark.sql("""
SELECT VendorID
,pickup_datetime
,dropoff_datetime
,Store_and_fwd_flag
,RateCodeID
,Pickup_longitude
,Pickup_latitude
,Dropoff_longitude
,Dropoff_latitude
,-1 as Passenger_count
,Trip_distance
,Fare_amount
,Extra
,MTA_tax
,Tip_amount
,Tolls_amount
,Ehail_fee
,Total_amount
,Payment_type
,Trip_type
,id

FROM merge_nyctaxi

WHERE ID <= 10
""")

merge_df.write.saveAsTable("nyc_merge_data")

display(merge_df)

# COMMAND ----------

# DBTITLE 1,Add data to insert as well
max_id = (spark.sql("SELECT max(id) from merge_nyctaxi")).collect()[0][0]

results = spark.sql("""
  SELECT VendorID
  ,pickup_datetime
  ,dropoff_datetime
  ,Store_and_fwd_flag
  ,RateCodeID
  ,Pickup_longitude
  ,Pickup_latitude
  ,Dropoff_longitude
  ,Dropoff_latitude
  ,-1 as Passenger_count
  ,Trip_distance
  ,Fare_amount
  ,Extra
  ,MTA_tax
  ,Tip_amount
  ,Tolls_amount
  ,Ehail_fee
  ,Total_amount
  ,Payment_type
  ,Trip_type
  ,id + {} as id
  FROM nyc_merge_data""".format(max_id))

results.write.mode('append').saveAsTable('nyc_merge_data')

# COMMAND ----------

spark.sql("OPTIMIZE nyc_merge_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyc_merge_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE DATA 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO merge_nyctaxi as target
# MAGIC USING nyc_merge_data as source
# MAGIC ON target.id = source.id 
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Low Shuffle Merge

# COMMAND ----------

spark.sql("SET spark.databricks.delta.merge.enableLowShuffle = true")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO low_shuffle_merge_nyctaxi as target
# MAGIC USING nyc_merge_data as source
# MAGIC ON target.id = source.id 
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------


