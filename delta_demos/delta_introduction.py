# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/databricks icon.png?raw=true" width=100/> 
# MAGIC # Delta Introduction
# MAGIC 
# MAGIC In this demonstration we will deep dive into how delta works and how to work with delta for all your big data pipelines. 
# MAGIC 
# MAGIC #### Databricks' Delta Lake is the world's most advanced data lake technology.  
# MAGIC 
# MAGIC Delta Lake brings __*Performance*__ and __*Reliability*__ to Data Lakes
# MAGIC 
# MAGIC 
# MAGIC ###Why Delta Lake?<br><br>
# MAGIC 
# MAGIC <div style="img align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://user-images.githubusercontent.com/20408077/87175470-4d8e1580-c29e-11ea-8f33-0ee14348a2c1.png" width="500"/>
# MAGIC </div>
# MAGIC 
# MAGIC At a glance, Delta Lake is an open source storage layer that brings both **reliability and performance** to data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. 
# MAGIC 
# MAGIC Delta Lake runs on top of your existing data lake and is fully compatible with Apache Spark APIs. [For more information](https://docs.databricks.com/delta/delta-intro.html)

# COMMAND ----------

# MAGIC %md
# MAGIC Why did Delta Lake have to be invented?  Let's take a look...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/projects_failing.png?raw=true" width=1000/>
# MAGIC 
# MAGIC As the graphic above shows, Big Data Lake projects have a very high failure rate.  In fact, Gartner Group estimates that 85% of these projects fail (see https://www.infoworld.com/article/3393467/4-reasons-big-data-projects-failand-4-ways-to-succeed.html ).  *Why* is the failure rate so high?
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/projects_failing_reasons.png?raw=true" width=1000/>
# MAGIC 
# MAGIC The graphic above shows the main __*reliability*__ issues with data lakes.  Unlike relational databases, typical data lakes are not capable of transactional (ACID) behavior.  This leads to a number of reliability issues:
# MAGIC 
# MAGIC - When a job fails, incomplete work is not rolled back, as it would be in a relational database.  Data may be left in an inconsistent state.  This issue is extremely difficult to deal with in production.
# MAGIC 
# MAGIC - Data lakes typically cannot enforce schema.  This is often touted as a "feature" called "schema-on-read," because it allows flexibility at data ingest time.  However, when downstream jobs fail trying to read corrupt data, we have a very difficult recovery problem.  It is often difficult just to find the source application that caused the problem... which makes fixing the problem even harder!
# MAGIC 
# MAGIC - Relational databases allow multiple concurrent users, and ensure that each user gets a consistent view of data.  Half-completed transactions never show up in the result sets of other concurrent users.  This is not true in a typical data lake.  Therefore, it is almost impossible to have a concurrent mix of read jobs and write jobs.  This becomes an even bigger problem with streaming data, because streams typically don't pause to let other jobs run!
# MAGIC 
# MAGIC Next, let's look at the key __*performance issues*__ with data lakes...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/projects_failing_reasons_1.png?raw=true" width=1000/>
# MAGIC 
# MAGIC - We have already noted that data lakes cannot provide a consistent view of data to concurrent users.  This is a reliability problem, but it is also a __*performance*__ problem because if we must run jobs one at a time, our production time window becomes extremely limited.
# MAGIC 
# MAGIC - Most data lake engineers have come face-to-face with the "small-file problem."  Data is typically ingested into a data lake in batches.  Each batch typically becomes a separate physical file in a directory that defines a table in the lake.  Over time, the number of physical files can grow to be very large.  When this happens, performance suffers because opening and closing these files is a time-consuming operation.  
# MAGIC 
# MAGIC - Experienced relational database architects may be surprised to learn that Big Data usually cannot be indexed in the same way as relational databases.  The indexes become too large to be manageable and performant.  Instead, we "partition" data by putting it into sub-directories.  Each partition can represent a column (or a composite set of columns) in the table.  This lets us avoid scanning the entire data set... *if* our queries are based on the partition column.  However, in the real world, analysts are running a wide range of queries which may or may not be based on the partition column.  In these scenarios, there is no benefit to partitioning.  In addition, partitioning breaks down if we choose a partition column with extremely high cardinality.
# MAGIC 
# MAGIC - Data lakes typically live in cloud storage (e.g., S3 on AWS, ADLS on Azure), and these storage devices are quite slow compared to SSD disk drives.  Most data lakes have no capability to cache data on faster devices, and this fact has a major impact on performance.
# MAGIC 
# MAGIC __*Delta Lake was built to solve these reliability and performance problems.*__  First, let's consider how Delta Lake addresses *reliability* issues...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_reliability.png?raw=true" width=1000/>
# MAGIC 
# MAGIC Note the Key Features in the graphic above:
# MAGIC 
# MAGIC - __ACID Transactions:__ Delta Lake ACID compliance ensures that half-completed transactions are never persisted in the Lake, and concurrent users never see other users' in-flight transactions.
# MAGIC 
# MAGIC - __Mutations:__ Experienced relational database architects may be surprised to learn that most data lakes do not support updates and deletes.  These lakes concern themselves only with data ingest, which makes error correction and backfill very difficult.  In contrast, Delta Lake provides full support for Inserts, Updates, and Deletes.
# MAGIC 
# MAGIC - __Schema Enforcement:__ Delta Lake provides full support for schema enforcement at write time, greatly increasing data reliability.
# MAGIC 
# MAGIC - __Unified Batch and Streaming:__ Streaming data is becoming an essential capability for all enterprises.  We'll see how Delta Lake supports both batch and streaming modes, and in fact blurs the line between them, enabling architects to design systems that use both batch and streaming capabilities simultaneously.
# MAGIC 
# MAGIC - __Time Travel:__ unlike most data lakes, Delta Lake enables queries of data *as it existed* at a specific point in time.  This has important ramifications for reliability, error recovery, and synchronization with other systems, as we shall see later in this Workshop.
# MAGIC 
# MAGIC We have seen how Delta Lake enhances reliability.  Next, let's see how Delta Lake optimizes __*performance*__...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_performance.png?raw=true" width=1000/>
# MAGIC 
# MAGIC Again, we'll be diving into all these capabilities throughout the Workshop.  We'll be concentrating especially on features that are only available in Databricks' distribution of Delta Lake...
# MAGIC 
# MAGIC - __Compaction:__ Delta Lake provides sophisticated capabilities to solve the "small-file problem" by compacting small files into larger units.
# MAGIC 
# MAGIC - __Caching:__ Delta Lake transparently caches data on the SSD drives of worker nodes in a Spark cluster, greatly improving performance.
# MAGIC 
# MAGIC - __Data Skipping:__ this Delta Lake feature goes far beyond the limits of mere partitioning.
# MAGIC 
# MAGIC - __Z-Ordering:__ this is a brilliant alternative to traditional indexing, and further enhances Delta Lake performance.
# MAGIC 
# MAGIC Now that we have introduced the value proposition of Delta Lake, let's get a deeper understanding of the overall "Data Lake" concept.

# COMMAND ----------

database_name = "rac_demo_db"
raw_path = "/Users/ryan.chynoweth@databricks.com/bronze/assignment_1.csv"
parquet_path = "/Users/ryan.chynoweth@databricks.com/bronze/parquet_iot_table"
raw_backfill_path = "/Users/ryan.chynoweth@databricks.com/bronze/assignment_1_backfill.csv"
silver_path = "/Users/ryan.chynoweth@databricks.com/silver/iot_data"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bringing data into a dataframe
# MAGIC 
# MAGIC The use case we will be demonstrating here illustrates the "Bronze-Silver-Gold" paradigm which is a best practice for data lakes.
# MAGIC 
# MAGIC - We ingest data as soon as we can into the lake, even though we know it may need cleansing or enrichment.  This gives us a baseline of the freshest possible data for exploration.  We call this the __Bronze__ version of the data.
# MAGIC 
# MAGIC - We then cleanse and enrich the Bronze data, creating a "single version of truth" that we call the __Silver__ version.
# MAGIC 
# MAGIC - From the Silver data, we can generate many __Gold__ versions of the data.  Gold versions are typically project-specific, and typically filter, aggregate, and re-format Silver data to make it easy to use in specific projects.
# MAGIC 
# MAGIC We'll read the raw data into a __Dataframe__.  The dataframe is a key structure in Apache Spark.  It is an in-memory data structure in a rows-and-columns format that is very similar to a relational database table.  In fact, we'll be creating SQL Views against the dataframes so that we can manipulate them using standard SQL.

# COMMAND ----------

# df.write.format("parquet").save(parquet_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CONVERT TO DELTA [ table_identifier | parquet.`<path-to-table>` ] [NO STATISTICS]
# MAGIC -- [PARTITIONED BY (col_name1 col_type1, col_name2 col_type2, ...)]

# COMMAND ----------

# load and display data
df = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("sep", ",") \
  .load(raw_path)

display(df)

# COMMAND ----------

# Read the backfill data into a dataframe so that we can "stream" into delta
df_backfill = (spark
               .read
               .format("csv")
               .option("inferSchema", "true")
               .option("header", "true")
               .load(raw_backfill_path))

display(df_backfill)

# COMMAND ----------

# Create a temporary view on the dataframes to enable SQL
df.createOrReplaceTempView("historical_bronze_vw")
df_backfill.createOrReplaceTempView("historical_bronze_backfill_vw")

# COMMAND ----------

# MAGIC %md
# MAGIC First let's create a managed delta table. 
# MAGIC 
# MAGIC To do so we will write the data from the CSV to the delta format. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Delta Lake table for the main bronze table
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_bronze;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_bronze
# MAGIC Using Delta
# MAGIC -- LOCATION '/Users/ryan.chynoweth@databricks.com/silver/iot_data'
# MAGIC as Select * from historical_bronze_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's count the records in the Bronze table
# MAGIC 
# MAGIC SELECT COUNT(*) FROM sensor_readings_historical_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) as status_count, device_operational_status
# MAGIC FROM sensor_readings_historical_bronze
# MAGIC GROUP BY device_operational_status
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver table
# MAGIC 
# MAGIC So we have our original dataset and a backfill dataset. To demonstrate a key feature of Delta we will merge our backfill data to create a silver delta table. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a Silver table.  We'll start with the Bronze data, then make several improvements
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_silver 
# MAGIC using Delta
# MAGIC as select * from historical_bronze_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_silver where id = 'ZZZdbfac1b5-f6af-4d0d-a98a-c1be9be29678'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's merge in the Bronze backfill data
# MAGIC -- MERGE INTO is one of the most important differentiators for Delta Lake
# MAGIC -- The entire backfill batch will be treated as an atomic transaction,
# MAGIC -- and we can do both inserts and updates within a single batch.
# MAGIC 
# MAGIC MERGE INTO sensor_readings_historical_silver AS target 
# MAGIC USING historical_bronze_backfill_vw AS source  
# MAGIC   ON target.id = source.id
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# MAGIC %md 
# MAGIC The inserted rows have "ZZZ" and updated rows do not. All rows we inserted have the date 2015-02-21

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify that the upserts worked correctly.
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_silver
# MAGIC where id = 'ZZZdbfac1b5-f6af-4d0d-a98a-c1be9be29678'

# COMMAND ----------

# MAGIC %md 
# MAGIC Lets look into updating some bad data by using some window functions. We want to replace the values of 999.99 with local averages. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We'll create a table of these interpolated readings, then later we'll merge it into the Silver table.
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_interpolations;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_interpolations AS (
# MAGIC   WITH lags_and_leads AS (
# MAGIC     SELECT
# MAGIC       id, 
# MAGIC       reading_time,
# MAGIC       device_type,
# MAGIC       device_id,
# MAGIC       device_operational_status,
# MAGIC       reading_1,
# MAGIC       LAG(reading_1, 1, 0)  OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_1_lag,
# MAGIC       LEAD(reading_1, 1, 0) OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_1_lead,
# MAGIC       reading_2,
# MAGIC       LAG(reading_2, 1, 0)  OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_2_lag,
# MAGIC       LEAD(reading_2, 1, 0) OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_2_lead,
# MAGIC       reading_3,
# MAGIC       LAG(reading_3, 1, 0)  OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_3_lag,
# MAGIC       LEAD(reading_3, 1, 0) OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_3_lead
# MAGIC     FROM sensor_readings_historical_silver
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     device_operational_status,
# MAGIC     ((reading_1_lag + reading_1_lead) / 2) AS reading_1,
# MAGIC     ((reading_2_lag + reading_2_lead) / 2) AS reading_2,
# MAGIC     ((reading_3_lag + reading_3_lead) / 2) AS reading_3
# MAGIC   FROM lags_and_leads
# MAGIC   WHERE reading_1 = 999.99
# MAGIC   ORDER BY id ASC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lets take a look at the data we fixed 
# MAGIC SELECT * FROM sensor_readings_historical_interpolations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's see how many interpolations we have.  There should be 367 rows.
# MAGIC 
# MAGIC SELECT COUNT(*) FROM sensor_readings_historical_interpolations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lets merge the tables together so that we can fix those 999.99 values
# MAGIC MERGE INTO sensor_readings_historical_silver AS target 
# MAGIC USING sensor_readings_historical_interpolations AS source 
# MAGIC ON target.id = source.id
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now make sure we got rid of all the bogus readings.
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %md
# MAGIC Now we've lost visibility into which readings were initially faulty... use Time Travel to recover this information.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all the versions of the table that are available to us
# MAGIC 
# MAGIC DESCRIBE HISTORY sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ah, version 1 should have the 999.99 values
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver 
# MAGIC VERSION AS OF 1 
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %md
# MAGIC What does the delta table look like under the hood?

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{database_name}.db/sensor_readings_historical_silver")

# As you can see, the data is just broken into a set of files, without regard to the meaning of the data

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize sensor_readings_historical_silver 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a Silver table partitioned by Device. 
# MAGIC -- Create a new table, so we can compare new and old
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver_by_device;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_silver_by_device 
# MAGIC using Delta
# MAGIC partitioned by (device_id) 
# MAGIC as select * from sensor_readings_historical_silver 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can see partition information
# MAGIC 
# MAGIC DESCRIBE EXTENDED sensor_readings_historical_silver_by_device

# COMMAND ----------

# Now we have subdirectories for each device, with physical files inside them
# Will that speed up queries?

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{database_name}.db/sensor_readings_historical_silver_by_device")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- insert new data with an extra column into the gold table
# MAGIC insert into sensor_readings_historical_silver
# MAGIC select *, 1 as new_col
# MAGIC from sensor_readings_historical_silver
# MAGIC limit 10

# COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")  # set our spark configuration for schema merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- insert new data with an extra column into the gold table
# MAGIC insert into sensor_readings_historical_silver
# MAGIC select *, 1 as new_col
# MAGIC from sensor_readings_historical_silver
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sensor_readings_historical_silver -- look at the new column

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sensor_readings_historical_silver where new_col is not null -- look at the new column with values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets get rid of the new column and do some streaming
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_silver
# MAGIC using Delta
# MAGIC as select * from sensor_readings_historical_silver_by_device

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Delta Streaming 
# MAGIC 
# MAGIC Now let's look into using delta as a streaming source and a streaming sink. 

# COMMAND ----------

silver_stream_df = spark.readStream.format("delta").table("{}.sensor_readings_historical_silver".format(database_name))

# COMMAND ----------

from pyspark.sql.functions import col 

def transform_stream(microBatchDF, batchId):
    # TRANSFORMER
    (microBatchDF.filter(col("device_type") == "TRANSFORMER")
     .write
     .format("delta")
     .mode("append")
     .saveAsTable("transformer_readings_agg") )
    
    # RECTIFIER
    (microBatchDF.filter(col("device_type") == "RECTIFIER")
     .write
     .format("delta")
     .mode("append")
     .saveAsTable("rectifier_readings_agg") )

# COMMAND ----------

checkpoint_loc = "/Users/ryan.chynoweth@databricks.com/checkpoints_demo"
dbutils.fs.rm(checkpoint_loc, True)

# COMMAND ----------

(silver_stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_loc)
#     .trigger(once=True)
    .foreachBatch(transform_stream)
    .outputMode("update")
    .start())

# COMMAND ----------

import time

next_row = 0

while next_row < 12000:
  
  time.sleep(1)

  next_row += 1
  
  spark.sql(f"""
    INSERT INTO sensor_readings_historical_silver (
      SELECT * FROM historical_bronze_vw
      LIMIT 1000 ) """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rectifier_readings_agg

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from transformer_readings_agg

# COMMAND ----------

# stop all of our streams
for s in spark.streams.active:
  s.stop()

# COMMAND ----------


