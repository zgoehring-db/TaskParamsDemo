# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Data from a RDBMS and ETL 
# MAGIC 
# MAGIC Databricks has built in support for [JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) connections to all databases that support it. In addition, there are often high performance connectors for specific Databases like [Azure Synapse DW]() or [Azure SQL Database](). 
# MAGIC 
# MAGIC Because of this connectivity, Databricks can be used to [connect directly to source systems](https://databricks.com/blog/2019/03/08/securely-accessing-external-data-sources-from-databricks-for-aws.html) and ingest the data for analytics. Whether these systems are on-premises or in the cloud, corporate policies usually have firewalls enabled to prevent unwanted access. This will require a VNET or VPC injected deployment of a Databricks workspace so that the networking is appropriate. In this scenario I have a demo database that does not have this restriction, but please note the code is the same regardless.  
# MAGIC 
# MAGIC Below is a sample architecture where users leverage Databricks to ingest data from source systems.   
# MAGIC <br></br>
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/AWS Architecture General.png" width=900/>
# MAGIC <br></br>
# MAGIC Please note that as an alternative some customers use third party tools that push data to the cloud instead of pulling data from systems. In these scenarios we often leverage the [Databricks AutoLoader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) or the [COPY INTO](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-copy-into.html) functionality of Spark SQL.  

# COMMAND ----------

# MAGIC %md 
# MAGIC To Dos:
# MAGIC 1. Write a notebook to download and write the data to a database (Azure SQL DB?)
# MAGIC 1. Secrets stored in a scope. Maybe automate this?
# MAGIC 1. Connect to the data and perform the required ETL
# MAGIC 1. Add a table diagram of the ETL i.e. DAG

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


