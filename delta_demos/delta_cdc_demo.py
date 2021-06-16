# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Delta CDC
# MAGIC 
# MAGIC The way that Delta is architected using the transaction log, we needed to add additional logging in order to provide CDC datasets between two versions of a delta table. When CDC logging is enabled for a delta table we will track which rows were inserted, updated, and deleted.
# MAGIC 
# MAGIC [Internal Documentation](https://docs.google.com/document/d/1QjWorw8-udLMFC5QOXjkfg2_d5AYe2wQt5_nJ6Wucho/edit#) 

# COMMAND ----------


