# Data Warehouse to Databricks Migration - Adventure Works

This demo goes through the basic process of migrating a data warehouse to Databricks' Lakehouse.  

The basic steps are as follows:  
1. Data Migration - requires creating temporary pipelines that regularly migrate data from the data warehouse to the Lakehouse.   
1. Code Migration - stored procedures, views, etl, etc.   
1. Solution Migration - reporting, ML applications, etc.  
1. Ingestion Pipelines Migration - these are the operations from true source systems to the data warehouse, these will need to start writing to the Lakehouse.  
1. Decommission the data warehouse. 



