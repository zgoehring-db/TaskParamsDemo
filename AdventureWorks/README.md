# Data Warehouse to Databricks Migration - Adventure Works

This demo goes through the basic process of migrating a data warehouse to Databricks' Lakehouse.  

### Data Warehouse versus Databricks' Lakehouse

<img src="https://racadlsgen2.blob.core.windows.net/public/EDW Migration Visuals - Logical Benefits.png" />


The basic steps are as follows:  
1. Data Migration - requires creating temporary pipelines that regularly migrate data from the data warehouse to the Lakehouse.   
1. Code Migration - stored procedures, views, etl, etc.   
1. Solution Migration - reporting, ML applications, etc.  
1. Ingestion Pipelines Migration - these are the operations from true source systems to the data warehouse, these will need to start writing to the Lakehouse.  
1. Decommission the data warehouse. 

### Legacy Solution  

<img src="https://racadlsgen2.blob.core.windows.net/public/EDW Migration Visuals - Legacy solution.png" />

### Migration Process 

<img src="https://racadlsgen2.blob.core.windows.net/public/EDW Migration Visuals - Migration Process.png" />

Executing this demo: 
1. Execute all three notebooks in the `setup` directory in the appropriate numbered order. Please note that you may need to add secret values to save the data to an Azure SQL Database.  
1. Run `migration` directory in the appropriate order
1. Migrate ingestion pipelines from source  

