# Databricks notebook source
# MAGIC %md
# MAGIC # Data Management 
# MAGIC 
# MAGIC In this notebook we will walk through one of the most popular (potentially a universal best practice) for accessing data within the data lake. 
# MAGIC 
# MAGIC We will complete the following items. 
# MAGIC 1. ADLS Access  
# MAGIC 1. Create a Database with Location  
# MAGIC 1. Create a Delta table
# MAGIC 1. Manage access to the delta table
# MAGIC   - READ, WRITE, Row Level Security, and Column Level Security 
# MAGIC 
# MAGIC [Internal Document](https://docs.google.com/document/d/1v4TQMIeeLAzSr4TRqZESydOiD6yk3jRvVMknmBPxhS0)
# MAGIC 
# MAGIC Please note that while this notebook is Python we will write as must of the code in SQL in order to demonstrate these patterns to the broadest set of users. 
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC RG: oneeenv, Resource: oneeenvadls
# MAGIC Display name:oneenv-adls
# MAGIC Application (client) ID:ed573937-9c53-4ed6-b016-929e765443eb
# MAGIC Directory (tenant) ID:9f37a392-f0ae-4280-9796-f1864a10effc
# MAGIC Object ID:57d2dfed-fa92-4e85-8aa3-fba97deb68e1
# MAGIC secret: q7JwBZ:uF-f6.ls-PJ5AhwBy97[rtY57
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC General overview of cloud storage access:
# MAGIC - AWS  
# MAGIC   - [Instance profiles](https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html#step-5-add-the-instance-profile-to-databricks) attached to a cluster then allow users to attach to specific clusters  
# MAGIC - Azure  
# MAGIC   - Service principals should be used to access ADLS Gen2 directly and users should be granted specific permissions to the secret scope containing the service principles. Typically this can be done at a cluster setting to reduce complexity.  
# MAGIC     ```
# MAGIC     spark.conf.set("fs.azure.account.auth.type.<storage-account-name>.dfs.core.windows.net", "OAuth")
# MAGIC     spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account-name>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC     spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account-name>.dfs.core.windows.net", "<application-id>")
# MAGIC     spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account-name>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"))
# MAGIC     spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account-name>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")
# MAGIC     ```
# MAGIC - GCP 
# MAGIC   - You should use service accounts associated to clusters and control access at the cluster level. 
# MAGIC - Databricks SQL
# MAGIC   - Databricks SQL connects to the Delta Lake using a single authentication mechanism (Instance Profile / Service Principle). This means that principle should have access to all the data with the lowest requried priviledge level. Once this is set then users should be controlled using [Table ACLs](https://docs.databricks.com/security/access-control/table-acls/index.html).  
# MAGIC   - Lowest requried priviledge level means that if all DBSQL users only need READ access then the account should be set to that. But if some users need manage or write access then the principle will need that level of access.  

# COMMAND ----------

storage_account_name = "oneenv"
container_name = "deltalake"
client_id = "ed573937-9c53-4ed6-b016-929e765443eb"
client_secret = "q7JwBZ:uF-f6.ls-PJ5AhwBy97[rtY57"
tenant_id = "9f37a392-f0ae-4280-9796-f1864a10effc"

# COMMAND ----------

# Set configs
spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storage_account_name), "OAuth")

spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storage_account_name), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storage_account_name), client_id)

spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storage_account_name), client_secret )

spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storage_account_name),  "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))

# COMMAND ----------

# Create Directory 
dbutils.widgets.text("Directory", "ryan.chynoweth@databricks.com")
directory = dbutils.widgets.get("Directory")
data_path = "abfss://{}@{}.dfs.core.windows.net/{}".format(container_name, storage_account_name, directory)


reset_dir = False 
if reset_dir:
  dbutils.fs.rm(data_path, True)
  
dbutils.fs.mkdirs(data_path)

# COMMAND ----------

dbutils.fs.ls(data_path)

# COMMAND ----------


