# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access and Management - AWS (UNDER DEVELOPMENT)
# MAGIC 
# MAGIC In this notebook we will walk through one of the most popular (potentially a universal best practice) for accessing data in Azure Data Lake Storage Gen2. This notebook hopes to show the controls that should be put in place for companies that require a sophisticated data governance model. Not all organizations require this level of data management as some companies may have general data requirements that apply to the majority of users, while other orgs may have various levels of access for each individual users. This notebook applies to the more complex data access requirements or wishes to have. 
# MAGIC 
# MAGIC As an example, some customers are completly fine using Databricks mounts even though all users will have the same data access.  
# MAGIC 
# MAGIC We will complete the following items. 
# MAGIC 1. S3 Access  
# MAGIC 1. Create a Database with Location  
# MAGIC 1. Create a Delta table
# MAGIC 1. Manage access to the delta table
# MAGIC   - READ, WRITE, Row Level Security, and Column Level Security 
# MAGIC 1. Show how to enforce secret scope ACLs  
# MAGIC 1. Show how to enforce cluster ACLs  
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Please note that while this notebook is Python we will write as must of the code in SQL (`spark.sql()` commands) in order to demonstrate these patterns to the broadest set of users. 
# MAGIC 
# MAGIC [Secret Access Control](https://docs.databricks.com/security/access-control/secret-acl.html#secret-access-control) and [Cluster Access Control](https://docs.databricks.com/security/access-control/cluster-acl.html) are to very important features required to ensure fine grained control over your data platform.  
# MAGIC 
# MAGIC General overview of cloud storage access:
# MAGIC - AWS  
# MAGIC   - [Instance profiles](https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html#step-5-add-the-instance-profile-to-databricks) attached to a cluster then allow users to attach to specific clusters  
# MAGIC - Azure  
# MAGIC   - Service principals should be used to access ADLS Gen2 directly and users should be granted specific permissions to the secret scope containing the service principles. Typically this can be done at a cluster setting to reduce complexity.  
# MAGIC - GCP 
# MAGIC   - You should use service accounts associated to clusters and control access at the cluster level. 
# MAGIC - Databricks SQL
# MAGIC   - Databricks SQL connects to the Delta Lake using a single authentication mechanism (Instance Profile / Service Principle). This means that principle should have access to all the data with the lowest requried priviledge level. Once this is set then users should be controlled using [Table ACLs](https://docs.databricks.com/security/access-control/table-acls/index.html).  
# MAGIC   - Lowest requried priviledge level means that if all DBSQL users only need READ access then the account should be set to that. But if some users need manage or write access then the principle will need that level of access.  
# MAGIC   
# MAGIC   
# MAGIC 
# MAGIC [Access S3 Directly](https://docs.databricks.com/data/data-sources/aws/amazon-s3.html#access-s3-buckets-directly) using an instance profile

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "")
database_name = dbutils.widgets.get("DatabaseName")
dbutils.widgets.text("Directory", "ryan.chynoweth@databricks.com")
directory = dbutils.widgets.get("Directory")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secret Scope ACLs
# MAGIC 
# MAGIC In the next cell we use a Databricks scope to securely store and use sensitive information or environment variables. To create a Secret Scope and add secrets to the scope users need to use the [Databricks REST API](https://docs.databricks.com/dev-tools/api/latest/secrets.html#secrets-api) or the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html). In this example I will use the Databricks CLI. I will assume that the configuration of the CLI has already occurred using a PAT.     
# MAGIC 
# MAGIC 1. Create a secret scope.  
# MAGIC   ```
# MAGIC   databricks secrets create-scope --scope demo
# MAGIC   ``` 
# MAGIC 
# MAGIC 1. Add a secret to the scope.  
# MAGIC   ```
# MAGIC   databricks secrets put --scope demo --key oneenvstoragename --string-value <STORAGE ACCOUNT NAME>
# MAGIC   ```
# MAGIC   
# MAGIC 1. Grant a user/group the ability to READ and LIST secrets from the scope. Possible permissions are READ, WRITE, MANAGE  
# MAGIC   ```
# MAGIC   databricks secrets put-acl --scope demo --principal <user i.e. ryan.chynoweth@databricks.com OR group> --permission READ
# MAGIC   ```
# MAGIC   
# MAGIC   It is important to note that Databricks admins will have MANAGE permissions for all secret scopes in the workspace. So non-admin users will not have access to the scope you create unless explicitly granted. In the scenario above I would have created a scope and added a secret, then gave someone permission to READ the scope. Since we are using service principals to authenticate against ADLS Gen2, we want to ensure that only specific people have access to the credentials. It would be a best practice to use groups to manage these ACLs.  
# MAGIC   
# MAGIC #### Summary
# MAGIC 
# MAGIC 1. Use secret scopes and manage permissions to the scopes at a group level.  
# MAGIC 1. Use cluster policies and cluster ACLs to restrict which users can connect to which clusters.  
# MAGIC 1. Ideally 1 and 2 would be managed using groups where all users would have same access to secrets and clusters. 
# MAGIC 1. Allow cluster level or session level configuration of authentication to S3

# COMMAND ----------

storage_account_name = dbutils.secrets.get("rac_scope", "oneenvstoragename")
container_name = dbutils.secrets.get("rac_scope", "oneenvcontainer")
client_id = dbutils.secrets.get("rac_scope", "oneenvclientid")
client_secret = dbutils.secrets.get("rac_scope", "oneenvclientsecret")
tenant_id = dbutils.secrets.get("rac_scope", "oneenvtenantid")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster Configuration - MUST FIX THE CONFIGS HERE. THEY ARE FOR ADLS NOT S3
# MAGIC 
# MAGIC I am using cluster configurations for my S3 authentication.  
# MAGIC 
# MAGIC In the Spark configuration of the cluster configuration you can input the following: 
# MAGIC ```
# MAGIC fs.azure.account.auth.type OAuth
# MAGIC fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
# MAGIC fs.azure.account.oauth2.client.id {{secrets/rac_scope/oneenvclientid}}
# MAGIC fs.azure.account.oauth2.client.secret {{secrets/rac_scope/oneenvclientsecret}}
# MAGIC fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token
# MAGIC ```
# MAGIC 
# MAGIC Where `{{secrets/rac_scope/oneenvclientid}}` is our ability to reference secrets stored in scopes.  
# MAGIC 
# MAGIC When we do this then all people who can connect to the cluster have access to these credentials. This is where [cluster ACLs](https://docs.databricks.com/security/access-control/cluster-acl.html) come into play. Here we have the ability to set up different clusters with various. In union with cluster policies you can enforce that these Spark configurations are set in a particular way so that users still have the ability to create their own clusters. 
# MAGIC 
# MAGIC As with Secret ACLs, admins should utilize groups as much as possible when managing access to the various resources.  
# MAGIC 
# MAGIC In my opinion setting these at the cluster level is a better as it is a better experience for the developer to not have to set configurations each time. Additionally, there is some limitations to the session level configs that I am not fully aware of, but one being `CREATE DATABASE <database> LOCATION '/path/database'` where this does not work if you provide a location for the database.  
# MAGIC 
# MAGIC Session Level Authentication is done with the following Python/Scala:  
# MAGIC ```
# MAGIC spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storage_account_name), "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storage_account_name), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storage_account_name), client_id)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storage_account_name), client_secret )
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storage_account_name),  "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))
# MAGIC ```

# COMMAND ----------

# list files
dbutils.fs.ls("abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name))

# COMMAND ----------

# DBTITLE 1,Create Directory for new Database
# FIX THE DATA PATH!
data_path = "abfss://{}@{}.dfs.core.windows.net/{}/databases/{}".format(container_name, storage_account_name, directory, database_name)


reset_dir = False
if reset_dir:
  dbutils.fs.rm(data_path, True)
  
dbutils.fs.mkdirs(data_path)

# COMMAND ----------

# should be empty after we first create it. There may be tables in it if I didn't delete them
dbutils.fs.ls(data_path)

# COMMAND ----------

# DBTITLE 1,Create an UNMANAGED external table by providing it an abfss location 
spark.sql("""CREATE TABLE test_table
  USING DELTA
  LOCATION '{}/test_table'
  AS SELECT * FROM rac_demo_db.application_output
  """.format(data_path)
         )

# COMMAND ----------

dbutils.fs.ls(data_path+"/test_table")

# COMMAND ----------

# DBTITLE 1,Must drop table and delete dir because it is an UNMANAGED table
spark.sql("drop table if exists test_table")
dbutils.fs.rm(data_path+"/test_table", True)

# COMMAND ----------

# DBTITLE 1,Create a database with location - allows us to have MANAGED external tables
spark.sql("""CREATE DATABASE IF NOT EXISTS {}
             LOCATION '{}' """.format(database_name, data_path)
)

## 
## this does not work on standard or hc with notebook configs
## this does work for standard and hc using cluster configs

# COMMAND ----------

spark.sql(""" USE {}""".format(database_name))

# COMMAND ----------

# DBTITLE 1,Same code as above but without the location
spark.sql("""CREATE TABLE test_table
  USING DELTA
  AS SELECT * FROM rac_demo_db.application_output
  """.format(data_path)
         )

# COMMAND ----------

# DBTITLE 1,Stores it in the database location
dbutils.fs.ls(data_path)

# COMMAND ----------

# DBTITLE 1,This helps automatically add it to hive without an explicit "CREATE TABLE" command
# MAGIC %sql
# MAGIC select * from test_table

# COMMAND ----------

display(
spark.sql("""
SELECT * FROM rac_universal_db.test_table
""")
)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS test_table")

# COMMAND ----------

# DBTITLE 1,Our "test_table" should be gone from the dir because it is a MANAGED table
dbutils.fs.ls(data_path)
