# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Integration with Composer and/or Airflow
# MAGIC 
# MAGIC ## Cloud Composer
# MAGIC 
# MAGIC Cloud composer is simply a managed Apache Airflow offering in GCP. There are some custom integrations that make it easier to use in the GCP environment but there are not necessarily huge differences between it and the open source version but I thought there is good value add for using this service.  
# MAGIC 
# MAGIC As for integration with Databricks, users should reference the open source airflow documentation. Engineers will have two options:  
# MAGIC - [Apache Spark Operators](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html)  (available after 1.0.0)
# MAGIC - [Databricks Operators](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators.html) (available after 1.9.0)
# MAGIC 
# MAGIC Because Databricks support is for versions `1.9.0+`, This means that native Databricks integration with cloud composer is currently in preview, as the current releases are lagging according to the Apache Airflow release schedule.  
# MAGIC <br></br>
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/ComposerImageVersion.png" />  
# MAGIC <br></br>
# MAGIC In the end users have the following options to run jobs in Databricks.   
# MAGIC - Databricks Integration
# MAGIC     - [Submit Run](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/_api/airflow/providers/databricks/operators/databricks/index.html#airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator)  
# MAGIC         - Submits a Spark job to Databricks using the [api/2.0/jobs/runs/submit](https://docs.databricks.com/api/latest/jobs.html#runs-submit) API endpoint.
# MAGIC         - Which submits a single job to be executed one time. This will not create a job and will not display in the UI. 
# MAGIC     - [Run Now](https://docs.databricks.com/api/latest/jobs.html#runs-submit)  
# MAGIC         - Runs an existing Spark job run to Databricks using the [api/2.0/jobs/run-now](https://docs.databricks.com/api/latest/jobs.html#run-now) API endpoint.
# MAGIC - Spark Integration
# MAGIC     - [Spark JDBC](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_jdbc/index.html#module-airflow.providers.apache.spark.operators.spark_jdbc)  
# MAGIC         - Submits code via a JDBC connection to an existing Spark cluster (Interactive clusters in Databricks)
# MAGIC     - [Spark SQL](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_sql/index.html)  
# MAGIC         - Executes a SQL Query on an existing Spark cluster (Interactive clusters in Databricks)
# MAGIC     - [Spark Submit](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_submit/index.html) 
# MAGIC         - Executes a spark binary to an existing Spark cluster (Interactive clusters in Databricks)
# MAGIC 
# MAGIC 
# MAGIC Users should almost always rely on the Databricks integration as it allows users to take advantage of job compute. 
# MAGIC 
# MAGIC Honestly, multi-task jobs are a better option than airflow at this time. Unfortunately, extremely complex DAGs are not supported in multi-task jobs at this time. Airflow accomplishes a lot of conditional logic and dependency mapping using things like [Trigger Rules](https://airflow.incubator.apache.org/concepts.html#trigger-rules).
# MAGIC 
# MAGIC There is an early project within Databricks called [Airbreeze](https://databricks.atlassian.net/wiki/spaces/UN/pages/2232388819/Managed+Airflow+Jobs) that Chris presented on that would allow for users to author Airflow DAGs and host them within our multi-task jobs interface.  
# MAGIC <br></br>
# MAGIC 
# MAGIC ### Resources 
# MAGIC 
# MAGIC - Documentation - [Managing Dependecies in Data Pipelines](https://docs.databricks.com/dev-tools/data-pipelines.html)
# MAGIC - [DIAS 2021 Talk](https://databricks.com/session_na21/scaling-your-data-pipelines-with-apache-spark-on-kubernetes) - uses data proc and not databricks. This is a Spark integration not a Databricks integration.  
# MAGIC <br></br>
# MAGIC 
# MAGIC 
# MAGIC ## Airflow 
# MAGIC 
# MAGIC Commands to create a local airflow instance inside a conda environment:   
# MAGIC ```
# MAGIC conda create -n databricks_gcp_composer python==3.8 -y
# MAGIC 
# MAGIC conda activate databricks_gcp_composer
# MAGIC 
# MAGIC pip install apache-airflow==2.1.1 
# MAGIC 
# MAGIC pip install apache-airflow-providers-databricks
# MAGIC 
# MAGIC airflow db init
# MAGIC 
# MAGIC airflow users create --username admin --firstname ryan --lastname Chynoweth --role Admin --email ryan.chynoweth@databricks.com
# MAGIC 
# MAGIC airflow webserver
# MAGIC ```
# MAGIC 
# MAGIC These commands:
# MAGIC 
# MAGIC 1. Create a directory named airflow and change into that directory.  
# MAGIC 1. Use conda to create and spawn a Python virtual environment. Databricks recommends using a Python virtual environment to isolate package versions and code dependencies to that environment. This isolation helps reduce unexpected package version mismatches and code dependency collisions.  
# MAGIC 1. Install Airflow and the Airflow Databricks provider packages.  
# MAGIC 1. Initialize a SQLite database that Airflow uses to track metadata. In a production Airflow deployment, you would configure Airflow with a standard database. The SQLite database and default configuration for your Airflow deployment are initialized in the airflow directory.  
# MAGIC 1. Create an admin user for Airflow.  
# MAGIC 
# MAGIC 
# MAGIC NOTE - when using cloud composer you will only need to add the additional pip library `apache-airflow-providers-databricks`.  

# COMMAND ----------


