# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # EDW Ingestion - Parent Notebook
# MAGIC 
# MAGIC This a parent notebook using notebook workflows. Note that notebook workflows are not recommended for long running tasks.   

# COMMAND ----------

notebook_path = "00_EDW_Ingest"
database_prefix = "rac"
user_name = "ryan.chynoweth@databricks.com"

# COMMAND ----------

table_list = [r.table_name for r in spark.sql("SELECT DISTINCT table_name FROM {}_adventureworks_metadata.table_metadata WHERE table_name not in ('AWBuildVersion', 'JobCandidate')".format(database_prefix)).collect()]
table_list

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

class NotebookData:
  def __init__(self, path, timeout, parameters=None, retry=0):
    self.path = path
    self.timeout = timeout
    self.parameters = parameters
    self.retry = retry

  def submitNotebook(notebook):
    print("Running notebook %s" % notebook.path)
    try:
      if (notebook.parameters):
        return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
      else:
        return dbutils.notebook.run(notebook.path, notebook.timeout)
    except Exception:
       if notebook.retry < 1:
        raise
    print("Retrying notebook %s" % notebook.path)
    notebook.retry = notebook.retry - 1
    submitNotebook(notebook)


# COMMAND ----------

def parallelNotebooks(notebooks, numInParallel):
   # If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once. 
   # This code limits the number of parallel notebooks.
   with ThreadPoolExecutor(max_workers=numInParallel) as ec:
    return [ec.submit(NotebookData.submitNotebook, notebook) for notebook in notebooks]


# COMMAND ----------

notebooks = [NotebookData(path=notebook_path, timeout=0, parameters={'DatabaseNamePrefix':database_prefix, 'TableName': t, 'UserName': user_name} ) for t in table_list]

res = parallelNotebooks(notebooks, 5)
result = [i.result(timeout=3600) for i in res] # This is a blocking call.
print(result)      



# COMMAND ----------


