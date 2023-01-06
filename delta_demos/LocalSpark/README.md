# OSS PySpark and Delta Lake in ADLS Gen2

This directory demonstrates how to connect to and read Delta Lake tables stored in Azure Data Lake Storage Gen2. Please reference [Delta Lake Documentation](https://docs.delta.io/latest/delta-storage.html#azure-data-lake-storage-gen2) additional details.  

This shows how users are able to access their data without using Databricks compute.  

## Instructions 

1. Create a Python 3.9 Virtual Environment and install delta lake and pyspark
    ```
    conda create -n pyspark_env python=3.9

    conda activate pyspark_end

    pip install pyspark 
    pip install delta-spark 
    ```

1. Add secrets to config.conf (see example below). 
    ```
    storage_account_name=mystorageaccountname
    container_name=mycontainername
    client_id=myclientid
    client_secret=myclientsecret
    tenant_id=azuretenantid
    ```

1. Execute `run.py`. Please note that you will need to provide full path values for the CSV and Delta table. 
    - Line 48: `data_path = "abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name)` 
    - Line 54: `delta_path = "abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name)` 


