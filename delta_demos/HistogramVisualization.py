# Databricks notebook source
import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = np.array([["Denied","Denied","Accepted","Accepted","Denied","Denied","Accepted","Denied","Accepted", "Accepted"], [495.6, 1806.28, 261.3, 8076.5, 1041.24, 507.88, 208.0, 152.49, 146.0, 2794.14]])
data

# COMMAND ----------

dataset = pd.DataFrame({'Status': data[0, :], 'Charges': data[1, :]})
dataset.head(25)

# COMMAND ----------

spark.createDataFrame(dataset).printSchema()

# COMMAND ----------

display(spark.createDataFrame(dataset).withColumn("Charges", col("Charges").cast(IntegerType())))

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------


