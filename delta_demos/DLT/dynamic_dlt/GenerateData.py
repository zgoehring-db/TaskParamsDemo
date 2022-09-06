# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Data  
# MAGIC 
# MAGIC In this demo we will generate fake data using Faker to simulate point of sale data. Please note the tables with brief explainations.  
# MAGIC - customer 
# MAGIC - address 
# MAGIC - product 
# MAGIC - order 

# COMMAND ----------

num_customers = 100
user_name = spark.sql("SELECT current_user()").collect()[0][0]

raw_files = "/dbfs/Users/{}/dynamic_dlt/raw".format(user_name)
dbutils.fs.rm(raw_files.replace('/dbfs', ''), True)
dbutils.fs.mkdirs(raw_files.replace('/dbfs', ''))

# COMMAND ----------

import subprocess 

def pip_install(name):
    subprocess.call(['pip', 'install', name])
    

pip_install('Faker')

# COMMAND ----------

import os
import uuid
import json
import time 
import random
import datetime 
import sys 
from faker import Faker

fake = Faker()

# COMMAND ----------

class FakeDataGenerator():
  
  def __init__(self):
    self.customer_ids = []
    
    
  def create_customer(self):
    """
    Creates a customer 
    """
    cid = str(uuid.uuid1())
    is_mem = random.randint(0,1)
    
    data = {
      'customer_id': cid,
      'first_name': fake.first_name(),
      'last_name': fake.last_name(), 
      'is_member': is_mem,
      'member_number': fake.msisdn()[3:] if is_mem == 1 else None 
      'created_date': str(datetime.datetime.utcnow()), 
      'created_by': 'system'
    }
    
    self.customer_ids.append(cid)
    return data
  
  
  def create_customer_address(self, cid):
    """
    When given a customer id, this will generate address information for the customer 
    """
    aid = str(uuid.uuid1())
    
    data = {
      'address_id': aid
      'customer_id': cid,
      'address': fake.street_address(),
      'city': fake.city(),
      'state': fake.state(),
      'zip_code': fake.postcode(), 
      'created_date': str(datetime.datetime.utcnow()), ## maybe back date this some to make it more 'real'
      'created_by': 'system'
    }
    return data
  
  
  def create_product(self)
  
  
  
    

# COMMAND ----------

'6048764759382'[3:]

8764759382

# COMMAND ----------

gen = FakeDataGenerator()

# COMMAND ----------

# DBTITLE 1,Generate Data 
for i in range(0, num_customers):
  d = gen.create_customer()

  with open("{}/customers/{}.json".format(raw_files, str(uuid.uuid1())), "w") as f:
      json.dump(d, f)
  

# COMMAND ----------


