# Databricks notebook source
# MAGIC %md
# MAGIC # Data Set Up
# MAGIC 
# MAGIC The purpose of this entire demo is to demonstrate the EDW Migration process from end to end. This includes: 
# MAGIC 1. Data migration 
# MAGIC 1. Source code migration
# MAGIC 1. Solution migration (PBI Report)
# MAGIC 
# MAGIC In this notebook we will set up data from the demo. For ease of use we will download the data from blob storage and write it to our Azure SQL Database which we will be migrating away from. In a real life scenario there would only be the database and no need for this actual step.  
# MAGIC 
# MAGIC Please ensure you have `com.microsoft.azure:spark-mssql-connector_2.12:1.2.0` installed (Spark 3.1.x).. or the appropriate driver for your spark version.  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT DatabaseName DEFAULT '';
# MAGIC CREATE WIDGET TEXT UserName DEFAULT '';

# COMMAND ----------

# MAGIC %sql
# MAGIC SET var.database_name = $DatabaseName ; 
# MAGIC SET var.user_name = $UserName ; 
# MAGIC SET var.table_location = '/users/${var.user_name}/databases/${var.database_name}' ;
# MAGIC CREATE DATABASE IF NOT EXISTS ${var.database_name} LOCATION ${var.table_location} ;
# MAGIC USE ${var.database_name} ;

# COMMAND ----------

database_name = dbutils.widgets.get("DatabaseName")  
user_name = dbutils.widgets.get("UserName")

# COMMAND ----------

import requests

# COMMAND ----------

def download_file(url):
  local_filename = "/dbfs/tmp/adventureworks/{}".format(current_file.split("/")[-1])
  print("Local File: {}".format(local_filename))

  req = requests.get(url)
  url_content = req.content
  csv_file = open(local_filename, 'wb')

  csv_file.write(url_content)
  csv_file.close()

  return local_filename

# COMMAND ----------

tables = [ 'Address', 'AddressType', 'BillOfMaterials', 'BusinessEntity', 'BusinessEntityAddress', 'BusinessEntityContact', 'ContactType', 'CountryRegion', 'CountryRegionCurrency', 'CreditCard', 'Culture', 'Currency', 'CurrencyRate', 'Customer', 'Department', 'Document', 'EmailAddress', 'Employee', 'EmployeeDepartmentHistory', 'EmployeePayHistory', 'Illustration', 'JobCandidate', 'JobCandidate_TOREMOVE', 'Location', 'Password', 'Person', 'PersonCreditCard', 'PersonPhone', 'PhoneNumberType', 'Product', 'ProductCategory', 'ProductCostHistory', 'ProductDescription', 'ProductDocument', 'ProductInventory', 'ProductListPriceHistory', 'ProductModel', 'ProductModelIllustration', 'ProductModelProductDescriptionCulture', 'ProductModelorg', 'ProductPhoto', 'ProductProductPhoto', 'ProductReview', 'ProductSubcategory', 'ProductVendor', 'PurchaseOrderDetail', 'PurchaseOrderHeader', 'SalesOrderDetail', 'SalesOrderHeader', 'SalesOrderHeaderSalesReason', 'SalesPerson', 'SalesPersonQuotaHistory', 'SalesReason', 'SalesTaxRate', 'SalesTerritory', 'SalesTerritoryHistory', 'ScrapReason', 'Shift', 'ShipMethod', 'ShoppingCartItem', 'SpecialOffer', 'SpecialOfferProduct', 'StateProvince', 'Store', 'TransactionHistory', 'TransactionHistoryArchive', 'UnitMeasure', 'Vendor', 'WorkOrder', 'WorkOrderRouting']

# COMMAND ----------

data_files = [
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Address.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/AddressType.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/BillOfMaterials.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/BusinessEntity.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/BusinessEntityAddress.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/BusinessEntityContact.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ContactType.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/CountryRegion.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/CountryRegionCurrency.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/CreditCard.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Culture.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Currency.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/CurrencyRate.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Customer.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Department.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Document.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/EmailAddress.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Employee.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/EmployeeDepartmentHistory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/EmployeePayHistory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Illustration.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/JobCandidate.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Location.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Password.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Person.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/PersonCreditCard.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/PersonPhone.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/PhoneNumberType.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Product.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductCategory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductCostHistory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductDescription.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductDocument.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductInventory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductListPriceHistory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductModel.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductModelIllustration.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductModelProductDescriptionCulture.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductPhoto.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductProductPhoto.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductReview.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductSubcategory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ProductVendor.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/PurchaseOrderDetail.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/PurchaseOrderHeader.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SalesOrderDetail.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SalesOrderHeader.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SalesOrderHeaderSalesReason.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SalesPerson.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SalesPersonQuotaHistory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SalesReason.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SalesTaxRate.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SalesTerritory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SalesTerritoryHistory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ScrapReason.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Shift.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ShipMethod.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/ShoppingCartItem.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SpecialOffer.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/SpecialOfferProduct.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/StateProvince.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Store.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/TransactionHistory.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/TransactionHistoryArchive.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/UnitMeasure.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/Vendor.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/WorkOrder.csv',
 'https://racadlsgen2.blob.core.windows.net/adventureworks/oltp/WorkOrderRouting.csv']

# COMMAND ----------

dbutils.fs.rm("/tmp/adventureworks", True)

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/adventureworks")

# COMMAND ----------

dbutils.fs.ls("/tmp/adventureworks")

# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "rac_scope", key = "azuresqluser")
jdbcPassword = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlpassword")
jdbcHostname = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlserver")
jdbcPort = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlport")
jdbcDatabase = dbutils.secrets.get(scope = "rac_scope", key = "azuresqldatabase")

# COMMAND ----------

url = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={jdbcUsername}@{jdbcDatabase};password={jdbcPassword};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

### 
### THE FIRST LINE IN ALL THE CSV FILES HAVE ISSUES
### 
for current_file in data_files:

  table_name = current_file.split("/")[-1].replace(".csv", "")
  local_file = download_file(current_file)
  print("Downloading to: '{}'".format(local_file))

  sc = (spark.read.table(table_name).schema)

  df = spark.read.format("csv").option("delimiter", "\t").schema(sc).option("header", "false").option("encoding", "utf-8").load(local_file.replace("/dbfs", ""))

  try:

    (df.write 
      .format("com.microsoft.sqlserver.jdbc.spark") 
      .mode("overwrite") 
      .option("url", url) 
      .option("dbtable", "edw_migration.{}".format(table_name)) 
      .save() )
  except Exception as error :
      print("Connector write failed", error)


