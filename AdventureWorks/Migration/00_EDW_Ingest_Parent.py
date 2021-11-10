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

table_list = [ 'Address', 'AddressType', 'BillOfMaterials', 'BusinessEntity', 'BusinessEntityAddress', 'BusinessEntityContact', 'ContactType', 'CountryRegion', 'CountryRegionCurrency', 'CreditCard', 'Culture', 'Currency', 'CurrencyRate', 'Customer', 'Department', 'Document', 'EmailAddress', 'Employee', 'EmployeeDepartmentHistory', 'EmployeePayHistory', 'Illustration', 'JobCandidate', 'JobCandidate_TOREMOVE', 'Location', 'Password', 'Person', 'PersonCreditCard', 'PersonPhone', 'PhoneNumberType', 'Product', 'ProductCategory', 'ProductCostHistory', 'ProductDescription', 'ProductDocument', 'ProductInventory', 'ProductListPriceHistory', 'ProductModel', 'ProductModelIllustration', 'ProductModelProductDescriptionCulture', 'ProductModelorg', 'ProductPhoto', 'ProductProductPhoto', 'ProductReview', 'ProductSubcategory', 'ProductVendor', 'PurchaseOrderDetail', 'PurchaseOrderHeader', 'SalesOrderDetail', 'SalesOrderHeader', 'SalesOrderHeaderSalesReason', 'SalesPerson', 'SalesPersonQuotaHistory', 'SalesReason', 'SalesTaxRate', 'SalesTerritory', 'SalesTerritoryHistory', 'ScrapReason', 'Shift', 'ShipMethod', 'ShoppingCartItem', 'SpecialOffer', 'SpecialOfferProduct', 'StateProvince', 'Store', 'TransactionHistory', 'TransactionHistoryArchive', 'UnitMeasure', 'Vendor', 'WorkOrder', 'WorkOrderRouting']


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


