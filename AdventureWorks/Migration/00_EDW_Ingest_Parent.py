# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # EDW Ingestion - Parent Notebook
# MAGIC 
# MAGIC This a parent notebook using notebook workflows. Note that notebook workflows are not recommended for long running tasks.   

# COMMAND ----------

notebook_path = "AdventureWorks/Migration/00_EDW_Ingest"

# COMMAND ----------

table_list = [ 'Address', 'AddressType', 'BillOfMaterials', 'BusinessEntity', 'BusinessEntityAddress', 'BusinessEntityContact', 'ContactType', 'CountryRegion', 'CountryRegionCurrency', 'CreditCard', 'Culture', 'Currency', 'CurrencyRate', 'Customer', 'Department', 'Document', 'EmailAddress', 'Employee', 'EmployeeDepartmentHistory', 'EmployeePayHistory', 'Illustration', 'JobCandidate', 'JobCandidate_TOREMOVE', 'Location', 'Password', 'Person', 'PersonCreditCard', 'PersonPhone', 'PhoneNumberType', 'Product', 'ProductCategory', 'ProductCostHistory', 'ProductDescription', 'ProductDocument', 'ProductInventory', 'ProductListPriceHistory', 'ProductModel', 'ProductModelIllustration', 'ProductModelProductDescriptionCulture', 'ProductModelorg', 'ProductPhoto', 'ProductProductPhoto', 'ProductReview', 'ProductSubcategory', 'ProductVendor', 'PurchaseOrderDetail', 'PurchaseOrderHeader', 'SalesOrderDetail', 'SalesOrderHeader', 'SalesOrderHeaderSalesReason', 'SalesPerson', 'SalesPersonQuotaHistory', 'SalesReason', 'SalesTaxRate', 'SalesTerritory', 'SalesTerritoryHistory', 'ScrapReason', 'Shift', 'ShipMethod', 'ShoppingCartItem', 'SpecialOffer', 'SpecialOfferProduct', 'StateProvince', 'Store', 'TransactionHistory', 'TransactionHistoryArchive', 'UnitMeasure', 'Vendor', 'WorkOrder', 'WorkOrderRouting']


# COMMAND ----------


