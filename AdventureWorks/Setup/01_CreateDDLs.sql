-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Migrating the AdventureWorks Database to a Lakehouse
-- MAGIC 
-- MAGIC This project utilizes the [Microsoft AdventureWorks Database](https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver15&tabs=ssms) as an example for a Lakehouse migration project. This will include both the OLTP and DW examples for redundency.  
-- MAGIC 
-- MAGIC This notebook contains example Schema Conversions that can be used as a reference for an actual migration project. We will not show the exact conversion for all tables but hope to convey the process in practices.  
-- MAGIC 
-- MAGIC 
-- MAGIC All tables with `ModifiedDate` and `rowguid` columns have default values with `current_timestamp()` and `uuid()` respectfully.  Row guid is only on insert while modified date is insert and updates.  
-- MAGIC 
-- MAGIC In this example we decided to migrate all schemas to a single hive database, as opposed to keeping them in seperate databases because table names are unique across schemas.  
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Note - total time to convert schema to Spark SQL was less than 16 hours of effort (74 tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Database Schema 
-- MAGIC 
-- MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/AdWorksSchema.png" />

-- COMMAND ----------

CREATE WIDGET TEXT DatabaseNamePrefix DEFAULT '';
CREATE WIDGET TEXT UserName DEFAULT '';

-- COMMAND ----------

SET var.database_name_prefix = $DatabaseNamePrefix ; 
SET var.database_name = ${var.database_name_prefix}_adventureworks_metadata;
SET var.user_name = $UserName ; 
SET var.table_location = '/users/${var.user_name}/databases/${var.database_name_prefix}_adventureworks_metadata' ;
CREATE DATABASE IF NOT EXISTS ${var.database_name} LOCATION ${var.table_location} ;
USE ${var.database_name} -- use the metadatabase by default;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbs = [d.database_name for d in spark.sql("SELECT DISTINCT database_name from table_metadata").collect()]
-- MAGIC for d in dbs:
-- MAGIC   spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(d))
-- MAGIC   database_location = f'/users/{dbutils.widgets.get("UserName")}/databases/{d}'
-- MAGIC   print(database_location)
-- MAGIC   if d not in ['dbo']:
-- MAGIC     spark.sql(f"CREATE DATABASE IF NOT EXISTS {d} LOCATION '{database_location}'")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC table_list = [i.n for i in spark.sql("SELECT concat(database_name, '.', table_name) as n FROM table_metadata").collect()]
-- MAGIC table_list
-- MAGIC for t in table_list:
-- MAGIC   spark.sql("DROP TABLE IF EXISTS {}".format(t))

-- COMMAND ----------

-- TSQL STATEMENT IN COMMENTS

-- CREATE TABLE [Person].[Address](
--     [AddressID] [int] IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
--     [AddressLine1] [nvarchar](60) NOT NULL, 
--     [AddressLine2] [nvarchar](60) NULL, 
--     [City] [nvarchar](30) NOT NULL, 
--     [StateProvinceID] [int] NOT NULL,
--     [PostalCode] [nvarchar](15) NOT NULL, 
-- 	[SpatialLocation] [geography] NULL,
--     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_Address_rowguid] DEFAULT (NEWID()),
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Address_ModifiedDate] DEFAULT (GETDATE())
-- ) ON [PRIMARY];
-- GO

CREATE OR REPLACE TABLE ${var.database_name_prefix}_person.Address (
    AddressID int NOT NULL,
    AddressLine1 string NOT NULL, 
    AddressLine2 string, 
    City string NOT NULL, 
    StateProvinceID int NOT NULL,
    PostalCode string NOT NULL, 
	SpatialLocation string,
    rowguid string NOT NULL COMMENT 'default value is uuid()',
    ModifiedDate timestamp NOT NULL COMMENT 'default value is current_timestamp()'
    --     ModifiedDate timestamp GENERATED ALWAYS AS (current_timestamp()) -- not possible to do default values. Must be done on update/insert

); 

-- COMMAND ----------

-- TSQL STATEMENT IN COMMENTS

-- CREATE TABLE [Person].[AddressType](
--     [AddressTypeID] [int] IDENTITY (1, 1) NOT NULL,
--     [Name] [Name] NOT NULL,
--     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_AddressType_rowguid] DEFAULT (NEWID()),
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_AddressType_ModifiedDate] DEFAULT (GETDATE())
-- ) ON [PRIMARY];
-- GO

CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.AddressType(
    AddressTypeID int NOT NULL,
    Name string NOT NULL,
    rowguid string NOT NULL COMMENT 'default value is uuid()',
    ModifiedDate timestamp NOT NULL COMMENT 'default value is current_timestamp()'
) ;

-- COMMAND ----------

-- TSQL STATEMENT IN COMMENTS

-- CREATE TABLE [Production].[BillOfMaterials](
--     [BillOfMaterialsID] [int] IDENTITY (1, 1) NOT NULL,
--     [ProductAssemblyID] [int] NULL,
--     [ComponentID] [int] NOT NULL,
--     [StartDate] [datetime] NOT NULL CONSTRAINT [DF_BillOfMaterials_StartDate] DEFAULT (GETDATE()),
--     [EndDate] [datetime] NULL,
--     [UnitMeasureCode] [nchar](3) NOT NULL, 
--     [BOMLevel] [smallint] NOT NULL,
--     [PerAssemblyQty] [decimal](8, 2) NOT NULL CONSTRAINT [DF_BillOfMaterials_PerAssemblyQty] DEFAULT (1.00),
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_BillOfMaterials_ModifiedDate] DEFAULT (GETDATE()),
--     CONSTRAINT [CK_BillOfMaterials_EndDate] CHECK (([EndDate] > [StartDate]) OR ([EndDate] IS NULL)),
--     CONSTRAINT [CK_BillOfMaterials_BOMLevel] CHECK ((([ProductAssemblyID] IS NULL) 
--         AND ([BOMLevel] = 0) AND ([PerAssemblyQty] = 1.00)) 
--         OR (([ProductAssemblyID] IS NOT NULL) AND ([BOMLevel] >= 1))), 
--     CONSTRAINT [CK_BillOfMaterials_PerAssemblyQty] CHECK ([PerAssemblyQty] >= 1.00) 
-- ) ON [PRIMARY];
-- GO


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.BillOfMaterials(
    BillOfMaterialsID int NOT NULL,
    ProductAssemblyID int,
    ComponentID int NOT NULL,
    StartDate timestamp NOT NULL,
    EndDate timestamp,
    UnitMeasureCode string NOT NULL, 
    BOMLevel short NOT NULL,
    PerAssemblyQty DECIMAL NOT NULL ,
    ModifiedDate timestamp NOT NULL COMMENT 'default value is current_timestamp()'
    --     ModifiedDate timestamp GENERATED ALWAYS AS (current_timestamp()) -- not possible to do default values. Must be done on update/insert

) ;

ALTER TABLE ${var.database_name_prefix}_Production.BillOfMaterials ADD CONSTRAINT CK_BillOfMaterials_EndDate CHECK ((EndDate > StartDate) OR (EndDate IS NULL));
ALTER TABLE ${var.database_name_prefix}_Production.BillOfMaterials ADD CONSTRAINT CK_BillOfMaterials_PerAssemblyQty CHECK (PerAssemblyQty >= 1.00);
ALTER TABLE ${var.database_name_prefix}_Production.BillOfMaterials ADD CONSTRAINT CK_BillOfMaterials_BOMLevel CHECK (((ProductAssemblyID IS NULL) 
        AND (BOMLevel = 0) AND (PerAssemblyQty = 1.00)) 
        OR ((ProductAssemblyID IS NOT NULL) AND (BOMLevel >= 1)));
        



-- COMMAND ----------

-- TSQL STATEMENT IN COMMENTS 

-- CREATE TABLE [Person].[BusinessEntity](
-- 	[BusinessEntityID] [int] IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
--     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_BusinessEntity_rowguid] DEFAULT (NEWID()), 
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_BusinessEntity_ModifiedDate] DEFAULT (GETDATE())	
-- ) ON [PRIMARY];
-- GO

CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.BusinessEntity(
	BusinessEntityID int NOT NULL COMMENT 'Must be unique',
    rowguid string NOT NULL COMMENT 'default value is uuid()',
    ModifiedDate timestamp NOT NULL COMMENT 'default value is current_timestamp()'
) ;

-- COMMAND ----------

-- TSQL STATEMENT IN COMMENTS 
-- CREATE TABLE [Person].[BusinessEntityAddress](
-- 	[BusinessEntityID] [int] NOT NULL,
--     [AddressID] [int] NOT NULL,
--     [AddressTypeID] [int] NOT NULL,
--     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_BusinessEntityAddress_rowguid] DEFAULT (NEWID()),
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_BusinessEntityAddress_ModifiedDate] DEFAULT (GETDATE()) 
-- ) ON [PRIMARY];


CREATE OR REPLACE  TABLE ${var.database_name_prefix}_Person.BusinessEntityAddress(
	BusinessEntityID int,
    AddressID int NOT NULL,
    AddressTypeID int NOT NULL,
    rowguid string NOT NULL COMMENT 'default value is uuid()',
    ModifiedDate timestamp NOT NULL COMMENT 'default value is current_timestamp()'
);

-- COMMAND ----------

-- CREATE TABLE [Person].[BusinessEntityContact](
-- 	[BusinessEntityID] [int] NOT NULL,
--     [PersonID] [int] NOT NULL,
--     [ContactTypeID] [int] NOT NULL,
--     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_BusinessEntityContact_rowguid] DEFAULT (NEWID()), 
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_BusinessEntityContact_ModifiedDate] DEFAULT (GETDATE()) 
-- ) ON [PRIMARY];
-- GO

CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.BusinessEntityContact(
	BusinessEntityID int NOT NULL,
    PersonID int NOT NULL,
    ContactTypeID int NOT NULL,
     rowguid string NOT NULL COMMENT 'default value is uuid()',
    ModifiedDate timestamp NOT NULL COMMENT 'default value is current_timestamp()'
)

-- COMMAND ----------

-- CREATE TABLE [Person].[ContactType](
--     [ContactTypeID] [int] IDENTITY (1, 1) NOT NULL,
--     [Name] [Name] NOT NULL, 
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ContactType_ModifiedDate] DEFAULT (GETDATE()) 
-- ) ON [PRIMARY];
-- GO

CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.ContactType(
    ContactTypeID int NOT NULL,
    Name string NOT NULL, 
    ModifiedDate timestamp not null 
);

-- COMMAND ----------

-- CREATE TABLE [Sales].[CountryRegionCurrency](
--     [CountryRegionCode] [nvarchar](3) NOT NULL, 
--     [CurrencyCode] [nchar](3) NOT NULL, 
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_CountryRegionCurrency_ModifiedDate] DEFAULT (GETDATE()) 
-- ) ON [PRIMARY];
-- GO

CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.CountryRegionCurrency(
    CountryRegionCode string NOT NULL, 
    CurrencyCode string NOT NULL, 
    ModifiedDate timestamp NOT NULL 
);

-- COMMAND ----------

-- CREATE TABLE [Sales].[CountryRegion](
--     [CountryRegionCode] [nvarchar](3) NOT NULL, 
--     [Name] [Name] NOT NULL, 
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_CountryRegion_ModifiedDate] DEFAULT (GETDATE()) 
-- ) ON [PRIMARY];
-- GO


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.CountryRegion (
    CountryRegionCode string NOT NULL, 
    Name string NOT NULL, 
    ModifiedDate timestamp NOT NULL 
) ;

ALTER TABLE ${var.database_name_prefix}_Person.CountryRegion ADD CONSTRAINT CK_CountryRegion_Code_Length CHECK (length(CountryRegionCode) <= 3); 


-- COMMAND ----------

-- CREATE TABLE [Sales].[CreditCard](
--     [CreditCardID] [int] IDENTITY (1, 1) NOT NULL,
--     [CardType] [nvarchar](50) NOT NULL,
--     [CardNumber] [nvarchar](25) NOT NULL,
--     [ExpMonth] [tinyint] NOT NULL,
--     [ExpYear] [smallint] NOT NULL, 
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_CreditCard_ModifiedDate] DEFAULT (GETDATE()) 
-- ) ON [PRIMARY];
-- GO

CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.CreditCard(
    CreditCardID int NOT NULL,
    CardType string NOT NULL,
    CardNumber string NOT NULL,
    ExpMonth short NOT NULL,
    ExpYear short NOT NULL, 
    ModifiedDate timestamp not null
);

-- COMMAND ----------

-- CREATE TABLE [Production].[Culture](
--     [CultureID] [nchar](6) NOT NULL,
--     [Name] [Name] NOT NULL, 
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Culture_ModifiedDate] DEFAULT (GETDATE()) 
-- ) ON [PRIMARY];
-- GO

CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.Culture(
    CultureID string NOT NULL,
    Name string NOT NULL, 
    ModifiedDate timestamp NOT NULL
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${var.database_name_prefix}_HumanResources.Employee(
    BusinessEntityID int NOT NULL,
    NationalIDNumber string not null, 
    LoginID string not null,     
    OrganizationNode string,
	OrganizationLevel string,
    JobTitle string not null, 
    BirthDate date NOT NULL,
    MaritalStatus string not null, 
    Gender string not null, 
    HireDate date NOT NULL,
    SalariedFlag boolean NOT NULL,
    VacationHours short NOT NULL,
    SickLeaveHours short NOT NULL,
    CurrentFlag boolean NOT NULL,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_Employee_BirthDate CHECK (BirthDate BETWEEN '1930-01-01' AND DATEADD(YEAR, -18, GETDATE())),
--     CONSTRAINT CK_Employee_MaritalStatus CHECK (UPPER(MaritalStatus) IN ('M', 'S')), -- Married or Single
--     CONSTRAINT CK_Employee_HireDate CHECK (HireDate BETWEEN '1996-07-01' AND DATEADD(DAY, 1, GETDATE())),
--     CONSTRAINT CK_Employee_Gender CHECK (UPPER(Gender) IN ('M', 'F')), -- Male or Female
--     CONSTRAINT CK_Employee_VacationHours CHECK (VacationHours BETWEEN -40 AND 240), 
--     CONSTRAINT CK_Employee_SickLeaveHours CHECK (SickLeaveHours BETWEEN 0 AND 120) 
);

ALTER TABLE ${var.database_name_prefix}_HumanResources.Employee ADD CONSTRAINT CK_Employee_BirthDate CHECK (BirthDate BETWEEN '1930-01-01' AND add_months(current_timestamp(), -18*12));
ALTER TABLE ${var.database_name_prefix}_HumanResources.Employee ADD CONSTRAINT CK_Employee_MaritalStatus CHECK (UPPER(MaritalStatus) IN ('M', 'S')); -- Married or Single
ALTER TABLE ${var.database_name_prefix}_HumanResources.Employee ADD CONSTRAINT CK_Employee_HireDate CHECK (HireDate BETWEEN '1996-07-01' AND add_months(current_timestamp(), 1));
ALTER TABLE ${var.database_name_prefix}_HumanResources.Employee ADD CONSTRAINT CK_Employee_Gender CHECK (UPPER(Gender) IN ('M', 'F')); -- Male or Female
ALTER TABLE ${var.database_name_prefix}_HumanResources.Employee ADD CONSTRAINT CK_Employee_VacationHours CHECK (VacationHours BETWEEN -40 AND 240);
ALTER TABLE ${var.database_name_prefix}_HumanResources.Employee ADD CONSTRAINT CK_Employee_SickLeaveHours CHECK (SickLeaveHours BETWEEN 0 AND 120) ;


-- COMMAND ----------

-- DBTITLE 1,The customer table requires a SQL Function 
-- BLOG: https://databricks.com/blog/2021/10/20/introducing-sql-user-defined-functions.html
-- This version of UDFs using only SQL is available in DBR 9.1+ 
-- If using a lesser version then you must define a UDF in scala/python 

-- TSQL Function ---
-- CREATE FUNCTION [dbo].[ufnLeadingZeros](
--     @Value int
-- ) 
-- RETURNS varchar(8) 
-- WITH SCHEMABINDING 
-- AS 
-- BEGIN
--     DECLARE @ReturnValue varchar(8);

--     SET @ReturnValue = CONVERT(varchar(8), @Value);
--     SET @ReturnValue = REPLICATE('0', 8 - DATALENGTH(@ReturnValue)) + @ReturnValue;

--     RETURN (@ReturnValue);
-- END;
-- GO


CREATE OR REPLACE FUNCTION rac_adventureworks_metadata.ufnLeadingZeros(value INT)
RETURNS STRING 
COMMENT 'Adds leading zeros to an account number to ensure exact length of string'
CONTAINS SQL DETERMINISTIC
RETURN LPAD(CAST(value as String), 8, '0')

-- COMMAND ----------

-- TSQL Statement 

-- CREATE TABLE [Sales].[Customer](
-- 	[CustomerID] [int] IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
-- 	-- A customer may either be a person, a store, or a person who works for a store
-- 	[PersonID] [int] NULL, -- If this customer represents a person, this is non-null
--     [StoreID] [int] NULL,  -- If the customer is a store, or is associated with a store then this is non-null.
--     [TerritoryID] [int] NULL,
--     [AccountNumber] AS ISNULL('AW' + [dbo].[ufnLeadingZeros](CustomerID), ''),
--     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_Customer_rowguid] DEFAULT (NEWID()), 
--     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Customer_ModifiedDate] DEFAULT (GETDATE())
-- ) ON [PRIMARY];
-- GO

CREATE TABLE ${var.database_name_prefix}_Sales.Customer(
	CustomerID int NOT NULL,
	-- A customer may either be a person, a store, or a person who works for a store
	PersonID int, -- If this customer represents a person, this is non-null
    StoreID int,  -- If the customer is a store, or is associated with a store then this is non-null.
    TerritoryID int,
--     AccountNumber string GENERATED ALWAYS AS (CONCAT('AW', ufnLeadingZeros(CustomerID))), -- unable to use UDFs for generated columns
    AccountNumber string not null COMMENT "Must use (CONCAT('AW', ufnLeadingZeros(CustomerID)))", 
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now that we have showed a number of different examples, we will just execute the remaining in spark SQL. 
-- MAGIC 
-- MAGIC Note some of the following resources:  
-- MAGIC - [Generated Columns](https://docs.microsoft.com/en-us/azure/databricks/delta/delta-batch#deltausegeneratedcolumns)  
-- MAGIC - [Constraints](https://docs.databricks.com/delta/delta-constraints.html)  
-- MAGIC   - Not Null
-- MAGIC   - Check 

-- COMMAND ----------

CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.Currency(
    CurrencyCode string NOT NULL, 
    Name string NOT NULL, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.CurrencyRate(
    CurrencyRateID int NOT NULL,
    CurrencyRateDate timestamp NOT NULL,    
    FromCurrencyCode string NOT NULL, 
    ToCurrencyCode string NOT NULL, 
    AverageRate string NOT NULL,
    EndOfDayRate string NOT NULL, 
    ModifiedDate timestamp NOT NULL
);


CREATE OR REPLACE TABLE ${var.database_name_prefix}_HumanResources.Department(
    DepartmentID short NOT NULL,
    Name string not null,
    GroupName string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.Document(
    DocumentNode string not null,
	DocumentLevel string,
    Title string not null, 
	Owner int NOT NULL,
	FolderFlag string not null,
    FileName string not null, 
    FileExtension string not null,
    Revision string not null, 
    ChangeNumber int NOT NULL,
    Status short NOT NULL,
    DocumentSummary string,
    Document string,  
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.EmailAddress(
	BusinessEntityID int NOT NULL,
	EmailAddressID int NOT NULL,
    EmailAddress string, 
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
);




CREATE OR REPLACE TABLE ${var.database_name_prefix}_HumanResources.EmployeeDepartmentHistory(
    BusinessEntityID int NOT NULL,
    DepartmentID short NOT NULL,
    ShiftID short NOT NULL,
    StartDate date NOT NULL,
    EndDate date, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_EmployeeDepartmentHistory_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL)),
);
ALTER TABLE ${var.database_name_prefix}_HumanResources.EmployeeDepartmentHistory ADD CONSTRAINT CK_EmployeeDepartmentHistory_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL));


CREATE OR REPLACE TABLE ${var.database_name_prefix}_HumanResources.EmployeePayHistory(
    BusinessEntityID int NOT NULL,
    RateChangeDate timestamp NOT NULL,
    Rate string NOT NULL,
    PayFrequency short NOT NULL COMMENT '1 = monthly salary, 2 = biweekly salary', 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_EmployeePayHistory_PayFrequency CHECK (PayFrequency IN (1, 2)), -- 1 = monthly salary, 2 = biweekly salary
--     CONSTRAINT CK_EmployeePayHistory_Rate CHECK (Rate BETWEEN 6.50 AND 200.00) 
);
ALTER TABLE ${var.database_name_prefix}_HumanResources.EmployeePayHistory ADD CONSTRAINT CK_EmployeePayHistory_PayFrequency CHECK (PayFrequency IN (1, 2)); -- 1 = monthly salary, 2 = biweekly salary
ALTER TABLE ${var.database_name_prefix}_HumanResources.EmployeePayHistory ADD CONSTRAINT CK_EmployeePayHistory_Rate CHECK (Rate BETWEEN 6.50 AND 200.00) ;


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.Illustration(
    IllustrationID int NOT NULL,
    Diagram string, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_HumanResources.JobCandidate(
    JobCandidateID int,
    BusinessEntityID int,
    Resume string, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.Location(
    LocationID short NOT NULL,
    Name string NOT NULL,
    CostRate DECIMAL,
    Availability DECIMAL, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_Location_CostRate CHECK (CostRate >= 0.00), 
--     CONSTRAINT CK_Location_Availability CHECK (Availability >= 0.00) 
);

ALTER TABLE ${var.database_name_prefix}_Production.Location ADD CONSTRAINT CK_Location_CostRate CHECK (CostRate >= 0.00);
ALTER TABLE ${var.database_name_prefix}_Production.Location ADD CONSTRAINT CK_Location_Availability CHECK (Availability >= 0.00);


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.Password(
	BusinessEntityID int NOT NULL,
    PasswordHash  string not null, 
    PasswordSalt string not null,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL

);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.Person(
    BusinessEntityID int NOT NULL,
	PersonType string not null,
    NameStyle string not null,
    Title string , 
    FirstName string not null,
    MiddleName string,
    LastName string not null,
    Suffix string , 
    EmailPromotion int NOT NULL, 
    AdditionalContactInfo string,
    Demographics string, 
    rowguid string not null, 
    ModifiedDate timestamp 
--     CONSTRAINT CK_Person_EmailPromotion CHECK (EmailPromotion BETWEEN 0 AND 2),
--     CONSTRAINT CK_Person_PersonType CHECK (PersonType IS NULL OR UPPER(PersonType) IN ('SC', 'VC', 'IN', 'EM', 'SP', 'GC'))
);

ALTER TABLE ${var.database_name_prefix}_Person.Person ADD CONSTRAINT CK_Person_EmailPromotion CHECK (EmailPromotion BETWEEN 0 AND 2);
ALTER TABLE ${var.database_name_prefix}_Person.Person ADD CONSTRAINT CK_Person_PersonType CHECK (PersonType IS NULL OR UPPER(PersonType) IN ('SC', 'VC', 'IN', 'EM', 'SP', 'GC'));



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.PersonCreditCard(
    BusinessEntityID int NOT NULL,
    CreditCardID int NOT NULL, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.PersonPhone(
    BusinessEntityID int NOT NULL,
	PhoneNumber string NOT NULL,
	PhoneNumberTypeID int NOT NULL,
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.PhoneNumberType(
	PhoneNumberTypeID int NOT NULL,
	Name string NOT NULL,
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.Product(
    ProductID int NOT NULL,
    Name string not null,
    ProductNumber string not null, 
    MakeFlag boolean,
    FinishedodsFlag boolean not null,
    Color string, 
    SafetyStockLevel short NOT NULL,
    ReorderPoint short NOT NULL,
    StandardCost string not null,
    ListPrice string not null,
    Size string not null, 
    SizeUnitMeasureCode string not null, 
    WeightUnitMeasureCode string not null, 
    Weight decimal,
    DaysToManufacture int NOT NULL,
    ProductLine string not null, 
    Class string not null, 
    Style string not null, 
    ProductSubcateryID int,
    ProductModelID int,
    SellStartDate string not null,
    SellEndDate string,
    DiscontinuedDate string,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_Product_SafetyStockLevel CHECK (SafetyStockLevel > 0),
--     CONSTRAINT CK_Product_ReorderPoint CHECK (ReorderPoint > 0),
--     CONSTRAINT CK_Product_StandardCost CHECK (StandardCost >= 0.00),
--     CONSTRAINT CK_Product_ListPrice CHECK (ListPrice >= 0.00),
--     CONSTRAINT CK_Product_Weight CHECK (Weight > 0.00),
--     CONSTRAINT CK_Product_DaysToManufacture CHECK (DaysToManufacture >= 0),
--     CONSTRAINT CK_Product_ProductLine CHECK (UPPER(ProductLine) IN ('S', 'T', 'M', 'R') OR ProductLine IS NULL),
--     CONSTRAINT CK_Product_Class CHECK (UPPER(Class) IN ('L', 'M', 'H') OR Class IS NULL),
--     CONSTRAINT CK_Product_Style CHECK (UPPER(Style) IN ('W', 'M', 'U') OR Style IS NULL), 
--     CONSTRAINT CK_Product_SellEndDate CHECK ((SellEndDate >= SellStartDate) OR (SellEndDate IS NULL))
);

ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_SafetyStockLevel CHECK (SafetyStockLevel > 0);
ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_ReorderPoint CHECK (ReorderPoint > 0);
ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_StandardCost CHECK (StandardCost >= 0.00);
ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_ListPrice CHECK (ListPrice >= 0.00);
ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_Weight CHECK (Weight >= 0.00 OR Weight is null);
ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_DaysToManufacture CHECK (DaysToManufacture >= 0);
ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_ProductLine CHECK (UPPER(ProductLine) IN ('S', 'T', 'M', 'R') OR ProductLine IS NULL);
ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_Class CHECK (UPPER(Class) IN ('L', 'M', 'H') OR Class IS NULL);
ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_Style CHECK (UPPER(Style) IN ('W', 'M', 'U') OR Style IS NULL); 
ALTER TABLE ${var.database_name_prefix}_Production.Product ADD CONSTRAINT CK_Product_SellEndDate CHECK ((SellEndDate >= SellStartDate) OR (SellEndDate IS NULL));


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductCategory(
    ProductCateryID int NOT NULL,
    Name string not null,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductCostHistory(
    ProductID int NOT NULL,
    StartDate timestamp NOT NULL,
    EndDate timestamp,
    StandardCost string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_ProductCostHistory_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL)),
--     CONSTRAINT CK_ProductCostHistory_StandardCost CHECK (StandardCost >= 0.00)
);
ALTER TABLE ${var.database_name_prefix}_Production.ProductCostHistory ADD
CONSTRAINT CK_ProductCostHistory_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL));
ALTER TABLE ${var.database_name_prefix}_Production.ProductCostHistory ADD CONSTRAINT CK_ProductCostHistory_StandardCost CHECK (StandardCost >= 0.00);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductDescription(
    ProductDescriptionID int,
    Description string ,
    rowguid string, 
    ModifiedDate timestamp
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductDocument(
    ProductID int NOT NULL,
    DocumentNode string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductInventory(
    ProductID int NOT NULL,
    LocationID short NOT NULL,
    Shelf string NOT NULL, 
    Bin short NOT NULL,
    Quantity short not null,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_ProductInventory_Bin CHECK (Bin BETWEEN 0 AND 100)
);

ALTER TABLE ${var.database_name_prefix}_Production.ProductInventory ADD CONSTRAINT CK_ProductInventory_Bin CHECK (Bin BETWEEN 0 AND 100);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductListPriceHistory(
    ProductID int NOT NULL,
    StartDate string not null,
    EndDate timestamp,
    ListPrice string NOT NULL, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_ProductListPriceHistory_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL)),
--     CONSTRAINT CK_ProductListPriceHistory_ListPrice CHECK (ListPrice > 0.00)
);

ALTER TABLE ${var.database_name_prefix}_Production.ProductListPriceHistory Add CONSTRAINT CK_ProductListPriceHistory_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL));
ALTER TABLE ${var.database_name_prefix}_Production.ProductListPriceHistory Add CONSTRAINT CK_ProductListPriceHistory_ListPrice CHECK (ListPrice > 0.00);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductModel(
    ProductModelID int NOT NULL,
    Name string not null,
    CatalogDescription string,
    Instructions string,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductModelIllustration(
    ProductModelID int NOT NULL,
    IllustrationID int NOT NULL, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductModelProductDescriptionCulture(
    ProductModelID int NOT NULL,
    ProductDescriptionID int NOT NULL,
    CultureID string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductPhoto(
    ProductPhotoID int NOT NULL,
    ThumbNailPhoto string not null,
    ThumbnailPhotoFileName string not null,
    LargePhoto string not null,
    LargePhotoFileName string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductProductPhoto(
    ProductID int NOT NULL,
    ProductPhotoID int NOT NULL,
    Primary boolean,
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductReview(
    ProductReviewID int NOT NULL,
    ProductID int NOT NULL,
    ReviewerName string not null,
    ReviewDate timestamp NOT NULL,
    EmailAddress string not null,
    Rating int NOT NULL,
    Comments string, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_ProductReview_Rating CHECK (Rating BETWEEN 1 AND 5), 
);
ALTER TABLE ${var.database_name_prefix}_Production.ProductReview ADD CONSTRAINT CK_ProductReview_Rating CHECK (Rating BETWEEN 1 AND 5);


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ProductSubcategory(
    ProductSubcateryID int NOT NULL,
    ProductCateryID int NOT NULL,
    Name string not null,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL 
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Purchasing.ProductVendor(
    ProductID int NOT NULL,
    BusinessEntityID int NOT NULL,
    AverageLeadTime int NOT NULL,
    StandardPrice string NOT NULL,
    LastReceiptCost string,
    LastReceiptDate string,
    MinOrderQty int NOT NULL,
    MaxOrderQty int NOT NULL,
    OnOrderQty int,
    UnitMeasureCode string NOT NULL, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_ProductVendor_AverageLeadTime CHECK (AverageLeadTime >= 1),
--     CONSTRAINT CK_ProductVendor_StandardPrice CHECK (StandardPrice > 0.00),
--     CONSTRAINT CK_ProductVendor_LastReceiptCost CHECK (LastReceiptCost > 0.00),
--     CONSTRAINT CK_ProductVendor_MinOrderQty CHECK (MinOrderQty >= 1),
--     CONSTRAINT CK_ProductVendor_MaxOrderQty CHECK (MaxOrderQty >= 1),
--     CONSTRAINT CK_ProductVendor_OnOrderQty CHECK (OnOrderQty >= 0)
);
ALTER TABLE ${var.database_name_prefix}_Purchasing.ProductVendor ADD CONSTRAINT CK_ProductVendor_AverageLeadTime CHECK (AverageLeadTime >= 1);
ALTER TABLE ${var.database_name_prefix}_Purchasing.ProductVendor ADD CONSTRAINT CK_ProductVendor_StandardPrice CHECK (StandardPrice > 0.00);
ALTER TABLE ${var.database_name_prefix}_Purchasing.ProductVendor ADD CONSTRAINT CK_ProductVendor_LastReceiptCost CHECK (LastReceiptCost > 0.00 or LastReceiptCost IS NULL);
ALTER TABLE ${var.database_name_prefix}_Purchasing.ProductVendor ADD CONSTRAINT CK_ProductVendor_MinOrderQty CHECK (MinOrderQty >= 1);
ALTER TABLE ${var.database_name_prefix}_Purchasing.ProductVendor ADD CONSTRAINT CK_ProductVendor_MaxOrderQty CHECK (MaxOrderQty >= 1);
ALTER TABLE ${var.database_name_prefix}_Purchasing.ProductVendor ADD CONSTRAINT CK_ProductVendor_OnOrderQty CHECK (OnOrderQty >= 0 OR OnOrderQty IS NULL);


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderDetail(
    PurchaseOrderID int NOT NULL,
    PurchaseOrderDetailID int NOT NULL,
    DueDate timestamp NOT NULL,
    OrderQty short NOT NULL,
    ProductID int NOT NULL,
    UnitPrice double NOT NULL,
    LineTotal double GENERATED ALWAYS AS (OrderQty * UnitPrice) COMMENT 'GENERATED COLUMN', 
    ReceivedQty double NOT NULL,
    RejectedQty double NOT NULL,
    StockedQty double GENERATED ALWAYS AS (ReceivedQty - RejectedQty) COMMENT 'GENERATED COLUMN',
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_PurchaseOrderDetail_OrderQty CHECK (OrderQty > 0), 
--     CONSTRAINT CK_PurchaseOrderDetail_UnitPrice CHECK (UnitPrice >= 0.00), 
--     CONSTRAINT CK_PurchaseOrderDetail_ReceivedQty CHECK (ReceivedQty >= 0.00), 
--     CONSTRAINT CK_PurchaseOrderDetail_RejectedQty CHECK (RejectedQty >= 0.00) 
);
ALTER TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderDetail ADD CONSTRAINT CK_PurchaseOrderDetail_OrderQty CHECK (OrderQty > 0) ;
ALTER TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderDetail ADD CONSTRAINT CK_PurchaseOrderDetail_UnitPrice CHECK (UnitPrice >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderDetail ADD CONSTRAINT CK_PurchaseOrderDetail_ReceivedQty CHECK (ReceivedQty >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderDetail ADD CONSTRAINT CK_PurchaseOrderDetail_RejectedQty CHECK (RejectedQty >= 0.00) ;



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderHeader(
    PurchaseOrderID int NOT NULL, 
    RevisionNumber short NOT NULL, 
    Status short NOT NULL COMMENT '1 = Pending, 2 = Approved, 3 = Rejected, 4 = Complete ', 
    EmployeeID int NOT NULL, 
    VendorID int NOT NULL, 
    ShipMethodID int NOT NULL, 
    OrderDate timestamp NOT NULL, 
    ShipDate timestamp, 
    SubTotal double not null, 
    TaxAmt double not null, 
    Freight double not null, 
    TotalDue double GENERATED ALWAYS AS (SubTotal + TaxAmt + Freight) COMMENT 'GENERATED COLUMN', 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_PurchaseOrderHeader_Status CHECK (Status BETWEEN 1 AND 4),
--     CONSTRAINT CK_PurchaseOrderHeader_ShipDate CHECK ((ShipDate >= OrderDate) OR (ShipDate IS NULL)), 
--     CONSTRAINT CK_PurchaseOrderHeader_SubTotal CHECK (SubTotal >= 0.00), 
--     CONSTRAINT CK_PurchaseOrderHeader_TaxAmt CHECK (TaxAmt >= 0.00), 
--     CONSTRAINT CK_PurchaseOrderHeader_Freight CHECK (Freight >= 0.00) 
);
ALTER TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderHeader ADD CONSTRAINT CK_PurchaseOrderHeader_Status CHECK (Status BETWEEN 1 AND 4);
ALTER TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderHeader ADD CONSTRAINT CK_PurchaseOrderHeader_ShipDate CHECK ((ShipDate >= OrderDate) OR (ShipDate IS NULL)) ;
ALTER TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderHeader ADD CONSTRAINT CK_PurchaseOrderHeader_SubTotal CHECK (SubTotal >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderHeader ADD CONSTRAINT CK_PurchaseOrderHeader_TaxAmt CHECK (TaxAmt >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Purchasing.PurchaseOrderHeader ADD CONSTRAINT CK_PurchaseOrderHeader_Freight CHECK (Freight >= 0.00) ;


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SalesOrderDetail(
    SalesOrderID int NOT NULL,
    SalesOrderDetailID int NOT NULL,
    CarrierTrackingNumber string, 
    OrderQty short NOT NULL,
    ProductID int NOT NULL,
    SpecialOfferID int NOT NULL,
    UnitPrice double NOT NULL,
    UnitPriceDiscount double not null,
    LineTotal double GENERATED ALWAYS AS (UnitPrice * (1.0 - UnitPriceDiscount) * OrderQty) COMMENT 'GENERATED COLUMN',
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_SalesOrderDetail_OrderQty CHECK (OrderQty > 0), 
--     CONSTRAINT CK_SalesOrderDetail_UnitPrice CHECK (UnitPrice >= 0.00), 
--     CONSTRAINT CK_SalesOrderDetail_UnitPriceDiscount CHECK (UnitPriceDiscount >= 0.00) 
);
ALTER TABLE ${var.database_name_prefix}_Sales.SalesOrderDetail ADD CONSTRAINT CK_SalesOrderDetail_OrderQty CHECK (OrderQty > 0) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesOrderDetail ADD CONSTRAINT CK_SalesOrderDetail_UnitPrice CHECK (UnitPrice >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesOrderDetail ADD CONSTRAINT CK_SalesOrderDetail_UnitPriceDiscount CHECK (UnitPriceDiscount >= 0.00) ;


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SalesOrderHeader(
    SalesOrderID int NOT NULL,
    RevisionNumber short NOT NULL,
    OrderDate timestamp NOT NULL,
    DueDate timestamp NOT NULL,
    ShipDate timestamp ,
    Status short NOT NULL,
    OnlineOrderFlag boolean,
    SalesOrderNumber string GENERATED ALWAYS AS (cast(SalesOrderID as STRING)) COMMENT 'GENERATED COLUMN',
    PurchaseOrderNumber string,
    AccountNumber string,
    CustomerID int NOT NULL,
    SalesPersonID int,
    TerritoryID int,
    BillToAddressID int NOT NULL,
    ShipToAddressID int NOT NULL,
    ShipMethodID int NOT NULL,
    CreditCardID int,
    CreditCardApprovalCode string,    
    CurrencyRateID int,
    SubTotal double NOT NULL,
    TaxAmt double NOT NULL,
    Freight double NOT NULL,
    TotalDue double GENERATED ALWAYS AS (SubTotal + TaxAmt + Freight) COMMENT 'GENERATED COLUMN',
    Comment string,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_SalesOrderHeader_Status CHECK (Status BETWEEN 0 AND 8), 
--     CONSTRAINT CK_SalesOrderHeader_DueDate CHECK (DueDate >= OrderDate), 
--     CONSTRAINT CK_SalesOrderHeader_ShipDate CHECK ((ShipDate >= OrderDate) OR (ShipDate IS NULL)), 
--     CONSTRAINT CK_SalesOrderHeader_SubTotal CHECK (SubTotal >= 0.00), 
--     CONSTRAINT CK_SalesOrderHeader_TaxAmt CHECK (TaxAmt >= 0.00), 
--     CONSTRAINT CK_SalesOrderHeader_Freight CHECK (Freight >= 0.00) 
);

ALTER TABLE ${var.database_name_prefix}_Sales.SalesOrderHeader ADD CONSTRAINT CK_SalesOrderHeader_Status CHECK (Status BETWEEN 0 AND 8) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesOrderHeader ADD CONSTRAINT CK_SalesOrderHeader_DueDate CHECK (DueDate >= OrderDate) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesOrderHeader ADD CONSTRAINT CK_SalesOrderHeader_ShipDate CHECK ((ShipDate >= OrderDate) OR (ShipDate IS NULL)) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesOrderHeader ADD CONSTRAINT CK_SalesOrderHeader_SubTotal CHECK (SubTotal >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesOrderHeader ADD CONSTRAINT CK_SalesOrderHeader_TaxAmt CHECK (TaxAmt >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesOrderHeader ADD CONSTRAINT CK_SalesOrderHeader_Freight CHECK (Freight >= 0.00) ;





CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SalesOrderHeaderSalesReason(
    SalesOrderID int NOT NULL,
    SalesReasonID int NOT NULL, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SalesPerson(
    BusinessEntityID int NOT NULL,
    TerritoryID int,
    SalesQuota string,
    Bonus string not null,
    CommissionPct string not null,
    SalesYTD string not null,
    SalesLastYear string not null,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_SalesPerson_SalesQuota CHECK (SalesQuota > 0.00), 
--     CONSTRAINT CK_SalesPerson_Bonus CHECK (Bonus >= 0.00), 
--     CONSTRAINT CK_SalesPerson_CommissionPct CHECK (CommissionPct >= 0.00), 
--     CONSTRAINT CK_SalesPerson_SalesYTD CHECK (SalesYTD >= 0.00), 
--     CONSTRAINT CK_SalesPerson_SalesLastYear CHECK (SalesLastYear >= 0.00) 
);

ALTER TABLE ${var.database_name_prefix}_Sales.SalesPerson ADD CONSTRAINT CK_SalesPerson_SalesQuota CHECK (SalesQuota >= 0.00 OR SalesQuota is Null) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesPerson ADD CONSTRAINT CK_SalesPerson_Bonus CHECK (Bonus >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesPerson ADD CONSTRAINT CK_SalesPerson_CommissionPct CHECK (CommissionPct >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesPerson ADD CONSTRAINT CK_SalesPerson_SalesYTD CHECK (SalesYTD >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesPerson ADD CONSTRAINT CK_SalesPerson_SalesLastYear CHECK (SalesLastYear >= 0.00) ;



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SalesPersonQuotaHistory(
    BusinessEntityID int NOT NULL,
    QuotaDate timestamp NOT NULL,
    SalesQuota string NOT NULL,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_SalesPersonQuotaHistory_SalesQuota CHECK (SalesQuota > 0.00) 
);

ALTER TABLE ${var.database_name_prefix}_Sales.SalesPersonQuotaHistory ADD CONSTRAINT CK_SalesPersonQuotaHistory_SalesQuota CHECK (SalesQuota > 0.00) ;



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SalesReason(
    SalesReasonID int NOT NULL,
    Name string NOT NULL,
    ReasonType string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SalesTaxRate(
    SalesTaxRateID int NOT NULL,
    StateProvinceID int NOT NULL,
    TaxType short NOT NULL,
    TaxRate double NOT NULL,
    Name string NOT NULL,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_SalesTaxRate_TaxType CHECK (TaxType BETWEEN 1 AND 3)
);
ALTER TABLE ${var.database_name_prefix}_Sales.SalesTaxRate ADD CONSTRAINT CK_SalesTaxRate_TaxType CHECK (TaxType BETWEEN 1 AND 3);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SalesTerritory(
    TerritoryID int NOT NULL,
    Name string NOT NULL,
    CountryRegionCode string not null, 
    Group string not null,
    SalesYTD string not null,
    SalesLastYear string not null,
    CostYTD string not null,
    CostLastYear string not null,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_SalesTerritory_SalesYTD CHECK (SalesYTD >= 0.00), 
--     CONSTRAINT CK_SalesTerritory_SalesLastYear CHECK (SalesLastYear >= 0.00), 
--     CONSTRAINT CK_SalesTerritory_CostYTD CHECK (CostYTD >= 0.00), 
--     CONSTRAINT CK_SalesTerritory_CostLastYear CHECK (CostLastYear >= 0.00) 
);
ALTER TABLE ${var.database_name_prefix}_Sales.SalesTerritory ADD CONSTRAINT CK_SalesTerritory_SalesYTD CHECK (SalesYTD >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesTerritory ADD CONSTRAINT CK_SalesTerritory_SalesLastYear CHECK (SalesLastYear >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesTerritory ADD CONSTRAINT CK_SalesTerritory_CostYTD CHECK (CostYTD >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SalesTerritory ADD CONSTRAINT CK_SalesTerritory_CostLastYear CHECK (CostLastYear >= 0.00) ;



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SalesTerritoryHistory(
    BusinessEntityID int NOT NULL,  -- A sales person
    TerritoryID int NOT NULL,
    StartDate timestamp NOT NULL,
    EndDate timestamp,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_SalesTerritoryHistory_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL))
);

ALTER TABLE ${var.database_name_prefix}_Sales.SalesTerritoryHistory ADD CONSTRAINT CK_SalesTerritoryHistory_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL));



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.ScrapReason(
    ScrapReasonID short NOT NULL,
    Name string NOT NULL, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_HumanResources.Shift(
    ShiftID short NOT NULL,
    Name string NOT NULL,
    StartTime string NOT NULL,
    EndTime string NOT NULL, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Purchasing.ShipMethod(
    ShipMethodID int NOT NULL,
    Name string NOT NULL,
    ShipBase string not null,
    ShipRate string not null,
    rowguid string not null, 
    ModifiedDate timestamp not null
--     CONSTRAINT CK_ShipMethod_ShipBase CHECK (ShipBase > 0.00), 
--     CONSTRAINT CK_ShipMethod_ShipRate CHECK (ShipRate > 0.00), 
);
ALTER TABLE ${var.database_name_prefix}_Purchasing.ShipMethod ADD CONSTRAINT CK_ShipMethod_ShipBase CHECK (ShipBase > 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Purchasing.ShipMethod ADD CONSTRAINT CK_ShipMethod_ShipRate CHECK (ShipRate > 0.00) ;



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.ShoppingCartItem(
    ShoppingCartItemID int NOT NULL,
    ShoppingCartID string not null,
    Quantity int NOT NULL,
    ProductID int NOT NULL,
    DateCreated timestamp NOT NULL, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_ShoppingCartItem_Quantity CHECK (Quantity >= 1) 
);
ALTER TABLE ${var.database_name_prefix}_Sales.ShoppingCartItem ADD CONSTRAINT CK_ShoppingCartItem_Quantity CHECK (Quantity >= 1) ;


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SpecialOffer(
    SpecialOfferID int NOT NULL,
    Description string not null,
    DiscountPct string not null,
    Type string NOT NULL,
    Catery string not null,
    StartDate timestamp NOT NULL,
    EndDate timestamp NOT NULL,
    MinQty int NOT NULL, 
    MaxQty int,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_SpecialOffer_EndDate CHECK (EndDate >= StartDate), 
--     CONSTRAINT CK_SpecialOffer_DiscountPct CHECK (DiscountPct >= 0.00), 
--     CONSTRAINT CK_SpecialOffer_MinQty CHECK (MinQty >= 0), 
--     CONSTRAINT CK_SpecialOffer_MaxQty  CHECK (MaxQty >= 0)
);
ALTER TABLE ${var.database_name_prefix}_Sales.SpecialOffer ADD CONSTRAINT CK_SpecialOffer_EndDate CHECK (EndDate >= StartDate) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SpecialOffer ADD CONSTRAINT CK_SpecialOffer_DiscountPct CHECK (DiscountPct >= 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SpecialOffer ADD CONSTRAINT CK_SpecialOffer_MinQty CHECK (MinQty >= 0) ;
ALTER TABLE ${var.database_name_prefix}_Sales.SpecialOffer ADD CONSTRAINT CK_SpecialOffer_MaxQty  CHECK (MaxQty >= 0 or MaxQty IS NULL);


CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.SpecialOfferProduct(
    SpecialOfferID int NOT NULL,
    ProductID int NOT NULL,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Person.StateProvince(
    StateProvinceID int NOT NULL,
    StateProvinceCode string not null, 
    CountryRegionCode string not null, 
    IsOnlyStateProvinceFlag boolean not null,
    Name string NOT NULL,
    TerritoryID int NOT NULL,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Sales.Store(
    BusinessEntityID int NOT NULL,
    Name string not null,
    SalesPersonID int,
    Demographics string,
    rowguid string not null, 
    ModifiedDate timestamp NOT NULL 
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.TransactionHistory(
    TransactionID int NOT NULL,
    ProductID int NOT NULL,
    ReferenceOrderID int NOT NULL,
    ReferenceOrderLineID int NOT NULL ,
    TransactionDate timestamp NOT NULL,
    TransactionType string not null, 
    Quantity int NOT NULL,
    ActualCost string not null, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_TransactionHistory_TransactionType CHECK (UPPER(TransactionType) IN ('W', 'S', 'P'))
);

ALTER TABLE ${var.database_name_prefix}_Production.TransactionHistory ADD CONSTRAINT CK_TransactionHistory_TransactionType CHECK (UPPER(TransactionType) IN ('W', 'S', 'P'));



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.TransactionHistoryArchive(
    TransactionID int NOT NULL,
    ProductID int NOT NULL,
    ReferenceOrderID int NOT NULL,
    ReferenceOrderLineID int NOT NULL,
    TransactionDate timestamp NOT NULL,
    TransactionType string NOT NULL, 
    Quantity int NOT NULL,
    ActualCost double NOT NULL, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_TransactionHistoryArchive_TransactionType CHECK (UPPER(TransactionType) IN ('W', 'S', 'P'))
);

ALTER TABLE ${var.database_name_prefix}_Production.TransactionHistoryArchive ADD CONSTRAINT CK_TransactionHistoryArchive_TransactionType CHECK (UPPER(TransactionType) IN ('W', 'S', 'P'));



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.UnitMeasure(
    UnitMeasureCode string NOT NULL, 
    Name string NOT NULL, 
    ModifiedDate timestamp NOT NULL
);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Purchasing.Vendor(
    BusinessEntityID int NOT NULL,
    AccountNumber string NOT NULL,
    Name string NOT NULL,
    CreditRating short NOT NULL,
    PreferredVendorStatus string NOT NULL, 
    ActiveFlag int not null,
    PurchasingWebServiceURL string, 
    ModifiedDate timestamp NOT NULL
--     CONSTRAINT CK_Vendor_CreditRating CHECK (CreditRating BETWEEN 1 AND 5)
);

ALTER TABLE ${var.database_name_prefix}_Purchasing.Vendor ADD CONSTRAINT CK_Vendor_CreditRating CHECK (CreditRating BETWEEN 1 AND 5);



CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.WorkOrder(
    WorkOrderID int NOT NULL,
    ProductID int NOT NULL,
    OrderQty int NOT NULL,
    StockedQty double GENERATED ALWAYS AS (cast((OrderQty - ScrappedQty) as double)) COMMENT 'GENERATED COLUMN',
    ScrappedQty short NOT NULL,
    StartDate timestamp NOT NULL,
    EndDate timestamp,
    DueDate timestamp NOT NULL,
    ScrapReasonID short, 
    ModifiedDate timestamp NOT NULL 
--     CONSTRAINT CK_WorkOrder_OrderQty CHECK (OrderQty > 0), 
--     CONSTRAINT CK_WorkOrder_ScrappedQty CHECK (ScrappedQty >= 0), 
--     CONSTRAINT CK_WorkOrder_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL))
);

ALTER TABLE ${var.database_name_prefix}_Production.WorkOrder ADD CONSTRAINT CK_WorkOrder_OrderQty CHECK (OrderQty > 0) ;
ALTER TABLE ${var.database_name_prefix}_Production.WorkOrder ADD CONSTRAINT CK_WorkOrder_ScrappedQty CHECK (ScrappedQty >= 0) ;
ALTER TABLE ${var.database_name_prefix}_Production.WorkOrder ADD CONSTRAINT CK_WorkOrder_EndDate CHECK ((EndDate >= StartDate) OR (EndDate IS NULL));

CREATE OR REPLACE TABLE ${var.database_name_prefix}_Production.WorkOrderRouting(
    WorkOrderID int NOT NULL,
    ProductID int NOT NULL,
    OperationSequence short NOT NULL,
    LocationID short NOT NULL,
    ScheduledStartDate timestamp,
    ScheduledEndDate timestamp,
    ActualStartDate timestamp,
    ActualEndDate timestamp,
    ActualResourceHrs decimal,
    PlannedCost double NOT NULL,
    ActualCost double, 
    ModifiedDate timestamp NOT NULL 
--     CONSTRAINT CK_WorkOrderRouting_ScheduledEndDate CHECK (ScheduledEndDate >= ScheduledStartDate), 
--     CONSTRAINT CK_WorkOrderRouting_ActualEndDate CHECK ((ActualEndDate >= ActualStartDate) OR (ActualEndDate IS NULL) OR (ActualStartDate IS NULL)), 
--     CONSTRAINT CK_WorkOrderRouting_ActualResourceHrs CHECK (ActualResourceHrs >= 0.0000), 
--     CONSTRAINT CK_WorkOrderRouting_PlannedCost CHECK (PlannedCost > 0.00), 
--     CONSTRAINT CK_WorkOrderRouting_ActualCost CHECK (ActualCost > 0.00) 
);

ALTER TABLE ${var.database_name_prefix}_Production.WorkOrderRouting ADD CONSTRAINT CK_WorkOrderRouting_ScheduledEndDate CHECK (ScheduledEndDate >= ScheduledStartDate) ;
ALTER TABLE ${var.database_name_prefix}_Production.WorkOrderRouting ADD CONSTRAINT CK_WorkOrderRouting_ActualEndDate CHECK ((ActualEndDate >= ActualStartDate) OR (ActualEndDate IS NULL) OR (ActualStartDate IS NULL)) ;
ALTER TABLE ${var.database_name_prefix}_Production.WorkOrderRouting ADD CONSTRAINT CK_WorkOrderRouting_ActualResourceHrs CHECK (ActualResourceHrs >= 0.0000) ;
ALTER TABLE ${var.database_name_prefix}_Production.WorkOrderRouting ADD CONSTRAINT CK_WorkOrderRouting_PlannedCost CHECK (PlannedCost > 0.00) ;
ALTER TABLE ${var.database_name_prefix}_Production.WorkOrderRouting ADD CONSTRAINT CK_WorkOrderRouting_ActualCost CHECK (ActualCost > 0.00) ;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Completed DDL Setup")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Converting SQL Programmatically 
-- MAGIC 
-- MAGIC There are A LOT better ways to programmatically convert TSQL to Spark SQL. This is a quick function I wrote that allowed me to avoid the completely manual conversion. But please note that this still required manual effort. 
-- MAGIC 
-- MAGIC PLEASE IGNORE THE REST OF THIS NOTEBOOK

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC # Not official mappings as I determined these myself
-- MAGIC ## key = TSQL Type
-- MAGIC ## Value = Spark SQL Type
-- MAGIC 
-- MAGIC ### For future iterations of this notebook. Not currently used for DDL conversions. 
-- MAGIC data_type_mappings = {
-- MAGIC   "varbinary":"string",
-- MAGIC   "datetime":"timestamp",
-- MAGIC   "datetime2":"timestamp",
-- MAGIC   "date":"date",
-- MAGIC   "int":"int",
-- MAGIC   "double":"double",
-- MAGIC   "smallint":"short",
-- MAGIC   "bigint":"long",
-- MAGIC   "name":"string",
-- MAGIC   "money":"string",
-- MAGIC   "varchar":"string",
-- MAGIC   "nchar":"string",
-- MAGIC   "nvarchar":"string",
-- MAGIC   "uniqueidentifier": "string",
-- MAGIC   "tinyint":"short",
-- MAGIC   "flag":"boolean",
-- MAGIC   "xml":"string"
-- MAGIC }

-- COMMAND ----------

-- DBTITLE 1,Used to translate constraint comments
string_value = """
--     CONSTRAINT CK_WorkOrderRouting_ScheduledEndDate CHECK (ScheduledEndDate >= ScheduledStartDate), 
--     CONSTRAINT CK_WorkOrderRouting_ActualEndDate CHECK ((ActualEndDate >= ActualStartDate) OR (ActualEndDate IS NULL) OR (ActualStartDate IS NULL)), 
--     CONSTRAINT CK_WorkOrderRouting_ActualResourceHrs CHECK (ActualResourceHrs >= 0.0000), 
--     CONSTRAINT CK_WorkOrderRouting_PlannedCost CHECK (PlannedCost > 0.00), 
--     CONSTRAINT CK_WorkOrderRouting_ActualCost CHECK (ActualCost > 0.00) 
"""

table_name = "WorkOrderRouting"

string_arr = string_value.split("-- ")

i = 0
for s in string_arr:
  string_arr[i] = s.replace("    ", "").replace(",", "").replace("\n", "")
  i = i + 1

i = 0
for s in string_arr:
  string_arr[i] = "ALTER TABLE {} ADD {};".format(table_name, s)
  i = i + 1
  
print("\n".join(string_arr))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def parse_sql(sql):
-- MAGIC   sql = sql.replace("[Person].", "").replace("[Sales].", "").replace("[Production].", "").replace("[HumanResources].", "").replace("[Purchasing]", "")
-- MAGIC   sql = sql.replace(" ON [PRIMARY]", ";")
-- MAGIC   sql = sql.replace("[", "").replace("]", "")
-- MAGIC   print(sql)
-- MAGIC   

-- COMMAND ----------

-- MAGIC %python
-- MAGIC all_sql = """
-- MAGIC CREATE TABLE [Sales].[Currency](
-- MAGIC     [CurrencyCode] [nchar](3) NOT NULL, 
-- MAGIC     [Name] [Name] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Currency_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[CurrencyRate](
-- MAGIC     [CurrencyRateID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [CurrencyRateDate] [datetime] NOT NULL,    
-- MAGIC     [FromCurrencyCode] [nchar](3) NOT NULL, 
-- MAGIC     [ToCurrencyCode] [nchar](3) NOT NULL, 
-- MAGIC     [AverageRate] [money] NOT NULL,
-- MAGIC     [EndOfDayRate] [money] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_CurrencyRate_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[Customer](
-- MAGIC 	[CustomerID] [int] IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
-- MAGIC 	-- A customer may either be a person, a store, or a person who works for a store
-- MAGIC 	[PersonID] [int] NULL, -- If this customer represents a person, this is non-null
-- MAGIC     [StoreID] [int] NULL,  -- If the customer is a store, or is associated with a store then this is non-null.
-- MAGIC     [TerritoryID] [int] NULL,
-- MAGIC     [AccountNumber] AS ISNULL('AW' + [dbo].[ufnLeadingZeros](CustomerID), ''),
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_Customer_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Customer_ModifiedDate] DEFAULT (GETDATE())
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [HumanResources].[Department](
-- MAGIC     [DepartmentID] [smallint] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [GroupName] [Name] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Department_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[Document](
-- MAGIC     [DocumentNode] [hierarchyid] NOT NULL,
-- MAGIC 	[DocumentLevel] AS DocumentNode.GetLevel(),
-- MAGIC     [Title] [nvarchar](50) NOT NULL, 
-- MAGIC 	[Owner] [int] NOT NULL,
-- MAGIC 	[FolderFlag] [bit] NOT NULL CONSTRAINT [DF_Document_FolderFlag] DEFAULT (0),
-- MAGIC     [FileName] [nvarchar](400) NOT NULL, 
-- MAGIC     [FileExtension] nvarchar(8) NOT NULL,
-- MAGIC     [Revision] [nchar](5) NOT NULL, 
-- MAGIC     [ChangeNumber] [int] NOT NULL CONSTRAINT [DF_Document_ChangeNumber] DEFAULT (0),
-- MAGIC     [Status] [tinyint] NOT NULL,
-- MAGIC     [DocumentSummary] [nvarchar](max) NULL,
-- MAGIC     [Document] [varbinary](max)  NULL,  
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL UNIQUE CONSTRAINT [DF_Document_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Document_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_Document_Status] CHECK ([Status] BETWEEN 1 AND 3)
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Person].[EmailAddress](
-- MAGIC 	[BusinessEntityID] [int] NOT NULL,
-- MAGIC 	[EmailAddressID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [EmailAddress] [nvarchar](50) NULL, 
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_EmailAddress_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_EmailAddress_ModifiedDate] DEFAULT (GETDATE())
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC CREATE TABLE [HumanResources].[Employee](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC     [NationalIDNumber] [nvarchar](15) NOT NULL, 
-- MAGIC     [LoginID] [nvarchar](256) NOT NULL,     
-- MAGIC     [OrganizationNode] [hierarchyid] NULL,
-- MAGIC 	[OrganizationLevel] AS OrganizationNode.GetLevel(),
-- MAGIC     [JobTitle] [nvarchar](50) NOT NULL, 
-- MAGIC     [BirthDate] [date] NOT NULL,
-- MAGIC     [MaritalStatus] [nchar](1) NOT NULL, 
-- MAGIC     [Gender] [nchar](1) NOT NULL, 
-- MAGIC     [HireDate] [date] NOT NULL,
-- MAGIC     [SalariedFlag] [Flag] NOT NULL CONSTRAINT [DF_Employee_SalariedFlag] DEFAULT (1),
-- MAGIC     [VacationHours] [smallint] NOT NULL CONSTRAINT [DF_Employee_VacationHours] DEFAULT (0),
-- MAGIC     [SickLeaveHours] [smallint] NOT NULL CONSTRAINT [DF_Employee_SickLeaveHours] DEFAULT (0),
-- MAGIC     [CurrentFlag] [Flag] NOT NULL CONSTRAINT [DF_Employee_CurrentFlag] DEFAULT (1),
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_Employee_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Employee_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_Employee_BirthDate] CHECK ([BirthDate] BETWEEN '1930-01-01' AND DATEADD(YEAR, -18, GETDATE())),
-- MAGIC     CONSTRAINT [CK_Employee_MaritalStatus] CHECK (UPPER([MaritalStatus]) IN ('M', 'S')), -- Married or Single
-- MAGIC     CONSTRAINT [CK_Employee_HireDate] CHECK ([HireDate] BETWEEN '1996-07-01' AND DATEADD(DAY, 1, GETDATE())),
-- MAGIC     CONSTRAINT [CK_Employee_Gender] CHECK (UPPER([Gender]) IN ('M', 'F')), -- Male or Female
-- MAGIC     CONSTRAINT [CK_Employee_VacationHours] CHECK ([VacationHours] BETWEEN -40 AND 240), 
-- MAGIC     CONSTRAINT [CK_Employee_SickLeaveHours] CHECK ([SickLeaveHours] BETWEEN 0 AND 120) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [HumanResources].[EmployeeDepartmentHistory](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC     [DepartmentID] [smallint] NOT NULL,
-- MAGIC     [ShiftID] [tinyint] NOT NULL,
-- MAGIC     [StartDate] [date] NOT NULL,
-- MAGIC     [EndDate] [date] NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_EmployeeDepartmentHistory_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_EmployeeDepartmentHistory_EndDate] CHECK (([EndDate] >= [StartDate]) OR ([EndDate] IS NULL)),
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [HumanResources].[EmployeePayHistory](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC     [RateChangeDate] [datetime] NOT NULL,
-- MAGIC     [Rate] [money] NOT NULL,
-- MAGIC     [PayFrequency] [tinyint] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_EmployeePayHistory_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_EmployeePayHistory_PayFrequency] CHECK ([PayFrequency] IN (1, 2)), -- 1 = monthly salary, 2 = biweekly salary
-- MAGIC     CONSTRAINT [CK_EmployeePayHistory_Rate] CHECK ([Rate] BETWEEN 6.50 AND 200.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[Illustration](
-- MAGIC     [IllustrationID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Diagram] [XML] NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Illustration_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [HumanResources].[JobCandidate](
-- MAGIC     [JobCandidateID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [BusinessEntityID] [int] NULL,
-- MAGIC     [Resume] [XML]([HumanResources].[HRResumeSchemaCollection]) NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_JobCandidate_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[Location](
-- MAGIC     [LocationID] [smallint] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [CostRate] [smallmoney] NOT NULL CONSTRAINT [DF_Location_CostRate] DEFAULT (0.00),
-- MAGIC     [Availability] [decimal](8, 2) NOT NULL CONSTRAINT [DF_Location_Availability] DEFAULT (0.00), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Location_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_Location_CostRate] CHECK ([CostRate] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_Location_Availability] CHECK ([Availability] >= 0.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Person].[Password](
-- MAGIC 	[BusinessEntityID] [int] NOT NULL,
-- MAGIC     [PasswordHash] [varchar](128) NOT NULL, 
-- MAGIC     [PasswordSalt] [varchar](10) NOT NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_Password_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Password_ModifiedDate] DEFAULT (GETDATE())
-- MAGIC 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Person].[Person](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC 	[PersonType] [nchar](2) NOT NULL,
-- MAGIC     [NameStyle] [NameStyle] NOT NULL CONSTRAINT [DF_Person_NameStyle] DEFAULT (0),
-- MAGIC     [Title] [nvarchar](8) NULL, 
-- MAGIC     [FirstName] [Name] NOT NULL,
-- MAGIC     [MiddleName] [Name] NULL,
-- MAGIC     [LastName] [Name] NOT NULL,
-- MAGIC     [Suffix] [nvarchar](10) NULL, 
-- MAGIC     [EmailPromotion] [int] NOT NULL CONSTRAINT [DF_Person_EmailPromotion] DEFAULT (0), 
-- MAGIC     [AdditionalContactInfo] [XML]([Person].[AdditionalContactInfoSchemaCollection]) NULL,
-- MAGIC     [Demographics] [XML]([Person].[IndividualSurveySchemaCollection]) NULL, 
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_Person_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Person_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_Person_EmailPromotion] CHECK ([EmailPromotion] BETWEEN 0 AND 2),
-- MAGIC     CONSTRAINT [CK_Person_PersonType] CHECK ([PersonType] IS NULL OR UPPER([PersonType]) IN ('SC', 'VC', 'IN', 'EM', 'SP', 'GC'))
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[PersonCreditCard](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC     [CreditCardID] [int] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_PersonCreditCard_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Person].[PersonPhone](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC 	[PhoneNumber] [Phone] NOT NULL,
-- MAGIC 	[PhoneNumberTypeID] [int] NOT NULL,
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_PersonPhone_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Person].[PhoneNumberType](
-- MAGIC 	[PhoneNumberTypeID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC 	[Name] [Name] NOT NULL,
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_PhoneNumberType_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[Product](
-- MAGIC     [ProductID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [ProductNumber] [nvarchar](25) NOT NULL, 
-- MAGIC     [MakeFlag] [Flag] NOT NULL CONSTRAINT [DF_Product_MakeFlag] DEFAULT (1),
-- MAGIC     [FinishedodsFlag] [Flag] NOT NULL CONSTRAINT [DF_Product_FinishedodsFlag] DEFAULT (1),
-- MAGIC     [Color] [nvarchar](15) NULL, 
-- MAGIC     [SafetyStockLevel] [smallint] NOT NULL,
-- MAGIC     [ReorderPoint] [smallint] NOT NULL,
-- MAGIC     [StandardCost] [money] NOT NULL,
-- MAGIC     [ListPrice] [money] NOT NULL,
-- MAGIC     [Size] [nvarchar](5) NULL, 
-- MAGIC     [SizeUnitMeasureCode] [nchar](3) NULL, 
-- MAGIC     [WeightUnitMeasureCode] [nchar](3) NULL, 
-- MAGIC     [Weight] [decimal](8, 2) NULL,
-- MAGIC     [DaysToManufacture] [int] NOT NULL,
-- MAGIC     [ProductLine] [nchar](2) NULL, 
-- MAGIC     [Class] [nchar](2) NULL, 
-- MAGIC     [Style] [nchar](2) NULL, 
-- MAGIC     [ProductSubcateryID] [int] NULL,
-- MAGIC     [ProductModelID] [int] NULL,
-- MAGIC     [SellStartDate] [datetime] NOT NULL,
-- MAGIC     [SellEndDate] [datetime] NULL,
-- MAGIC     [DiscontinuedDate] [datetime] NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_Product_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Product_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_Product_SafetyStockLevel] CHECK ([SafetyStockLevel] > 0),
-- MAGIC     CONSTRAINT [CK_Product_ReorderPoint] CHECK ([ReorderPoint] > 0),
-- MAGIC     CONSTRAINT [CK_Product_StandardCost] CHECK ([StandardCost] >= 0.00),
-- MAGIC     CONSTRAINT [CK_Product_ListPrice] CHECK ([ListPrice] >= 0.00),
-- MAGIC     CONSTRAINT [CK_Product_Weight] CHECK ([Weight] > 0.00),
-- MAGIC     CONSTRAINT [CK_Product_DaysToManufacture] CHECK ([DaysToManufacture] >= 0),
-- MAGIC     CONSTRAINT [CK_Product_ProductLine] CHECK (UPPER([ProductLine]) IN ('S', 'T', 'M', 'R') OR [ProductLine] IS NULL),
-- MAGIC     CONSTRAINT [CK_Product_Class] CHECK (UPPER([Class]) IN ('L', 'M', 'H') OR [Class] IS NULL),
-- MAGIC     CONSTRAINT [CK_Product_Style] CHECK (UPPER([Style]) IN ('W', 'M', 'U') OR [Style] IS NULL), 
-- MAGIC     CONSTRAINT [CK_Product_SellEndDate] CHECK (([SellEndDate] >= [SellStartDate]) OR ([SellEndDate] IS NULL)),
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductCatery](
-- MAGIC     [ProductCateryID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_ProductCatery_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductCatery_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductCostHistory](
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [StartDate] [datetime] NOT NULL,
-- MAGIC     [EndDate] [datetime] NULL,
-- MAGIC     [StandardCost] [money] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductCostHistory_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_ProductCostHistory_EndDate] CHECK (([EndDate] >= [StartDate]) OR ([EndDate] IS NULL)),
-- MAGIC     CONSTRAINT [CK_ProductCostHistory_StandardCost] CHECK ([StandardCost] >= 0.00)
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductDescription](
-- MAGIC     [ProductDescriptionID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Description] [nvarchar](400) NOT NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_ProductDescription_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductDescription_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductDocument](
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [DocumentNode] [hierarchyid] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductDocument_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductInventory](
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [LocationID] [smallint] NOT NULL,
-- MAGIC     [Shelf] [nvarchar](10) NOT NULL, 
-- MAGIC     [Bin] [tinyint] NOT NULL,
-- MAGIC     [Quantity] [smallint] NOT NULL CONSTRAINT [DF_ProductInventory_Quantity] DEFAULT (0),
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_ProductInventory_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductInventory_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_ProductInventory_Shelf] CHECK (([Shelf] LIKE '[A-Za-z]') OR ([Shelf] = 'N/A')),
-- MAGIC     CONSTRAINT [CK_ProductInventory_Bin] CHECK ([Bin] BETWEEN 0 AND 100)
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductListPriceHistory](
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [StartDate] [datetime] NOT NULL,
-- MAGIC     [EndDate] [datetime] NULL,
-- MAGIC     [ListPrice] [money] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductListPriceHistory_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_ProductListPriceHistory_EndDate] CHECK (([EndDate] >= [StartDate]) OR ([EndDate] IS NULL)),
-- MAGIC     CONSTRAINT [CK_ProductListPriceHistory_ListPrice] CHECK ([ListPrice] > 0.00)
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductModel](
-- MAGIC     [ProductModelID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [CatalogDescription] [XML]([Production].[ProductDescriptionSchemaCollection]) NULL,
-- MAGIC     [Instructions] [XML]([Production].[ManuInstructionsSchemaCollection]) NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_ProductModel_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductModel_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductModelIllustration](
-- MAGIC     [ProductModelID] [int] NOT NULL,
-- MAGIC     [IllustrationID] [int] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductModelIllustration_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductModelProductDescriptionCulture](
-- MAGIC     [ProductModelID] [int] NOT NULL,
-- MAGIC     [ProductDescriptionID] [int] NOT NULL,
-- MAGIC     [CultureID] [nchar](6) NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductModelProductDescriptionCulture_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductPhoto](
-- MAGIC     [ProductPhotoID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [ThumbNailPhoto] [varbinary](max) NULL,
-- MAGIC     [ThumbnailPhotoFileName] [nvarchar](50) NULL,
-- MAGIC     [LargePhoto] [varbinary](max) NULL,
-- MAGIC     [LargePhotoFileName] [nvarchar](50) NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductPhoto_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductProductPhoto](
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [ProductPhotoID] [int] NOT NULL,
-- MAGIC     [Primary] [Flag] NOT NULL CONSTRAINT [DF_ProductProductPhoto_Primary] DEFAULT (0),
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductProductPhoto_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductReview](
-- MAGIC     [ProductReviewID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [ReviewerName] [Name] NOT NULL,
-- MAGIC     [ReviewDate] [datetime] NOT NULL CONSTRAINT [DF_ProductReview_ReviewDate] DEFAULT (GETDATE()),
-- MAGIC     [EmailAddress] [nvarchar](50) NOT NULL,
-- MAGIC     [Rating] [int] NOT NULL,
-- MAGIC     [Comments] [nvarchar](3850), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductReview_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_ProductReview_Rating] CHECK ([Rating] BETWEEN 1 AND 5), 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ProductSubcatery](
-- MAGIC     [ProductSubcateryID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [ProductCateryID] [int] NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_ProductSubcatery_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductSubcatery_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Purchasing].[ProductVendor](
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC     [AverageLeadTime] [int] NOT NULL,
-- MAGIC     [StandardPrice] [money] NOT NULL,
-- MAGIC     [LastReceiptCost] [money] NULL,
-- MAGIC     [LastReceiptDate] [datetime] NULL,
-- MAGIC     [MinOrderQty] [int] NOT NULL,
-- MAGIC     [MaxOrderQty] [int] NOT NULL,
-- MAGIC     [OnOrderQty] [int] NULL,
-- MAGIC     [UnitMeasureCode] [nchar](3) NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductVendor_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_ProductVendor_AverageLeadTime] CHECK ([AverageLeadTime] >= 1),
-- MAGIC     CONSTRAINT [CK_ProductVendor_StandardPrice] CHECK ([StandardPrice] > 0.00),
-- MAGIC     CONSTRAINT [CK_ProductVendor_LastReceiptCost] CHECK ([LastReceiptCost] > 0.00),
-- MAGIC     CONSTRAINT [CK_ProductVendor_MinOrderQty] CHECK ([MinOrderQty] >= 1),
-- MAGIC     CONSTRAINT [CK_ProductVendor_MaxOrderQty] CHECK ([MaxOrderQty] >= 1),
-- MAGIC     CONSTRAINT [CK_ProductVendor_OnOrderQty] CHECK ([OnOrderQty] >= 0)
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Purchasing].[PurchaseOrderDetail](
-- MAGIC     [PurchaseOrderID] [int] NOT NULL,
-- MAGIC     [PurchaseOrderDetailID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [DueDate] [datetime] NOT NULL,
-- MAGIC     [OrderQty] [smallint] NOT NULL,
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [UnitPrice] [money] NOT NULL,
-- MAGIC     [LineTotal] AS ISNULL([OrderQty] * [UnitPrice], 0.00), 
-- MAGIC     [ReceivedQty] [decimal](8, 2) NOT NULL,
-- MAGIC     [RejectedQty] [decimal](8, 2) NOT NULL,
-- MAGIC     [StockedQty] AS ISNULL([ReceivedQty] - [RejectedQty], 0.00),
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_PurchaseOrderDetail_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_PurchaseOrderDetail_OrderQty] CHECK ([OrderQty] > 0), 
-- MAGIC     CONSTRAINT [CK_PurchaseOrderDetail_UnitPrice] CHECK ([UnitPrice] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_PurchaseOrderDetail_ReceivedQty] CHECK ([ReceivedQty] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_PurchaseOrderDetail_RejectedQty] CHECK ([RejectedQty] >= 0.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Purchasing].[PurchaseOrderHeader](
-- MAGIC     [PurchaseOrderID] [int] IDENTITY (1, 1) NOT NULL, 
-- MAGIC     [RevisionNumber] [tinyint] NOT NULL CONSTRAINT [DF_PurchaseOrderHeader_RevisionNumber] DEFAULT (0), 
-- MAGIC     [Status] [tinyint] NOT NULL CONSTRAINT [DF_PurchaseOrderHeader_Status] DEFAULT (1), 
-- MAGIC     [EmployeeID] [int] NOT NULL, 
-- MAGIC     [VendorID] [int] NOT NULL, 
-- MAGIC     [ShipMethodID] [int] NOT NULL, 
-- MAGIC     [OrderDate] [datetime] NOT NULL CONSTRAINT [DF_PurchaseOrderHeader_OrderDate] DEFAULT (GETDATE()), 
-- MAGIC     [ShipDate] [datetime] NULL, 
-- MAGIC     [SubTotal] [money] NOT NULL CONSTRAINT [DF_PurchaseOrderHeader_SubTotal] DEFAULT (0.00), 
-- MAGIC     [TaxAmt] [money] NOT NULL CONSTRAINT [DF_PurchaseOrderHeader_TaxAmt] DEFAULT (0.00), 
-- MAGIC     [Freight] [money] NOT NULL CONSTRAINT [DF_PurchaseOrderHeader_Freight] DEFAULT (0.00), 
-- MAGIC     [TotalDue] AS ISNULL([SubTotal] + [TaxAmt] + [Freight], 0) PERSISTED NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_PurchaseOrderHeader_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_PurchaseOrderHeader_Status] CHECK ([Status] BETWEEN 1 AND 4), -- 1 = Pending; 2 = Approved; 3 = Rejected; 4 = Complete 
-- MAGIC     CONSTRAINT [CK_PurchaseOrderHeader_ShipDate] CHECK (([ShipDate] >= [OrderDate]) OR ([ShipDate] IS NULL)), 
-- MAGIC     CONSTRAINT [CK_PurchaseOrderHeader_SubTotal] CHECK ([SubTotal] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_PurchaseOrderHeader_TaxAmt] CHECK ([TaxAmt] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_PurchaseOrderHeader_Freight] CHECK ([Freight] >= 0.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SalesOrderDetail](
-- MAGIC     [SalesOrderID] [int] NOT NULL,
-- MAGIC     [SalesOrderDetailID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [CarrierTrackingNumber] [nvarchar](25) NULL, 
-- MAGIC     [OrderQty] [smallint] NOT NULL,
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [SpecialOfferID] [int] NOT NULL,
-- MAGIC     [UnitPrice] [money] NOT NULL,
-- MAGIC     [UnitPriceDiscount] [money] NOT NULL CONSTRAINT [DF_SalesOrderDetail_UnitPriceDiscount] DEFAULT (0.0),
-- MAGIC     [LineTotal] AS ISNULL([UnitPrice] * (1.0 - [UnitPriceDiscount]) * [OrderQty], 0.0),
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_SalesOrderDetail_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesOrderDetail_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_SalesOrderDetail_OrderQty] CHECK ([OrderQty] > 0), 
-- MAGIC     CONSTRAINT [CK_SalesOrderDetail_UnitPrice] CHECK ([UnitPrice] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesOrderDetail_UnitPriceDiscount] CHECK ([UnitPriceDiscount] >= 0.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SalesOrderHeader](
-- MAGIC     [SalesOrderID] [int] IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
-- MAGIC     [RevisionNumber] [tinyint] NOT NULL CONSTRAINT [DF_SalesOrderHeader_RevisionNumber] DEFAULT (0),
-- MAGIC     [OrderDate] [datetime] NOT NULL CONSTRAINT [DF_SalesOrderHeader_OrderDate] DEFAULT (GETDATE()),
-- MAGIC     [DueDate] [datetime] NOT NULL,
-- MAGIC     [ShipDate] [datetime] NULL,
-- MAGIC     [Status] [tinyint] NOT NULL CONSTRAINT [DF_SalesOrderHeader_Status] DEFAULT (1),
-- MAGIC     [OnlineOrderFlag] [Flag] NOT NULL CONSTRAINT [DF_SalesOrderHeader_OnlineOrderFlag] DEFAULT (1),
-- MAGIC     [SalesOrderNumber] AS ISNULL(N'SO' + CONVERT(nvarchar(23), [SalesOrderID]), N'*** ERROR ***'), 
-- MAGIC     [PurchaseOrderNumber] [OrderNumber] NULL,
-- MAGIC     [AccountNumber] [AccountNumber] NULL,
-- MAGIC     [CustomerID] [int] NOT NULL,
-- MAGIC     [SalesPersonID] [int] NULL,
-- MAGIC     [TerritoryID] [int] NULL,
-- MAGIC     [BillToAddressID] [int] NOT NULL,
-- MAGIC     [ShipToAddressID] [int] NOT NULL,
-- MAGIC     [ShipMethodID] [int] NOT NULL,
-- MAGIC     [CreditCardID] [int] NULL,
-- MAGIC     [CreditCardApprovalCode] [varchar](15) NULL,    
-- MAGIC     [CurrencyRateID] [int] NULL,
-- MAGIC     [SubTotal] [money] NOT NULL CONSTRAINT [DF_SalesOrderHeader_SubTotal] DEFAULT (0.00),
-- MAGIC     [TaxAmt] [money] NOT NULL CONSTRAINT [DF_SalesOrderHeader_TaxAmt] DEFAULT (0.00),
-- MAGIC     [Freight] [money] NOT NULL CONSTRAINT [DF_SalesOrderHeader_Freight] DEFAULT (0.00),
-- MAGIC     [TotalDue] AS ISNULL([SubTotal] + [TaxAmt] + [Freight], 0),
-- MAGIC     [Comment] [nvarchar](128) NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_SalesOrderHeader_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesOrderHeader_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_SalesOrderHeader_Status] CHECK ([Status] BETWEEN 0 AND 8), 
-- MAGIC     CONSTRAINT [CK_SalesOrderHeader_DueDate] CHECK ([DueDate] >= [OrderDate]), 
-- MAGIC     CONSTRAINT [CK_SalesOrderHeader_ShipDate] CHECK (([ShipDate] >= [OrderDate]) OR ([ShipDate] IS NULL)), 
-- MAGIC     CONSTRAINT [CK_SalesOrderHeader_SubTotal] CHECK ([SubTotal] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesOrderHeader_TaxAmt] CHECK ([TaxAmt] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesOrderHeader_Freight] CHECK ([Freight] >= 0.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SalesOrderHeaderSalesReason](
-- MAGIC     [SalesOrderID] [int] NOT NULL,
-- MAGIC     [SalesReasonID] [int] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesOrderHeaderSalesReason_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SalesPerson](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC     [TerritoryID] [int] NULL,
-- MAGIC     [SalesQuota] [money] NULL,
-- MAGIC     [Bonus] [money] NOT NULL CONSTRAINT [DF_SalesPerson_Bonus] DEFAULT (0.00),
-- MAGIC     [CommissionPct] [smallmoney] NOT NULL CONSTRAINT [DF_SalesPerson_CommissionPct] DEFAULT (0.00),
-- MAGIC     [SalesYTD] [money] NOT NULL CONSTRAINT [DF_SalesPerson_SalesYTD] DEFAULT (0.00),
-- MAGIC     [SalesLastYear] [money] NOT NULL CONSTRAINT [DF_SalesPerson_SalesLastYear] DEFAULT (0.00),
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_SalesPerson_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesPerson_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_SalesPerson_SalesQuota] CHECK ([SalesQuota] > 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesPerson_Bonus] CHECK ([Bonus] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesPerson_CommissionPct] CHECK ([CommissionPct] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesPerson_SalesYTD] CHECK ([SalesYTD] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesPerson_SalesLastYear] CHECK ([SalesLastYear] >= 0.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SalesPersonQuotaHistory](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC     [QuotaDate] [datetime] NOT NULL,
-- MAGIC     [SalesQuota] [money] NOT NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_SalesPersonQuotaHistory_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesPersonQuotaHistory_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_SalesPersonQuotaHistory_SalesQuota] CHECK ([SalesQuota] > 0.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SalesReason](
-- MAGIC     [SalesReasonID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [ReasonType] [Name] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesReason_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SalesTaxRate](
-- MAGIC     [SalesTaxRateID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [StateProvinceID] [int] NOT NULL,
-- MAGIC     [TaxType] [tinyint] NOT NULL,
-- MAGIC     [TaxRate] [smallmoney] NOT NULL CONSTRAINT [DF_SalesTaxRate_TaxRate] DEFAULT (0.00),
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_SalesTaxRate_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesTaxRate_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_SalesTaxRate_TaxType] CHECK ([TaxType] BETWEEN 1 AND 3)
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SalesTerritory](
-- MAGIC     [TerritoryID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [CountryRegionCode] [nvarchar](3) NOT NULL, 
-- MAGIC     [Group] [nvarchar](50) NOT NULL,
-- MAGIC     [SalesYTD] [money] NOT NULL CONSTRAINT [DF_SalesTerritory_SalesYTD] DEFAULT (0.00),
-- MAGIC     [SalesLastYear] [money] NOT NULL CONSTRAINT [DF_SalesTerritory_SalesLastYear] DEFAULT (0.00),
-- MAGIC     [CostYTD] [money] NOT NULL CONSTRAINT [DF_SalesTerritory_CostYTD] DEFAULT (0.00),
-- MAGIC     [CostLastYear] [money] NOT NULL CONSTRAINT [DF_SalesTerritory_CostLastYear] DEFAULT (0.00),
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_SalesTerritory_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesTerritory_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_SalesTerritory_SalesYTD] CHECK ([SalesYTD] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesTerritory_SalesLastYear] CHECK ([SalesLastYear] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesTerritory_CostYTD] CHECK ([CostYTD] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SalesTerritory_CostLastYear] CHECK ([CostLastYear] >= 0.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SalesTerritoryHistory](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,  -- A sales person
-- MAGIC     [TerritoryID] [int] NOT NULL,
-- MAGIC     [StartDate] [datetime] NOT NULL,
-- MAGIC     [EndDate] [datetime] NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_SalesTerritoryHistory_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesTerritoryHistory_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_SalesTerritoryHistory_EndDate] CHECK (([EndDate] >= [StartDate]) OR ([EndDate] IS NULL))
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[ScrapReason](
-- MAGIC     [ScrapReasonID] [smallint] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ScrapReason_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [HumanResources].[Shift](
-- MAGIC     [ShiftID] [tinyint] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [StartTime] [time] NOT NULL,
-- MAGIC     [EndTime] [time] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Shift_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Purchasing].[ShipMethod](
-- MAGIC     [ShipMethodID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [ShipBase] [money] NOT NULL CONSTRAINT [DF_ShipMethod_ShipBase] DEFAULT (0.00),
-- MAGIC     [ShipRate] [money] NOT NULL CONSTRAINT [DF_ShipMethod_ShipRate] DEFAULT (0.00),
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_ShipMethod_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ShipMethod_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_ShipMethod_ShipBase] CHECK ([ShipBase] > 0.00), 
-- MAGIC     CONSTRAINT [CK_ShipMethod_ShipRate] CHECK ([ShipRate] > 0.00), 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[ShoppingCartItem](
-- MAGIC     [ShoppingCartItemID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [ShoppingCartID] [nvarchar](50) NOT NULL,
-- MAGIC     [Quantity] [int] NOT NULL CONSTRAINT [DF_ShoppingCartItem_Quantity] DEFAULT (1),
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [DateCreated] [datetime] NOT NULL CONSTRAINT [DF_ShoppingCartItem_DateCreated] DEFAULT (GETDATE()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ShoppingCartItem_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_ShoppingCartItem_Quantity] CHECK ([Quantity] >= 1) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SpecialOffer](
-- MAGIC     [SpecialOfferID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [Description] [nvarchar](255) NOT NULL,
-- MAGIC     [DiscountPct] [smallmoney] NOT NULL CONSTRAINT [DF_SpecialOffer_DiscountPct] DEFAULT (0.00),
-- MAGIC     [Type] [nvarchar](50) NOT NULL,
-- MAGIC     [Catery] [nvarchar](50) NOT NULL,
-- MAGIC     [StartDate] [datetime] NOT NULL,
-- MAGIC     [EndDate] [datetime] NOT NULL,
-- MAGIC     [MinQty] [int] NOT NULL CONSTRAINT [DF_SpecialOffer_MinQty] DEFAULT (0), 
-- MAGIC     [MaxQty] [int] NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_SpecialOffer_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SpecialOffer_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_SpecialOffer_EndDate] CHECK ([EndDate] >= [StartDate]), 
-- MAGIC     CONSTRAINT [CK_SpecialOffer_DiscountPct] CHECK ([DiscountPct] >= 0.00), 
-- MAGIC     CONSTRAINT [CK_SpecialOffer_MinQty] CHECK ([MinQty] >= 0), 
-- MAGIC     CONSTRAINT [CK_SpecialOffer_MaxQty]  CHECK ([MaxQty] >= 0)
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[SpecialOfferProduct](
-- MAGIC     [SpecialOfferID] [int] NOT NULL,
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_SpecialOfferProduct_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SpecialOfferProduct_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Person].[StateProvince](
-- MAGIC     [StateProvinceID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [StateProvinceCode] [nchar](3) NOT NULL, 
-- MAGIC     [CountryRegionCode] [nvarchar](3) NOT NULL, 
-- MAGIC     [IsOnlyStateProvinceFlag] [Flag] NOT NULL CONSTRAINT [DF_StateProvince_IsOnlyStateProvinceFlag] DEFAULT (1),
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [TerritoryID] [int] NOT NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_StateProvince_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_StateProvince_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Sales].[Store](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [SalesPersonID] [int] NULL,
-- MAGIC     [Demographics] [XML]([Sales].[StoreSurveySchemaCollection]) NULL,
-- MAGIC     [rowguid] uniqueidentifier ROWGUIDCOL NOT NULL CONSTRAINT [DF_Store_rowguid] DEFAULT (NEWID()), 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Store_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[TransactionHistory](
-- MAGIC     [TransactionID] [int] IDENTITY (100000, 1) NOT NULL,
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [ReferenceOrderID] [int] NOT NULL,
-- MAGIC     [ReferenceOrderLineID] [int] NOT NULL CONSTRAINT [DF_TransactionHistory_ReferenceOrderLineID] DEFAULT (0),
-- MAGIC     [TransactionDate] [datetime] NOT NULL CONSTRAINT [DF_TransactionHistory_TransactionDate] DEFAULT (GETDATE()),
-- MAGIC     [TransactionType] [nchar](1) NOT NULL, 
-- MAGIC     [Quantity] [int] NOT NULL,
-- MAGIC     [ActualCost] [money] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_TransactionHistory_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_TransactionHistory_TransactionType] CHECK (UPPER([TransactionType]) IN ('W', 'S', 'P'))
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[TransactionHistoryArchive](
-- MAGIC     [TransactionID] [int] NOT NULL,
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [ReferenceOrderID] [int] NOT NULL,
-- MAGIC     [ReferenceOrderLineID] [int] NOT NULL CONSTRAINT [DF_TransactionHistoryArchive_ReferenceOrderLineID] DEFAULT (0),
-- MAGIC     [TransactionDate] [datetime] NOT NULL CONSTRAINT [DF_TransactionHistoryArchive_TransactionDate] DEFAULT (GETDATE()),
-- MAGIC     [TransactionType] [nchar](1) NOT NULL, 
-- MAGIC     [Quantity] [int] NOT NULL,
-- MAGIC     [ActualCost] [money] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_TransactionHistoryArchive_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_TransactionHistoryArchive_TransactionType] CHECK (UPPER([TransactionType]) IN ('W', 'S', 'P'))
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[UnitMeasure](
-- MAGIC     [UnitMeasureCode] [nchar](3) NOT NULL, 
-- MAGIC     [Name] [Name] NOT NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_UnitMeasure_ModifiedDate] DEFAULT (GETDATE()) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Purchasing].[Vendor](
-- MAGIC     [BusinessEntityID] [int] NOT NULL,
-- MAGIC     [AccountNumber] [AccountNumber] NOT NULL,
-- MAGIC     [Name] [Name] NOT NULL,
-- MAGIC     [CreditRating] [tinyint] NOT NULL,
-- MAGIC     [PreferredVendorStatus] [Flag] NOT NULL CONSTRAINT [DF_Vendor_PreferredVendorStatus] DEFAULT (1), 
-- MAGIC     [ActiveFlag] [Flag] NOT NULL CONSTRAINT [DF_Vendor_ActiveFlag] DEFAULT (1),
-- MAGIC     [PurchasingWebServiceURL] [nvarchar](1024) NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Vendor_ModifiedDate] DEFAULT (GETDATE()),
-- MAGIC     CONSTRAINT [CK_Vendor_CreditRating] CHECK ([CreditRating] BETWEEN 1 AND 5)
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[WorkOrder](
-- MAGIC     [WorkOrderID] [int] IDENTITY (1, 1) NOT NULL,
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [OrderQty] [int] NOT NULL,
-- MAGIC     [StockedQty] AS ISNULL([OrderQty] - [ScrappedQty], 0),
-- MAGIC     [ScrappedQty] [smallint] NOT NULL,
-- MAGIC     [StartDate] [datetime] NOT NULL,
-- MAGIC     [EndDate] [datetime] NULL,
-- MAGIC     [DueDate] [datetime] NOT NULL,
-- MAGIC     [ScrapReasonID] [smallint] NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_WorkOrder_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_WorkOrder_OrderQty] CHECK ([OrderQty] > 0), 
-- MAGIC     CONSTRAINT [CK_WorkOrder_ScrappedQty] CHECK ([ScrappedQty] >= 0), 
-- MAGIC     CONSTRAINT [CK_WorkOrder_EndDate] CHECK (([EndDate] >= [StartDate]) OR ([EndDate] IS NULL))
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE [Production].[WorkOrderRouting](
-- MAGIC     [WorkOrderID] [int] NOT NULL,
-- MAGIC     [ProductID] [int] NOT NULL,
-- MAGIC     [OperationSequence] [smallint] NOT NULL,
-- MAGIC     [LocationID] [smallint] NOT NULL,
-- MAGIC     [ScheduledStartDate] [datetime] NOT NULL,
-- MAGIC     [ScheduledEndDate] [datetime] NOT NULL,
-- MAGIC     [ActualStartDate] [datetime] NULL,
-- MAGIC     [ActualEndDate] [datetime] NULL,
-- MAGIC     [ActualResourceHrs] [decimal](9, 4) NULL,
-- MAGIC     [PlannedCost] [money] NOT NULL,
-- MAGIC     [ActualCost] [money] NULL, 
-- MAGIC     [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_WorkOrderRouting_ModifiedDate] DEFAULT (GETDATE()), 
-- MAGIC     CONSTRAINT [CK_WorkOrderRouting_ScheduledEndDate] CHECK ([ScheduledEndDate] >= [ScheduledStartDate]), 
-- MAGIC     CONSTRAINT [CK_WorkOrderRouting_ActualEndDate] CHECK (([ActualEndDate] >= [ActualStartDate]) 
-- MAGIC         OR ([ActualEndDate] IS NULL) OR ([ActualStartDate] IS NULL)), 
-- MAGIC     CONSTRAINT [CK_WorkOrderRouting_ActualResourceHrs] CHECK ([ActualResourceHrs] >= 0.0000), 
-- MAGIC     CONSTRAINT [CK_WorkOrderRouting_PlannedCost] CHECK ([PlannedCost] > 0.00), 
-- MAGIC     CONSTRAINT [CK_WorkOrderRouting_ActualCost] CHECK ([ActualCost] > 0.00) 
-- MAGIC ) ON [PRIMARY];
-- MAGIC 
-- MAGIC """

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC tables_sql = all_sql.split(";")
-- MAGIC print(tables_sql[0])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for t in tables_sql:
-- MAGIC   parse_sql(t)

-- COMMAND ----------


