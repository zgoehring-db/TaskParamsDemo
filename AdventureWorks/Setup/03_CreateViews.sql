-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Views

-- COMMAND ----------

CREATE WIDGET TEXT DatabaseNamePrefix DEFAULT '';
CREATE WIDGET TEXT UserName DEFAULT '';

-- COMMAND ----------

SET var.database_name_prefix = $DatabaseNamePrefix ; 
SET var.database_name = ${var.database_name_prefix}_adventureworks_metadata;
SET var.user_name = $UserName ; 
SET var.table_location = '/users/${var.user_name}/databases/${var.database_name_prefix}_adventureworks_metadata' ;


USE ${var.database_name} -- use the metadatabase by default;

-- COMMAND ----------

-- CREATE VIEW [HumanResources].[vEmployee] 
-- AS 
-- SELECT 
--     e.[BusinessEntityID]
--     ,p.[Title]
--     ,p.[FirstName]
--     ,p.[MiddleName]
--     ,p.[LastName]
--     ,p.[Suffix]
--     ,e.[JobTitle]  
--     ,pp.[PhoneNumber]
--     ,pnt.[Name] AS [PhoneNumberType]
--     ,ea.[EmailAddress]
--     ,p.[EmailPromotion]
--     ,a.[AddressLine1]
--     ,a.[AddressLine2]
--     ,a.[City]
--     ,sp.[Name] AS [StateProvinceName] 
--     ,a.[PostalCode]
--     ,cr.[Name] AS [CountryRegionName] 
--     ,p.[AdditionalContactInfo]
-- FROM [HumanResources].[Employee] e
--     INNER JOIN [Person].[Person] p
--     ON p.[BusinessEntityID] = e.[BusinessEntityID]
--     INNER JOIN [Person].[BusinessEntityAddress] bea 
--     ON bea.[BusinessEntityID] = e.[BusinessEntityID] 
--     INNER JOIN [Person].[Address] a 
--     ON a.[AddressID] = bea.[AddressID]
--     INNER JOIN [Person].[StateProvince] sp 
--     ON sp.[StateProvinceID] = a.[StateProvinceID]
--     INNER JOIN [Person].[CountryRegion] cr 
--     ON cr.[CountryRegionCode] = sp.[CountryRegionCode]
--     LEFT OUTER JOIN [Person].[PersonPhone] pp
--     ON pp.BusinessEntityID = p.[BusinessEntityID]
--     LEFT OUTER JOIN [Person].[PhoneNumberType] pnt
--     ON pp.[PhoneNumberTypeID] = pnt.[PhoneNumberTypeID]
--     LEFT OUTER JOIN [Person].[EmailAddress] ea
--     ON p.[BusinessEntityID] = ea.[BusinessEntityID];
-- GO



CREATE OR REPLACE VIEW ${var.database_name_prefix}_HumanResources.vEmployee 
AS 
SELECT 
    e.BusinessEntityID
    ,p.Title
    ,p.FirstName
    ,p.MiddleName
    ,p.LastName
    ,p.Suffix
    ,e.JobTitle  
    ,pp.PhoneNumber
    ,pnt.Name AS PhoneNumberType
    ,ea.EmailAddress
    ,p.EmailPromotion
    ,a.AddressLine1
    ,a.AddressLine2
    ,a.City
    ,sp.Name AS StateProvinceName 
    ,a.PostalCode
    ,cr.Name AS CountryRegionName 
    ,p.AdditionalContactInfo
FROM HumanResources.Employee e
    INNER JOIN ${var.database_name_prefix}_Person.Person p ON p.BusinessEntityID = e.BusinessEntityID
    INNER JOIN ${var.database_name_prefix}_Person.BusinessEntityAddress bea ON bea.BusinessEntityID = e.BusinessEntityID 
    INNER JOIN ${var.database_name_prefix}_Person.Address a ON a.AddressID = bea.AddressID
    INNER JOIN ${var.database_name_prefix}_Person.StateProvince sp ON sp.StateProvinceID = a.StateProvinceID
    INNER JOIN ${var.database_name_prefix}_Person.CountryRegion cr ON cr.CountryRegionCode = sp.CountryRegionCode
    LEFT OUTER JOIN ${var.database_name_prefix}_Person.PersonPhone pp ON pp.BusinessEntityID = p.BusinessEntityID
    LEFT OUTER JOIN ${var.database_name_prefix}_Person.PhoneNumberType pnt ON pp.PhoneNumberTypeID = pnt.PhoneNumberTypeID
    LEFT OUTER JOIN ${var.database_name_prefix}_Person.EmailAddress ea ON p.BusinessEntityID = ea.BusinessEntityID

-- COMMAND ----------

SELECT * FROM ${var.database_name_prefix}_HumanResources.vEmployee LIMIT 100

-- COMMAND ----------

-- CREATE VIEW [HumanResources].[vEmployeeDepartment] 
-- AS 
-- SELECT 
--     e.[BusinessEntityID] 
--     ,p.[Title] 
--     ,p.[FirstName] 
--     ,p.[MiddleName] 
--     ,p.[LastName] 
--     ,p.[Suffix] 
--     ,e.[JobTitle]
--     ,d.[Name] AS [Department] 
--     ,d.[GroupName] 
--     ,edh.[StartDate] 
-- FROM [HumanResources].[Employee] e
--     INNER JOIN [Person].[Person] p
--     ON p.[BusinessEntityID] = e.[BusinessEntityID]
--     INNER JOIN [HumanResources].[EmployeeDepartmentHistory] edh 
--     ON e.[BusinessEntityID] = edh.[BusinessEntityID] 
--     INNER JOIN [HumanResources].[Department] d 
--     ON edh.[DepartmentID] = d.[DepartmentID] 
-- WHERE edh.EndDate IS NULL
-- GO


CREATE OR REPLACE VIEW ${var.database_name_prefix}_HumanResources.vEmployeeDepartment 
AS 
SELECT 
    e.BusinessEntityID 
    ,p.Title 
    ,p.FirstName 
    ,p.MiddleName 
    ,p.LastName 
    ,p.Suffix 
    ,e.JobTitle
    ,d.Name AS Department 
    ,d.GroupName 
    ,edh.StartDate 
FROM ${var.database_name_prefix}_HumanResources.Employee e
    INNER JOIN ${var.database_name_prefix}_Person.Person p
    ON p.BusinessEntityID = e.BusinessEntityID
    INNER JOIN ${var.database_name_prefix}_HumanResources.EmployeeDepartmentHistory edh 
    ON e.BusinessEntityID = edh.BusinessEntityID 
    INNER JOIN ${var.database_name_prefix}_HumanResources.Department d 
    ON edh.DepartmentID = d.DepartmentID 
WHERE edh.EndDate IS NULL

-- COMMAND ----------

-- CREATE VIEW [HumanResources].[vEmployeeDepartmentHistory] 
-- AS 
-- SELECT 
--     e.[BusinessEntityID] 
--     ,p.[Title] 
--     ,p.[FirstName] 
--     ,p.[MiddleName] 
--     ,p.[LastName] 
--     ,p.[Suffix] 
--     ,s.[Name] AS [Shift]
--     ,d.[Name] AS [Department] 
--     ,d.[GroupName] 
--     ,edh.[StartDate] 
--     ,edh.[EndDate]
-- FROM [HumanResources].[Employee] e
-- 	INNER JOIN [Person].[Person] p
-- 	ON p.[BusinessEntityID] = e.[BusinessEntityID]
--     INNER JOIN [HumanResources].[EmployeeDepartmentHistory] edh 
--     ON e.[BusinessEntityID] = edh.[BusinessEntityID] 
--     INNER JOIN [HumanResources].[Department] d 
--     ON edh.[DepartmentID] = d.[DepartmentID] 
--     INNER JOIN [HumanResources].[Shift] s
--     ON s.[ShiftID] = edh.[ShiftID];
-- GO



CREATE OR REPLACE VIEW ${var.database_name_prefix}_HumanResources.vEmployeeDepartmentHistory 
AS 
SELECT 
    e.BusinessEntityID 
    ,p.Title 
    ,p.FirstName 
    ,p.MiddleName 
    ,p.LastName 
    ,p.Suffix 
    ,s.Name AS Shift
    ,d.Name AS Department 
    ,d.GroupName 
    ,edh.StartDate 
    ,edh.EndDate
FROM ${var.database_name_prefix}_HumanResources.Employee e
	INNER JOIN ${var.database_name_prefix}_Person.Person p
	ON p.BusinessEntityID = e.BusinessEntityID
    INNER JOIN ${var.database_name_prefix}_HumanResources.EmployeeDepartmentHistory edh 
    ON e.BusinessEntityID = edh.BusinessEntityID 
    INNER JOIN ${var.database_name_prefix}_HumanResources.Department d 
    ON edh.DepartmentID = d.DepartmentID 
    INNER JOIN ${var.database_name_prefix}_HumanResources.Shift s
    ON s.ShiftID = edh.ShiftID;



-- COMMAND ----------

-- CREATE VIEW [Sales].[vIndividualCustomer] 
-- AS 
-- SELECT 
--     p.[BusinessEntityID]
--     ,p.[Title]
--     ,p.[FirstName]
--     ,p.[MiddleName]
--     ,p.[LastName]
--     ,p.[Suffix]
--     ,pp.[PhoneNumber]
-- 	,pnt.[Name] AS [PhoneNumberType]
--     ,ea.[EmailAddress]
--     ,p.[EmailPromotion]
--     ,at.[Name] AS [AddressType]
--     ,a.[AddressLine1]
--     ,a.[AddressLine2]
--     ,a.[City]
--     ,[StateProvinceName] = sp.[Name]
--     ,a.[PostalCode]
--     ,[CountryRegionName] = cr.[Name]
--     ,p.[Demographics]
-- FROM [Person].[Person] p
--     INNER JOIN [Person].[BusinessEntityAddress] bea 
--     ON bea.[BusinessEntityID] = p.[BusinessEntityID] 
--     INNER JOIN [Person].[Address] a 
--     ON a.[AddressID] = bea.[AddressID]
--     INNER JOIN [Person].[StateProvince] sp 
--     ON sp.[StateProvinceID] = a.[StateProvinceID]
--     INNER JOIN [Person].[CountryRegion] cr 
--     ON cr.[CountryRegionCode] = sp.[CountryRegionCode]
--     INNER JOIN [Person].[AddressType] at 
--     ON at.[AddressTypeID] = bea.[AddressTypeID]
-- 	INNER JOIN [Sales].[Customer] c
-- 	ON c.[PersonID] = p.[BusinessEntityID]
-- 	LEFT OUTER JOIN [Person].[EmailAddress] ea
-- 	ON ea.[BusinessEntityID] = p.[BusinessEntityID]
-- 	LEFT OUTER JOIN [Person].[PersonPhone] pp
-- 	ON pp.[BusinessEntityID] = p.[BusinessEntityID]
-- 	LEFT OUTER JOIN [Person].[PhoneNumberType] pnt
-- 	ON pnt.[PhoneNumberTypeID] = pp.[PhoneNumberTypeID]
-- WHERE c.StoreID IS NULL;


CREATE OR REPLACE VIEW ${var.database_name_prefix}_Sales.vIndividualCustomer 
AS 
SELECT 
    p.BusinessEntityID
    ,p.Title
    ,p.FirstName
    ,p.MiddleName
    ,p.LastName
    ,p.Suffix
    ,pp.PhoneNumber
	,pnt.Name AS PhoneNumberType
    ,ea.EmailAddress
    ,p.EmailPromotion
    ,at.Name AS AddressType
    ,a.AddressLine1
    ,a.AddressLine2
    ,a.City
    ,sp.Name as StateProvinceName
    ,a.PostalCode
    ,cr.Name as CountryRegionName
    ,p.Demographics
FROM ${var.database_name_prefix}_Person.Person p
    INNER JOIN ${var.database_name_prefix}_Person.BusinessEntityAddress bea 
    ON bea.BusinessEntityID = p.BusinessEntityID 
    INNER JOIN ${var.database_name_prefix}_Person.Address a 
    ON a.AddressID = bea.AddressID
    INNER JOIN ${var.database_name_prefix}_Person.StateProvince sp 
    ON sp.StateProvinceID = a.StateProvinceID
    INNER JOIN ${var.database_name_prefix}_Person.CountryRegion cr 
    ON cr.CountryRegionCode = sp.CountryRegionCode
    INNER JOIN ${var.database_name_prefix}_Person.AddressType at 
    ON at.AddressTypeID = bea.AddressTypeID
	INNER JOIN ${var.database_name_prefix}_Sales.Customer c
	ON c.PersonID = p.BusinessEntityID
	LEFT OUTER JOIN ${var.database_name_prefix}_Person.EmailAddress ea
	ON ea.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN ${var.database_name_prefix}_Person.PersonPhone pp
	ON pp.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN ${var.database_name_prefix}_Person.PhoneNumberType pnt
	ON pnt.PhoneNumberTypeID = pp.PhoneNumberTypeID
WHERE c.StoreID IS NULL;

-- COMMAND ----------

-- CREATE VIEW [Production].[vProductAndDescription] 
-- WITH SCHEMABINDING 
-- AS 
-- -- View (indexed or standard) to display products and product descriptions by language.
-- SELECT 
--     p.[ProductID] 
--     ,p.[Name] 
--     ,pm.[Name] AS [ProductModel] 
--     ,pmx.[CultureID] 
--     ,pd.[Description] 
-- FROM [Production].[Product] p 
--     INNER JOIN [Production].[ProductModel] pm 
--     ON p.[ProductModelID] = pm.[ProductModelID] 
--     INNER JOIN [Production].[ProductModelProductDescriptionCulture] pmx 
--     ON pm.[ProductModelID] = pmx.[ProductModelID] 
--     INNER JOIN [Production].[ProductDescription] pd 
--     ON pmx.[ProductDescriptionID] = pd.[ProductDescriptionID];

CREATE OR REPLACE VIEW ${var.database_name_prefix}_Production.vProductAndDescription 

AS 
-- View (indexed or standard) to display products and product descriptions by language.
SELECT 
    p.ProductID 
    ,p.Name 
    ,pm.Name AS ProductModel 
    ,pmx.CultureID 
    ,pd.Description 
FROM ${var.database_name_prefix}_Production.Product p 
    INNER JOIN ${var.database_name_prefix}_Production.ProductModel pm 
    ON p.ProductModelID = pm.ProductModelID 
    INNER JOIN ${var.database_name_prefix}_Production.ProductModelProductDescriptionCulture pmx 
    ON pm.ProductModelID = pmx.ProductModelID 
    INNER JOIN ${var.database_name_prefix}_Production.ProductDescription pd 
    ON pmx.ProductDescriptionID = pd.ProductDescriptionID;


-- COMMAND ----------

-- CREATE VIEW [Sales].[vSalesPerson] 
-- AS 
-- SELECT 
--     s.[BusinessEntityID]
--     ,p.[Title]
--     ,p.[FirstName]
--     ,p.[MiddleName]
--     ,p.[LastName]
--     ,p.[Suffix]
--     ,e.[JobTitle]
--     ,pp.[PhoneNumber]
-- 	,pnt.[Name] AS [PhoneNumberType]
--     ,ea.[EmailAddress]
--     ,p.[EmailPromotion]
--     ,a.[AddressLine1]
--     ,a.[AddressLine2]
--     ,a.[City]
--     ,[StateProvinceName] = sp.[Name]
--     ,a.[PostalCode]
--     ,[CountryRegionName] = cr.[Name]
--     ,[TerritoryName] = st.[Name]
--     ,[TerritoryGroup] = st.[Group]
--     ,s.[SalesQuota]
--     ,s.[SalesYTD]
--     ,s.[SalesLastYear]
-- FROM [Sales].[SalesPerson] s
--     INNER JOIN [HumanResources].[Employee] e 
--     ON e.[BusinessEntityID] = s.[BusinessEntityID]
-- 	INNER JOIN [Person].[Person] p
-- 	ON p.[BusinessEntityID] = s.[BusinessEntityID]
--     INNER JOIN [Person].[BusinessEntityAddress] bea 
--     ON bea.[BusinessEntityID] = s.[BusinessEntityID] 
--     INNER JOIN [Person].[Address] a 
--     ON a.[AddressID] = bea.[AddressID]
--     INNER JOIN [Person].[StateProvince] sp 
--     ON sp.[StateProvinceID] = a.[StateProvinceID]
--     INNER JOIN [Person].[CountryRegion] cr 
--     ON cr.[CountryRegionCode] = sp.[CountryRegionCode]
--     LEFT OUTER JOIN [Sales].[SalesTerritory] st 
--     ON st.[TerritoryID] = s.[TerritoryID]
-- 	LEFT OUTER JOIN [Person].[EmailAddress] ea
-- 	ON ea.[BusinessEntityID] = p.[BusinessEntityID]
-- 	LEFT OUTER JOIN [Person].[PersonPhone] pp
-- 	ON pp.[BusinessEntityID] = p.[BusinessEntityID]
-- 	LEFT OUTER JOIN [Person].[PhoneNumberType] pnt
-- 	ON pnt.[PhoneNumberTypeID] = pp.[PhoneNumberTypeID];

CREATE OR REPLACE VIEW ${var.database_name_prefix}_Sales.vSalesPerson 
AS 
SELECT 
    s.BusinessEntityID
    ,p.Title
    ,p.FirstName
    ,p.MiddleName
    ,p.LastName
    ,p.Suffix
    ,e.JobTitle
    ,pp.PhoneNumber
	,pnt.Name AS PhoneNumberType
    ,ea.EmailAddress
    ,p.EmailPromotion
    ,a.AddressLine1
    ,a.AddressLine2
    ,a.City
    ,sp.Name as StateProvinceName 
    ,a.PostalCode
    ,cr.Name as CountryRegionName 
    ,st.Name as TerritoryName 
    ,st.Group as TerritoryGroup
    ,s.SalesQuota
    ,s.SalesYTD
    ,s.SalesLastYear
FROM ${var.database_name_prefix}_Sales.SalesPerson s
    INNER JOIN ${var.database_name_prefix}_HumanResources.Employee e 
    ON e.BusinessEntityID = s.BusinessEntityID
	INNER JOIN ${var.database_name_prefix}_Person.Person p
	ON p.BusinessEntityID = s.BusinessEntityID
    INNER JOIN ${var.database_name_prefix}_Person.BusinessEntityAddress bea 
    ON bea.BusinessEntityID = s.BusinessEntityID 
    INNER JOIN ${var.database_name_prefix}_Person.Address a 
    ON a.AddressID = bea.AddressID
    INNER JOIN ${var.database_name_prefix}_Person.StateProvince sp 
    ON sp.StateProvinceID = a.StateProvinceID
    INNER JOIN ${var.database_name_prefix}_Person.CountryRegion cr 
    ON cr.CountryRegionCode = sp.CountryRegionCode
    LEFT OUTER JOIN ${var.database_name_prefix}_Sales.SalesTerritory st 
    ON st.TerritoryID = s.TerritoryID
	LEFT OUTER JOIN ${var.database_name_prefix}_Person.EmailAddress ea
	ON ea.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN ${var.database_name_prefix}_Person.PersonPhone pp
	ON pp.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN ${var.database_name_prefix}_Person.PhoneNumberType pnt
	ON pnt.PhoneNumberTypeID = pp.PhoneNumberTypeID;

-- COMMAND ----------

-- PIVOT EXAMPLE NOTEBOOK: https://docs.databricks.com/_static/notebooks/pivot-in-sql.html 

-- CREATE VIEW [Sales].[vSalesPersonSalesByFiscalYears] 
-- AS 
-- SELECT 
--     pvt.[SalesPersonID]
--     ,pvt.[FullName]
--     ,pvt.[JobTitle]
--     ,pvt.[SalesTerritory]
--     ,pvt.[2002]
--     ,pvt.[2003]
--     ,pvt.[2004] 
-- FROM (SELECT 
--         soh.[SalesPersonID]
--         ,p.[FirstName] + ' ' + COALESCE(p.[MiddleName], '') + ' ' + p.[LastName] AS [FullName]
--         ,e.[JobTitle]
--         ,st.[Name] AS [SalesTerritory]
--         ,soh.[SubTotal]
--         ,YEAR(DATEADD(m, 6, soh.[OrderDate])) AS [FiscalYear] 
--     FROM [Sales].[SalesPerson] sp 
--         INNER JOIN [Sales].[SalesOrderHeader] soh 
--         ON sp.[BusinessEntityID] = soh.[SalesPersonID]
--         INNER JOIN [Sales].[SalesTerritory] st 
--         ON sp.[TerritoryID] = st.[TerritoryID] 
--         INNER JOIN [HumanResources].[Employee] e 
--         ON soh.[SalesPersonID] = e.[BusinessEntityID] 
-- 		INNER JOIN [Person].[Person] p
-- 		ON p.[BusinessEntityID] = sp.[BusinessEntityID]
-- 	 ) AS soh 
-- PIVOT 
-- (
--     SUM([SubTotal]) 
--     FOR [FiscalYear] 
--     IN ([2002], [2003], [2004])
-- ) AS pvt;


CREATE OR REPLACE VIEW ${var.database_name_prefix}_Sales.vSalesPersonSalesByFiscalYears 
AS 
SELECT *
FROM (
    SELECT 
        soh.SalesPersonID
        ,p.FirstName + ' ' + COALESCE(p.MiddleName, '') + ' ' + p.LastName AS FullName
        ,e.JobTitle
        ,st.Name AS SalesTerritory
        ,soh.SubTotal
        ,YEAR(add_months(soh.OrderDate, 6)) AS FiscalYear  -- NOTE THIS CHANGE IN SQL FUNCTIONS BETWEEN TSQL AND DBSQL
    FROM ${var.database_name_prefix}_Sales.SalesPerson sp 
        INNER JOIN ${var.database_name_prefix}_Sales.SalesOrderHeader soh ON sp.BusinessEntityID = soh.SalesPersonID
        INNER JOIN ${var.database_name_prefix}_Sales.SalesTerritory st ON sp.TerritoryID = st.TerritoryID 
        INNER JOIN ${var.database_name_prefix}_HumanResources.Employee e ON soh.SalesPersonID = e.BusinessEntityID 
		INNER JOIN ${var.database_name_prefix}_Person.Person p ON p.BusinessEntityID = sp.BusinessEntityID
	 ) AS soh 
PIVOT 
(
    SUM(SubTotal) 
    FOR FiscalYear 
    IN (2002, 2003, 2004)
) ;

SELECT * FROM ${var.database_name_prefix}_Sales.vSalesPersonSalesByFiscalYears 

-- COMMAND ----------

-- CREATE VIEW [Person].[vStateProvinceCountryRegion] 
-- WITH SCHEMABINDING 
-- AS 
-- SELECT 
--     sp.[StateProvinceID] 
--     ,sp.[StateProvinceCode] 
--     ,sp.[IsOnlyStateProvinceFlag] 
--     ,sp.[Name] AS [StateProvinceName] 
--     ,sp.[TerritoryID] 
--     ,cr.[CountryRegionCode] 
--     ,cr.[Name] AS [CountryRegionName]
-- FROM [Person].[StateProvince] sp 
--     INNER JOIN [Person].[CountryRegion] cr 
--     ON sp.[CountryRegionCode] = cr.[CountryRegionCode];
-- GO

CREATE OR REPLACE VIEW ${var.database_name_prefix}_Person.vStateProvinceCountryRegion 

AS 
SELECT 
    sp.StateProvinceID 
    ,sp.StateProvinceCode 
    ,sp.IsOnlyStateProvinceFlag 
    ,sp.Name AS StateProvinceName 
    ,sp.TerritoryID 
    ,cr.CountryRegionCode 
    ,cr.Name AS CountryRegionName
FROM ${var.database_name_prefix}_Person.StateProvince sp 
    INNER JOIN ${var.database_name_prefix}_Person.CountryRegion cr 
    ON sp.CountryRegionCode = cr.CountryRegionCode;


-- COMMAND ----------

-- CREATE VIEW [Sales].[vStoreWithContacts] AS 
-- SELECT 
--     s.[BusinessEntityID] 
--     ,s.[Name] 
--     ,ct.[Name] AS [ContactType] 
--     ,p.[Title] 
--     ,p.[FirstName] 
--     ,p.[MiddleName] 
--     ,p.[LastName] 
--     ,p.[Suffix] 
--     ,pp.[PhoneNumber] 
-- 	,pnt.[Name] AS [PhoneNumberType]
--     ,ea.[EmailAddress] 
--     ,p.[EmailPromotion] 
-- FROM [Sales].[Store] s
--     INNER JOIN [Person].[BusinessEntityContact] bec 
--     ON bec.[BusinessEntityID] = s.[BusinessEntityID]
-- 	INNER JOIN [Person].[ContactType] ct
-- 	ON ct.[ContactTypeID] = bec.[ContactTypeID]
-- 	INNER JOIN [Person].[Person] p
-- 	ON p.[BusinessEntityID] = bec.[PersonID]
-- 	LEFT OUTER JOIN [Person].[EmailAddress] ea
-- 	ON ea.[BusinessEntityID] = p.[BusinessEntityID]
-- 	LEFT OUTER JOIN [Person].[PersonPhone] pp
-- 	ON pp.[BusinessEntityID] = p.[BusinessEntityID]
-- 	LEFT OUTER JOIN [Person].[PhoneNumberType] pnt
-- 	ON pnt.[PhoneNumberTypeID] = pp.[PhoneNumberTypeID];
-- GO

CREATE OR REPLACE VIEW ${var.database_name_prefix}_Sales.vStoreWithContacts AS 
SELECT 
    s.BusinessEntityID 
    ,s.Name 
    ,ct.Name AS ContactType 
    ,p.Title 
    ,p.FirstName 
    ,p.MiddleName 
    ,p.LastName 
    ,p.Suffix 
    ,pp.PhoneNumber 
	,pnt.Name AS PhoneNumberType
    ,ea.EmailAddress 
    ,p.EmailPromotion 
FROM ${var.database_name_prefix}_Sales.Store s
    INNER JOIN ${var.database_name_prefix}_Person.BusinessEntityContact bec 
    ON bec.BusinessEntityID = s.BusinessEntityID
	INNER JOIN ${var.database_name_prefix}_Person.ContactType ct
	ON ct.ContactTypeID = bec.ContactTypeID
	INNER JOIN ${var.database_name_prefix}_Person.Person p
	ON p.BusinessEntityID = bec.PersonID
	LEFT OUTER JOIN ${var.database_name_prefix}_Person.EmailAddress ea
	ON ea.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN ${var.database_name_prefix}_Person.PersonPhone pp
	ON pp.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN ${var.database_name_prefix}_Person.PhoneNumberType pnt
	ON pnt.PhoneNumberTypeID = pp.PhoneNumberTypeID;

-- COMMAND ----------

-- CREATE VIEW [Sales].[vStoreWithAddresses] AS 
-- SELECT 
--     s.[BusinessEntityID] 
--     ,s.[Name] 
--     ,at.[Name] AS [AddressType]
--     ,a.[AddressLine1] 
--     ,a.[AddressLine2] 
--     ,a.[City] 
--     ,sp.[Name] AS [StateProvinceName] 
--     ,a.[PostalCode] 
--     ,cr.[Name] AS [CountryRegionName] 
-- FROM [Sales].[Store] s
--     INNER JOIN [Person].[BusinessEntityAddress] bea 
--     ON bea.[BusinessEntityID] = s.[BusinessEntityID] 
--     INNER JOIN [Person].[Address] a 
--     ON a.[AddressID] = bea.[AddressID]
--     INNER JOIN [Person].[StateProvince] sp 
--     ON sp.[StateProvinceID] = a.[StateProvinceID]
--     INNER JOIN [Person].[CountryRegion] cr 
--     ON cr.[CountryRegionCode] = sp.[CountryRegionCode]
--     INNER JOIN [Person].[AddressType] at 
--     ON at.[AddressTypeID] = bea.[AddressTypeID];
-- GO


CREATE OR REPLACE VIEW ${var.database_name_prefix}_Sales.vStoreWithAddresses AS 
SELECT 
    s.BusinessEntityID 
    ,s.Name 
    ,at.Name AS AddressType
    ,a.AddressLine1 
    ,a.AddressLine2 
    ,a.City 
    ,sp.Name AS StateProvinceName 
    ,a.PostalCode 
    ,cr.Name AS CountryRegionName 
FROM ${var.database_name_prefix}_Sales.Store s
    INNER JOIN ${var.database_name_prefix}_Person.BusinessEntityAddress bea 
    ON bea.BusinessEntityID = s.BusinessEntityID 
    INNER JOIN ${var.database_name_prefix}_Person.Address a 
    ON a.AddressID = bea.AddressID
    INNER JOIN ${var.database_name_prefix}_Person.StateProvince sp 
    ON sp.StateProvinceID = a.StateProvinceID
    INNER JOIN ${var.database_name_prefix}_Person.CountryRegion cr 
    ON cr.CountryRegionCode = sp.CountryRegionCode
    INNER JOIN ${var.database_name_prefix}_Person.AddressType at 
    ON at.AddressTypeID = bea.AddressTypeID;

-- COMMAND ----------


-- CREATE VIEW [Purchasing].[vVendorWithContacts] AS 
-- SELECT 
--     v.[BusinessEntityID]
--     ,v.[Name]
--     ,ct.[Name] AS [ContactType] 
--     ,p.[Title] 
--     ,p.[FirstName] 
--     ,p.[MiddleName] 
--     ,p.[LastName] 
--     ,p.[Suffix] 
--     ,pp.[PhoneNumber] 
--     ,pnt.[Name] AS [PhoneNumberType]
--     ,ea.[EmailAddress] 
--     ,p.[EmailPromotion] 
-- FROM [Purchasing].[Vendor] v
--     INNER JOIN [Person].[BusinessEntityContact] bec 
--     ON bec.[BusinessEntityID] = v.[BusinessEntityID]
--     INNER JOIN [Person].ContactType ct
--     ON ct.[ContactTypeID] = bec.[ContactTypeID]
--     INNER JOIN [Person].[Person] p
--     ON p.[BusinessEntityID] = bec.[PersonID]
--     LEFT OUTER JOIN [Person].[EmailAddress] ea
--     ON ea.[BusinessEntityID] = p.[BusinessEntityID]
--     LEFT OUTER JOIN [Person].[PersonPhone] pp
--     ON pp.[BusinessEntityID] = p.[BusinessEntityID]
--     LEFT OUTER JOIN [Person].[PhoneNumberType] pnt
--     ON pnt.[PhoneNumberTypeID] = pp.[PhoneNumberTypeID];
-- GO


CREATE OR REPLACE VIEW ${var.database_name_prefix}_Purchasing.vVendorWithContacts AS 
SELECT 
    v.BusinessEntityID
    ,v.Name
    ,ct.Name AS ContactType 
    ,p.Title 
    ,p.FirstName 
    ,p.MiddleName 
    ,p.LastName 
    ,p.Suffix 
    ,pp.PhoneNumber 
    ,pnt.Name AS PhoneNumberType
    ,ea.EmailAddress 
    ,p.EmailPromotion 
FROM ${var.database_name_prefix}_Purchasing.Vendor v
    INNER JOIN ${var.database_name_prefix}_Person.BusinessEntityContact bec 
    ON bec.BusinessEntityID = v.BusinessEntityID
    INNER JOIN ${var.database_name_prefix}_Person.ContactType ct
    ON ct.ContactTypeID = bec.ContactTypeID
    INNER JOIN ${var.database_name_prefix}_Person.Person p
    ON p.BusinessEntityID = bec.PersonID
    LEFT OUTER JOIN ${var.database_name_prefix}_Person.EmailAddress ea
    ON ea.BusinessEntityID = p.BusinessEntityID
    LEFT OUTER JOIN ${var.database_name_prefix}_Person.PersonPhone pp
    ON pp.BusinessEntityID = p.BusinessEntityID
    LEFT OUTER JOIN ${var.database_name_prefix}_Person.PhoneNumberType pnt
    ON pnt.PhoneNumberTypeID = pp.PhoneNumberTypeID;

-- COMMAND ----------

-- CREATE VIEW [Purchasing].[vVendorWithAddresses] AS 
-- SELECT 
--     v.[BusinessEntityID]
--     ,v.[Name]
--     ,at.[Name] AS [AddressType]
--     ,a.[AddressLine1] 
--     ,a.[AddressLine2] 
--     ,a.[City] 
--     ,sp.[Name] AS [StateProvinceName] 
--     ,a.[PostalCode] 
--     ,cr.[Name] AS [CountryRegionName] 
-- FROM [Purchasing].[Vendor] v
--     INNER JOIN [Person].[BusinessEntityAddress] bea 
--     ON bea.[BusinessEntityID] = v.[BusinessEntityID] 
--     INNER JOIN [Person].[Address] a 
--     ON a.[AddressID] = bea.[AddressID]
--     INNER JOIN [Person].[StateProvince] sp 
--     ON sp.[StateProvinceID] = a.[StateProvinceID]
--     INNER JOIN [Person].[CountryRegion] cr 
--     ON cr.[CountryRegionCode] = sp.[CountryRegionCode]
--     INNER JOIN [Person].[AddressType] at 
--     ON at.[AddressTypeID] = bea.[AddressTypeID];
-- GO

CREATE OR REPLACE VIEW ${var.database_name_prefix}_Purchasing.vVendorWithAddresses AS 
SELECT 
    v.BusinessEntityID
    ,v.Name
    ,at.Name AS AddressType
    ,a.AddressLine1 
    ,a.AddressLine2 
    ,a.City 
    ,sp.Name AS StateProvinceName 
    ,a.PostalCode 
    ,cr.Name AS CountryRegionName 
FROM ${var.database_name_prefix}_Purchasing.Vendor v
    INNER JOIN ${var.database_name_prefix}_Person.BusinessEntityAddress bea 
    ON bea.BusinessEntityID = v.BusinessEntityID 
    INNER JOIN ${var.database_name_prefix}_Person.Address a 
    ON a.AddressID = bea.AddressID
    INNER JOIN ${var.database_name_prefix}_Person.StateProvince sp 
    ON sp.StateProvinceID = a.StateProvinceID
    INNER JOIN ${var.database_name_prefix}_Person.CountryRegion cr 
    ON cr.CountryRegionCode = sp.CountryRegionCode
    INNER JOIN ${var.database_name_prefix}_Person.AddressType at 
    ON at.AddressTypeID = bea.AddressTypeID;

-- COMMAND ----------



-- COMMAND ----------


