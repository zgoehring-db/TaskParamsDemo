# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Migration  
# MAGIC 
# MAGIC In this notebook we will cover how to migrate Stored Procedures from SQL Server to Databricks. Please note that this notebook will cover how to convert a stored procedure, but each stored procedure would be converted to its own Databricks notebook. Please refer to the ETL folder for all source code.  

# COMMAND ----------

def parse_sql(sql):
  sql = sql.replace(" ON [PRIMARY]", ";")
  sql = sql.replace("[", "").replace("]", "")
  print(sql)
  

# COMMAND ----------

# DBTITLE 1,uspGetBillOfMaterials
### TSQL Stored Procedure 

# CREATE PROCEDURE [dbo].[uspGetBillOfMaterials]
#     @StartProductID [int],
#     @CheckDate [datetime]
# AS
# BEGIN
#     SET NOCOUNT ON;
# 
#     INSERT INTO [Production].[BillOfMaterialsByDate]
#     WITH [BOM_cte]([ProductAssemblyID], [ComponentID], [ComponentDesc], [PerAssemblyQty], [StandardCost], [ListPrice], [BOMLevel]) -- CTE name and columns
#     AS (
#         SELECT b.[ProductAssemblyID], b.[ComponentID], p.[Name], b.[PerAssemblyQty], p.[StandardCost], p.[ListPrice], b.[BOMLevel]
#         FROM [Production].[BillOfMaterials] b
#             INNER JOIN [Production].[Product] p 
#             ON b.[ComponentID] = p.[ProductID] 
#         WHERE b.[ProductAssemblyID] = @StartProductID 
#             AND @CheckDate >= b.[StartDate] 
#             AND @CheckDate <= ISNULL(b.[EndDate], @CheckDate)
        
#         )
#     -- Outer select from the CTE
#     SELECT b.[ProductAssemblyID], b.[ComponentID], b.[ComponentDesc], SUM(b.[PerAssemblyQty]) AS [TotalQuantity] , b.[StandardCost], b.[ListPrice], b.[BOMLevel]
#     FROM [BOM_cte] b
#     GROUP BY b.[ComponentID], b.[ComponentDesc], b.[ProductAssemblyID], b.[BOMLevel], b.[StandardCost], b.[ListPrice]
#     ORDER BY b.[BOMLevel], b.[ProductAssemblyID], b.[ComponentID]

# END;
# GO

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rac_Production.BillOfMaterials

# COMMAND ----------

StartProductID = 0 # int
CheckDate = '' # datetime 

spark.sql("""
WITH BOM_cte(ProductAssemblyID, ComponentID, ComponentDesc, PerAssemblyQty, StandardCost, ListPrice, BOMLevel) -- CTE name and columns
AS (
    SELECT b.ProductAssemblyID, b.ComponentID, p.Name, b.PerAssemblyQty, p.StandardCost, p.ListPrice, b.BOMLevel
    FROM Production.BillOfMaterials b
        INNER JOIN Production.Product p 
        ON b.ComponentID = p.ProductID 
    WHERE b.ProductAssemblyID = {} 
        AND {} >= b.StartDate 
        AND {} <= ISNULL(b.EndDate, {})

    )
-- Outer select from the CTE
SELECT b.ProductAssemblyID, b.ComponentID, b.ComponentDesc, SUM(b.PerAssemblyQty) AS TotalQuantity , b.StandardCost, b.ListPrice, b.BOMLevel
FROM BOM_cte b
GROUP BY b.ComponentID, b.ComponentDesc, b.ProductAssemblyID, b.BOMLevel, b.StandardCost, b.ListPrice
ORDER BY b.BOMLevel, b.ProductAssemblyID, b.ComponentID

""".format(StartProductID, CheckDate,CheckDate,CheckDate)).createOrReplaceTempView('bill_of_materials')

# COMMAND ----------

parse_sql("""
WITH [BOM_cte]([ProductAssemblyID], [ComponentID], [ComponentDesc], [PerAssemblyQty], [StandardCost], [ListPrice], [BOMLevel]) -- CTE name and columns
AS (
    SELECT b.[ProductAssemblyID], b.[ComponentID], p.[Name], b.[PerAssemblyQty], p.[StandardCost], p.[ListPrice], b.[BOMLevel]
    FROM [Production].[BillOfMaterials] b
        INNER JOIN [Production].[Product] p 
        ON b.[ComponentID] = p.[ProductID] 
    WHERE b.[ProductAssemblyID] = @StartProductID 
        AND @CheckDate >= b.[StartDate] 
        AND @CheckDate <= ISNULL(b.[EndDate], @CheckDate)

    )
-- Outer select from the CTE
SELECT b.[ProductAssemblyID], b.[ComponentID], b.[ComponentDesc], SUM(b.[PerAssemblyQty]) AS [TotalQuantity] , b.[StandardCost], b.[ListPrice], b.[BOMLevel]
FROM [BOM_cte] b
GROUP BY b.[ComponentID], b.[ComponentDesc], b.[ProductAssemblyID], b.[BOMLevel], b.[StandardCost], b.[ListPrice]
ORDER BY b.[BOMLevel], b.[ProductAssemblyID], b.[ComponentID]
""")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Sample Stored Procedures   
# MAGIC 
# MAGIC ```sql
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CREATE PROCEDURE [dbo].[uspGetEmployeeManagers]
# MAGIC     @BusinessEntityID [int]
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC 
# MAGIC     -- Use recursive query to list out all Employees required for a particular Manager
# MAGIC     WITH [EMP_cte]([BusinessEntityID], [OrganizationNode], [FirstName], [LastName], [JobTitle], [RecursionLevel]) -- CTE name and columns
# MAGIC     AS (
# MAGIC         SELECT e.[BusinessEntityID], e.[OrganizationNode], p.[FirstName], p.[LastName], e.[JobTitle], 0 -- Get the initial Employee
# MAGIC         FROM [HumanResources].[Employee] e 
# MAGIC 			INNER JOIN [Person].[Person] as p
# MAGIC 			ON p.[BusinessEntityID] = e.[BusinessEntityID]
# MAGIC         WHERE e.[BusinessEntityID] = @BusinessEntityID
# MAGIC         UNION ALL
# MAGIC         SELECT e.[BusinessEntityID], e.[OrganizationNode], p.[FirstName], p.[LastName], e.[JobTitle], [RecursionLevel] + 1 -- Join recursive member to anchor
# MAGIC         FROM [HumanResources].[Employee] e 
# MAGIC             INNER JOIN [EMP_cte]
# MAGIC             ON e.[OrganizationNode] = [EMP_cte].[OrganizationNode].GetAncestor(1)
# MAGIC             INNER JOIN [Person].[Person] p 
# MAGIC             ON p.[BusinessEntityID] = e.[BusinessEntityID]
# MAGIC     )
# MAGIC     -- Join back to Employee to return the manager name 
# MAGIC     SELECT [EMP_cte].[RecursionLevel], [EMP_cte].[BusinessEntityID], [EMP_cte].[FirstName], [EMP_cte].[LastName], 
# MAGIC         [EMP_cte].[OrganizationNode].ToString() AS [OrganizationNode], p.[FirstName] AS 'ManagerFirstName', p.[LastName] AS 'ManagerLastName'  -- Outer select from the CTE
# MAGIC     FROM [EMP_cte] 
# MAGIC         INNER JOIN [HumanResources].[Employee] e 
# MAGIC         ON [EMP_cte].[OrganizationNode].GetAncestor(1) = e.[OrganizationNode]
# MAGIC         INNER JOIN [Person].[Person] p 
# MAGIC         ON p.[BusinessEntityID] = e.[BusinessEntityID]
# MAGIC     ORDER BY [RecursionLevel], [EMP_cte].[OrganizationNode].ToString()
# MAGIC     OPTION (MAXRECURSION 25) 
# MAGIC END;
# MAGIC GO
# MAGIC 
# MAGIC CREATE PROCEDURE [dbo].[uspGetManagerEmployees]
# MAGIC     @BusinessEntityID [int]
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC 
# MAGIC     -- Use recursive query to list out all Employees required for a particular Manager
# MAGIC     WITH [EMP_cte]([BusinessEntityID], [OrganizationNode], [FirstName], [LastName], [RecursionLevel]) -- CTE name and columns
# MAGIC     AS (
# MAGIC         SELECT e.[BusinessEntityID], e.[OrganizationNode], p.[FirstName], p.[LastName], 0 -- Get the initial list of Employees for Manager n
# MAGIC         FROM [HumanResources].[Employee] e 
# MAGIC 			INNER JOIN [Person].[Person] p 
# MAGIC 			ON p.[BusinessEntityID] = e.[BusinessEntityID]
# MAGIC         WHERE e.[BusinessEntityID] = @BusinessEntityID
# MAGIC         UNION ALL
# MAGIC         SELECT e.[BusinessEntityID], e.[OrganizationNode], p.[FirstName], p.[LastName], [RecursionLevel] + 1 -- Join recursive member to anchor
# MAGIC         FROM [HumanResources].[Employee] e 
# MAGIC             INNER JOIN [EMP_cte]
# MAGIC             ON e.[OrganizationNode].GetAncestor(1) = [EMP_cte].[OrganizationNode]
# MAGIC 			INNER JOIN [Person].[Person] p 
# MAGIC 			ON p.[BusinessEntityID] = e.[BusinessEntityID]
# MAGIC         )
# MAGIC     -- Join back to Employee to return the manager name 
# MAGIC     SELECT [EMP_cte].[RecursionLevel], [EMP_cte].[OrganizationNode].ToString() as [OrganizationNode], p.[FirstName] AS 'ManagerFirstName', p.[LastName] AS 'ManagerLastName',
# MAGIC         [EMP_cte].[BusinessEntityID], [EMP_cte].[FirstName], [EMP_cte].[LastName] -- Outer select from the CTE
# MAGIC     FROM [EMP_cte] 
# MAGIC         INNER JOIN [HumanResources].[Employee] e 
# MAGIC         ON [EMP_cte].[OrganizationNode].GetAncestor(1) = e.[OrganizationNode]
# MAGIC 			INNER JOIN [Person].[Person] p 
# MAGIC 			ON p.[BusinessEntityID] = e.[BusinessEntityID]
# MAGIC     ORDER BY [RecursionLevel], [EMP_cte].[OrganizationNode].ToString()
# MAGIC     OPTION (MAXRECURSION 25) 
# MAGIC END;
# MAGIC GO
# MAGIC 
# MAGIC CREATE PROCEDURE [dbo].[uspGetWhereUsedProductID]
# MAGIC     @StartProductID [int],
# MAGIC     @CheckDate [datetime]
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC 
# MAGIC     --Use recursive query to generate a multi-level Bill of Material (i.e. all level 1 components of a level 0 assembly, all level 2 components of a level 1 assembly)
# MAGIC     WITH [BOM_cte]([ProductAssemblyID], [ComponentID], [ComponentDesc], [PerAssemblyQty], [StandardCost], [ListPrice], [BOMLevel], [RecursionLevel]) -- CTE name and columns
# MAGIC     AS (
# MAGIC         SELECT b.[ProductAssemblyID], b.[ComponentID], p.[Name], b.[PerAssemblyQty], p.[StandardCost], p.[ListPrice], b.[BOMLevel], 0 -- Get the initial list of components for the bike assembly
# MAGIC         FROM [Production].[BillOfMaterials] b
# MAGIC             INNER JOIN [Production].[Product] p 
# MAGIC             ON b.[ProductAssemblyID] = p.[ProductID] 
# MAGIC         WHERE b.[ComponentID] = @StartProductID 
# MAGIC             AND @CheckDate >= b.[StartDate] 
# MAGIC             AND @CheckDate <= ISNULL(b.[EndDate], @CheckDate)
# MAGIC         UNION ALL
# MAGIC         SELECT b.[ProductAssemblyID], b.[ComponentID], p.[Name], b.[PerAssemblyQty], p.[StandardCost], p.[ListPrice], b.[BOMLevel], [RecursionLevel] + 1 -- Join recursive member to anchor
# MAGIC         FROM [BOM_cte] cte
# MAGIC             INNER JOIN [Production].[BillOfMaterials] b 
# MAGIC             ON cte.[ProductAssemblyID] = b.[ComponentID]
# MAGIC             INNER JOIN [Production].[Product] p 
# MAGIC             ON b.[ProductAssemblyID] = p.[ProductID] 
# MAGIC         WHERE @CheckDate >= b.[StartDate] 
# MAGIC             AND @CheckDate <= ISNULL(b.[EndDate], @CheckDate)
# MAGIC         )
# MAGIC     -- Outer select from the CTE
# MAGIC     SELECT b.[ProductAssemblyID], b.[ComponentID], b.[ComponentDesc], SUM(b.[PerAssemblyQty]) AS [TotalQuantity] , b.[StandardCost], b.[ListPrice], b.[BOMLevel], b.[RecursionLevel]
# MAGIC     FROM [BOM_cte] b
# MAGIC     GROUP BY b.[ComponentID], b.[ComponentDesc], b.[ProductAssemblyID], b.[BOMLevel], b.[RecursionLevel], b.[StandardCost], b.[ListPrice]
# MAGIC     ORDER BY b.[BOMLevel], b.[ProductAssemblyID], b.[ComponentID]
# MAGIC     OPTION (MAXRECURSION 25) 
# MAGIC END;
# MAGIC GO
# MAGIC 
# MAGIC CREATE PROCEDURE [HumanResources].[uspUpdateEmployeeHireInfo]
# MAGIC     @BusinessEntityID [int], 
# MAGIC     @JobTitle [nvarchar](50), 
# MAGIC     @HireDate [datetime], 
# MAGIC     @RateChangeDate [datetime], 
# MAGIC     @Rate [money], 
# MAGIC     @PayFrequency [tinyint], 
# MAGIC     @CurrentFlag [dbo].[Flag] 
# MAGIC WITH EXECUTE AS CALLER
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC 
# MAGIC     BEGIN TRY
# MAGIC         BEGIN TRANSACTION;
# MAGIC 
# MAGIC         UPDATE [HumanResources].[Employee] 
# MAGIC         SET [JobTitle] = @JobTitle 
# MAGIC             ,[HireDate] = @HireDate 
# MAGIC             ,[CurrentFlag] = @CurrentFlag 
# MAGIC         WHERE [BusinessEntityID] = @BusinessEntityID;
# MAGIC 
# MAGIC         INSERT INTO [HumanResources].[EmployeePayHistory] 
# MAGIC             ([BusinessEntityID]
# MAGIC             ,[RateChangeDate]
# MAGIC             ,[Rate]
# MAGIC             ,[PayFrequency]) 
# MAGIC         VALUES (@BusinessEntityID, @RateChangeDate, @Rate, @PayFrequency);
# MAGIC 
# MAGIC         COMMIT TRANSACTION;
# MAGIC     END TRY
# MAGIC     BEGIN CATCH
# MAGIC         -- Rollback any active or uncommittable transactions before
# MAGIC         -- inserting information in the ErrorLog
# MAGIC         IF @@TRANCOUNT > 0
# MAGIC         BEGIN
# MAGIC             ROLLBACK TRANSACTION;
# MAGIC         END
# MAGIC 
# MAGIC         EXECUTE [dbo].[uspLogError];
# MAGIC     END CATCH;
# MAGIC END;
# MAGIC GO
# MAGIC 
# MAGIC CREATE PROCEDURE [HumanResources].[uspUpdateEmployeeLogin]
# MAGIC     @BusinessEntityID [int], 
# MAGIC     @OrganizationNode [hierarchyid],
# MAGIC     @LoginID [nvarchar](256),
# MAGIC     @JobTitle [nvarchar](50),
# MAGIC     @HireDate [datetime],
# MAGIC     @CurrentFlag [dbo].[Flag]
# MAGIC WITH EXECUTE AS CALLER
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC 
# MAGIC     BEGIN TRY
# MAGIC         UPDATE [HumanResources].[Employee] 
# MAGIC         SET [OrganizationNode] = @OrganizationNode 
# MAGIC             ,[LoginID] = @LoginID 
# MAGIC             ,[JobTitle] = @JobTitle 
# MAGIC             ,[HireDate] = @HireDate 
# MAGIC             ,[CurrentFlag] = @CurrentFlag 
# MAGIC         WHERE [BusinessEntityID] = @BusinessEntityID;
# MAGIC     END TRY
# MAGIC     BEGIN CATCH
# MAGIC         EXECUTE [dbo].[uspLogError];
# MAGIC     END CATCH;
# MAGIC END;
# MAGIC GO
# MAGIC 
# MAGIC CREATE PROCEDURE [HumanResources].[uspUpdateEmployeePersonalInfo]
# MAGIC     @BusinessEntityID [int], 
# MAGIC     @NationalIDNumber [nvarchar](15), 
# MAGIC     @BirthDate [datetime], 
# MAGIC     @MaritalStatus [nchar](1), 
# MAGIC     @Gender [nchar](1)
# MAGIC WITH EXECUTE AS CALLER
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC 
# MAGIC     BEGIN TRY
# MAGIC         UPDATE [HumanResources].[Employee] 
# MAGIC         SET [NationalIDNumber] = @NationalIDNumber 
# MAGIC             ,[BirthDate] = @BirthDate 
# MAGIC             ,[MaritalStatus] = @MaritalStatus 
# MAGIC             ,[Gender] = @Gender 
# MAGIC         WHERE [BusinessEntityID] = @BusinessEntityID;
# MAGIC     END TRY
# MAGIC     BEGIN CATCH
# MAGIC         EXECUTE [dbo].[uspLogError];
# MAGIC     END CATCH;
# MAGIC END;
# MAGIC GO
# MAGIC 
# MAGIC --A stored procedure which demonstrates integrated full text search
# MAGIC 
# MAGIC CREATE PROCEDURE [dbo].[uspSearchCandidateResumes]
# MAGIC     @searchString [nvarchar](1000),   
# MAGIC     @useInflectional [bit]=0,
# MAGIC     @useThesaurus [bit]=0,
# MAGIC     @language[int]=0
# MAGIC 
# MAGIC 
# MAGIC WITH EXECUTE AS CALLER
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC 
# MAGIC       DECLARE @string nvarchar(1050)
# MAGIC       --setting the lcid to the default instance LCID if needed
# MAGIC       IF @language = NULL OR @language = 0 
# MAGIC       BEGIN 
# MAGIC             SELECT @language =CONVERT(int, serverproperty('lcid'))  
# MAGIC       END
# MAGIC       
# MAGIC 
# MAGIC             --FREETEXTTABLE case as inflectional and Thesaurus were required
# MAGIC       IF @useThesaurus = 1 AND @useInflectional = 1  
# MAGIC         BEGIN
# MAGIC                   SELECT FT_TBL.[JobCandidateID], KEY_TBL.[RANK] FROM [HumanResources].[JobCandidate] AS FT_TBL 
# MAGIC                         INNER JOIN FREETEXTTABLE([HumanResources].[JobCandidate],*, @searchString,LANGUAGE @language) AS KEY_TBL
# MAGIC                    ON  FT_TBL.[JobCandidateID] =KEY_TBL.[KEY]
# MAGIC             END
# MAGIC 
# MAGIC       ELSE IF @useThesaurus = 1
# MAGIC             BEGIN
# MAGIC                   SELECT @string ='FORMSOF(THESAURUS,"'+@searchString +'"'+')'      
# MAGIC                   SELECT FT_TBL.[JobCandidateID], KEY_TBL.[RANK] FROM [HumanResources].[JobCandidate] AS FT_TBL 
# MAGIC                         INNER JOIN CONTAINSTABLE([HumanResources].[JobCandidate],*, @string,LANGUAGE @language) AS KEY_TBL
# MAGIC                    ON  FT_TBL.[JobCandidateID] =KEY_TBL.[KEY]
# MAGIC         END
# MAGIC 
# MAGIC       ELSE IF @useInflectional = 1
# MAGIC             BEGIN
# MAGIC                   SELECT @string ='FORMSOF(INFLECTIONAL,"'+@searchString +'"'+')'
# MAGIC                   SELECT FT_TBL.[JobCandidateID], KEY_TBL.[RANK] FROM [HumanResources].[JobCandidate] AS FT_TBL 
# MAGIC                         INNER JOIN CONTAINSTABLE([HumanResources].[JobCandidate],*, @string,LANGUAGE @language) AS KEY_TBL
# MAGIC                    ON  FT_TBL.[JobCandidateID] =KEY_TBL.[KEY]
# MAGIC         END
# MAGIC   
# MAGIC       ELSE --base case, plain CONTAINSTABLE
# MAGIC             BEGIN
# MAGIC                   SELECT @string='"'+@searchString +'"'
# MAGIC                   SELECT FT_TBL.[JobCandidateID],KEY_TBL.[RANK] FROM [HumanResources].[JobCandidate] AS FT_TBL 
# MAGIC                         INNER JOIN CONTAINSTABLE([HumanResources].[JobCandidate],*,@string,LANGUAGE @language) AS KEY_TBL
# MAGIC                    ON  FT_TBL.[JobCandidateID] =KEY_TBL.[KEY]
# MAGIC             END
# MAGIC 
# MAGIC END;
# MAGIC GO
# MAGIC 
# MAGIC ```

# COMMAND ----------


