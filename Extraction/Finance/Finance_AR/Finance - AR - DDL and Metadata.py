# Databricks notebook source
# MAGIC %md
# MAGIC ### DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.finance_account_receivable (
# MAGIC   Company BIGINT NOT NULL,
# MAGIC   CompanyName VARCHAR(200) NOT NULL,
# MAGIC   CorporateCustomerId INT NOT NULL,
# MAGIC   CorporateCustomerName VARCHAR(100) NOT NULL,
# MAGIC   CustomerCode VARCHAR(50) NOT NULL,
# MAGIC   CustomerName VARCHAR(100) NOT NULL,
# MAGIC   OwnerProfitCenter VARCHAR(20) NOT NULL,
# MAGIC   RevenueProfitCenter VARCHAR(20) NOT NULL,
# MAGIC   InvoiceNumber VARCHAR(50) NOT NULL,
# MAGIC   InvoiceUserDefined1 VARCHAR(50),
# MAGIC   InvoiceDate VARCHAR(12),
# MAGIC   ArAgeInv_OrigAdjAmt DECIMAL(19,6) NOT NULL,
# MAGIC   ArAgeInv_OrigAmt DECIMAL(19,6) NOT NULL,
# MAGIC   ArAgeInv_OrigAppliedAmt DECIMAL(19,6) NOT NULL,
# MAGIC   ArAgeInv_OrigOpenAmt DECIMAL(19,6) NOT NULL,
# MAGIC   ArAgeInv_PastDueAmt DECIMAL(19,6) NOT NULL,
# MAGIC   ArAgeInv_PastDueCnt DECIMAL(19,6) NOT NULL,
# MAGIC   ArAge_OrigCurrent DECIMAL(19,6) NOT NULL,
# MAGIC   ArAge_OrigFifteen DECIMAL(19,6) NOT NULL,
# MAGIC   ArAge_OrigThirty DECIMAL(19,6) NOT NULL,
# MAGIC   ArAge_OrigFortyFive DECIMAL(19,6) NOT NULL,
# MAGIC   ArAge_OrigSixty DECIMAL(19,6) NOT NULL,
# MAGIC   ArAge_OrigNinety DECIMAL(19,6) NOT NULL,
# MAGIC   ArAge_OrigOneHundredTwenty DECIMAL(19,6) NOT NULL,
# MAGIC   ArAge_OrigOneHundredTwentyOne DECIMAL(19,6) NOT NULL,
# MAGIC   ArPstDue_OrigCurrent DECIMAL(19,6) NOT NULL,
# MAGIC   ArPstDue_OrigFifteen DECIMAL(19,6) NOT NULL,
# MAGIC   ArPstDue_OrigThirty DECIMAL(19,6) NOT NULL,
# MAGIC   ArPstDue_OrigFortyFive DECIMAL(19,6) NOT NULL,
# MAGIC   ArPstDue_OrigSixty DECIMAL(19,6) NOT NULL,
# MAGIC   ArPstDue_OrigNinety DECIMAL(19,6) NOT NULL,
# MAGIC   ArPstDue_OrigOneHundredTwenty DECIMAL(19,6) NOT NULL,
# MAGIC   ArPstDue_OrigOneHundredTwentyOne DECIMAL(19,6) NOT NULL,
# MAGIC   SnapshotDateTime VARCHAR(18),
# MAGIC   Collector VARCHAR(100) NOT NULL,
# MAGIC   Location VARCHAR(100),
# MAGIC   SalesRep VARCHAR(100),
# MAGIC   AM VARCHAR(50),
# MAGIC   Currency VARCHAR(5),
# MAGIC   LineOfBusiness VARCHAR(200),
# MAGIC   Division VARCHAR(200),
# MAGIC   Region VARCHAR(200),
# MAGIC   SourceSystem_ID SMALLINT NOT NULL,
# MAGIC   SourceSystem_Name VARCHAR(20) NOT NULL)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata SET SourceSystem = 'Lawson System', SubjectArea = 'Finance', Job_Name = 'NFI_Finance_Extraction', SourceSchema = 'dbo', SourceDBName = 'DM_ARData', SourceTableName = 'vwArSelectAllAging_DivisionBrokerage', DWHTableName = 'Finance_Account_Receivable' WHERE TableID = 'F16'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC     metadata.MasterMetadata (
# MAGIC     SourceSystem,
# MAGIC     TableID,
# MAGIC     SubjectArea,
# MAGIC     SourceSchema,
# MAGIC     SourceTableName,
# MAGIC     LoadType,
# MAGIC     IsActive,
# MAGIC     Frequency,
# MAGIC     SourceSelectQuery
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     "Lawson System",
# MAGIC     "F16",
# MAGIC     "Finance",
# MAGIC     "Bronze",
# MAGIC     "Finance_Account_Receivable",
# MAGIC     "truncate and load",
# MAGIC     "1",
# MAGIC     "once a day",
# MAGIC     "SELECT [Company]
# MAGIC       ,[CompanyName]
# MAGIC       ,[CorporateCustomerId]
# MAGIC       ,[CorporateCustomerName]
# MAGIC       ,[CustomerCode]
# MAGIC       ,[CustomerName]
# MAGIC       ,[OwnerProfitCenter]
# MAGIC       ,[RevenueProfitCenter]
# MAGIC       ,[InvoiceNumber]
# MAGIC       ,[InvoiceUserDefined1]
# MAGIC       ,[InvoiceDate]
# MAGIC       ,[ArAgeInv_OrigAdjAmt]
# MAGIC       ,[ArAgeInv_OrigAmt]
# MAGIC       ,[ArAgeInv_OrigAppliedAmt]
# MAGIC       ,[ArAgeInv_OrigOpenAmt]
# MAGIC       ,[ArAgeInv_PastDueAmt]
# MAGIC       ,[ArAgeInv_PastDueCnt]
# MAGIC       ,[ArAge_OrigCurrent]
# MAGIC       ,[ArAge_OrigFifteen]
# MAGIC       ,[ArAge_OrigThirty]
# MAGIC       ,[ArAge_OrigFortyFive]
# MAGIC       ,[ArAge_OrigSixty]
# MAGIC       ,[ArAge_OrigNinety]
# MAGIC       ,[ArAge_OrigOneHundredTwenty]
# MAGIC       ,[ArAge_OrigOneHundredTwentyOne]
# MAGIC       ,[ArPstDue_OrigCurrent]
# MAGIC       ,[ArPstDue_OrigFifteen]
# MAGIC       ,[ArPstDue_OrigThirty]
# MAGIC       ,[ArPstDue_OrigFortyFive]
# MAGIC       ,[ArPstDue_OrigSixty]
# MAGIC       ,[ArPstDue_OrigNinety]
# MAGIC       ,[ArPstDue_OrigOneHundredTwenty]
# MAGIC       ,[ArPstDue_OrigOneHundredTwentyOne]
# MAGIC       ,[SnapshotDateTime]
# MAGIC       ,[Collector]
# MAGIC       ,[Location]
# MAGIC       ,[SalesRep]
# MAGIC       ,[AM]
# MAGIC       ,[Currency]
# MAGIC       ,[LineOfBusiness]
# MAGIC       ,[Division]
# MAGIC       ,[Region]
# MAGIC   FROM [DM_ARData].[dbo].[vwArSelectAllAging_DivisionBrokerage]"
# MAGIC       );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete from metadata.MasterMetadata where TableID = "AR-1"

# COMMAND ----------

