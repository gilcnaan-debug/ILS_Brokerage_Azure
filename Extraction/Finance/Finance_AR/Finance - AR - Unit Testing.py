# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract the data from Source and load to Bronze zone
# MAGIC * **Created Date:** 11/18/2024
# MAGIC * **Created By:** Hariharan
# MAGIC * **Modified Date:** 
# MAGIC * **Modified By:** 
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Row Count Check in Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from bronze.finance_account_recievable

# COMMAND ----------

# MAGIC %md
# MAGIC ##DUPLICATE CHECK

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     Company,
# MAGIC     CompanyName,
# MAGIC     CorporateCustomerId,
# MAGIC     CorporateCustomerName,
# MAGIC     CustomerCode,
# MAGIC     CustomerName,
# MAGIC     OwnerProfitCenter,
# MAGIC     RevenueProfitCenter,
# MAGIC     InvoiceNumber,
# MAGIC     InvoiceUserDefined1,
# MAGIC     InvoiceDate,
# MAGIC     ArAgeInv_OrigAdjAmt,
# MAGIC     ArAgeInv_OrigAmt,
# MAGIC     ArAgeInv_OrigAppliedAmt,
# MAGIC     ArAgeInv_OrigOpenAmt,
# MAGIC     ArAgeInv_PastDueAmt,
# MAGIC     ArAgeInv_PastDueCnt,
# MAGIC     ArAge_OrigCurrent,
# MAGIC     ArAge_OrigFifteen,
# MAGIC     ArAge_OrigThirty,
# MAGIC     ArAge_OrigFortyFive,
# MAGIC     ArAge_OrigSixty,
# MAGIC     ArAge_OrigNinety,
# MAGIC     ArAge_OrigOneHundredTwenty,
# MAGIC     ArAge_OrigOneHundredTwentyOne,
# MAGIC     ArPstDue_OrigCurrent,
# MAGIC     ArPstDue_OrigFifteen,
# MAGIC     ArPstDue_OrigThirty,
# MAGIC     ArPstDue_OrigFortyFive,
# MAGIC     ArPstDue_OrigSixty,
# MAGIC     ArPstDue_OrigNinety,
# MAGIC     ArPstDue_OrigOneHundredTwenty,
# MAGIC     ArPstDue_OrigOneHundredTwentyOne,
# MAGIC     SnapshotDateTime,
# MAGIC     SourceSystem_ID,
# MAGIC     SourceSystem_Name,
# MAGIC     COUNT(*) AS DuplicateCount
# MAGIC FROM bronze.Finance_Account_Recievable
# MAGIC GROUP BY 
# MAGIC     Company,
# MAGIC     CompanyName,
# MAGIC     CorporateCustomerId,
# MAGIC     CorporateCustomerName,
# MAGIC     CustomerCode,
# MAGIC     CustomerName,
# MAGIC     OwnerProfitCenter,
# MAGIC     RevenueProfitCenter,
# MAGIC     InvoiceNumber,
# MAGIC     InvoiceUserDefined1,
# MAGIC     InvoiceDate,
# MAGIC     ArAgeInv_OrigAdjAmt,
# MAGIC     ArAgeInv_OrigAmt,
# MAGIC     ArAgeInv_OrigAppliedAmt,
# MAGIC     ArAgeInv_OrigOpenAmt,
# MAGIC     ArAgeInv_PastDueAmt,
# MAGIC     ArAgeInv_PastDueCnt,
# MAGIC     ArAge_OrigCurrent,
# MAGIC     ArAge_OrigFifteen,
# MAGIC     ArAge_OrigThirty,
# MAGIC     ArAge_OrigFortyFive,
# MAGIC     ArAge_OrigSixty,
# MAGIC     ArAge_OrigNinety,
# MAGIC     ArAge_OrigOneHundredTwenty,
# MAGIC     ArAge_OrigOneHundredTwentyOne,
# MAGIC     ArPstDue_OrigCurrent,
# MAGIC     ArPstDue_OrigFifteen,
# MAGIC     ArPstDue_OrigThirty,
# MAGIC     ArPstDue_OrigFortyFive,
# MAGIC     ArPstDue_OrigSixty,
# MAGIC     ArPstDue_OrigNinety,
# MAGIC     ArPstDue_OrigOneHundredTwenty,
# MAGIC     ArPstDue_OrigOneHundredTwentyOne,
# MAGIC     SnapshotDateTime,
# MAGIC     SourceSystem_ID,
# MAGIC     SourceSystem_Name
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ##NULL COUNT CHECK

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Company' AS ColumnName, COUNT(*) AS NullCount FROM bronze.Finance_Account_Recievable WHERE Company IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CompanyName', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE CompanyName IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CorporateCustomerId', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE CorporateCustomerId IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CorporateCustomerName', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE CorporateCustomerName IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CustomerCode', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE CustomerCode IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CustomerName', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE CustomerName IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'OwnerProfitCenter', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE OwnerProfitCenter IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'RevenueProfitCenter', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE RevenueProfitCenter IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'InvoiceNumber', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE InvoiceNumber IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'InvoiceUserDefined1', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE InvoiceUserDefined1 IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'InvoiceDate', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE InvoiceDate IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_OrigAdjAmt', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAgeInv_OrigAdjAmt IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_OrigAmt', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAgeInv_OrigAmt IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_OrigAppliedAmt', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAgeInv_OrigAppliedAmt IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_OrigOpenAmt', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAgeInv_OrigOpenAmt IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_PastDueAmt', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAgeInv_PastDueAmt IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_PastDueCnt', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAgeInv_PastDueCnt IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigCurrent', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAge_OrigCurrent IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigFifteen', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAge_OrigFifteen IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigThirty', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAge_OrigThirty IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigFortyFive', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAge_OrigFortyFive IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigSixty', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAge_OrigSixty IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigNinety', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAge_OrigNinety IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigOneHundredTwenty', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAge_OrigOneHundredTwenty IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigOneHundredTwentyOne', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArAge_OrigOneHundredTwentyOne IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigCurrent', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArPstDue_OrigCurrent IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigFifteen', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArPstDue_OrigFifteen IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigThirty', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArPstDue_OrigThirty IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigFortyFive', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArPstDue_OrigFortyFive IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigSixty', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArPstDue_OrigSixty IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigNinety', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArPstDue_OrigNinety IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigOneHundredTwenty', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArPstDue_OrigOneHundredTwenty IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigOneHundredTwentyOne', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE ArPstDue_OrigOneHundredTwentyOne IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'SnapshotDateTime', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE SnapshotDateTime IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'SourceSystem_ID', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE SourceSystem_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'SourceSystem_Name', COUNT(*) FROM bronze.Finance_Account_Recievable WHERE SourceSystem_Name IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     -- Company,
# MAGIC     -- CompanyName,
# MAGIC     CorporateCustomerId,
# MAGIC     CorporateCustomerName,
# MAGIC     -- CustomerCode,
# MAGIC     -- CustomerName,
# MAGIC     OwnerProfitCenter,
# MAGIC     RevenueProfitCenter,
# MAGIC     InvoiceNumber,
# MAGIC     InvoiceUserDefined1,
# MAGIC     -- InvoiceDate,
# MAGIC     -- ArAgeInv_OrigAdjAmt,
# MAGIC     -- ArAgeInv_OrigAmt,
# MAGIC     -- ArAgeInv_OrigAppliedAmt,
# MAGIC     -- ArAgeInv_OrigOpenAmt,
# MAGIC     -- ArAgeInv_PastDueAmt,
# MAGIC     -- ArAgeInv_PastDueCnt,
# MAGIC     -- ArAge_OrigCurrent,
# MAGIC     -- ArAge_OrigFifteen,
# MAGIC     -- ArAge_OrigThirty,
# MAGIC     -- ArAge_OrigFortyFive,
# MAGIC     -- ArAge_OrigSixty,
# MAGIC     -- ArAge_OrigNinety,
# MAGIC     -- ArAge_OrigOneHundredTwenty,
# MAGIC     -- ArAge_OrigOneHundredTwentyOne,
# MAGIC     -- ArPstDue_OrigCurrent,
# MAGIC     -- ArPstDue_OrigFifteen,
# MAGIC     -- ArPstDue_OrigThirty,
# MAGIC     -- ArPstDue_OrigFortyFive,
# MAGIC     -- ArPstDue_OrigSixty,
# MAGIC     -- ArPstDue_OrigNinety,
# MAGIC     -- ArPstDue_OrigOneHundredTwenty,
# MAGIC     -- ArPstDue_OrigOneHundredTwentyOne,
# MAGIC     -- SnapshotDateTime,
# MAGIC     -- SourceSystem_ID,
# MAGIC     -- SourceSystem_Name,
# MAGIC     COUNT(*) AS DuplicateCount
# MAGIC FROM bronze.Finance_Account_Recievable
# MAGIC GROUP BY 
# MAGIC     -- Company,
# MAGIC     -- CompanyName,
# MAGIC     CorporateCustomerId,
# MAGIC     CorporateCustomerName,
# MAGIC     -- CustomerCode,
# MAGIC     -- CustomerName,
# MAGIC     OwnerProfitCenter,
# MAGIC     RevenueProfitCenter,
# MAGIC     InvoiceNumber,
# MAGIC     InvoiceUserDefined1
# MAGIC     -- InvoiceDate
# MAGIC     -- ArAgeInv_OrigAdjAmt,
# MAGIC     -- ArAgeInv_OrigAmt,
# MAGIC     -- ArAgeInv_OrigAppliedAmt,
# MAGIC     -- ArAgeInv_OrigOpenAmt,
# MAGIC     -- ArAgeInv_PastDueAmt,
# MAGIC     -- ArAgeInv_PastDueCnt,
# MAGIC     -- ArAge_OrigCurrent,
# MAGIC     -- ArAge_OrigFifteen,
# MAGIC     -- ArAge_OrigThirty,
# MAGIC     -- ArAge_OrigFortyFive,
# MAGIC     -- ArAge_OrigSixty,
# MAGIC     -- ArAge_OrigNinety,
# MAGIC     -- ArAge_OrigOneHundredTwenty,
# MAGIC     -- ArAge_OrigOneHundredTwentyOne,
# MAGIC     -- ArPstDue_OrigCurrent,
# MAGIC     -- ArPstDue_OrigFifteen,
# MAGIC     -- ArPstDue_OrigThirty,
# MAGIC     -- ArPstDue_OrigFortyFive,
# MAGIC     -- ArPstDue_OrigSixty,
# MAGIC     -- ArPstDue_OrigNinety,
# MAGIC     -- ArPstDue_OrigOneHundredTwenty,
# MAGIC     -- ArPstDue_OrigOneHundredTwentyOne,
# MAGIC     -- SnapshotDateTime,
# MAGIC     -- SourceSystem_ID,
# MAGIC     -- SourceSystem_Name
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.finance_account_recievable where InvoiceNumber in ('N1948291',
# MAGIC 'N1948289',
# MAGIC 'N1947909',
# MAGIC 'N1941612')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.finance_account_recievable order by InvoiceDate desc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where table_id = 'AR-1'

# COMMAND ----------

# MAGIC %md
# MAGIC ##NULL COUNT CHECK IN SOURCE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Company' AS ColumnName, COUNT(*) AS NullCount FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [Company] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CompanyName', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [CompanyName] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CorporateCustomerId', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [CorporateCustomerId] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CorporateCustomerName', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [CorporateCustomerName] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CustomerCode', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [CustomerCode] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'CustomerName', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [CustomerName] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'OwnerProfitCenter', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [OwnerProfitCenter] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'RevenueProfitCenter', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [RevenueProfitCenter] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'InvoiceNumber', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [InvoiceNumber] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'InvoiceUserDefined1', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [InvoiceUserDefined1] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'InvoiceDate', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [InvoiceDate] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_OrigAdjAmt', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAgeInv_OrigAdjAmt] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_OrigAmt', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAgeInv_OrigAmt] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_OrigAppliedAmt', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAgeInv_OrigAppliedAmt] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_OrigOpenAmt', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAgeInv_OrigOpenAmt] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_PastDueAmt', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAgeInv_PastDueAmt] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAgeInv_PastDueCnt', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAgeInv_PastDueCnt] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigCurrent', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAge_OrigCurrent] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigFifteen', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAge_OrigFifteen] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigThirty', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAge_OrigThirty] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigFortyFive', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAge_OrigFortyFive] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigSixty', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAge_OrigSixty] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigNinety', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAge_OrigNinety] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigOneHundredTwenty', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAge_OrigOneHundredTwenty] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArAge_OrigOneHundredTwentyOne', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArAge_OrigOneHundredTwentyOne] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigCurrent', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArPstDue_OrigCurrent] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigFifteen', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArPstDue_OrigFifteen] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigThirty', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArPstDue_OrigThirty] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigFortyFive', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArPstDue_OrigFortyFive] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigSixty', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArPstDue_OrigSixty] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigNinety', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArPstDue_OrigNinety] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigOneHundredTwenty', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArPstDue_OrigOneHundredTwenty] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ArPstDue_OrigOneHundredTwentyOne', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [ArPstDue_OrigOneHundredTwentyOne] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'SnapshotDateTime', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [SnapshotDateTime] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'SourceSystem_ID', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [SourceSystem_ID] IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'SourceSystem_Name', COUNT(*) FROM [dbo].[vwArSelectAllAging_Comp200] WHERE [SourceSystem_Name] IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## DUPLICATE CHECK IN SOURCE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     [Company],
# MAGIC     [CompanyName],
# MAGIC     [CorporateCustomerId],
# MAGIC     [CorporateCustomerName],
# MAGIC     [CustomerCode],
# MAGIC     [CustomerName],
# MAGIC     [OwnerProfitCenter],
# MAGIC     [RevenueProfitCenter],
# MAGIC     [InvoiceNumber],
# MAGIC     [InvoiceUserDefined1],
# MAGIC     [InvoiceDate],
# MAGIC     [ArAgeInv_OrigAdjAmt],
# MAGIC     [ArAgeInv_OrigAmt],
# MAGIC     [ArAgeInv_OrigAppliedAmt],
# MAGIC     [ArAgeInv_OrigOpenAmt],
# MAGIC     [ArAgeInv_PastDueAmt],
# MAGIC     [ArAgeInv_PastDueCnt],
# MAGIC     [ArAge_OrigCurrent],
# MAGIC     [ArAge_OrigFifteen],
# MAGIC     [ArAge_OrigThirty],
# MAGIC     [ArAge_OrigFortyFive],
# MAGIC     [ArAge_OrigSixty],
# MAGIC     [ArAge_OrigNinety],
# MAGIC     [ArAge_OrigOneHundredTwenty],
# MAGIC     [ArAge_OrigOneHundredTwentyOne],
# MAGIC     [ArPstDue_OrigCurrent],
# MAGIC     [ArPstDue_OrigFifteen],
# MAGIC     [ArPstDue_OrigThirty],
# MAGIC     [ArPstDue_OrigFortyFive],
# MAGIC     [ArPstDue_OrigSixty],
# MAGIC     [ArPstDue_OrigNinety],
# MAGIC     [ArPstDue_OrigOneHundredTwenty],
# MAGIC     [ArPstDue_OrigOneHundredTwentyOne],
# MAGIC     [SnapshotDateTime],
# MAGIC     COUNT(*) AS DuplicateCount
# MAGIC FROM [dbo].[vwArSelectAllAging_Comp200]
# MAGIC GROUP BY 
# MAGIC     [Company],
# MAGIC     [CompanyName],
# MAGIC     [CorporateCustomerId],
# MAGIC     [CorporateCustomerName],
# MAGIC     [CustomerCode],
# MAGIC     [CustomerName],
# MAGIC     [OwnerProfitCenter],
# MAGIC     [RevenueProfitCenter],
# MAGIC     [InvoiceNumber],
# MAGIC     [InvoiceUserDefined1],
# MAGIC     [InvoiceDate],
# MAGIC     [ArAgeInv_OrigAdjAmt],
# MAGIC     [ArAgeInv_OrigAmt],
# MAGIC     [ArAgeInv_OrigAppliedAmt],
# MAGIC     [ArAgeInv_OrigOpenAmt],
# MAGIC     [ArAgeInv_PastDueAmt],
# MAGIC     [ArAgeInv_PastDueCnt],
# MAGIC     [ArAge_OrigCurrent],
# MAGIC     [ArAge_OrigFifteen],
# MAGIC     [ArAge_OrigThirty],
# MAGIC     [ArAge_OrigFortyFive],
# MAGIC     [ArAge_OrigSixty],
# MAGIC     [ArAge_OrigNinety],
# MAGIC     [ArAge_OrigOneHundredTwenty],
# MAGIC     [ArAge_OrigOneHundredTwentyOne],
# MAGIC     [ArPstDue_OrigCurrent],
# MAGIC     [ArPstDue_OrigFifteen],
# MAGIC     [ArPstDue_OrigThirty],
# MAGIC     [ArPstDue_OrigFortyFive],
# MAGIC     [ArPstDue_OrigSixty],
# MAGIC     [ArPstDue_OrigNinety],
# MAGIC     [ArPstDue_OrigOneHundredTwenty],
# MAGIC     [ArPstDue_OrigOneHundredTwentyOne],
# MAGIC     [SnapshotDateTime]
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Row Count Check In source

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS RowCount
# MAGIC FROM [dbo].[vwArSelectAllAging_Comp200];

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check in Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from [dbo].[vwArSelectAllAging_Comp200] order by [InvoiceDate] desc limit 10