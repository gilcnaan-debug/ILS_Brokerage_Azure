# Databricks notebook source
# MAGIC %md
# MAGIC ##Metadata Table Scripts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.mastermetadata (
# MAGIC     SourceSystem,
# MAGIC     TableID,
# MAGIC     SubjectArea,
# MAGIC     SourceDBName,
# MAGIC     SourceSchema,
# MAGIC     SourceTableName,
# MAGIC     LoadType,
# MAGIC     IsActive,
# MAGIC     Frequency,
# MAGIC     LastLoadDateColumn,
# MAGIC     LastLoadDateValue,
# MAGIC     MergeKey,
# MAGIC     Zone,
# MAGIC     SourceSelectQuery
# MAGIC     )
# MAGIC VALUES (
# MAGIC     'Azure Blob Storage',
# MAGIC     'F13',
# MAGIC     'Finance',
# MAGIC     'Finance',
# MAGIC     'Finance Prod',
# MAGIC     'Finance_GLDetail',
# MAGIC     'Incremental Load',
# MAGIC     '1',
# MAGIC     '24 hrs',
# MAGIC     'Timestamp from File Name',
# MAGIC     '2024-07-03 00:00:00',
# MAGIC     'OBJ_ID',
# MAGIC     'Bronze',         
# MAGIC     'COMPANY, FISCAL_YEAR, ACCT_PERIOD, JE_NUMBER, SOURCE_SYSTEM, JE_TYPE, ACCT_UNIT, ACCT_UNIT_DESCR, ACCT_SUB, ACCOUNT_DESC, SOURCE_CODE, REFERENCE, SOURCE_DESC, AUTO_REV, POSTING_DATE, TRAN_AMOUNT, BASE_AMOUNT, RPT_AMOUNT_1, UNITS_AMOUNT, TRANS_DESC, CURRENCY_CODE, OBJ_ID'
# MAGIC ); 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.mastermetadata (
# MAGIC     SourceSystem,
# MAGIC     TableID,
# MAGIC     SubjectArea,
# MAGIC     SourceDBName,
# MAGIC     SourceSchema,
# MAGIC     SourceTableName,
# MAGIC     LoadType,
# MAGIC     IsActive,
# MAGIC     Frequency,
# MAGIC     LastLoadDateColumn,
# MAGIC     LastLoadDateValue,
# MAGIC     MergeKey,
# MAGIC     Zone,
# MAGIC     SourceSelectQuery
# MAGIC     )
# MAGIC VALUES (
# MAGIC     'Azure Blob Storage',
# MAGIC     'F14',
# MAGIC     'Finance',
# MAGIC     'Finance',
# MAGIC     'Finance Prod',
# MAGIC     'Finance_GLSummary',
# MAGIC     'Incremental Load',
# MAGIC     '1',
# MAGIC     '24 hrs',
# MAGIC     'Timestamp from File Name',
# MAGIC     '2024-07-03 00:00:00',
# MAGIC     'OBJ_ID',
# MAGIC     'Bronze',         
# MAGIC     'COMPANY, ACCT_UNIT, ACCT_UNIT_DESCR, ACCT_SUB, ACCOUNT_DESC, ACCT_PERIOD, FISCAL_YEAR, CURRENCY_CODE, TRAN_AMOUNT, BASE_AMOUNT, RPT_AMOUNT_1, UNITS_AMOUNT, OBJ_ID'
# MAGIC ); 

# COMMAND ----------

