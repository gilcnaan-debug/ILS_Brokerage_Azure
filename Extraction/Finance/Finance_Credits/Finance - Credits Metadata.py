# Databricks notebook source
# MAGIC %md
# MAGIC ### Metadata

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
# MAGIC     SourceSelectQuery,
# MAGIC     JOB_ID,
# MAGIC     NB_ID,
# MAGIC     Job_Name
# MAGIC     )
# MAGIC VALUES (
# MAGIC     'Azure Blob Storage',
# MAGIC     'F15',
# MAGIC     'Finance',
# MAGIC     'Finance',
# MAGIC     'Finance Credits',
# MAGIC     'Finance_Credits',
# MAGIC     'Incremental Load',
# MAGIC     '1',
# MAGIC     '24 hrs',
# MAGIC     'Timestamp from File Name',
# MAGIC     '2024-07-03 00:00:00',
# MAGIC     'CREDIT_KEY',
# MAGIC     'Bronze',         
# MAGIC     '"CUSTOMER", "SEARCH_NAME", "COMPANY", "ADDR1", "ADDR2", "CITY", "STATE", "COUNTRY_CODE", "ZIP", "START_DATE", "ACTIVE_STATUS", "CREDIT_LIM", "CURRENCY_CD", "TERMS_CD", "TERM_RDESC_01"',
# MAGIC     'JOB_F14',
# MAGIC     'NB56',
# MAGIC     'NFI_Finance_Bronze'
# MAGIC ); 