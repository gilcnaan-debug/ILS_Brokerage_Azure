# Databricks notebook source
# MAGIC %md
# MAGIC ####Metadata Scripts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT INTO METADATA.MASTERMETADATA (
# MAGIC --     SourceSystem,
# MAGIC --     SourceSecretName,
# MAGIC --     SubjectArea,
# MAGIC --     SourceDBName,
# MAGIC --     SourceSchema,
# MAGIC --     SourceTableName,
# MAGIC --     LoadType,
# MAGIC --     IsActive,
# MAGIC --     Frequency,
# MAGIC --     StagePath,
# MAGIC --     RawPath,
# MAGIC --     CuratedPath,
# MAGIC --     DWHSchemaName,
# MAGIC --     DWHTableName,
# MAGIC --     ErrorLogPath,
# MAGIC --     LastLoadDateColumn,
# MAGIC --     LastLoadDateValue,
# MAGIC --     MergeKey,
# MAGIC --     DependencyTableIDs,
# MAGIC --     PipelineStartTime,
# MAGIC --     PipelineEndTime,
# MAGIC --     PipelineRunStatus,
# MAGIC --     Zone,
# MAGIC --     MergeKeyColumn,
# MAGIC --     SourceSelectQuery,
# MAGIC --     Notebook_Name,
# MAGIC --     TableID
# MAGIC -- ) VALUES
# MAGIC -- ('Aljex', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_Aljex', NULL, 'updated_at', '1999-01-01', 'Id', NULL, NULL, NULL, NULL, 'Silver', 'Id', NULL, 'Silver_Aljex', 'SL2'),
# MAGIC -- ('Relay', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_Relay_Customer_Money', NULL, 'updated_at', '1999-01-01', 'Load_ID', NULL, NULL, NULL, NULL, 'Silver', 'Load_ID', NULL, 'Silver_Relay_Customer_Money', 'SL3'),
# MAGIC -- ('Relay', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_Relay_Carrier_Money', NULL, 'updated_at', '1999-01-01', 'Load_ID', NULL, NULL, NULL, NULL, 'Silver', 'Load_ID', NULL, 'Silver_Relay_Carrier_Money', 'SL4'),
# MAGIC -- ('Relay', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_Relay', NULL, 'updated_at', '1999-01-01', 'Load_ID', NULL, NULL, NULL, NULL, 'Silver', 'Load_ID', NULL, 'Silver_Relay', 'SL5'),
# MAGIC -- ('Trasfix', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_Transfix', NULL, 'updated_at', '1999-01-01', 'Load_ID', NULL, NULL, NULL, NULL, 'Silver', 'Load_ID', NULL, 'Silver_Transfix', 'SL6'),
# MAGIC -- ('TMS', NULL, 'TMS', 'Databricks', 'Silver', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Gold', 'Fact_Loads_By_Billing', NULL, 'Last_Modified_Date', '1999-01-01', 'DW_Load_ID', NULL, NULL, NULL, NULL, 'Gold', 'DW_Load_ID', NULL, 'Fact_Loads_By_Billing', 'GL1'),
# MAGIC -- ('TMS', NULL, 'TMS', 'Databricks', 'Silver', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Gold', 'Fact_Loads_Split', NULL, 'Last_Modified_Date', '1999-01-01', 'DW_Load_ID', NULL, NULL, NULL, NULL, 'Gold', 'DW_Load_ID', NULL, 'Fact_Loads_Split', 'GL2'),
# MAGIC -- ('TMS', NULL, 'TMS', 'Databricks', 'Silver', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Gold', 'Fact_Loads_NonSplit',NULL,'Last_Modified_Date',' 1999-01-01','DW_Load_ID',NULL,NULL,NULL,NULL,'Gold','DW_Load_ID',NULL,'Fact_Loads_NonSplit','GL3');
# MAGIC
# MAGIC