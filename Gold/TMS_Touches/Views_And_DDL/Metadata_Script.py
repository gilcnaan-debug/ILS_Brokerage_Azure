# Databricks notebook source
# MAGIC  %sql
# MAGIC -- # INSERT INTO METADATA.MASTERMETADATA (
# MAGIC -- #     SourceSystem,
# MAGIC -- #     SourceSecretName,
# MAGIC -- #     SubjectArea,
# MAGIC -- #     SourceDBName,
# MAGIC -- #     SourceSchema,
# MAGIC -- #     SourceTableName,
# MAGIC -- #     LoadType,
# MAGIC -- #     IsActive,
# MAGIC -- #     Frequency,
# MAGIC -- #     StagePath,
# MAGIC -- #     RawPath,
# MAGIC -- #     CuratedPath,
# MAGIC -- #     DWHSchemaName,
# MAGIC -- #     DWHTableName,
# MAGIC -- #     ErrorLogPath,
# MAGIC -- #     LastLoadDateColumn,
# MAGIC -- #     LastLoadDateValue,
# MAGIC -- #     MergeKey,
# MAGIC -- #     DependencyTableIDs,
# MAGIC -- #     PipelineStartTime,
# MAGIC -- #     PipelineEndTime,
# MAGIC -- #     PipelineRunStatus,
# MAGIC -- #     Zone,
# MAGIC -- #     MergeKeyColumn,
# MAGIC -- #     SourceSelectQuery,
# MAGIC -- #     Notebook_Name,
# MAGIC -- #     TableID
# MAGIC -- # ) VALUES
# MAGIC -- # ('Aljex_Touches', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Sliver_Touch_Aljex', NULL, 'updated_at', '1999-01-01', 'DW_Touch_ID', NULL, NULL, NULL, NULL, 'Silver', 'DW_Touch_ID', NULL, 'Sliver_Touch_Aljex', 'SL9'),
# MAGIC -- # ('Relay_Touches', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Sliver_Touch_Relay', NULL, 'updated_at', '1999-01-01', 'DW_Touch_ID', NULL, NULL, NULL, NULL, 'Silver', 'DW_Touch_ID', NULL, 'Sliver_Touch_Relay', 'SL10'),
# MAGIC -- # ('HubSpot_Touches', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Sliver_Touch_Hubspot', NULL, 'updated_at', '1999-01-01', 'DW_Touch_ID', NULL, NULL, NULL, NULL, 'Silver', 'DW_Touch_ID', NULL, 'Sliver_Touch_Hubspot', 'SL11'),
# MAGIC -- #  ('Slack_Touches', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Sliver_Touches_Slack', NULL, 'updated_at', '1999-01-01', 'DW_Touch_ID', NULL, NULL, NULL, NULL, 'Silver', 'DW_Touch_ID', NULL, 'Sliver_Touches_Slack', 'SL12');
# MAGIC
# MAGIC
# MAGIC -- UPDATE METADATA.MASTERMETADATA
# MAGIC -- SET TableID = 'SL9'
# MAGIC -- WHERE SourceSystem = 'Aljex_Touches';
# MAGIC
# MAGIC -- UPDATE METADATA.MASTERMETADATA
# MAGIC -- SET TableID = 'SL10'
# MAGIC -- WHERE SourceSystem = 'Relay_Touches';
# MAGIC
# MAGIC -- UPDATE METADATA.MASTERMETADATA
# MAGIC -- SET Notebook_Name = 'Sliver_Touch_Relay' , DWHTableName = 'Relay_Touches'
# MAGIC -- WHERE SourceSystem = 'Relay_Touches';
# MAGIC
# MAGIC -- UPDATE METADATA.MASTERMETADATA
# MAGIC -- SET TableID = 'SL11'
# MAGIC -- WHERE SourceSystem = 'HubSpot_Touches';
# MAGIC
# MAGIC -- UPDATE METADATA.MASTERMETADATA
# MAGIC -- SET TableID = 'SL12'
# MAGIC -- WHERE SourceSystem = 'Slack_Touches';
# MAGIC
# MAGIC --  INSERT INTO METADATA.MASTERMETADATA (
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
# MAGIC --      DWHSchemaName,
# MAGIC --      DWHTableName,
# MAGIC --      ErrorLogPath,
# MAGIC --      LastLoadDateColumn,
# MAGIC --      LastLoadDateValue,
# MAGIC --      MergeKey,
# MAGIC --      DependencyTableIDs,
# MAGIC --      PipelineStartTime,
# MAGIC --      PipelineEndTime,
# MAGIC --      PipelineRunStatus,
# MAGIC --      Zone,
# MAGIC --      MergeKeyColumn,
# MAGIC --      SourceSelectQuery,
# MAGIC --      Notebook_Name,
# MAGIC --      TableID
# MAGIC --  ) VALUES
# MAGIC --  ('Touches_Gold', NULL, 'TMS', 'Databricks', 'Bronze', NULL, 'Incremental Load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Fact_Touches_Gold', NULL, 'updated_at', '1999-01-01', 'DW_Touch_ID', NULL, NULL, NULL, NULL, 'Silver', 'DW_Touch_ID', NULL, 'Fact_Touches_Gold', 'Gl5')
# MAGIC
# MAGIC UPDATE METADATA.MASTERMETADATA
# MAGIC SET TableID = 'GL5'
# MAGIC WHERE SourceSystem = 'Touches_Gold';
# MAGIC