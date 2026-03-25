# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Bronze.Lookup_Address_Awards (
# MAGIC     Addresskey STRING NOT NULL,
# MAGIC     Country_Code VARCHAR(5),
# MAGIC     Country_Name VARCHAR(100),
# MAGIC     Zip VARCHAR(10),
# MAGIC     Place_Name VARCHAR(255),
# MAGIC     Division VARCHAR(100),
# MAGIC     State_Name VARCHAR(255),
# MAGIC     State_Code VARCHAR(5),
# MAGIC     Latitude FLOAT,
# MAGIC     Longitude FLOAT,
# MAGIC     Accuracy INT,   
# MAGIC     Is_Deleted SMALLINT NOT NULL,
# MAGIC     Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Created_By VARCHAR(255) NOT NULL,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC     CONSTRAINT PK_Lookup_Address_Awards PRIMARY KEY (Addresskey)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Metadata.MasterMetadata(
# MAGIC     SourceSystem, SourceSecretName, TableID, SubjectArea, SourceDBName, SourceSchema, 
# MAGIC     SourceTableName, LoadType, IsActive, Frequency, StagePath, RawPath, CuratedPath, 
# MAGIC     DWHSchemaName, DWHTableName, ErrorLogPath, LastLoadDateColumn, MergeKey, 
# MAGIC     DependencyTableIDs, PipelineEndTime, PipelineStartTime, PipelineRunStatus, Zone, 
# MAGIC     MergeKeyColumn, SourceSelectQuery, LastLoadDateValue, UnixTime, JOB_ID, NB_ID, 
# MAGIC     Job_Name, Notebook_Name
# MAGIC ) VALUES 
# MAGIC ('Geonames', 'NA', 'LA1', 'Address Automation', 'Zip File', 'Zip File', 'Zip File', 'Incremental Load', 1, 'Once a Week', NULL, NULL, NULL, 'Bronze', 'Lookup_Address_Awards', NULL, 'Last_Modified_Date', NULL, NULL, '1970-01-01 00:00:00', '1970-01-01 00:00:00', 'Succeeded', 'Bronze', 'Addresskey', NULL, NULL, NULL, 'JOB_LA1', 'NB27', 'NFI_Lookup_Address', 'Lookup_Address_Automation')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE FROM metadata.mastermetadata WHERE TableID = 'LA1'