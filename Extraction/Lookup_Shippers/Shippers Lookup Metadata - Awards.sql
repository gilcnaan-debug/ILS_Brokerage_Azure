-- Databricks notebook source
INSERT INTO  Metadata.MasterMetadata(
    SourceSystem, SourceSecretName, TableID, SubjectArea, LoadType, IsActive, Frequency, StagePath, RawPath, CuratedPath, 
    DWHSchemaName, DWHTableName, ErrorLogPath, Zone, 
    MergeKeyColumn, SourceSelectQuery, LastLoadDateValue, UnixTime, JOB_ID, NB_ID, 
    Job_Name, Notebook_Name
) VALUES 
('Operational Data', '', 'SL1', 'Shippers Lookup', 'Incremental load', 1, NULL, NULL, NULL, NULL, 'Bronze', 'lookup_shippers', NULL, 'Bronze', NULL, NULL, NULL, NULL, 'JOB_SL1', 'NB28', NULL, 'Shippers Lookup')