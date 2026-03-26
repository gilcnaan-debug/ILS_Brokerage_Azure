-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Delete Statement for Existing Awards Metadata

-- COMMAND ----------

DELETE FROM metadata.mastermetadata where tableid IN('AW1','AW2','AW3', 'AW4', 'AW5', 'AW6', 'AW7', 'AW8') 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Insert Script for Metadata

-- COMMAND ----------



INSERT INTO  Metadata.MasterMetadata(
    SourceSystem, SourceSecretName, TableID, SubjectArea, SourceDBName, SourceSchema, 
    SourceTableName, LoadType, IsActive, Frequency, StagePath, RawPath, CuratedPath, 
    DWHSchemaName, DWHTableName, ErrorLogPath, LastLoadDateColumn, MergeKey, 
    DependencyTableIDs, PipelineEndTime, PipelineStartTime, PipelineRunStatus, Zone, 
    MergeKeyColumn, SourceSelectQuery, LastLoadDateValue, UnixTime, JOB_ID, NB_ID, 
    Job_Name, Notebook_Name
) VALUES 
('Google Drive', '', 'AW1', 'Awards', 'Google Drive', 'Google Sheets', 'NFI_Awards', 'Incremental load', 1, NULL, NULL, NULL, NULL, 'Bronze', 'NFI_Awards', NULL, 'Updated_date', 'RFP_Name,Customer_Slug,SAM_Name,Lane_Description,Origin_Facility,Origin_Address,Origin_City,Origin_State,Origin_Zip,Origin_Country,Destination_Facility,Destination_Address,Destination_City,Destination_State,Destination_Zip,Destination_Country,Origin_Live_Drop,Destination_Live_Drop,Equipment_Type,Estimated_Volume,Estimated_Miles,Linehaul,RPM,Flat_Rate,Min_Rate,Max_Rate,Validation_Criteria,Award_Type,Award_Notes,Periodic_Start_Date,Periodic_End_Date,Shipper_Recurrence,Updated_by,Updated_date,Contract_Type,Expected Margin', NULL, NULL, NULL, 'Succeeded', 'Bronze', NULL, NULL, NULL, NULL, 'JOB_AW1', 'NB19', NULL, 'Awards_Extraction'),
('Google Drive', '', 'AW2', 'Awards', 'Brokerage', 'Bronze', 'NFI_Awards',  'Incremental load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_NFI_Awards_Filtered', NULL, 'Last_Modified_Date', NULL, NULL, NULL, NULL, NULL, 'Silver', NULL, "SELECT DISTINCT 
    DW_Award_ID,
    TRIM(RFP_Name) AS RFP_Name,
    TRIM(Customer_Master) AS Customer_Master,
    TRIM(Customer_Slug) AS Customer_Slug,
    TRIM(SAM_Name) AS SAM_Name,
    CASE 
        WHEN LOWER(REPLACE(TRIM(Validation_Criteria), ' ', '')) LIKE '5digit-%' THEN UPPER(REPLACE(TRIM(Origin_Zip), ' ', ''))
        ELSE CONCAT(COALESCE(UPPER(TRIM(Origin_City)), 'None'), ',', COALESCE(UPPER(TRIM(Origin_State)), 'None')) 
    END AS Origin,
    TRIM(UPPER(Origin_Country)) as Origin_Country,
    CASE 
        WHEN LOWER(REPLACE(TRIM(Validation_Criteria), ' ', '')) LIKE '%-5digit' THEN UPPER(REPLACE(TRIM(Destination_Zip), ' ', ''))
        ELSE CONCAT(COALESCE(UPPER(TRIM(Destination_City)), 'None'), ',', COALESCE(UPPER(TRIM(Destination_State)), 'None')) 
    END AS Destination,
    TRIM(UPPER(Destination_Country)) AS Destination_Country,
    UPPER(TRIM(Equipment_Type)) AS Equipment_Type,
    Estimated_Volume,
    Estimated_Miles,
    Linehaul,
    RPM,
    Flat_Rate,
    Min_Rate,
    Max_Rate,
    -- For Validation_Criteria, trimming and converting to lowercase
    LOWER(REPLACE(TRIM(Validation_Criteria), ' ', '')) AS Validation_Criteria,
    TRIM(Award_Type) AS Award_Type,
    TRIM(Award_Notes) AS Award_Notes,
    Periodic_Start_Date,
    Periodic_End_Date,
    TRIM(Shipper_Recurrence) AS Shipper_Recurrence,
    TRIM(updated_by) AS updated_by,
    Updated_date,
    INITCAP(TRIM(Contract_Type)) AS Contract_Type,
    Expected_Margin
FROM 
    Bronze.NFI_Awards
WHERE 
    LOWER(REPLACE(TRIM(Validation_Criteria), ' ', '')) IN ('5digit-5digit', 'city-city') 
    AND INITCAP(TRIM(Contract_Type)) IN ('Primary', 'Secondary', 'Backup') 
    AND Status = 'I' 
    AND Last_Modified_Date > '{0}';", "2021-10-02 10:45:35.000", NULL, 'JOB_AW2', 'NB20', NULL, 'Awards_Silver_Filtered'),
('Google Drive', '', 'AW3','Awards', 'Brokerage', 'Silver', 'Silver_NFI_Awards_Filtered', 'Incremental load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_NFI_Awards_Validated', NULL, 'Last_Modified_Date', NULL, NULL, NULL, NULL, NULL, 'Silver', NULL, NULL, NULL, NULL, 'JOB_AW3', 'NB21', NULL, 'Awards_Silver_Validation_&_Exceptions'),
('Google Drive', '', 'AW4', 'Awards', 'Brokerage', 'Silver', 'Silver_NFI_Awards_Filtered','Incremental load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_NFI_Awards_Exceptions', NULL, 'Last_Modified_Date', NULL, NULL, NULL, NULL, NULL, 'Silver', NULL, NULL, NULL, NULL, 'JOB_AW4', 'NB22', NULL, 'Awards_Silver_Validation_&_Exceptions'),
('Google Drive', '', 'AW5', 'Awards', 'Brokerage', 'Silver', 'Silver_NFI_Awards_Validated', 'Incremental load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_NFI_Awards_Time_Table', NULL, 'Last_Modified_Date', NULL, NULL, NULL, NULL, NULL, 'Silver', NULL, NULL, '2021-10-02 10:45:35.000', NULL, 'JOB_AW5', 'NB23', NULL, 'Awards_Silver_TimeTable'),
('Google Drive', '', 'AW6', 'Awards', 'Brokerage', 'Silver', 'Silver_NFI_Awards_Validated', 'Incremental load', 1, NULL, NULL, NULL, NULL, 'Silver', 'Silver_NFI_Awards_Zips', NULL, 'Last_Modified_Date', NULL, NULL, NULL, NULL, NULL, 'Silver', NULL, NULL, '2021-10-02 10:45:35.000', NULL, 'JOB_AW6', 'NB24', NULL, 'Awards_Silver_Zips'),
('Google Drive', '', 'AW7','Awards', 'Brokerage', 'Silver', 'Silver_NFI_Awards_Zips, Silver_NFI_Awards_Time_Table', 'Incremental Load',  1, NULL, NULL, NULL, NULL, 'Gold', 'Fact_Awards_Consolidated', NULL, 'Last_Modified_Date', NULL, NULL, NULL, NULL, NULL, 'Gold', NULL, NULL, '2021-10-02 10:45:35.000', NULL, 'JOB_AW7', 'NB25', NULL, 'Awards_Gold_Consolidated'),
('Google Drive', '', 'AW8', 'Awards' , 'Brokerage', 'Gold', 'Fact_Awards_Consolidated', 'Incremental load', 1, NULL, NULL, NULL, NULL, 'Gold', 'Fact_Awards_Summary', NULL, 'Last_Modified_Date', NULL, NULL, NULL, NULL, NULL, 'Gold', NULL, NULL, NULL, NULL, 'JOB_AW8', 'NB26', NULL, 'Awards_Gold_Summary');

