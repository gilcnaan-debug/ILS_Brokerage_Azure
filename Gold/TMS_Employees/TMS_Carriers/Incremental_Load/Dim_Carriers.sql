-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Dim Carriers
-- MAGIC * **Description:** Load from Silver to Gold
-- MAGIC * **Created Date:** 03/07/2025
-- MAGIC * **Created By:** Uday
-- MAGIC * **Modified Date:** 06/10/2025
-- MAGIC * **Modified By:** Uday
-- MAGIC * **Changes Made:** Logic Update

-- COMMAND ----------

UPDATE metadata.mastermetadata 
SET PipelineStartTime = current_timestamp()
WHERE TableID = 'GL4'

-- COMMAND ----------

MERGE INTO Gold.Dim_Carriers shc
USING GOld.VW_Dim_Carrier AS hc
ON hc.DW_Carrier_ID = shc.DW_Carrier_ID
WHEN MATCHED AND shc.HashKey != hc.HashKey THEN
    UPDATE SET   
        shc.DW_HubSpot_Carrier_ID = hc.DW_HubSpot_Carrier_ID, 
        shc.Carrier_DOT = hc.Carrier_DOT,
        shc.Carrier_MC = hc.Carrier_MC,
        shc.Aljex_Carrier_ID = hc.Aljex_Carrier_ID,
        shc.Relay_Carrier_ID = hc.Relay_Carrier_ID,
        shc.Carrier_Name = hc.Carrier_Name,
        shc.HubSpot_Account = hc.HubSpot_Account,
        shc.Carrier_Vertical_Industry = hc.Carrier_Vertical_Industry,
        shc.Address_Line_1 = hc.Address_Line_1,
        shc.Address_Line_2 = hc.Address_Line_2,
        shc.City = hc.City,
        shc.State = hc.State,
        shc.Country = hc.Country,
        shc.Zip_Code = hc.Zip_Code,
        shc.Carrier_Email_Address = hc.Carrier_Email_Address,
        shc.Carrier_Phone_Number = hc.Carrier_Phone_Number,
        shc.Tracking_Platform = hc.Tracking_Platform,
        shc.Tracking_Platform_Status = hc.Tracking_Platform_Status,
        shc.Flatbed_Count = hc.Flatbed_Count,
        shc.Reefer_Count = hc.Reefer_Count,
        shc.Tractor_Count = hc.Tractor_Count,
        shc.Trailer_Count = hc.Trailer_Count,
        shc.Van_Count = hc.Van_Count,
        shc.Is_Active = hc.Is_Active,
        shc.Onboarded_Date = hc.Onboarded_Date,
        shc.Current_Lifecycle_Stage = hc.Current_Lifecycle_Stage,
        shc.Is_Managed_Carrier = hc.Is_Managed_Carrier,
        shc.Managed_Carrier_Converted_Date = hc.Managed_Carrier_Converted_Date,
        shc.Last_Load_Date = hc.Last_Load_Date,
        shc.Carrier_Regular_Office = hc.Carrier_Regular_Office,
        shc.Carrier_Recent_Office = hc.Carrier_Recent_Office, 
        shc.CDR_Rep_Relay_ID = hc.CDR_Rep_Relay_ID,
        shc.CDR_Rep_Aljex_ID = hc.CDR_Rep_Aljex_ID,
        shc.CDR_Rep_Name = hc.CDR_Rep_Name, 
        shc.Carrier_Rep_Aljex_ID = hc.Carrier_Rep_Aljex_ID,
        shc.Carrier_Rep_Relay_ID = hc.Carrier_Rep_Relay_ID ,
        shc.Carrier_Rep_Name = hc.Carrier_Rep_Name,
        shc.Carrier_Rep_Source_Flag = hc.Carrier_Rep_Source_Flag,
        shc.HashKey = hc.HashKey,
        shc.Last_Modified_Date = current_timestamp(),
        shc.Last_Modified_By = 'Databricks'
WHEN MATCHED AND shc.Is_Deleted = 1 AND shc.HashKey = hc.HashKey  THEN 
    UPDATE SET
        shc.Last_Modified_Date = current_timestamp(),
        shc.Last_Modified_By = 'Databricks',
        shc.Is_Deleted = 0
WHEN NOT MATCHED THEN
    INSERT ( 
        DW_Carrier_ID,
        DW_HubSpot_Carrier_ID, 
        Carrier_DOT,
        Carrier_MC,
        Aljex_Carrier_ID,
        Relay_Carrier_ID,
        Carrier_Name,
        HubSpot_Account,
        Carrier_Vertical_Industry,
        Address_Line_1,
        Address_Line_2,
        City,
        State,
        Country,
        Zip_Code,
        Carrier_Email_Address,
        Carrier_Phone_Number,
        Tracking_Platform,
        Tracking_Platform_Status,
        Flatbed_Count,
        Reefer_Count,
        Tractor_Count,
        Trailer_Count,
        Van_Count,
        Is_Active,
        Onboarded_Date,
        Current_Lifecycle_Stage,
        Is_Managed_Carrier,
        Managed_Carrier_Converted_Date,
        Last_Load_Date,
        Carrier_Regular_Office,
        Carrier_Recent_Office, 
        CDR_Rep_Relay_ID,
        CDR_Rep_Aljex_ID,
        CDR_Rep_Name,
        Carrier_Rep_Aljex_ID,
        Carrier_Rep_Relay_ID, 
        Carrier_Rep_Name,    
        HashKey,
        Created_Date,
        Created_By,
        Last_Modified_Date,
        Last_Modified_By,
        Is_Deleted
    )
    VALUES (
        hc.DW_Carrier_ID,
        hc.DW_HubSpot_Carrier_ID,
        hc.Carrier_DOT,
        hc.Carrier_MC,
        hc.Aljex_Carrier_ID,
        hc.Relay_Carrier_ID,
        hc.Carrier_Name,
        hc.HubSpot_Account,
        hc.Carrier_Vertical_Industry,
        hc.Address_Line_1,
        hc.Address_Line_2,
        hc.City,
        hc.State,
        hc.Country,
        hc.Zip_Code,
        hc.Carrier_Email_Address,
        hc.Carrier_Phone_Number,
        hc.Tracking_Platform,
        hc.Tracking_Platform_Status,
        hc.Flatbed_Count,
        hc.Reefer_Count,
        hc.Tractor_Count,
        hc.Trailer_Count,
        hc.Van_Count,
        hc.Is_Active,
        hc.Onboarded_Date,
        hc.Current_Lifecycle_Stage,
        hc.Is_Managed_Carrier,
        hc.Managed_Carrier_Converted_Date,
        hc.Last_Load_Date,
        hc.Carrier_Regular_Office,
        hc.Carrier_Recent_Office,
        hc.CDR_Rep_Relay_ID,
        hc.CDR_Rep_Aljex_ID,
        hc.CDR_Rep_Name,
        hc.Carrier_Rep_Aljex_ID, 
        hc.Carrier_Rep_Relay_ID, 
        hc.Carrier_Rep_Name,     
        hc.HashKey,
        current_timestamp(),  
        'Databricks',    
        current_timestamp(), -- Last_Modified_Date
        'Databricks',       -- Last_Modified_By
        0                   -- Is_Deleted
    )

-- COMMAND ----------

UPDATE metadata.mastermetadata
SET LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM Gold.Dim_Carriers where Is_Deleted = 0),
    PipelineRunStatus = 'Success'
    , PipelineEndTime = current_timestamp()
WHERE TableID = "GL4"