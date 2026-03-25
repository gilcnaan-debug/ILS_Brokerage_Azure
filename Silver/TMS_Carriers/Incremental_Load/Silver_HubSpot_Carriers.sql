-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver HubSpot Carriers
-- MAGIC * **Description:** Query for Loading Table in Silver Zone
-- MAGIC * **Created Date:** 03/07/2025
-- MAGIC * **Created By:** Uday
-- MAGIC * **Modified Date:** 06/10/2025
-- MAGIC * **Modified By:** Uday
-- MAGIC * **Changes Made:** Logic Update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver_HubSpot_Carriers

-- COMMAND ----------

UPDATE metadata.mastermetadata 
SET PipelineStartTime = current_timestamp()
WHERE TableID = 'SL7'

-- COMMAND ----------

-- DBTITLE 1,Merge Script
MERGE INTO Silver.Silver_HubSpot_Carriers AS shc
USING Silver.VW_Silver_HubSpot_Carrier AS hc
ON hc.HubSpot_Company_ID = shc.HubSpot_Company_ID
WHEN MATCHED AND shc.HashKey != hc.HashKey THEN
    UPDATE SET   
        shc.HubSpot_Company_ID = hc.HubSpot_Company_ID,
        shc.Carrier_DOT = hc.Carrier_DOT,
        shc.Carrier_MC = hc.Carrier_MC,
        shc.HubSpot_Relay_Carrier_ID = hc.HubSpot_Relay_Carrier_ID,
        shc.Carrier_Name = hc.Carrier_Name,
        shc.Carrier_Vertical_Industry = hc.Carrier_Vertical_Industry,
        shc.Address_Line_1 = hc.Address_Line_1,
        shc.Address_Line_2 = hc.Address_Line_2,
        shc.City = hc.City,
        shc.State = hc.State,
        shc.Country = hc.Country,
        shc.Zip_Code = hc.Zip_Code,
        shc.CAM_HubSpot_ID = hc.CAM_HubSpot_ID,
        shc.CAM_Email = hc.CAM_Email,
        shc.CAM_Name = hc.CAM_Name,
        shc.CDR_HubSpot_ID = hc.CDR_HubSpot_ID,
        shc.CDR_Email = hc.CDR_Email,
        shc.CDR_Name = hc.CDR_Name,
        shc.HubSpot_Team_ID = hc.HubSpot_Team_ID,
        shc.Carrier_Email_Address = hc.Carrier_Email_Address,
        shc.Carrier_Phone_Number = hc.Carrier_Phone_Number,
        shc.Tracking_Platform = hc.Tracking_Platform,
        shc.Tracking_Platform_Status = hc.Tracking_Platform_Status,
        shc.Flatbed_Count = hc.Flatbed_Count,
        shc.Reefer_Count = hc.Reefer_Count,
        shc.Tractor_Count = hc.Tractor_Count,
        shc.Trailer_Count = hc.Trailer_Count,
        shc.Van_Count = hc.Van_Count,
        shc.Account_Created_By_User_ID = hc.Account_Created_By_User_ID,
        shc.Account_Created_Date = hc.Account_Created_Date,
        shc.Last_Call_Date = hc.Last_Call_Date,
        shc.Total_Calls = hc.Total_Calls,
        shc.Last_Email_Date = hc.Last_Email_Date,
        shc.Total_Emails = hc.Total_Emails,
        shc.Current_Lifecycle_Stage = hc.Current_Lifecycle_Stage,
        shc.Is_Managed_Carrier = hc.Is_Managed_Carrier,
        shc.Managed_Carrier_Converted_Date = hc.Managed_Carrier_Converted_Date,
        shc.HashKey = hc.Hashkey,
        shc.Last_Modified_Date = current_timestamp(),
        shc.Last_Modified_By = 'Databricks' 
WHEN MATCHED AND shc.Is_Deleted = 1 AND shc.HashKey = hc.HashKey  THEN 
    UPDATE SET
        shc.Last_Modified_Date = current_timestamp(),
        shc.Last_Modified_By = 'Databricks',
        shc.Is_Deleted = 0
WHEN NOT MATCHED THEN
    INSERT ( 
        HubSpot_Company_ID,
        Carrier_DOT,
        Carrier_MC,
        HubSpot_Relay_Carrier_ID,
        Carrier_Name,
        Carrier_Vertical_Industry,
        Address_Line_1,
        Address_Line_2,
        City,
        State,
        Country,
        Zip_Code,
        CAM_HubSpot_ID,
        CAM_Email,
        CAM_Name,
        CDR_HubSpot_ID,
        CDR_Email,
        CDR_Name,
        HubSpot_Team_ID,
        Carrier_Email_Address,
        Carrier_Phone_Number,
        Tracking_Platform,
        Tracking_Platform_Status,
        Flatbed_Count,
        Reefer_Count,
        Tractor_Count,
        Trailer_Count,
        Van_Count,
        Account_Created_By_User_ID,
        Account_Created_Date,
        Last_Call_Date,
        Total_Calls,
        Last_Email_Date,
        Total_Emails,
        Current_Lifecycle_Stage,
        Is_Managed_Carrier,
        Managed_Carrier_Converted_Date,
        HashKey,
        Created_Date,
        Created_By,
        Last_Modified_Date,
        Last_Modified_By,
        Is_Deleted
    )
    VALUES (
        hc.HubSpot_Company_ID,
        hc.Carrier_DOT,
        hc.Carrier_MC,
        hc.HubSpot_Relay_Carrier_ID,
        hc.Carrier_Name,
        hc.Carrier_Vertical_Industry,
        hc.Address_Line_1,
        hc.Address_Line_2,
        hc.City,
        hc.State,
        hc.Country,
        hc.Zip_Code,
        hc.CAM_HubSpot_ID,
        hc.CAM_Email,
        hc.CAM_Name,
        hc.CDR_HubSpot_ID,
        hc.CDR_Email,
        hc.CDR_Name,
        hc.HubSpot_Team_ID,
        hc.Carrier_Email_Address,
        hc.Carrier_Phone_Number,
        hc.Tracking_Platform,
        hc.Tracking_Platform_Status,
        hc.Flatbed_Count,
        hc.Reefer_Count,
        hc.Tractor_Count,
        hc.Trailer_Count,
        hc.Van_Count,
        hc.Account_Created_By_User_ID,
        hc.Account_Created_Date,
        hc.Last_Call_Date,
        hc.Total_Calls,
        hc.Last_Email_Date,
        hc.Total_Emails,
        hc.Current_Lifecycle_Stage,
        hc.Is_Managed_Carrier,
        hc.Managed_Carrier_Converted_Date,
        hc.Hashkey,
        current_timestamp(), -- Last_Modified_Date
        'Databricks',
        current_timestamp(), -- Last_Modified_Date
        'Databricks',       -- Last_Modified_By
        0                   -- Is_Deleted
    )

-- COMMAND ----------

-- DBTITLE 1,Success Status Update
UPDATE metadata.mastermetadata
SET LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM Silver.Silver_HubSpot_Carriers where Is_Deleted = 0),
    PipelineRunStatus = 'Success'
    , PipelineEndTime = current_timestamp()
WHERE TableID = "SL7"