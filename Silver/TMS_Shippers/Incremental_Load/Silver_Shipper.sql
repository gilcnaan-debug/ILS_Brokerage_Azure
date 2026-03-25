-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver Shipper
-- MAGIC * **Description:** To incrementally load the data from Bronze to Silver as delta file
-- MAGIC * **Created Date:** 03/07/2025
-- MAGIC * **Created By:** Uday
-- MAGIC * **Modified Date:** 06/10/2025
-- MAGIC * **Modified By:** Uday
-- MAGIC * **Changes Made:** Logic Update

-- COMMAND ----------

UPDATE metadata.mastermetadata 
SET PipelineStartTime = current_timestamp()
WHERE TableID = 'SL13'

-- COMMAND ----------

-- DBTITLE 1,Merge Script
MERGE INTO Silver.Silver_Shipper AS tgt
USING Silver.VW_Silver_Shipper AS src
ON tgt.MergeKey = src.MergeKey
WHEN MATCHED AND tgt.Hashkey != src.HashKey THEN
    UPDATE SET
        tgt.Lawson_ID = src.Lawson_ID,
        tgt.Customer_Master = src.Customer_Master,
        tgt.Shipper_ID = src.Shipper_ID,
        tgt.Customer_Name = src.Customer_Name,
        tgt.Customer_Aljex_ID = src.Customer_Aljex_ID,
        tgt.Customer_Relay_ID = src.Customer_Relay_ID,
        tgt.Shipper_Rep = src.Shipper_Rep,
        tgt.Sales_Rep = src.Sales_Rep,
        tgt.Vertical = src.Vertical,
        tgt.Shipper_Office = src.Shipper_Office,
        tgt.Rep_Email_Address = src.Rep_Email_Address,
        tgt.Shipper_Slack_Channel = src.Shipper_Slack_Channel,
        tgt.HubSpot_Account = src.HubSpot_Account,
        tgt.HubSpot_Rep_Name = src.HubSpot_Rep_Name,
        tgt.Rep_Aljex_ID = src.Rep_Aljex_ID,
        tgt.Rep_Relay_ID = src.Rep_Relay_ID,
        tgt.Last_Load_Date = src.Last_Load_Date,
        tgt.Is_Active = src.Is_Active,
        tgt.MergeKey = src.MergeKey,
        tgt.Hashkey = src.HashKey,
        tgt.Last_Modified_Date = current_timestamp(),
        tgt.Last_Modified_By = 'Databricks'
WHEN MATCHED AND tgt.Is_Deleted = 1 AND tgt.Hashkey = src.HashKey THEN
    UPDATE SET
        tgt.Last_Modified_Date = current_timestamp(),
        tgt.Last_Modified_By = 'Databricks',
        tgt.Is_Deleted = 0
WHEN NOT MATCHED THEN
    INSERT (
        Lawson_ID,
        Customer_Master,
        Shipper_ID,
        Customer_Name,
        Customer_Aljex_ID,
        Customer_Relay_ID,
        Shipper_Rep,
        Sales_Rep,
        Vertical,
        Shipper_Office,
        Rep_Email_Address,
        Shipper_Slack_Channel,
        HubSpot_Account,
        HubSpot_Rep_Name,
        Rep_Aljex_ID,
        Rep_Relay_ID,
        Last_Load_Date,
        Is_Active,
        MergeKey,
        Hashkey,
        Created_Date,
        Created_By,
        Last_Modified_Date,
        Last_Modified_By,
        Is_Deleted
    )
    VALUES (
        src.Lawson_ID,
        src.Customer_Master,
        src.Shipper_ID,
        src.Customer_Name,
        src.Customer_Aljex_ID,
        src.Customer_Relay_ID,
        src.Shipper_Rep,
        src.Sales_Rep,
        src.Vertical,
        src.Shipper_Office,
        src.Rep_Email_Address,
        src.Shipper_Slack_Channel,
        src.HubSpot_Account,
        src.HubSpot_Rep_Name,
        src.Rep_Aljex_ID,
        src.Rep_Relay_ID,
        src.Last_Load_Date,
        src.Is_Active,
        src.MergeKey,
        src.HashKey,
        current_timestamp(),
        'Databricks',
        current_timestamp(),
        'Databricks',
        0
    )

-- COMMAND ----------

-- DBTITLE 1,Metadata update
UPDATE metadata.mastermetadata
SET LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM Silver.Silver_Carriers where Is_Deleted = 0),
    PipelineRunStatus = 'Success'
    , PipelineEndTime = current_timestamp()
WHERE TableID = "SL13"