-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Relay_Carrier_Money
-- MAGIC * **Description:** To extract data incrementally from Bronze to Silver as delta file
-- MAGIC * **Created Date:** 03/07/2025
-- MAGIC * **Created By:** Uday
-- MAGIC * **Modified Date:** 06/10/2025
-- MAGIC * **Modified By:** Uday
-- MAGIC * **Changes Made:** Logic Update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Update the Pipeline Start Time

-- COMMAND ----------

UPDATE metadata.mastermetadata 
SET PipelineStartTime = current_timestamp()
WHERE TableID = 'SL4'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Load the Data to Target Table

-- COMMAND ----------

MERGE INTO Silver.Silver_Relay_Carrier_Money AS target
USING Silver.VW_Silver_Relay_Carrier_Money_Full_Load AS source
ON target.Booking_ID = source.Booking_ID

WHEN MATCHED AND target.Hashkey <> source.Hashkey THEN
  UPDATE SET
    target.Relay_Reference_Number     = source.Relay_Reference_Number,
    target.Status                     = source.Status,
    target.Carrier_ID                  = source.Carrier_ID,
    target.LTL_or_TL                  = source.LTL_or_TL,
    target.Carrier_Linehaul_USD       = source.Carrier_Linehaul_USD,
    target.Carrier_Linehaul_CAD       = source.Carrier_Linehaul_CAD,
    target.Carrier_FuelSurcharge_USD  = source.Carrier_FuelSurcharge_USD,
    target.Carrier_FuelSurcharge_CAD  = source.Carrier_FuelSurcharge_CAD,
    target.Carrier_Accessorial_USD    = source.Carrier_Accessorial_USD,
    target.Carrier_Accessorial_CAD    = source.Carrier_Accessorial_CAD,
    target.Carrier_Final_Rate_USD     = source.Carrier_Final_Rate_USD,
    target.Carrier_Final_Rate_CAD     = source.Carrier_Final_Rate_CAD,
    target.Hashkey                    = source.Hashkey,
    target.Last_Modified_Date         = current_timestamp(),
    target.Last_Modified_By           = 'Databricks',
    target.Is_Deleted                 = 0

WHEN NOT MATCHED THEN
  INSERT (
    Booking_ID,
    Relay_Reference_Number,
    Carrier_ID,
    Status,
    LTL_or_TL,
    Carrier_Linehaul_USD,
    Carrier_Linehaul_CAD,
    Carrier_FuelSurcharge_USD,
    Carrier_FuelSurcharge_CAD,
    Carrier_Accessorial_USD,
    Carrier_Accessorial_CAD,
    Carrier_Final_Rate_USD,
    Carrier_Final_Rate_CAD,
    Hashkey,
    Created_Date,
    Created_By,
    Last_Modified_Date,
    Last_Modified_By,
    Is_Deleted
  )
  VALUES (
    source.Booking_ID,
    source.Relay_Reference_Number,
    source.Carrier_ID,
    source.Status,
    source.LTL_or_TL,
    source.Carrier_Linehaul_USD,
    source.Carrier_Linehaul_CAD,
    source.Carrier_FuelSurcharge_USD,
    source.Carrier_FuelSurcharge_CAD,
    source.Carrier_Accessorial_USD,
    source.Carrier_Accessorial_CAD,
    source.Carrier_Final_Rate_USD,
    source.Carrier_Final_Rate_CAD,
    source.Hashkey,
    current_timestamp(),
    'Databricks',
    current_timestamp(),
    'Databricks',
    0
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Update the Deleted Records Flag

-- COMMAND ----------

UPDATE Silver.silver_relay_carrier_money 
SET Is_Deleted = 1, Last_Modified_Date = current_timestamp(), Last_Modified_By = 'Databricks'
WHERE Booking_ID NOT IN (SELECT Booking_ID FROM Silver.VW_Silver_Relay_Carrier_Money_Full_Load);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Update the Run Status

-- COMMAND ----------

UPDATE metadata.mastermetadata 
SET PipelineEndTime = current_timestamp(),
LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM silver.silver_relay_carrier_money WHERE Is_Deleted=0),
PipelineRunStatus = 'Success'
WHERE TableID = 'SL4'