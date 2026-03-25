-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Relay_Customer_Money
-- MAGIC * **Description:** Full Load the data from Bronze to Silver as delta file
-- MAGIC * **Created Date:** 03/07/2025
-- MAGIC * **Created By:** Uday
-- MAGIC * **Modified Date:** 06/10/2025
-- MAGIC * **Modified By:** Uday
-- MAGIC * **Changes Made:** Logic Update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Update Pipeline Start Time

-- COMMAND ----------

UPDATE metadata.mastermetadata 
SET PipelineStartTime = current_timestamp()
WHERE TableID = 'SL3'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Load the Data to Target 

-- COMMAND ----------

MERGE INTO Silver.Silver_Relay_Customer_Money AS target
USING Silver.VW_Silver_Relay_Customer_Money_Full_Load AS source
ON target.Booking_ID = source.Booking_ID

WHEN MATCHED AND target.Hashkey != source.Hashkey 
THEN UPDATE SET
  target.Customer_ID                = source.Customer_ID,
  target.Lawson_ID                  = source.Lawson_ID,
  target.Status                     = source.Status,
  target.Load_Currency              = source.Load_Currency,
  target.LTL_or_TL                  = source.LTL_or_TL,
  target.Total_Customer_Rate_WO_Cred_CAD = source.Total_Customer_Rate_WO_Cred_CAD,
  target.Total_Customer_Rate_WO_Cred_USD = source.Total_Customer_Rate_WO_Cred_USD,
  target.Customer_Linehaul_USD     = source.Customer_Linehaul_USD,
  target.Customer_Linehaul_CAD     = source.Customer_Linehaul_CAD,
  target.Customer_FuelSurcharge_USD = source.Customer_FuelSurcharge_USD,
  target.Customer_FuelSurcharge_CAD = source.Customer_FuelSurcharge_CAD,
  target.Customer_Accessorial_USD  = source.Customer_Accessorial_USD,
  target.Customer_Accessorial_CAD  = source.Customer_Accessorial_CAD,
  target.Invoicing_Credits_USD     = source.Invoicing_Credits_USD,
  target.Invoicing_Credits_CAD     = source.Invoicing_Credits_CAD,
  target.Customer_Final_Rate_USD   = source.Customer_Final_Rate_USD,
  target.Customer_Final_Rate_CAD   = source.Customer_Final_Rate_CAD,
  target.Stop_Off_Acc              = source.Stop_Off_Acc,
  target.Det_Acc                   = source.Det_Acc,
  target.Hashkey                   = source.Hashkey,
  target.Last_Modified_Date        = current_timestamp(),
  target.Last_Modified_By          = 'Databricks',
  target.Is_Deleted                = "0"

WHEN NOT MATCHED THEN INSERT (
    Booking_ID,
    Relay_Reference_Number,
    Customer_ID,
    Lawson_ID,
    Status,
    Load_Currency,
    LTL_or_TL,
    Total_Customer_Rate_WO_Cred_CAD,
    Total_Customer_Rate_WO_Cred_USD,
    Customer_Linehaul_USD,
    Customer_Linehaul_CAD,
    Customer_FuelSurcharge_USD,
    Customer_FuelSurcharge_CAD,
    Customer_Accessorial_USD,
    Customer_Accessorial_CAD,
    Invoicing_Credits_USD,
    Invoicing_Credits_CAD,
    Customer_Final_Rate_USD,
    Customer_Final_Rate_CAD,
    Stop_Off_Acc,
    Det_Acc,
    Hashkey,
    Created_Date,
    Created_By,
    Last_Modified_Date,
    Last_Modified_By,
    Is_Deleted
) VALUES (
    source.Booking_ID,
    source.Relay_Reference_Number,
    source.Customer_ID,
    source.Lawson_ID,
    source.Status,
    source.Load_Currency,
    source.LTL_or_TL,
    source.Total_Customer_Rate_WO_Cred_CAD,
    source.Total_Customer_Rate_WO_Cred_USD,
    source.Customer_Linehaul_USD,
    source.Customer_Linehaul_CAD,
    source.Customer_FuelSurcharge_USD,
    source.Customer_FuelSurcharge_CAD,
    source.Customer_Accessorial_USD,
    source.Customer_Accessorial_CAD,
    source.Invoicing_Credits_USD,
    source.Invoicing_Credits_CAD,
    source.Customer_Final_Rate_USD,
    source.Customer_Final_Rate_CAD,
    source.Stop_Off_Acc,
    source.Det_Acc,
    source.Hashkey,
    current_timestamp(),
    'Databricks',
    current_timestamp(),
    'Databricks',
    0
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Update the Is_Deleted flag

-- COMMAND ----------

MERGE INTO Silver.silver_relay_customer_money AS target
USING Silver.VW_Silver_Relay_Customer_Money_Full_Load AS source
ON target.Booking_ID = source.Booking_ID AND target.Is_Deleted = 0
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET
    Is_Deleted = '1',
    Last_Modified_Date = current_timestamp(),
    Last_Modified_By = 'Databricks'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Update the Run Status

-- COMMAND ----------

UPDATE metadata.mastermetadata 
SET PipelineEndTime = current_timestamp(),
LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM silver.silver_relay_customer_money WHERE Is_Deleted=0),
PipelineRunStatus = 'Success'
WHERE TableID = 'SL3'

-- COMMAND ----------

