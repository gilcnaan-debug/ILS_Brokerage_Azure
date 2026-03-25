# Databricks notebook source
# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata 
# MAGIC SET PipelineStartTime = current_timestamp()
# MAGIC WHERE TableID = 'SL3'

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO Silver.Silver_Relay_Customer_Money AS target
# MAGIC USING Silver.VW_Silver_Relay_Customer_Money AS source
# MAGIC ON target.Booking_ID = source.Booking_ID
# MAGIC
# MAGIC WHEN MATCHED AND target.Hashkey <> source.Hashkey 
# MAGIC THEN UPDATE SET
# MAGIC   target.Customer_ID                = source.Customer_ID,
# MAGIC   target.Lawson_ID                  = source.Lawson_ID,
# MAGIC   target.Status                     = source.Status,
# MAGIC   target.Load_Currency              = source.Load_Currency,
# MAGIC   target.LTL_or_TL                  = source.LTL_or_TL,
# MAGIC   target.Total_Customer_Rate_WO_Cred_CAD = source.Total_Customer_Rate_WO_Cred_CAD,
# MAGIC   target.Total_Customer_Rate_WO_Cred_USD = source.Total_Customer_Rate_WO_Cred_USD,
# MAGIC   target.Customer_Linehaul_USD     = source.Customer_Linehaul_USD,
# MAGIC   target.Customer_Linehaul_CAD     = source.Customer_Linehaul_CAD,
# MAGIC   target.Customer_FuelSurcharge_USD = source.Customer_FuelSurcharge_USD,
# MAGIC   target.Customer_FuelSurcharge_CAD = source.Customer_FuelSurcharge_CAD,
# MAGIC   target.Customer_Accessorial_USD  = source.Customer_Accessorial_USD,
# MAGIC   target.Customer_Accessorial_CAD  = source.Customer_Accessorial_CAD,
# MAGIC   target.Invoicing_Credits_USD     = source.Invoicing_Credits_USD,
# MAGIC   target.Invoicing_Credits_CAD     = source.Invoicing_Credits_CAD,
# MAGIC   target.Customer_Final_Rate_USD   = source.Customer_Final_Rate_USD,
# MAGIC   target.Customer_Final_Rate_CAD   = source.Customer_Final_Rate_CAD,
# MAGIC   target.Stop_Off_Acc              = source.Stop_Off_Acc,
# MAGIC   target.Det_Acc                   = source.Det_Acc,
# MAGIC   target.Hashkey                   = source.Hashkey,
# MAGIC   target.Last_Modified_Date        = current_timestamp(),
# MAGIC   target.Last_Modified_By          = 'Databricks',
# MAGIC   target.Is_Deleted                = "0"
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     Booking_ID,
# MAGIC     Relay_Reference_Number,
# MAGIC     Customer_ID,
# MAGIC     Lawson_ID,
# MAGIC     Status,
# MAGIC     Load_Currency,
# MAGIC     LTL_or_TL,
# MAGIC     Total_Customer_Rate_WO_Cred_CAD,
# MAGIC     Total_Customer_Rate_WO_Cred_USD,
# MAGIC     Customer_Linehaul_USD,
# MAGIC     Customer_Linehaul_CAD,
# MAGIC     Customer_FuelSurcharge_USD,
# MAGIC     Customer_FuelSurcharge_CAD,
# MAGIC     Customer_Accessorial_USD,
# MAGIC     Customer_Accessorial_CAD,
# MAGIC     Invoicing_Credits_USD,
# MAGIC     Invoicing_Credits_CAD,
# MAGIC     Customer_Final_Rate_USD,
# MAGIC     Customer_Final_Rate_CAD,
# MAGIC     Stop_Off_Acc,
# MAGIC     Det_Acc,
# MAGIC     Hashkey,
# MAGIC     Created_Date,
# MAGIC     Created_By,
# MAGIC     Last_Modified_Date,
# MAGIC     Last_Modified_By,
# MAGIC     Is_Deleted
# MAGIC ) VALUES (
# MAGIC     source.Booking_ID,
# MAGIC     source.Relay_Reference_Number,
# MAGIC     source.Customer_ID,
# MAGIC     source.Lawson_ID,
# MAGIC     source.Status,
# MAGIC     source.Load_Currency,
# MAGIC     source.LTL_or_TL,
# MAGIC     source.Total_Customer_Rate_WO_Cred_CAD,
# MAGIC     source.Total_Customer_Rate_WO_Cred_USD,
# MAGIC     source.Customer_Linehaul_USD,
# MAGIC     source.Customer_Linehaul_CAD,
# MAGIC     source.Customer_FuelSurcharge_USD,
# MAGIC     source.Customer_FuelSurcharge_CAD,
# MAGIC     source.Customer_Accessorial_USD,
# MAGIC     source.Customer_Accessorial_CAD,
# MAGIC     source.Invoicing_Credits_USD,
# MAGIC     source.Invoicing_Credits_CAD,
# MAGIC     source.Customer_Final_Rate_USD,
# MAGIC     source.Customer_Final_Rate_CAD,
# MAGIC     source.Stop_Off_Acc,
# MAGIC     source.Det_Acc,
# MAGIC     source.Hashkey,
# MAGIC     current_timestamp(),
# MAGIC     'Databricks',
# MAGIC     current_timestamp(),
# MAGIC     'Databricks',
# MAGIC     0
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata 
# MAGIC SET PipelineEndTime = current_timestamp(),
# MAGIC LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM silver.silver_relay_customer_money WHERE Is_Deleted=0),
# MAGIC PipelineRunStatus = 'Success'
# MAGIC WHERE TableID = 'SL3'