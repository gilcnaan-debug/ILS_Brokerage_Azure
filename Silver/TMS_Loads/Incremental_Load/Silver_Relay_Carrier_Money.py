# Databricks notebook source
# MAGIC %md
# MAGIC ## Relay_Carrier_Money
# MAGIC * **Description:** To extract data incrementally from Bronze to Silver as delta file
# MAGIC * **Created Date:** 03/07/2025
# MAGIC * **Created By:** Uday
# MAGIC * **Modified Date:** 06/10/2025
# MAGIC * **Modified By:** Uday
# MAGIC * **Changes Made:** Logic Update

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update the Pipeline Start Time

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata 
# MAGIC SET PipelineStartTime = current_timestamp()
# MAGIC WHERE TableID = 'SL4'

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load the Data to Target Table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO Silver.Silver_Relay_Carrier_Money AS target
# MAGIC USING Silver.VW_Silver_Relay_Carrier_Money AS source
# MAGIC ON target.Booking_ID = source.Booking_ID
# MAGIC
# MAGIC WHEN MATCHED AND target.Hashkey <> source.Hashkey THEN
# MAGIC   UPDATE SET
# MAGIC     target.Relay_Reference_Number     = source.Relay_Reference_Number,
# MAGIC     target.Status                     = source.Status,
# MAGIC     target.Carrier_ID                  = source.Carrier_ID,
# MAGIC     target.LTL_or_TL                  = source.LTL_or_TL,
# MAGIC     target.Carrier_Linehaul_USD       = source.Carrier_Linehaul_USD,
# MAGIC     target.Carrier_Linehaul_CAD       = source.Carrier_Linehaul_CAD,
# MAGIC     target.Carrier_FuelSurcharge_USD  = source.Carrier_FuelSurcharge_USD,
# MAGIC     target.Carrier_FuelSurcharge_CAD  = source.Carrier_FuelSurcharge_CAD,
# MAGIC     target.Carrier_Accessorial_USD    = source.Carrier_Accessorial_USD,
# MAGIC     target.Carrier_Accessorial_CAD    = source.Carrier_Accessorial_CAD,
# MAGIC     target.Carrier_Final_Rate_USD     = source.Carrier_Final_Rate_USD,
# MAGIC     target.Carrier_Final_Rate_CAD     = source.Carrier_Final_Rate_CAD,
# MAGIC     target.Hashkey                    = source.Hashkey,
# MAGIC     target.Last_Modified_Date         = current_timestamp(),
# MAGIC     target.Last_Modified_By           = 'Databricks',
# MAGIC     target.Is_Deleted                 = 0
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     Booking_ID,
# MAGIC     Relay_Reference_Number,
# MAGIC     Carrier_ID,
# MAGIC     Status,
# MAGIC     LTL_or_TL,
# MAGIC     Carrier_Linehaul_USD,
# MAGIC     Carrier_Linehaul_CAD,
# MAGIC     Carrier_FuelSurcharge_USD,
# MAGIC     Carrier_FuelSurcharge_CAD,
# MAGIC     Carrier_Accessorial_USD,
# MAGIC     Carrier_Accessorial_CAD,
# MAGIC     Carrier_Final_Rate_USD,
# MAGIC     Carrier_Final_Rate_CAD,
# MAGIC     Hashkey,
# MAGIC     Created_Date,
# MAGIC     Created_By,
# MAGIC     Last_Modified_Date,
# MAGIC     Last_Modified_By,
# MAGIC     Is_Deleted
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.Booking_ID,
# MAGIC     source.Relay_Reference_Number,
# MAGIC     source.Carrier_ID,
# MAGIC     source.Status,
# MAGIC     source.LTL_or_TL,
# MAGIC     source.Carrier_Linehaul_USD,
# MAGIC     source.Carrier_Linehaul_CAD,
# MAGIC     source.Carrier_FuelSurcharge_USD,
# MAGIC     source.Carrier_FuelSurcharge_CAD,
# MAGIC     source.Carrier_Accessorial_USD,
# MAGIC     source.Carrier_Accessorial_CAD,
# MAGIC     source.Carrier_Final_Rate_USD,
# MAGIC     source.Carrier_Final_Rate_CAD,
# MAGIC     source.Hashkey,
# MAGIC     current_timestamp(),
# MAGIC     'Databricks',
# MAGIC     current_timestamp(),
# MAGIC     'Databricks',
# MAGIC     0
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update the Run Status

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata 
# MAGIC SET PipelineEndTime = current_timestamp(),
# MAGIC LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM silver.silver_relay_carrier_money WHERE Is_Deleted=0),
# MAGIC PipelineRunStatus = 'Success'
# MAGIC WHERE TableID = 'SL4'