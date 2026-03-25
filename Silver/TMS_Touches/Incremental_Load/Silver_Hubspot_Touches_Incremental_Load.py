# Databricks notebook source
# MAGIC %md
# MAGIC Update the Pipeline Start Time

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata 
# MAGIC SET PipelineStartTime = current_timestamp()
# MAGIC WHERE TableID = 'SL11'

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load the data into Target Table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO Silver.Sliver_Touch_Hubspot AS target
# MAGIC USING Silver.VW_Silver_Hubspot_Touche AS source
# MAGIC ON target.DW_Touches_Id = source.DW_Touch_Id
# MAGIC WHEN MATCHED AND target.Hash_key <> source.Hashkey THEN
# MAGIC  UPDATE SET
# MAGIC  target.Activity_Id = source.Activity_Id,
# MAGIC  target.Activity_Type = source.Activity_Type,
# MAGIC  target.Activity_Date = source.Activity_Date,
# MAGIC  target.Office = source.Office,
# MAGIC  target.Employee_Name = source.Employee_Name,
# MAGIC  target.Touches_Type = source.Touch_Type,
# MAGIC  target.period_year = source.period_year,
# MAGIC  target.Week_End_Date = source.Week_End_Date,
# MAGIC  target.Source_System_Type = source.Source_System_Type,
# MAGIC  target.Hash_key = source.Hashkey,
# MAGIC  target.Last_Modified_Date = current_timestamp(),
# MAGIC  target.Last_Modified_By = 'Databricks',
# MAGIC  target.Is_Deleted = 0
# MAGIC
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC period_year,
# MAGIC Week_End_Date,
# MAGIC Source_System_Type,
# MAGIC Hash_key,
# MAGIC Created_Date,
# MAGIC Created_By,
# MAGIC Last_Modified_Date,
# MAGIC Last_Modified_By,
# MAGIC Is_Deleted
# MAGIC )
# MAGIC
# MAGIC VALUES (
# MAGIC source.DW_Touch_ID,
# MAGIC source.Activity_Id,
# MAGIC source.Activity_Type,
# MAGIC source.Activity_Date,
# MAGIC source.Office,
# MAGIC source.Employee_Name,
# MAGIC source.Touch_Type,
# MAGIC source.period_year,
# MAGIC source.Week_End_Date,
# MAGIC source.Source_System_Type,
# MAGIC source.Hashkey,
# MAGIC current_timestamp(),
# MAGIC 'Databricks',
# MAGIC current_timestamp(),
# MAGIC 'Databricks',
# MAGIC 0
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Update the Run Status

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata 
# MAGIC SET PipelineEndTime = current_timestamp(),
# MAGIC LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM silver.Sliver_Touch_Hubspot WHERE Is_Deleted=0),
# MAGIC PipelineRunStatus = 'Success'
# MAGIC WHERE TableID = 'SL11'