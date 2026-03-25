# Databricks notebook source
# MAGIC %md
# MAGIC Incremental Load

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace view Gold.VW_Fact_Touches_Incremental_Load as 
# MAGIC
# MAGIC Select 
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type , 
# MAGIC MD5(
# MAGIC   concat(
# MAGIC Coalesce(DW_Touches_ID::string,''),
# MAGIC Coalesce(Activity_Id::string,'' ),
# MAGIC Coalesce(Activity_Type::string, '') ,
# MAGIC Coalesce(Activity_Date::string, ''),
# MAGIC Coalesce(Office::string,''),
# MAGIC Coalesce(Employee_Name::string, ''),
# MAGIC Coalesce(Touches_Type::string, ''),
# MAGIC Coalesce(Period_Year::string, ''),
# MAGIC Coalesce(Week_end_date::string, ''),
# MAGIC Coalesce(Source_System_Type::string, ''))) as Hashkey from 
# MAGIC
# MAGIC (
# MAGIC Select 
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type
# MAGIC from Silver.sliver_touch_aljex
# MAGIC where 
# MAGIC last_modified_date >(Select LastLoadDateValue - Interval 1 day from metadata.mastermetadata where TableID = 'SL9') and Is_Deleted = '0'
# MAGIC
# MAGIC union All
# MAGIC
# MAGIC Select 
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type
# MAGIC from Silver.sliver_touch_relay
# MAGIC where 
# MAGIC last_modified_date >(Select LastLoadDateValue - Interval 1 day from metadata.mastermetadata where TableID = 'SL10') and Is_Deleted = '0'
# MAGIC
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC Select
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type
# MAGIC from Silver.sliver_touch_Hubspot
# MAGIC where 
# MAGIC last_modified_date >(Select LastLoadDateValue - Interval 1 day from metadata.mastermetadata where TableID = 'SL11') and Is_Deleted = '0'
# MAGIC
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type
# MAGIC from Silver.Sliver_Touches_Slack
# MAGIC where 
# MAGIC last_modified_date >(Select LastLoadDateValue - Interval 1 day from metadata.mastermetadata where TableID = 'SL12') and Is_Deleted = '0'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Full Load

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace view Gold.VW_Fact_Touches_Full_Load as 
# MAGIC
# MAGIC Select 
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type , 
# MAGIC MD5(
# MAGIC   concat(
# MAGIC Coalesce(DW_Touches_ID::string,''),
# MAGIC Coalesce(Activity_Id::string,'' ),
# MAGIC Coalesce(Activity_Type::string, '') ,
# MAGIC Coalesce(Activity_Date::string, ''),
# MAGIC Coalesce(Office::string,''),
# MAGIC Coalesce(Employee_Name::string, ''),
# MAGIC Coalesce(Touches_Type::string, ''),
# MAGIC Coalesce(Period_Year::string, ''),
# MAGIC Coalesce(Week_end_date::string, ''),
# MAGIC Coalesce(Source_System_Type::string, ''))) as Hashkey from 
# MAGIC
# MAGIC (
# MAGIC Select 
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type
# MAGIC from Silver.sliver_touch_aljex
# MAGIC where 
# MAGIC Is_Deleted = '0'
# MAGIC
# MAGIC union All
# MAGIC
# MAGIC Select 
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type
# MAGIC from Silver.sliver_touch_relay
# MAGIC where 
# MAGIC Is_Deleted = '0'
# MAGIC
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC Select
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type
# MAGIC from Silver.sliver_touch_Hubspot
# MAGIC where 
# MAGIC Is_Deleted = '0'
# MAGIC
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select
# MAGIC DW_Touches_ID,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touches_Type,
# MAGIC Period_Year,
# MAGIC Week_end_date,
# MAGIC Source_system_Type
# MAGIC from Silver.Sliver_Touches_Slack
# MAGIC where 
# MAGIC Is_Deleted = '0'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC DDl Code

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table Gold.Fact_Touches_Gold (
# MAGIC     DW_Touches_ID VARCHAR(255)NOT NULL PRIMARY KEY,
# MAGIC     Activity_Id VARCHAR (255),
# MAGIC     Activity_Type VARCHAR (255),
# MAGIC     Activity_Date DATE,
# MAGIC     Office VARCHAR (255),
# MAGIC     Employee_Name VARCHAR (255),
# MAGIC     Touches_Type INT,
# MAGIC     Period_Year VARCHAR (255),
# MAGIC     Week_end_date DATE,
# MAGIC     Source_System_Type VARCHAR(255),
# MAGIC     Hash_key VARCHAR (255),
# MAGIC     Created_Date TIMESTAMP_NTZ,
# MAGIC     Created_By VARCHAR(50) NOT NULL,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ,
# MAGIC     Last_Modified_by VARCHAR(50) NOT NULL,
# MAGIC     Is_Deleted SMALLINT
# MAGIC );
# MAGIC
# MAGIC