# Databricks notebook source
# MAGIC %md
# MAGIC Incremental Load

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Hubspot_Touche AS
# MAGIC With Silver_HubSpot_Touches as (
# MAGIC Select 
# MAGIC 'HubSpot-Email' as Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_email.hs_object_id as Activity_Id,
# MAGIC hs_createdate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast (1 as Int) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_emails as hs_email 
# MAGIC left join  Hubspot_Owners as hs_owner on hs_email.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_email.hs_createdate = dim_date.calendar_date
# MAGIC where hs_email.hs_email_direction = 'EMAIL'
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot-Meeting' as Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_meeting.hs_object_id as Activity_Id,
# MAGIC hs_meeting.hs_createdate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_meetings as hs_meeting
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_meeting.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_meeting.hs_createdate = dim_date.calendar_date
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Meeting' As Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_meeting.hs_object_id as Activity_Id,
# MAGIC hs_meeting.hs_meeting_start_time::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_meetings as hs_meeting
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_meeting.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_meeting.hs_meeting_start_time::date = dim_date.calendar_date
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Notes' as Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_notes.hs_object_id as Activity_id,
# MAGIC hs_notes.hs_createdate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_notes as hs_notes 
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_notes.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_notes.hs_createdate::date = dim_date.calendar_date
# MAGIC
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Deals' as Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_deals.hs_object_id as Activity_id,
# MAGIC hs_deals.createdate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_deals as hs_deals
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_deals.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_deals.createdate::date = dim_date.calendar_date
# MAGIC
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Deals' As Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_deals.hs_object_id as Activity_id,
# MAGIC TRY_CAST(NULLIF(hs_deals.Closedate, '') AS DATE) Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_deals as hs_deals 
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_deals.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on TRY_CAST(NULLIF(hs_deals.Closedate, '') AS DATE) = dim_date.calendar_date 
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Deals' As Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_deals.hs_object_id as Activity_id,
# MAGIC hs_deals.hs_lastmodifieddate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_deals as hs_deals 
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_deals.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_deals.hs_lastmodifieddate::date = dim_date.calendar_date  
# MAGIC
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC  select
# MAGIC 'Hubspot - External-Business-Review-Meeting' As Touch_Type,
# MAGIC concat(hs_owners.First_name, ' ' , hs_owners.last_name) as Employee_Name, 
# MAGIC hs_calls.hs_object_id as Activity_Id,
# MAGIC hs_createdate::date as actvity_date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast (1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC from bronze.hubspot_calls hs_calls 
# MAGIC left join hubspot_owners hs_owners on hs_calls.hubspot_owner_id = hs_owners.id 
# MAGIC Left join analytics.dim_date on hs_calls.hs_lastmodifieddate::date = dim_date.calendar_date  
# MAGIC where hs_calls.sourcesystemid = '4' and hs_calls.hs_activity_type = 'External Business Review Meeting'
# MAGIC
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC select
# MAGIC 'Hubspot - Introcalls ' As Touch_Type,
# MAGIC concat(hs_owners.First_name, ' ' , hs_owners.last_name) as Employee_Name, 
# MAGIC hs_calls.hs_object_id as Activity_Id,
# MAGIC hs_createdate::date as actvity_date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast (1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC from bronze.hubspot_calls hs_calls 
# MAGIC left join hubspot_owners hs_owners on hs_calls.hubspot_owner_id = hs_owners.id 
# MAGIC Left join analytics.dim_date on hs_calls.hs_lastmodifieddate::date = dim_date.calendar_date  
# MAGIC where hs_calls.sourcesystemid = '5' and hs_calls.hs_activity_type = 'Introduce NFI'  
# MAGIC ),
# MAGIC
# MAGIC Final_Code as (
# MAGIC Select 
# MAGIC Concat_ws(Activity_Type,Activity_Id,Activity_Date) as DW_Touch_ID,
# MAGIC activity_type As Activity_Type,
# MAGIC Activity_Id,
# MAGIC Activity_Date,
# MAGIC null as Office,
# MAGIC Employee_Name,
# MAGIC Touch_Type,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Source_System_Type
# MAGIC  from Silver_HubSpot_Touches )
# MAGIC
# MAGIC  Select *,
# MAGIC SHA1(
# MAGIC   Concat(
# MAGIC     COALESCE(Activity_Id::string,''),
# MAGIC     COALESCE(Activity_Type::String,''),
# MAGIC     COALESCE(Activity_Date::String,''),
# MAGIC     COALESCE(Office::String,''),
# MAGIC     COALESCE(Employee_Name::String,''),
# MAGIC     COALESCE(period_year::String,''),
# MAGIC     COALESCE(Touch_Type::String,''),
# MAGIC     COALESCE(Week_End_Date::String,''),
# MAGIC     COALESCE(Source_System_Type,'') 
# MAGIC   )
# MAGIC  )As HashKey from Final_Code
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Full Load

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.Silver_Hubspot_Touches_Full_Load AS
# MAGIC With Silver_HubSpot_Touches as (
# MAGIC Select 
# MAGIC 'HubSpot-Email' as Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_email.hs_object_id as Activity_Id,
# MAGIC hs_createdate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast (1 as Int) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_emails as hs_email 
# MAGIC left join  Hubspot_Owners as hs_owner on hs_email.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_email.hs_createdate = dim_date.calendar_date
# MAGIC where hs_email.hs_email_direction = 'EMAIL'
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot-Meeting' as Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_meeting.hs_object_id as Activity_Id,
# MAGIC hs_meeting.hs_createdate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_meetings as hs_meeting
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_meeting.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_meeting.hs_createdate = dim_date.calendar_date
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Meeting' As Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_meeting.hs_object_id as Activity_Id,
# MAGIC hs_meeting.hs_meeting_start_time::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_meetings as hs_meeting
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_meeting.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_meeting.hs_meeting_start_time::date = dim_date.calendar_date
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Notes' as Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_notes.hs_object_id as Activity_id,
# MAGIC hs_notes.hs_createdate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_notes as hs_notes 
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_notes.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_notes.hs_createdate::date = dim_date.calendar_date
# MAGIC
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Deals' as Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_deals.hs_object_id as Activity_id,
# MAGIC hs_deals.createdate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_deals as hs_deals
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_deals.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_deals.createdate::date = dim_date.calendar_date
# MAGIC
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Deals' As Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_deals.hs_object_id as Activity_id,
# MAGIC TRY_CAST(NULLIF(hs_deals.Closedate, '') AS DATE) Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_deals as hs_deals 
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_deals.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on TRY_CAST(NULLIF(hs_deals.Closedate, '') AS DATE) = dim_date.calendar_date 
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC
# MAGIC Select 
# MAGIC 'HubSpot - Deals' As Activity_Type,
# MAGIC Concat(hs_owner.First_name, ' ' , hs_owner.last_name) as Employee_Name, 
# MAGIC hs_deals.hs_object_id as Activity_id,
# MAGIC hs_deals.hs_lastmodifieddate::date as Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast(1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC From bronze.hubspot_deals as hs_deals 
# MAGIC Left Join Hubspot_Owners as hs_owner on hs_deals.hubspot_owner_id = hs_owner.id
# MAGIC Left join analytics.dim_date on hs_deals.hs_lastmodifieddate::date = dim_date.calendar_date  
# MAGIC
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC  select
# MAGIC 'Hubspot - External-Business-Review-Meeting' As Touch_Type,
# MAGIC concat(hs_owners.First_name, ' ' , hs_owners.last_name) as Employee_Name, 
# MAGIC hs_calls.hs_object_id as Activity_Id,
# MAGIC hs_createdate::date as actvity_date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast (1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC from bronze.hubspot_calls hs_calls 
# MAGIC left join hubspot_owners hs_owners on hs_calls.hubspot_owner_id = hs_owners.id 
# MAGIC Left join analytics.dim_date on hs_calls.hs_lastmodifieddate::date = dim_date.calendar_date  
# MAGIC where hs_calls.sourcesystemid = '4' and hs_calls.hs_activity_type = 'External Business Review Meeting'
# MAGIC
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC select
# MAGIC 'Hubspot - Introcalls ' As Touch_Type,
# MAGIC concat(hs_owners.First_name, ' ' , hs_owners.last_name) as Employee_Name, 
# MAGIC hs_calls.hs_object_id as Activity_Id,
# MAGIC hs_createdate::date as actvity_date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Cast (1 as INT) as Touch_Type,
# MAGIC 'Hubspot' as Source_System_Type
# MAGIC from bronze.hubspot_calls hs_calls 
# MAGIC left join hubspot_owners hs_owners on hs_calls.hubspot_owner_id = hs_owners.id 
# MAGIC Left join analytics.dim_date on hs_calls.hs_lastmodifieddate::date = dim_date.calendar_date  
# MAGIC where hs_calls.sourcesystemid = '5' and hs_calls.hs_activity_type = 'Introduce NFI'  
# MAGIC ),
# MAGIC
# MAGIC Final_Code as (
# MAGIC Select 
# MAGIC Concat_ws(Activity_Type,Activity_Id,Activity_Date) as DW_Touch_ID,
# MAGIC activity_type As Activity_Type,
# MAGIC Activity_Id,
# MAGIC Activity_Date,
# MAGIC null as Office,
# MAGIC Employee_Name,
# MAGIC Touch_Type,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Source_System_Type
# MAGIC  from Silver_HubSpot_Touches )
# MAGIC
# MAGIC  Select *,
# MAGIC SHA1(
# MAGIC   Concat(
# MAGIC     COALESCE(Activity_Id::string,''),
# MAGIC     COALESCE(Activity_Type::String,''),
# MAGIC     COALESCE(Activity_Date::String,''),
# MAGIC     COALESCE(Office::String,''),
# MAGIC     COALESCE(Employee_Name::String,''),
# MAGIC     COALESCE(Touch_Type::String,''),
# MAGIC     COALESCE(period_year::String,''),
# MAGIC     COALESCE(Week_End_Date::String,''),
# MAGIC     COALESCE(Source_System_Type,'') 
# MAGIC   )
# MAGIC  )As HashKey from Final_Code
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Silver.Sliver_Touch_Hubspot (
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