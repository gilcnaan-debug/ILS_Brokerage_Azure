# Databricks notebook source
# MAGIC %md
# MAGIC Incremental Load

# COMMAND ----------

# MAGIC %sql
# MAGIC Use schema Silver;
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Slack_Touches AS
# MAGIC With Slack_Private_Public_Channels as (
# MAGIC
# MAGIC   SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         h.Conversation_Type AS Channel_type,
# MAGIC         h.Channel_ID,
# MAGIC         h.Actual_Text,
# MAGIC         TRY_CAST(h.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_new AS h
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON h.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON h.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(h.DW_Timestamp  as Date)
# MAGIC 		
# MAGIC ),
# MAGIC
# MAGIC Selected_Data_DM as (
# MAGIC SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         DM.Conversation_Type AS Channel_type,
# MAGIC         DM.Channel_ID,
# MAGIC         DM.Actual_Text AS DMText,
# MAGIC         TRY_CAST(DM.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_dm DM
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON DM.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON DM.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(DM.DW_Timestamp  as Date)
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC Selected_Data_Group AS (
# MAGIC     SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         GRP.Conversation_Type AS Channel_type,
# MAGIC         GRP.Channel_ID,
# MAGIC         GRP.Actual_Text AS GrpText,
# MAGIC         TRY_CAST(GRP.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_groups GRP
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON GRP.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON GRP.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(GRP.DW_Timestamp  as Date)
# MAGIC ),
# MAGIC
# MAGIC Slack_All_Data_DM as (
# MAGIC  SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_ID as Activity_ID,
# MAGIC         Channel_type,
# MAGIC         DT1 as Activity_Date,
# MAGIC 		period_year,
# MAGIC 		week_end_date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touch_ID,
# MAGIC         Actual_Text
# MAGIC     FROM Slack_Private_Public_Channels
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, Actual_Text, DT1, FifteenMinGroup,Employee_Name, period_year, week_end_date
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1 as Activity_Date,
# MAGIC 		period_year,
# MAGIC 		week_end_date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         DMText AS Actual_Text
# MAGIC     FROM Selected_Data_DM
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, Actual_Text, DT1, FifteenMinGroup,Employee_Name, period_year, week_end_date
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC         Employee_Name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1 as Activity_Date,
# MAGIC 		period_year,
# MAGIC 		week_end_date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         GrpText AS Actual_Text
# MAGIC     FROM Selected_Data_Group
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, Actual_Text, DT1, FifteenMinGroup,Employee_Name, period_year, week_end_date
# MAGIC 	),
# MAGIC 	
# MAGIC
# MAGIC Slack_Conversations_All as (
# MAGIC
# MAGIC Select 
# MAGIC CONCAT_WS('', User_ID, Channel_name, Channel_type, Activity_Date, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC Case when Channel_type = 'Public' then 'Slack - Public Channel' End as Activity_Type,
# MAGIC Activity_ID,
# MAGIC Employee_Name, 
# MAGIC Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Touch_ID as Touch_Type,
# MAGIC CAST(6 AS INT) AS Source_System_ID,
# MAGIC 'Slack' as Source_System_Type
# MAGIC from Slack_All_Data_DM
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC Select 
# MAGIC CONCAT_WS('', User_ID, Channel_name, Channel_type, Activity_Date, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC Case when Channel_type = 'Private' then 'Slack - Private Channel' End as Activity_Type,
# MAGIC Activity_ID,
# MAGIC Employee_Name, 
# MAGIC Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Touch_ID as Touch_Type,
# MAGIC CAST(6 AS INT) AS Touch_ID,
# MAGIC 'Slack' as Source_System_Type
# MAGIC from Slack_All_Data_DM
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC Select 
# MAGIC CONCAT_WS('', User_ID, Channel_name, Channel_type, Activity_Date, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC Case when Channel_type = 'Direct Message' then 'Slack - Direct Message' End as Activity_Type,
# MAGIC Activity_ID,
# MAGIC Employee_Name, 
# MAGIC Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC touch_id,
# MAGIC CAST(6 AS INT) AS Touch_ID,
# MAGIC 'Slack' as Source_System_Type
# MAGIC from Slack_All_Data_DM
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC Select 
# MAGIC CONCAT_WS('', User_ID, Channel_name, Channel_type, Activity_Date, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC Case when Channel_type = 'Group Message' then 'Slack - Group Message' End as Activity_Type,
# MAGIC Activity_ID,
# MAGIC Employee_Name, 
# MAGIC Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Touch_id,
# MAGIC CAST(6 AS INT) AS Source_System_ID,
# MAGIC 'Slack' as Source_System_Type
# MAGIC from Slack_All_Data_DM
# MAGIC ),
# MAGIC ----------------------------------------------- Reactions Data for all Public , Private , Reactions --------------------------------------------
# MAGIC reactions_selected_data AS (
# MAGIC     SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         h.Conversation_Type AS Channel_type,
# MAGIC         h.Channel_ID,
# MAGIC         h.Reaction_Name,
# MAGIC         TRY_CAST(h.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_reactions_new AS h
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON h.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON h.User_ID = u.User_ID 
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(h.DW_Timestamp  as Date)
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC reactions_selected_data_DM AS (
# MAGIC     SELECT
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         DM.Conversation_Type AS Channel_type,
# MAGIC         DM.Channel_ID,
# MAGIC         DM.Reactions AS Reaction_Name,
# MAGIC         TRY_CAST(DM.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		week_end_date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_dm DM
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON DM.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u ON DM.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(DM.DW_Timestamp  as Date)
# MAGIC     WHERE Is_Reactions = '1'
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC reactions_selected_data_Group AS (
# MAGIC     SELECT
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         GRP.Conversation_Type AS Channel_type,
# MAGIC         GRP.Channel_ID,
# MAGIC         GRP.Reactions AS Reaction_Name,
# MAGIC         TRY_CAST(GRP.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_groups GRP
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON GRP.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON GRP.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(GRP.DW_Timestamp  as Date)
# MAGIC     WHERE Is_Reactions = '1'
# MAGIC ),
# MAGIC
# MAGIC reaction_aggregated_data AS (
# MAGIC     SELECT
# MAGIC         User_ID,
# MAGIC         Channel_ID as Activity_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         Reaction_Name
# MAGIC     FROM reactions_selected_data
# MAGIC     GROUP BY User_ID, Channel_ID, Channel_name, Channel_type, Reaction_Name, DT1, FifteenMinGroup, Employee_Name , period_year,Week_end_Date
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         User_ID,
# MAGIC         Channel_ID as Activity_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         Reaction_Name
# MAGIC     FROM reactions_selected_data_DM
# MAGIC     GROUP BY User_ID, Channel_ID, Channel_name, Channel_type, Reaction_Name, DT1, FifteenMinGroup , Employee_Name , period_year,Week_end_Date
# MAGIC  
# MAGIC   UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         User_ID,
# MAGIC         Channel_ID as Touch_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         Reaction_Name
# MAGIC     FROM reactions_selected_data_Group
# MAGIC     GROUP BY User_ID, Channel_ID, Channel_name, Channel_type, Reaction_Name, DT1, FifteenMinGroup , Employee_Name , period_year,Week_end_Date
# MAGIC ),
# MAGIC
# MAGIC Slack_reactions_all as (
# MAGIC SELECT
# MAGIC         CONCAT_WS('', User_ID, Channel_name, Channel_type, DT1, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC         'Slack - Reactions' AS Activity_Type,
# MAGIC         Activity_ID,
# MAGIC         Employee_Name AS full_name,
# MAGIC         DT1 AS Activity_Date,
# MAGIC         	period_year,
# MAGIC           Week_end_Date,
# MAGIC        -- User_ID AS Email,
# MAGIC         Touches_Score As Touch_Type,
# MAGIC         CAST(6 AS INT) AS Sourcesystem_id,
# MAGIC 		 'Slack' as Source_System_Type
# MAGIC from reaction_aggregated_data		 
# MAGIC ),
# MAGIC
# MAGIC Slack_Conversation_Reactions as (
# MAGIC   Select * from Slack_Conversations_All 
# MAGIC   
# MAGIC Union
# MAGIC   
# MAGIC Select * from Slack_reactions_all 
# MAGIC ),
# MAGIC
# MAGIC Final_Code as (
# MAGIC Select 
# MAGIC DW_Touch_ID,
# MAGIC Activity_ID,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC null as Office,
# MAGIC Employee_Name,
# MAGIC Touch_Type,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Source_System_Type
# MAGIC from Slack_Conversation_Reactions limit 100)
# MAGIC
# MAGIC Select *,
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
# MAGIC Full Load

# COMMAND ----------

# MAGIC %sql
# MAGIC Use schema Silver;
# MAGIC CREATE OR REPLACE VIEW Silver.Silver_Slack_Touches_Full_Load AS
# MAGIC With Slack_Private_Public_Channels as (
# MAGIC
# MAGIC   SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         h.Conversation_Type AS Channel_type,
# MAGIC         h.Channel_ID,
# MAGIC         h.Actual_Text,
# MAGIC         TRY_CAST(h.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_new AS h
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON h.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON h.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(h.DW_Timestamp  as Date)
# MAGIC 		
# MAGIC ),
# MAGIC
# MAGIC Selected_Data_DM as (
# MAGIC SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         DM.Conversation_Type AS Channel_type,
# MAGIC         DM.Channel_ID,
# MAGIC         DM.Actual_Text AS DMText,
# MAGIC         TRY_CAST(DM.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_dm DM
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON DM.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON DM.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(DM.DW_Timestamp  as Date)
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC Selected_Data_Group AS (
# MAGIC     SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         GRP.Conversation_Type AS Channel_type,
# MAGIC         GRP.Channel_ID,
# MAGIC         GRP.Actual_Text AS GrpText,
# MAGIC         TRY_CAST(GRP.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_groups GRP
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON GRP.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON GRP.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(GRP.DW_Timestamp  as Date)
# MAGIC ),
# MAGIC
# MAGIC Slack_All_Data_DM as (
# MAGIC  SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_ID as Activity_ID,
# MAGIC         Channel_type,
# MAGIC         DT1 as Activity_Date,
# MAGIC 		period_year,
# MAGIC 		week_end_date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touch_ID,
# MAGIC         Actual_Text
# MAGIC     FROM Slack_Private_Public_Channels
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, Actual_Text, DT1, FifteenMinGroup,Employee_Name, period_year, week_end_date
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1 as Activity_Date,
# MAGIC 		period_year,
# MAGIC 		week_end_date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         DMText AS Actual_Text
# MAGIC     FROM Selected_Data_DM
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, Actual_Text, DT1, FifteenMinGroup,Employee_Name, period_year, week_end_date
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC         Employee_Name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1 as Activity_Date,
# MAGIC 		period_year,
# MAGIC 		week_end_date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         GrpText AS Actual_Text
# MAGIC     FROM Selected_Data_Group
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, Actual_Text, DT1, FifteenMinGroup,Employee_Name, period_year, week_end_date
# MAGIC 	),
# MAGIC 	
# MAGIC
# MAGIC Slack_Conversations_All as (
# MAGIC
# MAGIC Select 
# MAGIC CONCAT_WS('', User_ID, Channel_name, Channel_type, Activity_Date, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC Case when Channel_type = 'Public' then 'Slack - Public Channel' End as Activity_Type,
# MAGIC Activity_ID,
# MAGIC Employee_Name, 
# MAGIC Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Touch_ID as Touch_Type,
# MAGIC CAST(6 AS INT) AS Source_System_ID,
# MAGIC 'Slack' as Source_System_Type
# MAGIC from Slack_All_Data_DM
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC Select 
# MAGIC CONCAT_WS('', User_ID, Channel_name, Channel_type, Activity_Date, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC Case when Channel_type = 'Private' then 'Slack - Private Channel' End as Activity_Type,
# MAGIC Activity_ID,
# MAGIC Employee_Name, 
# MAGIC Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Touch_ID as Touch_Type,
# MAGIC CAST(6 AS INT) AS Touch_ID,
# MAGIC 'Slack' as Source_System_Type
# MAGIC from Slack_All_Data_DM
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC Select 
# MAGIC CONCAT_WS('', User_ID, Channel_name, Channel_type, Activity_Date, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC Case when Channel_type = 'Direct Message' then 'Slack - Direct Message' End as Activity_Type,
# MAGIC Activity_ID,
# MAGIC Employee_Name, 
# MAGIC Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC touch_id,
# MAGIC CAST(6 AS INT) AS Touch_ID,
# MAGIC 'Slack' as Source_System_Type
# MAGIC from Slack_All_Data_DM
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC Select 
# MAGIC CONCAT_WS('', User_ID, Channel_name, Channel_type, Activity_Date, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC Case when Channel_type = 'Group Message' then 'Slack - Group Message' End as Activity_Type,
# MAGIC Activity_ID,
# MAGIC Employee_Name, 
# MAGIC Activity_Date,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Touch_id,
# MAGIC CAST(6 AS INT) AS Source_System_ID,
# MAGIC 'Slack' as Source_System_Type
# MAGIC from Slack_All_Data_DM
# MAGIC ),
# MAGIC ----------------------------------------------- Reactions Data for all Public , Private , Reactions --------------------------------------------
# MAGIC reactions_selected_data AS (
# MAGIC     SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         h.Conversation_Type AS Channel_type,
# MAGIC         h.Channel_ID,
# MAGIC         h.Reaction_Name,
# MAGIC         TRY_CAST(h.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_reactions_new AS h
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON h.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON h.User_ID = u.User_ID 
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(h.DW_Timestamp  as Date)
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC reactions_selected_data_DM AS (
# MAGIC     SELECT
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         DM.Conversation_Type AS Channel_type,
# MAGIC         DM.Channel_ID,
# MAGIC         DM.Reactions AS Reaction_Name,
# MAGIC         TRY_CAST(DM.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		week_end_date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_dm DM
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON DM.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u ON DM.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(DM.DW_Timestamp  as Date)
# MAGIC     WHERE Is_Reactions = '1'
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC reactions_selected_data_Group AS (
# MAGIC     SELECT
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC 		u.User_Real_Name as Employee_Name,
# MAGIC         GRP.Conversation_Type AS Channel_type,
# MAGIC         GRP.Channel_ID,
# MAGIC         GRP.Reactions AS Reaction_Name,
# MAGIC         TRY_CAST(GRP.DW_Timestamp AS Date) AS DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_groups GRP
# MAGIC         LEFT JOIN silver.silver_slack_channels_new AS c
# MAGIC         ON GRP.Channel_ID = c.Channel_ID
# MAGIC         LEFT JOIN silver.silver_slack_users_new AS u
# MAGIC         ON GRP.User_ID = u.User_ID
# MAGIC 		LEFT JOIN analytics.dim_date on dim_date.Calendar_Date = try_cast(GRP.DW_Timestamp  as Date)
# MAGIC     WHERE Is_Reactions = '1'
# MAGIC ),
# MAGIC
# MAGIC reaction_aggregated_data AS (
# MAGIC     SELECT
# MAGIC         User_ID,
# MAGIC         Channel_ID as Activity_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         Reaction_Name
# MAGIC     FROM reactions_selected_data
# MAGIC     GROUP BY User_ID, Channel_ID, Channel_name, Channel_type, Reaction_Name, DT1, FifteenMinGroup, Employee_Name , period_year,Week_end_Date
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         User_ID,
# MAGIC         Channel_ID as Activity_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         Reaction_Name
# MAGIC     FROM reactions_selected_data_DM
# MAGIC     GROUP BY User_ID, Channel_ID, Channel_name, Channel_type, Reaction_Name, DT1, FifteenMinGroup , Employee_Name , period_year,Week_end_Date
# MAGIC  
# MAGIC   UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         User_ID,
# MAGIC         Channel_ID as Touch_ID,
# MAGIC         Channel_name,
# MAGIC 		Employee_Name,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC 		period_year,
# MAGIC 		Week_end_Date,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         Reaction_Name
# MAGIC     FROM reactions_selected_data_Group
# MAGIC     GROUP BY User_ID, Channel_ID, Channel_name, Channel_type, Reaction_Name, DT1, FifteenMinGroup , Employee_Name , period_year,Week_end_Date
# MAGIC ),
# MAGIC
# MAGIC Slack_reactions_all as (
# MAGIC SELECT
# MAGIC         CONCAT_WS('', User_ID, Channel_name, Channel_type, DT1, FifteenMinGroup) AS DW_Touch_ID,
# MAGIC         'Slack - Reactions' AS Activity_Type,
# MAGIC         Activity_ID,
# MAGIC         Employee_Name AS full_name,
# MAGIC         DT1 AS Activity_Date,
# MAGIC         	period_year,
# MAGIC           Week_end_Date,
# MAGIC        -- User_ID AS Email,
# MAGIC         Touches_Score As Touch_Type,
# MAGIC         CAST(6 AS INT) AS Sourcesystem_id,
# MAGIC 		 'Slack' as Source_System_Type
# MAGIC from reaction_aggregated_data		 
# MAGIC ),
# MAGIC
# MAGIC Slack_Conversation_Reactions as (
# MAGIC   Select * from Slack_Conversations_All 
# MAGIC   
# MAGIC Union
# MAGIC   
# MAGIC Select * from Slack_reactions_all 
# MAGIC ),
# MAGIC
# MAGIC Final_Code as (
# MAGIC Select 
# MAGIC DW_Touch_ID,
# MAGIC Activity_ID,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC null as Office,
# MAGIC Employee_Name,
# MAGIC Touch_Type,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC Source_System_Type
# MAGIC from Slack_Conversation_Reactions limit 100)
# MAGIC
# MAGIC Select *,
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
# MAGIC CREATE TABLE Silver.Sliver_Touches_Slack (
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