# Databricks notebook source
# MAGIC %md
# MAGIC ## Reporting Views
# MAGIC * **Description:** To bulid a code that extracts req data from source for Carrier KPI report
# MAGIC * **Created Date:** 06/25/2025
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 07/15/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact_Carrier_Touches

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table analytics.Fact_Carrier_Touches
# MAGIC as
# MAGIC
# MAGIC with Temp as 
# MAGIC (
# MAGIC     select 
# MAGIC
# MAGIC touch_id as Touches_ID,
# MAGIC  Touch_type as Touches_Type,
# MAGIC   full_name as Employee_Name,
# MAGIC   EMP.Email_Address  as Email,
# MAGIC   
# MAGIC     w.Weight * 1 as Touches_Score,
# MAGIC      date as Touch_Timestamp,
# MAGIC
# MAGIC  --case when touch_type like 'Aljex%' then 'Aljex' else 'Relay' end as Source_System, 
# MAGIC  case when touch_type like 'Aljex%' then 1 else 2 end as Sourcesystem_id
# MAGIC
# MAGIC from Bronze.Load_Touch as LT
# MAGIC
# MAGIC Left Join
# MAGIC
# MAGIC analytics.dim_employee_brokerage_mapping as EMP
# MAGIC
# MAGIC on LT.full_name = EMP.TMS_Name     --- all the TMS name are matched with the Full name in load_Touch
# MAGIC
# MAGIC left join 
# MAGIC
# MAGIC analytics.tms_events_weightage as W
# MAGIC
# MAGIC on LT.Touch_type  = W.Events
# MAGIC )
# MAGIC
# MAGIC ,
# MAGIC temp2 as 
# MAGIC (
# MAGIC select touches_ID,Touches_Type, Employee_Name, Email, Touches_score, Touch_Timestamp,sourcesystem_id from gold.fact_touches where sourcesystem_id not in (1,2,6)
# MAGIC )
# MAGIC ,
# MAGIC
# MAGIC  selected_data_Public_Private AS (
# MAGIC SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC         h.Conversation_Type AS Channel_type,
# MAGIC         h.Channel_ID,
# MAGIC         h.Actual_Text,
# MAGIC         TRY_CAST(h.DW_Timestamp AS Date) AS DT1,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_new AS h
# MAGIC     LEFT JOIN silver.silver_slack_channels_new AS c ON h.Channel_ID = c.Channel_ID
# MAGIC     LEFT JOIN silver.silver_slack_users_new AS u ON h.User_ID = u.User_ID
# MAGIC )
# MAGIC
# MAGIC ,Selected_Data_DM As (
# MAGIC SELECT 
# MAGIC     REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC     c.Channel_Name,
# MAGIC     DM.Conversation_Type AS Channel_type,
# MAGIC     DM.Channel_ID,
# MAGIC     DM.Actual_Text AS DMText,
# MAGIC     TRY_CAST(DM.DW_Timestamp AS Date) AS DT1,
# MAGIC     CASE
# MAGIC         WHEN MINUTE(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC         WHEN MINUTE(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC         WHEN MINUTE(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC         WHEN MINUTE(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC     END AS FifteenMinGroup
# MAGIC FROM 
# MAGIC     silver.silver_slack_conversations_history_dm DM
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_channels_new AS c ON DM.Channel_ID = c.Channel_ID
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_users_new AS u ON DM.User_ID = u.User_ID
# MAGIC )
# MAGIC
# MAGIC ,Selected_Data_Group As 
# MAGIC (
# MAGIC SELECT 
# MAGIC     REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC     c.Channel_Name,
# MAGIC     GRP.Conversation_Type AS Channel_type,
# MAGIC     GRP.Channel_ID,
# MAGIC     GRP.Actual_Text AS GrpText,
# MAGIC     TRY_CAST(GRP.DW_Timestamp AS Date) AS DT1,
# MAGIC     CASE
# MAGIC         WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC         WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC         WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC         WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC     END AS FifteenMinGroup
# MAGIC FROM 
# MAGIC     Silver.silver_slack_conversations_history_groups GRP
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_channels_new AS c ON GRP.Channel_ID = c.Channel_ID
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_users_new AS u ON GRP.User_ID = u.User_ID
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC     -- Aggregate the dataframe based on User_ID, Channel_name, Channel_type, and datetime (every 15 mins)
# MAGIC     -- Calculate the count of action ID
# MAGIC     ,aggregated_data AS (
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         Actual_Text
# MAGIC     FROM selected_data_Public_Private
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, Actual_Text, DT1, FifteenMinGroup
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         DMText AS Actual_Text
# MAGIC     FROM Selected_Data_DM
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, DMText, DT1, FifteenMinGroup
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         GrpText AS Actual_Text
# MAGIC     FROM Selected_Data_Group
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, GrpText, DT1, FifteenMinGroup
# MAGIC ),
# MAGIC 	
# MAGIC slack_meassages as (
# MAGIC     -- Store the result in the DataFrame DF_Touches_Slack
# MAGIC     SELECT
# MAGIC         
# MAGIC         CONCAT_WS(User_ID, Channel_name, Channel_type, DT1,FifteenMinGroup) AS Touches_id,
# MAGIC         'Slack - Activities' AS Touch_Type,
# MAGIC         Null as full_name,
# MAGIC         User_ID as Email,
# MAGIC         Touches_Score,
# MAGIC         DT1 AS Date,
# MAGIC         CAST(6 AS INT) AS Source_system_id,
# MAGIC 		CASE WHEN Channel_type = 'Public' THEN Channel_ID END AS Publicchannel,
# MAGIC 		CASE WHEN Channel_type = 'Private' THEN Channel_ID END AS Privatechannel,
# MAGIC 		CASE WHEN Channel_type = 'Direct Message' THEN Channel_ID END AS DirectMessages,
# MAGIC 		CASE WHEN Channel_type = 'Group Message' THEN Channel_ID END AS GroupMessages,
# MAGIC 		CASE WHEN Channel_type = 'Public' THEN Actual_Text END AS Publicchannelconversation,
# MAGIC 		CASE WHEN Channel_type = 'Private' THEN Actual_Text END AS Privatechannelconversation,
# MAGIC 		CASE WHEN Channel_type = 'Direct Message' THEN Actual_Text END AS DMConversations,
# MAGIC 		CASE WHEN Channel_type = 'Group Message' THEN Actual_Text END AS GroupConverstation
# MAGIC 		
# MAGIC     FROM aggregated_data)
# MAGIC
# MAGIC ----Select * from slack_meassages  where Date >= '2024-07-28' and Date <= '2024-08-24'
# MAGIC
# MAGIC
# MAGIC
# MAGIC --- Reactions
# MAGIC
# MAGIC ,
# MAGIC  reactions_selected_data AS (select REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,c.Channel_Name,
# MAGIC h.Conversation_Type as Channel_type,	
# MAGIC h.Channel_ID,
# MAGIC h.Reaction_Name,
# MAGIC --DM.Reactions as DMReaction,
# MAGIC
# MAGIC             try_cast(h.DW_Timestamp AS Date) as DT1,
# MAGIC         CASE
# MAGIC             WHEN minute(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN minute(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN minute(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN minute(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC from  silver.silver_slack_reactions_new as h
# MAGIC left join  silver.silver_slack_channels_new as c
# MAGIC on h.Channel_ID = c.Channel_ID ---and c.Is_Private = false
# MAGIC left join silver.silver_slack_users_new as u
# MAGIC on h.User_ID = u.User_ID
# MAGIC ---where h.reply_count is null
# MAGIC
# MAGIC     )
# MAGIC
# MAGIC , reactions_selected_data_DM AS (
# MAGIC select REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC c.Channel_Name,
# MAGIC DM.Conversation_Type as Channel_type,	
# MAGIC DM.Channel_ID,
# MAGIC --h.Reaction_Name,
# MAGIC DM.Reactions as Reaction_Name,
# MAGIC
# MAGIC             try_cast(DM.DW_Timestamp AS Date) as DT1,
# MAGIC         CASE
# MAGIC             WHEN minute(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN minute(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN minute(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN minute(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC
# MAGIC FROM 
# MAGIC     silver.silver_slack_conversations_history_dm DM
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_channels_new AS c ON DM.Channel_ID = c.Channel_ID
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_users_new AS u ON DM.User_ID = u.User_ID
# MAGIC where Is_Reactions = '1'
# MAGIC ---where h.reply_count is null
# MAGIC
# MAGIC     )
# MAGIC
# MAGIC ,reactions_selected_data_Group as (
# MAGIC select REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC c.Channel_Name,
# MAGIC GRP.Conversation_Type as Channel_type,	
# MAGIC GRP.Channel_ID,
# MAGIC --h.Reaction_Name,
# MAGIC GRP.Reactions as Reaction_Name,
# MAGIC
# MAGIC             try_cast(GRP.DW_Timestamp AS Date) as DT1,
# MAGIC         CASE
# MAGIC             WHEN minute(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN minute(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN minute(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN minute(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC
# MAGIC FROM 
# MAGIC     silver.silver_slack_conversations_history_groups GRP
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_channels_new AS c ON GRP.Channel_ID = c.Channel_ID
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_users_new AS u ON GRP.User_ID = u.User_ID
# MAGIC where Is_Reactions = '1'
# MAGIC )
# MAGIC
# MAGIC     -- Aggregate the dataframe based on User_ID, Channel_name, Channel_type, and datetime (every 15 mins)
# MAGIC     -- Calculate the count of action ID
# MAGIC     , reaction_aggregated_data AS (
# MAGIC         SELECT
# MAGIC             User_ID,
# MAGIC 			Channel_ID,
# MAGIC             Channel_name,
# MAGIC             Channel_type,
# MAGIC             DT1,
# MAGIC             FifteenMinGroup,
# MAGIC             CAST(1 AS INT) AS Touches_Score,
# MAGIC 			Reaction_Name
# MAGIC             --DMReaction
# MAGIC         FROM reactions_selected_data
# MAGIC         GROUP BY User_ID,Channel_ID, Channel_name, Channel_type,Reaction_Name, DT1,FifteenMinGroup
# MAGIC 		
# MAGIC 		Union 
# MAGIC 		
# MAGIC 		 SELECT
# MAGIC             User_ID,
# MAGIC 			Channel_ID,
# MAGIC             Channel_name,
# MAGIC             Channel_type,
# MAGIC             DT1,
# MAGIC             FifteenMinGroup,
# MAGIC             CAST(1 AS INT) AS Touches_Score,
# MAGIC 			Reaction_Name  
# MAGIC         FROM reactions_selected_data_DM
# MAGIC         GROUP BY User_ID,Channel_ID, Channel_name, Channel_type,Reaction_Name, DT1,FifteenMinGroup
# MAGIC 		
# MAGIC 		UNION
# MAGIC 		SELECT
# MAGIC             User_ID,
# MAGIC 			Channel_ID,
# MAGIC             Channel_name,
# MAGIC             Channel_type,
# MAGIC             DT1,
# MAGIC             FifteenMinGroup,
# MAGIC             CAST(1 AS INT) AS Touches_Score,
# MAGIC 			Reaction_Name           
# MAGIC         FROM reactions_selected_data_Group
# MAGIC         GROUP BY User_ID,Channel_ID, Channel_name, Channel_type,Reaction_Name, DT1,FifteenMinGroup
# MAGIC 			
# MAGIC     )
# MAGIC 	
# MAGIC --Select * from reaction_aggregated_data limit 100
# MAGIC ,
# MAGIC Slack_reactions as (
# MAGIC     -- Store the result in the DataFrame DF_Touches_Slack
# MAGIC     SELECT
# MAGIC         
# MAGIC         CONCAT_WS(User_ID, Channel_name, Channel_type, DT1,FifteenMinGroup) AS Touches_id,
# MAGIC         'Slack - Reactions' AS Touch_Type,
# MAGIC         Null as full_name,
# MAGIC         User_ID as Email,
# MAGIC         Touches_Score,
# MAGIC         DT1 AS Date,
# MAGIC         CAST(6 AS INT) AS Source_system_id,
# MAGIC 		CASE WHEN Channel_type = 'Public' THEN Channel_ID END AS Publicchannel,
# MAGIC 		CASE WHEN Channel_type = 'Private' THEN Channel_ID END AS Privatechannel,
# MAGIC 		CASE WHEN Channel_type = 'Direct Message' THEN Channel_ID END AS DirectMessages,
# MAGIC 		CASE WHEN Channel_type = 'Group Message' THEN Channel_ID END AS GroupMessages,
# MAGIC 		CASE WHEN Channel_type = 'Public' THEN Reaction_Name END AS Publicchannelconversation,
# MAGIC 		CASE WHEN Channel_type = 'Private' THEN Reaction_Name END AS Privatechannelconversation,
# MAGIC         CASE WHEN Channel_type = 'Direct Message' THEN Reaction_Name END AS DMConversations,
# MAGIC 		CASE WHEN Channel_type = 'Group Message' THEN Reaction_Name END AS GroupConverstation		
# MAGIC     FROM reaction_aggregated_data)
# MAGIC
# MAGIC ,
# MAGIC
# MAGIC slack_consolidated as (
# MAGIC
# MAGIC     select * from Slack_reactions
# MAGIC
# MAGIC     union 
# MAGIC
# MAGIC select * from slack_meassages
# MAGIC )
# MAGIC ,
# MAGIC hubspot_calls_df( select 
# MAGIC hs_activity_type,
# MAGIC hs_created_by,
# MAGIC hs_object_id,
# MAGIC hubspot_owner_id,
# MAGIC hs_createdate,
# MAGIC from_utc_timestamp(hs_createdate, 'CST') AS hs_createdate_cst,
# MAGIC SourcesystemID
# MAGIC from bronze.hubspot_calls where sourcesystemid = '5' and hs_activity_type = 'Introduce NFI'),
# MAGIC
# MAGIC hubspot_owners_df as (
# MAGIC select id,email,user_id from bronze.hubspot_owners ),
# MAGIC
# MAGIC Hubspot_call as (select distinct
# MAGIC 'Hubspot-Introcalls' as Touches_Type,
# MAGIC CONCAT_WS(hs_object_id,email,'Hubspot-Introcalls',hs_createdate_cst) AS Touches_id,
# MAGIC email AS email,
# MAGIC '' as Employee_Name,
# MAGIC 1 as Touches_score,
# MAGIC hs_createdate_cst,
# MAGIC CAST(hs_createdate_cst AS TIMESTAMP) AS Touch_Timestamp,
# MAGIC SourcesystemID AS sourcesystem_id
# MAGIC FROM hubspot_calls_df n
# MAGIC JOIN Hubspot_Owners_DF o ON n.hs_created_by = o.user_id),
# MAGIC
# MAGIC consolidate as (
# MAGIC select touches_ID,Touches_Type, Employee_Name, Email, Touches_score, Touch_Timestamp,sourcesystem_id,Null as Publicchannel,Null as Privatechannel, Null as DirectMessages, 
# MAGIC Null as GroupMessages , Null as Publicchannelconversation,Null as Privatechannelconversation,
# MAGIC Null as DMConversations,
# MAGIC Null as GroupConverstation
# MAGIC from gold.fact_touches
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select *,Null as Publicchannel,Null as Privatechannel, Null as DirectMessages, 
# MAGIC Null as GroupMessages , Null as Publicchannelconversation,Null as Privatechannelconversation,
# MAGIC Null as DMConversations,
# MAGIC Null as GroupConverstation
# MAGIC  from temp 
# MAGIC
# MAGIC union 
# MAGIC select touches_ID,Touches_Type, Employee_Name, Email, Touches_score, Touch_Timestamp,sourcesystem_id,Null as Publicchannel,Null as Privatechannel, Null as DirectMessages, 
# MAGIC Null as GroupMessages , Null as Publicchannelconversation,Null as Privatechannelconversation,
# MAGIC Null as DMConversations,
# MAGIC Null as GroupConverstation
# MAGIC from Hubspot_call
# MAGIC union 
# MAGIC select * from slack_consolidated )
# MAGIC
# MAGIC --Select * from slack_consolidated where DMConversations is not null;
# MAGIC ,
# MAGIC
# MAGIC  EmployeeQuartiles AS (
# MAGIC     SELECT
# MAGIC         lower(email) AS Email,
# MAGIC         m.`Group` AS Role,
# MAGIC         period_year,
# MAGIC         financial_period_number,
# MAGIC         Calendar_Year,
# MAGIC         sum(Touches_Score) AS Touch_Score,
# MAGIC 		COUNT (distinct Publicchannel) as Publicchannel,
# MAGIC 		COUNT (distinct Privatechannel) as Privatechannel,
# MAGIC 		COUNT (distinct DirectMessages) as DirectMessages,
# MAGIC 		COUNT (distinct GroupMessages) as GroupMessages,
# MAGIC 		COUNT (Publicchannelconversation) as Publicchannel_conversation,
# MAGIC 		COUNT (Privatechannelconversation) as Privatechannel_conversation,
# MAGIC 		COUNT (DMConversations) as DMConversations,
# MAGIC 		COUNT (GroupConverstation) as GroupConverstation,
# MAGIC 		(COUNT(Publicchannelconversation) + COUNT(Privatechannelconversation) + COUNT (DMConversations) + COUNT (GroupConverstation) ) AS Total_conversation,
# MAGIC         NTILE(4) OVER (PARTITION BY m.`Group`, period_year, financial_period_number, Calendar_Year ORDER BY sum(Touches_Score) DESC) AS Quartile_Number
# MAGIC     FROM 
# MAGIC         consolidate AS T
# MAGIC         LEFT JOIN `gold`.`lookup_sourcesystem` AS S ON T.Sourcesystem_ID = S.DW_Sourcesystem_ID
# MAGIC         INNER JOIN analytics.dim_employee_brokerage_mapping AS m ON lower(T.email) = m.Email_Address
# MAGIC         LEFT JOIN (
# MAGIC             SELECT calendar_date, period_year, financial_period_number, Calendar_Year 
# MAGIC             FROM `analytics`.`dim_date`
# MAGIC         ) AS Cal ON cast(T.Touch_Timestamp AS Date) = Cal.calendar_date
# MAGIC     WHERE 
# MAGIC         CAST(T.Touch_Timestamp AS Date) BETWEEN DATEADD(year, -2, CURRENT_DATE()) AND CURRENT_DATE()
# MAGIC     GROUP BY 
# MAGIC         lower(email), period_year, financial_period_number, Calendar_Year, m.`Group`
# MAGIC )
# MAGIC ,
# MAGIC  TouchesSummary AS (
# MAGIC     -- The provided CTE to summarize touch data
# MAGIC     -- You can keep this CTE as is
# MAGIC     SELECT 
# MAGIC         lower(email) AS Email,
# MAGIC         Touches_Type,
# MAGIC         COALESCE(S.Sourcesystem_Name, 'Slack') AS Sourcesystem_Name,
# MAGIC         Sourcesystem_ID,
# MAGIC         CAST(Touch_Timestamp AS Date) AS Touch_Date,
# MAGIC         SUM(Touches_Score) AS Touch_Score,
# MAGIC         CASE 
# MAGIC             WHEN Touches_Type IN ('Aljex - Load Booked', 'Aljex - Marked Delivered', 'Aljex - Marked Driver_Dispatched', 'Aljex - Marked Loaded') THEN 'TMS - Aljex'
# MAGIC             WHEN Touches_Type IN ('Hubspot-Deals', 'Hubspot-Email', 'Hubspot-Meeting', 'Hubspot-Notes','Hubspot-Introcalls') THEN 'Hubspot'
# MAGIC             WHEN Touches_Type IN ('Relay - Appt Scheduled', 'Relay - Assigned load', 'Relay - Bounced a Load', 'Relay - Detention Reported', 'Relay - Driver Dispatched', 'Relay - Driver Info Captured', 'Relay - Equipment Assigned', 'Relay - In Transit Update Captured', 'Relay - Load Booked', 'Relay - Load Cancelled', 'Relay - Manual Tender Created', 'Relay - Marked Arrived At Stop', 'Relay - Marked Delivered', 'Relay - Marked Loaded', 'Relay - Put Offer on a Load', 'Relay - Reserved a Load', 'Relay - Rolled a Load', 'Relay - Set a Max Buy', 'Relay - Stop Marked Delivered', 'Relay - Stop Recorded', 'Relay - Tender Accepted', 'Relay - Tracking Contact Info Captured', 'Relay - Tracking Note Captured', 'Relay - Tracking Stopped') THEN 'TMS - Relay'
# MAGIC             WHEN Touches_Type in ('Slack - Activities','Slack - Reactions')  THEN 'Slack'
# MAGIC             ELSE 'Other'
# MAGIC         END AS Event_Group,
# MAGIC         CASE 
# MAGIC             WHEN Touches_Type IN ('Aljex - Load Booked', 'Aljex - Marked Delivered', 'Aljex - Marked Driver_Dispatched', 'Aljex - Marked Loaded') THEN 'Operational'
# MAGIC             WHEN Touches_Type IN ('Hubspot-Deals', 'Hubspot-Email', 'Hubspot-Meeting', 'Hubspot-Notes','Hubspot-Introcalls') THEN 'Hubspot'
# MAGIC             WHEN Touches_Type IN ('Relay - Appt Scheduled', 'Relay - Assigned load', 'Relay - Bounced a Load', 'Relay - Detention Reported', 'Relay - Driver Dispatched', 'Relay - Driver Info Captured', 'Relay - Equipment Assigned', 'Relay - In Transit Update Captured', 'Relay - Load Booked', 'Relay - Load Cancelled', 'Relay - Manual Tender Created', 'Relay - Marked Arrived At Stop', 'Relay - Marked Delivered', 'Relay - Marked Loaded', 'Relay - Put Offer on a Load', 'Relay - Reserved a Load', 'Relay - Rolled a Load', 'Relay - Set a Max Buy', 'Relay - Stop Marked Delivered', 'Relay - Stop Recorded', 'Relay - Tender Accepted', 'Relay - Tracking Contact Info Captured', 'Relay - Tracking Note Captured', 'Relay - Tracking Stopped') THEN 'Operational'
# MAGIC             WHEN Touches_Type in ('Slack - Activities','Slack - Reactions') THEN 'Slack'
# MAGIC             ELSE 'Other'
# MAGIC         END AS Event_Type
# MAGIC     FROM 
# MAGIC         consolidate AS T
# MAGIC         LEFT JOIN  `gold`.`lookup_sourcesystem` AS S ON T.Sourcesystem_ID = S.DW_Sourcesystem_ID
# MAGIC     WHERE 
# MAGIC         CAST(T.Touch_Timestamp AS Date) BETWEEN DATEADD(year, -2, CURRENT_DATE()) AND CURRENT_DATE()
# MAGIC     GROUP BY 
# MAGIC         lower(email), Touches_Type, CAST(Touch_Timestamp AS Date), S.Sourcesystem_Name, Sourcesystem_ID
# MAGIC ), 
# MAGIC
# MAGIC TouchesAggregated AS (
# MAGIC     -- CTE to aggregate touch data according to specifications
# MAGIC     SELECT 
# MAGIC         D.Calendar_Year AS Calendar_Year,
# MAGIC         D.financial_period_number AS Period,
# MAGIC         CONCAT(D.financial_period_number, '-', D.Calendar_Year) AS Period_Year,
# MAGIC         SUM(Touch_Score) AS Total_Touch_Score,
# MAGIC        TS.Touch_Date,
# MAGIC         SUM(CASE WHEN event_type = 'Operational' THEN Touch_Score ELSE 0 END) AS Operational_Touches,
# MAGIC         SUM(CASE WHEN Event_Group = 'TMS - Relay' THEN Touch_Score ELSE 0 END) AS Relay_Touches,
# MAGIC         SUM(CASE WHEN Event_Group = 'TMS - Aljex' THEN Touch_Score ELSE 0 END) AS Aljex_Touches,
# MAGIC         SUM(CASE WHEN Event_Group = 'Hubspot' THEN Touch_Score ELSE 0 END) AS Hubspot_Touches,
# MAGIC         SUM(CASE WHEN Event_Group = 'Slack' THEN Touch_Score ELSE 0 END) AS Slack_Touches,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Hubspot-Email' THEN Touch_Score ELSE 0 END) AS Hubspot_Email_Touches,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Hubspot-Deals' THEN Touch_Score ELSE 0 END) AS Hubspot_Deals,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Hubspot-Notes' THEN Touch_Score ELSE 0 END) AS Hubspot_Notes,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Hubspot-Meeting' THEN Touch_Score ELSE 0 END) AS Hubspot_Meetings,
# MAGIC 		SUM(CASE WHEN Touches_Type = 'Hubspot-Introcalls' THEN Touch_Score ELSE 0 END) AS Hubspot_Introcalls,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Slack - Activities' THEN Touch_Score ELSE 0 END) AS Slack_Message_Touches,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Slack - Reactions' THEN Touch_Score ELSE 0 END) AS Slack_Reaction_Touches,
# MAGIC         Email AS Email
# MAGIC
# MAGIC     FROM 
# MAGIC         TouchesSummary TS
# MAGIC         JOIN `analytics`.`dim_date` D ON TS.Touch_Date = D.calendar_date
# MAGIC     GROUP BY 
# MAGIC         D.Calendar_Year, D.financial_period_number, D.Calendar_Year, Email,touch_date
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC RevenueSummary AS (
# MAGIC Select period_year, financial_period_number, Calendar_Year,emp.Email_address,(Sum(Revenue)/2) as Revenue,count(distinct loadnum) as Volume,((sum(Revenue)/2) - (sum(expense)/2) ) as Margin  from 
# MAGIC (
# MAGIC select 'Carrier' as Revenue_Type ,
# MAGIC CASE 
# MAGIC         WHEN Full_name IS NULL THEN booked_by 
# MAGIC         ELSE Full_name 
# MAGIC     END AS Full_name, l.Ship_date as shipdate , l.Revenue,l.load_id as loadnum, l.expense from analytics.Fact_Load_GenAI_NonSplit as l 
# MAGIC left join `bronze`.`aljex_user_report_listing` as u1 
# MAGIC on l.booked_by = u1.full_name
# MAGIC where dot_num is not null
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC select 'Shipper' as Shipper ,
# MAGIC CASE 
# MAGIC         WHEN Full_name IS NULL THEN l.Employee 
# MAGIC         ELSE Full_name 
# MAGIC     END AS Full_name, l.Ship_date as shipdate, l.Revenue,l.load_id as loadnum, l.expense from analytics.Fact_Load_GenAI_NonSplit as l 
# MAGIC left join `bronze`.`aljex_user_report_listing` as u1 
# MAGIC on l.Employee = u1.Full_name
# MAGIC where dot_num is not null and full_name is not null
# MAGIC )
# MAGIC as S
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC             SELECT calendar_date, period_year, financial_period_number, Calendar_Year 
# MAGIC             FROM `analytics`.`dim_date`
# MAGIC         ) AS Cal ON cast(S.Shipdate AS Date) = Cal.calendar_date
# MAGIC Left JOIN
# MAGIC analytics.dim_employee_brokerage_mapping as Emp
# MAGIC on s.Full_name = emp.TMS_name
# MAGIC where s.Shipdate BETWEEN DATEADD(year, -2, CURRENT_DATE()) AND CURRENT_DATE()
# MAGIC
# MAGIC group by
# MAGIC period_year, financial_period_number, Calendar_Year,emp.Email_address
# MAGIC
# MAGIC )
# MAGIC ,
# MAGIC
# MAGIC templast as (
# MAGIC SELECT concat("Quartile - ",Quartile_Number) as Quartile_Name,Quartile_Number, A2.*,A3.Revenue,A3.margin, A3.Volume,emp2.*,Concat(emp2.TMS_Name,',',A2.period_year) as TMS_period,
# MAGIC Concat(emp2.Employee_Name,',',A2.period_year) as Employee_period,A1.Publicchannel,A1.Privatechannel,A1.DirectMessages,A1.GroupMessages,
# MAGIC A1.Publicchannel_conversation,A1.Privatechannel_conversation,A1.DMConversations, A1.GroupConverstation,
# MAGIC A1.Total_conversation
# MAGIC FROM 
# MAGIC     EmployeeQuartiles as A1
# MAGIC     
# MAGIC     left join 
# MAGIC     
# MAGIC     TouchesAggregated as A2
# MAGIC     
# MAGIC     on A1.Email  = a2.Email and a1.period_year = a2.Period_Year
# MAGIC     
# MAGIC     left join 
# MAGIC
# MAGIC     RevenueSummary  as A3
# MAGIC     
# MAGIC     on A1.Email  = a3.Email_address and a1.period_year = a3.Period_Year
# MAGIC     
# MAGIC     left join analytics.dim_employee_brokerage_mapping as Emp2
# MAGIC on trim(A1.Email) = trim(Emp2.Email_Address) ),
# MAGIC
# MAGIC
# MAGIC hubspot as ( Select
# MAGIC Email,
# MAGIC null as Role,
# MAGIC 'HubSpot_Introcalls' as Touches_Type,
# MAGIC 'Hubspot' as Source_Type,
# MAGIC Employee_Name,
# MAGIC TMS_Name,
# MAGIC Internal_Group,
# MAGIC d.period_year,
# MAGIC d.financial_period_number,
# MAGIC D.Calendar_Year,
# MAGIC D.Week_End_Date,
# MAGIC Hubspot_Introcalls as Touch_Score,
# MAGIC Employee_Period,
# MAGIC TMS_Period,
# MAGIC concat(Email,TMS_Name,D.period_year) As TouchKey
# MAGIC from  templast left join analytics.dim_date D on templast.touch_date = Calendar_Date),
# MAGIC fact_touches as ( select 
# MAGIC     trim(T.email) AS email,
# MAGIC     m.`Group` as Role,
# MAGIC     Touches_Type,
# MAGIC     Case
# MAGIC       when Touches_Type like 'Slack%' then 'Slack'
# MAGIC       When lower(Touches_type) like 'hubspot%' then 'Hubspot'
# MAGIC       When Touches_type like 'Aljex%' then 'Aljex'
# MAGIC       When Touches_type like 'Relay%' then 'Relay'
# MAGIC       else 'Others'
# MAGIC     END as SourceType,
# MAGIC     m.Employee_Name,
# MAGIC     m.TMS_Name,
# MAGIC     m.Internal_Group,
# MAGIC     period_year,
# MAGIC     financial_period_number,
# MAGIC     Calendar_Year,
# MAGIC     Week_End_Date,
# MAGIC     sum(Touches_Score) as Touch_Score,
# MAGIC     CONCAT(m.Employee_Name, ',', period_year) AS Employee_period,
# MAGIC     CONCAT(m.TMS_Name, ',', period_year) AS TMS_period
# MAGIC   from
# MAGIC     `gold`.`fact_touches` as T
# MAGIC       left join `gold`.`lookup_sourcesystem` as S
# MAGIC         on T.Sourcesystem_ID = S.DW_Sourcesystem_ID
# MAGIC       inner join analytics.dim_employee_brokerage_mapping as m
# MAGIC         on lower(trim(T.email))= lower(trim(m.Email_Address))
# MAGIC       left join (
# MAGIC         select
# MAGIC           calendar_date,
# MAGIC           period_year,
# MAGIC           financial_period_number,
# MAGIC           Calendar_Year,
# MAGIC           Week_End_Date
# MAGIC         from
# MAGIC           `analytics`.`dim_date`
# MAGIC       ) as Cal
# MAGIC         on cast(T.Touch_Timestamp as Date) = cal.calendar_date
# MAGIC   where
# MAGIC     year(Touch_Timestamp) >= 2022
# MAGIC   group by
# MAGIC     T.email,
# MAGIC     period_year,
# MAGIC     financial_period_number,
# MAGIC     S.Sourcesystem_Name,
# MAGIC     Calendar_Year,
# MAGIC     Week_End_Date,
# MAGIC     m.`Group`,
# MAGIC     m.Employee_Name,
# MAGIC     m.TMS_Name,
# MAGIC     m.Internal_Group,
# MAGIC     Touches_Type
# MAGIC )
# MAGIC   
# MAGIC ,Final_code (
# MAGIC select  
# MAGIC Email,
# MAGIC Role,
# MAGIC Touches_Type,
# MAGIC SourceType,
# MAGIC Employee_Name,
# MAGIC TMS_Name,
# MAGIC Internal_Group,
# MAGIC period_year,
# MAGIC financial_period_number,
# MAGIC Calendar_Year,
# MAGIC week_end_date,
# MAGIC Touch_Score,
# MAGIC Employee_Period,
# MAGIC TMS_Period,
# MAGIC concat(Email,TMS_Name,week_end_date) as TouchKey from fact_touches
# MAGIC union 
# MAGIC Select
# MAGIC Email,
# MAGIC null as Role,
# MAGIC  Touches_Type,
# MAGIC  Source_Type,
# MAGIC Employee_Name,
# MAGIC TMS_Name,
# MAGIC Internal_Group,
# MAGIC period_year,
# MAGIC financial_period_number,
# MAGIC Calendar_Year,
# MAGIC Week_end_date,
# MAGIC sum(Touch_Score) as Touch_Score,
# MAGIC Employee_Period,
# MAGIC TMS_Period,
# MAGIC concat(Email,TMS_Name,Week_end_date) As TouchKey from hubspot group by 
# MAGIC Email,
# MAGIC  Role,
# MAGIC  Touches_Type,
# MAGIC  Source_Type,
# MAGIC Employee_Name,
# MAGIC TMS_Name,
# MAGIC Internal_Group,
# MAGIC period_year,
# MAGIC financial_period_number,
# MAGIC Calendar_Year,
# MAGIC Week_end_date,
# MAGIC Employee_Period,
# MAGIC TMS_Period,
# MAGIC TouchKey)
# MAGIC
# MAGIC Select * from Final_code