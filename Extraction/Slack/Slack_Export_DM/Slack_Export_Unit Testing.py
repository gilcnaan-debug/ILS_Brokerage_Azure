# Databricks notebook source
# MAGIC %md
# MAGIC ##Row Count

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from bronze.slack_Conversations_History_DM

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from silver.silver_slack_Conversations_History_DM

# COMMAND ----------

# MAGIC %md
# MAGIC #Null count check for bronze Conversations

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'Channel_ID' AS column_name, COUNT(*) AS null_count FROM bronze.Slack_Conversations_History_DM WHERE Channel_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'user', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE user IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'type', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE type IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ts', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE ts IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'client_msg_id', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE client_msg_id IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'text', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE text IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'team', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'attachments', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE attachments IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'thread_ts', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE thread_ts IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'parent_user_id', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE parent_user_id IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'blocks', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE blocks IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'upload', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE upload IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'edited', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE edited IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'files', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE files IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'reply_count', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE reply_count IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'reply_users_count', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE reply_users_count IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'latest_reply', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE latest_reply IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'reply_users', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE reply_users IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'replies', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE replies IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'user_team', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE user_team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'source_team', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE source_team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'user_profile', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE user_profile IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'is_locked', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE is_locked IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'reactions', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE reactions IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'HashKey', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE HashKey IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'MergeKey', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE MergeKey IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Conversation_Type', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE Conversation_Type IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'DW_Timestamp', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE DW_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Sourcesystem_Name', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE Sourcesystem_Name IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Created_By', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE Created_By IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Created_Date', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE Created_Date IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Last_Modified_By', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE Last_Modified_By IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Last_Modified_Date', COUNT(*) FROM bronze.Slack_Conversations_History_DM WHERE Last_Modified_Date IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Null count for silver conversations

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'DW_Conversations_History_ID' AS column_name, COUNT(*) AS null_count FROM silver.Silver_Slack_Conversations_History_DM WHERE DW_Conversations_History_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Channel_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Channel_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'User_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE User_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Message_Type', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Message_Type IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Message_Timestamp', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Message_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Client_Msg_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Client_Msg_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Actual_Text', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Actual_Text IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Team_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Team_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Attachments', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Attachments IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Thread_Msg_Timestamp', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Thread_Msg_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Parent_Msg_User_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Parent_Msg_User_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Blocks', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Blocks IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Is_Emoji', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Is_Emoji IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Is_Reactions', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Is_Reactions IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Message_Category', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Message_Category IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Upload', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Upload IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Edited', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Edited IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Files', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Files IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Reply_Count', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Reply_Count IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Actual_Replies', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Actual_Replies IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Replied_Users_Count', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Replied_Users_Count IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Latest_Reply_Timestamp', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Latest_Reply_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Replied_Users_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Replied_Users_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'User_Team', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE User_Team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Source_Team', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Source_Team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'User_Profile', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE User_Profile IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Is_Locked', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Is_Locked IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Reactions', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Reactions IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'MergeKey', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE MergeKey IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'HashKey', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE HashKey IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Conversation_Type', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Conversation_Type IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'DW_Timestamp', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE DW_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Workspace_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Workspace_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Workspace_Name', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Workspace_Name IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Sourcesystem_Name', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Sourcesystem_Name IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Created_By', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Created_By IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Created_Date', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Created_Date IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Last_Modified_By', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Last_Modified_By IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Last_Modified_Date', COUNT(*) FROM silver.Silver_Slack_Conversations_History_DM WHERE Last_Modified_Date IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicate check for bronze Conversations

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(ts), MergeKey ,count(*) from bronze.slack_conversations_history_DM
# MAGIC group by ts, MergeKey 
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicate check for Silver Conversations

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(Message_Timestamp),MergeKey,count(*) from silver.silver_slack_conversations_History_DM
# MAGIC group by Message_Timestamp,MergeKey
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check In Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select Channel_ID,text from Bronze.slack_conversations_history_DM where Conversation_Type = "Direct Message" order by ts asc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check In Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select Channel_ID,Actual_Text from silver.silver_slack_conversations_history_DM where Conversation_Type = "Direct Message" order by  Message_Timestamp asc limit 10
# MAGIC

# COMMAND ----------

