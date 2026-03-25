# Databricks notebook source
# MAGIC %md
# MAGIC ##Row Count

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from bronze.slack_conversations_history_groups

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1) from silver.silver_slack_Conversations_History_Groups

# COMMAND ----------

# MAGIC %md
# MAGIC #Null count check for bronze Conversations

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'Channel_ID' AS column_name, COUNT(*) AS null_count FROM bronze.Slack_Conversations_History_Groups WHERE Channel_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'user', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE user IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'name', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE name IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'type', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE type IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'ts', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE ts IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'client_msg_id', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE client_msg_id IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'text', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE text IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'team', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'attachments', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE attachments IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'thread_ts', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE thread_ts IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'parent_user_id', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE parent_user_id IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'blocks', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE blocks IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'upload', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE upload IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'edited', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE edited IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'files', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE files IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'reply_count', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE reply_count IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'reply_users_count', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE reply_users_count IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'latest_reply', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE latest_reply IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'reply_users', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE reply_users IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'replies', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE replies IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'user_team', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE user_team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'source_team', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE source_team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'user_profile', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE user_profile IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'is_locked', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE is_locked IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'reactions', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE reactions IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'HashKey', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE HashKey IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'MergeKey', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE MergeKey IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Conversation_Type', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE Conversation_Type IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'DW_Timestamp', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE DW_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Sourcesystem_Name', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE Sourcesystem_Name IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Created_By', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE Created_By IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Created_Date', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE Created_Date IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Last_Modified_By', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE Last_Modified_By IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Last_Modified_Date', COUNT(*) FROM bronze.Slack_Conversations_History_Groups WHERE Last_Modified_Date IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Null count for silver conversations

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'DW_Conversations_History_ID' AS column_name, COUNT(*) AS null_count FROM silver.Silver_Slack_Conversations_History_Groups WHERE DW_Conversations_History_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Channel_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Channel_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'User_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE User_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Channel_Name', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Channel_Name IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Message_Type', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Message_Type IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Message_Timestamp', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Message_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Client_Msg_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Client_Msg_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Actual_Text', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Actual_Text IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Team_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Team_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Attachments', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Attachments IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Thread_Msg_Timestamp', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Thread_Msg_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Parent_Msg_User_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Parent_Msg_User_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Blocks', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Blocks IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Is_Emoji', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Is_Emoji IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Is_Reactions', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Is_Reactions IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Message_Category', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Message_Category IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Upload', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Upload IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Edited', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Edited IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Files', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Files IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Reply_Count', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Reply_Count IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Actual_Replies', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Actual_Replies IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Replied_Users_Count', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Replied_Users_Count IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Latest_Reply_Timestamp', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Latest_Reply_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Replied_Users_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Replied_Users_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'User_Team', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE User_Team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Source_Team', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Source_Team IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'User_Profile', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE User_Profile IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Is_Locked', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Is_Locked IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Reactions', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Reactions IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'MergeKey', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE MergeKey IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'HashKey', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE HashKey IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Conversation_Type', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Conversation_Type IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'DW_Timestamp', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE DW_Timestamp IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Workspace_ID', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Workspace_ID IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Workspace_Name', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Workspace_Name IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Sourcesystem_Name', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Sourcesystem_Name IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Created_By', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Created_By IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Created_Date', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Created_Date IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Last_Modified_By', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Last_Modified_By IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'Last_Modified_Date', COUNT(*) FROM silver.Silver_Slack_Conversations_History_Groups WHERE Last_Modified_Date IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicate check for bronze Conversations

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(ts), MergeKey ,count(*) from bronze.Slack_Conversations_History_Groups
# MAGIC group by ts, MergeKey 
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicate check for Silver Conversations

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(Message_Timestamp),MergeKey,count(*) from silver.silver_slack_conversations_History_Groups
# MAGIC group by Message_Timestamp,MergeKey
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check In Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select Channel_ID,text from Bronze.slack_conversations_history_Groups where Conversation_Type = "Group Message" order by ts asc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check In Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select Channel_ID,Actual_Text from silver.Silver_Slack_Conversations_History_Groups where Conversation_Type = "Group Message" order by  Message_Timestamp asc limit 10
# MAGIC