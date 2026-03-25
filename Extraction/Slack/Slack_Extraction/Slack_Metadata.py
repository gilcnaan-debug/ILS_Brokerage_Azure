# Databricks notebook source
# MAGIC %md
# MAGIC ##Slack_conversations_history

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.MasterMetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC Frequency,
# MAGIC IsActive,
# MAGIC LastLoadDateColumn ,
# MAGIC MergeKey ,
# MAGIC UnixTime 
# MAGIC )
# MAGIC VALUES ("Slack"	,
# MAGIC "SCH-1"	,
# MAGIC "Conversations"	,
# MAGIC "Slack_Conversations_History"	,
# MAGIC "incremental load"	,
# MAGIC "1hr",
# MAGIC "1",
# MAGIC "ts",
# MAGIC "Conversation_ID,Sourcesystem_ID,ChannelID,User,type,ts,edited,client_msg_id,text,team,thread_ts,reply_count,reply_users_count,latest_reply,reply_users,is_locked,subscribed,blocks,reactions,subtype,inviter",
# MAGIC "1511799718.000519"
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Slack_Conversations_Replies

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.MasterMetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC Frequency,
# MAGIC IsActive,
# MAGIC LastLoadDateColumn ,
# MAGIC MergeKey ,
# MAGIC UnixTime 
# MAGIC )
# MAGIC VALUES ("Slack"	,
# MAGIC "SCR-1"	,
# MAGIC "Conversations"	,
# MAGIC "Slack_Conversations_Replies"	,
# MAGIC "incremental load"	,
# MAGIC "1hr",
# MAGIC "1",
# MAGIC "ts",
# MAGIC "Reply_ID,Sourcesystem_ID",
# MAGIC "1511799718.000519"
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##slack_channels

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.MasterMetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC Frequency,
# MAGIC IsActive,
# MAGIC LastLoadDateColumn ,
# MAGIC MergeKey ,
# MAGIC UnixTime 
# MAGIC )
# MAGIC VALUES ("Slack"	,
# MAGIC "SC1"	,
# MAGIC "Channels"	,
# MAGIC "Slack_Channels"	,
# MAGIC "incremental load"	,
# MAGIC "1hr",
# MAGIC "1"	,
# MAGIC "created",
# MAGIC "id,created,Sourcesystem_ID",
# MAGIC "1672574400");
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##slack_users

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.MasterMetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC Frequency,
# MAGIC IsActive
# MAGIC )
# MAGIC VALUES ("Slack"	,
# MAGIC "SU1"	,
# MAGIC "Users"	,
# MAGIC "Slack_Users"	,
# MAGIC "truncate and load"	,
# MAGIC "1hr",
# MAGIC "1"	);

# COMMAND ----------

# MAGIC %md
# MAGIC ##silver_slack_users

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.MasterMetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC Frequency,
# MAGIC IsActive
# MAGIC )
# MAGIC VALUES ("Slack"	,
# MAGIC "SZ21"	,
# MAGIC "Users"	,
# MAGIC "Silver_Slack_Users"	,
# MAGIC "truncate and load"	,
# MAGIC "1hr",
# MAGIC "1"	
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Slack_Conversation_History

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.MasterMetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC Frequency,
# MAGIC IsActive,
# MAGIC Mergekey,
# MAGIC LastLoadDateColumn,
# MAGIC UnixTime
# MAGIC
# MAGIC )
# MAGIC VALUES ("Slack"	,
# MAGIC "SZ22",
# MAGIC "Conversations"	,
# MAGIC "Silver_Slack_Conversations_History"	,
# MAGIC "incremental load"	,
# MAGIC "1hr",
# MAGIC "1"	,
# MAGIC "Conversation_ID,Sourcesystem_ID,DW_Timestamp,blocks,inviter",
# MAGIC "Message_Timestamp",
# MAGIC "1511799718.000519"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##silver_slack_Channels

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.MasterMetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC Frequency,
# MAGIC IsActive
# MAGIC )
# MAGIC VALUES ("Slack"	,
# MAGIC "SZ20"	,
# MAGIC "Channels"	,
# MAGIC "Silver_Slack_Channels"	,
# MAGIC "truncate and load"	,
# MAGIC "1hr",
# MAGIC "1"	
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Slack_Conversations_Replies

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.MasterMetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC Frequency,
# MAGIC IsActive,
# MAGIC Mergekey,
# MAGIC LastLoadDateColumn,
# MAGIC UnixTime
# MAGIC
# MAGIC )
# MAGIC VALUES ("Slack"	,
# MAGIC "SZ23",
# MAGIC "Conversations"	,
# MAGIC "Silver_Slack_Conversations_Replies"	,
# MAGIC "incremental load"	,
# MAGIC "1hr",
# MAGIC "1"	,
# MAGIC "Reply_ID,Sourcesystem_ID,DW_Timestamp,User,Message_Timestamp",
# MAGIC "Message_Timestamp",
# MAGIC "1511799718.000519"
# MAGIC )