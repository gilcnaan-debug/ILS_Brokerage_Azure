# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from Source to Bronze as delta file
# MAGIC * **Created Date:** 19/09/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 19/09/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Slack_Temp_DM4

# COMMAND ----------

DF_TEMP4 = spark.sql("select * from bronze.slack_temp_dm4")
DF_TEMP4.createOrReplaceTempView("Vw_Temp4")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ##Merge Into Table

# COMMAND ----------


try:
    MergeQuery = '''
        MERGE INTO bronze.Slack_Conversations_History_DM AS Target
        USING Vw_Temp4 AS Source
        ON Target.MergeKey = Source.MergeKey
        WHEN MATCHED AND Target.HashKey != Source.HashKey
        THEN UPDATE SET
            Target.Channel_ID = Source.Channel_ID,
            Target.user = Source.user,
            Target.type = Source.type,
            Target.client_msg_id = Source.client_msg_id,
            Target.text = Source.text,
            Target.team = Source.team,
            Target.attachments = Source.attachments,
            Target.thread_ts = Source.thread_ts,
            Target.parent_user_id = Source.parent_user_id,
            Target.blocks = Source.blocks,
            Target.upload = Source.upload,
            Target.edited = Source.edited,
            Target.files = Source.files,
            Target.reply_count = Source.reply_count,
            Target.reply_users_count = Source.reply_users_count,
            Target.latest_reply = Source.latest_reply,
            Target.reply_users = Source.reply_users,
            Target.replies = Source.replies,
            Target.user_team = Source.user_team,
            Target.source_team = Source.source_team,
            Target.user_profile = Source.user_profile,
            Target.is_locked = Source.is_locked,
            Target.reactions = Source.reactions,
            Target.HashKey = Source.HashKey,
            Target.MergeKey = Source.MergeKey,
            Target.Conversation_Type = Source.Conversation_Type,
            Target.DW_Timestamp = Source.DW_Timestamp,
            Target.Sourcesystem_Name = Source.Sourcesystem_Name,
            Target.Created_By = Source.Created_By,
            Target.Created_Date = Source.Created_Date,
            Target.Last_Modified_By = Source.Last_Modified_By,
            Target.Last_Modified_Date = Source.Last_Modified_Date
        WHEN NOT MATCHED THEN
        INSERT (
            Channel_ID,
            user,
            type,
            ts,
            client_msg_id,
            text,
            team,
            attachments,
            thread_ts,
            parent_user_id,
            blocks,
            upload,
            edited,
            files,
            reply_count,
            reply_users_count,
            latest_reply,
            reply_users,
            replies,
            user_team,
            source_team,
            user_profile,
            is_locked,
            reactions,
            HashKey,
            MergeKey,
            Conversation_Type,
            DW_Timestamp,
            Sourcesystem_Name,
            Created_By,
            Created_Date,
            Last_Modified_By,
            Last_Modified_Date
        ) VALUES (
            Source.Channel_ID,
            Source.user,
            Source.type,
            Source.ts,
            Source.client_msg_id,
            Source.text,
            Source.team,
            Source.attachments,
            Source.thread_ts,
            Source.parent_user_id,
            Source.blocks,
            Source.upload,
            Source.edited,
            Source.files,
            Source.reply_count,
            Source.reply_users_count,
            Source.latest_reply,
            Source.reply_users,
            Source.replies,
            Source.user_team,
            Source.source_team,
            Source.user_profile,
            Source.is_locked,
            Source.reactions,
            Source.HashKey,
            Source.MergeKey,
            Source.Conversation_Type,
            Source.DW_Timestamp,
            Source.Sourcesystem_Name,
            Source.Created_By,
            Source.Created_Date,
            Source.Last_Modified_By,
            Source.Last_Modified_Date
        )
    '''

    spark.sql(MergeQuery)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Slack_Temp_DM5

# COMMAND ----------

DF_TEMP5 = spark.sql("select * from bronze.slack_temp_dm5")
DF_TEMP5.createOrReplaceTempView("Vw_Temp5")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ##Merge Into Table

# COMMAND ----------


try:
    MergeQuery = '''
        MERGE INTO bronze.Slack_Conversations_History_DM AS Target
        USING Vw_Temp5 AS Source
        ON Target.MergeKey = Source.MergeKey
        WHEN MATCHED AND Target.HashKey != Source.HashKey
        THEN UPDATE SET
            Target.Channel_ID = Source.Channel_ID,
            Target.user = Source.user,
            Target.type = Source.type,
            Target.client_msg_id = Source.client_msg_id,
            Target.text = Source.text,
            Target.team = Source.team,
            Target.attachments = Source.attachments,
            Target.thread_ts = Source.thread_ts,
            Target.parent_user_id = Source.parent_user_id,
            Target.blocks = Source.blocks,
            Target.upload = Source.upload,
            Target.edited = Source.edited,
            Target.files = Source.files,
            Target.reply_count = Source.reply_count,
            Target.reply_users_count = Source.reply_users_count,
            Target.latest_reply = Source.latest_reply,
            Target.reply_users = Source.reply_users,
            Target.replies = Source.replies,
            Target.user_team = Source.user_team,
            Target.source_team = Source.source_team,
            Target.user_profile = Source.user_profile,
            Target.is_locked = Source.is_locked,
            Target.reactions = Source.reactions,
            Target.HashKey = Source.HashKey,
            Target.MergeKey = Source.MergeKey,
            Target.Conversation_Type = Source.Conversation_Type,
            Target.DW_Timestamp = Source.DW_Timestamp,
            Target.Sourcesystem_Name = Source.Sourcesystem_Name,
            Target.Created_By = Source.Created_By,
            Target.Created_Date = Source.Created_Date,
            Target.Last_Modified_By = Source.Last_Modified_By,
            Target.Last_Modified_Date = Source.Last_Modified_Date
        WHEN NOT MATCHED THEN
        INSERT (
            Channel_ID,
            user,
            type,
            ts,
            client_msg_id,
            text,
            team,
            attachments,
            thread_ts,
            parent_user_id,
            blocks,
            upload,
            edited,
            files,
            reply_count,
            reply_users_count,
            latest_reply,
            reply_users,
            replies,
            user_team,
            source_team,
            user_profile,
            is_locked,
            reactions,
            HashKey,
            MergeKey,
            Conversation_Type,
            DW_Timestamp,
            Sourcesystem_Name,
            Created_By,
            Created_Date,
            Last_Modified_By,
            Last_Modified_Date
        ) VALUES (
            Source.Channel_ID,
            Source.user,
            Source.type,
            Source.ts,
            Source.client_msg_id,
            Source.text,
            Source.team,
            Source.attachments,
            Source.thread_ts,
            Source.parent_user_id,
            Source.blocks,
            Source.upload,
            Source.edited,
            Source.files,
            Source.reply_count,
            Source.reply_users_count,
            Source.latest_reply,
            Source.reply_users,
            Source.replies,
            Source.user_team,
            Source.source_team,
            Source.user_profile,
            Source.is_locked,
            Source.reactions,
            Source.HashKey,
            Source.MergeKey,
            Source.Conversation_Type,
            Source.DW_Timestamp,
            Source.Sourcesystem_Name,
            Source.Created_By,
            Source.Created_Date,
            Source.Last_Modified_By,
            Source.Last_Modified_Date
        )
    '''

    spark.sql(MergeQuery)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Slck_Export_Temp_DM6

# COMMAND ----------

DF_TEMP6 = spark.sql("select * from bronze.slack_temp_dm6")
DF_TEMP6.createOrReplaceTempView("Vw_Temp6")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge Into Table

# COMMAND ----------


try:
    MergeQuery = '''
        MERGE INTO bronze.Slack_Conversations_History_DM AS Target
        USING Vw_Temp6 AS Source
        ON Target.MergeKey = Source.MergeKey
        WHEN MATCHED AND Target.HashKey != Source.HashKey
        THEN UPDATE SET
            Target.Channel_ID = Source.Channel_ID,
            Target.user = Source.user,
            Target.type = Source.type,
            Target.client_msg_id = Source.client_msg_id,
            Target.text = Source.text,
            Target.team = Source.team,
            Target.attachments = Source.attachments,
            Target.thread_ts = Source.thread_ts,
            Target.parent_user_id = Source.parent_user_id,
            Target.blocks = Source.blocks,
            Target.upload = Source.upload,
            Target.edited = Source.edited,
            Target.files = Source.files,
            Target.reply_count = Source.reply_count,
            Target.reply_users_count = Source.reply_users_count,
            Target.latest_reply = Source.latest_reply,
            Target.reply_users = Source.reply_users,
            Target.replies = Source.replies,
            Target.user_team = Source.user_team,
            Target.source_team = Source.source_team,
            Target.user_profile = Source.user_profile,
            Target.is_locked = Source.is_locked,
            Target.reactions = Source.reactions,
            Target.HashKey = Source.HashKey,
            Target.MergeKey = Source.MergeKey,
            Target.Conversation_Type = Source.Conversation_Type,
            Target.DW_Timestamp = Source.DW_Timestamp,
            Target.Sourcesystem_Name = Source.Sourcesystem_Name,
            Target.Created_By = Source.Created_By,
            Target.Created_Date = Source.Created_Date,
            Target.Last_Modified_By = Source.Last_Modified_By,
            Target.Last_Modified_Date = Source.Last_Modified_Date
        WHEN NOT MATCHED THEN
        INSERT (
            Channel_ID,
            user,
            type,
            ts,
            client_msg_id,
            text,
            team,
            attachments,
            thread_ts,
            parent_user_id,
            blocks,
            upload,
            edited,
            files,
            reply_count,
            reply_users_count,
            latest_reply,
            reply_users,
            replies,
            user_team,
            source_team,
            user_profile,
            is_locked,
            reactions,
            HashKey,
            MergeKey,
            Conversation_Type,
            DW_Timestamp,
            Sourcesystem_Name,
            Created_By,
            Created_Date,
            Last_Modified_By,
            Last_Modified_Date
        ) VALUES (
            Source.Channel_ID,
            Source.user,
            Source.type,
            Source.ts,
            Source.client_msg_id,
            Source.text,
            Source.team,
            Source.attachments,
            Source.thread_ts,
            Source.parent_user_id,
            Source.blocks,
            Source.upload,
            Source.edited,
            Source.files,
            Source.reply_count,
            Source.reply_users_count,
            Source.latest_reply,
            Source.reply_users,
            Source.replies,
            Source.user_team,
            Source.source_team,
            Source.user_profile,
            Source.is_locked,
            Source.reactions,
            Source.HashKey,
            Source.MergeKey,
            Source.Conversation_Type,
            Source.DW_Timestamp,
            Source.Sourcesystem_Name,
            Source.Created_By,
            Source.Created_Date,
            Source.Last_Modified_By,
            Source.Last_Modified_Date
        )
    '''

    spark.sql(MergeQuery)
except Exception as e:
    print(e)

# COMMAND ----------

