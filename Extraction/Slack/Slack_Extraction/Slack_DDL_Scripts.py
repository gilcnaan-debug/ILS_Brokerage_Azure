# Databricks notebook source
## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Slack_Channels

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table bronze.Slack_Channels(
# MAGIC DW_Channel_ID BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
# MAGIC id string  ,
# MAGIC name   string,
# MAGIC Is_Deleted String,
# MAGIC is_channel   string,
# MAGIC is_group   string,
# MAGIC is_im   string,
# MAGIC is_mpim   string,
# MAGIC is_private   string,
# MAGIC created   string,
# MAGIC is_archived   string,
# MAGIC is_general   string,
# MAGIC unlinked   string,
# MAGIC name_normalized   string,
# MAGIC is_shared   string,
# MAGIC is_org_shared   string,
# MAGIC is_pending_ext_shared   string,
# MAGIC pending_shared   string,
# MAGIC context_team_id   string,
# MAGIC updated   string,
# MAGIC parent_conversation   string,
# MAGIC creator   string,
# MAGIC is_moved   string,
# MAGIC is_ext_shared   string,
# MAGIC enterprise_id string,
# MAGIC is_global_shared string,
# MAGIC is_org_default string,
# MAGIC is_org_mandatory string,
# MAGIC shared_team_ids   string,
# MAGIC internal_team_ids   string,
# MAGIC pending_connected_team_ids   string,
# MAGIC is_member   string,
# MAGIC topic   string,
# MAGIC purpose   string,
# MAGIC properties string,
# MAGIC previous_names   string,
# MAGIC num_members   string,
# MAGIC Sourcesystem_ID INT,
# MAGIC Sourcesystem_Name string,
# MAGIC DW_Timestamp TIMESTAMP_NTZ,
# MAGIC Created_By	String,
# MAGIC Created_Date	TIMESTAMP_NTZ,
# MAGIC Last_Modified_By	string,
# MAGIC Last_Modified_Date	TIMESTAMP_NTZ,
# MAGIC Partition string,
# MAGIC CONSTRAINT PK_Channels PRIMARY KEY (DW_Channel_ID)
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Slack_Users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.Slack_Users (
# MAGIC DW_User_ID BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
# MAGIC id   String ,
# MAGIC team_id   String,
# MAGIC name   String,
# MAGIC deleted   String,
# MAGIC color   String,
# MAGIC real_name   String,
# MAGIC tz   String,
# MAGIC tz_label   String,
# MAGIC tz_offset   String,
# MAGIC profile   String,
# MAGIC is_admin   String,
# MAGIC is_owner   String,
# MAGIC is_primary_owner   String,
# MAGIC is_restricted   String,
# MAGIC is_ultra_restricted   String,
# MAGIC is_bot   String,
# MAGIC is_app_user   String,
# MAGIC updated   String,
# MAGIC is_email_confirmed   String,
# MAGIC who_can_share_contact_card   String,
# MAGIC Sourcesystem_ID   Integer,
# MAGIC Sourcesystem_Name   String,
# MAGIC DW_Timestamp TIMESTAMP_NTZ,
# MAGIC Created_By   String,
# MAGIC Created_Date   TIMESTAMP_NTZ,
# MAGIC Last_Modified_By   string,
# MAGIC Last_Modified_Date   TIMESTAMP_NTZ,
# MAGIC CONSTRAINT PK_Users PRIMARY KEY (DW_User_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Slack_Conversations_History

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.Slack_Conversations_History (
# MAGIC     DW_Conversations_History_ID BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
# MAGIC     Hashkey String,
# MAGIC     Mergekey string,
# MAGIC     Conversation_ID string,
# MAGIC     ChannelID String,
# MAGIC     Conversation_Type String,
# MAGIC     User string,
# MAGIC     type string,
# MAGIC     ts string,
# MAGIC     edited string,
# MAGIC     client_msg_id string,
# MAGIC     text string,
# MAGIC     team string,
# MAGIC     thread_ts string,
# MAGIC     reply_count string,
# MAGIC     reply_users_count string,
# MAGIC     latest_reply string,
# MAGIC     reply_users string,
# MAGIC     is_locked string,
# MAGIC     subscribed string,
# MAGIC     blocks string,
# MAGIC     reactions string,
# MAGIC     subtype string,
# MAGIC     inviter string,
# MAGIC     DW_Timestamp Timestamp_ntz,
# MAGIC     Sourcesystem_ID INT,
# MAGIC     Sourcesystem_Name string,
# MAGIC     Created_By String,
# MAGIC     Created_Date TIMESTAMP_NTZ,
# MAGIC     Last_Modified_By string,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ,
# MAGIC     Partition INTEGER,
# MAGIC     CONSTRAINT Pkey_History PRIMARY KEY (DW_Conversations_History_ID)
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Slack_Conversations_Replies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recreate the table with the desired starting value for the identity column
# MAGIC CREATE OR REPLACE TABLE bronze.Slack_Conversations_Replies (
# MAGIC     DW_Conversations_Reply_ID BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
# MAGIC     Hashkey String,
# MAGIC     Mergekey String,
# MAGIC     Reply_ID string,
# MAGIC     ChannelID String,
# MAGIC     Conversation_Type String,
# MAGIC     User string,
# MAGIC     type string,
# MAGIC     ts string,
# MAGIC     client_msg_id string,
# MAGIC     text string,
# MAGIC     team string,
# MAGIC     thread_ts string,
# MAGIC     reply_count string,
# MAGIC     reply_users_count string,
# MAGIC     latest_reply string,
# MAGIC     reply_users string,
# MAGIC     is_locked string,
# MAGIC     subscribed string,
# MAGIC     blocks string,
# MAGIC     parent_user_id string,
# MAGIC     reactions string,
# MAGIC     DW_Timestamp Timestamp_ntz,
# MAGIC     Sourcesystem_ID INT,
# MAGIC     Sourcesystem_Name string,
# MAGIC     Created_By String,
# MAGIC     Created_Date TIMESTAMP_NTZ,
# MAGIC     Last_Modified_By string,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ,
# MAGIC     CONSTRAINT Pkey_Replies PRIMARY KEY (Reply_ID, Sourcesystem_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver DDL

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Slack_Channels

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Slack_Channels(
# MAGIC DW_Channel_ID BIGINT,
# MAGIC Channel_ID  string NOT NULL,
# MAGIC Channel_Name   string,
# MAGIC Is_Deleted String,
# MAGIC Is_Channel   string,
# MAGIC Is_Group   string,
# MAGIC Is_IM   string,
# MAGIC Is_MPIM   string,
# MAGIC Is_Private   string,
# MAGIC Channel_Created_Date   string,
# MAGIC Is_Archived   string,
# MAGIC Is_General   string,
# MAGIC Unlinked   string,
# MAGIC Normalized_Name   string,
# MAGIC Is_shared   string,
# MAGIC Is_Org_Shared   string,
# MAGIC Is_Pending_ext_shared   string,
# MAGIC Pending_Shared   string,
# MAGIC Context_Team_ID   string,
# MAGIC Updated_Date   string,
# MAGIC Parent_Conversation   string,
# MAGIC Creator_Name   string,
# MAGIC Is_Moved   String,
# MAGIC Is_Ext_Shared   string,
# MAGIC Enterprise_ID string,
# MAGIC Is_Global_Shared string,
# MAGIC Is_Org_Default string,
# MAGIC Is_Org_Mandatory string,
# MAGIC Shared_Team_IDs   string,
# MAGIC Internal_Team_IDs   string,
# MAGIC Pending_Connected_Team_IDs   string,
# MAGIC Is_Member   string,
# MAGIC Topic   string,
# MAGIC Purpose   string,
# MAGIC Properties string,
# MAGIC Previous_Names   string,
# MAGIC Members_Count   string,
# MAGIC Sourcesystem_ID   Integer,
# MAGIC Sourcesystem_Name   String,
# MAGIC DW_Timestamp TIMESTAMP_NTZ,
# MAGIC Created_By   String,
# MAGIC Created_Date   Timestamp_NTZ,
# MAGIC Last_Modified_By   string,
# MAGIC Last_Modified_Date   TIMESTAMP_NTZ,
# MAGIC Partition   String,
# MAGIC CONSTRAINT PKey_Channels PRIMARY KEY (Channel_ID,Sourcesystem_ID)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Slack_Users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  silver.Silver_Slack_Users(
# MAGIC DW_User_ID BIGINT,
# MAGIC User_ID   string NOT NULL,
# MAGIC Team_ID   string,
# MAGIC User_Name   string,
# MAGIC Is_Deleted   string,
# MAGIC Color   string,
# MAGIC User_Real_Name   string,
# MAGIC User_TimeZone   string,
# MAGIC TimeZone_Label   string,
# MAGIC TimeZone_Offset   string,
# MAGIC Profile   string,
# MAGIC Is_Admin   string,
# MAGIC Is_Owner   string,
# MAGIC Is_Primary_Owner   string,
# MAGIC Is_Restricted   string,
# MAGIC Is_Ultra_Restricted   string,
# MAGIC Is_Bot   string,
# MAGIC Is_App_User   string,
# MAGIC Updated_Date   string,
# MAGIC Is_Email_Confirmed   string,
# MAGIC Who_Can_Share_Contact_Card   string,
# MAGIC Sourcesystem_ID   Integer,
# MAGIC Sourcesystem_Name   String,
# MAGIC DW_Timestamp TIMESTAMP_NTZ,
# MAGIC Created_By   String,
# MAGIC Created_Date   TIMESTAMP_NTZ,
# MAGIC Last_Modified_By   string,
# MAGIC Last_Modified_Date   TIMESTAMP_NTZ,
# MAGIC CONSTRAINT PKey_Users PRIMARY KEY (User_ID,Sourcesystem_ID)
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Slack_Conversations_History

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or Replace TABLE silver.Silver_Slack_Conversations_History(
# MAGIC DW_Conversations_History_ID BIGINT,
# MAGIC Hashkey String,
# MAGIC Conversation_ID	string,
# MAGIC Channel_ID String,
# MAGIC Conversation_Type String,
# MAGIC User_ID	string,
# MAGIC Text_Type	string,
# MAGIC Message_Timestamp	string,
# MAGIC Edited string,
# MAGIC Client_Msg_ID	string,
# MAGIC Actual_Text	string,
# MAGIC Team_ID	string,
# MAGIC Thread_Msg_Timestamp	string,
# MAGIC Reply_Count	string,
# MAGIC Replied_Users_Count	string,
# MAGIC Latest_Reply_Timestamp	string,
# MAGIC Replied_Users_ID	string,
# MAGIC Is_Locked	string,
# MAGIC Is_Subscribed	string,
# MAGIC Blocks	String,
# MAGIC Reactions String,
# MAGIC Sub_Type	string,
# MAGIC Inviter	string,
# MAGIC Is_Emoji	SMALLINT,
# MAGIC Is_Reaction SMALLINT,
# MAGIC Sourcesystem_ID	Integer,
# MAGIC Sourcesystem_Name	String,
# MAGIC DW_Timestamp TIMESTAMP_NTZ,
# MAGIC Created_By	String,
# MAGIC Created_Date	TIMESTAMP_NTZ,
# MAGIC Last_Modified_By	string,
# MAGIC Last_Modified_Date	TIMESTAMP_NTZ,
# MAGIC CONSTRAINT PKey_Silver_History PRIMARY KEY (Conversation_ID,Sourcesystem_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Slack_Conversations_Replies

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Slack_Conversations_Replies(
# MAGIC DW_Conversations_Reply_ID BIGINT ,
# MAGIC Hashkey string,
# MAGIC Reply_ID	string ,
# MAGIC Channel_ID STRING,
# MAGIC Conversation_Type String,
# MAGIC User_ID	string,
# MAGIC Text_Type	string,
# MAGIC Message_Timestamp	string,
# MAGIC Client_Msg_ID	string,
# MAGIC Actual_Text	string,
# MAGIC Team_ID	string,
# MAGIC Thread_Msg_Timestamp	string,
# MAGIC Reply_Count	string,
# MAGIC Replied_Users_Count	string,
# MAGIC Latest_Reply_Timestamp	string,
# MAGIC Replied_Users_ID	string,
# MAGIC Is_Locked	Boolean,
# MAGIC Is_Subscribed	string,
# MAGIC Blocks	string,
# MAGIC Parent_User_ID	string,
# MAGIC Reactions String,
# MAGIC DW_Timestamp TIMESTAMP_NTZ,
# MAGIC Is_Reaction SMALLINT,
# MAGIC Sourcesystem_ID	Integer,
# MAGIC Sourcesystem_Name	String,
# MAGIC Created_By	String,
# MAGIC Created_Date	Timestamp_NTZ,
# MAGIC Last_Modified_By	string,
# MAGIC Last_Modified_Date	TIMESTAMP_NTZ,
# MAGIC CONSTRAINT PKey_Slack_Replies PRIMARY KEY (Reply_ID,Sourcesystem_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Log Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metadata.Error_Log_Table(
# MAGIC DW_Log_ID BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
# MAGIC Job_ID String,
# MAGIC Notebook_ID String,
# MAGIC Table_ID String,
# MAGIC Table_Name String,
# MAGIC Zone String,
# MAGIC Run_Status String,
# MAGIC Error_Statement String,
# MAGIC Error_Type String,
# MAGIC Is_Failed SMALLINT,
# MAGIC Instance String,
# MAGIC Created_At TIMESTAMP_NTZ,
# MAGIC CONSTRAINT PKey_Error_Logs PRIMARY KEY (DW_Log_ID)
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

