# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from Source to Bronze as delta file
# MAGIC * **Created Date:** 17/07/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 24/07/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Importing packages

# COMMAND ----------

# Import packages
from pprint import pprint
import time
import pandas as pd
from pyspark.sql.functions import * 
from pyspark.sql.functions import current_timestamp
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytz
import math
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run 
# MAGIC "//Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC #Error logger variables

# COMMAND ----------

try:
    ErrorLogger = ErrorLogs("Slack_conversation_History_log_public_incremental")
    logger = ErrorLogger[0]
    p_logfile = ErrorLogger[1]
    p_filename = ErrorLogger[2]
except Exception as e:
  print("Unable to create variables",e)
  print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC #Fetching metadata details

# COMMAND ----------

try:
    DF_Metadata= spark.sql(
        "SELECT * FROM Metadata.MasterMetadata where TableID='SCH-N' and IsActive='1' "
    )
except Exception as e:
    print("Unable to fetch metadata details",e)

    print(e)

# COMMAND ----------

try:   
    TableID  =  (DF_Metadata.select(col("TableID")).where(col("TableID") == "SCH-N").collect()[0].TableID)
except Exception as e:
    logger.info("Unable to get table id",e)
    print(e)

# COMMAND ----------

try:
    TableName = (DF_Metadata.select(col("SourceTableName")).where(col("TableID") == TableID).collect()[0].SourceTableName)
    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == TableID).collect()[0].MergeKey) 
    LoadType = (DF_Metadata.select(col("LoadType")).where(col("TableID") == TableID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('UnixTime')).where(col('TableID') == TableID).collect()[0].UnixTime)
except Exception as e:
    logger.info("Unable to fetch metadata details")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #Variables to log error in log table

# COMMAND ----------

try:
    Job_ID = "SJOB_3_Public_incremental"
    Notebook_ID = "SNB_3_Public_incremental"
    Zone = "Bronze"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

try:
    MaxDateQuery = "Select max({0}) as Max_Date from bronze.slack_Conversations_History_New where Conversation_Type  = 'Public' "
    MaxDateQuery = MaxDateQuery.format('ts')
    DF_MaxDate = spark.sql(MaxDateQuery)
    MaxTime = (DF_MaxDate.select(col("Max_Date")).collect()[0].Max_Date)
    print(MaxTime)
except Exception as e:
    logger.info("Unable to fetch max time",e)
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #Connection String

# COMMAND ----------

try:
    tokens = [
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Discovery-token1"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Discovery-token2"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Discovery-token3"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Discovery-token4"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Discovery-token5")
]
except Exception as e:
    logger.info("unable to get tokens")
    print(e)

# COMMAND ----------

try:
    max_time = DF_MaxDate.select(col("Max_Date")).collect()[0]["Max_Date"]
    DF_Query = f"select id, context_team_id from bronze.slack_Channels_new where Conversation_Type = 'Public' and Is_Channel_Deleted = 0 and id not in('CHW++BZ60M9','CJDULBLHL','CF1R473C0','CC8LA3EHG','CAQGRNPCH','C058K2SNB0U','C038Z6XA3PV','C037J5Z6C4F','C036ZGRRNBV','C036J6J74HM','C036K61SMGF','C036VT4AKCM','C036YPAQ1U3','C036ZPX4FM0','C03724P8892','C06DRTB2XT6','C06L1NZKPN1','C06ET3CR0TY','C06NLK8R95K','C06GAH289SP','C06L62G9KSL','C06DFBZ3CKX','C8ZT4LSKX','C8YB67FAL','C6ZAT1YL9','C32R9F88L','C1VBAUP28','C051GV3GQ4V','C04HZQ5GRPX','C04HN5TQDFH','C04BTATFXP1','C048KSN788J','C048GTVL0DB','C04814U5CG2','C03L9N1RF2T','CLLMT4AUW','CLJ38JD3M','CJW7UAQHW','CJ67CFJ5D','CEXRN4FHA','CCJEK2Q2V','C9W4H10J3','C037PH8KJ48','C0373GQC5MX','C037253JZ28','C02P2CU9P98','C02NZ19U729','C02JJD8D69L','C02HJ04CESV','C02GT3HKG1E','C02G90Y3GA1','C02F0B94F8S','C018DEM955X','C013EGRDPT4','C011NDXHSS1') and last_activity_ts > '{max_time}' "
    DF_Query = spark.sql(DF_Query)
    print(DF_Query.count())
except Exception as e:
    logger.info("unable to retrieve channels")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #Retrieveing data from Source

# COMMAND ----------


clients = [WebClient(token=token) for token in tokens]
all_channels = []
Next_Cursor = ""
token_index = 0
client = clients[token_index]
for channel_row in DF_Query.collect():
    channel_id = channel_row.id
    team_id = channel_row.context_team_id

    while True:
        params = {
            'channel': channel_id,
            'team': team_id,  # Use the team_id from the current channel_row
            'limit': 1000,
            'cursor': Next_Cursor,
            'oldest': max_time  # Add cursor for pagination
        }

        try:
            channels_response = client.api_call(
                api_method='discovery.conversations.history',
                http_verb='GET',
                params=params
            )
            print(channel_id)
            if channels_response.get('ok', False):
                # Process the messages here
                messages = channels_response.get('messages', [])
                messages = [message for message in messages if not message.get("bot_id")]
                for message in messages:
                        message["Channel_ID"] = channel_id
                all_channels.extend(messages)
                # Add your message processing logic here

                # Check if there's a next cursor for pagination
                Next_Cursor = channels_response.get('response_metadata', {}).get('next_cursor', '')
                if not Next_Cursor:
                    break  # No more pages, exit the while loop
            else:
                print(f"Error fetching messages for channel {channel_id}: {channels_response['error']}")
                break

        except SlackApiError as e:
                Error_Statement = str(e).replace("'", "''")
                UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Ignorable",1,"Slack-public")
                print(f"Error fetching conversation history for channel {channel_id}: {e}") 
                if e.response['error'] == "channel_not_found":
                    logger.warning(f"Channel {channel_id} not found. Skipping to the next channel.")
                    break
                if e.response.get("headers", {}).get("Retry-After"):
                    # Handle rate limits by waiting and trying again with the same token
                    retry_after = int(e.response["headers"]["Retry-After"])
                    print(f"Rate limited. Retrying after {retry_after} seconds.")
                    time.sleep(0.3)     
                else:
                    # Move to the next token
                    token_index = (token_index + 1) % len(clients)
                    client = clients[token_index]
                    # Retry the request with the same channel and the next token
                    continue
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Slack-public")
print(len(all_channels))

# COMMAND ----------

try:
    schema = StructType([
        StructField("Channel_ID", StringType(), nullable=True),
        StructField("user", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("ts", StringType(), nullable=True),
        StructField("client_msg_id", StringType(), nullable=True),
        StructField("text", StringType(), nullable=True),
        StructField("team", StringType(), nullable=True),
        StructField("attachments", StringType(), nullable=True),
        StructField("thread_ts", StringType(), nullable=True),
        StructField("parent_user_id", StringType(), nullable=True),
        StructField("blocks", StringType(), nullable=True),
        StructField("upload", StringType(), nullable=True),
        StructField("edited", StringType(), nullable=True),
        StructField("files", StringType(), nullable=True),
        StructField("reply_count", StringType(), nullable=True),
        StructField("reply_users_count", StringType(), nullable=True),
        StructField("latest_reply", StringType(), nullable=True),
        StructField("reply_users", StringType(), nullable=True),
        StructField("replies", StringType(), nullable=True),
        StructField("user_team", StringType(), nullable=True),
        StructField("source_team", StringType(), nullable=True),
        StructField("user_profile", StringType(), nullable=True),
        StructField("is_locked", StringType(), nullable=True)
    ])
except Exception as e:
    logger.ingo('Unable to parse the columns')
    print(e)
DF_Messages = spark.createDataFrame(all_channels, schema)
DF_Messages.count()

# COMMAND ----------

try:
    DF_Messages = DF_Messages.withColumn("Sourcesystem_Name", lit("Slack"))
    DF_Messages = DF_Messages.withColumn("DW_Timestamp", to_timestamp(DF_Messages.ts.cast("double")))
    DF_Messages = DF_Messages.withColumn("Created_By",lit("Databricks")) 
    DF_Messages = DF_Messages.withColumn("Created_Date", current_timestamp()) 
    DF_Messages = DF_Messages.withColumn("Last_Modified_By", lit("Databricks"))  
    DF_Messages = DF_Messages.withColumn("Last_Modified_Date", current_timestamp()) 
    Hash_Key = ["user", 
                "type",  
                "edited",
                "client_msg_id",
                "text",
                "team", 
                "thread_ts",
                "reply_count",
                "reply_users_count",
                "latest_reply",
                "reply_users",
                "is_locked", 
                "blocks",
                "attachments",
                "parent_user_id",
                "upload",
                "files",
                "replies", 
                "user_team",
                "source_team",
                "user_profile"]
    DF_Messages = DF_Messages.withColumn("HashKey",md5(concat_ws("",*Hash_Key)))
    Merge_Key = ["Channel_ID","ts"]
    DF_Messages = DF_Messages.withColumn("MergeKey",md5(concat_ws("",*Merge_Key)))
    DF_Messages = DF_Messages.withColumn("Conversation_Type",lit("None"))

except Exception as e:
    logger.info("Unable to create Newcolumns for DF_Messages",e)

# COMMAND ----------

try: 
    DF_Channels=spark.sql(''' select id,Conversation_type from bronze.slack_channels_new''')
    DF_Channels.createOrReplaceTempView("VW_Channels")
except Exception as e:
    logger.info("Unable to create view",e)
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #Merge Data into Target Table

# COMMAND ----------

DF_Messages.createOrReplaceTempView('VW_Conversation')
            
            # Count the number of rows
Rowcount = DF_Messages.count()
            
            # Define the merge query
try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            print('Loading ' + TableName)
            
            # Define the merge query
            MergeQuery = '''
                MERGE INTO bronze.Slack_Conversations_History_New AS Target
                USING VW_Conversation AS Source
                ON Target.MergeKey = Source.MergeKey
                WHEN MATCHED AND Target.HashKey != Source.HashKey
                THEN UPDATE SET
                    Target.Channel_ID = Source.Channel_ID,
                    Target.user = Source.user,
                    Target.type = Source.type,
                    Target.ts = Source.ts,
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
            dg=spark.sql('''MERGE INTO bronze.Slack_Conversations_History_New AS OT USING VW_Channels AS OS 
              ON OT.channel_ID = OS.id WHEN MATCHED THEN UPDATE SET 
              OT.conversation_type = OS.conversation_type''')                  
            # Execute the merge query
            
            

            logger.info('Successfully loaded the ' + TableName + ' to bronze')
            print('Loaded ' + TableName)

            # Find the maximum date
            MaxDateQuery = "SELECT MAX({0}) AS Max_Date FROM VW_Conversation".format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID, DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID, 'Bronze')
        except Exception as e:
            Error_Statement = str(e).replace("'", "''")
            UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Merge-Target Table")
            logger.info('Failed for Bronze load')
            UpdateFailedStatus(TableID, 'Bronze')
            logger.info('Updated the Metadata for Failed Status ' + TableName)
            print('Unable to load ' + TableName)
            print(e)
        UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Merge-Target Table")
except Exception as e:
    logger.info("Table is already loaded")
    print(e)




# COMMAND ----------

