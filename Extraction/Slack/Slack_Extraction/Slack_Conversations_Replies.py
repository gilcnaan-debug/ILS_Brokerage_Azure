# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from Source to Bronze as delta file
# MAGIC * **Created Date:** 22/05/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 07/06/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Required Packages

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
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import pickle


# COMMAND ----------

# MAGIC %md
# MAGIC ####Initializing Utilities

# COMMAND ----------

# MAGIC %run 
# MAGIC "//Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initializing Logger

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("Slack_conversation1_reply_Extraction_1hr_Refresh")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Use catalog

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Metadata detials

# COMMAND ----------

DF_Metadata= spark.sql(
    "SELECT * FROM Metadata.MasterMetadata where TableID='SCR-1' and IsActive='1' "
)

# COMMAND ----------

TableID  =  (DF_Metadata.select(col("TableID")).where(col("TableID") == "SCR-1").collect()[0].TableID)


# COMMAND ----------

try:
    TableName = (DF_Metadata.select(col("SourceTableName")).where(col("TableID") == TableID).collect()[0].SourceTableName)
    ErrorPath = (DF_Metadata.select(col("ErrorLogPath")).where(col("TableID") == TableID).collect()[0].ErrorLogPath)
    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == TableID).collect()[0].MergeKey) 
    LoadType = (DF_Metadata.select(col("LoadType")).where(col("TableID") == TableID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('UnixTime')).where(col('TableID') == TableID).collect()[0].UnixTime)
except Exception as e:
    logger.info("Unable to fetch metadata details")
    print(e)

# COMMAND ----------

try:
    Job_ID = "JOB_7"
    Notebook_ID = "NB_7"
    Zone = "Bronze"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

MaxDateQuery = "Select max({0}) as Max_Date from bronze.slack_conversations_replies where sourcesystem_id=6 and Conversation_Type='Public' "
MaxDateQuery = MaxDateQuery.format('latest_reply')
DF_MaxDate = spark.sql(MaxDateQuery)
MaxTime = (DF_MaxDate.select(col("Max_Date")).collect()[0].Max_Date)
print(MaxTime)

# COMMAND ----------

# MAGIC %md
# MAGIC ##create connection string for Brokerage Instance

# COMMAND ----------

try:
    # Define your Slack tokens
    tokens = [
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Brokerage-Token1"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Brokerage-Token2"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Brokerage-Token3"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Brokerage-Token4"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Brokerage-Token5"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Brokerage-Token6"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Brokerage-Token7")
]

# Create Slack clients for each token
    clients = [WebClient(token=token) for token in tokens]

# Initialize all_channels outside the loop
    all_channels = []

# Loop through Partition_IDs
    max_time = DF_MaxDate.select(col("Max_Date")).collect()[0]["Max_Date"]
    query = 'SELECT ChannelID, ts  FROM bronze.slack_conversations_history WHERE sourcesystem_id = 6 AND reply_count > "0" AND latest_reply > ' + max_time
    DF_Query = spark.sql(query)
    print(max_time)
# Initialize token index
    token_index = 0

# Keep track of successfully processed channels
    # processed_channels = set()

# Loop through channel IDs
    for channel_row in DF_Query.collect():
        channel_id = channel_row.ChannelID
        ts=channel_row.ts

    # Check if the channel has already been successfully processed
    # if (channel_id,ts) in processed_channels:
    #     continue

    # Get the current token
        client = clients[token_index]
        next_cursor = ""
    
        while True:
            try:
                # Fetch conversation history for the current channel ID
                channels_response = client.conversations_replies(channel=channel_id,ts=ts, cursor=next_cursor, limit=999, oldest=max_time,exclude_bots=True)
                print(channel_id)
                if channels_response["ok"]:
                    messages = channels_response["messages"]
                    # Filter out bot messages
                    messages = [message for message in messages if not message.get("bot_id")]
                    for message in messages:
                        message["ChannelID"] = channel_id
                    all_channels.extend(messages)
                    # Update next_cursor
                    next_cursor = channels_response.get("response_metadata", {}).get("next_cursor", "")
                    
                    # Check if there is more data to fetch
                    if not next_cursor:
                        # Add the channel to the set of processed channels
                        # processed_channels.add((channel_id,ts))
                        break
                else:
                    print(f"Error fetching conversation history for channel {channel_id}: {channels_response['error']}")
                    break
                
            except SlackApiError as e:
                Error_Statement = str(e).replace("'", "''")
                UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Ignorable",1,"Slack-Bokerage")
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
            UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Slack-Brokerage")
except Exception as e:
    logger.info("Unable to get Messages")


# COMMAND ----------

schema = StructType([
    StructField("ChannelID", StringType(), nullable=True),
    StructField("user", StringType(), nullable=True),
    StructField("type", StringType(), nullable=True),
    StructField("ts", StringType(), nullable=True),
    StructField("edited", StringType(), nullable=True),
    StructField("client_msg_id", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("team", StringType(), nullable=True),
    StructField("thread_ts", StringType(), nullable=True),
    StructField("reply_count", StringType(), nullable=True),
    StructField("reply_users_count", StringType(), nullable=True),
    StructField("latest_reply", StringType(), nullable=True),
    StructField("reply_users", StringType(), nullable=True),
    StructField("is_locked", StringType(), nullable=True),
    StructField("subscribed", StringType(), nullable=True),
    StructField("blocks", ArrayType(StringType()), nullable=True),
    StructField("parent_user_id", StringType(), nullable=True),
    StructField("reactions", StringType(), nullable=True),
])

# Create DataFrame from all_channels list
DF_Brokerage_Messages = spark.createDataFrame(all_channels, schema)
DF_Brokerage_Messages.count()

# COMMAND ----------

DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("concatenated_columns", concat_ws(
    '',
    col("ts"),
    col("thread_ts"),
    col("text"),
    col("client_msg_id"),
    col("user")
))
DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("Reply_ID", md5("concatenated_columns"))
DF_Brokerage_Messages = DF_Brokerage_Messages.drop("concatenated_columns")
DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("DW_Timestamp", to_timestamp(DF_Brokerage_Messages.ts.cast("double")))
DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("Sourcesystem_ID", lit(6))
DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("Conversation_Type", lit("Public"))
DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("Sourcesystem_Name", lit("Slack-Brokerage"))
DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("Created_By", lit("Databricks"))
DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("Created_Date", current_timestamp())
DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("Last_Modified_By", lit("Databricks"))
DF_Brokerage_Messages = DF_Brokerage_Messages.withColumn("Last_Modified_Date", current_timestamp())

# COMMAND ----------

MaxDateQuery = "Select max({0}) as Max_Date from bronze.slack_conversations_replies where sourcesystem_id=7 and Conversation_Type='Public' "
MaxDateQuery = MaxDateQuery.format('latest_reply')
DF_MaxDate = spark.sql(MaxDateQuery)
MaxTime = (DF_MaxDate.select(col("Max_Date")).collect()[0].Max_Date)
print(MaxTime)

# COMMAND ----------

try:
    # Define your Slack tokens
    tokens = [
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="ILS-Token1"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="ILS-Token2"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="ILS-Token03"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="ILS-Token4"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="ILS-Token5"),
    dbutils.secrets.get(scope="NFI_Slack_Secrets", key="ILS-Token6")
]

# Create Slack clients for each token
    clients = [WebClient(token=token) for token in tokens]

# Initialize all_channels outside the loop
    all_channels = []

# Loop through Partition_IDs
    max_time = DF_MaxDate.select(col("Max_Date")).collect()[0]["Max_Date"]
    query = 'SELECT ChannelID, ts  FROM bronze.slack_conversations_history WHERE sourcesystem_id = 7 AND reply_count > "0" AND latest_reply > ' + max_time
    DF_Query = spark.sql(query)
    print(max_time)
# Initialize token index
    token_index = 0

# Keep track of successfully processed channels
    # processed_channels = set()

# Loop through channel IDs
    for channel_row in DF_Query.collect():
        channel_id = channel_row.ChannelID
        ts=channel_row.ts

    # Check if the channel has already been successfully processed
        # if (channel_id,ts) in processed_channels:
        #     continue

    # Get the current token
        client = clients[token_index]
        next_cursor = ""
    
        while True:
            try:
                # Fetch conversation history for the current channel ID
                channels_response = client.conversations_replies(channel=channel_id,ts=ts,oldest=max_time, cursor=next_cursor, limit=999,exclude_bots=True)
                print(channel_id)
                if channels_response["ok"]:
                    messages = channels_response["messages"]
                    # Filter out bot messages
                    messages = [message for message in messages if not message.get("bot_id")]
                    for message in messages:
                        message["ChannelID"] = channel_id
                    all_channels.extend(messages)
                    # Update next_cursor
                    next_cursor = channels_response.get("response_metadata", {}).get("next_cursor", "")
                    
                    # Check if there is more data to fetch
                    if not next_cursor:
                        # Add the channel to the set of processed channels
                        # processed_channels.add((channel_id,ts))
                        break
                else:
                    print(f"Error fetching conversation history for channel {channel_id}: {channels_response['error']}")
                    break
                
            except SlackApiError as e:
                Error_Statement = str(e).replace("'", "''")
                UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Ignorable",1,"Slack-ILS")
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
            UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Slack-ILS")
except Exception as e:
    logger.info("Unable to get Messages")


# COMMAND ----------

schema = StructType([
    StructField("ChannelID", StringType(), nullable=True),
    StructField("user", StringType(), nullable=True),
    StructField("type", StringType(), nullable=True),
    StructField("ts", StringType(), nullable=True),
    StructField("edited", StringType(), nullable=True),
    StructField("client_msg_id", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("team", StringType(), nullable=True),
    StructField("thread_ts", StringType(), nullable=True),
    StructField("reply_count", StringType(), nullable=True),
    StructField("reply_users_count", StringType(), nullable=True),
    StructField("latest_reply", StringType(), nullable=True),
    StructField("reply_users", StringType(), nullable=True),
    StructField("is_locked", StringType(), nullable=True),
    StructField("subscribed", StringType(), nullable=True),
    StructField("blocks", ArrayType(StringType()), nullable=True),
    StructField("parent_user_id", StringType(), nullable=True),
    StructField("reactions", StringType(), nullable=True),
])

# Create DataFrame from all_channels list
DF_ILS_Messages = spark.createDataFrame(all_channels, schema)
DF_ILS_Messages.count()

# COMMAND ----------

DF_ILS_Messages = DF_ILS_Messages.withColumn("concatenated_columns", concat_ws(
'',
col("ts"),
col("thread_ts"),
col("text"),
col("client_msg_id"),
col("user")
))
DF_ILS_Messages = DF_ILS_Messages.withColumn("Reply_ID", md5("concatenated_columns"))
DF_ILS_Messages = DF_ILS_Messages.drop("concatenated_columns")
DF_ILS_Messages = DF_ILS_Messages.withColumn("DW_Timestamp", to_timestamp(DF_ILS_Messages.ts.cast("double")))
DF_ILS_Messages = DF_ILS_Messages.withColumn("Sourcesystem_ID", lit(7))
DF_ILS_Messages = DF_ILS_Messages.withColumn("Conversation_Type", lit("Public"))
DF_ILS_Messages = DF_ILS_Messages.withColumn("Sourcesystem_Name", lit("Slack-ILS"))
DF_ILS_Messages = DF_ILS_Messages.withColumn("Created_By",lit("Databricks")) 
DF_ILS_Messages = DF_ILS_Messages.withColumn("Created_Date", current_timestamp()) 
DF_ILS_Messages = DF_ILS_Messages.withColumn("Last_Modified_By", lit("Databricks")) 
DF_ILS_Messages = DF_ILS_Messages.withColumn("Last_Modified_Date", current_timestamp()) 

# COMMAND ----------

#Union Both the dataframe and Reorder the columns accordingly 
try:
     DF_History=DF_Brokerage_Messages.union(DF_ILS_Messages)
    #  DF_History=DF_Brokerage_Messages.union(DF_ILS_Messages)
     DF_Replies = DF_History.select(col("Reply_ID"),col("ChannelID"),col("Conversation_Type"),col("User"),col("type"),col("ts"),col("client_msg_id"),col("text"),col("team"),col("thread_ts"),col("reply_count"),col("reply_users_count"),col("latest_reply"),col("reply_users"),col("is_locked"),col("subscribed"),col("blocks"),col("reactions"),col("parent_user_id"),col("DW_Timestamp"),col("Sourcesystem_ID"),col("Sourcesystem_Name"),col("Created_By"),col("Created_Date"),col("Last_Modified_By"),col("Last_Modified_Date"))
     try:
          DF_Replies=DF_Replies.drop_duplicates(["ChannelID","ts","text"])
     except Exception as e:
          logger.info("unable to drop duplicate")
          print(e)
     try:
               Hashkey_Merge = ["Reply_ID","thread_ts","DW_Timestamp","team","reply_count","reply_users_count","latest_reply","reply_users","is_locked","subscribed","blocks","reactions","parent_user_id","Sourcesystem_ID","User","Conversation_Type"]

               DF_SlackMessages_1 = DF_Replies.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
     
     except Exception as e:
               logger.info(f"Unable to create Hashkey for Silver_Shippers {e}")
               print(e)
     try:
          MergeKey_Merge = ["ChannelID","ts","text"]

          DF_SlackMessages = DF_SlackMessages_1.withColumn("Mergekey",md5(concat_ws("",*MergeKey_Merge)))
     
     except Exception as e:
               logger.info(f"Unable to create Hashkey for SlackMessage {e}")
               print(e)


except Exception as e:
    logger.info("Unable to perform union")
    print(e)


# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            
            # Add HashKey column
            DF_SlackMessages.createOrReplaceTempView('SourceToBronze')
            
            # Count the number of rows
            Rowcount = DF_SlackMessages.count()
            
            # Define the merge query
            MergeQuery = '''
                MERGE INTO bronze.Slack_Conversations_Replies AS Target
                USING SourceToBronze AS Source ON Target.Mergekey=Source.Mergekey  When Matched and  Target.Hashkey!= Source.Hashkey  THEN UPDATE SET
                        Target.HashKey = Source.HashKey,
                        Target.Mergekey = Source.Mergekey,
                        Target.Reply_ID = Source.Reply_ID,
                        Target.ChannelID = Source.ChannelID,
                        Target.Conversation_Type = Source.Conversation_Type,
                        Target.User = Source.User,
                        Target.type = Source.type,
                        Target.ts = Source.ts,
                        Target.client_msg_id = Source.client_msg_id,
                        Target.text = Source.text,
                        Target.team = Source.team,
                        Target.thread_ts = Source.thread_ts,
                        Target.reply_count = Source.reply_count,
                        Target.reply_users_count = Source.reply_users_count,
                        Target.latest_reply = Source.latest_reply,
                        Target.reply_users = Source.reply_users,
                        Target.is_locked = Source.is_locked,
                        Target.subscribed = Source.subscribed,
                        Target.blocks = Source.blocks,
                        Target.parent_user_id = Source.parent_user_id,
                        Target.reactions=Source.reactions,
                        Target.DW_Timestamp = Source.DW_Timestamp,
                        Target.Sourcesystem_ID = Source.Sourcesystem_ID,
                        Target.Sourcesystem_Name = Source.Sourcesystem_Name,
                        Target.Created_By = Source.Created_By,
                        Target.Created_Date = Source.Created_Date,
                        Target.Last_Modified_By = Source.Last_Modified_By,
                        Target.Last_Modified_Date = Source.Last_Modified_Date
                WHEN NOT MATCHED THEN
                    INSERT(
                        HashKey,
                        Mergekey,
                        Reply_ID,
                        ChannelID,
                        Conversation_Type,
                        User,
                        type,
                        ts,
                        client_msg_id,
                        text,
                        team,
                        thread_ts,
                        reply_count,
                        reply_users_count,
                        latest_reply,
                        reply_users,
                        is_locked,
                        subscribed,
                        blocks,
                        parent_user_id,
                        reactions,
                        DW_Timestamp,
                        Sourcesystem_ID,
                        Sourcesystem_Name,
                        Created_By,
                        Created_Date,
                        Last_Modified_By,
                        Last_Modified_Date
                    ) VALUES (
                        Source.HashKey,
                        Source.Mergekey,
                        Source.Reply_ID,
                        Source.ChannelID,
                        Source.Conversation_Type,
                        Source.User,
                        Source.type,
                        Source.ts,
                        Source.client_msg_id,
                        Source.text,
                        Source.team,
                        Source.thread_ts,
                        Source.reply_count,
                        Source.reply_users_count,
                        Source.latest_reply,
                        Source.reply_users,
                        Source.is_locked,
                        Source.subscribed,
                        Source.blocks,
                        Source.parent_user_id,
                        Source.reactions,
                        Source.DW_Timestamp,
                        Source.Sourcesystem_ID,
                        Source.Sourcesystem_Name,
                        Source.Created_By,
                        Source.Created_Date,
                        Source.Last_Modified_By,
                        Source.Last_Modified_Date
                    )
            '''

            # Execute the merge query
            spark.sql(MergeQuery)

            logger.info('Successfully loaded the ' + TableName + ' to bronze')
            print('Loaded ' + TableName)

            # Find the maximum date
            MaxDateQuery = "SELECT MAX({0}) AS Max_Date FROM SourceToBronze".format(MaxLoadDateColumn)
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


