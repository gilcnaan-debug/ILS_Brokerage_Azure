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
# MAGIC #Importing Packages

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

# MAGIC %run 
# MAGIC "//Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC #Error logger variables

# COMMAND ----------

#Creating variables to store error log information
try:
    ErrorLogger = ErrorLogs("Slack_Reactions_log_Grp")
    logger = ErrorLogger[0]
    p_logfile = ErrorLogger[1]
    p_filename = ErrorLogger[2]
except Exception as e:
  print("Unable to create variables",e)
  print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC #Use Catalog

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Fetching metadata details

# COMMAND ----------

try:
    DF_Metadata= spark.sql(
        "SELECT * FROM Metadata.MasterMetadata where TableID='SCR-N' and IsActive='1' "
    )
except Exception as e:
    print("Unable to fetch metadata details",e)
    print(e)

# COMMAND ----------

try:
    TableID  =  (DF_Metadata.select(col("TableID")).where(col("TableID") == "SCR-N").collect()[0].TableID)
except Exception as e:
    logger.info("Unable to fetch metadata details ",e)
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
    Job_ID = "SJOB_4_Grp_incremental"
    Notebook_ID = "SNB_4_Grp_incremental"
    Zone = "Bronze"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

try:
    MaxDateQuery = "Select max({0}) as Max_Date from bronze.slack_Reactions_New  where Conversation_Type = 'Public'"
    MaxDateQuery = MaxDateQuery.format('date_create')
    DF_MaxDate = spark.sql(MaxDateQuery)
    MaxTime = (DF_MaxDate.select(col("Max_Date")).collect()[0].Max_Date)
    print(MaxTime)
except Exception as e:
    logger.info("Unable to get max time ")
    print(e)

# COMMAND ----------

try:
    DF_Query = spark.sql(''' select id from bronze.slack_Channels_new where Conversation_Type = 'Group Message' and Is_Channel_Deleted = 0 ''')
except Exception as e:
    logger.info("Unable to retrieve channel ids",e)
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

# MAGIC %md
# MAGIC #Retrieveing Data from Source

# COMMAND ----------


clients = [WebClient(token=token) for token in tokens]
all_channels = []
token_index = 0
client = clients[token_index]
max_time = DF_MaxDate.select(col("Max_Date")).collect()[0]["Max_Date"]

Next_Cursor = ""
for channel_row in DF_Query.collect():
    channel_id = channel_row.id

    while True:
        params = {
            'channel': channel_id,
            'limit': 1000,
            'cursor': Next_Cursor,
            'oldest': max_time
        }

        try:
            channels_response = client.api_call(
                api_method='discovery.conversations.reactions',
                http_verb='GET',
                params=params
            )

            if channels_response.get('ok', False):
                messages = channels_response.get('reactions', [])
                messages = [message for message in messages if not message.get("bot_id")]
                for message in messages:
                    message["Channel_ID"] = channel_id
                all_channels.extend(messages)

                Next_Cursor = channels_response.get('response_metadata', {}).get('next_cursor', '')
                print(channel_id)
                if not Next_Cursor:
                    break
            else:
                print(f"Error fetching messages for channel {channel_id}: {channels_response['error']}")
                break

        except SlackApiError as e:
            Error_Statement = str(e).replace("'", "''")
            UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Ignorable",1,"Slack-reactions-Grp")
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
                    # Retry the request with the same channel and the next token
                continue
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Slack-reactions-grp")
print(len(all_channels))

# COMMAND ----------

try:
    schema = StructType([
        StructField("Channel_ID", StringType(), nullable=True),
        StructField("user", StringType(), nullable=True),
        StructField("team", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("date_create", StringType(), nullable=True),
        StructField("ts", StringType(), nullable=True)
    ])
except Exception as e:
    logger.ingo('Unable to parse the columns')
    print(e)

# COMMAND ----------

try:
    DF_reactions=spark.createDataFrame(all_channels,schema)
    DF_reactions.count()
except Exception as e:
    logger.info("Unable to pharse columns into schema",e)
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC Adding New Columns

# COMMAND ----------

try:
    DF_reactions = DF_reactions.withColumn("DW_Timestamp", to_timestamp(DF_reactions.date_create.cast("double")))
    DF_reactions = DF_reactions.withColumn("Conversation_Type", lit("None"))
    DF_reactions = DF_reactions.withColumn("Sourcesystem_Name", lit("Slack"))
    DF_reactions = DF_reactions.withColumn("Created_By", lit("Databricks"))
    DF_reactions = DF_reactions.withColumn("Created_Date", current_timestamp())
    DF_reactions = DF_reactions.withColumn("Last_Modified_By", lit("Databricks"))
    DF_reactions = DF_reactions.withColumn("Last_Modified_Date", current_timestamp())
    Hash_Key = ["Channel_ID","date_create","team"]
    DF_reactions = DF_reactions.withColumn("HashKey",md5(concat_ws("",*Hash_Key)))
    Merge_Key = ["ts","name","user"]
    DF_reactions = DF_reactions.withColumn("MergeKey",md5(concat_ws("",*Merge_Key)))
    DF_reactions.createOrReplaceTempView('VW_Reactions')
except Exception as e:
    logger.info("Unable to create Newcolumns for DF_Messages",e)
    print(e)

# COMMAND ----------

try:
    DF_Channels=spark.sql(''' select id,Conversation_type from bronze.slack_channels_new''')
    DF_Channels.createOrReplaceTempView("VW_Channels")
except Exception as e:
    logger.info("Unable to create view for DF_channels",e)
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC #Merge Data into Target Table

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            
            # Add HashKey column
            
            # Count the number of rows
            Rowcount = DF_reactions.count()
            
            # Define the merge query
            MergeQuery = '''
                MERGE INTO bronze.Slack_Reactions_New AS Target
                USING VW_Reactions AS Source ON Target.Mergekey=Source.Mergekey  When Matched and  Target.Hashkey!= Source.Hashkey  THEN UPDATE SET
                        
                        Target.Channel_ID = Source.Channel_ID,
                        Target.team = Source.team,
                        Target.user = Source.user,
                        Target.name = Source.name,
                        Target.date_create = Source.date_create,
                        Target.ts = Source.ts,
                        Target.HashKey = Source.HashKey,
                        Target.MergeKey = Source.MergeKey,
                        Target.DW_Timestamp = Source.DW_Timestamp,
                        Target.Sourcesystem_Name = Source.Sourcesystem_Name,
                        Target.Conversation_Type = Source.Conversation_Type,
                        Target.Created_By = Source.Created_By,
                        Target.Created_Date = Source.Created_Date,
                        Target.Last_Modified_By = Source.Last_Modified_By,
                        Target.Last_Modified_Date = Source.Last_Modified_Date
                WHEN NOT MATCHED THEN
                    INSERT(
                        
                        Channel_ID,
                        team,
                        user,
                        name,
                        date_create,
                        ts,
                        MergeKey,
                        HashKey,
                        DW_Timestamp,
                        Conversation_Type,
                        Sourcesystem_Name,
                        Created_By,
                        Created_Date,
                        Last_Modified_By,
                        Last_Modified_Date
                    ) VALUES (
                      
                        Source.Channel_ID,
                        Source.team,
                        Source.user,
                        Source.name,
                        Source.date_create,
                        Source.ts,
                        Source.MergeKey,
                        Source.HashKey,
                        Source.DW_Timestamp,
                        Source.Conversation_Type,
                        Source.Sourcesystem_Name,
                        Source.Created_By,
                        Source.Created_Date,
                        Source.Last_Modified_By,
                        Source.Last_Modified_Date
                    )'''
            spark.sql(MergeQuery)
            dg=spark.sql('''MERGE INTO bronze.Slack_Reactions_New AS OT USING VW_Channels AS OS 
              ON OT.channel_ID = OS.id WHEN MATCHED THEN UPDATE SET 
              OT.conversation_type = OS.conversation_type''')                  
            # Execute the merge query

            # Execute the merge query
            spark.sql(MergeQuery)

            logger.info('Successfully loaded the ' + TableName + ' to bronze')
            print('Loaded ' + TableName)

            # Find the maximum date
            MaxDateQuery = "SELECT MAX({0}) AS Max_Date FROM VW_Reactions".format(MaxLoadDateColumn)
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

