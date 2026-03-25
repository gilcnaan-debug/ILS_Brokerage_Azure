# Databricks notebook source
# MAGIC %md
# MAGIC ## BronzeToSilver
# MAGIC * **Description:** To extract tables from Bronze to Silver as delta file
# MAGIC * **Created Date:** 22/05/2024
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** 06/06/2024
# MAGIC * **Modified By:** Freedon Demi
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Required Packages

# COMMAND ----------

from pyspark.sql.functions import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, explode
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initializing Utilities

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initializing Logger

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

ErrorLogger = ErrorLogs("Silver_slack_Conversaions_History")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Metadata Details

# COMMAND ----------

TableID = 'SZ22'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")

# COMMAND ----------

try:
    TablesList = DF_Metadata.select(col("TableID")).collect()
    TableName = (
        DF_Metadata.select(col("SourceTableName"))
        .where(col("TableID") == TableID)
        .collect()[0]
        .SourceTableName
    )
    ErrorPath = (
        DF_Metadata.select(col("ErrorLogPath"))
        .where(col("TableID") == TableID)
        .collect()[0]
        .ErrorLogPath
    )
    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == TableID).collect()[0].MergeKey) 
    LoadType = (DF_Metadata.select(col("LoadType")).where(col("TableID") == TableID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('UnixTime')).where(col('TableID') == TableID).collect()[0].UnixTime)
except Exception as e:
    logger.info("Unable to fetch details from metadata")
    print(e)

# COMMAND ----------

try:
    Job_ID = "JOB_6"
    Notebook_ID = "NB_6"
    Zone = "Silver"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Perform Required Transformations

# COMMAND ----------

try:
    DF_Bronze_History = spark.sql(
                    '''select 
                        ch.DW_Conversations_History_ID AS DW_Conversations_History_ID,
                        ch.Conversation_ID as Conversation_ID,
                        ch.ChannelID as Channel_ID,
                        ch.Conversation_Type as Conversation_Type,
                        ch.user as User_ID,
                        ch.type as Text_Type,
                        ch.ts as Message_Timestamp,
                        ch.edited as Edited,
                        ch.client_msg_id as Client_Msg_ID,
                        ch.text as Actual_Text,
                        ch.team as Team_ID,
                        ch.thread_ts as Thread_Msg_Timestamp,
                        ch.reply_count as Reply_Count,
                        ch.reply_users_count as Replied_Users_Count,
                        ch.latest_reply as Latest_Reply_Timestamp,
                        ch.reply_users as Replied_Users_ID,
                        ch.is_locked as Is_Locked,
                        ch.Subscribed as Is_Subscribed,
                        ch.blocks as Blocks,
                        ch.reactions as Reactions,
                        ch.subtype as Sub_Type,
                        ch.inviter as Inviter,
                        ch.DW_Timestamp as DW_Timestamp,
                        CASE WHEN ch.Blocks LIKE '%emoji%' THEN '1' ELSE '0' END AS Is_Emoji,
                        CASE WHEN ch.reactions is not null THEN '1' ELSE '0' END AS Is_Reaction,
                        ch.Sourcesystem_ID as Sourcesystem_ID,
                        ch.Sourcesystem_Name as Sourcesystem_Name,
                        ch.Created_By as Created_By,
                        ch.Created_Date as Created_Date,
                        ch.Last_Modified_By as Last_Modified_By,
                        ch.Last_Modified_Date as Last_Modified_Date
                        from bronze.slack_conversations_history ch
                         '''
                )
except Exception as e:
    logger.info("unable to fetch data from bronze")
    print(e)


# COMMAND ----------

try:
    DF_Slack_History = DF_Bronze_History.select(col("DW_Conversations_History_ID"),col("Conversation_ID"),col("Channel_ID"),col("Conversation_Type"),col("User_ID"),col("Text_Type"),col("Message_Timestamp"),col("Edited"),col("Client_Msg_ID"),col("Actual_Text"),col("Team_ID"),col("Thread_Msg_Timestamp"),col("Reply_Count"),col("Replied_Users_Count"),col("Latest_Reply_Timestamp"),col("Replied_Users_ID"),col("Is_Locked"),col("Is_Subscribed"),col("Blocks"),col("Reactions"),col("Sub_Type"),col("Inviter"),col("Is_Emoji"),col("Is_Reaction"),col("Sourcesystem_ID"),col("Sourcesystem_Name"),col("DW_Timestamp"),col("Created_By"),col("Created_Date"),col("Last_Modified_By"),col("Last_Modified_Date"))

    
except Exception as e:
    logger.info("Unable to parse the json data")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge To target Table

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            DF_Conversation_History = DF_Slack_History.withColumn('HashKey',md5(concat_ws("",*MergeKey.split(","))))
            DF_Conversation_History.createOrReplaceTempView('BronzeToSilver')
            Rowcount = DF_Conversation_History.count()
            MergeQuery = 'Merge into Silver.{0} Target using BronzeToSilver source on Target.DW_Conversations_History_ID = Source.DW_Conversations_History_ID WHEN MATCHED AND Target.Hashkey!=Source.Hashkey THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
            spark.sql(MergeQuery.format(TableName,MergeKey,MergeKey))
            logger.info('Successfully loaded the'+TableName+'to bronze')

            print('Loaded ' + TableName)  
            MaxDateQuery = "Select max({0}) as Max_Date from BronzeToSilver"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Bronze')
        except Exception as e:
            Error_Statement = str(e).replace("'", "''")
            UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Merge-Target Table")
            logger.info('Failed for Bronze load')
            UpdateFailedStatus(TableID,'Bronze')
            logger.info('Updated the Metadata for Failed Status '+TableName)
            print('Unable to load '+TableName)
            print(e)
        UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Merge-Target Table")
except Exception as e:
    # logger error message
    logger.error(
        f"Table is already loaded: {str(e)}")
    print(e)



# COMMAND ----------

