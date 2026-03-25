# Databricks notebook source
# MAGIC %md
# MAGIC ## BronzeToSilver
# MAGIC * **Description:** To extract tables from Bronze to Silver as delta file
# MAGIC * **Created Date:** 18/07/2024
# MAGIC * **Created By:** Hariharan
# MAGIC * **Modified Date:** 24/07/2024
# MAGIC * **Modified By:** Hariharan
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

ErrorLogger = ErrorLogs("Silver_slack_Conversaions_History_log_Silver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC #Use Catalog

# COMMAND ----------

CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Metadata Details

# COMMAND ----------

try:
    TableID = 'SZ22-N'
    DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
except Exception as e:
    logger.info("unable to fetch details",e)
    print(e)

# COMMAND ----------

try:
    TablesList = DF_Metadata.select(col("TableID")).collect()
    TableName = (
        DF_Metadata.select(col("SourceTableName"))
        .where(col("TableID") == TableID)
        .collect()[0]
        .SourceTableName
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
    Job_ID = "SJOB_7"
    Notebook_ID = "SNB_7"
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
                        ch.Conversation_Type as Conversation_Type,
                        ch.Channel_ID as Channel_ID,
                        ch.user as User_ID,
                        ch.type AS Message_Type,
                        ch.ts as Message_Timestamp,
                        ch.client_msg_id as Client_Msg_ID,
                        ch.text as Actual_Text,
                        ch.team as Team_ID,
                        ch.attachments AS Attachments,
                        ch.thread_ts as Thread_Msg_Timestamp,
                        ch.parent_user_id as Parent_Msg_User_ID,
                        ch.blocks as Blocks,
                        CASE WHEN ch.Blocks LIKE '%emoji%' THEN '1' ELSE '0' END AS Is_Emoji,
                        ch.upload AS Upload,
                        ch.files AS Files,
                        ch.edited as Edited,
                        ch.reply_count as Reply_Count,
                        ch.replies AS Actual_Replies,
                        ch.reply_users_count as Replied_Users_Count,
                        ch.latest_reply as Latest_Reply_Timestamp,
                        ch.reply_users as Replied_Users_ID,
                        ch.user_team AS User_Team,
                        ch.source_team AS Source_Team,
                        ch.user_profile AS User_Profile,
                        ch.is_locked as Is_Locked,
                        ch.Sourcesystem_Name as Sourcesystem_Name,
                        ch.HashKey as HashKey,
                        ch.MergeKey AS MergeKey,
                        ch.DW_Timestamp AS DW_Timestamp,
                        ch.Created_By as Created_By,
                        ch.Created_Date as Created_Date,
                        ch.Last_Modified_By as Last_Modified_By,
                        ch.Last_Modified_Date as Last_Modified_Date
                        from bronze.slack_conversations_history_New ch
                         '''
                )
except Exception as e:
    logger.info("unable to fetch data from bronze")
    print(e)


# COMMAND ----------

try:
    DF_Bronze_History = DF_Bronze_History.withColumn("Workspace_ID", lit("None"))
    DF_Bronze_History = DF_Bronze_History.withColumn("Workspace_Name", lit("None"))
    DF_Bronze_History = DF_Bronze_History.withColumn("Message_Category", lit("None"))

except Exception as e:
    print("unable to add new columns",e)
DF_Bronze_History.createOrReplaceTempView('VW_Conversation')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge To target Table

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            
            # Add HashKey column
            
            # Count the number of rows
            Rowcount = DF_Bronze_History.count()
            
            # Define the merge query
            MergeQuery = '''
                MERGE INTO silver.Silver_Slack_Conversations_History_New AS Target
                USING VW_Conversation AS Source ON Target.DW_Conversations_History_ID=Source.DW_Conversations_History_ID  When Matched and  Target.Hashkey!= Source.Hashkey THEN
                UPDATE SET
                    Target.DW_Conversations_History_ID = Source.DW_Conversations_History_ID,
                        Target.Conversation_Type = Source.Conversation_Type,
                        Target.Channel_ID = Source.Channel_ID,
                        Target.User_ID = Source.User_ID,
                        Target.Message_Type = Source.Message_Type,
                        Target.Message_Timestamp = Source.Message_Timestamp,
                        Target.Client_Msg_ID = Source.Client_Msg_ID,
                        Target.Actual_Text = Source.Actual_Text,
                        Target.Team_ID = Source.Team_ID,
                        Target.Attachments = Source.Attachments,
                        Target.Thread_Msg_Timestamp = Source.Thread_Msg_Timestamp,
                        Target.Parent_Msg_User_ID = Source.Parent_Msg_User_ID,
                        Target.Blocks = Source.Blocks,
                        Target.Is_Emoji = Source.Is_Emoji,
                        Target.Message_Category = Source.Message_Category,
                        Target.Upload = Source.Upload,
                        Target.Files = Source.Files,
                        Target.Edited = Source.Edited,
                        Target.Reply_Count = Source.Reply_Count,
                        Target.Actual_Replies = Source.Actual_Replies,
                        Target.Replied_Users_Count = Source.Replied_Users_Count,
                        Target.Latest_Reply_Timestamp = Source.Latest_Reply_Timestamp,
                        Target.Replied_Users_ID = Source.Replied_Users_ID,
                        Target.User_Team = Source.User_Team,
                        Target.Source_Team = Source.Source_Team,
                        Target.User_Profile = Source.User_Profile,
                        Target.Is_Locked = Source.Is_Locked,
                        Target.Sourcesystem_Name = Source.Sourcesystem_Name,
                        Target.HashKey = Source.HashKey,
                        Target.MergeKey = Source.MergeKey,
                        Target.DW_Timestamp = Source.DW_Timestamp,
                        Target.Workspace_ID = Source.Workspace_ID,
                        Target.Workspace_Name = Source.Workspace_Name,
                        Target.Created_By = Source.Created_By,
                        Target.Created_Date = Source.Created_Date,
                        Target.Last_Modified_By = Source.Last_Modified_By,
                        Target.Last_Modified_Date = Source.Last_Modified_Date
                WHEN NOT MATCHED 
                    THEN INSERT (
                        DW_Conversations_History_ID,
                        Channel_ID,
                        User_ID,
                        Message_Type,
                        Message_Timestamp,
                        Client_Msg_ID,
                        Actual_Text,
                        Team_ID,
                        Attachments,
                        Thread_Msg_Timestamp,
                        Parent_Msg_User_ID,
                        Blocks,
                        Is_Emoji,
                        Message_Category,
                        Upload,
                        Edited,
                        Files,
                        Reply_Count,
                        Actual_Replies,
                        Replied_Users_Count,
                        Latest_Reply_Timestamp,
                        Replied_Users_ID,
                        User_Team,
                        Source_Team,
                        User_Profile,
                        Is_Locked,
                        MergeKey,
                        HashKey,
                        Conversation_Type,
                        DW_Timestamp,
                        Workspace_ID,
                        Workspace_Name,
                        Sourcesystem_Name,
                        Created_By,
                        Created_Date,
                        Last_Modified_By,
                        Last_Modified_Date

                    )
                    VALUES (
                        Source.DW_Conversations_History_ID,
                        Source.Channel_ID,
                        Source.User_ID,
                        Source.Message_Type,
                        Source.Message_Timestamp,
                        Source.Client_Msg_ID,
                        Source.Actual_Text,
                        Source.Team_ID,
                        Source.Attachments,
                        Source.Thread_Msg_Timestamp,
                        Source.Parent_Msg_User_ID,
                        Source.Blocks,
                        Source.Is_Emoji,
                        Source.Message_Category,
                        Source.Upload,
                        Source.Edited,
                        Source.Files,
                        Source.Reply_Count,
                        Source.Actual_Replies,
                        Source.Replied_Users_Count,
                        Source.Latest_Reply_Timestamp,
                        Source.Replied_Users_ID,
                        Source.User_Team,
                        Source.Source_Team,
                        Source.User_Profile,
                        Source.Is_Locked,
                        Source.MergeKey,
                        Source.HashKey,
                        Source.Conversation_Type,
                        Source.DW_Timestamp,
                        Source.Workspace_ID,
                        Source.Workspace_Name,
                        Source.Sourcesystem_Name,
                        Source.Created_By,
                        Source.Created_Date,
                        Source.Last_Modified_By,
                        Source.Last_Modified_Date

                    )'''
            spark.sql(MergeQuery)
            DF_Workspace=spark.sql('''
                UPDATE silver.Silver_Slack_Conversations_History_New wc 
                SET 
                    wc.Workspace_ID = CASE
                        WHEN wc.Team_ID = 'T1D2RFN9H' THEN 'W1'
                        WHEN wc.Team_ID = 'T046156LPKM' THEN 'W2'
                        WHEN wc.Team_ID = 'T02RUKYS21Y' THEN 'W3'
                        WHEN wc.Team_ID = 'T05G1BTTU3T' THEN 'W4'
                        WHEN wc.Team_ID = 'T03EF2ZDFBJ' THEN 'W5'
                        WHEN wc.Team_ID = 'T036CCPJ7DH' THEN 'W6'
                        WHEN wc.Team_ID = 'T060H7TQXKK' THEN 'W7'
                        WHEN wc.Team_ID = 'T044ZHKAVTN' THEN 'W8'
                        WHEN wc.Team_ID = 'T030MHW5A86' THEN 'W9'
                        WHEN wc.Team_ID = 'T06ET71TBT8' THEN 'W10'
                        WHEN wc.Team_ID = 'T039KT5S2MQ' THEN 'W11'
                        WHEN wc.Team_ID = 'E02F5ESJP0F' THEN 'W12'
                        WHEN wc.Team_ID is NULL THEN "No Workspace_ID"
                        ELSE 'W13'
                    END,
                    wc.Workspace_Name = CASE
                        WHEN wc.Team_ID = 'T1D2RFN9H' THEN 'Brokerage'
                        WHEN wc.Team_ID = 'T046156LPKM' THEN 'Distribution'
                        WHEN wc.Team_ID = 'T02RUKYS21Y' THEN 'Integrated Logistics'
                        WHEN wc.Team_ID = 'T05G1BTTU3T' THEN 'Dryage'
                        WHEN wc.Team_ID = 'T03EF2ZDFBJ' THEN 'Global'
                        WHEN wc.Team_ID = 'T036CCPJ7DH' THEN 'Intermodal'
                        WHEN wc.Team_ID = 'T060H7TQXKK' THEN 'IT'
                        WHEN wc.Team_ID = 'T044ZHKAVTN' THEN 'NFI ILS Sandbox'
                        WHEN wc.Team_ID = 'T030MHW5A86' THEN 'Relay'
                        WHEN wc.Team_ID = 'T06ET71TBT8' THEN 'Slack Integration Team'
                        WHEN wc.Team_ID = 'T039KT5S2MQ' THEN 'TM'
                        WHEN wc.Team_ID = 'E02F5ESJP0F' THEN 'NFI Integrated Logistics'
                        WHEN wc.Team_ID is NULL THEN "Slack"
                        ELSE 'Slack'
                    END
                WHERE Conversation_Type in("Public","Private")
            ''') 

            DF_Workspace=spark.sql('''
                UPDATE silver.Silver_Slack_Conversations_History_New wc 
                SET 
                    wc.Workspace_ID = "W14",
                    wc.Workspace_Name = "Slack-General"
                WHERE Conversation_Type in("Group Message","Direct Message")
            ''')

            DF_Message_Category = spark.sql('''
                MERGE INTO silver.Silver_Slack_Conversations_History_New AS target
                USING vw_Conversation AS source
                ON target.DW_Conversations_History_ID = source.DW_Conversations_History_ID
                WHEN MATCHED THEN
                UPDATE SET target.Message_Category = 
                    CASE 
                    WHEN target.Thread_Msg_Timestamp IS NULL THEN 'Actual Message Without Reply'
                    WHEN target.Thread_Msg_Timestamp = target.Message_Timestamp 
                        AND (target.Reply_Count IS NOT NULL 
                        AND target.Replied_Users_ID IS NOT NULL 
                        AND target.Actual_Replies IS NOT NULL) THEN 'Actual Message With Replies'
                    WHEN target.Thread_Msg_Timestamp != target.Message_Timestamp 
                        AND target.Parent_Msg_User_ID IS NOT NULL THEN 'Reply Message'
                    ELSE 'Undefined'
                    END
            ''')
                 

            logger.info('Successfully loaded the ' + TableName + ' to bronze')
            print('Loaded ' + TableName)

            # Find the maximum date
            MaxDateQuery = "SELECT MAX({0}) AS Max_Date FROM VW_Conversation".format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdatePipelineStatusAndTime(TableID,'silver')
            logger.info('Successfully loaded the'+TableName+'to silver')
        except Exception as e:
            Error_Statement = str(e).replace("'", "''")
            UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Merge-Target Table")
            logger.info('Failed for silver load')
            UpdateFailedStatus(TableID,'silver')
            logger.info('Updated the Metadata for Failed Status '+TableName)
            print('Unable to load '+TableName)
            print(e)
        UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Merge-Target Table")
except Exception as e:
    logger.error(f"Table is already loaded: {str(e)}")
    print(e)


# COMMAND ----------

