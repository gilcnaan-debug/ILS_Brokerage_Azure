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
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initializing Utilities

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initializing Logger

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

ErrorLogger = ErrorLogs("Silver_slack_channels_1hr_Refresh")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Use Catalog

# COMMAND ----------

CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Metadata Details

# COMMAND ----------

DF_Metadata= spark.sql(
    "SELECT * FROM Metadata.MasterMetadata where TableID='SZ20-N' and IsActive='1' "
)

# COMMAND ----------

TableID  =  (DF_Metadata.select(col("TableID")).where(col("TableID") == "SZ20-N").collect()[0].TableID)

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

try:
    Job_ID = "JOB_5"
    Notebook_ID = "NB_5"
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
    DF_Bronze_Channels = spark.sql(
                    '''select
                        ch.DW_channel_ID AS DW_Channel_ID, 
                        ch.id AS Channel_ID,
                        ch.name AS Channel_Name,
                        ch.purpose AS Purpose,
                        ch.member_count AS Member_Count,
                        ch.external_user_count AS External_User_Count,
                        ch.channel_manager_count AS Channel_Manager_Count,
                        ch.created AS Channel_Created_Date,
                        ch.creator_id AS Channel_Creator_ID,
                        ch.is_private AS Is_Private,
                        ch.is_im AS Is_IM,
                        ch.is_mpim AS Is_MPIM,
                        ch.is_archived AS Is_Archived,
                        ch.is_general AS Is_General,
                        ch.is_deleted AS Is_Deleted,
                        ch.is_file AS Is_File,
                        ch.is_org_default AS Is_Org_Default,
                        ch.last_activity_ts AS Last_Activity_Timestamp,
                        ch.is_org_mandatory AS Is_Org_Mandatory,
                        ch.is_ext_shared AS Is_Ext_Shared,
                        ch.is_org_shared AS Is_Org_Shared,
                        ch.is_global_shared AS Is_Global_Shared,
                        ch.is_frozen AS Is_Frozen,
                        ch.context_team_id AS Context_Team_ID,
                        ch.connected_team_ids AS Connected_Limited_Team_IDs,
                        ch.connected_limited_team_ids AS Connected_Team_IDs,
                        ch.internal_team_ids_count AS Internal_Team_IDs,
                        ch.internal_team_ids_sample_team AS Internal_Team_IDs_Count,
                        ch.internal_team_ids AS Internal_Team_IDs_Sample_Team,
                        ch.pending_connected_team_ids AS Is_Pending_Ext_Shared,
                        ch.is_pending_ext_shared AS Pending_Connected_Team_IDs,
                        ch.channel_email_addresses AS Channel_Email_Addresses,
                        ch.is_disconnect_in_progress AS Is_Disconnect_In_Progress,
                        ch.canvas AS Canvas,
                        ch.topic AS Topic,
                        ch.HashKey AS HashKey,
                        ch.Conversation_Type AS Conversation_Type,
                        ch.Is_Channel_Deleted AS Is_Channel_Deleted,
                        ch.Sourcesystem_Name AS Sourcesystem_Name,
                        ch.DW_Timestamp AS DW_Timestamp,
                        ch.Created_By AS Created_By,
                        ch.Created_Date AS Created_Date,
                        ch.Last_Modified_By AS Last_Modified_By,
                        ch.Last_Modified_Date AS Last_Modified_Date
                        from bronze.slack_channels_New ch '''
                )
except Exception as e:
    logger.info("unable to fetch data from bronze")
    print(e)


# COMMAND ----------

try:
    DF_Bronze_Channels = DF_Bronze_Channels.withColumn("Workspace_ID", lit('None'))
    DF_Bronze_Channels = DF_Bronze_Channels.withColumn("Workspace_Name", lit('None'))
except Exception as e:
    logger.info("unable to add new columns",e)
    print(e)
DF_Bronze_Channels.createOrReplaceTempView('VW_Channels')

# COMMAND ----------

try:
    DF_Workspace = spark.sql('''select context_team_id from bronze.slack_channels_new''')
    DF_Workspace.createOrReplaceTempView('VW_Channels1')
except Exception as e:
    logger.info("unable to create view",e)
    print(e)



# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge To Target Table

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            
            # Add HashKey column
            
            # Count the number of rows
            Rowcount = DF_Bronze_Channels.count()
            
            # Define the merge query
            MergeQuery = '''
                MERGE INTO silver.Silver_Slack_Channels_New AS Target
                USING VW_Channels AS Source ON Target.DW_Channel_ID=Source.DW_Channel_ID  When Matched and  Target.Hashkey!= Source.Hashkey THEN
                UPDATE SET
                    Target.DW_Channel_ID = Source.DW_Channel_ID,
                    Target.Channel_ID = Source.Channel_ID,
                    Target.Channel_Name = Source.Channel_Name,
                    Target.Purpose = Source.Purpose,
                    Target.Member_Count = Source.Member_Count,
                    Target.External_User_Count = Source.External_User_Count,
                    Target.Channel_Manager_Count = Source.Channel_Manager_Count,
                    Target.Channel_Created_Date = Source.Channel_Created_Date,
                    Target.Channel_Creator_ID = Source.Channel_Creator_ID,
                    Target.Is_Private = Source.Is_Private,
                    Target.Is_IM = Source.Is_IM,
                    Target.Is_MPIM = Source.Is_MPIM,
                    Target.Is_Archived = Source.Is_Archived,
                    Target.Is_General = Source.Is_General,
                    Target.Is_Deleted = Source.Is_Deleted,
                    Target.Is_File = Source.Is_File,
                    Target.Is_Org_Default = Source.Is_Org_Default,
                    Target.Last_Activity_Timestamp = Source.Last_Activity_Timestamp,
                    Target.Is_Org_Mandatory = Source.Is_Org_Mandatory,
                    Target.Is_Ext_Shared = Source.Is_Ext_Shared,
                    Target.Is_Org_Shared = Source.Is_Org_Shared,
                    Target.Is_Global_Shared = Source.Is_Global_Shared,
                    Target.Is_Frozen = Source.Is_Frozen,
                    Target.Context_Team_ID = Source.Context_Team_ID,
                    Target.Connected_Limited_Team_IDs = Source.Connected_Limited_Team_IDs,
                    Target.Connected_Team_IDs = Source.Connected_Team_IDs,
                    Target.Internal_Team_IDs = Source.Internal_Team_IDs,
                    Target.Internal_Team_IDs_Count = Source.Internal_Team_IDs_Count,
                    Target.Internal_Team_IDs_Sample_Team = Source.Internal_Team_IDs_Sample_Team,
                    Target.Is_Pending_Ext_Shared = Source.Is_Pending_Ext_Shared,
                    Target.Pending_Connected_Team_IDs = Source.Pending_Connected_Team_IDs,
                    Target.Channel_Email_Addresses = Source.Channel_Email_Addresses,
                    Target.Is_Disconnect_In_Progress = Source.Is_Disconnect_In_Progress,
                    Target.Canvas = Source.Canvas,
                    Target.Topic = Source.Topic,
                    Target.HashKey = source.HashKey,
                    Target.Is_Channel_Deleted = Source.Is_Channel_Deleted,
                    Target.Conversation_Type = Source.Conversation_Type,
                    Target.DW_Timestamp = Source.DW_Timestamp,
                    Target.SourceSystem_Name = Source.SourceSystem_Name,
                    Target.Workspace_ID = Source.Workspace_ID,
                    Target.Workspace_Name = Source.Workspace_Name,
                    Target.Last_Modified_By = Source.Last_Modified_By,
                    Target.Last_Modified_Date = Source.Last_Modified_Date,
                    Target.Created_By = Source.Created_By,
                    Target.Created_Date = Source.Created_Date
            WHEN NOT MATCHED THEN
                INSERT (
                    DW_Channel_ID,
                    Channel_ID,
                    Channel_Name,
                    Purpose,
                    Member_Count,
                    External_User_Count,
                    Channel_Manager_Count,
                    Channel_Created_Date,
                    Channel_Creator_ID,
                    Is_Private,
                    Is_IM,
                    Is_MPIM,
                    Is_Archived,
                    Is_General,
                    Is_Deleted,
                    Is_File,
                    Is_Org_Default,
                    Last_Activity_Timestamp,
                    Is_Org_Mandatory,
                    Is_Ext_Shared,
                    Is_Org_Shared,
                    Is_Global_Shared,
                    Is_Frozen,
                    Context_Team_ID,
                    Connected_Limited_Team_IDs,
                    Connected_Team_IDs,
                    Internal_Team_IDs,
                    Internal_Team_IDs_Count,
                    Internal_Team_IDs_Sample_Team,
                    Is_Pending_Ext_Shared,
                    Pending_Connected_Team_IDs,
                    Channel_Email_Addresses,
                    Is_Disconnect_In_Progress,
                    Canvas,
                    Topic,
                    HashKey,
                    Is_Channel_Deleted,
                    Conversation_Type,
                    DW_Timestamp,
                    SourceSystem_Name,
                    Workspace_ID,
                    Workspace_Name,
                    Created_By,
                    Created_Date,
                    Last_Modified_By,
                    Last_Modified_Date
                ) VALUES (
                    Source.DW_Channel_ID,
                    Source.Channel_ID,
                    Source.Channel_Name,
                    Source.Purpose,
                    Source.Member_Count,
                    Source.External_User_Count,
                    Source.Channel_Manager_Count,
                    Source.Channel_Created_Date,
                    Source.Channel_Creator_ID,
                    Source.Is_Private,
                    Source.Is_IM,
                    Source.Is_MPIM,
                    Source.Is_Archived,
                    Source.Is_General,
                    Source.Is_Deleted,
                    Source.Is_File,
                    Source.Is_Org_Default,
                    Source.Last_Activity_Timestamp,
                    Source.Is_Org_Mandatory,
                    Source.Is_Ext_Shared,
                    Source.Is_Org_Shared,
                    Source.Is_Global_Shared,
                    Source.Is_Frozen,
                    Source.Context_Team_ID,
                    Source.Connected_Limited_Team_IDs,
                    Source.Connected_Team_IDs,
                    Source.Internal_Team_IDs,
                    Source.Internal_Team_IDs_Count,
                    Source.Internal_Team_IDs_Sample_Team,
                    Source.Is_Pending_Ext_Shared,
                    Source.Pending_Connected_Team_IDs,
                    Source.Channel_Email_Addresses,
                    Source.Is_Disconnect_In_Progress,
                    Source.Canvas,
                    Source.Topic,
                    Source.HashKey,
                    Source.Is_Channel_Deleted,
                    Source.Conversation_Type,
                    Source.DW_Timestamp,
                    Source.SourceSystem_Name,
                    Source.Workspace_ID,
                    Source.Workspace_Name,
                    Source.Created_By,
                    Source.Created_Date,
                    Source.Last_Modified_By,
                    Source.Last_Modified_Date
                )'''
            spark.sql(MergeQuery)
            dg=spark.sql('''UPDATE silver.Silver_Slack_Channels_New wc
                            SET 
                                wc.Workspace_ID = CASE
                                    WHEN wc.Context_Team_ID = 'T1D2RFN9H' THEN 'W1'
                                    WHEN wc.Context_Team_ID = 'T046156LPKM' THEN 'W2'
                                    WHEN wc.Context_Team_ID = 'T02RUKYS21Y' THEN 'W3'
                                    WHEN wc.Context_Team_ID = 'T05G1BTTU3T' THEN 'W4'
                                    WHEN wc.Context_Team_ID = 'T03EF2ZDFBJ' THEN 'W5'
                                    WHEN wc.Context_Team_ID = 'T036CCPJ7DH' THEN 'W6'
                                    WHEN wc.Context_Team_ID = 'T060H7TQXKK' THEN 'W7'
                                    WHEN wc.Context_Team_ID = 'T044ZHKAVTN' THEN 'W8'
                                    WHEN wc.Context_Team_ID = 'T030MHW5A86' THEN 'W9'
                                    WHEN wc.Context_Team_ID = 'T06ET71TBT8' THEN 'W10'
                                    WHEN wc.Context_Team_ID = 'T039KT5S2MQ' THEN 'W11'
                                    WHEN wc.Context_Team_ID = 'E02F5ESJP0F' THEN 'W12'
                                    WHEN Wc.Context_Team_ID is NULL and Wc.Context_Team_ID like 'D%' THEN "No Workspace_ID"
                                    WHEN Wc.Context_Team_ID is NULL and Wc.Channel_Name like 'mpdm%' THEN "No Workspace_ID"
                                    ELSE 'W13'
                                END,
                                wc.Workspace_Name = CASE
                                    WHEN wc.Context_Team_ID = 'T1D2RFN9H' THEN 'Brokerage'
                                    WHEN wc.Context_Team_ID = 'T046156LPKM' THEN 'Distribution'
                                    WHEN wc.Context_Team_ID = 'T02RUKYS21Y' THEN 'Integrated Logistics'
                                    WHEN wc.Context_Team_ID = 'T05G1BTTU3T' THEN 'Dryage'
                                    WHEN wc.Context_Team_ID = 'T03EF2ZDFBJ' THEN 'Global'
                                    WHEN wc.Context_Team_ID = 'T036CCPJ7DH' THEN 'Intermodal'
                                    WHEN wc.Context_Team_ID = 'T060H7TQXKK' THEN 'IT'
                                    WHEN wc.Context_Team_ID = 'T044ZHKAVTN' THEN 'NFI ILS Sandbox'
                                    WHEN wc.Context_Team_ID = 'T030MHW5A86' THEN 'Relay'
                                    WHEN wc.Context_Team_ID = 'T06ET71TBT8' THEN 'Slack Integration Team'
                                    WHEN wc.Context_Team_ID = 'T039KT5S2MQ' THEN 'TM'
                                    WHEN wc.Context_Team_ID = 'E02F5ESJP0F' THEN 'NFI Integrated Logistics'
                                    WHEN Wc.Context_Team_ID is NULL and Wc.Context_Team_ID like 'D%' THEN "Slack-DMs"
                                    WHEN Wc.Context_Team_ID is NULL and Wc.Channel_Name like 'mpdm%' THEN "Slack-Group Message"
                                    ELSE 'Slack'
                                END
                           

 ''')          

           

            logger.info('Successfully loaded the ' + TableName + ' to bronze')
            print('Loaded ' + TableName)

            # Find the maximum date
            MaxDateQuery = "SELECT MAX({0}) AS Max_Date FROM VW_Channels".format(MaxLoadDateColumn)
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

