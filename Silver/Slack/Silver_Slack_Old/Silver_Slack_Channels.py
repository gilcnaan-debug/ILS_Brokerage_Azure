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

TableID = 'SZ20'
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
except Exception as e:
    logger.info("Unable to fetch details from metadata")
    print(e)

# COMMAND ----------

try:
    Job_ID = "JOB_2"
    Notebook_ID = "NB_2"
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
                        ch.DW_channel_ID as DW_Channel_ID, 
                        ch.id AS Channel_ID,
                        ch.name AS Channel_Name,
                        ch.Is_Deleted,
                        ch.is_channel AS Is_Channel,
                        ch.is_group AS Is_Group,
                        ch.is_im AS Is_IM,
                        ch.is_mpim as IS_MPIM,
                        ch.is_private AS Is_Private,
                        ch.created AS Channel_Created_Date,
                        ch.is_archived AS Is_Archived,
                        ch.is_general AS Is_General,
                        ch.unlinked AS Unlinked,
                        ch.name_normalized AS Normalized_Name,
                        ch.is_shared AS Is_Shared,
                        ch.is_org_shared AS Is_Org_Shared,
                        ch.is_pending_ext_shared AS Is_Pending_Ext_Shared,
                        ch.pending_shared AS Pending_Shared,
                        ch.context_team_id AS Context_Team_ID,
                        ch.updated AS Updated_Date,
                        ch.parent_conversation AS Parent_Conversation,
                        ch.creator AS Creator_Name,
                        ch.is_moved AS Is_Moved,
                        ch.is_ext_shared AS Is_Ext_Shared,
                        ch.enterprise_id AS Enterprise_ID,
                        ch.is_global_shared AS Is_Global_Shared,
                        ch.is_org_default AS Is_Org_Default,
                        ch.is_org_mandatory AS Is_Org_Mandatory,
                        ch.shared_team_ids AS Shared_Team_IDs,
                        ch.internal_team_ids AS Internal_Team_IDs,
                        ch.pending_connected_team_ids AS Pending_Connected_Team_IDs,
                        ch.is_member AS Is_Member,
                        ch.topic AS Topic,
                        ch.purpose AS Purpose,
                        ch.properties AS Properties,
                        ch.previous_names AS Previous_Names,
                        ch.num_members AS Members_Count,
                        ch.Sourcesystem_ID AS Sourcesystem_ID,
                        ch.Sourcesystem_Name AS Sourcesystem_Name,
                        ch.DW_Timestamp AS DW_Timestamp,
                        ch.Created_By AS Created_By,
                        ch.Created_Date AS Created_Date,
                        ch.Last_Modified_By AS Last_Modified_By,
                        ch.Last_Modified_Date AS Last_Modified_Date,
                        ch.Partition AS Partition
                        from bronze.slack_channels ch
                         '''
                )
except Exception as e:
    logger.info("unable to fetch data from bronze")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge To Target Table

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            DF_Bronze_Channels.createOrReplaceTempView('BronzeToSilver')
            Rowcount = DF_Bronze_Channels.count()
            TruncateQuery = "Truncate Table silver."+TableName
            spark.sql(TruncateQuery)
            MergeQuery = "INSERT INTO silver."+TableName+" TABLE BronzeToSilver;"
            spark.sql(MergeQuery.format(TableName))
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
