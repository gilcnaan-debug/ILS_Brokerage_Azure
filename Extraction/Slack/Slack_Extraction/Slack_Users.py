# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from Source to Bronze as delta file
# MAGIC * **Created Date:** 22/05/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 27/05/2024
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

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initializing Utilities

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initializing Logger

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("SlackUsersdemo_Extraction_1hr_Refresh")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fetch Metadata Details

# COMMAND ----------

# Getting the list of DMS tables in the dataframe that needs to be loaded into stage
try:
    DF_Metadata = spark.sql(
    "SELECT * FROM Metadata.MasterMetadata WHERE Tableid in ('SU1') and IsActive='1' "
)
except Exception as e:
    logger.info("Unable to fetch tableid from metadata")
    print(e)
try:
    TablesList = DF_Metadata.select(col("TableID")).collect()
    for TableID in TablesList:
        TableID = TableID.TableID
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
    Job_ID = "JOB_3"
    Notebook_ID = "NB_3"
    Zone = "Bronze"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Connection String for First Instance

# COMMAND ----------

try:
    api_token = dbutils.secrets.get(scope="NFI_Slack_Secrets", key="Brokerage-Token1")
    client1 = WebClient(token=api_token)
except Exception as e:
    logger.error(f"Unable to connect to Brokerage Slack {e}")
    print(e)

# COMMAND ----------

try:
    all_channels = []
    next_cursor = ""
    while True:
        channels_response = client1.users_list(cursor=next_cursor,limit=999)
# Check if the API call was successful
        if channels_response["ok"]:
            channels = channels_response["members"]
            all_channels.extend(channels)
            next_cursor = channels_response.get("response_metadata", {}).get("next_cursor")
            if not next_cursor:
                break
except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Slack-Bokerage")
    logger.info(f"Unable to connect to Broerage Slack {e}")
    print(e)

UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Slack-Bokerage")
schema = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("team_id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("deleted", StringType(), nullable=True),
    StructField("color", StringType(), nullable=True),
    StructField("real_name", StringType(), nullable=True),
    StructField("tz", StringType(), nullable=True),
    StructField("tz_label", StringType(), nullable=True),
    StructField("tz_offset", StringType(), nullable=True),
    StructField('profile', StringType(), nullable=True),
    StructField("is_admin", StringType(), nullable=True),
    StructField("is_owner", StringType(), nullable=True),
    StructField("is_primary_owner", StringType(), nullable=True),
    StructField("is_restricted", StringType(), nullable=True),
    StructField("is_ultra_restricted", StringType(), nullable=True),
    StructField("is_bot", StringType(), nullable=True),
    StructField("is_app_user", StringType(), nullable=True),
    StructField("updated", StringType(), nullable=True),
    StructField("is_email_confirmed", StringType(), nullable=True),
    StructField("who_can_share_contact_card", StringType(), nullable=True),
])

# COMMAND ----------

try:
    DF_User1 = spark.createDataFrame(all_channels, schema)
    DF_User1 = DF_User1.withColumn("Sourcesystem_ID", lit(6))
    DF_User1 = DF_User1.withColumn("Sourcesystem_Name", lit("Slack-Brokerage"))
    DF_User1 = DF_User1.withColumn("DW_Timestamp", to_timestamp(DF_User1.updated.cast("double")))
    DF_User1 = DF_User1.withColumn("Created_By",lit("Databricks")) 
    DF_User1 = DF_User1.withColumn("Created_Date", current_timestamp()) 
    DF_User1 = DF_User1.withColumn("Last_Modified_By", lit("Databricks")) 
    DF_User1 = DF_User1.withColumn("Last_Modified_Date", current_timestamp()) 
except Exception as e: 
        logger.info('Unable to load data into dataFrame1')
        print(e)

# COMMAND ----------

try:
        DF_User1.count()
except Exception as e:
    schema = StructType([
StructField("id", StringType(), nullable=True),
StructField("team_id", StringType(), nullable=True),
StructField("name", StringType(), nullable=True),
StructField("deleted", StringType(), nullable=True),
StructField("color", StringType(), nullable=True),
StructField("real_name", StringType(), nullable=True),
StructField("tz", StringType(), nullable=True),
StructField("tz_label", StringType(), nullable=True),
StructField("tz_offset", StringType(), nullable=True),
StructField('profile', StringType(), nullable=True),
StructField("is_admin", StringType(), nullable=True),
StructField("is_owner", StringType(), nullable=True),
StructField("is_primary_owner", StringType(), nullable=True),
StructField("is_restricted", StringType(), nullable=True),
StructField("is_ultra_restricted", StringType(), nullable=True),
StructField("is_bot", StringType(), nullable=True),
StructField("is_app_user", StringType(), nullable=True),
StructField("updated", StringType(), nullable=True),
StructField("is_email_confirmed", StringType(), nullable=True),
StructField("who_can_share_contact_card", StringType(), nullable=True),
])
    dataFrame1 = spark.createDataFrame([], schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Connection string for Second Instance

# COMMAND ----------

try:
    api_token = dbutils.secrets.get(scope="NFI_Slack_Secrets", key="ILS-Token1")
    client2 = WebClient(token=api_token)
except Exception as e:
    logger.info(f"Unable to connect to ILS-Slack {e}")
    print(e)

# COMMAND ----------

try:
    all_channels = []
    next_cursor = ""
    while True:
            channels_response = client2.users_list(cursor=next_cursor,limit=999)
# Check if the API call was successful
            if channels_response["ok"]:
                channels = channels_response["members"]
                all_channels.extend(channels)
                next_cursor = channels_response.get("response_metadata", {}).get("next_cursor")
                if not next_cursor:
                    break

except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Slack-ILS")
    logger.info(f"Unable to connect to Broerage Slack {e}")
    print(e)

UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Slack-ILS")
schema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("team_id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("deleted", StringType(), nullable=True),
        StructField("color", StringType(), nullable=True),
        StructField("real_name", StringType(), nullable=True),
        StructField("tz", StringType(), nullable=True),
        StructField("tz_label", StringType(), nullable=True),
        StructField("tz_offset", StringType(), nullable=True),
        StructField('profile', StringType(), nullable=True),
        StructField("is_admin", StringType(), nullable=True),
        StructField("is_owner", StringType(), nullable=True),
        StructField("is_primary_owner", StringType(), nullable=True),
        StructField("is_restricted", StringType(), nullable=True),
        StructField("is_ultra_restricted", StringType(), nullable=True),
        StructField("is_bot", StringType(), nullable=True),
        StructField("is_app_user", StringType(), nullable=True),
        StructField("updated", StringType(), nullable=True),
        StructField("is_email_confirmed", StringType(), nullable=True),
        StructField("who_can_share_contact_card", StringType(), nullable=True),
    ])

# COMMAND ----------

try:
    DF_User2 = spark.createDataFrame(all_channels, schema)
    DF_User2 = DF_User2.withColumn("Sourcesystem_ID", lit(7))
    DF_User2 = DF_User2.withColumn("Sourcesystem_Name", lit("Slack-ILS"))
    DF_User2 = DF_User2.withColumn("DW_Timestamp", to_timestamp(DF_User2.updated.cast("double")))
    DF_User2 = DF_User2.withColumn("Created_By",lit("Databricks")) 
    DF_User2 = DF_User2.withColumn("Created_Date", current_timestamp()) 
    DF_User2 = DF_User2.withColumn("Last_Modified_By", lit("Databricks")) 
    DF_User2 = DF_User2.withColumn("Last_Modified_Date", current_timestamp())

except Exception as e: 
    logger.info('Unable to load data into dataFrame1')
    print(e)

# COMMAND ----------

try:
    DF_User2.count()
except Exception as e:
    schema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("team_id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("deleted", StringType(), nullable=True),
        StructField("color", StringType(), nullable=True),
        StructField("real_name", StringType(), nullable=True),
        StructField("tz", StringType(), nullable=True),
        StructField("tz_label", StringType(), nullable=True),
        StructField("tz_offset", StringType(), nullable=True),
        StructField('profile', StringType(), nullable=True),
        StructField("is_admin", StringType(), nullable=True),
        StructField("is_owner", StringType(), nullable=True),
        StructField("is_primary_owner", StringType(), nullable=True),
        StructField("is_restricted", StringType(), nullable=True),
        StructField("is_ultra_restricted", StringType(), nullable=True),
        StructField("is_bot", StringType(), nullable=True),
        StructField("is_app_user", StringType(), nullable=True),
        StructField("updated", StringType(), nullable=True),
        StructField("is_email_confirmed", StringType(), nullable=True),
        StructField("who_can_share_contact_card", StringType(), nullable=True),
    ])
    dataFrame2 = spark.createDataFrame([], schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Peform Union and Merge to Target Table

# COMMAND ----------

try:
    DF_User=DF_User1.union(DF_User2)
    DF_User=DF_User.dropDuplicates(["id","name"])
except Exception as e:
    logger.info("unable to union Dataframe for DF_User")

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            DF_User.createOrReplaceTempView('SourceToBronze')
            Rowcount = DF_User.count()
            TruncateQuery = "Truncate Table bronze."+TableName
            spark.sql(TruncateQuery)
            MergeQuery = "INSERT INTO bronze."+TableName+" (id,team_id,name,deleted,color,real_name,tz,tz_label,tz_offset,profile,is_admin,is_owner,is_primary_owner,is_restricted,is_ultra_restricted,is_bot,is_app_user,updated,is_email_confirmed,who_can_share_contact_card,Sourcesystem_ID,Sourcesystem_Name,DW_Timestamp,Created_By,Created_Date,Last_Modified_By,Last_Modified_Date) TABLE SourceToBronze; "
            spark.sql(MergeQuery.format(TableName))
            UpdatePipelineStatusAndTime(TableID,'Bronze')
            logger.info('Successfully loaded the'+TableName+'to Bronze')
        except Exception as e:
            Error_Statement = str(e).replace("'", "''")
            UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Merge-Target Table")
            logger.info('Failed for Bronze load')
            UpdateFailedStatus(TableID,'Bronze')
            logger.info('Updated the Metadata for Failed Status '+TableName)
            print('Unable to load '+TableName)
            print(e)
except Exception as e:
    logger.info("table is already loaded")
    print(e)
UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Merge-Target Table")