# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from Source to Bronze as delta file
# MAGIC * **Created Date:** 22/05/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 06/06/2024
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
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import functions as F

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

# MAGIC %md
# MAGIC ##Error log variables

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("Slack_Channels_Extraction_1hr_Refresh")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Use catalog
# MAGIC

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Metadata Details

# COMMAND ----------

# Getting the list of DMS tables in the dataframe that needs to be loaded into stage
try:
    DF_Metadata = spark.sql(
    "SELECT * FROM Metadata.MasterMetadata WHERE Tableid in ('SC1') and IsActive='1' "
)
except Exception as e:
    logger.info("Unable to fetch tableid from metadata")
    print(e)
try:
    TablesList = DF_Metadata.select(col("TableID")).collect()
    for TableID in TablesList:
        TableID = TableID.TableID
    TableName = (DF_Metadata.select(col("SourceTableName")).where(col("TableID") == TableID).collect()[0].SourceTableName)
    ErrorPath = (DF_Metadata.select(col("ErrorLogPath")).where(col("TableID") == TableID).collect()[0].ErrorLogPath)
    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == TableID).collect()[0].MergeKey) 
    LoadType = (DF_Metadata.select(col("LoadType")).where(col("TableID") == TableID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('UnixTime')).where(col('TableID') == TableID).collect()[0].UnixTime)
except Exception as e:
    logger.info("Unable to fetch details from metadata")
    print(e)
   

# COMMAND ----------

try:
    Job_ID = "JOB_1"
    Notebook_ID = "NB_1"
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
    logger.info("Unable to create connection string for instance 1")
    print(e)

# COMMAND ----------

try:
        all_channels = []
        next_cursor = ""
        while True:
                time.sleep(0.1)
                channels_response = client1.conversations_list(cursor=next_cursor,limit=999)
# print(channels_response))
# Check if the API call was successful
                if channels_response["ok"]:
                    channels = channels_response["channels"]
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
                StructField("name", StringType(), nullable=True),
                StructField("is_channel", StringType(), nullable=True),
                StructField("is_group", StringType(), nullable=True),
                StructField("is_im", StringType(), nullable=True),
                StructField("is_mpim", StringType(), nullable=True),
                StructField("is_private", StringType(), nullable=True),
                StructField("created", StringType(), nullable=True),
                StructField("is_archived", StringType(), nullable=True),
                StructField("is_general", StringType(), nullable=True),
                StructField("unlinked", StringType(), nullable=True),
                StructField("name_normalized", StringType(), nullable=True),
                StructField("is_shared", StringType(), nullable=True),
                StructField("is_org_shared", StringType(), nullable=True),
                StructField("is_pending_ext_shared", StringType(), nullable=True),
                StructField("pending_shared", StringType(), nullable=True),
                StructField("context_team_id", StringType(), nullable=True),
                StructField("updated", StringType(), nullable=True),
                StructField("parent_conversation", StringType(), nullable=True),
                StructField("creator", StringType(), nullable=True),
                StructField("is_moved", StringType(), nullable=True),
                StructField("is_ext_shared", StringType(), nullable=True),
                StructField("enterprise_id", StringType(), nullable=True),
                StructField("is_global_shared", StringType(), nullable=True),
                StructField("is_org_default", StringType(), nullable=True),
                StructField("is_org_mandatory", StringType(), nullable=True),
                StructField("shared_team_ids", StringType(), nullable=True),
                StructField("internal_team_ids", StringType(), nullable=True),
                StructField("pending_connected_team_ids", StringType(), nullable=True),
                StructField("is_member", StringType(), nullable=True),
                StructField("topic", StringType(), nullable=True),
                StructField("purpose", StringType(), nullable=True),
                StructField("properties", StringType(), nullable=True),
                StructField("previous_names", StringType(), nullable=True),
                StructField("num_members", StringType(), nullable=True),
])

# COMMAND ----------

try:
# Converting the Pandas DataFrame to Spark DataFrame
    DF_Channel_1 = spark.createDataFrame(all_channels, schema)
    DF_Channel_1 = DF_Channel_1.withColumn("Sourcesystem_ID", lit(6))
    DF_Channel_1 = DF_Channel_1.withColumn("Sourcesystem_Name", lit("Slack-Brokerage"))
    DF_Channel_1 = DF_Channel_1.withColumn("DW_Timestamp", to_timestamp(DF_Channel_1.created.cast("double")))
    DF_Channel_1 = DF_Channel_1.withColumn("Created_By",lit("Databricks")) 
    DF_Channel_1 = DF_Channel_1.withColumn("Created_Date", current_timestamp()) 
    DF_Channel_1 = DF_Channel_1.withColumn("Last_Modified_By", lit("Databricks")) 
    DF_Channel_1 = DF_Channel_1.withColumn("Last_Modified_Date", current_timestamp()) 
        
except Exception as e: 
    logger.info('Unable to load data into dataFrame1')
    print(e)

# COMMAND ----------

try:
        DF_Channel_1.count()
except Exception as e:
    schema = StructType([
                StructField("id", StringType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("is_channel", StringType(), nullable=True),
                StructField("is_group", StringType(), nullable=True),
                StructField("is_im", StringType(), nullable=True),
                StructField("is_mpim", StringType(), nullable=True),
                StructField("is_private", StringType(), nullable=True),
                StructField("created", StringType(), nullable=True),
                StructField("is_archived", StringType(), nullable=True),
                StructField("is_general", StringType(), nullable=True),
                StructField("unlinked", StringType(), nullable=True),
                StructField("name_normalized", StringType(), nullable=True),
                StructField("is_shared", StringType(), nullable=True),
                StructField("is_org_shared", StringType(), nullable=True),
                StructField("is_pending_ext_shared", StringType(), nullable=True),
                StructField("pending_shared", StringType(), nullable=True),
                StructField("context_team_id", StringType(), nullable=True),
                StructField("updated", StringType(), nullable=True),
                StructField("parent_conversation", StringType(), nullable=True),
                StructField("creator", StringType(), nullable=True),
                StructField("is_moved", StringType(), nullable=True),
                StructField("is_ext_shared", StringType(), nullable=True),
                StructField("shared_team_ids", StringType(), nullable=True),
                StructField("internal_team_ids", StringType(), nullable=True),
                StructField("pending_connected_team_ids", StringType(), nullable=True),
                StructField("is_member", StringType(), nullable=True),
                StructField("topic", StringType(), nullable=True),
                StructField("purpose", StringType(), nullable=True),
                StructField("properties", StringType(), nullable=True),
                StructField("previous_names", StringType(), nullable=True),
                StructField("num_members", StringType(), nullable=True),
])
    dataFrame1 = spark.createDataFrame([], schema=schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Create connection string for second instance

# COMMAND ----------

try:
    api_token = dbutils.secrets.get(scope="NFI_Slack_Secrets", key="ILS-Token1")
    client2 = WebClient(token=api_token)
except Exception as e:
    logger.info("Unable to create connection string for instance 2")
    print(e)

# COMMAND ----------

try:
        all_channels = []
        next_cursor = ""
        while True:
                time.sleep(0.1)
                channels_response = client2.conversations_list(cursor=next_cursor,limit=999)
# Check if the API call was successful
                if channels_response["ok"]:
                    channels = channels_response["channels"]
                    all_channels.extend(channels)
                    next_cursor = channels_response.get("response_metadata", {}).get("next_cursor")

                    if not next_cursor:
                        break
except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Slack-ILS")
    logger.info(f"Unable to connect to ILS Slack {e}")
    print(e)
UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Slack-ILS")
schema = StructType([
                StructField("id", StringType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("is_channel", StringType(), nullable=True),
                StructField("is_group", StringType(), nullable=True),
                StructField("is_im", StringType(), nullable=True),
                StructField("is_mpim", StringType(), nullable=True),
                StructField("is_private", StringType(), nullable=True),
                StructField("created", StringType(), nullable=True),
                StructField("is_archived", StringType(), nullable=True),
                StructField("is_general", StringType(), nullable=True),
                StructField("unlinked", StringType(), nullable=True),
                StructField("name_normalized", StringType(), nullable=True),
                StructField("is_shared", StringType(), nullable=True),
                StructField("is_org_shared", StringType(), nullable=True),
                StructField("is_pending_ext_shared", StringType(), nullable=True),
                StructField("pending_shared", StringType(), nullable=True),
                StructField("context_team_id", StringType(), nullable=True),
                StructField("updated", StringType(), nullable=True),
                StructField("parent_conversation", StringType(), nullable=True),
                StructField("creator", StringType(), nullable=True),
                StructField("is_moved", StringType(), nullable=True),
                StructField("is_ext_shared", StringType(), nullable=True),
                StructField("enterprise_id", StringType(), nullable=True),
                StructField("is_global_shared", StringType(), nullable=True),
                StructField("is_org_default", StringType(), nullable=True),
                StructField("is_org_mandatory", StringType(), nullable=True),
                StructField("shared_team_ids", StringType(), nullable=True),
                StructField("internal_team_ids", StringType(), nullable=True),
                StructField("pending_connected_team_ids", StringType(), nullable=True),
                StructField("is_member", StringType(), nullable=True),
                StructField("topic", StringType(), nullable=True),
                StructField("purpose", StringType(), nullable=True),
                StructField("properties", StringType(), nullable=True),
                StructField("previous_names", StringType(), nullable=True),
                StructField("num_members", StringType(), nullable=True),
])

# COMMAND ----------

try:
    # Converting the Pandas DataFrame to Spark DataFrame
    DF_Channel_2 = spark.createDataFrame(all_channels , schema)
    DF_Channel_2 = DF_Channel_2.withColumn("Sourcesystem_ID", lit("7"))
    DF_Channel_2 = DF_Channel_2.withColumn("Sourcesystem_Name", lit("Slack-ILS"))
    DF_Channel_2 = DF_Channel_2.withColumn("DW_Timestamp", to_timestamp(DF_Channel_2.created.cast("double")))
    DF_Channel_2 = DF_Channel_2.withColumn("Created_By",lit("Databricks")) 
    DF_Channel_2 = DF_Channel_2.withColumn("Created_Date", current_timestamp()) 
    DF_Channel_2 = DF_Channel_2.withColumn("Last_Modified_By", lit("Databricks")) 
    DF_Channel_2 = DF_Channel_2.withColumn("Last_Modified_Date", current_timestamp())
    

except Exception as e: 
    logger.info('Unable to load data into dataFrame1')
    print(e)

# COMMAND ----------

try:
    DF_Channel_2.count()
except Exception as e:
    schema = StructType([
                StructField("id", StringType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("is_channel", BooleanType(), nullable=True),
                StructField("is_group", BooleanType(), nullable=True),
                StructField("is_im", BooleanType(), nullable=True),
                StructField("is_mpim", BooleanType(), nullable=True),
                StructField("is_private", BooleanType(), nullable=True),
                StructField("created", StringType(), nullable=True),
                StructField("is_archived", BooleanType(), nullable=True),
                StructField("is_general", BooleanType(), nullable=True),
                StructField("unlinked", StringType(), nullable=True),
                StructField("name_normalized", StringType(), nullable=True),
                StructField("is_shared", BooleanType(), nullable=True),
                StructField("is_org_shared", BooleanType(), nullable=True),
                StructField("is_pending_ext_shared", BooleanType(), nullable=True),
                StructField("pending_shared", StringType(), nullable=True),
                StructField("context_team_id", StringType(), nullable=True),
                StructField("updated", StringType(), nullable=True),
                StructField("parent_conversation", StringType(), nullable=True),
                StructField("creator", StringType(), nullable=True),
                StructField("is_moved", StringType(), nullable=True),
                StructField("is_ext_shared", BooleanType(), nullable=True),
                StructField("shared_team_ids", StringType(), nullable=True),
                StructField("internal_team_ids", StringType(), nullable=True),
                StructField("pending_connected_team_ids", StringType(), nullable=True),
                StructField("is_member", BooleanType(), nullable=True),
                StructField("topic", StringType(), nullable=True),
                StructField("purpose", StringType(), nullable=True),
                StructField("previous_names", StringType(), nullable=True),
                StructField("num_members", StringType(), nullable=True),
    ])
    dataFrame2 = spark.createDataFrame([], schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Perform Union for both the instance

# COMMAND ----------

try:
        DF_Channel=DF_Channel_1.union(DF_Channel_2)             
        DF_Channel_Ordered = DF_Channel.select(col("id"),col("name"),
col("is_channel"),
col("is_group"),
col("is_im"),
col("is_mpim"),
col("is_private"),
col("created"),
col("is_archived"),
col("is_general"),
col("unlinked"),
col("name_normalized"),
col("is_shared"),
col("is_org_shared"),
col("is_pending_ext_shared"),
col("pending_shared"),
col("context_team_id"),
col("updated"),
col("parent_conversation"),
col("creator"),
col("is_moved"),
col("is_ext_shared"),
col("enterprise_id"),
col("is_global_shared"),
col("is_org_default"),
col("is_org_mandatory"),
col("shared_team_ids"),
col("internal_team_ids"),
col("pending_connected_team_ids"),
col("is_member"),
col("topic"),
col("purpose"),
col("properties"),
col("previous_names"),
col("num_members"),
col("Sourcesystem_ID"),
col("Sourcesystem_Name"),
col("DW_Timestamp"),
col("Created_By"),
col("Created_Date"),
col("Last_Modified_By"),
col("Last_Modified_Date")
)

        DF_VW_Channel=DF_Channel_Ordered.createOrReplaceTempView("VW_Channel")
except Exception as e:
    logger.info("unable to union the dataframe")
    print(e)


# COMMAND ----------

try:
    window = Window.orderBy("id") 

    # Add row numbers
    DF_Partition = DF_Channel_Ordered.withColumn("row_number", F.row_number().over(window))

    # Add labels for every 10 records
    DF_Partition = DF_Partition.withColumn("Partition", F.floor((F.col("row_number") - 1) / 100))
    DF_Partitioned_Channels= DF_Partition.drop("row_number")
    DF_Slack_Channel =DF_Partitioned_Channels.dropDuplicates(["id","name"])
        

except Exception as e:
    logger.info(f"Unable to partition DataFrame: {e}")
    print(e)


# COMMAND ----------


try:
    DF_Slack_Channel.createOrReplaceTempView("VW_Source_Channels")
except Exception as e:
    logger.info(f"Unable to create view: {e}")
    print(e)

# COMMAND ----------

DF_Target_Channels = spark.sql(''' select * from bronze.slack_channels ''')
try:
    DF_Target_Channels=DF_Target_Channels.createOrReplaceTempView("VW_Target_Channels")
except Exception as e:
    logger.info(f"Unable to create view: {e}")
    print(e)

# COMMAND ----------

try:
    DF_Slack_Channels = spark.sql('''
SELECT
  COALESCE(tc.id, sc.id) AS id,
  COALESCE(sc.name, tc.name) AS name,
  CASE 
    WHEN sc.id IS NULL AND tc.id IS NOT NULL THEN 'True'
    ELSE 'False'
  END AS Is_Deleted,
  COALESCE(sc.is_channel, tc.is_channel) AS is_channel,
  COALESCE(sc.is_group, tc.is_group) AS is_group ,
  COALESCE(sc.is_im, tc.is_im) AS is_im ,
  COALESCE(sc.is_mpim, tc.is_mpim) AS is_mpim,
  COALESCE(sc.is_private, tc.is_private) AS is_private ,
  COALESCE(sc.created, tc.created) AS created,
  COALESCE(sc.is_archived, tc.is_archived) AS is_archived,
  COALESCE(sc.is_general, tc.is_general) AS is_general ,
  COALESCE(sc.unlinked, tc.unlinked) AS unlinked,
  COALESCE(sc.name_normalized, tc.name_normalized) AS name_normalized ,
  COALESCE(sc.is_shared, tc.is_shared)AS is_shared,
  COALESCE(sc.is_org_shared, tc.is_org_shared) AS is_org_shared,
  COALESCE(sc.is_pending_ext_shared, tc.is_pending_ext_shared) AS is_pending_ext_shared,
  COALESCE(sc.pending_shared, tc.pending_shared) AS pending_shared,
  COALESCE(sc.context_team_id, tc.context_team_id)AS context_team_id ,
  COALESCE(sc.updated, tc.updated) AS updated,
  COALESCE(sc.parent_conversation, tc.parent_conversation)  AS parent_conversation,
  COALESCE(sc.creator, tc.creator) AS creator ,
  COALESCE(sc.is_moved, tc.is_moved) AS is_moved,
  COALESCE(sc.is_ext_shared, tc.is_ext_shared) AS is_ext_shared ,
  COALESCE(sc.enterprise_id, tc.enterprise_id)AS enterprise_id  ,
  COALESCE(sc.is_global_shared, tc.is_global_shared) AS is_global_shared,
  COALESCE(sc.is_org_default, tc.is_org_default) AS is_org_default,
  COALESCE(sc.is_org_mandatory, tc.is_org_mandatory)  AS is_org_mandatory,
  COALESCE(sc.shared_team_ids, tc.shared_team_ids)  As shared_team_ids,
  COALESCE(sc.internal_team_ids, tc.internal_team_ids)  AS internal_team_ids,
  COALESCE(sc.pending_connected_team_ids, tc.pending_connected_team_ids) As pending_connected_team_ids,
  COALESCE(sc.is_member, tc.is_member) AS is_member ,
  COALESCE(sc.topic, tc.topic) AS topic ,
  COALESCE(sc.purpose, tc.purpose) AS purpose ,
  COALESCE(sc.properties, tc.properties) AS properties,
  COALESCE(sc.previous_names, tc.previous_names) AS previous_names,
  COALESCE(sc.num_members, tc.num_members)  AS num_members,
  COALESCE(sc.Sourcesystem_ID, tc.Sourcesystem_ID)  AS Sourcesystem_ID,
  COALESCE(sc.Sourcesystem_Name, tc.Sourcesystem_Name) AS Sourcesystem_Name ,
  COALESCE(sc.DW_Timestamp, tc.DW_Timestamp)  As DW_Timestamp,
  COALESCE(sc.Created_By, tc.Created_By) AS Created_By,
  COALESCE(sc.Created_Date, tc.Created_Date)  AS Created_Date,
  COALESCE(sc.Last_Modified_By, tc.Last_Modified_By)  AS Last_Modified_By,
  COALESCE(sc.Last_Modified_Date, tc.Last_Modified_Date) AS Last_Modified_Date,
  COALESCE(sc.partition, tc.partition) AS partition
FROM
  VW_Target_Channels tc
FULL OUTER JOIN
  VW_Source_Channels sc
ON tc.id = sc.id
''')
except Exception as e:
  logger.info("unable to update logs")
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge to Target table

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            DF_Slack_Channels.createOrReplaceTempView('SourceToBronze')
            Rowcount = DF_Slack_Channels.count()
            MergeQuery = '''
                MERGE INTO bronze.Slack_Channels AS Target
                USING SourceToBronze AS Source
                ON Target.id=Source.id 
                WHEN MATCHED THEN UPDATE SET
                    Target.id = Source.id,
		            Target.name = Source.name,
                    Target.Is_Deleted = Source.Is_Deleted,
                    Target.is_channel = Source.is_channel,
                    Target.is_group = Source.is_group,
                    Target.is_im = Source.is_im,
                    Target.is_mpim = Source.is_mpim,
                    Target.is_private = Source.is_private,
                    Target.created = Source.created,
                    Target.is_archived = Source.is_archived,
                    Target.is_general = Source.is_general,
                    Target.unlinked = Source.unlinked,
                    Target.name_normalized = Source.name_normalized,
                    Target.is_shared = Source.is_shared,
                    Target.is_org_shared = Source.is_org_shared,
                    Target.is_pending_ext_shared = Source.is_pending_ext_shared,
                    Target.pending_shared = Source.pending_shared,
                    Target.context_team_id = Source.context_team_id,
                    Target.updated = Source.updated,
                    Target.parent_conversation = Source.parent_conversation,
                    Target.creator = Source.creator,
                    Target.is_moved = Source.is_moved,
                    Target.is_ext_shared = Source.is_ext_shared,
                    Target.enterprise_id = Source.enterprise_id,
                    Target.is_global_shared = Source.is_global_shared,
                    Target.is_org_default = Source.is_org_default,
                    Target.is_org_mandatory = Source.is_org_mandatory,
                    Target.shared_team_ids = Source.shared_team_ids,
                    Target.internal_team_ids = Source.internal_team_ids,
                    Target.pending_connected_team_ids = Source.pending_connected_team_ids,
                    Target.is_member = Source.is_member,
                    Target.topic = Source.topic,
                    Target.purpose = Source.purpose,
                    Target.properties = Source.properties,
                    Target.previous_names = Source.previous_names,
                    Target.num_members = Source.num_members,
                    Target.Sourcesystem_ID = Source.Sourcesystem_ID,
                    Target.Sourcesystem_Name = Source.Sourcesystem_Name,
                    Target.DW_Timestamp = Source.DW_Timestamp,
                    Target.Created_By = Source.Created_By,
                    Target.Created_Date = Source.Created_Date,
                    Target.Last_Modified_By = Source.Last_Modified_By,
                    Target.Last_Modified_Date = Source.Last_Modified_Date,
                    Target.Partition = Source.Partition
                WHEN NOT MATCHED THEN INSERT (
                    id,
                    name,
                    Is_Deleted,
                    is_channel,
                    is_group,
                    is_im,
                    is_mpim,
                    is_private,
                    created,
                    is_archived,
                    is_general,
                    unlinked,
                    name_normalized,
                    is_shared,
                    is_org_shared,
                    is_pending_ext_shared,
                    pending_shared,
                    context_team_id,
                    updated,
                    parent_conversation,
                    creator,
                    is_moved,
                    is_ext_shared,
                    enterprise_id,
                    is_global_shared,
                    is_org_default,
                    is_org_mandatory,
                    shared_team_ids,
                    internal_team_ids,
                    pending_connected_team_ids,
                    is_member,
                    topic,
                    purpose,
                    properties,
                    previous_names,
                    num_members,
                    Sourcesystem_ID,
                    Sourcesystem_Name,
                    DW_Timestamp,
                    Created_By,
                    Created_Date,
                    Last_Modified_By,
                    Last_Modified_Date,
                    Partition
                ) VALUES (
                    Source.id,
                    Source.name,
                    Source.Is_Deleted,
                    Source.is_channel,
                    Source.is_group,
                    Source.is_im,
                    Source.is_mpim,
                    Source.is_private,
                    Source.created,
                    Source.is_archived,
                    Source.is_general,
                    Source.unlinked,
                    Source.name_normalized,
                    Source.is_shared,
                    Source.is_org_shared,
                    Source.is_pending_ext_shared,
                    Source.pending_shared,
                    Source.context_team_id,
                    Source.updated,
                    Source.parent_conversation,
                    Source.creator,
                    Source.is_moved,
                    Source.is_ext_shared,
                    Source.enterprise_id,
                    Source.is_global_shared,
                    Source.is_org_default,
                    Source.is_org_mandatory,
                    Source.shared_team_ids,
                    Source.internal_team_ids,
                    Source.pending_connected_team_ids,
                    Source.is_member,
                    Source.topic,
                    Source.purpose,
                    Source.properties,
                    Source.previous_names,
                    Source.num_members,
                    Source.Sourcesystem_ID,
                    Source.Sourcesystem_Name,
                    Source.DW_Timestamp,
                    Source.Created_By,
                    Source.Created_Date,
                    Source.Last_Modified_By,
                    Source.Last_Modified_Date,
                    Source.Partition
                )'''
            spark.sql(MergeQuery)
            # UpdateQuery = update table bronze.slack_channels 
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
        UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Merge-Target Table")
except Exception as e:
    logger.info("table is already loaded")
    print(e)
