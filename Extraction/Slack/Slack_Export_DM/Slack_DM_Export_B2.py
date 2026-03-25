# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from Source to Bronze as delta file
# MAGIC * **Created Date:** 19/09/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 19/09/2024
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
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import functions as F
import os
import json
from pyspark.sql import SparkSession
import logging
from pyspark.sql.types import StructType, StructField, StringType
from io import BytesIO
from decimal import Decimal
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
import zipfile
from azure.storage.blob import BlobServiceClient
import json
from pyspark.sql.functions import lit
import builtins 

# COMMAND ----------

# MAGIC %run 
# MAGIC "//Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC
# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Error Logger variables

# COMMAND ----------

# Creating variables to store error log information
try:
    ErrorLogger = ErrorLogs("Slack_Export_DM_B2_test")
    logger = ErrorLogger[0]
    p_logfile = ErrorLogger[1]
    p_filename = ErrorLogger[2]
except Exception as e:
    print("Unable to create variable:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC #Fetching Metadata details

# COMMAND ----------

try:
    DF_Metadata= spark.sql(
        "SELECT * FROM Metadata.MasterMetadata where TableID='SCD-B2' and IsActive='1' "
)
except Exception as e:
    logger.info("Unable to fetch details",e)
    print(e)
    

# COMMAND ----------


try:
    TableID  =  (DF_Metadata.select(col("TableID")).where(col("TableID") == "SCD-B2").collect()[0].TableID)
except Exception as e:
    logger.info("Unable to fetch details",e)
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

try:
    Job_ID = "Sl_DM_2"
    Notebook_ID = "SlNB_DM_2"
    Zone = "Bronze"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC #Connection String

# COMMAND ----------

# Retrieve the secrets from Azure Key Vault
try:
    tenant_id = dbutils.secrets.get(scope="NFI_Finance_Storage", key="TenantID")
    client_id = dbutils.secrets.get(scope="NFI_Finance_Storage", key="ClientID")
    client_secret = dbutils.secrets.get(scope="NFI_Finance_Storage", key="Client-SecretID")
except Exception as e:
    logger.info("unable to fetch creds from azure keyvault")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC # Retrieve data from source

# COMMAND ----------


# Create a credential using your AD credentials
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Storage account URL
storage_account_url = "https://dlsfindatacomeus1.blob.core.windows.net/"

# Create the Blob service client
blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)

def test_blob_service_connection(blob_service_client):
    try:
        # List all containers in the blob storage
        containers = blob_service_client.list_containers()
        print("Successfully connected! Here are the containers:")
        for container in containers:
            print(f"- {container.name}")
        return True
    except Exception as e:
        print(f"Failed to connect or list containers: {e}")
        return False
    raise

if test_blob_service_connection(blob_service_client):
    print("Connection test successful.")
    UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Azure Connection")
else:
    print("Connection test failed.")
    UpdateFailedStatus(TableID, 'Bronze')
    Error_Statement = "Failed to connect Azure"
    UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Azure Connection")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Retrieve folders list from source

# COMMAND ----------


# Your Azure AD credentials
credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

# Storage account URL and other necessary variables…

# Create the Blob service client
blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)

# Container and zip blob details
container_name = "slack-bulk-export"
zip_blob_name = "Slack_MPDM_Export.zip"

# Create a container client and download the zip file
container_client = blob_service_client.get_container_client(container_name)
zip_blob_client = container_client.get_blob_client(zip_blob_name)
zip_blob_data = zip_blob_client.download_blob().readall()

# Read the zip file into memory
zip_data = BytesIO(zip_blob_data)

# Extract folders
folders_starting_with_d = set()

with zipfile.ZipFile(zip_data, 'r') as zip_ref:
    for entry in zip_ref.infolist():
        # Extract all parts of the path, which could indicate a folder
        path_parts = entry.filename.split('/')
        # We check each part for a folder that starts with 'D'
        for part in path_parts:
            if part.startswith('D'):
                # If the part starts with 'D', we consider it a folder and add it to our set
                folders_starting_with_d.add('/'.join(path_parts[:path_parts.index(part) + 1]))

# The folders are already unique because we used a set, now we just sort them
sorted_folders_starting_with_d = sorted(folders_starting_with_d)

print(sorted_folders_starting_with_d)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Retrieve data from source

# COMMAND ----------

# Process files within each folder in the batch and return data as a list of dictionaries.
def process_single_folder(zip_ref, folder_name):
    # Yields message data as a generator instead of accumulating in memory
    folder_files = [
        info for info in zip_ref.infolist() 
        if not info.is_dir() and info.filename.startswith(folder_name + '/')
    ]
    for zip_info in folder_files:
        with zip_ref.open(zip_info) as file:
            print(f"Processing file: {zip_info.filename}")
            file_content = file.read()
            try:
                data = json.loads(file_content.decode('utf-8'))
                for message in data:
                    if message.get("subtype") != "bot_message" and message.get("user") != "USLACKBOT":
                        message["Channel_ID"] = folder_name
                        yield message
            except Exception as e:
                print(f"Error processing file {zip_info.filename}: {e}")
def merge_data(df, table_name, table_id):
    try:
        AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
        if AutoSkipperCheck == 0:
            MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
            MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue

            df = df.withColumn("Sourcesystem_Name", lit("Slack"))
            df = df.withColumn("DW_Timestamp", to_timestamp(df.ts.cast("double")))
            df = df.withColumn("Created_By", lit("Databricks"))
            df = df.withColumn("Created_Date", current_timestamp())
            df = df.withColumn("Last_Modified_By", lit("Databricks"))
            df = df.withColumn("Last_Modified_Date", current_timestamp())

            Hash_Key = [
                "user",
                "type",
                "edited",
                "client_msg_id",
                "text",
                "team",
                "thread_ts",
                "reply_count",
                "reply_users_count",
                "latest_reply",
                "reactions",
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
                "user_profile"
            ]

            df = df.withColumn("HashKey", md5(concat_ws("", *Hash_Key)))
            Merge_Key = ["Channel_ID", "ts"]
            df = df.withColumn("MergeKey", md5(concat_ws("", *Merge_Key)))
            df = df.withColumn("Conversation_Type", lit("Direct Message"))
            df.createOrReplaceTempView("VW_Conversation_Export")
            
            try:
                print('Loading ' + TableName)

                # Define the merge query
                MergeQuery = '''
                    MERGE INTO bronze.Slack_Temp_DM2 AS Target
                    USING VW_Conversation_Export AS Source
                    ON Target.MergeKey = Source.MergeKey
                    WHEN MATCHED AND Target.HashKey != Source.HashKey
                    THEN UPDATE SET
                        Target.Channel_ID = Source.Channel_ID,
                        Target.user = Source.user,
                        Target.type = Source.type,
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
                        Target.reactions = Source.reactions,
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
                        reactions,
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
                        Source.reactions,
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
                print(f'Successfully merged data for table: {table_name}')
                logger.info('Successfully loaded the ' + TableName + ' to bronze')
            except Exception as e:
                # Find the maximum date
                MaxDateQuery = "SELECT MAX({0}) AS Max_Date FROM VW_Conversation_Export".format(MaxLoadDateColumn)
                DF_MaxDate = spark.sql(MaxDateQuery)
                UpdateLastLoadDate(TableID, DF_MaxDate)
                UpdatePipelineStatusAndTime(TableID, 'Bronze')
    except Exception as e:
        Error_Statement = str(e).replace("'", "''")
        UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, Table_Name, Zone, "Failed", Error_Statement, "Impactable Issue", 1, "Merge-Target Table")
        logger.info('Failed for Bronze load')
        UpdateFailedStatus(TableID, 'Bronze')
        logger.info('Updated the Metadata for Failed Status ' + TableName)
        print('Unable to load ' + TableName)
        print(e)
    
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, Table_Name, Zone, "Succeeded", "NULL", "NULL", 0, "Merge-Target Table")
    logger.info('Successfully loaded the ' + TableName + ' to bronze')
  
schema = StructType([
                        StructField("Channel_ID", StringType(), nullable=True),
                        StructField("user", StringType(), nullable=True),
                        StructField("type", StringType(), nullable=True),
                        StructField("ts", StringType(), nullable=True),
                        StructField("subtype", StringType(), nullable=True),
                        StructField("client_msg_id", StringType(), nullable=True),
                        StructField("reactions", StringType(), nullable=True),
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
# Initialize Spark session

# Azure AD Credentials and Blob Storage Details
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)

# Download and process the blob
container_client = blob_service_client.get_container_client(container_name)
zip_blob_client = container_client.get_blob_client(zip_blob_name)
zip_blob_data = zip_blob_client.download_blob().readall()

zip_data = BytesIO(zip_blob_data)
total_folders = len(sorted_folders_starting_with_d)
# Calculate batch size
batch_size = -(-total_folders // 6)
start_index = batch_size
end_index = builtins.min(2*batch_size, total_folders) #Set this to desired range or len(sorted_folders) for all
second_batch = sorted_folders_starting_with_d[start_index:end_index]
print(f"Total folders: {total_folders}")
print(f"Batch size: {batch_size}")
print(f"Processing folders from index {start_index} to {end_index}")

# Open the zip file once and reuse the handle for processing each batch
with zipfile.ZipFile(zip_data, 'r') as zip_ref:
    for index, folder_name in enumerate(second_batch, start=1):
        df_folder = spark.createDataFrame(
            process_single_folder(zip_ref, folder_name), 
            schema
        )
        merge_data(df_folder, TableName, TableID)
        print(f"Completed processing and merging data for folder {index}: {folder_name}")

print("Finished processing all folders in the batch.")

# COMMAND ----------

