# Databricks notebook source
# MAGIC %md
# MAGIC ## HubSpot Deletes and Archived
# MAGIC * **Description:** To update archives and deletes from Source to Bronze table
# MAGIC * **Created Date:** 25/03/2025
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 25/03/2025
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Packages

# COMMAND ----------

# Import packages
from pprint import pprint
import time
import pandas as pd
import pytz
from datetime import datetime
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import hubspot
from hubspot import HubSpot

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling Utility Notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling Logger Notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling Error Variables

# COMMAND ----------

# Creating variables to store error log information
ErrorLogger = ErrorLogs("HubSpot_Companies_SourceToBronze-Deletes")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declare and Use Catalog

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retriving the API Keys for Two Instances

# COMMAND ----------

# Try block to retrieve secrets
try:
    logger.info("Getting secret values and forming the connection string")
    Shipper_Instance_Token = dbutils.secrets.get(scope='NFI_HubSpot_Secrets', key='tokenkey')
    Carrier_Instance_Token = dbutils.secrets.get(scope='NFI_HubSpot_Secrets', key='tokenkey-instance2')
    # Define Access Tokens
    access_tokens = [Shipper_Instance_Token, Carrier_Instance_Token]
except Exception as e:
    logger.error(f"Error retrieving secrets: {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect the Metadata information 

# COMMAND ----------

try: 
    Table_ID = 'H4'
    DF_Metadata = spark.sql(f"SELECT * FROM Metadata.MasterMetadata WHERE Tableid = '{Table_ID}' and IsActive='1'")
    Job_ID = DF_Metadata.select(col('Job_ID')).where(col('TableID') == Table_ID).collect()[0].Job_ID
    Notebook_ID = DF_Metadata.select(col('NB_ID')).where(col('TableID') == Table_ID).collect()[0].NB_ID
    TableName = DF_Metadata.select(col('DWHTableName')).where(col('TableID') == Table_ID).collect()[0].DWHTableName
    Zone = DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone
    MergeKey = DF_Metadata.filter(col("Tableid") == Table_ID).select("MergeKey").collect()[0][0]
    SourceSelectQuery = DF_Metadata.select(col('SourceSelectQuery')).where(col('TableID') == Table_ID).collect()[0].SourceSelectQuery 
except Exception as e:
    logger.info('Failed to Retrieve Metadata')
    print('Failed to Retrieve Metadata', str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions for Extraction

# COMMAND ----------

API_TOKENS = [Shipper_Instance_Token, Carrier_Instance_Token]
import requests
def get_owners_data(api_token, after=None, include_archived=False):
    url = "https://api.hubapi.com/crm/v3/owners/"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    params = {
        "limit": 100  # Maximum items per request
    }
    if after:
        params["after"] = after
    if include_archived:
        params["archived"] = "true"  # Explicitly request archived records

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def fetch_all_owners_data(api_tokens):
    archived_data = []
    non_archived_data = []

    for idx, token in enumerate(api_tokens):
        source_system_id = idx + 4  # Generate source system ID using index (starting from 1)

        def fetch_and_store_data(token, include_archived):
            data_list = archived_data if include_archived else non_archived_data
            after = None

            while True:
                try:
                    data = get_owners_data(token, after=after, include_archived=include_archived)
                    results = data.get("results", [])
                    if not results:
                        break
                    for owner in results:
                        entry = {
                            "id": owner.get("id"),
                            "email": owner.get("email"),
                            "type": owner.get("type"),
                            "first_name": owner.get("firstName"),
                            "last_name": owner.get("lastName"),
                            "user_Id": owner.get("userId"),
                            "usedIdIncludingInactive": owner.get("userIdIncludingInactive"),
                            "created_At": owner.get("createdAt"),
                            "updated_At": owner.get("updatedAt"),
                            "archived": owner.get("archived", False),
                            "archivedAt": owner.get("archivedAt"),
                            "sourceSystemId": source_system_id  # Add source system ID
                        }
                        data_list.append(entry)
                    paging = data.get("paging")
                    if paging and "next" in paging:
                        after = paging["next"]["after"]
                    else:
                        break
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching {'archived' if include_archived else 'non-archived'} data with token {token}: {e}")
                    raise RuntimeError(f"Job failed: Unable to get the Data: {e}")          

        # Fetch non-archived owners
        fetch_and_store_data(token, include_archived=False)
        # Fetch archived owners
        fetch_and_store_data(token, include_archived=True)

    return archived_data, non_archived_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraction and Updation

# COMMAND ----------

try:
    # Define the schema
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("type", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("user_Id", StringType(), True),
        StructField("usedIdIncludingInactive", StringType(), True),
        StructField("created_At", StringType(), True),
        StructField("updated_At", StringType(), True),
        StructField("archived", StringType(), True),
        StructField("archivedAt", StringType(), True),
        StructField("sourceSystemId", IntegerType(), True)
    ])

    try:
        # Fetch both archived and non-archived data
        archived_data, non_archived_data = fetch_all_owners_data(API_TOKENS)

        # Convert both lists to Spark DataFrames
        archived_df = spark.createDataFrame(pd.DataFrame(archived_data), schema)
        non_archived_df = spark.createDataFrame(pd.DataFrame(non_archived_data), schema)

        # Union both DataFrames
        final_df = archived_df.unionByName(non_archived_df)
        final_df = final_df.withColumn("Is_Deleted", lit(0))
        final_df = final_df.dropDuplicates()
        # Optionally create a SQL temporary view
        final_df.createOrReplaceTempView("SourceToBronze")
        
    except Exception as e:
        print(f"Error processing data: {e}")
except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Deletes -Target Table")
    logger.error(f"Exception: {Error_Statement}")
    print(e)

# COMMAND ----------

# Try block for processing metadata and data
try:
    # Check if table should be skipped
    AutoSkipperCheck = AutoSkipper(Table_ID, 'Bronze')
    if AutoSkipperCheck == 0:
        # Define SQL Update queries
        DeleteUpdateQuery = f"""
        UPDATE 
            Bronze.{TableName}
        SET 
            Bronze.{TableName}.Is_Deleted = 1
        WHERE 
            Bronze.{TableName}.{MergeKey} 
            IN (SELECT Target.{MergeKey} FROM SourceToBronze AS Source 
                RIGHT JOIN Bronze.{TableName} as Target 
                ON Source.{MergeKey} = Target.{MergeKey}
                WHERE 
                Target.Is_Deleted = 0 AND Source.{MergeKey} IS NULL)
        """

        ArchiveUpdateQuery = f"""
        MERGE INTO Bronze.{TableName} AS Target
        USING SourceToBronze AS Source
        ON Target.{MergeKey} = Source.{MergeKey}
        WHEN MATCHED AND Target.Is_Deleted = 0 AND Source.archived != Target.archived 
        THEN
        UPDATE SET 
            Target.archived = Source.archived
        """

        # Execute SQL Update
        spark.sql(DeleteUpdateQuery)
        spark.sql(ArchiveUpdateQuery)
        UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", "NULL", "NULL", 0, "Deletes -Target Table")
        logger.info(f"The deletes and archives have been successfully updated to the {TableName}")

except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Deletes -Target Table")
    logger.error(f"Exception: {Error_Statement}")
    print(e)
    raise RuntimeError(f"Job failed: Unable to Merge: {e}")

# COMMAND ----------

