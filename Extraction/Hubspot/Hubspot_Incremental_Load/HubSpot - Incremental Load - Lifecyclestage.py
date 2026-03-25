# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract Data from HubSpot to Bronze as delta file
# MAGIC * **Created Date:** 27/03/2025
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 27/03/2025
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Packages

# COMMAND ----------

# Importing packages
import pandas as pd
import time
from datetime import datetime
import math
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from hubspot import HubSpot
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling Utilities and Loggers

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaring Error Loggers

# COMMAND ----------

# Creating variables to store error log information
ErrorLogger = ErrorLogs("HubSpot - Owners Incremental Load")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Secrets from Key Vaults

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

try:
    logger.info("Getting secret values and forming the connection string")
    Shipper_Instance_Token = dbutils.secrets.get(scope="NFI_Hubspot_Secrets", key="tokenkey")
    Carrier_Instance_Token = dbutils.secrets.get(scope="NFI_Hubspot_Secrets", key="tokenkey-instance2")
except Exception as e:
    logger.error(f"Error in retrieving secrets: {str(e)}")
    print(str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Metadata Details

# COMMAND ----------

try: 
    Table_ID = 'H4'
    DF_Metadata = spark.sql(f"SELECT * FROM Metadata.MasterMetadata WHERE Tableid = '{Table_ID}' and IsActive='1'")
    Job_ID = DF_Metadata.select(col('JOB_ID')).where(col('TableID') == Table_ID).collect()[0].JOB_ID
    Notebook_ID = DF_Metadata.select(col('NB_ID')).where(col('TableID') == Table_ID).collect()[0].NB_ID
    TableName = DF_Metadata.select(col('DWHTableName')).where(col('TableID') == Table_ID).collect()[0].DWHTableName
    MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateColumn
    MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateValue
    Zone = DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone
    MergeKey = DF_Metadata.filter(col("Tableid") == Table_ID).select("MergeKey").collect()[0][0]
    LoadType = DF_Metadata.select(col('LoadType')).where(col('TableID') == Table_ID).collect()[0].LoadType
    SourceSelectQuery = DF_Metadata.select(col('SourceSelectQuery')).where(col('TableID') == Table_ID).collect()[0].SourceSelectQuery 
except Exception as e:
    logger.info('Failed to Retrieve Metadata')
    print('Failed to Retrieve Metadata', str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Functions for Extraction

# COMMAND ----------

API_TOKENS = [Shipper_Instance_Token, Carrier_Instance_Token]


def get_owners_data(api_token, after=None, include_archived=False):
    url = "https://api.hubapi.com/crm/v3/owners/"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    params = {"limit": 100}  # Maximum items per request
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
        source_system_id = (
            idx + 4
        )  # Generate source system ID using index (starting from 1)

        def fetch_and_store_data(token, include_archived):
            data_list = archived_data if include_archived else non_archived_data
            after = None

            while True:
                try:
                    data = get_owners_data(
                        token, after=after, include_archived=include_archived
                    )
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
                            "usedIdIncludingInactive": owner.get(
                                "userIdIncludingInactive"
                            ),
                            "created_At": owner.get("createdAt"),
                            "updated_At": owner.get("updatedAt"),
                            "archived": str(owner.get("archived", False)).lower(),
                            "sourceSystemId": str(source_system_id),  # Add source system ID
                        }
                        data_list.append(entry)
                    paging = data.get("paging")
                    if paging and "next" in paging:
                        after = paging["next"]["after"]
                    else:
                        break
                except requests.exceptions.RequestException as e:
                    print(
                        f"Error fetching {'archived' if include_archived else 'non-archived'} data with token {token}: {e}"
                    )
                    raise RuntimeError(f"Job failed: Unable to get the Data: {e}")

        # Fetch non-archived owners
        fetch_and_store_data(token, include_archived=False)
        # Fetch archived owners
        fetch_and_store_data(token, include_archived=True)

    return archived_data, non_archived_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing the Extraction

# COMMAND ----------


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
    StructField("sourceSystemId", StringType(), True)
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

    # Optionally create a SQL temporary view
    final_df.createOrReplaceTempView("SourceToBronze")
    
except Exception as e:
    print(f"Error processing data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge Data To Target

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(Table_ID, 'Bronze')
    
    if AutoSkipperCheck == 0:
        
        if LoadType == "incremental load":
            MergeQuery = """
            MERGE INTO bronze.{0} AS Target
            USING SourceToBronze AS Source
            ON Target.{1} = Source.{2}
            WHEN MATCHED THEN
              UPDATE SET *
            WHEN NOT MATCHED THEN
              INSERT *
            """.format(TableName, MergeKey, MergeKey)

            spark.sql(MergeQuery)
            logger.info(f"Table {TableName} has been successfully loaded to stage")

            MaxDateQuery = "Select max({0}) as Max_Date from bronze.{1}"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn, TableName)
            DF_MaxDate = spark.sql(MaxDateQuery)

            DeletesQuery = """UPDATE Bronze.{0} SET Is_Deleted = 1 WHERE {1} in (SELECT {0}.{1} FROM SourcetoBronze RIGHT JOIN BRONZE.{0} ON SourceToBronze.{1} = BRONZE.{0}.{1} WHERE SourceToBronze.{1} IS NULL AND {0}.Is_Deleted = 0) """.format(TableName, MergeKey)
            spark.sql(DeletesQuery)
            
            UpdateLastLoadDate(Table_ID,DF_MaxDate)
            UpdatePipelineStatusAndTime(Table_ID, Zone)
            UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", "NULL", "NULL", 0, "Merge-Target Table")

except Exception as e:
    logger.info(f"Failed for bronze load: {TableName}")
    UpdateFailedStatus(Table_ID, 'Bronze')
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone,"Failed",Error_Statement,"NotIgnorable",1,"Merge-Target Table")
    logger.info(f"Updated Metadata for Failed Status: {TableName}")
    print(f"Unable to load {TableName}", e)
    raise RuntimeError(f"Job failed: Unable to Merge Data: {e}")

# COMMAND ----------

