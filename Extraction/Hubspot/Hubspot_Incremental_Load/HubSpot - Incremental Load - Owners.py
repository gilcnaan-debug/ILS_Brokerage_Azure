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
import hubspot
from hubspot import HubSpot
from pprint import pprint
import time
from datetime import datetime
import pytz
import pandas as pd
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

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
ErrorLogger = ErrorLogs("HubSpot_Contacts_SourceToBronze-Deletes")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Catalog

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
# Retrieve metadata for tables
    Table_ID_I1 = 'H11'
    DF_Metadata_I1 = spark.sql(f"SELECT * FROM Metadata.MasterMetadata WHERE TableID = '{Table_ID_I1}'")
    Metadata_I1 = DF_Metadata_I1.collect()[0]
    Job_ID_I1 = Metadata_I1.Job_ID
    Notebook_ID_I1 = Metadata_I1.NB_ID
    TableName_I1 = Metadata_I1.DWHTableName
    Zone_I1 = Metadata_I1.Zone
    MergeKey_I1 = Metadata_I1.MergeKey

    Table_ID_I2 = 'H3'
    DF_Metadata_I2 = spark.sql(f"SELECT * FROM Metadata.MasterMetadata WHERE TableID = '{Table_ID_I2}'")
    Metadata_I2 = DF_Metadata_I2.collect()[0]
    Job_ID_I2 = Metadata_I2.Job_ID
    Notebook_ID_I2 = Metadata_I2.NB_ID
    TableName_I2 = Metadata_I2.DWHTableName
    Zone_I2 = Metadata_I2.Zone
    MergeKey_I2 = Metadata_I2.MergeKey
except(Exception) as e:
    logger.info('Failed to Retrieve Metadata')
    print('Failed to Retrieve Metadata', str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions for Extraction

# COMMAND ----------

def get_data_for_instance(client, archived_status):
    api_list = []
    next_page = None

    try:
        while True:
            time.sleep(0.5)

            api_response = client.crm.contacts.basic_api.get_page(
                limit=100,
                archived=archived_status,
                after=next_page,
                properties=['hs_object_id']
            )

            # Check if the response is None
            if api_response is None:
                print(f"API response is None for archived={archived_status}.")
                break

            # Check if results exist
            if not api_response.results:
                print(f"No results found for archived={archived_status}.")
                break

            for item in api_response.results:
                item.properties['archived_at'] = item.archived_at
                item.properties['archived'] = item.archived
                api_list.append(item)

            # Ensure paging exists before accessing it
            if hasattr(api_response, 'paging') and api_response.paging:
                next_page = api_response.paging.next.after
            else:
                break
        
        print(f"Fetched {len(api_list)} items for archived={archived_status}.")
        return api_list

    except Exception as e:
        print(f"Error fetching data: {e}")
        if hasattr(e, 'response'):
            print(f"Response content: {e.response.content}")
        raise RuntimeError(f"Job failed: Unable to get the Data: {e}")


# Define function to convert response to DataFrame
def response_to_dataframe(source_data):
    if not source_data:
        return pd.DataFrame()  # Return an empty DataFrame if no companies are found
    data = [item.properties for item in source_data]
    return pd.DataFrame(data)

# Define function to infer schema
def infer_schema(df):
    fields = []
    for col in df.columns:
        fields.append(StructField(col, StringType(), True))
    return StructType(fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraction and Updation

# COMMAND ----------

try: 
    # Declare the set of properties
    all_dfs = []
    all_properties_set = set()
    for i, access_token in enumerate(access_tokens):
        client = HubSpot(access_token=access_token)

        NonArchivedData = get_data_for_instance(client,False)
        ArchivedData = get_data_for_instance(client,True)

        if len(NonArchivedData) > 0 and len(ArchivedData) > 0:
            SourceData = NonArchivedData + ArchivedData
        elif len(NonArchivedData) > 0 and len(ArchivedData) == 0:
            SourceData = NonArchivedData 
        else:
            SourceData = []
        
        if SourceData:
            df = response_to_dataframe(SourceData)
            print(f"Fetched {len(df)} records for token {i+4}")
            df["SourcesystemID"] = i + 4
            all_dfs.append(df)
            
    if all_dfs:
        
        all_properties_sorted =sorted(all_properties_set)
        standardized_dfs = []

        # Initialize list to hold standardized DataFrames
        for df in all_dfs:
            
            # Create a DataFrame with all columns, using None for missing values
            standardized_df = pd.DataFrame(columns=all_properties_sorted)
            
            # Update with actual data
            standardized_df = standardized_df.combine_first(df)
            
            # Ensure NaN values are converted to None for Spark compatibility
            standardized_df = standardized_df.applymap(lambda x: None if pd.isna(x) else x)
            standardized_df['archived_at'] = standardized_df['archived_at'].replace(pd.NaT, "null")
            standardized_df['archived_at'] = standardized_df['archived_at'].replace('null', None)
            standardized_df['archived_at'] = standardized_df['archived_at'].replace('None', None)
            # Add to list
            standardized_dfs.append(standardized_df)
        
        # Combine all DataFrames into a single Pandas DataFrame
        combined_df = pd.concat(standardized_dfs, ignore_index=True)  

        # Convert any timestamp columns to string, if necessary
        if 'archived_at' in combined_df.columns:
            combined_df['archived_at'] = combined_df['archived_at'].astype(str)

        # # Replace NaN values with None in the entire DataFrame
        combined_df['archived_at'] = combined_df['archived_at'].replace('None', None)

        # Convert SourcesystemID and boolean columns if needed
        combined_df['archived'] = combined_df['archived'].astype(str).apply(lambda x: x.lower())

        # Create Spark DataFrame
        schema = infer_schema(combined_df)
        DF_Source = spark.createDataFrame(combined_df, schema=schema)
        
        # Create or replace a temporary view
        DF_Source_Final = DF_Source.withColumn("Is_Deleted", lit("0"))
        # Create a hashkey with hs_object_id and SourcesystemID
        DF_Source_Final = DF_Source_Final.withColumn("Contact_Key", sha2(concat(DF_Source_Final.hs_object_id, DF_Source_Final.SourcesystemID), 256))
        DF_Source_Final = DF_Source_Final.dropDuplicates()
        DF_Source_Final.createOrReplaceTempView("SourceToBronze")    
        print("Data processing complete.")

    else:
        print("No data fetched from any instance.")

except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID_I1, Table_ID_I1, Notebook_ID_I1, TableName_I1, Zone_I1, "Failed", Error_Statement, "NotIgnorable", 1, "Deletes -Target Table")
    logger.error(f"Exception: {Error_Statement}")
    print(e)

# COMMAND ----------

# Try block for processing metadata and data
try:
    # Check if table should be skipped
    AutoSkipperCheck = AutoSkipper(Table_ID_I1, 'Bronze')
    if AutoSkipperCheck == 0:
        # Define SQL Update queries
        DeleteUpdateQuery = f"""
        MERGE INTO 
            Bronze.{TableName_I1} AS Target
        USING 
            (SELECT Bronze.{TableName_I1}.{MergeKey_I1}  
             FROM SourceToBronze 
             RIGHT JOIN Bronze.{TableName_I1} 
             ON SourceToBronze.{MergeKey_I1} = Bronze.{TableName_I1}.{MergeKey_I1} 
             WHERE Bronze.{TableName_I1}.Is_Deleted = 0 
             AND SourceToBronze.{MergeKey_I1} IS NULL) AS Source
        ON 
            Source.{MergeKey_I1} = Target.{MergeKey_I1}
        WHEN MATCHED
        THEN
        UPDATE SET 
            Target.Is_Deleted = 1
        """

        ArchiveUpdateQuery = f"""
        MERGE INTO 
            Bronze.{TableName_I1} AS Target
        USING 
            SourceToBronze AS Source
        ON 
            Source.{MergeKey_I1} = Target.{MergeKey_I1}
        WHEN MATCHED AND Target.Is_Deleted = 0 AND Source.archived != Target.archived 
        THEN
        UPDATE SET 
            Target.archived = Source.archived,
            Target.archived_at = Source.archived_at
        """

        # Execute SQL Update
        spark.sql(DeleteUpdateQuery)
        spark.sql(ArchiveUpdateQuery)
        UpdateLogStatus(Job_ID_I1, Table_ID_I1, Notebook_ID_I1, TableName_I1, Zone_I1, "Succeeded", "NULL", "NULL", 0, "Deletes -Target Table")
        logger.info(f"The deletes and archives have been successfully updated to the {TableName_I1}")

except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID_I1, Table_ID_I1, Notebook_ID_I1, TableName_I1, Zone_I1, "Failed", Error_Statement, "NotIgnorable", 1, "Deletes -Target Table")
    logger.error(f"Exception: {Error_Statement}")
    print(e)
    raise RuntimeError(f"Job failed: Unable to Merge: {e}")
