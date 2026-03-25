# Databricks notebook source
# MAGIC %md
# MAGIC ## HubSpot Deletes
# MAGIC * **Description:** To update deletes from Source to Bronze table
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
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytz

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
ErrorLogger = ErrorLogs("HubSpot_PostalMails_SourceToBronze-Deletes")
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
    Table_ID_I1 = 'H20'
    DF_Metadata_I1 = spark.sql(f"SELECT * FROM Metadata.MasterMetadata WHERE TableID = '{Table_ID_I1}'")
    Metadata_I1 = DF_Metadata_I1.collect()[0]
    Job_ID_I1 = Metadata_I1.Job_ID
    Notebook_ID_I1 = Metadata_I1.NB_ID
    TableName_I1 = Metadata_I1.DWHTableName
    Zone_I1 = Metadata_I1.Zone
    MergeKey_I1 = Metadata_I1.MergeKey

    Table_ID_I2 = 'H21'
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

def get_data_for_instance(client, archived):
    api_list = []
    next_page = 1

    try:
        
        while True:
            time.sleep(0.5)
            
            api_response = client.crm.objects.postal_mail.basic_api.get_page(
                limit=100,
                archived=archived,  # Using the 'archived' parameter
                after=0 if next_page == 1 else next_page,
                properties=['hs_object_id']
            )

            for item in api_response.results:
                item.properties['archived_at'] = item.archived_at
                item.properties['archived'] = item.archived
                api_list.append(item)  
                
            try:
                next_page = api_response.paging.next.after
            except AttributeError:
                break
        
        return api_list

    except Exception as e:
        print(f"Error fetching data: {e}")
        raise RuntimeError(f"Job failed: Unable to get the Data: {e}")
        return None

# Function to convert source data to pandas DataFrame
def response_to_dataframe(SourceData):
    if not SourceData:
        return pd.DataFrame()

    data = [result.properties for result in SourceData]
    return pd.DataFrame(data)

# Create a function to infer schema from a combined DataFrame
def infer_schema(df):
    return StructType([StructField(column, StringType(), True) for column in df.columns])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraction and Updation

# COMMAND ----------

try:
    all_dfs = []
    all_properties_set = set()

    for i,access_token in enumerate(access_tokens):
        client = HubSpot(access_token=access_token)

        SourceData = get_data_for_instance(client, False)
        
        if SourceData:
            df = response_to_dataframe(SourceData)
            print(f"Fetched {len(df)} records for token {i+4}")
            df["SourcesystemID"] = i + 4
            all_dfs.append(df)

    # Check if at least one DataFrame has been created
    if all_dfs:
        all_properties_sorted = sorted(all_properties_set)

        # Initialize list to hold standardized DataFrames
        standardized_dfs = []

        for df in all_dfs:
            # Create a DataFrame with all columns, using None for missing values
            standardized_df = pd.DataFrame(columns=all_properties_sorted)
            
            # Update with actual data
            standardized_df = standardized_df.combine_first(df)
            
            # Ensure NaN values are converted to None for Spark compatibility
            standardized_df = standardized_df.applymap(lambda x: None if pd.isna(x) else x)
            
            # Add to list
            standardized_dfs.append(standardized_df)
        
        # Combine all DataFrames into a single Pandas DataFrame
        combined_df = pd.concat(standardized_dfs, ignore_index=True)

        # Infer schema from the combined Pandas DataFrame
        schema = infer_schema(combined_df)

        # Create a Spark DataFrame with the inferred schema
        DF_Source = spark.createDataFrame(combined_df, schema=schema)
        
        # Create or replace a temporary view
        DF_Source = DF_Source.withColumn("Is_Deleted", lit("0"))
        DF_Source = DF_Source.dropDuplicates()
        DF_Source.createOrReplaceTempView("SourceToBronze")    
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
        UPDATE Bronze.{TableName_I1}
        SET Bronze.{TableName_I1}.Is_Deleted = 1
        WHERE Bronze.{TableName_I1}.{MergeKey_I1} IN 
            (SELECT Target.{MergeKey_I1} from SourceToBronze AS Source 
            RIGHT JOIN Bronze.{TableName_I1} as Target 
            ON Source.{MergeKey_I1} = Target.{MergeKey_I1}
            WHERE Target.Is_Deleted = 0 AND Source.{MergeKey_I1} IS NULL)
        """
        # Execute SQL Update
        spark.sql(DeleteUpdateQuery)
        print("Updated Successfully.")
        UpdateLogStatus(Job_ID_I1, Table_ID_I1, Notebook_ID_I1, TableName_I1, Zone_I1, "Succeeded", "NULL", "NULL", 0, "Deletes -Target Table")

except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID_I1, Table_ID_I1, Notebook_ID_I1, TableName_I1, Zone_I1, "Failed", Error_Statement, "NotIgnorable", 1, "Deletes -Target Table")
    logger.error(f"Exception: {Error_Statement}")
    print(e)
    raise RuntimeError(f"Job failed: Unable to Merge: {e}")


# COMMAND ----------

