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
from hubspot.crm.contacts import PublicObjectSearchRequest

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
ErrorLogger = ErrorLogs("HubSpot_Contacts_Incremental_Load")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Secrets From Key Vaults

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
    Table_ID_I1 = 'H11'
    DF_Metadata_I1 = spark.sql(f"SELECT * FROM Metadata.MasterMetadata WHERE TableID = '{Table_ID_I1}' and IsActive='1'")
    Job_ID_I1 = DF_Metadata_I1.select(col('JOB_ID')).where(col('TableID') == Table_ID_I1).collect()[0].JOB_ID
    Notebook_ID_I1 = DF_Metadata_I1.select(col('NB_ID')).where(col('TableID') == Table_ID_I1).collect()[0].NB_ID
    TableName_I1 = DF_Metadata_I1.select(col('DWHTableName')).where(col('TableID') == Table_ID_I1).collect()[0].DWHTableName
    MaxLoadDateColumn_I1 = DF_Metadata_I1.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID_I1).collect()[0].LastLoadDateColumn
    MaxLoadDate_I1 = DF_Metadata_I1.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID_I1).collect()[0].LastLoadDateValue
    Zone_I1 = DF_Metadata_I1.select(col('Zone')).where(col('TableID') == Table_ID_I1).collect()[0].Zone
    MergeKey_I1 = DF_Metadata_I1.filter(col("TableID") == Table_ID_I1).select("MergeKey").collect()[0][0]
    LoadType_I1 = DF_Metadata_I1.select(col('LoadType')).where(col('TableID') == Table_ID_I1).collect()[0].LoadType
    SourceSelectQuery_I1 = DF_Metadata_I1.select(col('SourceSelectQuery')).where(col('TableID') == Table_ID_I1).collect()[0].SourceSelectQuery 
except Exception as e:
    logger.info('Failed to Retrieve Metadata')
    print('Failed to Retrieve Metadata', str(e))

# COMMAND ----------

try: 
    Table_ID_I2 = 'H3'
    DF_Metadata_I2 = spark.sql(f"SELECT * FROM Metadata.MasterMetadata WHERE TableID = '{Table_ID_I2}' and IsActive='1'")
    Job_ID_I2 = DF_Metadata_I2.select(col('JOB_ID')).where(col('TableID') == Table_ID_I2).collect()[0].JOB_ID
    Notebook_ID_I2 = DF_Metadata_I2.select(col('NB_ID')).where(col('TableID') == Table_ID_I2).collect()[0].NB_ID
    TableName_I2 = DF_Metadata_I2.select(col('DWHTableName')).where(col('TableID') == Table_ID_I2).collect()[0].DWHTableName
    MaxLoadDateColumn_I2 = DF_Metadata_I2.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID_I2).collect()[0].LastLoadDateColumn
    MaxLoadDate_I2 = DF_Metadata_I2.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID_I2).collect()[0].LastLoadDateValue
    Zone_I2 = DF_Metadata_I2.select(col('Zone')).where(col('TableID') == Table_ID_I2).collect()[0].Zone
    MergeKey_I2 = DF_Metadata_I2.filter(col("TableID") == Table_ID_I2).select("MergeKey").collect()[0][0]
    LoadType_I2 = DF_Metadata_I2.select(col('LoadType')).where(col('TableID') == Table_ID_I2).collect()[0].LoadType
except Exception as e:
    logger.info('Failed to Retrieve Metadata')
    print('Failed to Retrieve Metadata', str(e))

# COMMAND ----------

#Code Change Start PK20260129
#print(f"Original: {SourceSelectQuery_I1}")
remvove_col_list=["hs_date_entered_941581212","hs_date_exited_941581212","hs_time_in_941581212"]
print(f"Missing Column:{remvove_col_list}")
SourceSelectQuery_I1_list = [x for x in SourceSelectQuery_I1.split(",") if x not in remvove_col_list]
SourceSelectQuery_I1 = ",".join(SourceSelectQuery_I1_list)
#print(f"Modified:{SourceSelectQuery_I1}")
#Code Change End PK20260129

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Functions for Extraction

# COMMAND ----------

# List of PAT tokens and their corresponding Max Load Dates
access_tokens = [Shipper_Instance_Token, Carrier_Instance_Token] 
max_dates = [MaxLoadDate_I1, MaxLoadDate_I2 ]

def convert_to_millisec(date_str):
    try:
        # Check if the input is already a datetime object
        if isinstance(date_str, datetime):
            dt_obj = date_str
        else:
            # If it's a string, parse it
            dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            # If parsing fails, try the other format
            if not dt_obj:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
                print("The datetime string doesn't have a fractional part.")

        # Subtract 15 minutes from the datetime object
        dt_obj_minus_15mins = dt_obj - timedelta(minutes=15)
        
        # Subtract 15 minutes from the datetime object
        return math.floor(dt_obj_minus_15mins.timestamp() * 1000)  # Convert to milliseconds
    
    except Exception as e:
        print(f"Error in date conversion: {e}")
        return None

# Function to fetch all properties dynamically
def get_all_properties(client):
    properties = []
    try:
        response = client.crm.properties.core_api.get_all(object_type='contacts')
        for prop in response.results:
            properties.append(prop.name)
    except Exception as e:
        print(f"Error fetching properties: {e}")
    return properties

# Function to get data for a specific instance
def get_data_for_instance(client, max_load_date, all_properties):
    api_list = []
    next_page = 1

    try:
        max_load_date_millisec = convert_to_millisec(max_load_date)
        
        while True:
            time.sleep(0.5)
            api_props = PublicObjectSearchRequest(
                limit=100, 
                after=0 if next_page == 1 else next_page, 
                properties=all_properties,
                filter_groups=[
                    {
                        "filters": [
                            {
                                "propertyName": "lastmodifieddate",
                                "operator": "GTE",
                                "value": max_load_date_millisec
                            }
                        ]
                    }
                ]
            )

            api_response = client.crm.contacts.search_api.do_search(public_object_search_request=api_props)

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
def response_to_dataframe(source_data):
    if not source_data:
        return pd.DataFrame()
    data = [result.properties for result in source_data]
    return pd.DataFrame(data)

# Create a function to infer schema from a combined DataFrame
def infer_schema(df):
    return StructType([StructField(column, StringType(), True) for column in df.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing the Extraction

# COMMAND ----------

# Declare the full set of properties
all_dfs = []
all_properties_set = set()

for i, access_token in enumerate(access_tokens):
    client = HubSpot(access_token=access_token)
    max_date = max_dates[i]

    # Fetch properties dynamically
    all_properties = get_all_properties(client)
    all_properties_set.update(all_properties)
    
    print(f"Retrieved {len(all_properties)} properties for token {i+4}")

    source_data = get_data_for_instance(client, max_date, list(all_properties_set))
    if source_data:
        df = response_to_dataframe(source_data)
        print(f"Fetched {len(df)} records for token {i+4}")
        df["SourcesystemID"] = i + 4
        df["SourcesystemID"] = df["SourcesystemID"].astype('str')
        df["archived"] = df["archived"].astype('str').str.lower()
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
    column_list = SourceSelectQuery_I1.split(",")
    column_list = [c.strip() for c in column_list]
    DF_Source_Final = DF_Source.select(column_list) 

    #Check for New Columns Added in Source API
    new_columns = []
    for i in sorted(DF_Source.columns): 
        if i in DF_Source_Final.columns:
            continue
        else:
            new_columns.append(i)

    if len(new_columns) > 0:
        print("New Columns Added in Source API -", new_columns)
        Error_Statement = "New Columns Added in Source API - " + ", ".join(new_columns)
        UpdateLogStatus(Job_ID_I2, Table_ID_I2, Notebook_ID_I2, TableName_I2, Zone_I2, "Attention Required", "NULL", "NULL", 0, Error_Statement)
    else: 
        print("Columns in Source Remains Same as Target")

    #Code Change Start PK20260129
    #Adding Missing Columns with Null value which were removed from Select List Created for API Feed.
    DF_Source_Final = DF_Source_Final.withColumn("hs_date_entered_941581212", lit(None))
    DF_Source_Final = DF_Source_Final.withColumn("hs_date_exited_941581212", lit(None))
    DF_Source_Final = DF_Source_Final.withColumn("hs_time_in_941581212", lit(None))
    #Code Change End PK20260129
    
    # Create or replace a temporary view
    DF_Source_Final = DF_Source_Final.withColumn("Is_Deleted", lit("0"))
    # Create a hashkey with hs_object_id and SourcesystemID
    DF_Source_Final = DF_Source_Final.withColumn("Contact_Key", sha2(concat(DF_Source_Final.hs_object_id, DF_Source_Final.SourcesystemID), 256))
    DF_Source_Final = DF_Source_Final.dropDuplicates()
    DF_Source_Final.createOrReplaceTempView("SourceToBronze")    
    print("Data processing complete.")

else:
    print("No data fetched from any instance.")
    DF_Source_Final = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge Data To Target

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(Table_ID_I1, 'Bronze')
    
    if AutoSkipperCheck == 0:
        
        if LoadType_I1 == "incremental load" and DF_Source_Final != None:
            MergeQuery = """
            MERGE INTO bronze.{0} AS Target
            USING SourceToBronze AS Source
            ON Target.{1} = Source.{2}
            WHEN MATCHED THEN
              UPDATE SET *
            WHEN NOT MATCHED THEN
              INSERT *
            """.format(TableName_I1, MergeKey_I1, MergeKey_I1)

            spark.sql(MergeQuery)
            logger.info(f"Table {TableName_I1} has been successfully loaded to stage")

            MaxDateQuery = "Select max({0}) as Max_Date from bronze.{1} WHERE SourcesystemID = '{2}'"
            MaxDateQuery_I1 = MaxDateQuery.format(MaxLoadDateColumn_I1, TableName_I1, '4')
            MaxDateQuery_I2 = MaxDateQuery.format(MaxLoadDateColumn_I2, TableName_I2, '5')
            DF_MaxDate_I1 = spark.sql(MaxDateQuery_I1)
            DF_MaxDate_I2 = spark.sql(MaxDateQuery_I2)
            UpdateLastLoadDate(Table_ID_I1,DF_MaxDate_I1)
            UpdateLastLoadDate(Table_ID_I2,DF_MaxDate_I2)
            UpdatePipelineStatusAndTime(Table_ID_I1, Zone_I1)
            UpdatePipelineStatusAndTime(Table_ID_I2 , Zone_I2)
            UpdateLogStatus(Job_ID_I1, Table_ID_I1, Notebook_ID_I1, TableName_I1, Zone_I1, "Succeeded", "NULL", "NULL", 0, "Merge-Target Table")

except Exception as e:
    logger.info(f"Failed for bronze load: {TableName_I1}")
    UpdateFailedStatus(Table_ID_I1, 'Bronze')
    UpdateFailedStatus(Table_ID_I2, 'Bronze')
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID_I1, Table_ID_I1, Notebook_ID_I1, TableName_I1, Zone_I1,"Failed",Error_Statement,"NotIgnorable",1,"Merge-Target Table")
    logger.info(f"Updated Metadata for Failed Status: {TableName_I1}")
    print(f"Unable to load {TableName_I1}", e)
    raise RuntimeError(f"Job failed: Unable to Merge Data: {e}")

# COMMAND ----------

