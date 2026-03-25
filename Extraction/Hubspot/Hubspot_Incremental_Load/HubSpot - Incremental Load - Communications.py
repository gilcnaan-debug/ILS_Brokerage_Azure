# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from HubSpot to Bronze as delta file
# MAGIC * **Created Date:** 06/29/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 06/29/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# Importing packages
import hubspot
from pprint import pprint
import time
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytz
import math
from hubspot.crm.companies import PublicObjectSearchRequest
from hubspot import HubSpot
from hubspot.crm.companies import ApiException


# COMMAND ----------

# MAGIC %md
# MAGIC ####Initializing Utilities

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initializing Logger

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# Creating variables to store error log information
ErrorLogger = ErrorLogs("HubSpot_Companies_Lifecyclestage_Extract")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating the connection string format

# COMMAND ----------

##Collecting Secret values from the Azure keyVault and storing them in variables
try:
    logger.info("Getting secret values and forming the connection string")

    api_token = dbutils.secrets.get(scope = "NFI_HubSpot_Secrets", key = "tokenkey-instance2")
    client = hubspot.Client.create(access_token=api_token)

except Exception as e:
    logger.error(f"Unable to connect to HubSpot {e}")

# COMMAND ----------

try: 
    Table_ID = 'H16'
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

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")# Access token for HubSpot

# Initialize the HubSpot client
api_client = HubSpot(access_token=api_token)

def get_all_companies(api_client, properties=None):
    all_companies = []
    after = None
    page = 1
    
    while True:  # Remove the max_pages limit
        try:
            api_response = api_client.crm.companies.basic_api.get_page(
                limit=50,  # Set the limit to 50 as per API constraints
                after=after,
                properties=properties,
                properties_with_history=properties
            )
            all_companies.extend(api_response.results)
            if api_response.paging and api_response.paging.next:
                after = api_response.paging.next.after
                page += 1
                time.sleep(0.2)  # Sleep a little to respect API rate limits
            else:
                break
        except ApiException as e:
            print(f"Exception when calling get_page: {e}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    return all_companies

def main():
    properties = ["lifecyclestage", "name", "hs_lastmodifieddate", "dot_number"]

    # Retrieve all companies with lifecycle stage, company name, last modified date, and dot_number properties
    all_companies = get_all_companies(api_client, properties=properties)

    print(f"Total companies fetched: {len(all_companies)}")

    # Store the data in a list of dictionaries
    company_data = []
    for company in all_companies:
        company_id = company.id
        company_properties = company.properties
        company_name = company_properties.get("name", "N/A")  # Fetch company name if exists else set to N/A
        last_modified_date = company_properties.get("hs_lastmodifieddate", "N/A")  # Fetch last modified date if exists else set to N/A
        dot_number = company_properties.get("dot_number", 0)  # Fetch dot_number if exists else set to 0
        lifecyclestage_history = company.properties_with_history.get("lifecyclestage", [])

        if lifecyclestage_history:  # Check if lifecyclestage_history is not empty
            for entry in lifecyclestage_history:
                company_data.append({
                    'Company_ID': company_id,
                    'Company_Name': company_name,
                    'Lifecycle_Stage_Value': entry.value,
                    'Cycle_Timestamp': entry.timestamp,
                    'Source_Type': entry.source_type,
                    'Source_ID': entry.source_id,
                    'Updated_By_User_ID': getattr(entry, 'updated_by_user_id', 'N/A'),
                    'Last_Modified_Date': last_modified_date,
                    'DOT_Number': str(dot_number)  # Add dot_number to the dictionary
                })

    print(f"Total data points: {len(company_data)}")
    try:
        # Convert the list of dictionaries to a Pandas DataFrame
        df = pd.DataFrame(company_data)
    #   tential issues with date parsing
        # Define schema for Spark DataFrame
        schema = StructType(
            [
                StructField("Company_ID", StringType(), nullable=True),
                StructField("Company_Name", StringType(), nullable=True),
                StructField("Lifecycle_Stage_Value", StringType(), nullable=True),
                StructField("Cycle_Timestamp", TimestampType(), nullable=True),
                StructField("Source_Type", StringType(), nullable=True),
                StructField("Source_ID", StringType(), nullable=True),
                StructField("Updated_By_User_ID", StringType(), nullable=True),
                StructField("Last_Modified_Date", StringType(), nullable=True),
                StructField("DOT_Number", StringType(), nullable=True)  # Add DOT_Number field
            ]
        )

        # Create Spark DataFrame from Pandas DataFrame
        DF_Companies = spark.createDataFrame(df, schema)
        
        # Create a merge key
        Merge_Key = ["Company_ID", "Cycle_Timestamp", "Lifecycle_Stage_Value"]
        DF_Companies = DF_Companies.withColumn("Mergekey", md5(concat_ws("", *Merge_Key)))
        DF_Companies = DF_Companies.dropDuplicates()        
        DF_Companies.createOrReplaceTempView('SourceToBronze')
        Rowcount = DF_Companies.count()
        print("View and dataframe created")
        print(f"Rowcount: {Rowcount}")
        
    except Exception as e:
        print(e)
        raise RuntimeError(f"Job failed: Unable to Extract Data: {e}")

if __name__ == '__main__':
    main()

# COMMAND ----------

# MAGIC %md
# MAGIC ##MERGE INTO TARGET TABLE

# COMMAND ----------

#Merge data to the Delta table

try:
    if LoadType == 'incremental load':
        
            print('Loading ' + TableName)

            MergeQuery = 'Merge into bronze.{0} Target using SourceToBronze source on Target.{1} = Source.{2} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'

            spark.sql(MergeQuery.format(TableName,MergeKey,MergeKey))
            logger.info('Successfully loaded the'+TableName+'to bronze')
            print('Loaded ' + TableName) 

            MaxDateQuery = "Select max({0}) as Max_Date from bronze.{1}"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn,TableName)
            DF_MaxDate = spark.sql(MaxDateQuery)

            UpdateLastLoadDate(Table_ID,DF_MaxDate)
            UpdatePipelineStatusAndTime(Table_ID,'Bronze')
            UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,TableName,Zone,"Succeeded","NULL","NULL",0,"Hubspot_lifecyclestage")

except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    logger.info('Failed for Bronze load')
    UpdateFailedStatus(Table_ID,'Bronze')
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,TableName,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Merge-Target Table")
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    raise RuntimeError(f"Job failed: Unable to Merge Data: {e}")
    print(e)

# COMMAND ----------

