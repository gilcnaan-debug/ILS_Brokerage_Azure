# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ## Hubspot Assoictaions - Bronze
# MAGIC * **Description:** To Extract the Data from Hubspot and load to Bronze as delta file
# MAGIC * **Created Date:** 02/24/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna 
# MAGIC * **Modified Date:** 02/24/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import the Required Packages

# COMMAND ----------

#Importing Required packages
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pytz
import requests
import pandas as pd
import json
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calling the Utilities and Logger Notebooks

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaring the Error Logger Variables

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("NFI_Hubspot_Associations_Extract")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ###Collecting the Secrets

# COMMAND ----------

# Collect Secret value CatalogName from the Azure KeyVault
CatalogName = dbutils.secrets.get(scope="Brokerage-Catalog", key="CatalogName")

# Use the catalog retrieved from Azure KeyVault
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
# MAGIC ### Getting the Metadata Details

# COMMAND ----------

try: 
    Table_ID = 'H17'
    DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = 'H17' and IsActive='1'")
    Job_ID = DF_Metadata.select(col('JOB_ID')).where(col('TableID') == Table_ID).collect()[0].JOB_ID
    Notebook_ID = DF_Metadata.select(col('NB_ID')).where(col('TableID') == Table_ID).collect()[0].NB_ID
    TableName = DF_Metadata.select(col('DWHTableName')).where(col('TableID') == Table_ID).collect()[0].DWHTableName
    MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateColumn
    MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateValue
    Zone = DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone
    MergeKey = DF_Metadata.filter(col("Tableid") == Table_ID).select("MergeKey").collect()[0][0]
    LoadType = DF_Metadata.select(col('LoadType')).where(col('TableID') == Table_ID).collect()[0].LoadType
except Exception as e:
    logger.info('Failed to Retrive Metadata')
    print('Failed to Retrive Metadata', str(e))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Extraction and Merge 

# COMMAND ----------


try:
    # Tokens for each HubSpot instance
    pat = [Shipper_Instance_Token, Carrier_Instance_Token]

    # Base URL for companies (limit=100 per page)
    base_url = "https://api.hubapi.com/crm/v3/objects/companies?limit=100&properties=hs_company_id,name&associations=emails&associations=contacts&associations=calls&associations=deals&associations=meetings&associations=notes&associations=communications&associations=postal_mail&associations=tasks"

    # Headers for API
    headers = {
        "Content-Type": "application/json"
    }

    # Mapping for association type to toObjectType ID
    association_mapping = {
        "emails": "0-49",
        "contacts": "0-1",
        "calls": "0-48",
        "deals": "0-3",
        "meetings": "0-50",
        "notes": "0-51",
        "postal mail": "0-67",
        "tasks": "0-52",
        "communications": "0-18"
    }

    all_rows = []
    from_object_type = "0-2"  # company object type

    for index, pat_token in enumerate(pat):
        headers["Authorization"] = f"Bearer {pat_token}"
        next_company_url = base_url

        while next_company_url:
            company_response = requests.get(next_company_url, headers=headers)

            if company_response.status_code != 200:
                print(f"Company API Error: {company_response.status_code} - {company_response.text}")
                break

            company_data = company_response.json()
            for company in company_data.get("results", []):
                company_id = company["id"]
                company_name = company["properties"].get("name", "")
                source_system = 4 if index == 0 else 5

                for assoc_type in association_mapping.keys():
                    assoc_obj = company.get("associations", {}).get(assoc_type, {})
                    assoc_results = assoc_obj.get("results", [])
                    assoc_paging = assoc_obj.get("paging", {})

                    to_object_type = association_mapping[assoc_type]

                    while True:
                        for assoc in assoc_results:
                            all_rows.append((company_id, company_name, source_system, assoc_type.capitalize(), assoc["id"]))

                        if not assoc_paging.get("next"):
                            break

                        after_token = assoc_paging["next"]["after"]
                        assoc_url = f"https://api.hubapi.com/crm/v3/objects/{from_object_type}/{company_id}/associations/{to_object_type}?after={after_token}"
                        assoc_response = requests.get(assoc_url, headers=headers)

                        if assoc_response.status_code != 200:
                            print(f"Association API Error for {assoc_type}: {assoc_response.status_code} - {assoc_response.text}")
                            break

                        assoc_data = assoc_response.json()
                        assoc_results = assoc_data.get("results", [])
                        assoc_paging = assoc_data.get("paging", {})

            next_company_url = company_data.get("paging", {}).get("next", {}).get("link", None)

    # Define schema
    schema = StructType([
        StructField("company_id", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("sourcesystem_id", IntegerType(), True),
        StructField("associated_type", StringType(), True),
        StructField("associated_id", StringType(), True)
    ])
    # Create DataFrame
    df = spark.createDataFrame(all_rows, schema)
    df = df.withColumn("Association_Key", sha2(concat_ws("", "company_id", "company_name", "sourcesystem_id", "associated_id", "associated_type"), 256))
    df = df.dropDuplicates()
    df.createOrReplaceTempView("SourceToBronze")

    try:
        AutoSkipperCheck = AutoSkipper(Table_ID, 'Bronze')

        if AutoSkipperCheck == 0:
            if LoadType.lower() == "incremental load":
                MergeQuery = """
                MERGE INTO bronze.{0} AS Target
                USING SourceToBronze AS Source
                ON Target.{1} = Source.{2}
                WHEN NOT MATCHED THEN
                    INSERT (
                        Association_Key,
                        Company_ID,
                        Company_Name,
                        Associated_ID,
                        Associated_Type,
                        Sourcesystem_ID,
                        Is_Deleted,
                        Created_Date,
                        Created_By,
                        Last_Modified_Date,
                        Last_Modified_By
                    )
                    VALUES (
                        Source.Association_Key,
                        Source.Company_ID,
                        Source.Company_Name,
                        Source.Associated_ID,
                        Source.Associated_Type,
                        Source.Sourcesystem_ID,
                        0, 
                        Current_timestamp(),
                        'Databricks',
                        Current_timestamp(),
                        'Databricks'
                    )
                """
                formatted_merge_query = MergeQuery.format(TableName, MergeKey, MergeKey)
                spark.sql(formatted_merge_query)

                Deletes_Query = '''UPDATE Bronze.{0}
                            SET  {1}.Is_Deleted = '1', 
                                {2}.last_modified_date = Current_Timestamp()
                            where  {3}.{4} in (select {11}.{5} from  Bronze.{6} LEFT JOIN SourceToBronze S ON {7}.{8} = S.{9} WHERE S.{10} is null)'''

                Deletes_Query = Deletes_Query.format(TableName, TableName, TableName, TableName, MergeKey, MergeKey, TableName, TableName, MergeKey, MergeKey, MergeKey, TableName)
                spark.sql(Deletes_Query)

            UpdatePipelineStatusAndTime(Table_ID, 'Bronze')
            MaxDateQuery = "Select max({0}) as Max_Date from Bronze.{1}"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn, TableName)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(Table_ID, DF_MaxDate)
            UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", "NULL", "NULL", 0, "Merge-Target Table")

    except Exception as e:
        logger.info(f"Failed for bronze load: {str(e)}")
        UpdateFailedStatus(Table_ID, 'Bronze')
        print('Failed for bronze load', str(e))
        logger.info(f"Unable to load {TableName}")
        raise RuntimeError(f"Job failed: Unable to Merge: {e}")

    else:
        try:
            UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", "NULL", "NULL", 0, "Merge-Target Table")
            print("No data available after API call.")
        except Exception as e:
            logger.info(f"Failed to update log status: {str(e)}")
            print(str(e))

except Exception as e:
    logger.info(f"Error in API fetching: {str(e)}")
    UpdateFailedStatus(Table_ID, 'Bronze')
    print(str(e))
    raise RuntimeError(f"Job failed: Job failed: Unable to get Data: {e}")

# COMMAND ----------

