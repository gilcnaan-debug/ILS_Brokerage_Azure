# Databricks notebook source
# MAGIC %md
# MAGIC ## Finance_Credits
# MAGIC * **Description:** Extract the Credit files from the azure blob and load that into the Bronze Schema  Finance_Credits Table. 
# MAGIC * **Created Date:** 09/05/2025
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 09/05/2025
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing Packages

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
import pandas as pd
from io import BytesIO
import os
from decimal import Decimal
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calling Utilities notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "//Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calling Logger notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Declaring the Error Logger Variables

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("Finance_Extraction_24hr_Refresh_Credits__")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ####Use catalog

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Get Metadata details

# COMMAND ----------

try:
    DF_Metadata= spark.sql("SELECT * FROM Metadata.MasterMetadata where TableID='F15' and IsActive='1' ")

    Table_ID  =  DF_Metadata.select(col("TableID")).where(col("TableID") == "F15").collect()[0].TableID
    Job_ID = DF_Metadata.select(col("JOB_ID")).where(col("TableID") == Table_ID).collect()[0].JOB_ID
    Notebook_ID = DF_Metadata.select(col("NB_ID")).where(col("TableID") == Table_ID).collect()[0].NB_ID
    TableName = (DF_Metadata.select(col("SourceTableName")).where(col("TableID") == Table_ID).collect()[0].SourceTableName)
    ErrorPath = (DF_Metadata.select(col("ErrorLogPath")).where(col("TableID") == Table_ID).collect()[0].ErrorLogPath)
    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == Table_ID).collect()[0].MergeKey) 
    LoadType = (DF_Metadata.select(col("LoadType")).where(col("TableID") == Table_ID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateValue)
    Zone =(DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone)
    SourceSelectQuery = (DF_Metadata.select(col('SourceSelectQuery')).where(col('TableID') == Table_ID).collect()[0].SourceSelectQuery)
except Exception as e:
    logger.info("Unable to fetch metadata details", e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retrive Secrets From Azure Key Vault 

# COMMAND ----------

# Retrieve the secrets from Azure Key Vault
try:
    #Update PipelineStartTime in Metadata
    UpdatePipelineStartTime(Table_ID, Zone)
    tenant_id = dbutils.secrets.get(scope="NFI_Finance_Storage", key="TenantID")
    client_id = dbutils.secrets.get(scope="NFI_Finance_Storage", key="ClientID")
    client_secret = dbutils.secrets.get(scope="NFI_Finance_Storage", key="Client-SecretID")
except Exception as e:
    logger.info("unable to fetch creds from azure keyvault")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Connection String for Azure Blob Storage 

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
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Azure Connection")
else:
    print("Connection test failed.")
    UpdateFailedStatus(Table_ID, 'Bronze')
    Error_Statement = "Failed to connect Azure"
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Azure Connection")

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC
# MAGIC %md
# MAGIC #### Extract the FileName that should be loaded from Source 

# COMMAND ----------


try: 
    container_name = "finance"
    folder_path = "Finance Credits/NFI_ILS_Customer_"

    # Create a container client
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs in the container and filter by folder path
    blob_list = container_client.list_blobs(name_starts_with=folder_path)

    customer_blobs = [result.name for result in blob_list]

    latest_blobs = []
    for blob_name in customer_blobs:
        timestamp_str = blob_name.split("_")[-1].split(".")[0]
        fiscal_year = blob_name.split("Customer_")[1].split("_")[0]
        timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        if timestamp > MaxLoadDate:
            latest_blobs.append((blob_name, fiscal_year, timestamp))
            
    latest_blobs.sort(key=lambda x: (x[1], x[2]))
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Incremental Files Extraction")
 
except Exception as e:
    logger.info("Unable to get the incremental files from azure")
    print(e)
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Incremental Files Extraction")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a variable for Merge Query

# COMMAND ----------

# Define the merge query
MergeQuery = '''
                    MERGE INTO {0}.{1} AS Target
                    USING VW_Credits AS Source ON Target.{2}= Source.{2}
                    WHEN MATCHED AND Target.HASHKEY != Source.HASHKEY  
                    THEN UPDATE SET
                        Target.SEARCH_NAME = Source.SEARCH_NAME,
                        Target.ADDR1 = Source.ADDR1,
                        Target.ADDR2 = Source.ADDR2,
                        Target.CITY = Source.CITY,
                        Target.STATE = Source.STATE,
                        Target.COUNTRY_CODE = Source.COUNTRY_CODE,
                        Target.ZIP = Source.ZIP,
                        Target.START_DATE = Source.START_DATE,
                        Target.ACTIVE_STATUS = Source.ACTIVE_STATUS,
                        Target.CREDIT_LIM = Source.CREDIT_LIM,
                        Target.CURRENCY_CD = Source.CURRENCY_CD,
                        Target.TERMS_CD = Source.TERMS_CD,
                        Target.TERM_RDESC_01 = Source.TERM_RDESC_01,
                        Target.HASHKEY = Source.HASHKEY,
                        Target.SOURCE_FILE_NAME = Source.SOURCE_FILE_NAME,
                        Target.SOURCESYSTEM_ID = '8',
                        Target.SOURCESYSTEM_NAME = 'Lawson System',
                        Target.LAST_MODIFIED_BY = 'Databricks',
                        Target.LAST_MODIFIED_DATE = Current_timestamp(),
                        Target.IS_DELETED = 0

                    WHEN MATCHED AND TARGET.HASHKEY = Source.HASHKEY AND Target.Is_Deleted = 1 
                    THEN UPDATE SET
                        Target.Is_Deleted = 0,
                        Target.LAST_MODIFIED_BY = 'Databricks',
                        Target.LAST_MODIFIED_DATE = Current_timestamp(),
                        Target.SOURCE_FILE_NAME = Source.SOURCE_FILE_NAME

                    WHEN NOT MATCHED THEN
                    INSERT (
                        CREDIT_KEY,
                        CUSTOMER,
                        COMPANY,
                        SEARCH_NAME,
                        ADDR1,
                        ADDR2,
                        CITY,
                        STATE,
                        COUNTRY_CODE,
                        ZIP,
                        START_DATE,
                        ACTIVE_STATUS,
                        CREDIT_LIM,
                        CURRENCY_CD,
                        TERMS_CD,
                        TERM_RDESC_01,
                        HASHKEY,
                        SOURCE_FILE_NAME,
                        SOURCESYSTEM_ID,
                        SOURCESYSTEM_NAME,
                        CREATED_DATE,
                        CREATED_BY,
                        LAST_MODIFIED_DATE,
                        LAST_MODIFIED_BY,
                        IS_DELETED
                    )
                    VALUES (
                        Source.CREDIT_KEY,
                        Source.CUSTOMER,
                        Source.COMPANY,
                        Source.SEARCH_NAME,
                        Source.ADDR1,
                        Source.ADDR2,
                        Source.CITY,
                        Source.STATE,
                        Source.COUNTRY_CODE,
                        Source.ZIP,
                        Source.START_DATE,
                        Source.ACTIVE_STATUS,
                        Source.CREDIT_LIM,
                        Source.CURRENCY_CD,
                        Source.TERMS_CD,
                        Source.TERM_RDESC_01,
                        Source.HASHKEY,
                        Source.SOURCE_FILE_NAME,
                        8 ,
                        'Lawson System',
                        current_timestamp(),
                        'Databricks',
                        current_timestamp(),
                        'Databricks',
                        0
                    );

            '''
MergeQuery = MergeQuery.format(Zone,TableName,MergeKey)



# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the blob contents and Load the Data into bronze 

# COMMAND ----------

try:
    for blob_name, fiscal_year,timestamp in latest_blobs:
    
        blob_client = container_client.get_blob_client(blob_name)
        downloaded_blob = blob_client.download_blob().readall()
   
        decimal_columns = ['CREDIT_LIM']
        converters = {col: Decimal for col in decimal_columns}

        df = pd.read_csv(BytesIO(downloaded_blob), sep='|', encoding='ISO-8859-1', decimal='.', converters=converters)

        new_columns = [col for col in df.columns if col not in SourceSelectQuery]
        
        if new_columns:
            error_message = f"New columns found in file {blob_name}: {', '.join(new_columns)}"
            print(error_message)
            UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Failed", error_message, "NotIgnorable", 1, "New Columns Added")
            logger.info(error_message)
            raise
    

        filename = os.path.basename(blob_name)  # Get the filename from the blob name
        df['SOURCE_FILE_NAME'] = filename
        pd_df = df

        schema = StructType([
                StructField("COMPANY", IntegerType(), True),
                StructField("CUSTOMER", StringType(), True),
                StructField("SEARCH_NAME", StringType(), True),
                StructField("ADDR1", StringType(), True),
                StructField("ADDR2", StringType(), True),
                StructField("CITY", StringType(), True),
                StructField("STATE", StringType(), True),
                StructField("COUNTRY_CODE", StringType(), True),
                StructField("ZIP", StringType(), True),
                StructField("START_DATE", StringType(), True),
                StructField("ACTIVE_STATUS", StringType(), True),
                StructField("CREDIT_LIM", DecimalType(18,2), True),
                StructField("CURRENCY_CD", StringType(), True),
                StructField("TERMS_CD", StringType(), True),
                StructField("TERM_RDESC_01", StringType(), True),
                StructField("SOURCE_FILE_NAME", StringType(), True)
            ])
        # Convert the pandas DataFrame to a list of dictionaries
        data = pd_df.to_dict('records')

        # Convert the list of dictionaries to a list of Row objects
        rows = [Row(**dict_) for dict_ in data]

        # Create the Spark DataFrame
        df = spark.createDataFrame(rows, schema)

        columns_to_replace = [
            "COMPANY",
            "CUSTOMER",
            "SEARCH_NAME",
            "ADDR1",
            "ADDR2",
            "CITY",
            "STATE",
            "COUNTRY_CODE",
            "ZIP",
            "START_DATE",
            "ACTIVE_STATUS",
            "CREDIT_LIM",
            "CURRENCY_CD",
            "TERMS_CD",
            "TERM_RDESC_01",
            "SOURCE_FILE_NAME"
        ]

        for col_name in columns_to_replace:
            df = df.withColumn(col_name, when(col(col_name) == 'NaN', None).otherwise(col(col_name)))
            DF_Credits = df

        try:
            Hashkey_Columns = [
            "SEARCH_NAME",
            "ADDR1",
            "ADDR2",
            "CITY",
            "STATE",
            "COUNTRY_CODE",
            "ZIP",
            "START_DATE",
            "ACTIVE_STATUS",
            "CREDIT_LIM",
            "CURRENCY_CD",
            "TERMS_CD",
            "TERM_RDESC_01"]

            MergeKey_Columns = [
            "COMPANY",
            "CUSTOMER"]

            DF_Credits = DF_Credits.withColumn("CREDIT_KEY",md5(concat_ws("",*MergeKey_Columns)))
            DF_Credits = DF_Credits.withColumn("HASHKEY",md5(concat_ws("",*Hashkey_Columns)))
    
        except Exception as e:
            logger.info(f"Unable to create Keys for Credits{e}")
            print(e)
        
        try:
           
            DF_Credits.dropDuplicates()
            DF_Credits.createOrReplaceTempView('VW_Credits')

        except Exception as e:
            logger.info(f"Unable to Create a View from the dataframe {e}")
            print(e)

        try:
            AutoSkipperCheck = AutoSkipper(Table_ID, Zone)

            if AutoSkipperCheck == 0:
            
                try:
                    print('Loading ' + TableName + ' ' + filename  )            

                    spark.sql(MergeQuery)
                    
                    try:
                        IsDeletedQuery = '''UPDATE {0}.{1}
                        SET IS_DELETED = 1
                        WHERE {2} IN (
                            SELECT C.{2}
                            FROM {0}.{1} C
                            Left JOIN VW_Credits VW ON C.{2} = VW.{2}
                            WHERE VW.{2} IS NULL)

                    '''
                        IsDeletedQuery = IsDeletedQuery.format(Zone,TableName,MergeKey)
                
                        spark.sql(IsDeletedQuery)

                    except Exception as e:
                        print('Cant Update IsDeleted', e)
                        logger.info(f"Unable to Update IsDeleted {e}")
                        

                    spark.sql(f"""
                              UPDATE metadata.mastermetadata
                                SET LastLoadDateValue ='{timestamp}'
                                WHERE TableID = '{Table_ID}'
                        """)
                    
                except Exception as e:
                    print('Cant Merge Table and View', e)
                    logger.info(f"Unable to Merge {e}")
        except Exception as e:
            print('Cant check autoskipper',e)
            logger.info(f"Unable to Check Autoskipper {e}")
        
        UpdatePipelineStatusAndTime(Table_ID, 'Bronze')
        UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", "NULL", "NULL", 0, "Merge-Target Table")

        
except Exception as e:
    logger.info("Unable to extract the data from the file.")
    print(e)
    UpdateFailedStatus(Table_ID,Zone)
    UpdatePipelineStatusAndTime(Table_ID, Zone)
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Extract Data & Load")