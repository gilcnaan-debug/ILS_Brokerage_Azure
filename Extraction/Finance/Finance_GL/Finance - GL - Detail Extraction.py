# Databricks notebook source
# MAGIC %md
# MAGIC ## Finance_GLDetail
# MAGIC * **Description:** Extract the Details files from the azure blob and load that into the Bronze Schema  GLDetail Table. 
# MAGIC * **Created Date:** 15/07/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 15/07/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing packages

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from io import BytesIO
from decimal import Decimal
import os
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calling Utilities Notebook

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
# MAGIC #### Delcaring the Error Logger variables
# MAGIC

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("Finance_Extraction_24hr_Refresh_GLDetail")
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
# MAGIC ####Get Metadata detials

# COMMAND ----------


try:
    DF_Metadata= spark.sql("SELECT * FROM Metadata.MasterMetadata where TableID='F13' and IsActive='1'")
    TableID  =  (DF_Metadata.select(col("TableID")).where(col("TableID") == "F13").collect()[0].TableID)
    TableName = (DF_Metadata.select(col("SourceTableName")).where(col("TableID") == TableID).collect()[0].SourceTableName)
    ErrorPath = (DF_Metadata.select(col("ErrorLogPath")).where(col("TableID") == TableID).collect()[0].ErrorLogPath)
    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == TableID).collect()[0].MergeKey) 
    LoadType = (DF_Metadata.select(col("LoadType")).where(col("TableID") == TableID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue)
    Zone =(DF_Metadata.select(col('Zone')).where(col('TableID') == TableID).collect()[0].Zone)
    Job_ID = (DF_Metadata.select(col('Job_ID')).where(col('TableID') == TableID).collect()[0].Job_ID)
    Notebook_ID = (DF_Metadata.select(col('NB_ID')).where(col('TableID') == TableID).collect()[0].NB_ID)

except Exception as e:
    logger.info("Unable to fetch metadata details", e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retrive Secrets From Azure Key Vault 

# COMMAND ----------

# Retrieve the secrets from Azure Key Vault
try:
    tenant_id = dbutils.secrets.get(scope="NFI_Finance_Storage", key="TenantID")
    client_id = dbutils.secrets.get(scope="NFI_Finance_Storage", key="ClientID")
    client_secret = dbutils.secrets.get(scope="NFI_Finance_Storage", key="Client-SecretID")
except Exception as e:
    logger.info("Unable to fetch creds from azure keyvault")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Connection String for Azure Blob Storage 

# COMMAND ----------

#Updating the pipeline start time
UpdatePipelineStartTime(TableID, Zone)

# Create a credential using your AD credentials
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Storage account URL
storage_account_url = "https://dlsfindatacomeus1.blob.core.windows.net/"

# Create the Blob service client
blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)

def test_blob_service_connection(blob_service_client):
    try:
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
# MAGIC #### Extract the FileName from Source Incrementally

# COMMAND ----------


try: 
    container_name = "finance"
    folder_path = "Finance Prod/NFI_ILS_GLDetail_"

    # Create a container client
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs in the container and filter by folder path
    blob_list = container_client.list_blobs(name_starts_with=folder_path)

    detail_blobs = [result.name for result in blob_list]

    latest_blobs = []
    for blob_name in detail_blobs:
        timestamp_str = blob_name.split("_")[-1].split(".")[0]
        fiscal_year = blob_name.split("GLDetail_")[1].split("_")[0]
        
        timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        if timestamp > MaxLoadDate:
            latest_blobs.append((blob_name, fiscal_year, timestamp))
    
    latest_blobs.sort(key=lambda x: (x[1], x[2]))
    UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Incremental Files Extraction")

except Exception as e:
    logger.info("Unable to get the incremental files from azure")
    print(e)
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Incremental Files Extraction")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a Variable for Merge Query 

# COMMAND ----------

 # Define the merge query
MergeQuery = '''
                    MERGE INTO {0}.{1} AS GLD
                    USING VW_GLDetail AS VW ON GLD.{2}=VW.{2}
                    WHEN MATCHED AND GLD.HASHKEY!= VW.HASHKEY  
                    THEN UPDATE SET
                        GLD.COMPANY = VW.COMPANY,
                        GLD.FISCAL_YEAR = VW.FISCAL_YEAR,
                        GLD.ACCT_PERIOD = VW.ACCT_PERIOD,
                        GLD.JE_NUMBER = VW.JE_NUMBER,
                        GLD.SOURCE_SYSTEM = VW.SOURCE_SYSTEM,
                        GLD.JE_TYPE = VW.JE_TYPE,
                        GLD.ACCT_UNIT = VW.ACCT_UNIT,
                        GLD.ACCT_UNIT_DESCR = VW.ACCT_UNIT_DESCR,
                        GLD.ACCT_SUB = VW.ACCT_SUB,
                        GLD.ACCOUNT_DESC = VW.ACCOUNT_DESC,
                        GLD.SOURCE_CODE = VW.SOURCE_CODE,
                        GLD.REFERENCE = VW.REFERENCE,
                        GLD.SOURCE_DESC = VW.SOURCE_DESC,
                        GLD.AUTO_REV = VW.AUTO_REV,
                        GLD.POSTING_DATE = VW.POSTING_DATE,
                        GLD.TRAN_AMOUNT = VW.TRAN_AMOUNT,
                        GLD.BASE_AMOUNT = VW.BASE_AMOUNT,
                        GLD.RPT_AMOUNT_1 = VW.RPT_AMOUNT_1,
                        GLD.UNITS_AMOUNT = VW.UNITS_AMOUNT,
                        GLD.TRANS_DESC = VW.TRANS_DESC,
                        GLD.CURRENCY_CODE = VW.CURRENCY_CODE,
                        GLD.HASHKEY = VW.HASHKEY,
                        GLD.SOURCE_FILE_NAME = VW.SOURCE_FILE_NAME,
                        GLD.LAST_MODIFIED_DATE = current_timestamp(),
                        GLD.LAST_MODIFIED_BY = 'Databricks',
                        GLD.IS_DELETED = 0
                    WHEN NOT MATCHED THEN
                    INSERT (
                        OBJ_ID,
                        COMPANY,
                        FISCAL_YEAR,
                        ACCT_PERIOD,
                        JE_NUMBER,
                        SOURCE_SYSTEM,
                        JE_TYPE,
                        ACCT_UNIT,
                        ACCT_UNIT_DESCR,
                        ACCT_SUB,
                        ACCOUNT_DESC,
                        SOURCE_CODE,
                        REFERENCE,
                        SOURCE_DESC,
                        AUTO_REV,
                        POSTING_DATE,
                        TRAN_AMOUNT,
                        BASE_AMOUNT,
                        RPT_AMOUNT_1,
                        UNITS_AMOUNT,
                        TRANS_DESC,
                        CURRENCY_CODE,
                        HASHKEY,
                        SOURCE_FILE_NAME,
                        SOURCESYSTEM_ID ,
                        SOURCESYSTEM_NAME,
                        CREATED_DATE,
                        CREATED_BY,
                        LAST_MODIFIED_DATE,
                        LAST_MODIFIED_BY,
                        IS_DELETED

                    )
                    VALUES (
                        VW.OBJ_ID,
                        VW.COMPANY,
                        VW.FISCAL_YEAR,
                        VW.ACCT_PERIOD,
                        VW.JE_NUMBER,
                        VW.SOURCE_SYSTEM,
                        VW.JE_TYPE,
                        VW.ACCT_UNIT,
                        VW.ACCT_UNIT_DESCR,
                        VW.ACCT_SUB,
                        VW.ACCOUNT_DESC,
                        VW.SOURCE_CODE,
                        VW.REFERENCE,
                        VW.SOURCE_DESC,
                        VW.AUTO_REV,
                        VW.POSTING_DATE,
                        VW.TRAN_AMOUNT,
                        VW.BASE_AMOUNT,
                        VW.RPT_AMOUNT_1,
                        VW.UNITS_AMOUNT,
                        VW.TRANS_DESC,
                        VW.CURRENCY_CODE,
                        VW.HASHKEY,
                        VW.SOURCE_FILE_NAME,
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
        
        decimal_columns = ['TRAN_AMOUNT', 'BASE_AMOUNT', 'RPT_AMOUNT_1', 'UNITS_AMOUNT']
        converters = {col: Decimal for col in decimal_columns}

        df = pd.read_csv(BytesIO(downloaded_blob), sep='|', encoding='ISO-8859-1', decimal='.', converters=converters)

        existing_columns = ['OBJ_ID','COMPANY','FISCAL_YEAR','ACCT_PERIOD','JE_NUMBER','SOURCE_SYSTEM','JE_TYPE','ACCT_UNIT','ACCT_UNIT_DESCR','ACCT_SUB','ACCOUNT_DESC','SOURCE_CODE','REFERENCE','SOURCE_DESC','AUTO_REV','POSTING_DATE','TRAN_AMOUNT', 'BASE_AMOUNT', 'RPT_AMOUNT_1', 'UNITS_AMOUNT', 'TRANS_DESC','CURRENCY_CODE']

        new_columns = [col for col in df.columns if col not in existing_columns]
        
        if new_columns:
            error_message = f"New columns found in file {blob_name}: {', '.join(new_columns)}"
            print(error_message)
            UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Failed", error_message, "NotIgnorable", 1, "New Columns Added")
            logger.info(error_message)
            raise
    
        filename = os.path.basename(blob_name)  # Get the filename from the blob name
        df['SOURCE_FILE_NAME'] = filename
        pd_df = df[['OBJ_ID','COMPANY','FISCAL_YEAR','ACCT_PERIOD','JE_NUMBER','SOURCE_SYSTEM','JE_TYPE','ACCT_UNIT','ACCT_UNIT_DESCR','ACCT_SUB','ACCOUNT_DESC','SOURCE_CODE','REFERENCE','SOURCE_DESC','AUTO_REV','POSTING_DATE','TRAN_AMOUNT', 'BASE_AMOUNT', 'RPT_AMOUNT_1', 'UNITS_AMOUNT', 'TRANS_DESC','CURRENCY_CODE','SOURCE_FILE_NAME']]

        schema = StructType([
            StructField("OBJ_ID", IntegerType(), True),
            StructField("COMPANY", IntegerType(), True),
            StructField("FISCAL_YEAR", ShortType(), True),
            StructField("ACCT_PERIOD", IntegerType(), True),
            StructField("JE_NUMBER", IntegerType(), True),
            StructField("SOURCE_SYSTEM", StringType(), True),
            StructField("JE_TYPE", StringType(), True),
            StructField("ACCT_UNIT", IntegerType(), True),
            StructField("ACCT_UNIT_DESCR", StringType(), True),
            StructField("ACCT_SUB", StringType(), True),
            StructField("ACCOUNT_DESC", StringType(), True),
            StructField("SOURCE_CODE", StringType(), True),
            StructField("REFERENCE", StringType(), True),
            StructField("SOURCE_DESC", StringType(), True),
            StructField("AUTO_REV", StringType(), True),
            StructField("POSTING_DATE", StringType(), True),
            StructField("TRAN_AMOUNT", DecimalType(18,2), True),
            StructField("BASE_AMOUNT",  DecimalType(18,2), True),
            StructField("RPT_AMOUNT_1", DecimalType(18,2), True),
            StructField("UNITS_AMOUNT",  DecimalType(18,2), True),
            StructField("TRANS_DESC", StringType(), True),
            StructField("CURRENCY_CODE", StringType(), True),
            StructField("SOURCE_FILE_NAME", StringType(),True)     
        ])
        # Convert the pandas DataFrame to a list of dictionaries
        data = pd_df.to_dict('records')

        # Convert the list of dictionaries to a list of Row objects
        rows = [Row(**dict_) for dict_ in data]

        # Create the Spark DataFrame
        df = spark.createDataFrame(rows, schema)

        columns_to_replace = [
            "OBJ_ID",
            "COMPANY",
            "FISCAL_YEAR",
            "ACCT_PERIOD",
            "JE_NUMBER",
            "SOURCE_SYSTEM",
            "JE_TYPE",
            "ACCT_UNIT",
            "ACCT_UNIT_DESCR",
            "ACCT_SUB",
            "ACCOUNT_DESC",
            "SOURCE_CODE",
            "REFERENCE",
            "SOURCE_DESC",
            "AUTO_REV",
            "POSTING_DATE",
            "TRAN_AMOUNT",
            "BASE_AMOUNT",
            "RPT_AMOUNT_1",
            "UNITS_AMOUNT",
            "TRANS_DESC",
            "CURRENCY_CODE",
            "SOURCE_FILE_NAME"
        ]

        for col_name in columns_to_replace:
            df = df.withColumn(col_name, when(col(col_name) == "NaN", None).otherwise(col(col_name)))

        DF_GLDetail = df.withColumn("POSTING_DATE", col("POSTING_DATE").cast("timestamp")) 

        try:
            Hashkey_Columns = ["COMPANY","FISCAL_YEAR","ACCT_PERIOD","JE_NUMBER","SOURCE_SYSTEM","JE_TYPE","ACCT_UNIT","ACCT_UNIT_DESCR","ACCT_SUB","ACCOUNT_DESC","SOURCE_CODE","REFERENCE","SOURCE_DESC","AUTO_REV","POSTING_DATE","TRAN_AMOUNT","BASE_AMOUNT","RPT_AMOUNT_1","UNITS_AMOUNT","TRANS_DESC","CURRENCY_CODE"]

            DF_GLDetail = DF_GLDetail.withColumn("HASHKEY",md5(concat_ws("",*Hashkey_Columns)))
    
        except Exception as e:
            logger.info(f"Unable to create Hashkey for summ {e}")
            print(e)
        try:
           
            DF_GLDetail.dropDuplicates(['OBJ_ID'])
    
        except Exception as e:
            logger.info(f"Unable to Drop Dupliactes from the dataframe {e}")
            print(e)

        try: 
            DF_GLDetail.createOrReplaceTempView('VW_GLDetail')
        except Exception as e:
            logger.info(f"Unable to Create View {e}")
            print(e)

        try:
            AutoSkipperCheck = AutoSkipper(TableID,Zone)

            if AutoSkipperCheck == 0:
            
                try:
                    print('Loading ' + TableName + ' ' + filename)            

                    spark.sql(MergeQuery)

                    
                    try:
                        IsDeletedQuery = '''UPDATE {0}.{1}
                        SET IS_DELETED = 1
                        WHERE {2} IN (
                            SELECT GLD.{2}
                            FROM {0}.{1} GLD
                            Left JOIN VW_GLDetail VW ON GLD.{2} = VW.{2}
                            WHERE VW.{2} IS NULL AND GLD.FISCAL_YEAR = {3})

                    '''
                        IsDeletedQuery = IsDeletedQuery.format(Zone,TableName,MergeKey,fiscal_year)
                        spark.sql(IsDeletedQuery)

                    except Exception as e:
                        print('Cant Update IsDeleted', e)
                        logger.info(f"Unable to Update IsDeleted {e}")
                    
                    Max_datetime = timestamp
                    
                    spark.sql(f"""
                              UPDATE metadata.mastermetadata
                                SET LastLoadDateValue ='{Max_datetime}'
                                WHERE TableID = '{TableID}'
                        """)
                    
                except Exception as e:
                    print('Cant Merge Table and View', e)
                    logger.info(f"Unable to Merge {e}")

                
        except Exception as e:
            print('Cant check autoskipper',e)
            logger.info(f"Unable to Check Autoskipper {e}")
        
    UpdatePipelineStatusAndTime(TableID, Zone)
    UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Extract Data & Load")

        
except Exception as e:
    logger.info("Unable to extract the data from the file.")
    print(e)
    UpdateFailedStatus(TableID,Zone)
    UpdatePipelineStatusAndTime(TableID, Zone)
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Extract Data & Load")

# COMMAND ----------

