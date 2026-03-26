# Databricks notebook source
# MAGIC %md
# MAGIC ##IMPORTING REQ PACKAGES

# COMMAND ----------

from pyspark.sql.functions import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, explode
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC ##CALLING UTILITIES

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ## CALLING ERROR LOGGER

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ##CREATING ERROR LOGGER PATH

# COMMAND ----------

ErrorLogger = ErrorLogs("Silver_slack_Conversaions_History_log_Silver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##RETRIEVEING CATALOG AND METADATA INFORMATION

# COMMAND ----------

CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

try:
    TableID = 'SP-2'
    DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
except Exception as e:
    logger.info("unable to fetch details",e)
    print(e)

# COMMAND ----------

try:
    TablesList = DF_Metadata.select(col("TableID")).collect()
    TableName = (
        DF_Metadata.select(col("SourceTableName"))
        .where(col("TableID") == TableID)
        .collect()[0]
        .SourceTableName
    )
    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == TableID).collect()[0].MergeKey) 
    LoadType = (DF_Metadata.select(col("LoadType")).where(col("TableID") == TableID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('UnixTime')).where(col('TableID') == TableID).collect()[0].UnixTime)
except Exception as e:
    logger.info("Unable to fetch details from metadata")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##ERROR LOGGER VARIABLES

# COMMAND ----------

try:
    Job_ID = "SM-2"
    Notebook_ID = "SM-Automation-Silver"
    Zone = "Silver"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ##TRANSFORMATIONS

# COMMAND ----------

try:
    DF_Shipper_mapping_Silver = spark.sql("""
                                        SELECT
                                        ch.Customer_Master,
                                        ch.Verified_AM AS Shipper_Rep,
                                        ch.Verified_Office AS Office,
                                        ch.Sales_Resource AS Sales_Rep,
                                        ch.Email_Address,
                                        ch.Hubspot_Company_Name,
                                        ch.Hubspot_AM AS Hubspot_Account_Manager,
                                        ch.Slack_Channel_Name,
                                        ch.Vertical,
                                        ch.Merge_Key,
                                        ch.Inserted_At,
                                        ch.Updated_At
                                        FROM bronze.shipper_mapping ch
                                        WHERE ch.Is_deleted = 0
                                        """)
    DF_Shipper_mapping_Silver.createOrReplaceTempView('VW_Shipper_mapping')
except Exception as e:
    logger.info("unable to fetch data from bronze")
    Error_Statement = f"Error-{e}"
    raise RuntimeError(f"Job failed: {Error_Statement}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##MERGE INTO THE TARGET TABLE

# COMMAND ----------


try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            
            # Count the number of rows
            Rowcount = DF_Shipper_mapping_Silver.count()
            Truncate_query = f'TRUNCATE TABLE silver.silver_shipper_mapping'
            spark.sql(Truncate_query)
            # Define the merge query
            MergeQuery = '''
                MERGE INTO silver.silver_shipper_mapping AS target
                USING VW_Shipper_mapping AS source
                ON target.Merge_Key = source.Merge_Key
                WHEN MATCHED THEN
                    UPDATE SET
                        target.Customer_Master = source.Customer_Master,
                        target.Shipper_Rep = source.Shipper_Rep,
                        target.Office = source.Office,
                        target.Sales_Rep = source.Sales_Rep,
                        target.Email_Address = source.Email_Address,
                        target.Hubspot_Company_Name = source.Hubspot_Company_Name,
                        target.Hubspot_Account_Manager = source.Hubspot_Account_Manager,
                        target.Slack_Channel_Name = source.Slack_Channel_Name,  
                        target.Vertical = source.Vertical,
                        target.Updated_At = source.Updated_At
                WHEN NOT MATCHED THEN
                    INSERT (
                        Customer_Master,
                        Shipper_Rep,
                        Office,
                        Sales_Rep,
                        Email_Address,
                        Hubspot_Company_Name,
                        Hubspot_Account_Manager,
                        Slack_Channel_Name, 
                        Vertical,
                        Merge_Key,
                        Inserted_At,
                        Updated_At
                    )
                    VALUES (
                        source.Customer_Master,
                        source.Shipper_Rep,
                        source.Office,
                        source.Sales_Rep,
                        source.Email_Address,
                        source.Hubspot_Company_Name,
                        source.Hubspot_Account_Manager,
                        source.Slack_Channel_Name,  
                        source.Vertical,
                        source.Merge_Key,
                        source.Inserted_At,
                        source.Updated_At
                    )
            '''

            spark.sql(MergeQuery)
            print('Loaded ' + TableName)
            UpdatePipelineStatusAndTime(TableID, 'silver')
            logger.info('Successfully loaded the ' + TableName + ' to silver')
        except Exception as e:
            Error_Statement = str(e).replace("'", "''")
            UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, Table_Name, Zone, "Failed", Error_Statement, "Impactable Issue", 1, "Merge-Target Table")
            logger.info('Failed for silver load')
            UpdateFailedStatus(TableID, 'silver')
            logger.info('Updated the Metadata for Failed Status ' + TableName)
            print('Unable to load ' + TableName)
            print(e)
        UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, Table_Name, Zone, "Succeeded", "NULL", "NULL", 0, "Merge-Target Table")
except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    raise RuntimeError(f"Job failed: {Error_Statement}")
    logger.error(f"Table is already loaded: {str(e)}")
    print(e)
