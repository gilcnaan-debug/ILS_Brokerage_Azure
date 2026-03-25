# Databricks notebook source
# MAGIC %md #Silver_Tender_Events

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Tender_Events table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 01/29/2024
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** - 
# MAGIC * **Modified By:**
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import the required packages

# COMMAND ----------

##Import all the required packages
from pyspark.sql.functions import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize the Utility notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initialize the Logger notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Create variables to store error log information

ErrorLogger = ErrorLogs("Silver_Tender_Events_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ16'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Employee table

# COMMAND ----------

# Select the Employee table from the bronze layer for Relay source and perform required transformations.
try:
    DF_Tender_Events = spark.sql('''
                                    select 
                                    "2" as Sourcesystem_ID,
                                    a.tender_id as Tender_ID,
                                    b.event_number as Event_Number,
                                    b.customer as Customer,
                                    b.shipment_id as Shipment_ID,
                                    b.tendered_at as Tendered_At,
                                    b.event_type as Event_Type,
                                    b.tenderer as Tenderer,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Last_Modified_By,
                                    Current_timestamp() as Last_Modified_Date
                                    from bronze.integration_tender_mapped_projection_v2 b left join bronze.tendering_acceptance a on a.shipment_id=b.shipment_id
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Tender_Events table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Tender_Events =DF_Tender_Events.dropDuplicates(['Event_Number','Shipment_ID','Tender_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Tender_Events table from Relay {e}")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Tender_Address

# COMMAND ----------

#Create the Hashkey to merge the data to Silver_Employee table
try:
    Hashkey_Merge = ["Event_Number","Shipment_ID","Tender_ID"]

    DF_Events = DF_Tender_Events.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_Tender_Events {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Events =DF_Events.dropDuplicates(['Event_Number','Shipment_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Tender_Events table from Relay {e}")
    print(e)

#Create a Temporary view for the Silver_Tender_Events table
try:
    DF_Events.createOrReplaceTempView('VW_Tender_Events')
except Exception as e:
    logger.info(f"Unable to create temporary view for the silver_Tender_Events table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.silver_Tender_Events as CT using VW_Tender_Events as CS ON  CT.Event_Number = CS.Event_Number and CT.Shipment_ID=CS.Shipment_ID  WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                    CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                    CT.Hashkey = CS.Hashkey,
                    CT.Tender_ID = CS.Tender_ID,
                    CT.Event_Number = CS.Event_Number,
                    CT.Customer = CS.Customer,
                    CT.Shipment_ID = CS.Shipment_ID,
                    CT.Tendered_At = CS.Tendered_At, 
                    CT.Event_Type = CS.Event_Type,
                    CT.Tenderer = CS.Tenderer, 
                    CT.Sourcesystem_Name = CS.Sourcesystem_Name,
                    CT.Created_By = CS.Created_By,
                    CT.Created_Date = CS.Created_Date,
                    CT.Last_Modified_By = CS.Last_Modified_By, 
                    CT.Last_Modified_date = CS.Last_Modified_Date
                    
                    WHEN NOT MATCHED  
                    THEN INSERT 
                    (   
                        CT.Sourcesystem_ID,
                        CT.Hashkey,
                        CT.Tender_ID,
                        CT.Event_Number,
                        CT.Customer,
                        CT.Shipment_ID,
                        CT.Tendered_At,
                        CT.Event_Type,
                        CT.Tenderer,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date
                        
                        )
                        VALUES
                        (
                            CS.Sourcesystem_ID,
                            CS.Hashkey,
                            CS.Tender_ID,
                            CS.Event_Number,
                            CS.Customer,
                            CS.Shipment_ID,
                            CS.Tendered_At,
                            CS.Event_Type,
                            CS.Tenderer,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                            
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.silver_Tender_Events"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to silver_Tender_Events table")
            print(e)
except Exception as e:
    logger.info('Failed for silver_Tender_Events')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

