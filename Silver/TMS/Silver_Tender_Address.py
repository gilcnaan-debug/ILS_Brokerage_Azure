# Databricks notebook source
# MAGIC %md #Silver_Tender_Address

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Tender_Address table with joins and transformations- Incremental load
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

ErrorLogger = ErrorLogs("Silver_Tender_Addres_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ15'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Employee table

# COMMAND ----------

# Select the Employee table from the bronze layer for Relay source and perform required transformations.
try:
    DF_Tender_Address = spark.sql('''
                                    select 
                                    "2" as Sourcesystem_ID,
                                    tod.tender_id as Tender_ID,
                                    tod.relay_reference_number as Load_Number,
                                    tod.shipper_id as Shipper_ID,
                                    tod.shipper_name as Shipper_Name,
                                    tod.origin_address_1 as Origin_Address_1,
                                    tod.origin_address_2 as Origin_Address_2,
                                    tod.origin_administrative_region as Origin_State_Code,
                                    tod.origin_city as Origin_city,
                                    tod.origin_country_code as Origin_Country_Code,
                                    tod.receiver_id as Receiver_ID,
                                    tod.receiver_name as Receiver_Name,
                                    tod.destination_address_1 as Destination_Address_1,
                                    tod.destination_address_2 as Destination_Address_2,
                                    tod.destination_city as Destination_City,
                                    tod.destination_administrative_region as Destination_State_Code,
                                    tod.destination_country_code as Destination_Country_Code,
                                    tod.`cancelled?` as Is_Cancelled,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Last_Modified_By,
                                    Current_timestamp() as Last_Modified_Date
                                    from bronze.tendering_origin_destination tod
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Tender_Address table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Tender_Address =DF_Tender_Address.dropDuplicates(['Tender_ID','Load_Number','Shipper_ID','Receiver_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Tender_Address table from Relay {e}")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Tender_Address

# COMMAND ----------

#Create the Hashkey to merge the data to Silver_Tender_Address table
try:
    Hashkey_Merge = ["Tender_ID","Load_Number","Shipper_ID","Receiver_ID"]

    DF_Tender_Address = DF_Tender_Address.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_Tender_Address {e}")
    print(e)

#Create a Temporary view for the Silver_Tender_Address table
try:
    DF_Tender_Address.createOrReplaceTempView('VW_Tender_Address')
except Exception as e:
    logger.info(f"Unable to create temporary view for the Silver_Tender_Address table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Tender_Address as CT using VW_Tender_Address as CS ON CS.Load_Number=CT.Load_Number WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                    CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                    CT.Hashkey = CS.Hashkey,
                    CT.Tender_ID = CS.Tender_ID,
                    CT.Load_Number = CS.Load_Number,
                    CT.Shipper_ID = CS.Shipper_ID,
                    CT.Shipper_Name = CS.Shipper_Name,
                    CT.Origin_Address_1 = CS.Origin_Address_1, 
                    CT.Origin_Address_2 = CS.Origin_Address_2,
                    CT.Origin_State_Code = CS.Origin_State_Code, 
                    CT.Origin_city = CS.Origin_city, 
                    CT.Origin_Country_Code = CS.Origin_Country_Code,
                    CT.Receiver_ID = CS.Receiver_ID,
                    CT.Receiver_Name = CS.Receiver_Name,
                    CT.Destination_Address_1 = CS.Destination_Address_1,
                    CT.Destination_Address_2 = CS.Destination_Address_2,
                    CT.Destination_City = CS.Destination_City,
                    CT.Destination_State_Code = CS.Destination_State_Code,
                    CT.Destination_Country_Code = CS.Destination_Country_Code,
                    CT.Is_Cancelled = CS.Is_Cancelled,
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
                        CT.Load_Number,
                        CT.Shipper_ID,
                        CT.Shipper_Name,
                        CT.Origin_Address_1,
                        CT.Origin_Address_2,
                        CT.Origin_State_Code,
                        CT.Origin_city,
                        CT.Origin_Country_Code,
                        CT.Receiver_ID,
                        CT.Receiver_Name,
                        CT.Destination_Address_1,
                        CT.Destination_Address_2,
                        CT.Destination_City,
                        CT.Destination_State_Code,
                        CT.Destination_Country_Code,
                        CT.Is_Cancelled,
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
                            CS.Load_Number,
                            CS.Shipper_ID,
                            CS.Shipper_Name,
                            CS.Origin_Address_1,
                            CS.Origin_Address_2,
                            CS.Origin_State_Code,
                            CS.Origin_city,
                            CS.Origin_Country_Code,
                            CS.Receiver_ID,
                            CS.Receiver_Name,
                            CS.Destination_Address_1,
                            CS.Destination_Address_2,
                            CS.Destination_City,
                            CS.Destination_State_Code,
                            CS.Destination_Country_Code,
                            CS.Is_Cancelled,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                            
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Tender_Address"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Tender_Address table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Tender_Address')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

