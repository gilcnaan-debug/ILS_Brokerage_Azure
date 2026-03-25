# Databricks notebook source
# MAGIC %md #Silver_Carrier_Onboarding_Status

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Carrier_Onboarding_Status table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 11/12/2023
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

ErrorLogger = ErrorLogs("Silver_Carrier_Onboarding_Status_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ7'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName


# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Carrier_Onboarding_Status table

# COMMAND ----------

# Select the Carrier_onboarding_status table from the bronze layer for Relay source and perform required transformations.
try:
    DF_Carrier = spark.sql('''
                                select
                                sc.DW_Carrier_ID AS DW_Carrier_ID,
                                "2" as Sourcesystem_ID,
                                cop.dot_number as DOT_Number,
                                cop.master_carrier_id as Master_Carrier_ID,
                                cop.name as  Carrier_Name,
                                cop.`approved?` as  Is_Approved ,
                                cast(cop.approval as string) as Approval_Description,
                                cop.current_status as Carrier_Status,
                                cop.`denied?` as Is_Denied,
                                CAST(cop.denial AS String)AS Denial_Description,
                                cop.`duplicated?` AS Is_Duplicated,
                                CAST(cop.initial_nomination AS String) AS Nomination_Description,
                                cop.nomination_count AS Nomination_Count,
                                cop.`onboarded?` AS Is_Onboarded,
                                CAST(cop.onboarding AS String) AS Onboarded_Description,
                                cop.`onboard_prohibited?` AS Is_Onboard_Prohibited,
                                CAST(cop.onboard_prohibition AS String) AS Prohibition_Reason,
                                "Relay" as Sourcesystem_Name,
                                "Databricks" as Created_By,
                                Current_timestamp() as Created_Date,
                                "Databricks" as Last_Modified_By,
                                Current_timestamp() as Last_Modified_Date
                                from bronze.carrier_nomination_and_onboarding_projection cop left join Silver.Silver_Carrier sc on cop.name = sc.Carrier_Name where sc.Sourcesystem_ID='2'
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Carrier_onboarding_status table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Carrier =DF_Carrier.dropDuplicates(["DW_Carrier_ID","DOT_Number","Carrier_Name","Master_Carrier_ID"])

except Exception as e:
    logger.info(f"Unable to drop duplicates for the Carrier_onboarding_status table from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Carrier_Onboarding_Status

# COMMAND ----------

#Create the Hashkey to merge the data to Silver_Carrier_Onboarding_Status table
try:
    Hashkey_Merge = ["DW_Carrier_ID","DOT_Number","Carrier_Name","Master_Carrier_ID"]

    DF_Carrier = DF_Carrier.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for Silver_Carrier_Onboarding_Status {e}")
    print(e)

#Create a Temporary view for the Silver_Carrier_Onboarding_Status table
try:
    DF_Carrier.createOrReplaceTempView('VW_Carrier')
except Exception as e:
    logger.info(f"Unable to create temporary view for the Silver_Carrier_Onboarding_Status table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Carrier_Onboarding_Status as CT using VW_Carrier as CS ON CS.DOT_Number=CT.DOT_Number AND CT.DW_Carrier_ID=CS.DW_Carrier_ID When MATCHED AND CS.Hashkey != CT.Hashkey  THEN UPDATE SET 
                        CT.DW_Carrier_ID = CS.DW_Carrier_ID,
                        CT.DOT_Number = CS.DOT_Number,
                        CT.Carrier_Name=CS.Carrier_Name,
                        CT.Master_Carrier_ID=CS.Master_Carrier_ID,
                        CT.Is_Approved = CS.Is_Approved,
                        CT.Approval_Description = CS.Approval_Description,
                        CT.Carrier_Status = CS.Carrier_Status,
                        CT.Is_Denied = CS.Is_Denied,
                        CT.Denial_Description = CS.Denial_Description,
                        CT.Is_Duplicated = CS.Is_Duplicated,
                        CT.Nomination_Description = CS.Nomination_Description,
                        CT.Nomination_Count = CS.Nomination_Count,
                        CT.Is_Onboarded = CS.Is_Onboarded,
                        CT.Onboarded_Description = CS.Onboarded_Description,
                        CT.Is_Onboard_Prohibited = CS.Is_Onboard_Prohibited,
                        CT.Prohibition_Reason = CS.Prohibition_Reason,
                        CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                        CT.Sourcesystem_Name = CS.Sourcesystem_Name,
                        CT.Created_By = CS.Created_By,
                        CT.Created_Date = CS.Created_Date,
                        CT.Last_Modified_By = CS.Last_Modified_By, 
                        CT.Last_Modified_Date = CS.Last_Modified_Date,
                        CT.Hashkey = CS.Hashkey
                    
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   CT.DW_Carrier_ID,
                        CT.DOT_Number,
                        CT.Carrier_Name,
                        CT.Master_Carrier_ID,
                        CT.Is_Approved,
                        CT.Approval_Description,
                        CT.Carrier_Status,
                        CT.Is_Denied,
                        CT.Denial_Description,
                        CT.Is_Duplicated,
                        CT.Nomination_Description,
                        CT.Nomination_Count,
                        CT.Is_Onboarded,
                        CT.Onboarded_Description,
                        CT.Is_Onboard_Prohibited,
                        CT.Prohibition_Reason,
                        CT.Sourcesystem_ID 	,
                        CT.Sourcesystem_Name 	,
                        CT.Created_By 	,
                        CT.Created_Date 	,
                        CT.Last_Modified_By 	,
                        CT.Last_Modified_Date 	,
                        CT.Hashkey	
                        )
                        VALUES
                        (
                            CS.DW_Carrier_ID,
                            CS.DOT_Number,
                            CS.Carrier_Name,
                            CS.Master_Carrier_ID,
                            CS.Is_Approved,
                            CS.Approval_Description,
                            CS.Carrier_Status,
                            CS.Is_Denied,
                            CS.Denial_Description,
                            CS.Is_Duplicated,
                            CS.Nomination_Description,
                            CS.Nomination_Count,
                            CS.Is_Onboarded,
                            CS.Onboarded_Description,
                            CS.Is_Onboard_Prohibited,
                            CS.Prohibition_Reason,
                            CS.Sourcesystem_ID, 
                            CS.Sourcesystem_Name, 
                            CS.Created_By, 
                            CS.Created_Date, 
                            CS.Last_Modified_By, 
                            CS.Last_Modified_Date,  
                            CS.Hashkey
                            
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Carrier_Onboarding_Status"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Carrier_Onboarding_Status table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Carrier_Onboarding_Status')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

