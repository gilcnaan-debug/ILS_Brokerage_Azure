# Databricks notebook source
# MAGIC %md #Silver_Tender_Manual_Draft

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

ErrorLogger = ErrorLogs("Silver_Tender_Manul_Draft_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ17'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Tender_Manual_Draft table

# COMMAND ----------

# Select the Tender_Manual_Draft_Table table from the bronze layer for Relay source and perform required transformations.
try:
    DF_Tender_Draft = spark.sql('''
                                    select 
                                    "2" as Sourcesystem_ID,
                                    ta.tender_id as  Tender_ID,
                                    tm.tender_draft_id as Tender_Draft_ID,
                                    td.relay_reference_number as Load_Number,
                                    tm.draft_by_user_id as  Tender_Drafted_by,
                                    tm.started_at as Tender_Started_At,
                                    tm.completed_at as Tender_Completed_At,
                                    tm.total_time_ms as Draft_TimeTaken_ms,
                                    tm.scratch_pad_notes_used as Is_Scratchpad_Notes_Used,
                                    tm.origin_type as TenderMade_Type,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Last_Modified_By,
                                    Current_timestamp() as Last_Modified_Date
                                    from bronze.tendering_manual_tender_entry_usage  tm left join bronze.tendering_tender_draft_id  td on tm.tender_draft_id= td.tender_draft_id
                                    left join  bronze.tendering_acceptance  ta  on td.relay_reference_number=  ta.relay_reference_number
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for DF_Tender_Draft table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Tender_Draft =DF_Tender_Draft.dropDuplicates(['Tender_ID','Tender_Draft_ID','Load_Number'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the DF_Tender_Draft table from Relay {e}")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Tender_Address

# COMMAND ----------

#Create the Hashkey to merge the data to Silver_Employee table
try:
    Hashkey_Merge = ["Tender_ID","Tender_Draft_ID","Load_Number"]

    DF_Tender_Draft = DF_Tender_Draft.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for Silver_Tender_Manual_Draft {e}")
    print(e)

#Create a Temporary view for the Silver_Tender_Events table
try:
    DF_Tender_Draft.createOrReplaceTempView('VW_Tender_Draft')
except Exception as e:
    logger.info(f"Unable to create temporary view for the Silver_Tender_Manual_Draft table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Tender_Manual_Draft as CT using VW_Tender_Draft as CS on CS.Tender_Draft_ID=CT.Tender_Draft_ID WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                    CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                    CT.Hashkey = CS.Hashkey,
                    CT.Tender_ID = CS.Tender_ID,
                    CT.Tender_Draft_ID = CS.Tender_Draft_ID,
                    CT.Load_Number = CS.Load_Number,
                    CT.Tender_Drafted_by = CS.Tender_Drafted_by,
                    CT.Tender_Started_At = CS.Tender_Started_At, 
                    CT.Tender_Completed_At = CS.Tender_Completed_At,
                    CT.Draft_TimeTaken_ms = CS.Draft_TimeTaken_ms, 
                    CT.Is_Scratchpad_Notes_Used=CS.Is_Scratchpad_Notes_Used,
                    CT.TenderMade_Type=CS.TenderMade_Type,
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
                        CT.Tender_Draft_ID,
                        CT.Load_Number,
                        CT.Tender_Drafted_by,
                        CT.Tender_Started_At,
                        CT.Tender_Completed_At,
                        CT.Draft_TimeTaken_ms,
                        CT.Is_Scratchpad_Notes_Used,
                        CT.TenderMade_Type,
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
                            CS.Tender_Draft_ID,
                            CS.Load_Number,
                            CS.Tender_Drafted_by,
                            CS.Tender_Started_At,
                            CS.Tender_Completed_At,
                            CS.Draft_TimeTaken_ms,
                            CS.Is_Scratchpad_Notes_Used,
                            CS.TenderMade_Type,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                            
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Tender_Manual_Draft"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Tender_Manual_Draft table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Tender_Manual_Draft')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)