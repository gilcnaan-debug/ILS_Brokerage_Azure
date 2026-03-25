# Databricks notebook source
# MAGIC %md #Silver_Budget

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Budget table with joins and transformations- Incremental load
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
# MAGIC "/Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initialize the Logger notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Create variables to store error log information

ErrorLogger = ErrorLogs("Silver_Budget_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ6'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName


# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Employee table

# COMMAND ----------

try:
# Select the Employee table from the bronze layer for Relay source and perform required transformations.
    DF_Relay_Budget = spark.sql('''
                                    select 
                                    "2" as Sourcesystem_ID,
                                    s.DW_Office_ID as DW_Office_ID,
                                    n.office_name as Office_Name,
                                    n.financial_period as Financial_Period,
                                    n.volume_budget as  Volume_Budget,
                                    n.rev_budget as Revenue_Budget,
                                    n.mar_budget as Margin_Budget,
                                    n.mar_perc_budget as Margin_Percent_Budget,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Last_Modified_By,
                                    Current_timestamp() as Last_Modified_Date
                                    from bronze.new_brokerage_budget  n left join silver.Silver_Office s on n.office_name=s.Office_Name
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Silver_Budget table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Relay_Budget =DF_Relay_Budget.dropDuplicates(['Financial_Period','Office_Name'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Silver_Budget table from Relay {e}")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Tender_Address

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        #Create the Hashkey to merge the data to Silver_Employee table
        try:
            Hashkey_Merge = ["Financial_Period","Office_Name","DW_Office_ID"]

            DF_Relay_Budget = DF_Relay_Budget.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
        except Exception as e:
            logger.info(f"Unable to create Hashkey for silver_Budget {e}")
            print(e)

        #Create a Temporary view for the silver_Budget table
        try:
            DF_Relay_Budget.createOrReplaceTempView('VW_Budget')
        except Exception as e:
            logger.info(f"Unable to create temporary view for the silver_Budget table")
            print(e)

        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.silver_Budget as CT using VW_Budget as CS ON CS.Financial_Period=CT.Financial_Period and CS.Office_Name=CT.Office_Name WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                    CT.DW_Office_ID=CS.DW_Office_ID,
                    CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                    CT.Hashkey = CS.Hashkey,
                    CT.Financial_Period = CS.Financial_Period,
                    CT.Office_Name=CS.Office_Name,
                    CT.Volume_Budget = CS.Volume_Budget,
                    CT.Revenue_Budget = CS.Revenue_Budget,
                    CT.Margin_Budget = CS.Margin_Budget,
                    CT.Margin_Percent_Budget = CS.Margin_Percent_Budget, 
                    CT.Sourcesystem_Name = CS.Sourcesystem_Name,
                    CT.Created_By = CS.Created_By,
                    CT.Created_Date = CS.Created_Date,
                    CT.Last_Modified_By = CS.Last_Modified_By, 
                    CT.Last_Modified_date = CS.Last_Modified_Date
                    
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   
                        CT.DW_Office_ID,
                        CT.Sourcesystem_ID,
                        CT.Hashkey,
                        CT.Financial_Period,
                        CT.Office_Name,
                        CT.Volume_Budget,
                        CT.Revenue_Budget,
                        CT.Margin_Budget,
                        CT.Margin_Percent_Budget,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date
                        
                        )
                        VALUES
                        (
                            CS.DW_Office_ID,
                            CS.Sourcesystem_ID,
                            CS.Hashkey,
                            CS.Financial_Period,
                            CS.Office_Name,
                            CS.Volume_Budget,
                            CS.Revenue_Budget,
                            CS.Margin_Budget,
                            CS.Margin_Percent_Budget,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                            
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.silver_load"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Budget table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Budget')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

