# Databricks notebook source
# MAGIC %md #Silver_Pricing

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Pricing table with joins and transformations- Incremental load
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

ErrorLogger = ErrorLogs("Silver_Pricing_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ12'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Pricing table

# COMMAND ----------

# Select the Pricing table from the bronze layer for Relay source and perform required transformations.
try:
    DF_Relay_Pricing = spark.sql('''
                                    select 
                                    "2" as Sourcesystem_ID,
                                    s.DW_Load_ID as DW_Load_ID,
                                    m.relay_reference_number as Load_Number,
                                    m.pricing_id as Pricing_ID,
                                    CAST(m.max_buy AS Float)as Max_Buy,
                                    CAST(m.set_at AS TIMESTAMP_NTZ) as Price_Set_At,
                                    m.set_by as Price_Set_By,
                                    m.currency as Currency,
                                    m.notes as Notes,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Last_Modified_By,
                                    Current_timestamp() as Last_Modified_Date
                                    from bronze.max_buy_projection m inner join Silver.silver_Load s ON
                                     m.relay_reference_number=s.Load_ID where s.Sourcesystem_ID=2
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Silver_Pricing table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Relay_Pricing =DF_Relay_Pricing.dropDuplicates(['Load_Number','Pricing_ID','DW_Load_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Silver_Pricing table from Relay {e}")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Pricing

# COMMAND ----------

#Create the Hashkey to merge the data to Silver_Employee table
try:
    Hashkey_Merge = ["Load_Number","Pricing_ID","DW_Load_ID"]

    DF_Relay_Pricing = DF_Relay_Pricing.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for Silver_Pricing {e}")
    print(e)

#Create a Temporary view for the Silver_Pricing table
try:
    DF_Relay_Pricing.createOrReplaceTempView('VW_Pricing')
except Exception as e:
    logger.info(f"Unable to create temporary view for the Silver_Pricing table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Pricing as CT using VW_Pricing as CS ON CS.Load_Number=CT.Load_Number AND CS.Pricing_ID=CT.Pricing_ID AND CT.DW_Load_ID=CS.DW_Load_ID WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                    CT.DW_Load_ID=CS.DW_Load_ID,
                    CT.Sourcesystem_ID=CS.Sourcesystem_ID,
                    CT.Load_Number=CS.Load_Number,
                    CT.Pricing_ID=CS.Pricing_ID,
                    CT.Max_Buy=CS.Max_Buy,
                    CT.Price_Set_At=CS.Price_Set_At,
                    CT.Price_Set_By=CS.Price_Set_By,
                    CT.Currency=CS.Currency,
                    CT.Notes=CS.Notes,
                    CT.Sourcesystem_Name=CS.Sourcesystem_Name,
                    CT.Created_By=CS.Created_By,
                    CT.Created_Date=CS.Created_Date,
                    CT.Last_Modified_By=CS.Last_Modified_By,
                    CT.Last_Modified_Date=CS.Last_Modified_Date
                    
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   
                        CT.DW_Load_ID,
                        CT.Sourcesystem_ID,
                        CT.Load_Number,
                        CT.Pricing_ID,
                        CT.Max_Buy,
                        CT.Price_Set_At,
                        CT.Price_Set_By,
                        CT.Currency,
                        CT.Notes,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date
                        
                        )
                        VALUES
                        (
                            CS.DW_Load_ID,
                            CS.Sourcesystem_ID,
                            CS.Load_Number,
                            CS.Pricing_ID,
                            CS.Max_Buy,
                            CS.Price_Set_At,
                            CS.Price_Set_By,
                            CS.Currency,
                            CS.Notes,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                            
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Pricing"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Pricing table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Pricing')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)