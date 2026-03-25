# Databricks notebook source
# MAGIC %md #Silver_Offer

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Customer table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 02/12/2024
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** - 
# MAGIC * **Modified By:**
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import the required packages

# COMMAND ----------

##Importing required Package
from pyspark.sql.functions import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window

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

#Creating variables to store error log information

ErrorLogger = ErrorLogs("silver_Offer_BronzeToSilver1")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ11'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Aljex Data to Silver_offer table

# COMMAND ----------

try:
    DF_Relay_Offer = spark.sql('''
                                    
                                    select 
                                    s.DW_Load_ID as DW_Load_ID,
                                    s.DW_Customer_ID AS DW_Customer_ID,
                                    "2" as Sourcesystem_ID,
                                    onr.offer_id as Offer_ID,
                                    onr.relay_reference_number as Load_Number,
                                    onr.carrier_name as Carrier_Name,
                                    cast(onr.master_carrier_id as Integer) as Offer_Carrier_ID,
                                    onr.offered_by as Offered_By_ID,
                                    onr.offered_by_name as Offered_By_Name,
                                    onr.offered_system as Offered_System,
                                    onr.total_rate_in_pennies as Final_Rate_In_Pennies,
                                    onr.total_rate_currency as Currency,
                                    cast(onr.authority_id as Integer) as Carrier_Dot_Number,
                                    oni.invalidated_by as Offer_Invalidated_By,
                                    oni.at as Offer_Invalidated_At,
                                    oni.reason as Invalidated_Reason,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.Offer_negotiation_reflected onr 
                                    left join Silver.silver_load s 
                                    on onr.relay_Reference_number=s.Load_ID
                                    left join bronze.offer_negotiation_invalidated oni on 
                                    oni.offer_id=onr.offer_id

                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for silver_Offer from Aljex {e}")
    print(e)
try:
##Create the dataframe DF_Relay_Offer
    DF_Relay_Offer = DF_Relay_Offer.dropDuplicates(['DW_Load_ID','DW_Customer_ID','Load_Number','Offer_ID','Carrier_Name','Offer_Carrier_ID','Offered_By_ID','Carrier_DOT_Number','Offer_Invalidated_By','Final_Rate_In_Pennies'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Offer from Relay {e}")
    print(e)

# COMMAND ----------

#Creating the Hashkey for merging the data
try:
    Hashkey_Merge = ['DW_Load_ID','DW_Customer_ID','Load_Number','Offer_ID','Carrier_Name','Offer_Carrier_ID','Offered_By_ID','Carrier_DOT_Number','Offer_Invalidated_By','Final_Rate_In_Pennies','Offer_Invalidated_At','Offer_Invalidated_By']

    DF_Offer = DF_Relay_Offer.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_Offer {e}")
    print(e)
try:
##Create the dataframe DF_Relay_Offer
    DF_Offer = DF_Offer.dropDuplicates(['DW_Load_ID','DW_Customer_ID','Load_Number','Offer_ID','Carrier_Name','Offer_Carrier_ID','Offered_By_ID','Carrier_DOT_Number','Offer_Invalidated_By','Final_Rate_In_Pennies'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Offer from Relay {e}")
    print(e)
    

#Create Temporary view for the silver_Offer table
try:
    DF_Offer.createOrReplaceTempView('VW_Offer')
except Exception as e:
    logger.info(f"Unable to create the view for the silver_Offer")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Merge Operation to form Silver_Offer

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.silver_Offer as CT using VW_Offer as CS ON CS.Offer_ID=CT.Offer_ID  WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET 
                        CT.DW_Load_ID=CS.DW_Load_ID,
                        CT.DW_Customer_ID=CS.DW_Customer_ID,
                        CT.Sourcesystem_ID=CS.Sourcesystem_ID,
                        CT.Hashkey=CS.Hashkey,
                        CT.Offer_ID=CS.Offer_ID,
                        CT.Load_Number=CS.Load_Number,
                        CT.Carrier_Name=CS.Carrier_Name,
                        CT.Offer_Carrier_ID=CS.Offer_Carrier_ID,
                        CT.Offered_By_ID=CS.Offered_By_ID,
                        CT.Offered_By_Name=CS.Offered_By_Name,
                        CT.Offered_System=CS.Offered_System,
                        CT.Final_Rate_In_Pennies=CS.Final_Rate_In_Pennies,
                        CT.Currency=CS.Currency,
                        CT.Carrier_Dot_Number=CS.Carrier_Dot_Number,
                        CT.Offer_Invalidated_By=CS.Offer_Invalidated_By,
                        CT.Offer_Invalidated_At=CS.Offer_Invalidated_At,
                        CT.Invalidated_Reason=CS.Invalidated_Reason,
                        CT.Sourcesystem_Name=CS.Sourcesystem_Name,
                        CT.Created_By=CS.Created_By,
                        CT.Created_Date=CS.Created_Date,
                        CT.Last_Modified_By=CS.Last_Modified_By,
                        CT.Last_Modified_Date=CS.Last_Modified_Date
                
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   CT.DW_Load_ID,
                        CT.DW_Customer_ID,
                        CT.Sourcesystem_ID,
                        CT.Hashkey,
                        CT.Offer_ID,
                        CT.Load_Number,
                        CT.Carrier_Name,
                        CT.Offer_Carrier_ID,
                        CT.Offered_By_ID,
                        CT.Offered_By_Name,
                        CT.Offered_System,
                        CT.Final_Rate_In_Pennies,
                        CT.Currency,
                        CT.Carrier_Dot_Number,
                        CT.Offer_Invalidated_By,
                        CT.Offer_Invalidated_At,
                        CT.Invalidated_Reason,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date	
                        )
                        VALUES
                        (
                            CS.DW_Load_ID,
                            CS.DW_Customer_ID,
                            CS.Sourcesystem_ID,
                            CS.Hashkey,
                            CS.Offer_ID,
                            CS.Load_Number,
                            CS.Carrier_Name,
                            CS.Offer_Carrier_ID,
                            CS.Offered_By_ID,
                            CS.Offered_By_Name,
                            CS.Offered_System,
                            CS.Final_Rate_In_Pennies,
                            CS.Currency,
                            CS.Carrier_Dot_Number,
                            CS.Offer_Invalidated_By,
                            CS.Offer_Invalidated_At,
                            CS.Invalidated_Reason,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.silver_Offer"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to create the view for the silver_Offer")
            print(e)
except Exception as e:
    logger.info('Failed for silver_Offer')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)