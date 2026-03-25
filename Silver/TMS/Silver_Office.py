# Databricks notebook source
# MAGIC %md #Silver_Office

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Office table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 11/12/2023
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** 
# MAGIC * **Modified By:** 
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import the Required Packages

# COMMAND ----------

##Import all the required Packages
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

ErrorLogger = ErrorLogs("Silver_Office_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ5'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load Relay Data to Silver_Office table

# COMMAND ----------

# Select the Office table from the bronze layer for Relay source and perform required transformations
try:
    DF_Relay_Office = spark.sql('''
                                    
                                    select 
                                     n.old_office as Office_ID,
                                     n.new_office as Office_Name,
                                     o.location as Office_Location,
                                    "2" as Sourcesystem_ID,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.new_office_lookup n left join bronze.office_to_cost_center_lookup o on 
                                    n.old_office=o.office_id

                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Office table from Relay {e}")

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe. 
    DF_Relay_Office = DF_Relay_Office.dropDuplicates(['Office_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Office table from Relay {e}")



# COMMAND ----------

# MAGIC %md
# MAGIC ####Load Edge Data to Silver_Office table

# COMMAND ----------

# Select the Office table from the bronze layer for Edge source and perform required transformations
try:      
    DF_Edge_Office = spark.sql('''
                                    select 
                                    ul.company_name as Office_ID,
                                    ul.company_name as Office_Name,
                                    ul.location_name as Office_Location,
                                    "3" as Sourcesystem_ID,
                                    "Edge" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.ultipro_list ul
                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for DF_Edge_Office from Edge {e}")
    print(e)
    
try:
##Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Edge_Office =DF_Edge_Office.dropDuplicates(['Office_ID','Office_Name'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Office table from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Union Operation and Merge the data to Silver_Office

# COMMAND ----------

try:
    #Perform union operation to union the data from Relay and Edge sources. 
    DF_Office = DF_Relay_Office.unionByName(DF_Edge_Office,allowMissingColumns=True)
except Exception as e:
    logger.info(f"Unable to perform union operation{e}")

#Create the Hashkey to merge data. 
try:
    Hashkey_Merge = ["Office_ID","Sourcesystem_ID"]
    DF_Office = DF_Office.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for DF_Office {e}")
    print(e)

#Create Temporary view for the DF_Office table
try:
    DF_Office.createOrReplaceTempView('VW_Office')
except Exception as e:
    logger.info(f"Unable to create the view for the DF_Office")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Office as CT using VW_Office as CS ON CS.Hashkey=CT.Hashkey WHEN MATCHED THEN UPDATE SET 
                    CT.Office_ID =CS.Office_ID,
                    CT.Office_Name=CS.Office_Name,
                    CT.Office_Location=CS.Office_Location,
                    CT.Sourcesystem_ID=CS.Sourcesystem_ID,
                    CT.Sourcesystem_Name=CS.Sourcesystem_Name,
                    CT.Created_By=CS.Created_By,
                    CT.Created_Date=CS.Created_Date,
                    CT.Last_Modified_By=CS.Last_Modified_By,
                    CT.Last_Modified_Date=CS.Last_Modified_Date,
                    CT.Hashkey=CS.Hashkey
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   CT.Office_ID,
                        CT.Office_Name,
                        CT.Office_Location,
                        CT.Sourcesystem_ID,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date,
                        CT.Hashkey
                        )
                        VALUES
                        (
                            CS.Office_ID ,
                            CS.Office_Name,
                            CS.Office_Location,
                            CS.Sourcesystem_ID,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date,
                            CS.Hashkey
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Office"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Office table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Office')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

