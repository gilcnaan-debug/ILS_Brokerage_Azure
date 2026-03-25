# Databricks notebook source
# MAGIC %md #Silver_Consignee

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Consignee table with joins and transformations- Incremental load
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

ErrorLogger = ErrorLogs("Silver_Consignee_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ8'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName


# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Consignee table

# COMMAND ----------

# Select the Employee table from the bronze layer for Edge source and perform required transformations.
try:
    DF_Relay_Consignee = spark.sql('''
                                    select
                                    d.delivery_id as Delivery_ID,
                                    d.receiver_id as Receiver_ID,
                                    d.relay_reference_number as Load_ID,
                                    d.action_needed_booking_id as Booking_ID,
                                    d.delivery_numbers as Order_Number,
                                    d.receiver_name as Consignee_Name,
                                    (d.location_address_1,'  ',d.location_address_2) as Consignee_Address,
                                    initcap(d.location_city) as Consignee_City,
                                    d.location_state as Consignee_State_Code,
                                    d.location_postal_code as Consignee_Zipcode,
                                    d.location_country_code as Consignee_Country_Code,
                                    cast(d.appointment_date as DATE) AS Confirmed_Appointment_DateTime,
                                    cast(d.scheduled_at as TIMESTAMP_NTZ) as Scheduled_At,
                                    d.is_appointment_scheduled as Is_Appointment_Scheduled,
                                    d.pieces_to_deliver_count as Pieces_To_Deliver_Count,
                                    d.weight_to_deliver_unit as Weight_To_Deliver_Unit,
                                    d.weight_to_deliver_amount as Weight_To_Deliver_Amount,
                                    d.shipping_units_to_deliver_type as Shipping_Units_Type,
                                    d.shipping_units_to_deliver_count  as Shipping_Units_Count,
                                    d.volume_to_deliver_unit as Volume_To_Deliver_Unit,
                                    d.volume_to_deliver_amount as Volume_To_Deliver_Amount,
                                    r.time_zone as Consignee_TimeZone,
                                    r.utc_offset as UTC_Offset,
                                    d.location_phone_number as Consignee_PhoneNumber,
                                    r.created_at as Consignee_Created_At,
                                    "2" as Sourcesystem_ID,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Last_Modified_By,
                                    Current_timestamp() as Last_Modified_Date
                                    from  bronze.delivery_projection  d left join  bronze.receivers r on d.receiver_id = r.uuid
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Consignee table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Relay_Consignee =DF_Relay_Consignee.dropDuplicates(["Delivery_ID","Receiver_ID"])

except Exception as e:
    logger.info(f"Unable to drop duplicates for the Consignee table from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Consignee

# COMMAND ----------

try:
    Hashkey_Merge = ["Delivery_ID","Receiver_ID","Booking_ID","Load_ID"]

    DF_Consignee = DF_Relay_Consignee.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for Silver_Consignee {e}")
    print(e)

#Create a Temporary view for the Silver_Consignee table
try:
    DF_Consignee.createOrReplaceTempView('VW_Silver_Consignee')
except Exception as e:
    logger.info(f"Unable to create temporary view for the Silver_Consignee table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Consignee as CT using VW_Silver_Consignee as CS ON CS.Delivery_ID = CT.Delivery_ID  When MATCHED and CS.Hashkey != CT.Hashkey THEN UPDATE SET 
                            CT.Sourcesystem_ID=CS.Sourcesystem_ID,
                            CT.Hashkey=CS.Hashkey,
                            CT.Delivery_ID = CS.Delivery_ID,
                            CT.Receiver_ID = CS.Receiver_ID,
                            CT.Load_ID = CS.Load_ID,
                            CT.Booking_ID = CS.Booking_ID,
                            CT.Order_Number = CS.Order_Number,
                            CT.Consignee_Name = CS.Consignee_Name,
                            CT.Consignee_Address = CS.Consignee_Address,
                            CT.Consignee_City = CS.Consignee_City,
                            CT.Consignee_State_Code = CS.Consignee_State_Code,
                            CT.Consignee_Zipcode = CS.Consignee_Zipcode,
                            CT.Consignee_Country_Code = CS.Consignee_Country_Code,
                            CT.Confirmed_Appointment_DateTime = CS.Confirmed_Appointment_DateTime,
                            CT.Scheduled_At = CS.Scheduled_At,
                            CT.Is_Appointment_Scheduled = CS.Is_Appointment_Scheduled,
                            CT.Pieces_To_Deliver_Count = CS.Pieces_To_Deliver_Count,
                            CT.Weight_To_Deliver_Unit = CS.Weight_To_Deliver_Unit,
                            CT.Weight_To_Deliver_Amount = CS.Weight_To_Deliver_Amount,
                            CT.Shipping_Units_Type = CS.Shipping_Units_Type,
                            CT.Shipping_Units_Count = CS.Shipping_Units_Count,
                            CT.Volume_To_Deliver_Unit = CS.Volume_To_Deliver_Unit,
                            CT.Volume_To_Deliver_Amount = CS.Volume_To_Deliver_Amount,
                            CT.Consignee_TimeZone = CS.Consignee_TimeZone,
                            CT.UTC_Offset = CS.UTC_Offset,
                            CT.Consignee_PhoneNumber = CS.Consignee_PhoneNumber,
                            CT.Consignee_Created_At = CS.Consignee_Created_At,
                            CT.Sourcesystem_Name = CS.Sourcesystem_Name,
                            CT.Created_By = CS.Created_By,
                            CT.Created_Date = CS.Created_Date,
                            CT.Last_Modified_By = CS.Last_Modified_By,
                            CT.Last_Modified_Date = CS.Last_Modified_Date
                    
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   
                        CT.Sourcesystem_ID,
                        CT.Hashkey,
                        CT.Delivery_ID,
                        CT.Receiver_ID,
                        CT.Load_ID,
                        CT.Booking_ID,
                        CT.Order_Number,
                        CT.Consignee_Name,
                        CT.Consignee_Address,
                        CT.Consignee_City,
                        CT.Consignee_State_Code,
                        CT.Consignee_Zipcode,
                        CT.Consignee_Country_Code,
                        CT.Confirmed_Appointment_DateTime,
                        CT.Scheduled_At,
                        CT.Is_Appointment_Scheduled,
                        CT.Pieces_To_Deliver_Count,
                        CT.Weight_To_Deliver_Amount,
                        CT.Shipping_Units_Type,
                        CT.Shipping_Units_Count,
                        CT.Volume_To_Deliver_Unit,
                        CT.Volume_To_Deliver_Amount,
                        CT.Consignee_TimeZone,
                        CT.UTC_Offset,
                        CT.Consignee_PhoneNumber,
                        CT.Consignee_Created_At,
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
                            CS.Delivery_ID,
                            CS.Receiver_ID,
                            CS.Load_ID,
                            CS.Booking_ID,
                            CS.Order_Number,
                            CS.Consignee_Name,
                            CS.Consignee_Address,
                            CS.Consignee_City,
                            CS.Consignee_State_Code,
                            CS.Consignee_Zipcode,
                            CS.Consignee_Country_Code,
                            CS.Confirmed_Appointment_DateTime,
                            CS.Scheduled_At,
                            CS.Is_Appointment_Scheduled,
                            CS.Pieces_To_Deliver_Count,
                            CS.Weight_To_Deliver_Amount,
                            CS.Shipping_Units_Type,
                            CS.Shipping_Units_Count,
                            CS.Volume_To_Deliver_Unit,
                            CS.Volume_To_Deliver_Amount,
                            CS.Consignee_TimeZone,
                            CS.UTC_Offset,
                            CS.Consignee_PhoneNumber,
                            CS.Consignee_Created_At,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                            
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Consignee"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Consignee table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Consignee')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

