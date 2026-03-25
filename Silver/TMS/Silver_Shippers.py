# Databricks notebook source
# MAGIC %md #Silver_Shippers

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Shippers table with joins and transformations- Incremental load
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

ErrorLogger = ErrorLogs("Silver_Shippers_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ13'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Shippers table

# COMMAND ----------

# Select the Employee table from the bronze layer for Edge source and perform required transformations.
try:
    DF_Relay_Shippers = spark.sql('''
                                    select
                                    p.schedule_id as Schedule_ID,
                                    p.shipper_id  as Shipper_ID,
                                    p.relay_reference_number as Load_ID,
                                    p.scheduling_action_booking_id as Booking_ID,
                                    io.truck_load_thing_id as Truck_Load_Thing_ID,
                                    p.pickup_numbers as Order_Number,
                                    p.shipper_name as Shipper_Name,
                                    concat(p.location_address_1,' ',p.location_address_2)  as Shipper_Address,
                                    initcap(p.location_city) as Shipper_City,
                                    p.location_state as Shipper_State_Code,
                                    p.location_postal_Code as Shipper_Zipcode,
                                    p.location_country_code as Shipper_Country_Code,
                                    cast(p.requested_appointment_time as TIMESTAMP_NTZ ) as Requested_Appoinment_Date,
                                    cast(p.appointment_datetime as TIMESTAMP_NTZ) as Confirmed_Appointment_DateTime,
                                    cast(p.scheduled_at as TIMESTAMP_NTZ)  Scheduled_At,
                                    p.is_appointment_scheduled as Is_Appointment_Scheduled,
                                    p.pieces_to_pickup_count as Pieces_To_Pickup_Count,
                                    p.weight_to_pickup_amount as Weight_To_Pickup_Amount,
                                    p.Weight_To_Pickup_unit as Weight_To_Pickup_Unit,
                                    p.shipping_units_to_pickup_type as Shipping_Units_To_Pickup_Type,
                                    p.shipping_units_to_pickup_count as Shipping_Units_To_Pickup_Count,
                                    p.volume_to_pickup_unit as Volume_To_Pickup_Unit,
                                    p.volume_to_pickup_amount as  Volume_To_Pickup_Amount,
                                    io.pickup_one_stop_id as Shipper_Stop_1,
                                    cast(io.pickup_one_in_date_time as TIMESTAMP_NTZ) as Arrival_Time_1,
                                    cast(io.pickup_one_out_date_time as TIMESTAMP_NTZ)  as Depature_Time_1 ,
                                    io.pickup_two_stop_id  as Shipper_Stop_2,
                                    cast(io.pickup_two_in_date_time as TIMESTAMP_NTZ) as Arrival_Time_2,
                                    cast(io.pickup_two_out_date_time  as TIMESTAMP_NTZ) as Depature_Time_2,
                                    sh.time_zone  as Shipper_TimeZone,
                                    sh.utc_offset  as UTC_Offset,
                                    p.location_phone_number as Shipper_Phone_Number,
                                    sh.created_at  as Shipper_Created_At,
                                    "2" as Sourcesystem_ID,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Last_Modified_By,
                                    Current_timestamp() as Last_Modified_Date
                                    from bronze.pickup_projection p LEFT join bronze.shipper_in_out_projection io 
                                    on p.relay_reference_number = io.relay_reference_number
                                    Left join bronze.shippers sh on p.shipper_id = sh.uuid 
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Shippers table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Relay_Shippers =DF_Relay_Shippers.dropDuplicates(["Schedule_ID","Shipper_ID"])

except Exception as e:
    logger.info(f"Unable to drop duplicates for the Shippers table from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Shippers

# COMMAND ----------

#Create the Hashkey to merge the data to Silver_Shippers table
try:
    Hashkey_Merge = ["Schedule_ID","Shipper_ID","Truck_Load_Thing_ID",'Load_ID','Booking_ID','Arrival_Time_1' ,'Depature_Time_1','Arrival_Time_2','Depature_Time_2']

    DF_Shippers = DF_Relay_Shippers.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for Silver_Shippers {e}")
    print(e)
try:
    Mergekey_Merge = ["Schedule_ID","Shipper_ID","Load_ID"]

    DF_Shippers = DF_Shippers.withColumn("Mergekey",md5(concat_ws("",*Mergekey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for Silver_Shippers {e}")
    print(e)

#Create a Temporary view for the Silver_Shippers table
try:
    DF_Shippers.createOrReplaceTempView('VW_Silver_Shippers')
except Exception as e:
    logger.info(f"Unable to create temporary view for the Silver_Shippers table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Shippers as CT using VW_Silver_Shippers as CS ON CT.Mergekey=CS.Mergekey When Matched and  CT.Hashkey != CS.Hashkey  THEN UPDATE SET
                            CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                            CT.Hashkey=CS.Hashkey,
                            CT.Mergekey=CS.Mergekey,
                            CT.Schedule_ID = CS.Schedule_ID,
                            CT.Shipper_ID = CS.Shipper_ID,
                            CT.Load_ID = CS.Load_ID,
                            CT.Booking_ID = CS.Booking_ID,
                            CT.Truck_Load_Thing_ID=CS.Truck_Load_Thing_ID,
                            CT.Order_Number = CS.Order_Number,
                            CT.Shipper_Name = CS.Shipper_Name,
                            CT.Shipper_Address = CS.Shipper_Address,
                            CT.Shipper_City = CS.Shipper_City,
                            CT.Shipper_State_Code = CS.Shipper_State_Code,
                            CT.Shipper_Zipcode = CS.Shipper_Zipcode,
                            CT.Shipper_Country_Code = CS.Shipper_Country_Code,
                            CT.Requested_Appoinment_Date = CS.Requested_Appoinment_Date,
                            CT.Confirmed_Appointment_DateTime = CS.Confirmed_Appointment_DateTime,
                            CT.Scheduled_At = CS.Scheduled_At,
                            CT.Is_Appointment_Scheduled = CS.Is_Appointment_Scheduled,
                            CT.Pieces_To_Pickup_Count = CS.Pieces_To_Pickup_Count,
                            CT.Weight_To_Pickup_Amount = CS.Weight_To_Pickup_Amount,
                            CT.Weight_To_Pickup_Unit = CS.Weight_To_Pickup_Unit,
                            CT.Shipping_Units_To_Pickup_Type = CS.Shipping_Units_To_Pickup_Type,
                            CT.Shipping_Units_To_Pickup_Count = CS.Shipping_Units_To_Pickup_Count,
                            CT.Volume_To_Pickup_Unit = CS.Volume_To_Pickup_Unit,
                            CT.Volume_To_Pickup_Amount = CS.Volume_To_Pickup_Amount,
                            CT.Shipper_Stop_1 = CS.Shipper_Stop_1,
                            CT.Arrival_Time_1 = CS.Arrival_Time_1,
                            CT.Depature_Time_1 = CS.Depature_Time_1,
                            CT.Shipper_Stop_2 = CS.Shipper_Stop_2,
                            CT.Arrival_Time_2 = CS.Arrival_Time_2,
                            CT.Depature_Time_2 = CS.Depature_Time_2,
                            CT.Shipper_TimeZone = CS.Shipper_TimeZone,
                            CT.UTC_Offset = CS.UTC_Offset,
                            CT.Shipper_Phone_Number = CS.Shipper_Phone_Number,
                            CT.Shipper_Created_At = CS.Shipper_Created_At,          
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
                        CT.Mergekey,
                        CT.Schedule_ID,
                        CT.Shipper_ID,
                        CT.Load_ID,
                        CT.Booking_ID,               
                        CT.Truck_Load_Thing_ID,             
                        CT.Order_Number,
                        CT.Shipper_Name,
                        CT.Shipper_Address,
                        CT.Shipper_City,
                        CT.Shipper_State_Code,
                        CT.Shipper_Zipcode,
                        CT.Shipper_Country_Code,
                        CT.Requested_Appoinment_Date,
                        CT.Confirmed_Appointment_DateTime,
                        CT.Scheduled_At,
                        CT.Is_Appointment_Scheduled,
                        CT.Pieces_To_Pickup_Count,
                        CT.Weight_To_Pickup_Amount,
                        CT.Weight_To_Pickup_Unit,
                        CT.Shipping_Units_To_Pickup_Type,
                        CT.Shipping_Units_To_Pickup_Count,
                        CT.Volume_To_Pickup_Unit,
                        CT.Volume_To_Pickup_Amount,
                        CT.Shipper_Stop_1,
                        CT.Arrival_Time_1,
                        CT.Depature_Time_1,
                        CT.Shipper_Stop_2,
                        CT.Arrival_Time_2,
                        CT.Depature_Time_2,
                        CT.Shipper_TimeZone,
                        CT.UTC_Offset,
                        CT.Shipper_Phone_Number,
                        CT.Shipper_Created_At,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date
                        )
                        VALUES
                        (   CS.Sourcesystem_ID,
                            CS.Hashkey, 
                            CS.Mergekey,
                            CS.Schedule_ID,
                            CS.Shipper_ID,
                            CS.Load_ID,
                            CS.Booking_ID,
                            CS.Truck_Load_Thing_ID,    
                            CS.Order_Number,
                            CS.Shipper_Name,
                            CS.Shipper_Address,
                            CS.Shipper_City,
                            CS.Shipper_State_Code,
                            CS.Shipper_Zipcode,
                            CS.Shipper_Country_Code,
                            CS.Requested_Appoinment_Date,
                            CS.Confirmed_Appointment_DateTime,
                            CS.Scheduled_At,
                            CS.Is_Appointment_Scheduled,
                            CS.Pieces_To_Pickup_Count,
                            CS.Weight_To_Pickup_Amount,
                            CS.Weight_To_Pickup_Unit,
                            CS.Shipping_Units_To_Pickup_Type,
                            CS.Shipping_Units_To_Pickup_Count,
                            CS.Volume_To_Pickup_Unit,
                            CS.Volume_To_Pickup_Amount,
                            CS.Shipper_Stop_1,
                            CS.Arrival_Time_1,
                            CS.Depature_Time_1,
                            CS.Shipper_Stop_2,
                            CS.Arrival_Time_2,
                            CS.Depature_Time_2,
                            CS.Shipper_TimeZone,
                            CS.UTC_Offset,
                            CS.Shipper_Phone_Number,
                            CS.Shipper_Created_At,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Shippers"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Shippers table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Shippers')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

