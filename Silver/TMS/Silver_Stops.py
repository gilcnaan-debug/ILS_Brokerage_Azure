# Databricks notebook source
# MAGIC %md #Silver_Stops

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Stops table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 02/12/2024
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** - 
# MAGIC * **Modified By:**
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import the required packages

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

ErrorLogger = ErrorLogs("silver_Stops_Bronze_to_Silver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ14'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data from Aljex to Silver_Stops

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_1 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    P.id AS Load_Number, 
                                    P.truck_num AS Truck_Number, 
                                    P.ps_arrive_code_1 as Stop_Type, 
                                    P.ps_city_1 as Stop_City, 
                                    P.ps_state_1 as Stop_State, 
                                    P.ps_company_1 as Stop_Name, 
                                    P.ps_arrive_date_1 as Arrival_Time, 
                                    P.ps_depart_date_1 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By

                                     from bronze.projection_load_2 P where p.ps_arrive_code_1 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_1 = DF_Aljex_Stop_1.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Relay {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_2 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p2.id AS Load_Number, 
                                    p2.truck_num AS Truck_Number, 
                                    p2.ps_arrive_code_2 as Stop_Type, 
                                    p2.ps_city_2 as Stop_City, 
                                    p2.ps_state_2 as Stop_State, 
                                    p2.ps_company_2 as Stop_Name, 
                                    p2.ps_arrive_date_2 as Arrival_Time, 
                                    p2.ps_depart_date_2 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By from bronze.projection_load_2 p2 where p2.ps_arrive_code_2 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_2 = DF_Aljex_Stop_2.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Relay {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_3 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p3.id AS Load_Number, 
                                    p3.truck_num AS Truck_Number, 
                                    p3.ps_arrive_code_3 as Stop_Type, 
                                    p3.ps_city_3 as Stop_City, 
                                    p3.ps_state_3 as Stop_State, 
                                    p3.ps_company_3 as Stop_Name, 
                                    p3.ps_arrive_date_3 as Arrival_Time, 
                                    p3.ps_depart_date_3 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By from bronze.projection_load_2 p3 where p3.ps_arrive_code_3 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_3 = DF_Aljex_Stop_3.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Relay {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_4 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p4.id AS Load_Number, 
                                    p4.truck_num AS Truck_Number, 
                                    p4.ps_arrive_code_4 as Stop_Type, 
                                    p4.ps_city_4 as Stop_City, 
                                    p4.ps_state_4 as Stop_State, 
                                    p4.ps_company_4 as Stop_Name, 
                                    p4.ps_arrive_date_4 as Arrival_Time, 
                                    p4.ps_depart_date_4 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By from bronze.projection_load_2 p4 where p4.ps_arrive_code_4 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_4 = DF_Aljex_Stop_4.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Relay {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_5 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p5.id AS Load_Number, 
                                    p5.truck_num AS Truck_Number, 
                                    p5.ps_arrive_code_5 as Stop_Type, 
                                    p5.ps_city_5 as Stop_City, 
                                    p5.ps_state_5 as Stop_State, 
                                    p5.ps_company_5 as Stop_Name, 
                                    p5.ps_arrive_date_5 as Arrival_Time, 
                                    p5.ps_depart_date_5 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By from bronze.projection_load_2 p5 where p5.ps_arrive_code_5 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_5 = DF_Aljex_Stop_5.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Relay {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_6 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p6.id AS Load_Number, 
                                    p6.truck_num AS Truck_Number, 
                                    p6.ps_arrive_code_6 as Stop_Type, 
                                    p6.ps_city_6 as Stop_City, 
                                    p6.ps_state_6 as Stop_State, 
                                    p6.ps_company_6 as Stop_Name, 
                                    p6.ps_arrive_date_6 as Arrival_Time, 
                                    p6.ps_depart_date_6 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By from bronze.projection_load_2 p6 where p6.ps_arrive_code_6 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_6 = DF_Aljex_Stop_6.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Relay {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_7 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p7.id AS Load_Number, 
                                    p7.truck_num AS Truck_Number, 
                                    p7.ps_arrive_code_7 as Stop_Type, 
                                    p7.ps_city_7 as Stop_City, 
                                    p7.ps_state_7 as Stop_State, 
                                    p7.ps_company_7 as Stop_Name, 
                                    p7.ps_arrive_date_7 as Arrival_Time, 
                                    p7.ps_depart_date_7 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By from bronze.projection_load_2 p7 where p7.ps_arrive_code_7 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_7 = DF_Aljex_Stop_7.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Relay {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_8 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p8.id AS Load_Number, 
                                    p8.truck_num AS Truck_Number, 
                                    p8.ps_arrive_code_8 as Stop_Type, 
                                    p8.ps_city_8 as Stop_City, 
                                    p8.ps_state_8 as Stop_State, 
                                    p8.ps_company_8 as Stop_Name, 
                                    p8.ps_arrive_date_8 as Arrival_Time, 
                                    p8.ps_depart_date_8 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By

                                     from bronze.projection_load_2 p8 where p8.ps_arrive_code_8 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_8 = DF_Aljex_Stop_8.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Aljex {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_9 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p9.id AS Load_Number, 
                                    p9.truck_num AS Truck_Number, 
                                    p9.ps_arrive_code_9 as Stop_Type, 
                                    p9.ps_city_9 as Stop_City, 
                                    p9.ps_state_9 as Stop_State, 
                                    p9.ps_company_9 as Stop_Name, 
                                    p9.ps_arrive_date_9 as Arrival_Time, 
                                    p9.ps_depart_date_9 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By

                                     from bronze.projection_load_2 p9  where p9.ps_arrive_code_9 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_9 = DF_Aljex_Stop_9.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Aljex {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_10 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p10.id AS Load_Number, 
                                    p10.truck_num AS Truck_Number, 
                                    p10.ps_arrive_code_10 as Stop_Type, 
                                    p10.ps_city_10 as Stop_City, 
                                    p10.ps_state_10 as Stop_State, 
                                    p10.ps_company_10 as Stop_Name, 
                                    p10.ps_arrive_date_10 as Arrival_Time, 
                                    p10.ps_depart_date_10 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By
                                     from bronze.projection_load_2 p10  
                                     where p10.ps_arrive_code_10 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_10 = DF_Aljex_Stop_10.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Aljex {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_11 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p11.id AS Load_Number, 
                                    p11.truck_num AS Truck_Number, 
                                    p11.ps_arrive_code_11 as Stop_Type, 
                                    p11.ps_city_11 as Stop_City, 
                                    p11.ps_state_11 as Stop_State, 
                                    p11.ps_company_11 as Stop_Name, 
                                    p11.ps_arrive_date_11 as Arrival_Time, 
                                    p11.ps_depart_date_11 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By
                                     from bronze.projection_load_2 p11  
                                     where p11.ps_arrive_code_11 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_11 = DF_Aljex_Stop_11.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Aljex {e}")
    print(e)

# COMMAND ----------

# Select the Stops table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Stop_12 = spark.sql('''
                                    select
                                    "1" as Sourcesystem_ID,
                                    p12.id AS Load_Number, 
                                    p12.truck_num AS Truck_Number, 
                                    p12.ps_arrive_code_12 as Stop_Type, 
                                    p12.ps_city_12 as Stop_City, 
                                    p12.ps_state_12 as Stop_State, 
                                    p12.ps_company_12 as Stop_Name, 
                                    p12.ps_arrive_date_12 as Arrival_Time, 
                                    p12.ps_depart_date_12 as Depature_Time,
                                    "Aljex" as Sourcesystem_Name,
                                    Current_timestamp() as Created_Date,
                                    "Databricks" as Created_By,
                                    Current_timestamp() as Last_Modified_Date,
                                    "Databricks" as Last_Modified_By
                                     from bronze.projection_load_2 p12  
                                     where p12.ps_arrive_code_12 is not null
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table  for Aljex{e}")
    print(e)

try:
# Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Stop_12 = DF_Aljex_Stop_12.dropDuplicates(['Load_Number','Truck_Number','Stop_Type','Stop_City','Stop_State','Arrival_Time','Depature_Time'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Stops from Aljex {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data from Relay to Silver_Stops

# COMMAND ----------

# Select the Stops table from the bronze layer for Edge source and perform required transformations.
try:      
    DF_Relay_Stop = spark.sql('''
                                    select
                                        "2" as Sourcesystem_ID,
                                        c.stop_id as Stop_ID,
                                        p.relay_reference_number  as Load_Number,
                                        t.truck_number as Truck_Number,
                                        c.stop_type as Stop_Type,
                                        case when c.address_1 is null then' ' 
                                        WHEN c.address_1 is not null then c.address_1
                                        when c.address_2 is null then' ' 
                                        WHEN c.address_2 is not null then c.address_2
                                        else concat(c.address_1,c.address_2)
                                        end as  Stop_Location,
                                        c.locality as Stop_City,
                                        c.administrative_region as Stop_State,
                                        p.stop_name  as Stop_Name,
                                        cast(c.in_date_time  as Timestamp_NTZ) as Arrival_Time,
                                        cast(out_date_time as TIMESTAMP_NTZ) as Depature_Time,
                                        "Relay" as Sourcesystem_Name,
                                        "Databricks" as Created_By,
                                        Current_timestamp() as Created_Date,
                                        "Databricks" as Last_Modified_By,
                                        Current_timestamp() as Last_Modified_Date
                                        from bronze.canonical_Stop  c left join  bronze.planning_stop_schedule p on c.stop_id=p.stop_id
                                        left join  bronze.truckload_projection  t  on  t.relay_reference_number=p.relay_reference_number 
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Stops table for Relay {e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Relay_Stop = DF_Relay_Stop.dropDuplicates(['Stop_ID','Load_Number'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Stops table from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Union Operation to form Silver_Stops

# COMMAND ----------

try:
    #Union all the Dataframes from all the source systems
    DF_Stop_Temp = DF_Aljex_Stop_1.unionByName(DF_Aljex_Stop_2,allowMissingColumns=True)
    DF_Stop_Temp1 = DF_Stop_Temp.unionByName(DF_Aljex_Stop_3,allowMissingColumns=True)
    DF_Stop_Temp2 = DF_Stop_Temp1.unionByName(DF_Aljex_Stop_4,allowMissingColumns=True)
    DF_Stop_Temp3 = DF_Stop_Temp2.unionByName(DF_Aljex_Stop_5,allowMissingColumns=True)
    DF_Stop_Temp4 = DF_Stop_Temp3.unionByName(DF_Aljex_Stop_6,allowMissingColumns=True)
    DF_Stop_Temp5= DF_Stop_Temp4.unionByName(DF_Aljex_Stop_7,allowMissingColumns=True)
    DF_Stop_Temp6 = DF_Stop_Temp5.unionByName(DF_Aljex_Stop_8,allowMissingColumns=True)
    DF_Stop_Temp7 = DF_Stop_Temp6.unionByName(DF_Aljex_Stop_9,allowMissingColumns=True)
    DF_Stop_Temp8 = DF_Stop_Temp7.unionByName(DF_Aljex_Stop_10,allowMissingColumns=True)
    DF_Stop_Temp9 = DF_Stop_Temp8.unionByName(DF_Aljex_Stop_11,allowMissingColumns=True)
    DF_Stop_Temp10 = DF_Stop_Temp9.unionByName(DF_Aljex_Stop_12,allowMissingColumns=True)
    DF_Stop = DF_Stop_Temp10.unionByName(DF_Relay_Stop,allowMissingColumns=True)
except Exception as e:
    logger.info(f"Unable to Union data from three sources to form silver_Stops {e}")
    print(e)

# Create the Hashkeyto merge 

try:
    Hashkey_Merge = ["Load_Number","Stop_Name","Stop_Type","Stop_City","Stop_State","Truck_Number","Arrival_Time","Depature_Time"]

    DF_Stop = DF_Stop.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_stops {e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Stop = DF_Stop.dropDuplicates(['Hashkey'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Stops table from Relay {e}")
    print(e)

try:
    DF_Stop.createOrReplaceTempView('VW_Stop')
except Exception as e:
    logger.info(f"Unable to create the view for the silver_stops")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Merge Operation to form Silver_Stops

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''
                Merge into silver.silver_stops as CT 
                using VW_stop as CS  
                ON CT.Hashkey=CS.Hashkey 
                WHEN MATCHED 
                THEN 
                    UPDATE SET 
                        CT.Sourcesystem_ID=CS.Sourcesystem_ID,
                        CT.Hashkey=CS.Hashkey,  
                        CT.Stop_ID=CS.Stop_ID,
                        CT.Load_Number=CS.Load_Number,
                        CT.Truck_Number=CS.Truck_Number,
                        CT.Stop_Type=CS.Stop_Type,
                        CT.Stop_Location=CS.Stop_Location,
                        CT.Stop_City=CS.Stop_City,
                        CT.Stop_State=CS.Stop_State,
                        CT.Stop_Name=CS.Stop_Name,
                        CT.Arrival_Time=CS.Arrival_Time,
                        CT.Depature_Time=CS.Depature_Time,
                        CT.Sourcesystem_Name=CS.Sourcesystem_Name,
                        CT.Created_By=CS.Created_By,
                        CT.Created_Date=CS.Created_Date,
                        CT.Last_Modified_By=CS.Last_Modified_By,
                        CT.Last_Modified_Date=CS.Last_Modified_Date
                WHEN NOT MATCHED 
                THEN 
                    INSERT (
                        CT.Sourcesystem_ID,
                        CT.Hashkey,
                        CT.Stop_ID,
                        CT.Load_Number,
                        CT.Truck_Number,
                        CT.Stop_Type,
                        CT.Stop_Location,
                        CT.Stop_City,
                        CT.Stop_State,
                        CT.Stop_Name,
                        CT.Arrival_Time,
                        CT.Depature_Time,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date
                    )
                    VALUES (
                        CS.Sourcesystem_ID,
                        CS.Hashkey,
                        CS.Stop_ID,
                        CS.Load_Number,
                        CS.Truck_Number,
                        CS.Stop_Type,
                        CS.Stop_Location,
                        CS.Stop_City,
                        CS.Stop_State,
                        CS.Stop_Name,
                        CS.Arrival_Time,
                        CS.Depature_Time,
                        CS.Sourcesystem_Name,
                        CS.Created_By,
                        CS.Created_Date,
                        CS.Last_Modified_By,
                        CS.Last_Modified_Date
                    )
            ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Stops"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info("Unable to perform merge operation to load data to Silver_Stops table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Stops')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)