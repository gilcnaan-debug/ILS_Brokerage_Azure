# Databricks notebook source
# MAGIC %md
# MAGIC ##Silver_Lookup_Tables

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form lookup tables with joins and transformations
# MAGIC * **Created Date:** 14/12/2023
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** - 
# MAGIC * **Modified By:**
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize the Utility Notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize the Logger Notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Creating variables to store error log information

ErrorLogger = ErrorLogs("Lookup_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Import Packages

# COMMAND ----------


##Import required Package
from pyspark.sql.functions import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lookup_Mode

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Insert into  silver.Lookup_Mode(
# MAGIC Source_TransportMode,
# MAGIC Derived_TransportMode,
# MAGIC Created_By,
# MAGIC Created_Date,
# MAGIC Last_Modified_By,
# MAGIC Last_Modified_Date
# MAGIC )
# MAGIC select 
# MAGIC distinct(p.equipment) as Source_TransportMode,
# MAGIC case 
# MAGIC when p.equipment like '%LTL%' then 'LTL'  
# MAGIC when p.equipment not like '%LTL%' then 'TL' 
# MAGIC when p.equipment = 'PORTD' then 'PORTD' ELSE p.equipment  end as Derived_TransportMode,
# MAGIC "Databricks" as Created_By,
# MAGIC CURRENT_TIMESTAMP() as Created_Date,
# MAGIC "Databricks" as  Last_Modified_By,
# MAGIC CURRENT_TIMESTAMP() as Last_Modified_Date
# MAGIC from bronze.projection_load_1 P
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC select 
# MAGIC distinct(cpp.mode) as Source_TrasnportMode,
# MAGIC case 
# MAGIC when cpp.mode like '%LTL%' then 'LTL' 
# MAGIC when cpp.mode not like '%LTL%' then 'TL' 
# MAGIC when cpp.mode = 'PORTD' then 'PORTD'  ELSE  cpp.mode end as Derived_TransportMode,
# MAGIC "Databricks" as Created_By,
# MAGIC CURRENT_TIMESTAMP() as Created_Date,
# MAGIC "Databricks" as  Last_Modified_By,
# MAGIC CURRENT_TIMESTAMP() as Last_Modified_Date
# MAGIC from bronze.canonical_plan_projection cpp
# MAGIC
# MAGIC UNION 
# MAGIC select 
# MAGIC distinct(c.mode) as Source_TransportMode,
# MAGIC case 
# MAGIC when c.mode like '%LTL%' then 'LTL'   
# MAGIC when c.mode not like '%LTL%' then 'TL' 
# MAGIC when c.mode = 'PORTD' then 'PORTD' ELSE c.mode end as Derived_TransportMode,
# MAGIC "Databricks" as Created_By,
# MAGIC CURRENT_TIMESTAMP() as Created_Date,
# MAGIC "Databricks" as  Last_Modified_By,
# MAGIC CURRENT_TIMESTAMP() as Last_Modified_Date
# MAGIC from bronze.cai_data c

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lookup_Equipment

# COMMAND ----------


try:
    DF_Aljex_Equipment = spark.sql('''
                                    select
                                    a.equipment_mode as Source_Equipment_Type,
                                    case when 
                                    a.equipment_mode = 'F' then 'Flatbed'
                                    when a.equipment_mode is null then 'Dry Van' 
                                    when a.equipment_mode = 'R' then 'Reefer'
                                    when a.equipment_mode = 'V' then 'Dry Van'
                                    when a.equipment_mode = 'PORTD' then 'PORTD'
                                    when a.equipment_mode = 'POWER ONLY' then 'POWER ONLY'
                                    when a.equipment_mode = 'IMDL' then 'IMDL'  
                                    when a.equipment_mode = 'HWY' then 'HWY' 
                                    when p.equipment = 'RLTL' then 'Reefer' else 'Dry Van' end as  Derived_Equipment_Type,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.aljex_mode_types a left join bronze.projection_load_1 p on a.equipment_mode=p.equipment
                                    ''') 

except Exception as e:
    logger.info(f"Unable to write data into do the transformation for DF_Aljex_Equipment from Edge {e}")
    print(e)
try:
##Create the dataframe DF_Aljex_Equipment
    DF_Aljex_Equipment = DF_Aljex_Equipment.dropDuplicates(['Source_Equipment_Type'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for DF_Aljex_Equipment from Edge {e}")
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into  silver.Lookup_Equipment(
# MAGIC Source_Equipment_Type,
# MAGIC Derived_Equipment_Type,
# MAGIC Created_By,
# MAGIC Created_Date,
# MAGIC Last_Modified_By,
# MAGIC Last_Modified_Date
# MAGIC ) 
# MAGIC  
# MAGIC select 
# MAGIC distinct(tendering_service_line.service_line_type) as Source_Equipment_Type,
# MAGIC case when tendering_service_line.equipment_type = 'power_only' then 'POWER ONLY' 
# MAGIC  when tendering_service_line.service_line_type = 'dry_van' then 'Dry Van'
# MAGIC  when tendering_service_line.service_line_type is null then 'Dry Van'
# MAGIC  when tendering_service_line.service_line_type like 'ref%' then 'Reefer'
# MAGIC  when tendering_service_line.service_line_type = 'flatbed' then 'Flatbed' else tendering_service_line.equipment_type end as Derived_Equipment_Type,
# MAGIC "Databricks" as Created_By,
# MAGIC CURRENT_TIMESTAMP() as Created_Date,
# MAGIC "Databricks" as  Last_Modified_By,
# MAGIC CURRENT_TIMESTAMP() as Last_Modified_Date 
# MAGIC from  bronze.tendering_service_line
# MAGIC UNION 
# MAGIC select 
# MAGIC distinct(c.mode) as Source_Equipment_Type,
# MAGIC case 
# MAGIC when c.mode = 'PORTD' then 'PORTD' 
# MAGIC when c.mode = 'OTR' then 'OTR'
# MAGIC when c.mode ='dry_van' then 'Dry_Van'
# MAGIC when c.mode is null then 'Dry Van'
# MAGIC when c.mode ='IMDL' then 'IMDL'
# MAGIC else c.mode
# MAGIC  end as Derived_Equipment_Type,
# MAGIC "Databricks" as Created_By,
# MAGIC CURRENT_TIMESTAMP() as Created_Date,
# MAGIC "Databricks" as  Last_Modified_By,
# MAGIC CURRENT_TIMESTAMP() as Last_Modified_Date 
# MAGIC from bronze.cai_data c

# COMMAND ----------

##Create Temporary view for the Lookup_Equipment table
try:
    DF_Aljex_Equipment.createOrReplaceTempView('VW_Aljex_Equipment')
except Exception as e:
    logger.info(f"Unable to create the view for the DF_Aljex_Equipment")
    print(e)


try:
    # Merging the records with the actual table
    spark.sql('''Merge into silver.Lookup_Equipment as CT using VW_Aljex_Equipment as CS ON CS.Source_Equipment_Type=CT.Source_Equipment_Type  WHEN MATCHED THEN 
         UPDATE SET 
            CT.Source_Equipment_Type=CS.Source_Equipment_Type,
            CT.Derived_Equipment_Type=CS.Derived_Equipment_Type,
            CT.Created_By=CS.Created_By,
            CT.Created_Date=CS.Created_Date,
            CT.Last_Modified_By=CS.Last_Modified_By,
            CT.Last_Modified_Date=CS.Last_Modified_Date
            WHEN NOT MATCHED 
            THEN INSERT 
            (   CT.Source_Equipment_Type,
                CT.Derived_Equipment_Type,
                CT.Created_By,
                CT.Created_Date,
                CT.Last_Modified_By,
                CT.Last_Modified_Date
                )
                VALUES
                (
                    CS.Source_Equipment_Type,
                    CS.Derived_Equipment_Type,
                    CS.Created_By,
                    CS.Created_Date,
                    CS.Last_Modified_By,
                    CS.Last_Modified_Date
                )
            ''')
except Exception as e:
    logger.info(f"Unable to merge the data for Lookup_Equipment")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lookup_Sourcesystem

# COMMAND ----------


try :
 spark.sql('''Insert into silver.Lookup_Sourcesystem  (
  DW_SourceSystem_ID,
  Sourcesystem_Name,
  Created_By,
  Created_Date,
  Last_Modified_By,
  Last_Modified_Date
  )
  values(
  "1" as DW_SourceSystem_ID,
  "Aljex" as Sourcesystem_Name,
  "Databricks" as Created_By,
  CURRENT_TIMESTAMP() as Created_Date,
  "Databricks" as  Last_Modified_By,
  CURRENT_TIMESTAMP() as Last_Modified_Date )

  UNION
  values(
  "2" as DW_SourceSystem_ID,
  "Relay" as Sourcesystem_Name,
  "Databricks" as Created_By,
  CURRENT_TIMESTAMP() as Created_Date,
  "Databricks" as  Last_Modified_By,
  CURRENT_TIMESTAMP() as Last_Modified_Date)

  UNION
  values( 
  "3" as DW_SourceSystem_ID,
  "Edge" as Sourcesystem_Name,
  "Databricks" as Created_By,
  CURRENT_TIMESTAMP() as Created_Date,
  "Databricks" as  Last_Modified_By,
  CURRENT_TIMESTAMP() as Last_Modified_Date)

  UNION
  values(
  "4" as DW_SourceSystem_ID,
  "Hubspot_AccountManger" as Sourcesystem_Name,
  "Databricks" as Created_By,
  CURRENT_TIMESTAMP() as Created_Date,
  "Databricks" as  Last_Modified_By,
  CURRENT_TIMESTAMP() as Last_Modified_Date)

  UNION
  values(
  "5" as DW_SourceSystem_ID,
  "Hubspot_NFIindustries" as Sourcesystem_Name,
  "Databricks" as Created_By,
  CURRENT_TIMESTAMP() as Created_Date,
  "Databricks" as  Last_Modified_By,
  CURRENT_TIMESTAMP() as Last_Modified_Date)
  ''')
except Exception as e:
    # Logging error message
    logger.error(f"Unable to insert data for the table: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md ##Lookup_Equip_Mode_Types

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Insert into  silver.Lookup_Equip_Mode_Types (
# MAGIC Equip_Mode_Type,
# MAGIC Created_By,
# MAGIC Created_Date,
# MAGIC Last_Modified_By,
# MAGIC Last_Modified_Date
# MAGIC )
# MAGIC select 
# MAGIC   a.equip_mode_type as Equip_Mode_Type,
# MAGIC   "Databricks" as Created_By,
# MAGIC   CURRENT_TIMESTAMP() as Created_Date,
# MAGIC   "Databricks" as  Last_Modified_By,
# MAGIC   CURRENT_TIMESTAMP() as Last_Modified_Date
# MAGIC   from bronze.equip_mode_table a

# COMMAND ----------

