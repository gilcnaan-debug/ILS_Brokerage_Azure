# Databricks notebook source
# MAGIC %md #Silver_Carrier

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Carrier table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 11/12/2023
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

ErrorLogger = ErrorLogs("silver_Carrier_BronzeToSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ1'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data from Aljex to Silver_Carrier

# COMMAND ----------

# Select the Carrier table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_Carrier = spark.sql('''
                                    select
                                    p.id as Carrier_ID,
                                    name as Carrier_Name,
                                    p.phone as  Contact_Details,
                                    p.email1  as Carrier_Email1,
                                    p.email2  as Carrier_Email2,
                                    p.address1 as Carrier_Address1,
                                    p.address2 as Carrier_Address2,
                                    p.state as Carrier_State,
                                    p.zip as Carrier_ZipCode,
                                    p.city as Carrier_City,
                                    p.carrier_type as Carrier_Type,
                                    p.mc_num as  Carrier_MCnumber,
                                    a.lawson_id as Lawson_ID,
                                    concat(P.comments_1,P.comments_2) as Carrier_Comments,
                                    P.Work_comp_insurer as Workman_Comp_Insurer,
                                    P.Work_comp_policy as  Workman_Comp_Policy,
                                    P.Work_comp_amount as  Workman_Comp_Amount,
                                    P.Work_comp_deduct  as Workman_Comp_Deduct,
                                    NULL as Carrier_Approved_At,
                                    NULL as Carrier_Approved_By,
                                    NULL as Denied_At,
                                    NULL as Denied_By,
                                    NULL as  Prohibted_At,
                                    status as Carrier_Status,
                                    P.liab_ins_policy as  Liab_Ins_PolicyID,
                                    P.cargo_ins_policy as  Cargo_Ins_PolicyID,
                                    P.cargo_insurer as  CargoInsurancer_Name,
                                    P.liab_insurer as  Liab_Insurer,
                                    P.liab_ins_amount as  Liab_Ins_Amount,
                                    P.cargo_ins_amount as Cargo_Ins_Amount,
                                    P.cargo_ins_exp as  DateOfExpiration_Cargo,
                                    P.liab_ins_exp as  DateOfExpiration_Liab,
                                    P.dot_num  as  DOT_Number ,
                                    "1" as  Sourcesystem_ID,
                                    "Aljex" as  SourceSystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    From bronze.projection_carrier p left join bronze.aljex_dot_lawson_ref a
                                    on p.id=a.aljex_carrier_id
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Carrier table  for Aljex{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_Carrier = DF_Aljex_Carrier.dropDuplicates(['Carrier_ID','Carrier_Name'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Carrier from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data from Relay to Silver_Carrier

# COMMAND ----------

# Select the Carrier table from the bronze layer for Relay source and perform required transformations.     
try:
    DF_Relay_Carrier = spark.sql('''
                                    
                                    select 
                                    carrier_id	as	Carrier_ID,
                                    carrier_name as	Carrier_Name,
                                    NULL as Carrier_Contacts,
                                    NULL as Carrier_Email1,
                                    NULL as Carrier_Email2,
                                    NULL as Carrier_Address1,
                                    NULL as Carrier_Address2,
                                    NULL as Carrier_State,
                                    NULL as Carrier_Zipcode,
                                    NULL as Carrier_City,
                                    NULL as Carrier_Type,
                                    NULL as Carrier_MCnumber,
                                    NULL as Carrier_Comments,
                                    NULL as Workman_Comp_Insurer,
                                    NULL as Workman_Comp_Policy,
                                    NULL as Workman_Comp_Amount,
                                    NULL as Workman_Comp_Deduct,
                                    approved_at	as	CarrierApproved_At,
                                    string(approved_by)	as	CarrierApproved_By,
                                    denied_at	as	Denied_At,
                                    string(denied_by)	as	Denied_by,
                                    prohibited_at	as	Prohibted_At,
                                    status	as	Carrier_Status,
                                    NULL	as	Liab_Ins_PolicyID,
                                    NULL	as	Cargo_Ins_PolicyID,
                                    NULL	as	Cargo_Insurancer_Name,
                                    NULL	as	Liab_Insurer,
                                    NULL	as	Liab_Ins_Amount,
                                    string(current_cargo_insurance_amount)	as	Cargo_Ins_Amount,
                                    string(cargo_expiration_date)	as	DateOfExpiration_Cargo,
                                    NULL	as	DateOfExpiration_Liab,
                                    dot_number	as	DOT_Number,
                                    "2"	as	Sourcesystem_ID,
                                    "Relay"	as	SourceSystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.carrier_projection

                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Carrier table for Relay{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Relay_Carrier = DF_Relay_Carrier.dropDuplicates(['Carrier_ID','Carrier_Name'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Carrier table from Aljex {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data from Edge to Silver_Carrier

# COMMAND ----------

# Select the Carrier table from the bronze layer for Edge source and perform required transformations.
try:      
    DF_Edge_Carrier = spark.sql('''
                                    select
                                        Carrier as Carrier_ID ,
                                        carrier as Carrier_Name          ,
                                        NULL as Contact_Details       ,
                                        NULL as Carrier_Email1        ,
                                        NULL as Carrier_Email2        ,
                                        concat(carr_city,carr_state,carr_zip) as Carrier_Address1      ,
                                        NULL as Carrier_Address2      ,
                                        carr_state as Carrier_State         ,
                                        carr_zip as Carrier_Zipcode       ,
                                        carr_city as Carrier_City          ,
                                        NULL as CarrierType           ,
                                        carr_mc as Carrier_MCnumber      ,
                                        NULL as Carrier_Comments      ,
                                        NULL as Workman_Comp_Insurer  ,
                                        NULL as Workman_Comp_Policy   ,
                                        NULL as Workman_Comp_Amount   ,
                                        NULL as Workman_Comp_Deduct   ,
                                        NULL as CarrierApproved_At    ,
                                        NULL as CarrierApproved_By    ,
                                        NULL as Denied_At             ,
                                        NULL as Denied_By             ,
                                        NULL as Prohibted_At          ,
                                        NULL as Carrier_Status        ,
                                        NULL as Liab_Ins_PolicyID     ,
                                        NULL as Cargo_Ins_PolicyID    ,
                                        NULL as CargoInsurancer_Name  ,
                                        NULL as Liab_insurer          ,
                                        NULL as Liab_ins_Amount       ,
                                        NULL as Cargo_ins_Amount      ,
                                        NULL as DateOfExpiration_Cargo,
                                        NULL as DateOfExpiration_Liab ,
                                        NULL as DOT_Number                   ,
                                        "3" as Sourcesystem_ID        ,
                                        "Edge" as Sourcesystem_Name,
                                        "Databricks" as Created_By,
                                        CURRENT_TIMESTAMP() as Created_Date,
                                        "Databricks" as  Last_Modified_By,
                                        CURRENT_TIMESTAMP() as Last_Modified_Date
                                from bronze.cai_data

                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Carrier table for Edge {e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Edge_Carrier = DF_Edge_Carrier.dropDuplicates(['Carrier_Name','Carrier_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Carrier table from Edge {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Union Operation to form Silver_Office

# COMMAND ----------

try:
    #Union all the Dataframes from all the source systems
    DF_Carrier_Temp = DF_Aljex_Carrier.unionByName(DF_Relay_Carrier,allowMissingColumns=True)
    DF_Carrier = DF_Carrier_Temp.unionByName(DF_Edge_Carrier,allowMissingColumns=True)

except Exception as e:
    logger.info(f"Unable to Union data from three sources to form silver_Carrier {e}")

#Create the Hashkeyto merge 
try:
    Hashkey_Merge = ["Carrier_ID","Carrier_Name","Carrier_Email1","Sourcesystem_ID","Contact_Details","Denied_At","Carrier_Status"]

    DF_Carrier = DF_Carrier.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_Carrier {e}")

#Create Temporary view for the silver_Carrier table
try:
    DF_Carrier.createOrReplaceTempView('VW_silver_Carrier')
except Exception as e:
    logger.info(f"Unable to create the view for the silver_Carrier")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Merge Operation to form Silver_Carrier

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Carrier as CT using VW_silver_Carrier as CS ON CS.Carrier_ID=CT.Carrier_ID WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET 
                    CT.Hashkey = CS.Hashkey,
                    CT.Carrier_ID = CS.Carrier_ID ,
                    CT.Carrier_Name = CS.Carrier_Name,
                    CT.Contact_Details = CS.Contact_Details,
                    CT.Carrier_Email1 = CS.Carrier_Email1,
                    CT.Carrier_Email2 = CS.Carrier_Email2,
                    CT.Carrier_Address1 = CS.Carrier_Address1,
                    CT.Carrier_Address2 = CS.Carrier_Address2,
                    CT.Carrier_State = CS.Carrier_State,
                    CT.Carrier_Zipcode = CS.Carrier_Zipcode,
                    CT.Carrier_City = CS.Carrier_City,
                    CT.Carrier_Type = CS.Carrier_Type,
                    CT.Carrier_MCnumber = CS.Carrier_MCnumber,
                    CT.Lawson_ID=CS.Lawson_ID,
                    CT.Carrier_Comments = CS.Carrier_Comments,
                    CT.Workman_Comp_Insurer = CS.Workman_Comp_Insurer,
                    CT.Workman_Comp_Policy = CS.Workman_Comp_Policy,
                    CT.Workman_Comp_Amount = CS.Workman_Comp_Amount,
                    CT.Workman_Comp_Deduct = CS.Workman_Comp_Deduct,
                    CT.CarrierApproved_At = CS.CarrierApproved_At,
                    CT.CarrierApproved_By = CS.CarrierApproved_By,
                    CT.Denied_At = CS.Denied_At,
                    CT.Denied_By = CS.Denied_By,
                    CT.Prohibted_At = CS.Prohibted_At,
                    CT.Carrier_Status = CS.Carrier_Status,
                    CT.Liab_Ins_PolicyID = CS.Liab_Ins_PolicyID,
                    CT.Cargo_Ins_PolicyID = CS.Cargo_Ins_PolicyID,
                    CT.CargoInsurancer_Name = CS.CargoInsurancer_Name,
                    CT.Liab_Insurer = CS.Liab_Insurer,
                    CT.Liab_Ins_Amount = CS.Liab_Ins_Amount,
                    CT.Cargo_Ins_Amount = CS.Cargo_Ins_Amount,
                    CT.DateOfExpiration_Cargo = CS.DateOfExpiration_Cargo,
                    CT.DateOfExpiration_Liab = CS.DateOfExpiration_Liab,
                    CT.DOT_Number = CS.DOT_Number,
                    CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                    CT.Sourcesystem_Name = CS.Sourcesystem_Name,
                    CT.Created_By = CS.Created_By,
                    CT.Created_Date = CS.created_date,
                    CT.Last_Modified_By = CS.Last_Modified_By,
                    CT.Last_Modified_Date = CS.Last_Modified_Date
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   CT.Hashkey,
                        CT.Carrier_ID ,
                        CT.Carrier_Name,
                        CT.Contact_Details,
                        CT.Carrier_Email1,
                        CT.Carrier_Email2,
                        CT.Carrier_Address1,
                        CT.Carrier_Address2,
                        CT.Carrier_State,
                        CT.Carrier_Zipcode,
                        CT.Carrier_City,
                        CT.Carrier_Type,
                        CT.Carrier_MCnumber,
                        CT.Lawson_ID,
                        CT.Carrier_Comments,
                        CT.Workman_Comp_Insurer,
                        CT.Workman_Comp_Policy,
                        CT.Workman_Comp_Amount,
                        CT.Workman_Comp_Deduct,
                        CT.CarrierApproved_At,
                        CT.CarrierApproved_By,
                        CT.Denied_At,
                        CT.Denied_By,
                        CT.Prohibted_At,
                        CT.Carrier_Status,
                        CT.Liab_Ins_PolicyID,
                        CT.Cargo_Ins_PolicyID,
                        CT.CargoInsurancer_Name,
                        CT.Liab_Insurer,
                        CT.Liab_Ins_Amount,
                        CT.Cargo_Ins_Amount,
                        CT.DateOfExpiration_Cargo,
                        CT.DateOfExpiration_Liab,
                        CT.DOT_Number,
                        CT.Sourcesystem_ID,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date
                        )
                        VALUES
                        (
                        CS.Hashkey,
                        CS.Carrier_ID,
                        CS.Carrier_Name,
                        CS.Contact_Details,
                        CS.Carrier_Email1,
                        CS.Carrier_Email2,
                        CS.Carrier_Address1,
                        CS.Carrier_Address2,
                        CS.Carrier_State,
                        CS.Carrier_Zipcode,
                        CS.Carrier_City,
                        CS.Carrier_Type,
                        CS.Carrier_MCnumber,
                        CS.Lawson_ID,
                        CS.Carrier_Comments,
                        CS.Workman_Comp_Insurer,
                        CS.Workman_Comp_Policy,
                        CS.Workman_Comp_Amount,
                        CS.Workman_Comp_Deduct,
                        CS.CarrierApproved_At,
                        CS.CarrierApproved_By,
                        CS.Denied_At,
                        CS.Denied_By,
                        CS.Prohibted_At,
                        CS.Carrier_Status,
                        CS.Liab_Ins_PolicyID,
                        CS.Cargo_Ins_PolicyID,
                        CS.CargoInsurancer_Name,
                        CS.Liab_Insurer,
                        CS.Liab_Ins_Amount,
                        CS.Cargo_Ins_Amount,
                        CS.DateOfExpiration_Cargo,
                        CS.DateOfExpiration_Liab,
                        CS.DOT_Number,
                        CS.Sourcesystem_ID,
                        CS.Sourcesystem_Name,
                        CS.Created_By,
                        CS.Created_Date,
                        CS.Last_modified_by,
                        CS.Last_modified_date
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Carrier"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Carrier table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Carrier')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

