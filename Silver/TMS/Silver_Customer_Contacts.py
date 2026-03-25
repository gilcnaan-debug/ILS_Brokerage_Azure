# Databricks notebook source
# MAGIC %md
# MAGIC #Silver_Customer_Contacts

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Customer table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 12/12/2023
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** - 
# MAGIC * **Modified By:**
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import the required packages

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

ErrorLogger = ErrorLogs("silver_Customer_Contacts_BronzeToSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ3'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Aljex Data to Silver_CustomerContacts table

# COMMAND ----------

# Select the CustomerContacts table from the bronze layer for Aljex source and perform required transformations.
try:
    DF_Aljex_CustomerContacts = spark.sql('''                                    
                                select
                                a.cust_id as Cust_ID,
                                a.cust_name as Cust_Name,
                                c.contact1 as  Contact_Name1,
                                c.contact2 as  Contact_Name2,
                                c.contact3 as Contact_Name3,
                                c.contact4  as Contact_Name4,
                                "null" as Contact_Email,
                                "null" as Contact_Phone,
                                "1" as Sourcesystem_ID,
                                "Aljex" as Sourcesystem_Name,
                                "Databricks" as Created_By,
                                CURRENT_TIMESTAMP() as Created_Date,
                                "Databricks" as  Last_Modified_By,
                                CURRENT_TIMESTAMP() as Last_Modified_Date
                                from bronze.aljex_customer_profiles a Inner join bronze.customers c on a.cust_id=c.id
                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for Silver_CustomerContacts from Aljex {e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Aljex_CustomerContacts = DF_Aljex_CustomerContacts.dropDuplicates(['Cust_ID','Cust_Name'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_CustomerContacts from Aljex {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Relay Data to Silver_CustomerContacts table

# COMMAND ----------

# Select the CustomerContacts table from the bronze layer for Relay source and perform required transformations.
try:
    DF_Relay_CustomerContacts = spark.sql('''                                    
                                           select
                                            c.customer_slug as Cust_ID,
                                            c.customer_name as Cust_Name,
                                            c.contact_name as Contact_Name1,
                                            "null" as Contact_Name2,
                                            "null" as Contact_Name3,
                                            "null" as Contact_Name4,
                                            c.contact_email as Contact_Email,
                                            c.contact_phone as Contact_phone,
                                            "2" as Sourcesystem_ID,
                                            "Relay" as Sourcesystem_Name,
                                            "Databricks" as Created_By,
                                            CURRENT_TIMESTAMP() as Created_Date,
                                            "Databricks" as  Last_Modified_By,
                                            CURRENT_TIMESTAMP() as Last_Modified_Date 
                                            FROM bronze.big_export_projection a Inner JOIN bronze.customer_profile_projection c ON a.customer_id = c.customer_slug
                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for Silver_CustomerContacts from Relay {e}")
    print(e)
try:
##Create dataframe DF_Aljex_Customer
    DF_Relay_CustomerContacts = DF_Relay_CustomerContacts.dropDuplicates(['Cust_ID','Cust_Name'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_CustomerContacts from Relay  {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Edge Data to Silver_CustomerContacts table

# COMMAND ----------


# Select the CustomerContacts table from the bronze layer for Edge source and perform required transformations.
try:      
    DF_Edge_CustomerContacts = spark.sql('''
                                    select
                                    ca.cust_ref_num as  Cust_ID,
                                    ca.customer as Cust_Name,
                                    "null"  as  Contact_Name1,
                                    "null"  as  Contact_Name2,
                                    "null"  as  Contact_Name3,
                                    "null"  as  Contact_Name4,
                                    "null"  as  Contact_Email,
                                    "null"  as  Contact_Phone,
                                    "3" as Sourcesystem_ID,
                                    "Edge" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    FROM bronze.cai_data ca
                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for Dsilver_Customer_Contacts from Edge {e}")
    print(e)
try:
##Create the dataframe DF_Edge_Customer
    DF_Edge_CustomerContacts = DF_Edge_CustomerContacts.dropDuplicates(['Cust_ID','Cust_Name'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Customer_Contacts from Edge {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Union Operation to form Silver_CustomerContacts

# COMMAND ----------

try:
    #Union all the Dataframes from all the source systems
    DF_CustomerContacts_Temp = DF_Aljex_CustomerContacts.unionByName(DF_Relay_CustomerContacts,allowMissingColumns=True)
    DF_CustomerContacts = DF_CustomerContacts_Temp.unionByName(DF_Edge_CustomerContacts,allowMissingColumns=True)

except Exception as e:
    logger.info(f"Unable to create Union the DataFrames for silver_Customer_Contacts {e}")
    print(e)

#Create the Hashkey for merging the data to form CustomerContacts 
try:
    Hashkey_Merge = ["Cust_ID","Cust_Name","Contact_Email","Sourcesystem_ID"]

    DF_CustomerContacts = DF_CustomerContacts.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_Customer_Contacts {e}")
    print(e)
try:
    DF_silver_Customers = spark.sql('''select DW_Customer_ID,customer_id,Customer_Name from silver.silver_Customer''')
    
    DF_CustomerContacts = DF_CustomerContacts.join(DF_silver_Customers,(DF_CustomerContacts.Cust_ID == DF_silver_Customers.customer_id) & (DF_CustomerContacts.Cust_Name == DF_silver_Customers.Customer_Name),"inner")
except Exception as e:
    logger.info(f"Unable to perform join Between DF_silver_Customers and silver_Customer_Contacts")
    print(e)
#Create Temporary view for the silver_Customer table
try:
    DF_CustomerContacts.createOrReplaceTempView('VW_silver_CustomerContacts')
except Exception as e:
    logger.info(f"Unable to create the view for the silver_Customer_Contacts")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Merge Operation to form Silver_CustomerContacts

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merge the records with the actual table
            spark.sql('''Merge into silver.silver_Customer_Contacts as CT using VW_silver_CustomerContacts as CS ON CT.DW_Customer_ID = CS.DW_Customer_ID and CT.Cust_ID = CS.Cust_ID WHEN MATCHED and CS.Hashkey != CT.Hashkey THEN UPDATE SET 
                        CT.Cust_ID = CS.Cust_ID,
                        CT.DW_Customer_ID = CS.DW_Customer_ID,
                        CT.Cust_Name = CS.Cust_Name,
                        CT.Contact_Name1 = CS.Contact_Name1,
                        CT.Contact_Name2 = CS.Contact_Name2,
                        CT.Contact_Name3 = CS.Contact_Name3,
                        CT.Contact_Name4 = CS.Contact_Name4,
                        CT.Contact_Email = CS.Contact_Email,
                        CT.Contact_Phone = CS.Contact_Phone,
                        CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                        CT.Sourcesystem_Name = CS.Sourcesystem_Name,
                        CT.Created_By = CS.created_by,
                        CT.Created_Date	= CS.created_date,
                        CT.Last_Modified_By	= CS.Last_modified_by,
                        CT.Last_Modified_Date = CS.last_modified_date,
                        CT.Hashkey = CS.Hashkey
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   
                        CT.DW_Customer_ID,
                        CT.Cust_ID,
                        CT.Cust_Name,
                        CT.Contact_Name1,
                        CT.Contact_Name2,
                        CT.Contact_Name3,
                        CT.Contact_Name4,
                        CT.Contact_Email,
                        CT.Contact_Phone,
                        CT.Sourcesystem_ID,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_modified_by,
                        CT.Last_Modified_Date,
                        CT.Hashkey
                        )
                        VALUES
                        (   
                            CS.DW_Customer_ID,
                            CS.Cust_ID,
                            CS.Cust_Name,
                            CS.Contact_Name1,
                            CS.Contact_Name2,
                            CS.Contact_Name3,
                            CS.Contact_Name4,
                            CS.Contact_Email,
                            CS.Contact_Phone,
                            CS.Sourcesystem_ID,
                            CS.Sourcesystem_Name,
                            CS.created_by,
                            CS.created_date,
                            CS.Last_modified_by,
                            CS.last_modified_date,
                            CS.Hashkey
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.silver_Customer_Contacts"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to create the view for the silver_Customer_Contacts")
            print(e)
except Exception as e:
    logger.info('Failed for silver_Customer_Contacts')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

