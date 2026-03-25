# Databricks notebook source
# MAGIC %md #Silver_Customer

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Customer table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 11/12/2023
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

ErrorLogger = ErrorLogs("silver_Customer_BronzeToSilver1")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ2'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Aljex Data to Silver_Customer table

# COMMAND ----------

try:
    DF_Aljex_Customer = spark.sql('''
                                    
                                    select 
                                    a.cust_id  as Customer_ID,
                                    a.cust_name as Customer_Name,
                                    c.phone1  as Customer_Phone1,
                                    c.phone2  as Customer_Phone2 ,
                                    c.phone3 as Customer_Phone3 ,
                                    c.email1  as Customer_Email1,
                                    c.email2   as Customer_Email2,
                                    c.email3   as Customer_Email3,
                                    c.email4   as Customer_Email4 ,
                                    a.address_one  as Cust_Address1,
                                    a.address_two  as Cust_Address2,
                                    a.address_city   as  Customer_City,
                                    a.cust_country as Customer_Country,
                                    a.address_zip as Customer_Zipcode,
                                    NULL as Invoice_Method,
                                    NULL as Invoicing_CustomerProfile_ID,
                                    c.credit_limit  as  Credit_Limit,
                                    a.status as Customer_Status,
                                    a.sales_rep as Salesperson_Name,
                                    "1" as Sourcesystem_ID,
                                    "Aljex" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.aljex_customer_profiles a left join bronze.customers c on a.cust_id=c.id

                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for silver_Customer from Aljex {e}")

try:
##Create the dataframe DF_Aljex_Customer
    DF_Aljex_Customer = DF_Aljex_Customer.dropDuplicates(['Customer_Name','Customer_Email1'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Customer from Relay {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Relay Data to Silver_Customer table

# COMMAND ----------

      
try:
    DF_Relay_Customer = spark.sql('''
                                    select
                                    b.customer_slug as Customer_ID,
                                    b.customer_name as Customer_Name,
                                    NULL as Customer_Phone1,
                                    NULL as Customer_Phone2,
                                    NULL as Customer_Phone3,
                                    b.bill_to_email as Customer_Email1,
                                    NULL as Customer_Email2,
                                    NULL as Customer_Email3,
                                    NULL as Customer_Email4,
                                    case when b.postal_address_1 is null then b.billing_address_1 else  b.postal_address_1  end as Cust_Address1,
                                    case when b.postal_address_2 is null then b.billing_address_2 else  b.postal_address_2  end as Cust_Address2,
                                    b.billing_address_city as Customer_City,
                                    NULL as Customer_Country,
                                    b.billing_address_state  as Customer_State,
                                    b.billing_address_zip  as Customer_Zipcode, 
                                    b.status as Customer_Status,
                                    b.invoicing_method as Invoice_Method,
                                    b.invoicing_customer_profile_id as Invoicing_CustomerProfile_ID,
                                    NULL as  Salesperson_Name,
                                    c.aljex_id	as Aljex_ID	,
                                    c.lawson_id	as Lawson_ID,
                                    c.been_discarded as	Been_Discarded,
                                    c.external_ids as External_IDs	,
                                    c.has_unmapped_external_ids	as Has_Unmapped_External_IDs,
                                    a.acct_mgr as Account_Manager,
                                    a.office as	Office,
                                    a.sales_rep as Sales_Rep,
                                    a.extra_bill_to	as Secondary_Bill_To,
                                    i.at as	Customer_Added_At,
                                    i.by as	Customer_Added_By,
                                    i.auto_invoice_delay as	Auto_Invoice_Delay,
                                    i.`auto_invoice_enabled?` as Is_Auto_Invoice_Enabled,
                                    i.`show_intermediary_stops?` as To_Show_Intermediary_Stops,
                                    i.`stop_tracking_requirement?` as	To_Stop_Tracking_Requirement,
                                    i.document_requirements	as	Document_Requirements,
                                    i.data_requirements	as	Data_Requirements,
                                    "2" as Sourcesystem_ID,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.customer_profile_projection b inner join bronze.customer_profile_external_ids c on b.customer_slug = c.customer_slug 
                                    left join bronze.am_sales_lookup a on b.customer_name = a.customer_name 
                                    left join bronze.invoicing_customer_profile i on b.invoicing_customer_profile_id = i.customer_profile_id

                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for Silver_Customer from Relay {e}")

try:
##Create dataframe DF_Aljex_Customer
    DF_Relay_Customer = DF_Relay_Customer.dropDuplicates(['Customer_Name','Customer_Email1'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Customer from Relay {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Edge Data to Silver_Customer table

# COMMAND ----------


try:      
    DF_Edge_Customer = spark.sql('''
                                    select
                                    cust_ref_num as Customer_ID,
                                    customer as Customer_Name,
                                    NULL as Customer_Phone1,
                                    NULL as Customer_Phone2,
                                    NULL as Customer_Phone3,
                                    NULL as  Customer_Email1,
                                    NULL as  Customer__Email2,
                                    NULL as  CustomerEmail3,
                                    NULL as Customer_Email4,
                                    concat(cust_city,cust_state,cust_zip) as  Cust_Address1,
                                    NULL as  Cust_Address2,
                                    cust_city as Customer_City,
                                    NULL as Customer_Country,
                                    cust_state as Customer_State,
                                    cust_zip  as Customer_Zipcode,
                                    NULL as Invoice_Method,
                                    NULL as Invoicing_CustomerProfile_ID,
                                    NULL as Credit_Limit,
                                    NULL as Customer_Status,
                                    salesperson as Salesperson_Name,
                                    "3" as Sourcesystem_ID,
                                    "Edge" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.cai_data

                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for silver_Customer from Edge {e}")

try:
##Create the dataframe DF_Edge_Customer
    DF_Edge_Customer = DF_Edge_Customer.dropDuplicates(['Customer_Name','Customer_Email1'])
except Exception as e:
    logger.info(f"Unable to create DataFrame for silver_Customer from Edge {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Union Operation to form Silver_Customer

# COMMAND ----------

try:
    #union all the Dataframes from all the source systems
    DF_Customer_Temp = DF_Aljex_Customer.unionByName(DF_Relay_Customer,allowMissingColumns=True)
    DF_Customer = DF_Customer_Temp.unionByName(DF_Edge_Customer,allowMissingColumns=True)

except Exception as e:
    logger.info(f"Unable to create Union the DataFrames for silver_Customer {e}")

#Creating the Hashkey for merging the data
try:
    Hashkey_Merge = ["Customer_ID","Customer_Name","Customer_Email1","Sourcesystem_ID"]

    DF_Customer = DF_Customer.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_Customer {e}")

#Create Temporary view for the silver_Customer table
try:
    DF_Customer.createOrReplaceTempView('VW_silver_Customer')
except Exception as e:
    logger.info(f"Unable to create the view for the silver_Customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Merge Operation to form Silver_Customer

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.silver_Customer as CT using VW_silver_Customer as CS ON CS.Customer_ID=CT.Customer_ID AND CS.Customer_Name=CT.Customer_Name WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET 
                        CT.Customer_ID = CS.Customer_ID,
                        CT.Customer_Name = CS.Customer_Name,
                        CT.Customer_Phone1 = CS.Customer_Phone1,
                        CT.Customer_Phone2 = CS.Customer_Phone2,
                        CT.Customer_Phone3 = CS.Customer_Phone3,
                        CT.Customer_Email1 = CS.Customer_Email1, 
                        CT.Customer_Email2 = CS.Customer__Email2,
                        CT.Customer_Email3 = CS.CustomerEmail3, 
                        CT.Customer_Email4 = CS.Customer_Email4, 
                        CT.Cust_Address1 = CS.Cust_Address1,
                        CT.Cust_Address2 = CS.Cust_Address2,
                        CT.Customer_City = CS.Customer_City,
                        CT.Customer_Country = CS.Customer_Country,
                        CT.Customer_State = CS.Customer_State,
                        CT.Customer_Zipcode = CS.Customer_Zipcode,
                        CT.Invoice_Method = CS.Invoice_Method,
                        CT.Invoicing_CustomerProfile_ID = CS.Invoicing_CustomerProfile_ID,
                        CT.Credit_Limit = CS.Credit_Limit,
                        CT.Customer_Status = CS.Customer_Status,
                        CT.Salesperson_Name = CS.Salesperson_Name,
                        CT.Aljex_ID = CS.Aljex_ID,
                        CT.Lawson_ID = CS.Lawson_ID,
                        CT.Been_Discarded = CS.Been_Discarded,
                        CT.External_IDs = CS.External_IDs,
                        CT.Has_Unmapped_External_IDs = CS.Has_Unmapped_External_IDs,
                        CT.Account_Manager = CS.Account_Manager,
                        CT.Office = CS.Office,
                        CT.Sales_Rep = CS.Sales_Rep,
                        CT.Secondary_Bill_To = CS.Secondary_Bill_To,
                        CT.Customer_Added_At = CS.Customer_Added_At,
                        CT.Customer_Added_By = CS.Customer_Added_By,
                        CT.Auto_Invoice_Delay = CS.Auto_Invoice_Delay,
                        CT.Is_Auto_Invoice_Enabled = CS.Is_Auto_Invoice_Enabled,
                        CT.To_Show_Intermediary_Stops = CS.To_Show_Intermediary_Stops,
                        CT.To_Stop_Tracking_Requirement = CS.To_Stop_Tracking_Requirement,
                        CT.Document_Requirements = CS.Document_Requirements,
                        CT.Data_Requirements = CS.Data_Requirements,
                        CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                        CT.Sourcesystem_Name = CS.Sourcesystem_Name,
                        CT.Created_By = CS.Created_By,
                        CT.Created_Date = CS.Created_Date,
                        CT.Last_modified_by = CS.last_modified_by, 
                        CT.Last_modified_date = CS.Last_modified_date,
                        CT.Hashkey = CS.Hashkey
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   CT.Customer_ID 	,
                        CT.Customer_Name 	,
                        CT.Customer_Phone1 	,
                        CT.Customer_Phone2 	,
                        CT.Customer_Phone3 	,
                        CT.Customer_Email1 	,
                        CT.Customer_Email2 	,
                        CT.Customer_Email3 	,
                        CT.Customer_Email4 	,
                        CT.Cust_Address1 	,
                        CT.Cust_Address2 	,
                        CT.Customer_City 	,
                        CT.Customer_Country,
                        CT.Customer_State,
                        CT.Customer_Zipcode ,
                        CT.Invoice_Method 	,
                        CT.Invoicing_CustomerProfile_ID 	,
                        CT.Credit_Limit 	,
                        CT.Customer_Status 	,
                        CT.Salesperson_Name ,
                        CT.Aljex_ID,
                        CT.Lawson_ID,
                        CT.Been_Discarded,
                        CT.External_IDs,
                        CT.Has_Unmapped_External_IDs,
                        CT.Account_Manager,
                        CT.Office,
                        CT.Sales_Rep,
                        CT.Secondary_Bill_To,
                        CT.Customer_Added_At,
                        CT.Customer_Added_By,
                        CT.Auto_Invoice_Delay,
                        CT.Is_Auto_Invoice_Enabled,
                        CT.To_Show_Intermediary_Stops,
                        CT.To_Stop_Tracking_Requirement,
                        CT.Document_Requirements,
                        CT.Data_Requirements,
                        CT.Sourcesystem_ID 	,
                        CT.Sourcesystem_Name 	,
                        CT.Created_By 	,
                        CT.Created_Date 	,
                        CT.Last_Modified_By 	,
                        CT.Last_Modified_Date 	,
                        CT.Hashkey	
                        )
                        VALUES
                        (
                            CS.Customer_ID, 
                            CS.Customer_Name, 
                            CS.Customer_Phone1, 
                            CS.Customer_Phone2, 
                            CS.Customer_Phone3, 
                            CS.Customer_Email1, 
                            CS.Customer_Email2, 
                            CS.Customer_Email3, 
                            CS.Customer_Email4, 
                            CS.Cust_Address1, 
                            CS.Cust_Address2, 
                            CS.Customer_City, 
                            CS.Customer_Country,
                            CS.Customer_State,
                            CS.Customer_Zipcode, 
                            CS.Invoice_Method, 
                            CS.Invoicing_CustomerProfile_ID, 
                            CS.Credit_Limit, 
                            CS.Customer_Status, 
                            CS.Salesperson_Name, 
                            CS.Aljex_ID,
                            CS.Lawson_ID,
                            CS.Been_Discarded,
                            CS.External_IDs,
                            CS.Has_Unmapped_External_IDs,
                            CS.Account_Manager,
                            CS.Office,
                            CS.Sales_Rep,
                            CS.Secondary_Bill_To,
                            CS.Customer_Added_At,
                            CS.Customer_Added_By,
                            CS.Auto_Invoice_Delay,
                            CS.Is_Auto_Invoice_Enabled,
                            CS.To_Show_Intermediary_Stops,
                            CS.To_Stop_Tracking_Requirement,
                            CS.Document_Requirements,
                            CS.Data_Requirements,
                            CS.Sourcesystem_ID, 
                            CS.Sourcesystem_Name, 
                            CS.Created_By, 
                            CS.Created_Date, 
                            CS.Last_Modified_By, 
                            CS.Last_Modified_Date,  
                            CS.Hashkey
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.silver_Customer"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to create the view for the silver_Customer")
            print(e)
except Exception as e:
    logger.info('Failed for silver_Customer')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

