# Databricks notebook source
# MAGIC %md #Silver_Invoice_And_Payments

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Carrier table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 02/10/2024
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

ErrorLogger = ErrorLogs("silver_Invoice_And_Payments_BronzeToSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ9'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data from Relay to Silver_Invoice_And_Payments

# COMMAND ----------

# Select the Invoice table from the bronze layer for Relay source particularly for customer invoice and perform required transformations.     
try:
    DF_Relay_Invoice1= spark.sql('''
                                    select 
                                    s.DW_Customer_ID as DW_Customer_ID,
                                    s.DW_Carrier_ID as DW_Carrier_ID,
                                    s.DW_Load_ID as DW_Load_ID,
                                    "2" as Sourcesystem_ID,
                                    i.invoice_number as Invoice_Number,
                                    i.relay_reference_number as Load_Number,
                                    cast(tl.invoiced_at as TIMESTAMP_NTZ) as Invoice_Date,
                                    cast(tl.fuel_surcharge_amount as float) as Fuel_Surcharge_Amount,
                                    cast(tl.linehaul_amount as float) as Linehaul_Amount,
                                    cast(tl.accessorial_amount as float) as Accessorial_Amount,
                                    i.invoice_total_float as  Total_Amount,
                                    i.currency as Currency,
                                    i.charge_ids as Charge_ID,
                                    "TL-Customer" AS Invoice_Type,
                                    "Relay"	as	SourceSystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.invoicing_invoices i left join bronze.tl_invoice_projection tl
                                    on  i.relay_reference_number = tl.relay_reference_number 
                                    left join silver.silver_load s 
                                    on i.relay_reference_number = s.Load_ID
                                    where i.invoice_number is not null and s.Sourcesystem_ID=2
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Invoice table for Relay{e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Relay_Invoice1 = DF_Relay_Invoice1.dropDuplicates(['Invoice_Number','Load_Number','DW_Load_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Invoice table from Aljex {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load LTL Data from relay to Silver_Invoice_And_Payments

# COMMAND ----------

# Select the Carrier table from the bronze layer for Edge source and perform required transformations.
try:      
    DF_Relay_Invoice2 = spark.sql('''
                                    select
                                        s.DW_Customer_ID as DW_Customer_ID,
                                        s.DW_Carrier_ID as DW_Carrier_ID,
                                        s.DW_Load_ID as DW_Load_ID,
                                        "2" as Sourcesystem_ID,
                                        ltl.invoice_number as Invoice_Number,
                                        tc.relay_reference_number as Load_Number,
                                        cast(ltl.invoiced_at AS TIMESTAMP_NTZ) as Invoice_Date,
                                        cast(tc.fuel_surcharge_amount as float) as Fuel_Surcharge_Amount,
                                        cast(tc.linehaul_amount as float) as Linehaul_Amount,
                                        cast(tc.accessorial_amount as float) as Accessorial_Amount,
                                        cast(ltl.invoiced_amount as float) as Total_Amount,
                                        "LTL-Customer" AS Invoice_Type,
                                        "Relay" as Sourcesystem_Name,
                                        "Databricks" as Created_By,
                                        CURRENT_TIMESTAMP() as Created_Date,
                                        "Databricks" as  Last_Modified_By,
                                        CURRENT_TIMESTAMP() as Last_Modified_Date
                                        from bronze.target_customer_money tc LEFT JOIN bronze.ltl_invoice_projection ltl on tc.relay_reference_number=ltl.relay_reference_number  left join  silver.Silver_Load s on tc.relay_Reference_number=s.Load_ID 
                                        where ltl.invoice_number is not null and s.Sourcesystem_ID=2
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Invoice table for Relay {e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Relay_Invoice2 = DF_Relay_Invoice2.dropDuplicates(['Invoice_Number','Load_Number','DW_Load_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Invoice table from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load LTL Data from relay to Silver_Invoice_And_Payments

# COMMAND ----------

# Select the Carrier table from the bronze layer for Edge source and perform required transformations.
try:      
    DF_Relay_Invoice3 = spark.sql('''
                                    select
                                        s.DW_Customer_ID as DW_Customer_ID,
                                        s.DW_Carrier_ID as DW_Carrier_ID,
                                        s.DW_Load_ID as DW_Load_ID,
                                        "2" as Sourcesystem_ID,
                                        vi.invoice_number as Invoice_Number,
                                        bo.relay_reference_number as Load_Number,
                                        cast(vi.invoice_date AS TIMESTAMP_NTZ)as Invoice_Date,
                                        cast(vi.total_approved_amount_to_pay AS Float)as Total_Amount,
                                        vi.currency as Currency,
                                        "Carrier" AS Invoice_Type,
                                        a.due_date  as Due_Date,
                                        cast(a.payment_amount AS Float) as Payment_Amount,
                                        a.payment_date as Payment_Date,
                                        a.payment_num as Payment_Number,
                                        "Relay" as Sourcesystem_Name,
                                        "Databricks" as Created_By,
                                        CURRENT_TIMESTAMP() as Created_Date,
                                        "Databricks" as  Last_Modified_By,
                                        CURRENT_TIMESTAMP() as Last_Modified_Date
                                        from  bronze.integration_hubtran_vendor_invoice_approved vi LEFT JOIN bronze.booking_projection bo
                                        ON vi.booking_id=bo.booking_id LEFT JOIN  bronze.ap_lawson_appt a on  a.invoice_number=vi.invoice_number
                                        left join  Silver.silver_Load s on bo.relay_reference_number=s.Load_ID
                                       where vi.invoice_number is not null and s.Sourcesystem_ID=2 
                                ''')  
except Exception as e:
    logger.info(f"Unable to perform the transformations for Invoice table for Relay {e}")
    print(e)

try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Relay_Invoice3 = DF_Relay_Invoice3.dropDuplicates(['Invoice_Number','Load_Number','DW_Load_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Invoice table from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Union Operation to form Silver_Invoice_And_Payments

# COMMAND ----------

try:
    #Union all the Dataframes from all the source systems
    DF_Invoice_Temp = DF_Relay_Invoice1.unionByName(DF_Relay_Invoice2,allowMissingColumns=True)
    DF_Invoice = DF_Invoice_Temp.unionByName(DF_Relay_Invoice3,allowMissingColumns=True)
except Exception as e:
    logger.info(f"Unable to Union data from three sources to form silver_Invoice_And_Payments {e}")


#Create the Hashkeyto merge 
try:
    Hashkey_Merge = (["DW_Load_ID","Load_Number","Charge_ID","Sourcesystem_ID","Invoice_Number","Invoice_Date"])

    DF_Invoice = DF_Invoice.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_Invoice_And_Payments {e}")
    print(e)
try:
    Mergekey_Merge = (["DW_Load_ID","Load_Number","Invoice_Number"])

    DF_Invoice = DF_Invoice.withColumn("MergeKey",md5(concat_ws("",*Mergekey_Merge)))
except Exception as e:
    logger.info(f"Unable to create MergeKey for silver_Invoice_And_Payments {e}")
    print(e)   
try:
#Use dropduplicates function to drop the duplicate columns and store it in the dataframe.
    DF_Invoice = DF_Invoice.dropDuplicates(["DW_Load_ID","Invoice_Number","Load_Number"])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Invoice table from Relay {e}")
    print(e)

#Create Temporary view for the silver_Invoice_And_Payments table
try:
    DF_Invoice.createOrReplaceTempView('VW_Invoice')
except Exception as e:
    logger.info(f"Unable to create the view for the silver_Invoice_And_Payments")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Merge Operation to form Silver_Invoice_And_Payments

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Invoice_And_Payments as CT using VW_Invoice as CS ON CS.MergeKey=CT.MergeKey WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET 
                    CT.DW_Customer_ID=CS.DW_Customer_ID,
                    CT.DW_Carrier_ID=CS.DW_Carrier_ID,
                    CT.DW_Load_ID=CS.DW_Load_ID,
                    CT.MergeKey=CS.MergeKey,
                    CT.Sourcesystem_ID=CS.Sourcesystem_ID,
                    CT.Hashkey=CS.Hashkey,
                    CT.Invoice_Type=CS.Invoice_Type,
                    CT.Invoice_Number=CS.Invoice_Number,
                    CT.Load_Number=CS.Load_Number,
                    CT.Invoice_Date=CS.Invoice_Date,
                    CT.Fuel_Surcharge_Amount=CS.Fuel_Surcharge_Amount,
                    CT.Linehaul_Amount=CS.Linehaul_Amount,
                    CT.Accessorial_Amount=CS.Accessorial_Amount,
                    CT.Total_Amount=CS.Total_Amount,
                    CT.Currency=CS.Currency,
                    CT.Charge_ID=CS.Charge_ID,
                    CT.Due_Date=CS.Due_Date,
                    CT.Payment_Amount=CS.Payment_Amount,
                    CT.Payment_Date=CS.Payment_Date,
                    CT.Payment_Number=CS.Payment_Number,
                    CT.Sourcesystem_Name=CS.Sourcesystem_Name,
                    CT.Created_By=CS.Created_By,
                    CT.Created_Date=CS.Created_Date,
                    CT.Last_Modified_By=CS.Last_Modified_By,
                    CT.Last_Modified_Date=CS.Last_Modified_Date
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   CT.DW_Customer_ID,
                        CT.DW_Carrier_ID,
                        CT.DW_Load_ID,
                        CT.MergeKey,
                        CT.Sourcesystem_ID,
                        CT.Hashkey,
                        CT.Invoice_Type,
                        CT.Invoice_Number,
                        CT.Load_Number,
                        CT.Invoice_Date,
                        CT.Fuel_Surcharge_Amount,
                        CT.Linehaul_Amount,
                        CT.Accessorial_Amount,
                        CT.Total_Amount,
                        CT.Currency,
                        CT.Charge_ID,
                        CT.Due_Date,
                        CT.Payment_Amount,
                        CT.Payment_Date,
                        CT.Payment_Number,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date
                        )
                        VALUES
                        (
                        CS.DW_Customer_ID,
                        CS.DW_Carrier_ID,
                        CS.DW_Load_ID,
                        CS.MergeKey,
                        CS.Sourcesystem_ID,
                        CS.Hashkey,
                        CS.Invoice_Type,
                        CS.Invoice_Number,
                        CS.Load_Number,
                        CS.Invoice_Date,
                        CS.Fuel_Surcharge_Amount,
                        CS.Linehaul_Amount,
                        CS.Accessorial_Amount,
                        CS.Total_Amount,
                        CS.Currency,
                        CS.Charge_ID,
                        CS.Due_Date,
                        CS.Payment_Amount,
                        CS.Payment_Date,
                        CS.Payment_Number,
                        CS.Sourcesystem_Name,
                        CS.Created_By,
                        CS.Created_Date,
                        CS.Last_Modified_By,
                        CS.Last_Modified_Date
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Invoice_And_Payments"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Inoice_And_Payments table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Inoice_And_Payments')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

