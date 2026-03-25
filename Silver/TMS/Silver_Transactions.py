# Databricks notebook source
# MAGIC %md #Silver_Transactions

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Transactions table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 01/29/2024
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

ErrorLogger = ErrorLogs("Silver_Transactions_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ19'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Transactions table

# COMMAND ----------

# Select the Transactions table from the bronze layer for Relay vendor source and perform required transformations.
try:
    DF_Vendor = spark.sql('''
                                    select
                                    S.DW_Load_ID as DW_Load_ID ,
                                    S.DW_Carrier_ID as DW_Carrier_ID,
                                    S.DW_Customer_ID as DW_Customer_ID,
                                    v.charge_id	as	Charge_ID	,
                                    v.booking_id	as	Booking_ID	,
                                    v.charge_code	as	Charge_Code	,
                                    v.incurred_at	as	Charge_Incurred_At	,
                                    cast (v.amount as Float)	as	Amount	,
                                    v.currency	as	Currency	,
                                    v.vendor_transaction_id	as	Billing_Party_Transaction_ID	,
                                    v.`finalized?`	as	Is_Finalized	,
                                    v.finalized_at	as	Finalized_At	,
                                    v.`voided?`	as	Is_Voided	,
                                    v.voided_at	as	Voided_At	,
                                    NULL as Voided_By,
                                    NULL as Is_Invoiceable,
                                    NULL as Invoiced_At,
                                    NULL as Invoiced_By,
                                    "2" as Sourcesystem_ID,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.vendor_transaction_projection v inner join silver.Silver_Load s on v.relay_reference_number = s.load_id where
                                    s.Sourcesystem_ID=2
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Transactions table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Vendor = DF_Vendor.dropDuplicates(["DW_Load_ID","DW_Customer_ID","Charge_ID","Booking_ID"])

except Exception as e:
    logger.info(f"Unable to drop duplicates for the Transactions table from Relay {e}")
    print(e)

# COMMAND ----------

# Select the Transactions table from the bronze layer for Relay customer source and perform required transformations.
try:
    DF_Customer = spark.sql('''
                                    select
                                    S.DW_Load_ID as DW_Load_ID ,
                                    S.DW_Carrier_ID as DW_Carrier_ID,
                                    S.DW_Customer_ID as DW_Customer_ID,
                                    m.charge_id as Charge_ID	,
                                    NULL as Booking_ID,
                                    m.charge_code as Charge_Code	,
                                    m.incurred_at as Charge_Incurred_At	,
                                    cast (m.amount as Float) as Amount	,
                                    m.currency as Currency	,
                                    m.billing_party_transaction_id as Billing_Party_Transaction_ID,
                                    NULL as Is_Finalized,
                                    NULL as Finalized_At,
                                    m.`voided?` as Is_Voided	,
                                    m.voided_at as Voided_At	,
                                    m.voided_by as Voided_By,
                                    m.`invoiceable?` as Is_Invoiceable,
                                    m.invoiced_at as Invoiced_At,
                                    m.invoiced_by as Invoiced_By,
                                    "2" as Sourcesystem_ID,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.moneying_billing_party_transaction m inner join silver.Silver_Load s on m.relay_reference_number = s.load_id
                                    where s.Sourcesystem_ID=2
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Transactions table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Customer = DF_Customer.dropDuplicates(["DW_Load_ID","DW_Customer_ID","Charge_ID","Booking_ID"])

except Exception as e:
    logger.info(f"Unable to drop duplicates for the Transactions table from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Transactions

# COMMAND ----------

try:
    #union all the Dataframes from customer and vendor
    DF_Transactions = DF_Vendor.unionByName(DF_Customer,allowMissingColumns=True)
except Exception as e:
    logger.info(f"Unable to create Union the DataFrames for silver_Transactions {e}")

#Create the Hashkey to merge the data to Silver_Transactions table
try:
    Hashkey_Merge = ["DW_Load_ID","DW_Customer_ID","Charge_ID","Booking_ID"]

    DF_Transactions = DF_Transactions.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for Silver_Transactions {e}")
    print(e)

#Create a Temporary view for the Silver_Transactions table
try:
    DF_Transactions.createOrReplaceTempView('VW_Silver_Transactions')
except Exception as e:
    logger.info(f"Unable to create temporary view for the Silver_Transactions table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Transactions as CT using VW_Silver_Transactions as CS ON CS.Charge_ID = CT.Charge_ID and CT.DW_Load_ID = CS.DW_Load_ID When MATCHED and CT.Hashkey!=CS.Hashkey
                        THEN UPDATE SET 
                            CT.DW_Load_ID = CS.DW_Load_ID,
                            CT.DW_Carrier_ID = CS.DW_Carrier_ID,
                            CT.DW_Customer_ID = CS.DW_Customer_ID,
                            CT.Charge_ID = CS.Charge_ID,
                            CT.Booking_ID = CS.Booking_ID,
                            CT.Charge_Code = CS.Charge_Code,
                            CT.Charge_Incurred_At = CS.Charge_Incurred_At,
                            CT.Amount = CS.Amount,
                            CT.Currency = CS.Currency,
                            CT.Billing_Party_Transaction_ID = CS.Billing_Party_Transaction_ID,
                            CT.Is_Finalized = CS.Is_Finalized,
                            CT.Finalized_At = CS.Finalized_At,
                            CT.Is_Voided = CS.Is_Voided,
                            CT.Voided_At = CS.Voided_At,
                            CT.Voided_By = CS.Voided_By,
                            CT.Is_Invoiceable = CS.Is_Invoiceable,
                            CT.Invoiced_At = CS.Invoiced_At,
                            CT.Invoiced_By = CS.Invoiced_By,
                            CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                            CT.Sourcesystem_Name = CS.Sourcesystem_Name,
                            CT.Created_By = CS.Created_By,
                            CT.Created_Date = CS.Created_Date,
                            CT.Last_Modified_By = CS.Last_Modified_By,
                            CT.Last_Modified_Date = CS.Last_Modified_Date,
                            CT.Hashkey = CS.Hashkey    
                    WHEN NOT MATCHED
                    THEN INSERT 
                    (   CT.DW_Load_ID,
                        CT.DW_Carrier_ID,
                        CT.DW_Customer_ID,
                        CT.Charge_ID,
                        CT.Booking_ID,
                        CT.Charge_Code,
                        CT.Charge_Incurred_At,
                        CT.Amount,
                        CT.Currency,
                        CT.Billing_Party_Transaction_ID,
                        CT.Is_Finalized,
                        CT.Finalized_At,
                        CT.Is_Voided,
                        CT.Voided_At,
                        CT.Voided_By,
                        CT.Is_Invoiceable,
                        CT.Invoiced_At,
                        CT.Invoiced_By,
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
                            CS.DW_Load_ID,
                            CS.DW_Carrier_ID,
                            CS.DW_Customer_ID,
                            CS.Charge_ID,
                            CS.Booking_ID,
                            CS.Charge_Code,
                            CS.Charge_Incurred_At,
                            CS.Amount,
                            CS.Currency,
                            CS.Billing_Party_Transaction_ID,
                            CS.Is_Finalized,
                            CS.Finalized_At,
                            CS.Is_Voided,
                            CS.Voided_At,
                            CS.Voided_By,
                            CS.Is_Invoiceable,
                            CS.Invoiced_At,
                            CS.Invoiced_By,
                            CS.Sourcesystem_ID,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date,
                            CS.Hashkey
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Transactions"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Transactions table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Transactions')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)