# Databricks notebook source
# MAGIC %md #Silver_Tenders

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Tenders table with joins and transformations- Incremental load
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

ErrorLogger = ErrorLogs("Silver_Tenders_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ18'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Employee table

# COMMAND ----------

# Select the Tender table from the bronze layer for Relay source and perform required transformations.
try:
    DF_Tenders = spark.sql('''
                                    select 
                                        s.DW_Load_ID as DW_Load_ID,
                                        "2" as Sourcesystem_ID,
                                        ta.tender_id as Tender_ID,
                                        ta.relay_reference_number as Load_Number,
                                        ta.`accepted?` as Is_Accepted,
                                        ta.accepted_at as Accepted_At,
                                        ta.accepted_by as Accepted_By_ID,
                                        ru.full_name as Accepted_By_Name,
                                        tc.assigned_carrier_name as Assigned_Carrier,
                                        tc.assigned_carrier_name as Assigned_Carrier_SCAC,
                                        cp.command_builder_url as Command_Builder_URL,
                                        ta.shipment_id as Shipment_ID,
                                        ta.tender_on_behalf_of_id as Customer,
                                        ta.tendered_at as Tendered_At,
                                        tr.is_split as Is_Split,
                                        ts.split_orders as  Split_Orders,
                                        ts.remaining_orders as Remaining_Orders,
                                        tr.nfi_pro_number as NFI_Pro_Number,
                                        tr.original_shipment_id as Original_Shipment_ID,
                                        tr.order_numbers as Order_Numbers,
                                        tr.po_numbers as PO_Numbers,
                                        tr.target_shipment_id as Target_Shipment_ID,
                                        tr.is_cancelled as Is_Cancelled,
                                        tr.cancelled_by as Cancelled_By,
                                        tr.cancelled_at as Cancelled_At,
                                        "Relay" as Sourcesystem_Name,
                                        "Databricks" as Created_By,
                                        Current_timestamp() as Created_Date,
                                        "Databricks" as Last_Modified_By,
                                        Current_timestamp() as Last_Modified_Date from bronze.tendering_acceptance  ta left join bronze.tender_split_projection ts on
                                        ta.tender_id =ts.tender_id left join silver.Silver_Load s on ta.relay_reference_number = s.Load_ID LEFT JOIN bronze.tender_reference_numbers_projection  tr  on ta.tender_id = tr.tender_id
                                        left join bronze.tendering_tendered_assigned_carrier  tc on tc.tender_id =ta.tender_id left join bronze.customer_distance_projection cp on cp.tender_id = ta.tender_id left join bronze.relay_users  ru  on ru.user_id =ta.accepted_by where s.Sourcesystem_ID=2

                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Tenders table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Tenders =DF_Tenders.dropDuplicates(['Tender_ID','Load_Number'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Tenders table from Relay {e}")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Tender_Address

# COMMAND ----------

#Create the Hashkey to merge the data to Silver_Tender_Address table
try:
    Hashkey_Merge = ["Tender_ID","Load_Number","DW_Load_ID"]

    DF_Tenders = DF_Tenders.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_Tenders {e}")
    print(e)

#Create a Temporary view for the Silver_Tenders table
try:
    DF_Tenders.createOrReplaceTempView('VW_Tenders')
except Exception as e:
    logger.info(f"Unable to create temporary view for the Silver_Tenders table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Tenders as CT using VW_Tenders as CS ON CS.Load_Number=CT.Load_Number AND CS.Tender_ID=CT.Tender_ID  WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                    CT.DW_Load_ID= CS.DW_Load_ID,
                    CT.Sourcesystem_ID= CS.Sourcesystem_ID,
                    CT.Hashkey=CS.Hashkey,
                    CT.Tender_ID= CS.Tender_ID,
                    CT.Load_Number= CS.Load_Number,
                    CT.Is_Accepted= CS.Is_Accepted,
                    CT.Accepted_At= CS.Accepted_At,
                    CT.Accepted_By_ID= CS.Accepted_By_ID,
                    CT.Accepted_By_Name= CS.Accepted_By_Name,
                    CT.Assigned_Carrier= CS.Assigned_Carrier,
                    CT.Assigned_Carrier_SCAC= CS.Assigned_Carrier_SCAC,
                    CT.Command_Builder_URL= CS.Command_Builder_URL,
                    CT.Shipment_ID= CS.Shipment_ID,
                    CT.Customer= CS.Customer,
                    CT.Tendered_At= CS.Tendered_At,
                    CT.Is_Split= CS.Is_Split,
                    CT.Split_Orders= CS.Split_Orders,
                    CT.Remaining_Orders= CS.Remaining_Orders,
                    CT.NFI_Pro_Number= CS.NFI_Pro_Number,
                    CT.Original_Shipment_ID= CS.Original_Shipment_ID,
                    CT.Order_Numbers= CS.Order_Numbers,
                    CT.PO_Numbers= CS.PO_Numbers,
                    CT.Target_Shipment_ID= CS.Target_Shipment_ID,
                    CT.Is_Cancelled= CS.Is_Cancelled,
                    CT.Cancelled_By= CS.Cancelled_By,
                    CT.Cancelled_At= CS.Cancelled_At,
                    CT.Sourcesystem_Name= CS.Sourcesystem_Name,
                    CT.Created_By= CS.Created_By,
                    CT.Created_Date= CS.Created_Date,
                    CT.Last_Modified_By= CS.Last_Modified_By,
                    CT.Last_Modified_Date= CS.Last_Modified_Date
                    
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   
                        CT.DW_Load_ID,
                        CT.Sourcesystem_ID,
                        CT.Hashkey,
                        CT.Tender_ID,
                        CT.Load_Number,
                        CT.Is_Accepted,
                        CT.Accepted_At,
                        CT.Accepted_By_ID,
                        CT.Accepted_By_Name,
                        CT.Assigned_Carrier,
                        CT.Assigned_Carrier_SCAC,
                        CT.Command_Builder_URL,
                        CT.Shipment_ID,
                        CT.Customer,
                        CT.Tendered_At,
                        CT.Is_Split,
                        CT.Split_Orders,
                        CT.Remaining_Orders,
                        CT.NFI_Pro_Number,
                        CT.Original_Shipment_ID,
                        CT.Order_Numbers,
                        CT.PO_Numbers,
                        CT.Target_Shipment_ID,
                        CT.Is_Cancelled,
                        CT.Cancelled_By,
                        CT.Cancelled_At,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date
                        
                        )
                        VALUES
                        (
                            CS.DW_Load_ID,
                            CS.Sourcesystem_ID,
                            CS.Hashkey,
                            CS.Tender_ID,
                            CS.Load_Number,
                            CS.Is_Accepted,
                            CS.Accepted_At,
                            CS.Accepted_By_ID,
                            CS.Accepted_By_Name,
                            CS.Assigned_Carrier,
                            CS.Assigned_Carrier_SCAC,
                            CS.Command_Builder_URL,
                            CS.Shipment_ID,
                            CS.Customer,
                            CS.Tendered_At,
                            CS.Is_Split,
                            CS.Split_Orders,
                            CS.Remaining_Orders,
                            CS.NFI_Pro_Number,
                            CS.Original_Shipment_ID,
                            CS.Order_Numbers,
                            CS.PO_Numbers,
                            CS.Target_Shipment_ID,
                            CS.Is_Cancelled,
                            CS.Cancelled_By,
                            CS.Cancelled_At,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                            
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Tenders"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Tenders table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Tenders')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)