# Databricks notebook source
# MAGIC %md #Silver_Load

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze and load to Silver for Silver_Load with joins and transformations- Incremental load
# MAGIC * **Created Date:** 01/29/2024
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

#Create variables to store error log information

ErrorLogger = ErrorLogs("Silver_Load_BronzeToSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ10'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Load Aljex data to Silver_Load

# COMMAND ----------

try:
    DF_Aljex_Load = spark.sql("""            
									select 
									p.id as Load_ID,
									d.DW_Customer_ID as DW_Customer_ID,
									dc.DW_Carrier_ID   as DW_Carrier_ID,
									lm.DW_Mode_ID as DW_Mode_ID,
									le.DW_Equipment_ID as DW_Equipment_ID,
									p.covered_date as Load_Booked_At,
									p.key_c_user as Carrier_Rep_Name,
									p1.srv_rep as customer_Rep_Name,
									a.pnl_code as Carrier_Office,
									p.driver_cell_num as Driver_Phone_Number,
									dc.DOT_Number as DOT_Number,
									p1.status  as  Load_Status,
									p.pickup_name  as  Origin_Shippper_Name,
									p.origin_city  as  Origin_Shippper_City,
									p.origin_state  as  Origin_Shippper_State,
									p.pickup_zip_code  as  Origin_Shippper_Zip,
									p.consignee  as  Destination_Consignee_Name,
									p.consignee_address  as  Destination_Consignee_City,
									NULL  as  Destination_Consignee_State,
									p.consignee_zip_code  as  Destination_Consignee_Zip,
									le.Derived_Equipment_Type as Equipment_Type,
									lm.Derived_TransportMode  as Mode,
									p.pickup_date  as Ship_Date,
									p1.weight  as Total_Weight,
									p.miles  as Total_Miles,
									case when carrier_line_haul is null then 0.00 
									when carrier_line_haul is not null then decimal(carrier_line_haul)
									else 0.00 end +
									case when carrier_accessorial_rate1 is null then 0.00 
									when carrier_accessorial_rate1 is not null then decimal(carrier_accessorial_rate1)
									else 0.00 end +
									case when carrier_accessorial_rate2 is null then 0.00 
									when carrier_accessorial_rate2 is not null then decimal(carrier_accessorial_rate2)
									else 0.00 end +
									case when carrier_accessorial_rate3 is null then 0.00 
									when carrier_accessorial_rate3 is not null then decimal(carrier_accessorial_rate3)
									else 0.00 end +
									case when carrier_accessorial_rate4 is null then 0.00 
									when carrier_accessorial_rate4 is not null then decimal(carrier_accessorial_rate4)
									else 0.00 end +
									case when carrier_accessorial_rate5 is null then 0.00 
									when carrier_accessorial_rate5 is not null then decimal(carrier_accessorial_rate5)
									else 0.00 end +
									case when carrier_accessorial_rate6 is null then 0.00 
									when carrier_accessorial_rate6 is not null then decimal(carrier_accessorial_rate6)
									else 0.00 end +
									case when carrier_accessorial_rate7 is null then 0.00 
									when carrier_accessorial_rate7 is not null then decimal(carrier_accessorial_rate7)
									else 0.00 end +
									case when carrier_accessorial_rate8 is null then 0.00 
									when carrier_accessorial_rate8 is not null then decimal(carrier_accessorial_rate8)
									else 0.00 end as Expense,
									case when p.invoice_total is null then 0.00 else cast(p.invoice_total as decimal(10,2))end as Revenue,
									cast(case when pc.name like '%CAD' then CAST(Expense as decimal(10,2)) / CAST(c.conversion as decimal(10,2))
									when pc.name like 'CANADIAN R%' then CAST(Expense as decimal(10,2)) / CAST(c.conversion as decimal(10,2)) 
									when pc.name like '%CAN' then CAST(Expense as decimal(10,2)) / CAST(c.conversion as decimal(10,2)) 
									when pc.name like '%(CAD)%' then CAST(Expense as decimal(10,2)) / CAST(c.conversion as decimal(10,2))
									when pc.name like '%-C' then CAST(Expense as decimal(10,2)) / CAST(c.conversion as decimal(10,2)) 
									when pc.name like '%- C%' then CAST(Expense as decimal(10,2)) / CAST(c.conversion as decimal(10,2)) else CAST(Expense as decimal(10,2)) end AS decimal(10,2)) as Expense_Conversion,
									case when p.invoice_total is null then 0.00 else cast(p.invoice_total as decimal(10,2))end as Revenue_Conversion,
									CAST(Expense_Conversion-Revenue_Conversion as decimal(10,2)) as Margin,         
									"1" as Sourcesystem_ID,
									"Aljex" as Sourcesystem_Name,
									"Databricks" as Created_By,
									CURRENT_TIMESTAMP() as Created_Date,
									"Databricks" as  Last_Modified_By,
									CURRENT_TIMESTAMP() as Last_Modified_Date
									from bronze.projection_load_1 p inner join bronze.projection_load_2 p1 on p.id=p1.id
									left join bronze.projection_carrier pc on p.carrier_id=pc.id
									left join bronze.canada_conversions c on p.pickup_date=c.ship_date
									left join silver.Silver_customer d on p1.customer_id=d.Customer_ID
									left join silver.silver_Carrier dc on p.carrier_id =dc.Carrier_ID
									left join silver.Lookup_Equipment le on p.equipment=le.Source_Equipment_Type
									left join silver.Lookup_Mode lm on p.equipment=lm.Source_TransportMode
									left join bronze.aljex_user_report_listing a on  p1.sales_rep = a.sales_rep
                              """)
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for Fact_Load from Aljex {e}")
    print(e)
try:
    ##Create the dataframe DF_Aljex_Load
    DF_Aljex_Load = DF_Aljex_Load.dropDuplicates(["Load_ID", "DW_Customer_ID","DW_Carrier_ID","ship_Date"])
except Exception as e:
    logger.info(f"Unable to create DataFrame for Fact_Load from Aljex {e}")
    print(e)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Load Relay data to Silver_Load

# COMMAND ----------

try:
    DF_Relay_Load = spark.sql('''
                                    
                                    select 
                                    string(b.load_number) as Load_ID,
									d.DW_Customer_ID as DW_Customer_ID,
									dc.DW_Carrier_ID   as DW_Carrier_ID,
									lm.DW_Mode_ID as DW_Mode_ID,
									le.DW_Equipment_ID as DW_Equipment_ID,
									bo.booked_at as Load_Booked_At,
									bo.booked_by_name as Carrier_Rep_Name,
									r.full_name as customer_Rep_Name,
									NULL  as Carrier_Office,
									bo.driver_phone_number as Driver_Phone_Number,
									dc.DOT_Number as DOT_Number,
									bo.status  as  Load_Status,
									b.pickup_name  as  Origin_Shippper_Name,
									b.pickup_city  as  Origin_Shippper_City,
									b.pickup_state  as  Origin_Shippper_State,
									b.pickup_zip  as  Origin_Shippper_Zip,
									b.consignee_name  as  Destination_Consignee_Name,
									b.consignee_city  as  Destination_Consignee_City,
									b.consignee_state  as  Destination_Consignee_State,
									b.consignee_zip  as  Destination_Consignee_Zip,
									le.Derived_Equipment_Type as Equipment_Type,
									lm.Derived_TransportMode  as Mode,
									b.ship_date  as Ship_Date,
									bo.total_weight  as Total_Weight,
									bo.total_miles  as Total_Miles,
									CAST(b.carrier_accessorial_expense+b.carrier_fuel_expense+b.carrier_linehaul_expense as decimal(10,0)) as Expense,
									case when ic.total_amount is null then 0.00 else ic.total_amount - Expense end as Revenue,
									Revenue as Revenue_Conversion,
									Expense as Expense_Conversion,
									Expense - Revenue as Margin,
                                    "2" as Sourcesystem_ID,
                                    "Relay" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.big_export_projection b
                                    left join bronze.invoicing_credits ic on b.load_number=ic.relay_reference_number
                                    full outer join bronze.booking_projection bo on b.load_number=bo.relay_reference_number 
                                    left join Silver.Silver_Carrier dc on dc.Carrier_Name=bo.booked_carrier_name
									left join bronze.customer_profile_projection c on b.customer_id=c.customer_slug
									left join bronze.relay_users r on c.primary_relay_user_id=r.user_id
									left join silver.silver_customer d on b.customer_id=d.Customer_ID
									left join bronze.tendering_service_line tsl on b.load_number=tsl.relay_reference_number
									left join silver.Lookup_Equipment le on le.Source_Equipment_Type = tsl.service_line_type
									left join bronze.canonical_plan_projection cpp on b.load_number=cpp.relay_reference_number
									left join silver.Lookup_Mode lm on lm.Source_TransportMode=cpp.mode  where b.load_number is not null 
                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for Fact_Load from Aljex {e}")
    print(e)
try:
##Create the dataframe DF_Relay_Load
    DF_Relay_Load = DF_Relay_Load.dropDuplicates(["Load_ID","DW_Carrier_ID","DW_Customer_ID","Ship_Date"])
except Exception as e:
    logger.info(f"Unable to create DataFrame for Fact_Load from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Load Edge data to Silver_Load

# COMMAND ----------

try:
    DF_Edge_Load = spark.sql('''
                                    
                                    select 
                                    string(c.load_num) as Load_ID,
									d.DW_Customer_ID as DW_Customer_ID,
									dc.DW_Carrier_ID   as DW_Carrier_ID,
									lm.DW_Mode_ID as DW_Mode_ID,
									le.DW_Equipment_ID as DW_Equipment_ID,
									c.booked_date as Load_Booked_At,
									c.booked_by as Carrier_Rep_Name,
									NULL   as customer_Rep_Name,
									NULL  as Carrier_Office,
									NULL  as Driver_Phone_Number,
									c.status  as  Load_Status,
									c.shipper  as  Origin_Shippper_Name,
									c.shipper_city  as  Origin_Shippper_City,
									c.shipper_state  as  Origin_Shippper_State,
									c.shipper_zip  as  Origin_Shippper_Zip,
									c.consignee  as  Destination_Consignee_Name,
									c.consignee_city  as  Destination_Consignee_City,
									c.consignee_state  as  Destination_Consignee_State,
									c.consignee_zip  as  Destination_Consignee_Zip,
									le.Derived_Equipment_Type as Equipment_Type,
									lm.Derived_TransportMode  as Mode,
									c.ship_date  as Ship_Date,
									NULL   as Total_Weight,
									c.miles  as Total_Miles,
									c.total_revenue as Revenue,
									c.total_expense as Expense,
									c.total_revenue as Revenue_Conversion,
									c.total_expense as Expense_Conversion,
									c.total_margin as Margin,								
                                    "3" as Sourcesystem_ID,
                                    "Edge" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.cai_data  c  left join silver.silver_Customer d on c.cust_ref_num=d.Customer_ID
									left join silver.silver_Carrier dc on c.carrier=dc.Carrier_ID
									left join silver.Lookup_Mode lm on c.mode=lm.Source_TransportMode
									left join silver.Lookup_Equipment le on c.mode = le.Source_Equipment_Type
									

                                ''')  
except Exception as e:
    logger.info(f"Unable to write data into do the transformation for Fact_Load from Edge {e}")
    print(e)
try:
##Create the dataframe DF_Edge_Load
    DF_Edge_Load = DF_Edge_Load.dropDuplicates(["Load_ID","DW_Carrier_ID","DW_Customer_ID","Ship_Date"])
except Exception as e:
    logger.info(f"Unable to create DataFrame for Fact_Load from Edge {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform Union Operation to load data to Siver_Load table

# COMMAND ----------

try:
    #union all the Dataframes from all the source systems
    DF_Load_Temp = DF_Aljex_Load.unionByName(DF_Relay_Load,allowMissingColumns=True)
    DF_Fact_Load = DF_Load_Temp.unionByName(DF_Edge_Load,allowMissingColumns=True)

except Exception as e:
    logger.info(f"Unable to create Union the DataFrames for DF_Fact_Load {e}")
    print(e)
try:
##Dropping duplicates in the dataframe DF_Fact_Load
    DF_Fact_Load = DF_Fact_Load.dropDuplicates(["Load_ID","Sourcesystem_ID"])
except Exception as e:
    logger.info(f"Unable to create DataFrame for Fact_Load {e}")
    print(e)
#Creating the Hashkey for merging the data
try:
    Hashkey_Merge = ["Load_ID","DW_Customer_ID","DW_Carrier_ID","DW_Mode_ID","DW_Equipment_ID","Sourcesystem_ID"]

    DF_Fact_Load = DF_Fact_Load.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for DF_Fact_Load {e}")

#Create Temporary view for the DF_Fact_Load table
try:
    DF_Fact_Load.createOrReplaceTempView('VW_Fact_Load')
except Exception as e:
    logger.info(f"Unable to create the view for the DF_Fact_Load")
    print(e)	

# COMMAND ----------

# MAGIC %md #### Perform the Merge operation to the Silver_Load

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Load as CT using VW_Fact_Load as CS ON CS.Load_ID=CT.Load_ID AND CS.Sourcesystem_ID=CT.Sourcesystem_ID WHEN MATCHED AND CT.Hashkey!=CS.Hashkey THEN UPDATE SET 	
                                
                            CT.Hashkey=CS.Hashkey,
                            CT.Load_ID=CS.Load_ID,
                            CT.DW_Customer_ID=CS.DW_Customer_ID,
                            CT.DW_Carrier_ID=CS.DW_Carrier_ID,
                            CT.DW_Mode_ID=CS.DW_Mode_ID,
                            CT.DW_Equipment_ID=CS.DW_Equipment_ID,
                            CT.Load_Booked_At=CS.Load_Booked_At,
                            CT.Carrier_Rep_Name=CS.Carrier_Rep_Name,
                            CT.customer_Rep_Name=CS.customer_Rep_Name,
                            CT.Driver_Phone_Number=CS.Driver_Phone_Number,
                            CT.DOT_Number=CS.DOT_Number,
                            CT.Load_Status=CS.Load_Status,
                            CT.Origin_Shippper_Name=CS.Origin_Shippper_Name,
                            CT.Origin_Shippper_City=CS.Origin_Shippper_City,
                            CT.Origin_Shippper_State=CS.Origin_Shippper_State,
                            CT.Origin_Shippper_Zip=CS.Origin_Shippper_Zip,
                            CT.Destination_Consignee_Name=CS.Destination_Consignee_Name,
                            CT.Destination_Consignee_City=CS.Destination_Consignee_City,
                            CT.Destination_Consignee_State=CS.Destination_Consignee_State,
                            CT.Destination_Consignee_Zip=CS.Destination_Consignee_Zip,
                            CT.Ship_Date=CS.Ship_Date,
                            CT.Total_Weight=CS.Total_Weight,
                            CT.Total_Miles=CS.Total_Miles,
                            CT.Expense=CS.Expense,
                            CT.Revenue=CS.Revenue,
                            CT.Expense_Conversion=CS.Expense_Conversion,
                            CT.Revenue_Conversion=CS.Revenue_Conversion,
                            CT.Margin=CS.Margin,
                            CT.Sourcesystem_ID=CS.Sourcesystem_ID,
                            CT.Sourcesystem_Name=CS.Sourcesystem_Name,
                            CT.Created_By=CS.Created_By,
                            CT.Created_Date=CS.Created_Date,
                            CT.Last_Modified_By=CS.Last_Modified_By,
                            CT.Last_Modified_Date=CS.Last_Modified_Date
                            WHEN NOT MATCHED 
                            THEN INSERT 
                            (
                            CT.Hashkey,
                            CT.Load_ID,
                            CT.DW_Customer_ID,
                            CT.DW_Carrier_ID,
                            CT.DW_Mode_ID,
                            CT.DW_Equipment_ID,
                            CT.Load_Booked_At,
                            CT.Carrier_Rep_Name,
                            CT.customer_Rep_Name,
                            CT.Driver_Phone_Number,
                            CT.DOT_Number,
                            CT.Load_Status,
                            CT.Origin_Shippper_Name,
                            CT.Origin_Shippper_City,
                            CT.Origin_Shippper_State,
                            CT.Origin_Shippper_Zip,
                            CT.Destination_Consignee_Name,
                            CT.Destination_Consignee_City,
                            CT.Destination_Consignee_State,
                            CT.Destination_Consignee_Zip,
                            CT.Ship_Date,
                            CT.Total_Weight,
                            CT.Total_Miles,
                            CT.Expense,
                            CT.Revenue,
                            CT.Expense_Conversion,
                            CT.Revenue_Conversion,
                            CT.Margin,
                            CT.Sourcesystem_ID,
                            CT.Sourcesystem_Name,
                            CT.Created_By,
                            CT.Created_Date,
                            CT.Last_Modified_By,
                            CT.Last_Modified_Date
                            )
                            Values
                            (
                            CS.Hashkey,
                            CS.Load_ID,
                            CS.DW_Customer_ID,
                            CS.DW_Carrier_ID,
                            CS.DW_Mode_ID,
                            CS.DW_Equipment_ID,
                            CS.Load_Booked_At,
                            CS.Carrier_Rep_Name,
                            CS.customer_Rep_Name,
                            CS.Driver_Phone_Number,
                            CS.DOT_Number,
                            CS.Load_Status,
                            CS.Origin_Shippper_Name,
                            CS.Origin_Shippper_City,
                            CS.Origin_Shippper_State,
                            CS.Origin_Shippper_Zip,
                            CS.Destination_Consignee_Name,
                            CS.Destination_Consignee_City,
                            CS.Destination_Consignee_State,
                            CS.Destination_Consignee_Zip,
                            CS.Ship_Date,
                            CS.Total_Weight,
                            CS.Total_Miles,
                            CS.Expense,
                            CS.Revenue,
                            CS.Expense_Conversion,
                            CS.Revenue_Conversion,
                            CS.Margin,
                            CS.Sourcesystem_ID,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                            )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.silver_load"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to create the view for the Fact_Load")
            print(e)
except Exception as e:
    logger.info('Failed for silver_load')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

