# Databricks notebook source
# MAGIC %md
# MAGIC ## Loads_Split
# MAGIC * **Description:** To extract data from Silver to Gold as delta file
# MAGIC * **Created Date:** 03/07/2025
# MAGIC * **Created By:** Uday
# MAGIC * **Modified Date:** 06/10/2025
# MAGIC * **Modified By:** Uday
# MAGIC * **Changes Made:** Logic Update

# COMMAND ----------

# MAGIC %md
# MAGIC ####Incremental Load View

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Gold.VW_Fact_Loads_Split AS
# MAGIC
# MAGIC SELECT *, MD5(
# MAGIC   COALESCE(DW_Load_ID, '') ||
# MAGIC   COALESCE(Load_Counter, '') ||
# MAGIC   COALESCE(Load_Number::STRING, '') ||
# MAGIC   COALESCE(Admin_Fees_Carrier::STRING, '') ||
# MAGIC   COALESCE(Admin_Fees_Customer::STRING, '') ||
# MAGIC   COALESCE(Billing_Status, '') ||
# MAGIC   COALESCE(Actual_Delivered_Date::STRING, '') ||
# MAGIC   COALESCE(Booking_Type, '') ||
# MAGIC   COALESCE(Booked_Date::STRING, '') ||
# MAGIC   COALESCE(Carrier_Fuel_Surcharge::STRING, '') ||
# MAGIC   COALESCE(Carrier_linehaul::STRING, '') ||
# MAGIC   COALESCE(Office_Type, '') ||
# MAGIC   COALESCE(Carrier_ID, '') ||
# MAGIC   COALESCE(Carrier_Name, '') ||
# MAGIC   COALESCE(Carrier_Other_Fees::STRING, '') ||
# MAGIC   COALESCE(Carrier_Rep_ID, '') ||
# MAGIC   COALESCE(Carrier_Rep, '') ||
# MAGIC   COALESCE(Combined_Load, '') ||
# MAGIC   COALESCE(Consignee_Delivery_Address, '') ||
# MAGIC   COALESCE(Consignee_Name, '') ||
# MAGIC   COALESCE(Consignee_Pickup_Address, '') ||
# MAGIC   COALESCE(Customer_Fuel_Surcharge::STRING, '') ||
# MAGIC   COALESCE(Customer_linehaul::STRING, '') ||
# MAGIC   COALESCE(Customer_Master, '') ||
# MAGIC   COALESCE(Customer_ID, '') ||
# MAGIC   COALESCE(Customer_Name, '') ||
# MAGIC   COALESCE(Customer_Other_Fees::STRING, '') ||
# MAGIC   COALESCE(Customer_Rep_ID, '') ||
# MAGIC   COALESCE(Customer_Rep, '') ||
# MAGIC   COALESCE(Days_Ahead, '') ||
# MAGIC   COALESCE(Bounced_Date::STRING, '') ||
# MAGIC   COALESCE(Cancelled_Date::STRING, '') ||
# MAGIC   COALESCE(Delivery_Pickup_Charges_Carrier::STRING, '') ||
# MAGIC   COALESCE(Delivery_Pickup_Charges_Customer::STRING, '') ||
# MAGIC   COALESCE(Delivery_Appointment_Date::STRING, '') ||
# MAGIC   COALESCE(Delivery_EndDate::STRING, '') ||
# MAGIC   COALESCE(Destination_City, '') ||
# MAGIC   COALESCE(Destination_State, '') ||
# MAGIC   COALESCE(Destination_Zip, '') ||
# MAGIC   COALESCE(DOT_Number, '') ||
# MAGIC   COALESCE(Delivery_StartDate::STRING, '') ||
# MAGIC   COALESCE(Equipment, '') ||
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Carrier::STRING, '') ||
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Customer::STRING, '') ||
# MAGIC   COALESCE(Expense::STRING, '') ||
# MAGIC   COALESCE(Financial_Period_Year, '') ||
# MAGIC   COALESCE(GreenScreen_Rate::STRING, '') ||
# MAGIC   COALESCE(Invoice_Amount::STRING, '') ||
# MAGIC   COALESCE(Invoiced_Date::STRING, '') ||
# MAGIC   COALESCE(Lawson_ID, '') ||
# MAGIC   COALESCE(Load_Currency, '') ||
# MAGIC   COALESCE(Load_Flag, '') ||
# MAGIC   COALESCE(Load_Lane, '') ||
# MAGIC   COALESCE(Load_Status, '') ||
# MAGIC   COALESCE(Load_Unload_Charges_Carrier::STRING, '') ||
# MAGIC   COALESCE(Load_Unload_Charges_Customer::STRING, '') ||
# MAGIC   COALESCE(Margin::STRING, '') ||
# MAGIC   COALESCE(Market_Buy_Rate::STRING, '') ||
# MAGIC   COALESCE(Market_Destination, '') ||
# MAGIC   COALESCE(Market_Lane, '') ||
# MAGIC   COALESCE(Market_Origin, '') ||
# MAGIC   COALESCE(MC_Number, '') ||
# MAGIC   COALESCE(Miles, '') ||
# MAGIC   COALESCE(Miscellaneous_Chargers_Carrier::STRING, '') ||
# MAGIC   COALESCE(Miscellaneous_Chargers_Customers::STRING, '') ||
# MAGIC   COALESCE(Mode, '') ||
# MAGIC   COALESCE(On_Time_Delivery, '') ||
# MAGIC   COALESCE(On_Time_Pickup, '') ||
# MAGIC   COALESCE(Origin_City, '') ||
# MAGIC   COALESCE(Origin_State, '') ||
# MAGIC   COALESCE(Origin_Zip, '') ||
# MAGIC   COALESCE(Permits_Compliance_Charges_Carrier::STRING, '') ||
# MAGIC   COALESCE(Permits_Compliance_Charges_Customer::STRING, '') ||
# MAGIC   COALESCE(Pickup_Appointment_Date::STRING, '') ||
# MAGIC   COALESCE(Actual_Pickup_Date::STRING, '') ||
# MAGIC   COALESCE(Pickup_EndDate::STRING, '') ||
# MAGIC   COALESCE(Pickup_StartDate::STRING, '') ||
# MAGIC   COALESCE(PO_Number, '') ||
# MAGIC   COALESCE(PreBookStatus, '') ||
# MAGIC   COALESCE(Revenue::STRING, '') ||
# MAGIC   COALESCE(Rolled_Date::STRING, '') ||
# MAGIC   COALESCE(Sales_Rep_ID, '') ||
# MAGIC   COALESCE(Sales_Rep, '') ||
# MAGIC   COALESCE(Ship_Date::STRING, '') ||
# MAGIC   COALESCE(Spot_Margin, '') ||
# MAGIC   COALESCE(Spot_Revenue, '') ||
# MAGIC   COALESCE(Tender_Source_Type, '') ||
# MAGIC   COALESCE(Tendered_Date::STRING, '') ||
# MAGIC   COALESCE(TMS_System, '') ||
# MAGIC   COALESCE(Trailer_Number, '') ||
# MAGIC   COALESCE(Transit_Routing_Chargers_Carrier::STRING, '') ||
# MAGIC   COALESCE(Transit_Routing_Chargers_Customer::STRING, '') ||
# MAGIC   COALESCE(Week_Number, '')
# MAGIC ) AS HASHKEY FROM
# MAGIC (SELECT DISTINCT
# MAGIC   CONCAT(DW_Load_ID, '-S') AS DW_load_ID,
# MAGIC   '0.5' AS Load_Counter,
# MAGIC   Load_number         AS Load_Number,
# MAGIC   Admin_Fees_Carrier_USD/2     AS Admin_Fees_Carrier,
# MAGIC   Admin_Fees_Customer_USD/2   AS Admin_Fees_Customer,
# MAGIC   Billing_Status,
# MAGIC   Actual_Delivered_Date,
# MAGIC   Booking_Type,
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD/2 AS Carrier_Fuel_Surcharge,
# MAGIC   Carrier_Linehaul_USD/2       AS Carrier_linehaul,
# MAGIC   "Shipper" AS Office_Type,
# MAGIC   Customer_New_Office          AS Office,
# MAGIC   Carrier_ID,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD/2      AS Carrier_Other_Fees,
# MAGIC   Carrier_Rep_ID,
# MAGIC   Carrier_Rep,
# MAGIC   Combined_Load,
# MAGIC   Consignee_Delivery_Address,
# MAGIC   Consignee_Name,
# MAGIC   Consignee_Pickup_Address,
# MAGIC   Customer_Fuel_Surcharge_USD/2  AS Customer_Fuel_Surcharge,
# MAGIC   Customer_Linehaul_USD/2     AS Customer_linehaul,
# MAGIC   Customer_Master,
# MAGIC   Customer_ID,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD/2     AS Customer_Other_Fees,
# MAGIC   Customer_Rep_ID,
# MAGIC   Customer_Rep,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   Delivery_Pickup_Charges_Carrier_USD/2  AS Delivery_Pickup_Charges_Carrier,
# MAGIC   Delivery_Pickup_Charges_Customer_USD/2 AS Delivery_Pickup_Charges_Customer,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   Equipment_Vehicle_Charges_Carrier_USD/2  AS Equipment_Vehicle_Charges_Carrier,
# MAGIC   Equipment_Vehicle_Charges_Customer_USD/2 AS Equipment_Vehicle_Charges_Customer,
# MAGIC   Expense_USD/2   AS Expense,
# MAGIC   Financial_Period_Year,
# MAGIC   GreenScreen_Rate_USD  AS GreenScreen_Rate,
# MAGIC   Invoice_Amount_USD/2    AS Invoice_Amount,
# MAGIC   Invoiced_Date,
# MAGIC   Lawson_ID,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   Load_Lane,
# MAGIC   Load_Status,
# MAGIC   Load_Unload_Charges_Carrier_USD/2   AS Load_Unload_Charges_Carrier,
# MAGIC   Load_Unload_Charges_Customer_USD/2  AS Load_Unload_Charges_Customer,
# MAGIC   Margin_USD/2       AS Margin,
# MAGIC   Market_Buy_Rate_USD   AS Market_Buy_Rate,
# MAGIC   Market_Destination,
# MAGIC   Market_Lane,
# MAGIC   Market_Origin,
# MAGIC   MC_Number,
# MAGIC   Miles,
# MAGIC   Miscellaneous_Chargers_Carrier_USD/2   AS Miscellaneous_Chargers_Carrier,
# MAGIC   Miscellaneous_Chargers_Customers_USD/2  AS Miscellaneous_Chargers_Customers,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   Permits_Compliance_Charges_Carrier_USD/2   AS Permits_Compliance_Charges_Carrier,
# MAGIC   Permits_Compliance_Charges_Customer_USD/2  AS Permits_Compliance_Charges_Customer,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   PO_Number,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD/2   AS Revenue,
# MAGIC   Rolled_Date,
# MAGIC   Sales_Rep_ID,
# MAGIC   Sales_Rep,
# MAGIC   Ship_Date,
# MAGIC   Spot_Margin,
# MAGIC   Spot_Revenue,
# MAGIC   Tender_Source_Type,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   Trailer_Number,
# MAGIC   Transit_Routing_Chargers_Carrier_USD/2   AS Transit_Routing_Chargers_Carrier,
# MAGIC   Transit_Routing_Chargers_Customer_USD/2  AS Transit_Routing_Chargers_Customer,
# MAGIC   Week_Number
# MAGIC
# MAGIC FROM Silver.silver_aljex
# MAGIC WHERE Last_Modified_Date > (
# MAGIC     SELECT LastLoadDateValue - INTERVAL 1 DAY
# MAGIC     FROM metadata.mastermetadata
# MAGIC     WHERE TableID = 'SL4'
# MAGIC )
# MAGIC   AND Is_Deleted = 0
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   CONCAT(DW_Load_ID, '-C') AS DW_load_ID,
# MAGIC   '0.5' AS Load_Counter,
# MAGIC   Load_number         AS Load_ID,
# MAGIC   Admin_Fees_Carrier_USD/2     AS Admin_Fees_Carrier,
# MAGIC   Admin_Fees_Customer_USD/2   AS Admin_Fees_Customer,
# MAGIC   Billing_Status,
# MAGIC   Actual_Delivered_Date,
# MAGIC   Booking_Type,
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD/2 AS Carrier_Fuel_Surcharge,
# MAGIC   Carrier_Linehaul_USD/2       AS Carrier_linehaul,
# MAGIC   "Carrier" AS Office_Type,
# MAGIC   Carrier_New_Office          AS Office,
# MAGIC   Carrier_ID,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD/2      AS Carrier_Other_Fees,
# MAGIC   Carrier_Rep_ID,
# MAGIC   Carrier_Rep,
# MAGIC   Combined_Load,
# MAGIC   Consignee_Delivery_Address,
# MAGIC   Consignee_Name,
# MAGIC   Consignee_Pickup_Address,
# MAGIC   Customer_Fuel_Surcharge_USD/2  AS Customer_Fuel_Surcharge,
# MAGIC   Customer_Linehaul_USD/2     AS Customer_linehaul,
# MAGIC   Customer_Master,
# MAGIC   Customer_ID,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD/2     AS Customer_Other_Fees,
# MAGIC   Customer_Rep_ID,
# MAGIC   Customer_Rep,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   Delivery_Pickup_Charges_Carrier_USD/2  AS Delivery_Pickup_Charges_Carrier,
# MAGIC   Delivery_Pickup_Charges_Customer_USD/2 AS Delivery_Pickup_Charges_Customer,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   Equipment_Vehicle_Charges_Carrier_USD/2  AS Equipment_Vehicle_Charges_Carrier,
# MAGIC   Equipment_Vehicle_Charges_Customer_USD/2 AS Equipment_Vehicle_Charges_Customer,
# MAGIC   Expense_USD/2   AS Expense,
# MAGIC   Financial_Period_Year,
# MAGIC   GreenScreen_Rate_USD  AS GreenScreen_Rate,
# MAGIC   Invoice_Amount_USD/2    AS Invoice_Amount,
# MAGIC   Invoiced_Date,
# MAGIC   Lawson_ID,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   Load_Lane,
# MAGIC   Load_Status,
# MAGIC   Load_Unload_Charges_Carrier_USD/2   AS Load_Unload_Charges_Carrier,
# MAGIC   Load_Unload_Charges_Customer_USD/2  AS Load_Unload_Charges_Customer,
# MAGIC   Margin_USD/2       AS Margin,
# MAGIC   Market_Buy_Rate_USD   AS Market_Buy_Rate,
# MAGIC   Market_Destination,
# MAGIC   Market_Lane,
# MAGIC   Market_Origin,
# MAGIC   MC_Number,
# MAGIC   Miles,
# MAGIC   Miscellaneous_Chargers_Carrier_USD/2   AS Miscellaneous_Chargers_Carrier,
# MAGIC   Miscellaneous_Chargers_Customers_USD/2  AS Miscellaneous_Chargers_Customers,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   Permits_Compliance_Charges_Carrier_USD/2   AS Permits_Compliance_Charges_Carrier,
# MAGIC   Permits_Compliance_Charges_Customer_USD/2  AS Permits_Compliance_Charges_Customer,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   PO_Number,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD/2   AS Revenue,
# MAGIC   Rolled_Date,
# MAGIC   Sales_Rep_ID,
# MAGIC   Sales_Rep,
# MAGIC   Ship_Date,
# MAGIC   Spot_Margin,
# MAGIC   Spot_Revenue,
# MAGIC   Tender_Source_Type,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   Trailer_Number,
# MAGIC   Transit_Routing_Chargers_Carrier_USD/2   AS Transit_Routing_Chargers_Carrier,
# MAGIC   Transit_Routing_Chargers_Customer_USD/2  AS Transit_Routing_Chargers_Customer,
# MAGIC   Week_Number
# MAGIC
# MAGIC FROM Silver.silver_aljex
# MAGIC WHERE Last_Modified_Date > (
# MAGIC     SELECT LastLoadDateValue - INTERVAL 1 DAY
# MAGIC     FROM metadata.mastermetadata
# MAGIC     WHERE TableID = 'SL4'
# MAGIC )
# MAGIC   AND Is_Deleted = 0
# MAGIC
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   CONCAT(DW_Load_ID, '-S'),
# MAGIC   "0.5" AS Load_Counter,
# MAGIC   Load_ID AS Load_ID,
# MAGIC   Admin_Fees_Carrier_USD/2,
# MAGIC   Admin_Fees_Customer_USD/2,
# MAGIC   Billing_Status,
# MAGIC   Actual_Delivered_Date,
# MAGIC   Booking_Type,
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD/2,
# MAGIC   Carrier_Linehaul_USD/2,
# MAGIC   "Shipper",
# MAGIC   Customer_New_Office AS Office_Type,
# MAGIC   Carrier_ID,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD/2,
# MAGIC   Carrier_Rep_ID,
# MAGIC   Carrier_Rep,
# MAGIC   Combined_Load,
# MAGIC   Consignee_Delivery_Address,
# MAGIC   Consignee_Name,
# MAGIC   Consignee_Pickup_Address,
# MAGIC   Customer_Fuel_Surcharge_USD/2,
# MAGIC   Customer_Linehaul_USD/2,
# MAGIC   Customer_Master,
# MAGIC   Customer_ID,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD/2,
# MAGIC   Customer_Rep_ID,
# MAGIC   Customer_Rep,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   Delivery_Pickup_Charges_Carrier_USD/2,
# MAGIC   Delivery_Pickup_Charges_Customer_USD/2,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   Equipment_Vehicle_Charges_Carrier_USD/2,
# MAGIC   Equipment_Vehicle_Charges_Customer_USD/2,
# MAGIC   Expense_USD/2,
# MAGIC   Financial_Period_Year,
# MAGIC   GreenScreen_Rate_USD,
# MAGIC   Invoice_Amount_USD/2,
# MAGIC   Invoiced_Date,
# MAGIC   Lawson_ID,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   Load_Lane,
# MAGIC   Load_Status,
# MAGIC   Load_Unload_Charges_Carrier_USD/2,
# MAGIC   Load_Unload_Charges_Customer_USD/2,
# MAGIC   Margin_USD/2,
# MAGIC   Market_Buy_Rate_USD,
# MAGIC   Market_Destination,
# MAGIC   Market_Lane,
# MAGIC   Market_Origin,
# MAGIC   MC_Number,
# MAGIC   Miles,
# MAGIC   Miscellaneous_Chargers_Carrier_USD/2,
# MAGIC   Miscellaneous_Chargers_Customers_USD/2,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   Permits_Compliance_Charges_Carrier_USD/2,
# MAGIC   Permits_Compliance_Charges_Customer_USD/2,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   PO_Number,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD/2,
# MAGIC   Rolled_Date,
# MAGIC   Sales_Rep_ID,
# MAGIC   Sales_Rep,
# MAGIC   Ship_Date,
# MAGIC   Spot_Margin,
# MAGIC   Spot_Revenue,
# MAGIC   Tender_Source_Type,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   Trailer_Number,
# MAGIC   Transit_Routing_Chargers_Carrier_USD/2,
# MAGIC   Transit_Routing_Chargers_Customer_USD/2,
# MAGIC   Week_Number
# MAGIC FROM Silver.silver_relay
# MAGIC WHERE Last_Modified_Date > (
# MAGIC     SELECT LastLoadDateValue - INTERVAL 1 DAY
# MAGIC     FROM metadata.mastermetadata
# MAGIC     WHERE TableID = 'SL5'
# MAGIC )
# MAGIC   AND Is_Deleted = 0
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   CONCAT(DW_Load_ID, '-C'),
# MAGIC   "0.5" AS Load_Counter,
# MAGIC   Load_ID AS Load_ID,
# MAGIC   Admin_Fees_Carrier_USD/2,
# MAGIC   Admin_Fees_Customer_USD/2,
# MAGIC   Billing_Status,
# MAGIC   Actual_Delivered_Date,
# MAGIC   Booking_Type,
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD/2,
# MAGIC   Carrier_Linehaul_USD/2,
# MAGIC   "Carrier",
# MAGIC   Carrier_New_Office AS Office_Type,
# MAGIC   Carrier_ID,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD/2,
# MAGIC   Carrier_Rep_ID,
# MAGIC   Carrier_Rep,
# MAGIC   Combined_Load,
# MAGIC   Consignee_Delivery_Address,
# MAGIC   Consignee_Name,
# MAGIC   Consignee_Pickup_Address,
# MAGIC   Customer_Fuel_Surcharge_USD/2,
# MAGIC   Customer_Linehaul_USD/2,
# MAGIC   Customer_Master,
# MAGIC   Customer_ID,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD/2,
# MAGIC   Customer_Rep_ID,
# MAGIC   Customer_Rep,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   Delivery_Pickup_Charges_Carrier_USD/2,
# MAGIC   Delivery_Pickup_Charges_Customer_USD/2,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   Equipment_Vehicle_Charges_Carrier_USD/2,
# MAGIC   Equipment_Vehicle_Charges_Customer_USD/2,
# MAGIC   Expense_USD/2,
# MAGIC   Financial_Period_Year,
# MAGIC   GreenScreen_Rate_USD,
# MAGIC   Invoice_Amount_USD/2,
# MAGIC   Invoiced_Date,
# MAGIC   Lawson_ID,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   Load_Lane,
# MAGIC   Load_Status,
# MAGIC   Load_Unload_Charges_Carrier_USD/2,
# MAGIC   Load_Unload_Charges_Customer_USD/2,
# MAGIC   Margin_USD/2,
# MAGIC   Market_Buy_Rate_USD,
# MAGIC   Market_Destination,
# MAGIC   Market_Lane,
# MAGIC   Market_Origin,
# MAGIC   MC_Number,
# MAGIC   Miles,
# MAGIC   Miscellaneous_Chargers_Carrier_USD/2,
# MAGIC   Miscellaneous_Chargers_Customers_USD/2,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   Permits_Compliance_Charges_Carrier_USD/2,
# MAGIC   Permits_Compliance_Charges_Customer_USD/2,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   PO_Number,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD/2,
# MAGIC   Rolled_Date,
# MAGIC   Sales_Rep_ID,
# MAGIC   Sales_Rep,
# MAGIC   Ship_Date,
# MAGIC   Spot_Margin,
# MAGIC   Spot_Revenue,
# MAGIC   Tender_Source_Type,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   Trailer_Number,
# MAGIC   Transit_Routing_Chargers_Carrier_USD/2,
# MAGIC   Transit_Routing_Chargers_Customer_USD/2,
# MAGIC   Week_Number
# MAGIC FROM Silver.silver_relay
# MAGIC WHERE Last_Modified_Date > (
# MAGIC     SELECT LastLoadDateValue - INTERVAL 1 DAY
# MAGIC     FROM metadata.mastermetadata
# MAGIC     WHERE TableID = 'SL5'
# MAGIC )
# MAGIC   AND Is_Deleted = 0
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT
# MAGIC   DW_Load_ID,
# MAGIC   "1" AS Load_Counter,
# MAGIC   Load_Number AS Load_ID,
# MAGIC   NULL,       -- possibly NULL
# MAGIC   NULL,      -- possibly NULL
# MAGIC   NULL,               -- possibly NULL
# MAGIC   Actual_Delivered_Date,
# MAGIC   NULL,                 -- possibly NULL
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD,
# MAGIC   Carrier_Linehaul_USD,
# MAGIC   'Both' AS Office_Type, 
# MAGIC   'DBG' AS Office,
# MAGIC   NULL,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD,
# MAGIC   NULL,
# MAGIC   'Digital Brokerage Group',
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Customer_Fuel_Surcharge_USD,
# MAGIC   Customer_Linehaul_USD,
# MAGIC   Customer_Master,
# MAGIC   NULL,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Expense_USD,
# MAGIC   Financial_Period_Year,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Invoiced_Date,
# MAGIC   NULL,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   CONCAT(
# MAGIC 		  INITCAP(origin_city), ',', origin_state, '>', INITCAP(Destination_City), ',', Destination_State
# MAGIC 		) AS load_lane,
# MAGIC   Load_Status,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Margin_USD,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Miles,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   NULL,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD,
# MAGIC   Rolled_Date,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Ship_Date,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Week_Number
# MAGIC FROM Silver.silver_transfix
# MAGIC WHERE Last_Modified_Date > (
# MAGIC     SELECT LastLoadDateValue - INTERVAL 1 DAY
# MAGIC     FROM metadata.mastermetadata
# MAGIC     WHERE TableID = 'SL6'
# MAGIC )
# MAGIC   AND Is_Deleted = 0)
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Full Load View

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Gold.VW_Fact_Loads_Split_Full_Load AS
# MAGIC
# MAGIC SELECT *, MD5(
# MAGIC   COALESCE(DW_Load_ID, '') ||
# MAGIC   COALESCE(Load_Counter, '') ||
# MAGIC   COALESCE(Load_Number::STRING, '') ||
# MAGIC   COALESCE(Admin_Fees_Carrier::STRING, '') ||
# MAGIC   COALESCE(Admin_Fees_Customer::STRING, '') ||
# MAGIC   COALESCE(Billing_Status, '') ||
# MAGIC   COALESCE(Actual_Delivered_Date::STRING, '') ||
# MAGIC   COALESCE(Booking_Type, '') ||
# MAGIC   COALESCE(Booked_Date::STRING, '') ||
# MAGIC   COALESCE(Carrier_Fuel_Surcharge::STRING, '') ||
# MAGIC   COALESCE(Carrier_linehaul::STRING, '') ||
# MAGIC   COALESCE(Office_Type, '') ||
# MAGIC   COALESCE(Carrier_ID, '') ||
# MAGIC   COALESCE(Carrier_Name, '') ||
# MAGIC   COALESCE(Carrier_Other_Fees::STRING, '') ||
# MAGIC   COALESCE(Carrier_Rep_ID, '') ||
# MAGIC   COALESCE(Carrier_Rep, '') ||
# MAGIC   COALESCE(Combined_Load, '') ||
# MAGIC   COALESCE(Consignee_Delivery_Address, '') ||
# MAGIC   COALESCE(Consignee_Name, '') ||
# MAGIC   COALESCE(Consignee_Pickup_Address, '') ||
# MAGIC   COALESCE(Customer_Fuel_Surcharge::STRING, '') ||
# MAGIC   COALESCE(Customer_linehaul::STRING, '') ||
# MAGIC   COALESCE(Customer_Master, '') ||
# MAGIC   COALESCE(Customer_ID, '') ||
# MAGIC   COALESCE(Customer_Name, '') ||
# MAGIC   COALESCE(Customer_Other_Fees::STRING, '') ||
# MAGIC   COALESCE(Customer_Rep_ID, '') ||
# MAGIC   COALESCE(Customer_Rep, '') ||
# MAGIC   COALESCE(Days_Ahead, '') ||
# MAGIC   COALESCE(Bounced_Date::STRING, '') ||
# MAGIC   COALESCE(Cancelled_Date::STRING, '') ||
# MAGIC   COALESCE(Delivery_Pickup_Charges_Carrier::STRING, '') ||
# MAGIC   COALESCE(Delivery_Pickup_Charges_Customer::STRING, '') ||
# MAGIC   COALESCE(Delivery_Appointment_Date::STRING, '') ||
# MAGIC   COALESCE(Delivery_EndDate::STRING, '') ||
# MAGIC   COALESCE(Destination_City, '') ||
# MAGIC   COALESCE(Destination_State, '') ||
# MAGIC   COALESCE(Destination_Zip, '') ||
# MAGIC   COALESCE(DOT_Number, '') ||
# MAGIC   COALESCE(Delivery_StartDate::STRING, '') ||
# MAGIC   COALESCE(Equipment, '') ||
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Carrier::STRING, '') ||
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Customer::STRING, '') ||
# MAGIC   COALESCE(Expense::STRING, '') ||
# MAGIC   COALESCE(Financial_Period_Year, '') ||
# MAGIC   COALESCE(GreenScreen_Rate::STRING, '') ||
# MAGIC   COALESCE(Invoice_Amount::STRING, '') ||
# MAGIC   COALESCE(Invoiced_Date::STRING, '') ||
# MAGIC   COALESCE(Lawson_ID, '') ||
# MAGIC   COALESCE(Load_Currency, '') ||
# MAGIC   COALESCE(Load_Flag, '') ||
# MAGIC   COALESCE(Load_Lane, '') ||
# MAGIC   COALESCE(Load_Status, '') ||
# MAGIC   COALESCE(Load_Unload_Charges_Carrier::STRING, '') ||
# MAGIC   COALESCE(Load_Unload_Charges_Customer::STRING, '') ||
# MAGIC   COALESCE(Margin::STRING, '') ||
# MAGIC   COALESCE(Market_Buy_Rate::STRING, '') ||
# MAGIC   COALESCE(Market_Destination, '') ||
# MAGIC   COALESCE(Market_Lane, '') ||
# MAGIC   COALESCE(Market_Origin, '') ||
# MAGIC   COALESCE(MC_Number, '') ||
# MAGIC   COALESCE(Miles, '') ||
# MAGIC   COALESCE(Miscellaneous_Chargers_Carrier::STRING, '') ||
# MAGIC   COALESCE(Miscellaneous_Chargers_Customers::STRING, '') ||
# MAGIC   COALESCE(Mode, '') ||
# MAGIC   COALESCE(On_Time_Delivery, '') ||
# MAGIC   COALESCE(On_Time_Pickup, '') ||
# MAGIC   COALESCE(Origin_City, '') ||
# MAGIC   COALESCE(Origin_State, '') ||
# MAGIC   COALESCE(Origin_Zip, '') ||
# MAGIC   COALESCE(Permits_Compliance_Charges_Carrier::STRING, '') ||
# MAGIC   COALESCE(Permits_Compliance_Charges_Customer::STRING, '') ||
# MAGIC   COALESCE(Pickup_Appointment_Date::STRING, '') ||
# MAGIC   COALESCE(Actual_Pickup_Date::STRING, '') ||
# MAGIC   COALESCE(Pickup_EndDate::STRING, '') ||
# MAGIC   COALESCE(Pickup_StartDate::STRING, '') ||
# MAGIC   COALESCE(PO_Number, '') ||
# MAGIC   COALESCE(PreBookStatus, '') ||
# MAGIC   COALESCE(Revenue::STRING, '') ||
# MAGIC   COALESCE(Rolled_Date::STRING, '') ||
# MAGIC   COALESCE(Sales_Rep_ID, '') ||
# MAGIC   COALESCE(Sales_Rep, '') ||
# MAGIC   COALESCE(Ship_Date::STRING, '') ||
# MAGIC   COALESCE(Spot_Margin, '') ||
# MAGIC   COALESCE(Spot_Revenue, '') ||
# MAGIC   COALESCE(Tender_Source_Type, '') ||
# MAGIC   COALESCE(Tendered_Date::STRING, '') ||
# MAGIC   COALESCE(TMS_System, '') ||
# MAGIC   COALESCE(Trailer_Number, '') ||
# MAGIC   COALESCE(Transit_Routing_Chargers_Carrier::STRING, '') ||
# MAGIC   COALESCE(Transit_Routing_Chargers_Customer::STRING, '') ||
# MAGIC   COALESCE(Week_Number, '')
# MAGIC ) AS HASHKEY FROM
# MAGIC (SELECT DISTINCT
# MAGIC   CONCAT(DW_Load_ID, '-S') AS DW_load_ID,
# MAGIC   '0.5' AS Load_Counter,
# MAGIC   Load_number         AS Load_Number,
# MAGIC   Admin_Fees_Carrier_USD/2     AS Admin_Fees_Carrier,
# MAGIC   Admin_Fees_Customer_USD/2   AS Admin_Fees_Customer,
# MAGIC   Billing_Status,
# MAGIC   Actual_Delivered_Date,
# MAGIC   Booking_Type,
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD/2 AS Carrier_Fuel_Surcharge,
# MAGIC   Carrier_Linehaul_USD/2       AS Carrier_linehaul,
# MAGIC   "Shipper" AS Office_Type,
# MAGIC   Customer_New_Office          AS Office,
# MAGIC   Carrier_ID,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD/2      AS Carrier_Other_Fees,
# MAGIC   Carrier_Rep_ID,
# MAGIC   Carrier_Rep,
# MAGIC   Combined_Load,
# MAGIC   Consignee_Delivery_Address,
# MAGIC   Consignee_Name,
# MAGIC   Consignee_Pickup_Address,
# MAGIC   Customer_Fuel_Surcharge_USD/2  AS Customer_Fuel_Surcharge,
# MAGIC   Customer_Linehaul_USD/2     AS Customer_linehaul,
# MAGIC   Customer_Master,
# MAGIC   Customer_ID,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD/2     AS Customer_Other_Fees,
# MAGIC   Customer_Rep_ID,
# MAGIC   Customer_Rep,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   Delivery_Pickup_Charges_Carrier_USD/2  AS Delivery_Pickup_Charges_Carrier,
# MAGIC   Delivery_Pickup_Charges_Customer_USD/2 AS Delivery_Pickup_Charges_Customer,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   Equipment_Vehicle_Charges_Carrier_USD/2  AS Equipment_Vehicle_Charges_Carrier,
# MAGIC   Equipment_Vehicle_Charges_Customer_USD/2 AS Equipment_Vehicle_Charges_Customer,
# MAGIC   Expense_USD/2   AS Expense,
# MAGIC   Financial_Period_Year,
# MAGIC   GreenScreen_Rate_USD  AS GreenScreen_Rate,
# MAGIC   Invoice_Amount_USD/2    AS Invoice_Amount,
# MAGIC   Invoiced_Date,
# MAGIC   Lawson_ID,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   Load_Lane,
# MAGIC   Load_Status,
# MAGIC   Load_Unload_Charges_Carrier_USD/2   AS Load_Unload_Charges_Carrier,
# MAGIC   Load_Unload_Charges_Customer_USD/2  AS Load_Unload_Charges_Customer,
# MAGIC   Margin_USD/2       AS Margin,
# MAGIC   Market_Buy_Rate_USD   AS Market_Buy_Rate,
# MAGIC   Market_Destination,
# MAGIC   Market_Lane,
# MAGIC   Market_Origin,
# MAGIC   MC_Number,
# MAGIC   Miles,
# MAGIC   Miscellaneous_Chargers_Carrier_USD/2   AS Miscellaneous_Chargers_Carrier,
# MAGIC   Miscellaneous_Chargers_Customers_USD/2  AS Miscellaneous_Chargers_Customers,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   Permits_Compliance_Charges_Carrier_USD/2   AS Permits_Compliance_Charges_Carrier,
# MAGIC   Permits_Compliance_Charges_Customer_USD/2  AS Permits_Compliance_Charges_Customer,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   PO_Number,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD/2   AS Revenue,
# MAGIC   Rolled_Date,
# MAGIC   Sales_Rep_ID,
# MAGIC   Sales_Rep,
# MAGIC   Ship_Date,
# MAGIC   Spot_Margin,
# MAGIC   Spot_Revenue,
# MAGIC   Tender_Source_Type,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   Trailer_Number,
# MAGIC   Transit_Routing_Chargers_Carrier_USD/2   AS Transit_Routing_Chargers_Carrier,
# MAGIC   Transit_Routing_Chargers_Customer_USD/2  AS Transit_Routing_Chargers_Customer,
# MAGIC   Week_Number
# MAGIC
# MAGIC FROM Silver.silver_aljex
# MAGIC WHERE Last_Modified_Date > (
# MAGIC     SELECT LastLoadDateValue - INTERVAL 1 DAY
# MAGIC     FROM metadata.mastermetadata
# MAGIC     WHERE TableID = 'SL4'
# MAGIC )
# MAGIC   AND Is_Deleted = 0
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   CONCAT(DW_Load_ID, '-C') AS DW_load_ID,
# MAGIC   '0.5' AS Load_Counter,
# MAGIC   Load_number         AS Load_ID,
# MAGIC   Admin_Fees_Carrier_USD/2     AS Admin_Fees_Carrier,
# MAGIC   Admin_Fees_Customer_USD/2   AS Admin_Fees_Customer,
# MAGIC   Billing_Status,
# MAGIC   Actual_Delivered_Date,
# MAGIC   Booking_Type,
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD/2 AS Carrier_Fuel_Surcharge,
# MAGIC   Carrier_Linehaul_USD/2       AS Carrier_linehaul,
# MAGIC   "Carrier" AS Office_Type,
# MAGIC   Carrier_New_Office          AS Office,
# MAGIC   Carrier_ID,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD/2      AS Carrier_Other_Fees,
# MAGIC   Carrier_Rep_ID,
# MAGIC   Carrier_Rep,
# MAGIC   Combined_Load,
# MAGIC   Consignee_Delivery_Address,
# MAGIC   Consignee_Name,
# MAGIC   Consignee_Pickup_Address,
# MAGIC   Customer_Fuel_Surcharge_USD/2  AS Customer_Fuel_Surcharge,
# MAGIC   Customer_Linehaul_USD/2     AS Customer_linehaul,
# MAGIC   Customer_Master,
# MAGIC   Customer_ID,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD/2     AS Customer_Other_Fees,
# MAGIC   Customer_Rep_ID,
# MAGIC   Customer_Rep,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   Delivery_Pickup_Charges_Carrier_USD/2  AS Delivery_Pickup_Charges_Carrier,
# MAGIC   Delivery_Pickup_Charges_Customer_USD/2 AS Delivery_Pickup_Charges_Customer,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   Equipment_Vehicle_Charges_Carrier_USD/2  AS Equipment_Vehicle_Charges_Carrier,
# MAGIC   Equipment_Vehicle_Charges_Customer_USD/2 AS Equipment_Vehicle_Charges_Customer,
# MAGIC   Expense_USD/2   AS Expense,
# MAGIC   Financial_Period_Year,
# MAGIC   GreenScreen_Rate_USD  AS GreenScreen_Rate,
# MAGIC   Invoice_Amount_USD/2    AS Invoice_Amount,
# MAGIC   Invoiced_Date,
# MAGIC   Lawson_ID,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   Load_Lane,
# MAGIC   Load_Status,
# MAGIC   Load_Unload_Charges_Carrier_USD/2   AS Load_Unload_Charges_Carrier,
# MAGIC   Load_Unload_Charges_Customer_USD/2  AS Load_Unload_Charges_Customer,
# MAGIC   Margin_USD/2       AS Margin,
# MAGIC   Market_Buy_Rate_USD   AS Market_Buy_Rate,
# MAGIC   Market_Destination,
# MAGIC   Market_Lane,
# MAGIC   Market_Origin,
# MAGIC   MC_Number,
# MAGIC   Miles,
# MAGIC   Miscellaneous_Chargers_Carrier_USD/2   AS Miscellaneous_Chargers_Carrier,
# MAGIC   Miscellaneous_Chargers_Customers_USD/2  AS Miscellaneous_Chargers_Customers,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   Permits_Compliance_Charges_Carrier_USD/2   AS Permits_Compliance_Charges_Carrier,
# MAGIC   Permits_Compliance_Charges_Customer_USD/2  AS Permits_Compliance_Charges_Customer,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   PO_Number,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD/2   AS Revenue,
# MAGIC   Rolled_Date,
# MAGIC   Sales_Rep_ID,
# MAGIC   Sales_Rep,
# MAGIC   Ship_Date,
# MAGIC   Spot_Margin,
# MAGIC   Spot_Revenue,
# MAGIC   Tender_Source_Type,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   Trailer_Number,
# MAGIC   Transit_Routing_Chargers_Carrier_USD/2   AS Transit_Routing_Chargers_Carrier,
# MAGIC   Transit_Routing_Chargers_Customer_USD/2  AS Transit_Routing_Chargers_Customer,
# MAGIC   Week_Number
# MAGIC
# MAGIC FROM Silver.silver_aljex
# MAGIC WHERE Last_Modified_Date > (
# MAGIC     SELECT LastLoadDateValue - INTERVAL 1 DAY
# MAGIC     FROM metadata.mastermetadata
# MAGIC     WHERE TableID = 'SL4'
# MAGIC )
# MAGIC   AND Is_Deleted = 0
# MAGIC
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   CONCAT(DW_Load_ID, '-S'),
# MAGIC   "0.5" AS Load_Counter,
# MAGIC   Load_ID AS Load_ID,
# MAGIC   Admin_Fees_Carrier_USD/2,
# MAGIC   Admin_Fees_Customer_USD/2,
# MAGIC   Billing_Status,
# MAGIC   Actual_Delivered_Date,
# MAGIC   Booking_Type,
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD/2,
# MAGIC   Carrier_Linehaul_USD/2,
# MAGIC   "Shipper",
# MAGIC   Customer_New_Office AS Office_Type,
# MAGIC   Carrier_ID,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD/2,
# MAGIC   Carrier_Rep_ID,
# MAGIC   Carrier_Rep,
# MAGIC   Combined_Load,
# MAGIC   Consignee_Delivery_Address,
# MAGIC   Consignee_Name,
# MAGIC   Consignee_Pickup_Address,
# MAGIC   Customer_Fuel_Surcharge_USD/2,
# MAGIC   Customer_Linehaul_USD/2,
# MAGIC   Customer_Master,
# MAGIC   Customer_ID,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD/2,
# MAGIC   Customer_Rep_ID,
# MAGIC   Customer_Rep,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   Delivery_Pickup_Charges_Carrier_USD/2,
# MAGIC   Delivery_Pickup_Charges_Customer_USD/2,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   Equipment_Vehicle_Charges_Carrier_USD/2,
# MAGIC   Equipment_Vehicle_Charges_Customer_USD/2,
# MAGIC   Expense_USD/2,
# MAGIC   Financial_Period_Year,
# MAGIC   GreenScreen_Rate_USD,
# MAGIC   Invoice_Amount_USD/2,
# MAGIC   Invoiced_Date,
# MAGIC   Lawson_ID,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   Load_Lane,
# MAGIC   Load_Status,
# MAGIC   Load_Unload_Charges_Carrier_USD/2,
# MAGIC   Load_Unload_Charges_Customer_USD/2,
# MAGIC   Margin_USD/2,
# MAGIC   Market_Buy_Rate_USD,
# MAGIC   Market_Destination,
# MAGIC   Market_Lane,
# MAGIC   Market_Origin,
# MAGIC   MC_Number,
# MAGIC   Miles,
# MAGIC   Miscellaneous_Chargers_Carrier_USD/2,
# MAGIC   Miscellaneous_Chargers_Customers_USD/2,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   Permits_Compliance_Charges_Carrier_USD/2,
# MAGIC   Permits_Compliance_Charges_Customer_USD/2,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   PO_Number,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD/2,
# MAGIC   Rolled_Date,
# MAGIC   Sales_Rep_ID,
# MAGIC   Sales_Rep,
# MAGIC   Ship_Date,
# MAGIC   Spot_Margin,
# MAGIC   Spot_Revenue,
# MAGIC   Tender_Source_Type,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   Trailer_Number,
# MAGIC   Transit_Routing_Chargers_Carrier_USD/2,
# MAGIC   Transit_Routing_Chargers_Customer_USD/2,
# MAGIC   Week_Number
# MAGIC FROM Silver.silver_relay
# MAGIC WHERE Is_Deleted = 0
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   CONCAT(DW_Load_ID, '-C'),
# MAGIC   "0.5" AS Load_Counter,
# MAGIC   Load_ID AS Load_ID,
# MAGIC   Admin_Fees_Carrier_USD/2,
# MAGIC   Admin_Fees_Customer_USD/2,
# MAGIC   Billing_Status,
# MAGIC   Actual_Delivered_Date,
# MAGIC   Booking_Type,
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD/2,
# MAGIC   Carrier_Linehaul_USD/2,
# MAGIC   "Carrier",
# MAGIC   Carrier_New_Office AS Office_Type,
# MAGIC   Carrier_ID,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD/2,
# MAGIC   Carrier_Rep_ID,
# MAGIC   Carrier_Rep,
# MAGIC   Combined_Load,
# MAGIC   Consignee_Delivery_Address,
# MAGIC   Consignee_Name,
# MAGIC   Consignee_Pickup_Address,
# MAGIC   Customer_Fuel_Surcharge_USD/2,
# MAGIC   Customer_Linehaul_USD/2,
# MAGIC   Customer_Master,
# MAGIC   Customer_ID,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD/2,
# MAGIC   Customer_Rep_ID,
# MAGIC   Customer_Rep,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   Delivery_Pickup_Charges_Carrier_USD/2,
# MAGIC   Delivery_Pickup_Charges_Customer_USD/2,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   Equipment_Vehicle_Charges_Carrier_USD/2,
# MAGIC   Equipment_Vehicle_Charges_Customer_USD/2,
# MAGIC   Expense_USD/2,
# MAGIC   Financial_Period_Year,
# MAGIC   GreenScreen_Rate_USD,
# MAGIC   Invoice_Amount_USD/2,
# MAGIC   Invoiced_Date,
# MAGIC   Lawson_ID,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   Load_Lane,
# MAGIC   Load_Status,
# MAGIC   Load_Unload_Charges_Carrier_USD/2,
# MAGIC   Load_Unload_Charges_Customer_USD/2,
# MAGIC   Margin_USD/2,
# MAGIC   Market_Buy_Rate_USD,
# MAGIC   Market_Destination,
# MAGIC   Market_Lane,
# MAGIC   Market_Origin,
# MAGIC   MC_Number,
# MAGIC   Miles,
# MAGIC   Miscellaneous_Chargers_Carrier_USD/2,
# MAGIC   Miscellaneous_Chargers_Customers_USD/2,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   Permits_Compliance_Charges_Carrier_USD/2,
# MAGIC   Permits_Compliance_Charges_Customer_USD/2,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   PO_Number,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD/2,
# MAGIC   Rolled_Date,
# MAGIC   Sales_Rep_ID,
# MAGIC   Sales_Rep,
# MAGIC   Ship_Date,
# MAGIC   Spot_Margin,
# MAGIC   Spot_Revenue,
# MAGIC   Tender_Source_Type,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   Trailer_Number,
# MAGIC   Transit_Routing_Chargers_Carrier_USD/2,
# MAGIC   Transit_Routing_Chargers_Customer_USD/2,
# MAGIC   Week_Number
# MAGIC FROM Silver.silver_relay
# MAGIC WHERE Is_Deleted = 0
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT
# MAGIC   DW_Load_ID,
# MAGIC   "1" AS Load_Counter,
# MAGIC   Load_Number AS Load_ID,
# MAGIC   NULL,       -- possibly NULL
# MAGIC   NULL,      -- possibly NULL
# MAGIC   NULL,               -- possibly NULL
# MAGIC   Actual_Delivered_Date,
# MAGIC   NULL,                 -- possibly NULL
# MAGIC   Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_USD,
# MAGIC   Carrier_Linehaul_USD,
# MAGIC   'Both' AS Office_Type, 
# MAGIC   'DBG' AS Office,
# MAGIC   NULL,
# MAGIC   Carrier_Name,
# MAGIC   Carrier_Other_Fees_USD,
# MAGIC   NULL,
# MAGIC   'Digital Brokerage Group',
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Customer_Fuel_Surcharge_USD,
# MAGIC   Customer_Linehaul_USD,
# MAGIC   Customer_Master,
# MAGIC   NULL,
# MAGIC   Customer_Name,
# MAGIC   Customer_Other_Fees_USD,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Days_Ahead,
# MAGIC   Bounced_Date,
# MAGIC   Cancelled_Date,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Delivery_Appointment_Date,
# MAGIC   Delivery_EndDate,
# MAGIC   Destination_City,
# MAGIC   Destination_State,
# MAGIC   Destination_Zip,
# MAGIC   DOT_Number,
# MAGIC   Delivery_StartDate,
# MAGIC   Equipment,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Expense_USD,
# MAGIC   Financial_Period_Year,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Invoiced_Date,
# MAGIC   NULL,
# MAGIC   Load_Currency,
# MAGIC   Load_Flag,
# MAGIC   CONCAT(
# MAGIC 		  INITCAP(origin_city), ',', origin_state, '>', INITCAP(Destination_City), ',', Destination_State
# MAGIC 		) AS load_lane,
# MAGIC   Load_Status,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Margin_USD,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Miles,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Mode,
# MAGIC   On_Time_Delivery,
# MAGIC   On_Time_Pickup,
# MAGIC   Origin_City,
# MAGIC   Origin_State,
# MAGIC   Origin_Zip,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Pickup_Appointment_Date,
# MAGIC   Actual_Pickup_Date,
# MAGIC   Pickup_EndDate,
# MAGIC   Pickup_StartDate,
# MAGIC   NULL,
# MAGIC   PreBookStatus,
# MAGIC   Revenue_USD,
# MAGIC   Rolled_Date,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Ship_Date,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Tendered_Date,
# MAGIC   TMS_System,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   Week_Number
# MAGIC FROM Silver.silver_transfix
# MAGIC WHERE Is_Deleted = 0)
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC #### DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE TABLE Gold.Fact_Loads_Split (
# MAGIC     DW_Load_ID VARCHAR(255) NOT NULL PRIMARY KEY,
# MAGIC     Load_Counter VARCHAR(255),
# MAGIC     Load_Number BIGINT,
# MAGIC     Admin_Fees_Carrier DECIMAL(10,2),
# MAGIC     Admin_Fees_Customer DECIMAL(10,2),
# MAGIC     Billing_Status VARCHAR(255),
# MAGIC     Actual_Delivered_Date TIMESTAMP_NTZ,
# MAGIC     Booking_Type VARCHAR(255),
# MAGIC     Booked_Date TIMESTAMP_NTZ,
# MAGIC     Carrier_Fuel_Surcharge DECIMAL(10,2),
# MAGIC     Carrier_linehaul DECIMAL(10,2),
# MAGIC     Office_Type VARCHAR(255),
# MAGIC     Office VARCHAR(255),
# MAGIC     Carrier_ID VARCHAR(255),
# MAGIC     Carrier_Name VARCHAR(255),
# MAGIC     Carrier_Other_Fees DECIMAL(10,2),
# MAGIC     Carrier_Rep_ID VARCHAR(255),
# MAGIC     Carrier_Rep VARCHAR(255),
# MAGIC     Combined_Load VARCHAR(255),
# MAGIC     Consignee_Delivery_Address VARCHAR(255),
# MAGIC     Consignee_Name VARCHAR(255),
# MAGIC     Consignee_Pickup_Address VARCHAR(255),
# MAGIC     Customer_Fuel_Surcharge DECIMAL(10,2),
# MAGIC     Customer_linehaul DECIMAL(10,2),
# MAGIC     Customer_Master VARCHAR(255),
# MAGIC     Customer_ID VARCHAR(255),
# MAGIC     Customer_Name VARCHAR(255),
# MAGIC     Customer_Other_Fees DECIMAL(10,2),
# MAGIC     Customer_Rep_ID VARCHAR(255),
# MAGIC     Customer_Rep VARCHAR(255),
# MAGIC     Days_Ahead VARCHAR(255),
# MAGIC     Bounced_Date TIMESTAMP_NTZ,
# MAGIC     Cancelled_Date TIMESTAMP_NTZ,
# MAGIC     Delivery_Pickup_Charges_Carrier DECIMAL(10,2),
# MAGIC     Delivery_Pickup_Charges_Customer DECIMAL(10,2),
# MAGIC     Delivery_Appointment_Date TIMESTAMP_NTZ,
# MAGIC     Delivery_EndDate TIMESTAMP_NTZ,
# MAGIC     Destination_City VARCHAR(255),
# MAGIC     Destination_State VARCHAR(255),
# MAGIC     Destination_Zip VARCHAR(255),
# MAGIC     DOT_Number VARCHAR(255),
# MAGIC     Delivery_StartDate TIMESTAMP_NTZ,
# MAGIC     Equipment VARCHAR(255),
# MAGIC     Equipment_Vehicle_Charges_Carrier DECIMAL(10,2),
# MAGIC     Equipment_Vehicle_Charges_Customer DECIMAL(10,2),
# MAGIC     Expense DECIMAL(10,2),
# MAGIC     Financial_Period_Year VARCHAR(255),
# MAGIC     GreenScreen_Rate DECIMAL(10,2),
# MAGIC     Invoice_Amount DECIMAL(10,2),
# MAGIC     Invoiced_Date TIMESTAMP_NTZ,
# MAGIC     Lawson_ID VARCHAR(50),
# MAGIC     Load_Currency VARCHAR(255),
# MAGIC     Load_Flag VARCHAR(255),
# MAGIC     Load_Lane VARCHAR(255),
# MAGIC     Load_Status VARCHAR(255),
# MAGIC     Load_Unload_Charges_Carrier DECIMAL(10,2),
# MAGIC     Load_Unload_Charges_Customer DECIMAL(10,2),
# MAGIC     Margin DECIMAL(10,2),
# MAGIC     Market_Buy_Rate DECIMAL(10,2),
# MAGIC     Market_Destination VARCHAR(255),
# MAGIC     Market_Lane VARCHAR(255),
# MAGIC     Market_Origin VARCHAR(255),
# MAGIC     MC_Number VARCHAR(255),
# MAGIC     Miles VARCHAR(255),
# MAGIC     Miscellaneous_Chargers_Carrier DECIMAL(10,2),
# MAGIC     Miscellaneous_Chargers_Customers DECIMAL(10,2),
# MAGIC     Mode VARCHAR(255),
# MAGIC     On_Time_Delivery VARCHAR(255),
# MAGIC     On_Time_Pickup VARCHAR(255),
# MAGIC     Origin_City VARCHAR(255),
# MAGIC     Origin_State VARCHAR(255),
# MAGIC     Origin_Zip VARCHAR(255),
# MAGIC     Permits_Compliance_Charges_Carrier DECIMAL(10,2),
# MAGIC     Permits_Compliance_Charges_Customer DECIMAL(10,2),
# MAGIC     Pickup_Appointment_Date TIMESTAMP_NTZ,
# MAGIC     Actual_Pickup_Date TIMESTAMP_NTZ,
# MAGIC     Pickup_EndDate TIMESTAMP_NTZ,
# MAGIC     Pickup_StartDate TIMESTAMP_NTZ,
# MAGIC     PO_Number VARCHAR(255),
# MAGIC     PreBookStatus VARCHAR(255),
# MAGIC     Revenue DECIMAL(10,2),
# MAGIC     Rolled_Date TIMESTAMP_NTZ,
# MAGIC     Sales_Rep_ID VARCHAR(255),
# MAGIC     Sales_Rep VARCHAR(255),
# MAGIC     Ship_Date TIMESTAMP_NTZ,
# MAGIC     Spot_Margin VARCHAR(255),
# MAGIC     Spot_Revenue VARCHAR(255),
# MAGIC     Tender_Source_Type VARCHAR(255),
# MAGIC     Tendered_Date TIMESTAMP_NTZ,
# MAGIC     TMS_System VARCHAR(50),
# MAGIC     Trailer_Number VARCHAR(255),
# MAGIC     Transit_Routing_Chargers_Carrier DECIMAL(10,2),
# MAGIC     Transit_Routing_Chargers_Customer DECIMAL(10,2),
# MAGIC     Week_Number VARCHAR(10),
# MAGIC     Hashkey VARCHAR(255),
# MAGIC     Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Created_By VARCHAR(50) NOT NULL,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Last_Modified_By VARCHAR(50) NOT NULL,
# MAGIC     Is_Deleted SMALLINT NOT NULL
# MAGIC );
# MAGIC