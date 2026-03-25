# Databricks notebook source
# MAGIC %md
# MAGIC ### Relay
# MAGIC * **Description:** Full Extract the data from Bronze to Silver as delta file
# MAGIC * **Created Date:** 03/07/2025
# MAGIC * **Created By:** Uday
# MAGIC * **Modified Date:** 06/10/2025
# MAGIC * **Modified By:** Uday
# MAGIC * **Changes Made:** Logic Update

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update Pipeline Start Time

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata 
# MAGIC SET PipelineStartTime = current_timestamp()
# MAGIC WHERE TableID = 'SL5'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Data to Target Table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO Silver.Silver_Relay AS target
# MAGIC USING Silver.VW_Silver_Relay_Full_Load AS source
# MAGIC ON target.DW_Load_ID = source.DW_Load_ID
# MAGIC WHEN MATCHED AND target.Hashkey <> source.Hashkey THEN
# MAGIC   UPDATE SET
# MAGIC     target.Admin_Fees_Carrier_USD = source.Admin_Fees_Carrier_USD,
# MAGIC     target.Admin_Fees_Carrier_CAD = source.Admin_Fees_Carrier_CAD,
# MAGIC     target.Admin_Fees_Customer_USD = source.Admin_Fees_Customer_USD,
# MAGIC     target.Admin_Fees_Customer_CAD = source.Admin_Fees_Customer_CAD,
# MAGIC     target.Billing_Status = source.Billing_Status,
# MAGIC     target.Actual_Delivered_Date = source.Actual_Delivered_Date,
# MAGIC     target.Booking_Type = source.Booking_Type,
# MAGIC     target.Booked_Date = source.Booked_Date,
# MAGIC     target.Carrier_Fuel_Surcharge_CAD = source.Carrier_Fuel_Surcharge_CAD,
# MAGIC     target.Carrier_Fuel_Surcharge_USD = source.Carrier_Fuel_Surcharge_USD,
# MAGIC     target.Carrier_linehaul_CAD = source.Carrier_linehaul_CAD,
# MAGIC     target.Carrier_linehaul_USD = source.Carrier_linehaul_USD,
# MAGIC     target.Carrier_ID = source.Carrier_ID,
# MAGIC     target.Carrier_Name = source.Carrier_Name,
# MAGIC     target.Carrier_New_Office = source.Carrier_New_Office,
# MAGIC     target.Carrier_Old_Office = source.Carrier_Old_Office,
# MAGIC     target.Carrier_Other_Fees_CAD = source.Carrier_Other_Fees_CAD,
# MAGIC     target.Carrier_Other_Fees_USD = source.Carrier_Other_Fees_USD,
# MAGIC     target.Carrier_Rep_ID = source.Carrier_Rep_ID,
# MAGIC     target.Carrier_Rep = source.Carrier_Rep,
# MAGIC     target.Combined_Load = source.Combined_Load,
# MAGIC     target.Consignee_Delivery_Address = source.Consignee_Delivery_Address,
# MAGIC     target.Consignee_Name = source.Consignee_Name,
# MAGIC     target.Consignee_Pickup_Address = source.Consignee_Pickup_Address,
# MAGIC     target.Customer_Fuel_Surcharge_USD = source.Customer_Fuel_Surcharge_USD,
# MAGIC     target.Customer_Fule_Surcharge_CAD = source.Customer_Fule_Surcharge_CAD,
# MAGIC     target.Customer_linehaul_CAD = source.Customer_linehaul_CAD,
# MAGIC     target.Customer_linehaul_USD = source.Customer_linehaul_USD,
# MAGIC     target.Customer_ID = source.Customer_ID,
# MAGIC     target.Customer_Master = source.Customer_Master,
# MAGIC     target.Customer_Name = source.Customer_Name,
# MAGIC     target.Customer_New_Office = source.Customer_New_Office,
# MAGIC     target.Customer_Old_Office = source.Customer_Old_Office,
# MAGIC     target.Customer_Other_Fees_CAD = source.Customer_Other_Fees_CAD,
# MAGIC     target.Customer_Other_Fees_USD = source.Customer_Other_Fees_USD,
# MAGIC     target.Customer_Rep_ID = source.Customer_Rep_ID,
# MAGIC     target.Customer_Rep = source.Customer_Rep,
# MAGIC     target.Days_Ahead = source.Days_Ahead,
# MAGIC     target.Bounced_Date = source.Bounced_Date,
# MAGIC     target.Cancelled_Date = source.Cancelled_Date,
# MAGIC     target.Delivery_Pickup_Charges_Carrier_CAD = source.Delivery_Pickup_Charges_Carrier_CAD,
# MAGIC     target.Delivery_Pickup_Charges_Carrier_USD = source.Delivery_Pickup_Charges_Carrier_USD,
# MAGIC     target.Delivery_Pickup_Charges_Customer_CAD = source.Delivery_Pickup_Charges_Customer_CAD,
# MAGIC     target.Delivery_Pickup_Charges_Customer_USD = source.Delivery_Pickup_Charges_Customer_USD,
# MAGIC     target.Delivery_Appointment_Date = source.Delivery_Appointment_Date,
# MAGIC     target.Delivery_EndDate = source.Delivery_EndDate,
# MAGIC     target.Destination_City = source.Destination_City,
# MAGIC     target.Destination_State = source.Destination_State,
# MAGIC     target.Destination_Zip = source.Destination_Zip,
# MAGIC     target.DOT_Number = source.DOT_Number,
# MAGIC     target.Delivery_StartDate = source.Delivery_StartDate,
# MAGIC     target.Equipment = source.Equipment,
# MAGIC     target.Equipment_Vehicle_Charges_Carrier_CAD = source.Equipment_Vehicle_Charges_Carrier_CAD,
# MAGIC     target.Equipment_Vehicle_Charges_Carrier_USD = source.Equipment_Vehicle_Charges_Carrier_USD,
# MAGIC     target.Equipment_Vehicle_Charges_Customer_CAD = source.Equipment_Vehicle_Charges_Customer_CAD,
# MAGIC     target.Equipment_Vehicle_Charges_Customer_USD = source.Equipment_Vehicle_Charges_Customer_USD,
# MAGIC     target.Expense_CAD = source.Expense_CAD,
# MAGIC     target.Expense_USD = source.Expense_USD,
# MAGIC     target.Financial_Period_Year = source.Financial_Period_Year,
# MAGIC     target.GreenScreen_Rate_CAD = source.GreenScreen_Rate_CAD,
# MAGIC     target.GreenScreen_Rate_USD = source.GreenScreen_Rate_USD,
# MAGIC     target.Invoice_Amount_CAD = source.Invoice_Amount_CAD,
# MAGIC     target.Invoice_Amount_USD = source.Invoice_Amount_USD,
# MAGIC     target.Invoiced_Date = source.Invoiced_Date,
# MAGIC     target.Lawson_ID = source.Lawson_ID,
# MAGIC     target.Load_Currency = source.Load_Currency,
# MAGIC     target.Load_Flag = source.Load_Flag,
# MAGIC     target.Load_Lane = source.Load_Lane,
# MAGIC     target.Load_Status = source.Load_Status,
# MAGIC     target.Load_Unload_Charges_Carrier_CAD = source.Load_Unload_Charges_Carrier_CAD,
# MAGIC     target.Load_Unload_Charges_Carrier_USD = source.Load_Unload_Charges_Carrier_USD,
# MAGIC     target.Load_Unload_Charges_Customer_CAD = source.Load_Unload_Charges_Customer_CAD,
# MAGIC     target.Load_Unload_Charges_Customer_USD = source.Load_Unload_Charges_Customer_USD,
# MAGIC     target.Margin_CAD = source.Margin_CAD,
# MAGIC     target.Margin_USD = source.Margin_USD,
# MAGIC     target.Market_Buy_Rate_CAD = source.Market_Buy_Rate_CAD,
# MAGIC     target.Market_Buy_Rate_USD = source.Market_Buy_Rate_USD,
# MAGIC     target.Market_Destination = source.Market_Destination,
# MAGIC     target.Market_Lane = source.Market_Lane,
# MAGIC     target.Market_Origin = source.Market_Origin,
# MAGIC     target.MC_Number = source.MC_Number,
# MAGIC     target.Miles = source.Miles,
# MAGIC     target.Miscellaneous_Chargers_Carrier_CAD = source.Miscellaneous_Chargers_Carrier_CAD,
# MAGIC     target.Miscellaneous_Chargers_Carrier_USD = source.Miscellaneous_Chargers_Carrier_USD,
# MAGIC     target.Miscellaneous_Chargers_Customers_CAD = source.Miscellaneous_Chargers_Customers_CAD,
# MAGIC     target.Miscellaneous_Chargers_Customers_USD = source.Miscellaneous_Chargers_Customers_USD,
# MAGIC     target.Mode = source.Mode,
# MAGIC     target.On_Time_Delivery = source.On_Time_Delivery,
# MAGIC     target.On_Time_Pickup = source.On_Time_Pickup,
# MAGIC     target.Origin_City = source.Origin_City,
# MAGIC     target.Origin_State = source.Origin_State,
# MAGIC     target.Origin_Zip = source.Origin_Zip,
# MAGIC     target.Permits_Compliance_Charges_Carrier_CAD = source.Permits_Compliance_Charges_Carrier_CAD,
# MAGIC     target.Permits_Compliance_Charges_Carrier_USD = source.Permits_Compliance_Charges_Carrier_USD,
# MAGIC     target.Permits_Compliance_Charges_Customer_CAD = source.Permits_Compliance_Charges_Customer_CAD,
# MAGIC     target.Permits_Compliance_Charges_Customer_USD = source.Permits_Compliance_Charges_Customer_USD,
# MAGIC     target.Pickup_Appointment_Date = source.Pickup_Appointment_Date,
# MAGIC     target.Actual_Pickup_Date = source.Actual_Pickup_Date,
# MAGIC     target.Pickup_EndDate = source.Pickup_EndDate,
# MAGIC     target.Pickup_StartDate = source.Pickup_StartDate,
# MAGIC     target.PO_Number = source.PO_Number,
# MAGIC     target.PreBookStatus = source.PreBookStatus,
# MAGIC     target.Revenue_CAD = source.Revenue_CAD,
# MAGIC     target.Revenue_USD = source.Revenue_USD,
# MAGIC     target.Rolled_Date = source.Rolled_Date,
# MAGIC     target.Sales_Rep_ID = source.Sales_Rep_ID,
# MAGIC     target.Sales_Rep = source.Sales_Rep,
# MAGIC     target.Ship_Date = source.Ship_Date,
# MAGIC     target.Spot_Margin = source.Spot_Margin,
# MAGIC     target.Spot_Revenue = source.Spot_Revenue,
# MAGIC     target.Tender_Source_Type = source.Tender_Source_Type,
# MAGIC     target.Tendered_Date = source.Tendered_Date,
# MAGIC     target.TMS_System = source.TMS_System,
# MAGIC     target.Trailer_Number = source.Trailer_Number,
# MAGIC     target.Transit_Routing_Chargers_Carrier_CAD = source.Transit_Routing_Chargers_Carrier_CAD,
# MAGIC     target.Transit_Routing_Chargers_Carrier_USD = source.Transit_Routing_Chargers_Carrier_USD,
# MAGIC     target.Transit_Routing_Chargers_Customer_CAD = source.Transit_Routing_Chargers_Customer_CAD,
# MAGIC     target.Transit_Routing_Chargers_Customer_USD = source.Transit_Routing_Chargers_Customer_USD,
# MAGIC     target.Week_Number = source.Week_Number,
# MAGIC     target.Carrier_Final_Rate_USD = source.Carrier_Final_Rate_USD,
# MAGIC     target.Carrier_Final_Rate_CAD = source.Carrier_Final_Rate_CAD,
# MAGIC     target.Last_Modified_Date = current_timestamp(),
# MAGIC     target.Last_Modified_By = 'Databricks',
# MAGIC     target.Is_Deleted = 0
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     DW_Load_ID, Load_ID, Admin_Fees_Carrier_USD, Admin_Fees_Carrier_CAD, Admin_Fees_Customer_USD,
# MAGIC     Admin_Fees_Customer_CAD, Billing_Status, Actual_Delivered_Date, Booking_Type, Booked_Date,
# MAGIC     Carrier_Fuel_Surcharge_CAD, Carrier_Fuel_Surcharge_USD, Carrier_linehaul_CAD, Carrier_linehaul_USD,
# MAGIC     Carrier_ID, Carrier_Name, Carrier_New_Office, Carrier_Old_Office, Carrier_Other_Fees_CAD,
# MAGIC     Carrier_Other_Fees_USD, Carrier_Rep_ID, Carrier_Rep, Combined_Load, Consignee_Delivery_Address,
# MAGIC     Consignee_Name, Consignee_Pickup_Address, Customer_Fuel_Surcharge_USD, Customer_Fule_Surcharge_CAD,
# MAGIC     Customer_linehaul_CAD, Customer_linehaul_USD, Customer_ID, Customer_Master, Customer_Name,
# MAGIC     Customer_New_Office, Customer_Old_Office, Customer_Other_Fees_CAD, Customer_Other_Fees_USD,
# MAGIC     Customer_Rep_ID, Customer_Rep, Days_Ahead, Bounced_Date, Cancelled_Date,
# MAGIC     Delivery_Pickup_Charges_Carrier_CAD, Delivery_Pickup_Charges_Carrier_USD, Delivery_Pickup_Charges_Customer_CAD,
# MAGIC     Delivery_Pickup_Charges_Customer_USD, Delivery_Appointment_Date, Delivery_EndDate, Destination_City,
# MAGIC     Destination_State, Destination_Zip, DOT_Number, Delivery_StartDate, Equipment,
# MAGIC     Equipment_Vehicle_Charges_Carrier_CAD, Equipment_Vehicle_Charges_Carrier_USD, Equipment_Vehicle_Charges_Customer_CAD,
# MAGIC     Equipment_Vehicle_Charges_Customer_USD, Expense_CAD, Expense_USD, Financial_Period_Year,
# MAGIC     GreenScreen_Rate_CAD, GreenScreen_Rate_USD, Invoice_Amount_CAD, Invoice_Amount_USD,
# MAGIC     Invoiced_Date, Lawson_ID, Load_Currency, Load_Flag, Load_Lane,
# MAGIC     Load_Status, Load_Unload_Charges_Carrier_CAD, Load_Unload_Charges_Carrier_USD, Load_Unload_Charges_Customer_CAD,
# MAGIC     Load_Unload_Charges_Customer_USD, Margin_CAD, Margin_USD, Market_Buy_Rate_CAD, Market_Buy_Rate_USD,
# MAGIC     Market_Destination, Market_Lane, Market_Origin, MC_Number, Miles,
# MAGIC     Miscellaneous_Chargers_Carrier_CAD, Miscellaneous_Chargers_Carrier_USD, Miscellaneous_Chargers_Customers_CAD,
# MAGIC     Miscellaneous_Chargers_Customers_USD, Mode, On_Time_Delivery, On_Time_Pickup, Origin_City,
# MAGIC     Origin_State, Origin_Zip, Permits_Compliance_Charges_Carrier_CAD, Permits_Compliance_Charges_Carrier_USD,
# MAGIC     Permits_Compliance_Charges_Customer_CAD, Permits_Compliance_Charges_Customer_USD, Pickup_Appointment_Date,
# MAGIC     Actual_Pickup_Date, Pickup_EndDate, Pickup_StartDate, PO_Number, PreBookStatus,
# MAGIC     Revenue_CAD, Revenue_USD, Rolled_Date, Sales_Rep_ID, Sales_Rep,
# MAGIC     Ship_Date, Spot_Margin, Spot_Revenue, Tender_Source_Type, Tendered_Date,
# MAGIC     TMS_System, Trailer_Number, Transit_Routing_Chargers_Carrier_CAD, Transit_Routing_Chargers_Carrier_USD,
# MAGIC     Transit_Routing_Chargers_Customer_CAD, Transit_Routing_Chargers_Customer_USD, Week_Number,
# MAGIC     Carrier_Final_Rate_USD, Carrier_Final_Rate_CAD, Hashkey, Created_Date,
# MAGIC     Created_By, Last_Modified_Date, Last_Modified_By, Is_Deleted
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.DW_Load_ID, source.Load_ID, source.Admin_Fees_Carrier_USD, source.Admin_Fees_Carrier_CAD, source.Admin_Fees_Customer_USD,
# MAGIC     source.Admin_Fees_Customer_CAD, source.Billing_Status, source.Actual_Delivered_Date, source.Booking_Type, source.Booked_Date,
# MAGIC     source.Carrier_Fuel_Surcharge_CAD, source.Carrier_Fuel_Surcharge_USD, source.Carrier_linehaul_CAD, source.Carrier_linehaul_USD,
# MAGIC     source.Carrier_ID, source.Carrier_Name, source.Carrier_New_Office, source.Carrier_Old_Office, source.Carrier_Other_Fees_CAD,
# MAGIC     source.Carrier_Other_Fees_USD, source.Carrier_Rep_ID, source.Carrier_Rep, source.Combined_Load, source.Consignee_Delivery_Address,
# MAGIC     source.Consignee_Name, source.Consignee_Pickup_Address, source.Customer_Fuel_Surcharge_USD, source.Customer_Fule_Surcharge_CAD,
# MAGIC     source.Customer_linehaul_CAD, source.Customer_linehaul_USD, source.Customer_ID, source.Customer_Master, source.Customer_Name,
# MAGIC     source.Customer_New_Office, source.Customer_Old_Office, source.Customer_Other_Fees_CAD, source.Customer_Other_Fees_USD,
# MAGIC     source.Customer_Rep_ID, source.Customer_Rep, source.Days_Ahead, source.Bounced_Date, source.Cancelled_Date,
# MAGIC     source.Delivery_Pickup_Charges_Carrier_CAD, source.Delivery_Pickup_Charges_Carrier_USD, source.Delivery_Pickup_Charges_Customer_CAD,
# MAGIC     source.Delivery_Pickup_Charges_Customer_USD, source.Delivery_Appointment_Date, source.Delivery_EndDate, source.Destination_City,
# MAGIC     source.Destination_State, source.Destination_Zip, source.DOT_Number, source.Delivery_StartDate, source.Equipment,
# MAGIC     source.Equipment_Vehicle_Charges_Carrier_CAD, source.Equipment_Vehicle_Charges_Carrier_USD, source.Equipment_Vehicle_Charges_Customer_CAD,
# MAGIC     source.Equipment_Vehicle_Charges_Customer_USD, source.Expense_CAD, source.Expense_USD, source.Financial_Period_Year,
# MAGIC     source.GreenScreen_Rate_CAD, source.GreenScreen_Rate_USD, source.Invoice_Amount_CAD, source.Invoice_Amount_USD,
# MAGIC     source.Invoiced_Date, source.Lawson_ID, source.Load_Currency, source.Load_Flag, source.Load_Lane,
# MAGIC     source.Load_Status, source.Load_Unload_Charges_Carrier_CAD, source.Load_Unload_Charges_Carrier_USD, source.Load_Unload_Charges_Customer_CAD,
# MAGIC     source.Load_Unload_Charges_Customer_USD, source.Margin_CAD, source.Margin_USD, source.Market_Buy_Rate_CAD, source.Market_Buy_Rate_USD,
# MAGIC     source.Market_Destination, source.Market_Lane, source.Market_Origin, source.MC_Number, source.Miles,
# MAGIC     source.Miscellaneous_Chargers_Carrier_CAD, source.Miscellaneous_Chargers_Carrier_USD, source.Miscellaneous_Chargers_Customers_CAD,
# MAGIC     source.Miscellaneous_Chargers_Customers_USD, source.Mode, source.On_Time_Delivery, source.On_Time_Pickup, source.Origin_City,
# MAGIC     source.Origin_State, source.Origin_Zip, source.Permits_Compliance_Charges_Carrier_CAD, source.Permits_Compliance_Charges_Carrier_USD,
# MAGIC     source.Permits_Compliance_Charges_Customer_CAD, source.Permits_Compliance_Charges_Customer_USD, source.Pickup_Appointment_Date,
# MAGIC     source.Actual_Pickup_Date, source.Pickup_EndDate, source.Pickup_StartDate, source.PO_Number, source.PreBookStatus,
# MAGIC     source.Revenue_CAD, source.Revenue_USD, source.Rolled_Date, source.Sales_Rep_ID, source.Sales_Rep,
# MAGIC     source.Ship_Date, source.Spot_Margin, source.Spot_Revenue, source.Tender_Source_Type, source.Tendered_Date,
# MAGIC     source.TMS_System, source.Trailer_Number, source.Transit_Routing_Chargers_Carrier_CAD, source.Transit_Routing_Chargers_Carrier_USD,
# MAGIC     source.Transit_Routing_Chargers_Customer_CAD, source.Transit_Routing_Chargers_Customer_USD, source.Week_Number,
# MAGIC     source.Carrier_Final_Rate_USD, source.Carrier_Final_Rate_CAD, source.Hashkey, current_timestamp(),
# MAGIC     'Databricks', current_timestamp(), 'Databricks', 0
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update Deleted Flag

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO Silver.silver_relay AS target
# MAGIC USING Silver.VW_Silver_Relay_Full_Load AS source
# MAGIC ON target.Dw_Load_ID = source.Dw_Load_ID AND target.Is_Deleted = 0
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   UPDATE SET
# MAGIC     Is_Deleted = '1',
# MAGIC     Last_Modified_Date = current_timestamp(),
# MAGIC     Last_Modified_By = 'Databricks'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update Run Status

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata 
# MAGIC SET PipelineEndTime = current_timestamp(),
# MAGIC LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM silver.silver_relay WHERE Is_Deleted=0),
# MAGIC PipelineRunStatus = 'Success'
# MAGIC WHERE TableID = 'SL5'