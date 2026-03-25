# Databricks notebook source
# MAGIC %md
# MAGIC ## Loads Non Split
# MAGIC * **Description:**  Load from Bronze to Silver as delta file
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
# MAGIC WHERE TableID = 'GL3';

# COMMAND ----------

# MAGIC %md
# MAGIC ####Merge the Target Data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO Gold.Fact_Loads_NonSplit AS TARGET
# MAGIC USING  Gold.VW_Fact_Loads_NonSplit_Full_Load AS SOURCE
# MAGIC ON TARGET.DW_Load_ID = SOURCE.DW_Load_ID
# MAGIC
# MAGIC WHEN MATCHED AND Target.Hashkey <> SOURCE.Hashkey THEN UPDATE SET
# MAGIC
# MAGIC     TARGET.Load_Number = SOURCE.Load_Number,
# MAGIC     TARGET.Admin_Fees_Carrier = SOURCE.Admin_Fees_Carrier,
# MAGIC     TARGET.Admin_Fees_Customer = SOURCE.Admin_Fees_Customer,
# MAGIC     TARGET.Billing_Status = SOURCE.Billing_Status,
# MAGIC     TARGET.Actual_Delivered_Date = SOURCE.Actual_Delivered_Date,
# MAGIC     TARGET.Booking_Type = SOURCE.Booking_Type,
# MAGIC     TARGET.Booked_Date = SOURCE.Booked_Date,
# MAGIC     TARGET.Carrier_Fuel_Surcharge = SOURCE.Carrier_Fuel_Surcharge,
# MAGIC     TARGET.Carrier_Linehaul = SOURCE.Carrier_Linehaul,
# MAGIC     TARGET.Carrier_New_Office = SOURCE.Carrier_New_Office,
# MAGIC     TARGET.Carrier_Old_Office = SOURCE.Carrier_Old_Office,
# MAGIC     TARGET.Carrier_ID = SOURCE.Carrier_ID,
# MAGIC     TARGET.Carrier_Name = SOURCE.Carrier_Name,
# MAGIC     TARGET.Carrier_Other_Fees = SOURCE.Carrier_Other_Fees,
# MAGIC     TARGET.Carrier_Rep_ID = SOURCE.Carrier_Rep_ID,
# MAGIC     TARGET.Carrier_Rep = SOURCE.Carrier_Rep,
# MAGIC     TARGET.Combined_Load = SOURCE.Combined_Load,
# MAGIC     TARGET.Consignee_Delivery_Address = SOURCE.Consignee_Delivery_Address,
# MAGIC     TARGET.Consignee_Name = SOURCE.Consignee_Name,
# MAGIC     TARGET.Consignee_Pickup_Address = SOURCE.Consignee_Pickup_Address,
# MAGIC     TARGET.Customer_Fuel_Surcharge = SOURCE.Customer_Fuel_Surcharge,
# MAGIC     TARGET.Customer_Linehaul = SOURCE.Customer_Linehaul,
# MAGIC     TARGET.Customer_Master = SOURCE.Customer_Master,
# MAGIC     TARGET.Customer_ID = SOURCE.Customer_ID,
# MAGIC     TARGET.Customer_Name = SOURCE.Customer_Name,
# MAGIC     TARGET.Customer_Other_Fees = SOURCE.Customer_Other_Fees,
# MAGIC     TARGET.Customer_Rep_ID = SOURCE.Customer_Rep_ID,
# MAGIC     TARGET.Customer_New_Office = SOURCE.Customer_New_Office,
# MAGIC     TARGET.Customer_Old_Office = SOURCE.Customer_Old_Office,
# MAGIC     TARGET.Customer_Rep = SOURCE.Customer_Rep,
# MAGIC     TARGET.Days_Ahead = SOURCE.Days_Ahead,
# MAGIC     TARGET.Bounced_Date = SOURCE.Bounced_Date,
# MAGIC     TARGET.Cancelled_Date = SOURCE.Cancelled_Date,
# MAGIC     TARGET.Delivery_Pickup_Charges_Carrier = SOURCE.Delivery_Pickup_Charges_Carrier,
# MAGIC     TARGET.Delivery_Pickup_Charges_Customer = SOURCE.Delivery_Pickup_Charges_Customer,
# MAGIC     TARGET.Delivery_Appointment_Date = SOURCE.Delivery_Appointment_Date,
# MAGIC     TARGET.Delivery_EndDate = SOURCE.Delivery_EndDate,
# MAGIC     TARGET.Destination_City = SOURCE.Destination_City,
# MAGIC     TARGET.Destination_State = SOURCE.Destination_State,
# MAGIC     TARGET.Destination_Zip = SOURCE.Destination_Zip,
# MAGIC     TARGET.DOT_Number = SOURCE.DOT_Number,
# MAGIC     TARGET.Delivery_StartDate = SOURCE.Delivery_StartDate,
# MAGIC     TARGET.Equipment = SOURCE.Equipment,
# MAGIC     TARGET.Equipment_Vehicle_Charges_Carrier = SOURCE.Equipment_Vehicle_Charges_Carrier,
# MAGIC     TARGET.Equipment_Vehicle_Charges_Customer = SOURCE.Equipment_Vehicle_Charges_Customer,
# MAGIC     TARGET.Expense = SOURCE.Expense,
# MAGIC     TARGET.Financial_Period_Year = SOURCE.Financial_Period_Year,
# MAGIC     TARGET.GreenScreen_Rate = SOURCE.GreenScreen_Rate,
# MAGIC     TARGET.Invoice_Amount = SOURCE.Invoice_Amount,
# MAGIC     TARGET.Invoiced_Date = SOURCE.Invoiced_Date,
# MAGIC     TARGET.Lawson_ID = SOURCE.Lawson_ID,
# MAGIC     TARGET.Load_Currency = SOURCE.Load_Currency,
# MAGIC     TARGET.Load_Flag = SOURCE.Load_Flag,
# MAGIC     TARGET.Load_Lane = SOURCE.Load_Lane,
# MAGIC     TARGET.Load_Status = SOURCE.Load_Status,
# MAGIC     TARGET.Load_Unload_Charges_Carrier = SOURCE.Load_Unload_Charges_Carrier,
# MAGIC     TARGET.Load_Unload_Charges_Customer = SOURCE.Load_Unload_Charges_Customer,
# MAGIC     TARGET.Margin = SOURCE.Margin,
# MAGIC     TARGET.Market_Buy_Rate = SOURCE.Market_Buy_Rate,
# MAGIC     TARGET.Market_Destination = SOURCE.Market_Destination,
# MAGIC     TARGET.Market_Lane = SOURCE.Market_Lane,
# MAGIC     TARGET.Market_Origin = SOURCE.Market_Origin,
# MAGIC     TARGET.MC_Number = SOURCE.MC_Number,
# MAGIC     TARGET.Miles = SOURCE.Miles,
# MAGIC     TARGET.Miscellaneous_Chargers_Carrier = SOURCE.Miscellaneous_Chargers_Carrier,
# MAGIC     TARGET.Miscellaneous_Chargers_Customers = SOURCE.Miscellaneous_Chargers_Customers,
# MAGIC     TARGET.Mode = SOURCE.Mode,
# MAGIC     TARGET.On_Time_Delivery = SOURCE.On_Time_Delivery,
# MAGIC     TARGET.On_Time_Pickup = SOURCE.On_Time_Pickup,
# MAGIC     TARGET.Origin_City = SOURCE.Origin_City,
# MAGIC     TARGET.Origin_State = SOURCE.Origin_State,
# MAGIC     TARGET.Origin_Zip = SOURCE.Origin_Zip,
# MAGIC     TARGET.Permits_Compliance_Charges_Carrier = SOURCE.Permits_Compliance_Charges_Carrier,
# MAGIC     TARGET.Permits_Compliance_Charges_Customer = SOURCE.Permits_Compliance_Charges_Customer,
# MAGIC     TARGET.Pickup_Appointment_Date = SOURCE.Pickup_Appointment_Date,
# MAGIC     TARGET.Actual_Pickup_Date = SOURCE.Actual_Pickup_Date,
# MAGIC     TARGET.Pickup_EndDate = SOURCE.Pickup_EndDate,
# MAGIC     TARGET.Pickup_StartDate = SOURCE.Pickup_StartDate,
# MAGIC     TARGET.PO_Number = SOURCE.PO_Number,
# MAGIC     TARGET.PreBookStatus = SOURCE.PreBookStatus,
# MAGIC     TARGET.Revenue = SOURCE.Revenue,
# MAGIC     TARGET.Rolled_Date = SOURCE.Rolled_Date,
# MAGIC     TARGET.Sales_Rep_ID = SOURCE.Sales_Rep_ID,
# MAGIC     TARGET.Sales_Rep = SOURCE.Sales_Rep,
# MAGIC     TARGET.Ship_Date = SOURCE.Ship_Date,
# MAGIC     TARGET.Spot_Margin = SOURCE.Spot_Margin,
# MAGIC     TARGET.Spot_Revenue = SOURCE.Spot_Revenue,
# MAGIC     TARGET.Tender_Source_Type = SOURCE.Tender_Source_Type,
# MAGIC     TARGET.Tendered_Date = SOURCE.Tendered_Date,
# MAGIC     TARGET.TMS_System = SOURCE.TMS_System,
# MAGIC     TARGET.Trailer_Number = SOURCE.Trailer_Number,
# MAGIC     TARGET.Transit_Routing_Chargers_Carrier = SOURCE.Transit_Routing_Chargers_Carrier,
# MAGIC     TARGET.Transit_Routing_Chargers_Customer = SOURCE.Transit_Routing_Chargers_Customer,
# MAGIC     TARGET.Week_Number = SOURCE.Week_Number,
# MAGIC     TARGET.Hashkey = SOURCE.Hashkey,
# MAGIC     TARGET.Last_Modified_Date = CURRENT_TIMESTAMP(),
# MAGIC     TARGET.Last_Modified_By = 'Databricks',
# MAGIC     TARGET.Is_Deleted = 0
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     DW_Load_ID, Load_Number, Admin_Fees_Carrier, Admin_Fees_Customer, Billing_Status,
# MAGIC     Actual_Delivered_Date, Booking_Type, Booked_Date, Carrier_Fuel_Surcharge, Carrier_Linehaul,
# MAGIC     Carrier_New_Office, Carrier_Old_Office, Carrier_ID, Carrier_Name, Carrier_Other_Fees,
# MAGIC     Carrier_Rep_ID, Carrier_Rep, Combined_Load, Consignee_Delivery_Address, Consignee_Name,
# MAGIC     Consignee_Pickup_Address, Customer_Fuel_Surcharge, Customer_Linehaul, Customer_Master, Customer_ID,
# MAGIC     Customer_Name, Customer_Other_Fees, Customer_Rep_ID, Customer_New_Office, Customer_Old_Office,
# MAGIC     Customer_Rep, Days_Ahead, Bounced_Date, Cancelled_Date, Delivery_Pickup_Charges_Carrier,
# MAGIC     Delivery_Pickup_Charges_Customer, Delivery_Appointment_Date, Delivery_EndDate, Destination_City,
# MAGIC     Destination_State, Destination_Zip, DOT_Number, Delivery_StartDate, Equipment,
# MAGIC     Equipment_Vehicle_Charges_Carrier, Equipment_Vehicle_Charges_Customer, Expense, Financial_Period_Year,
# MAGIC     GreenScreen_Rate, Invoice_Amount, Invoiced_Date, Lawson_ID, Load_Currency,
# MAGIC     Load_Flag, Load_Lane, Load_Status, Load_Unload_Charges_Carrier, Load_Unload_Charges_Customer,
# MAGIC     Margin, Market_Buy_Rate, Market_Destination, Market_Lane, Market_Origin,
# MAGIC     MC_Number, Miles, Miscellaneous_Chargers_Carrier, Miscellaneous_Chargers_Customers, Mode,
# MAGIC     On_Time_Delivery, On_Time_Pickup, Origin_City, Origin_State, Origin_Zip,
# MAGIC     Permits_Compliance_Charges_Carrier, Permits_Compliance_Charges_Customer, Pickup_Appointment_Date, Actual_Pickup_Date, Pickup_EndDate,
# MAGIC     Pickup_StartDate, PO_Number, PreBookStatus, Revenue, Rolled_Date,
# MAGIC     Sales_Rep_ID, Sales_Rep, Ship_Date, Spot_Margin, Spot_Revenue,
# MAGIC     Tender_Source_Type, Tendered_Date, TMS_System, Trailer_Number, Transit_Routing_Chargers_Carrier,
# MAGIC     Transit_Routing_Chargers_Customer, Week_Number, HashKey, Created_Date, Created_By, Last_Modified_Date, Last_Modified_By, Is_Deleted
# MAGIC ) VALUES (
# MAGIC     SOURCE.DW_Load_ID, SOURCE.Load_Number, SOURCE.Admin_Fees_Carrier, SOURCE.Admin_Fees_Customer, SOURCE.Billing_Status,
# MAGIC     SOURCE.Actual_Delivered_Date, SOURCE.Booking_Type, SOURCE.Booked_Date, SOURCE.Carrier_Fuel_Surcharge, SOURCE.Carrier_Linehaul,
# MAGIC     SOURCE.Carrier_New_Office, SOURCE.Carrier_Old_Office, SOURCE.Carrier_ID, SOURCE.Carrier_Name, SOURCE.Carrier_Other_Fees,
# MAGIC     SOURCE.Carrier_Rep_ID, SOURCE.Carrier_Rep, SOURCE.Combined_Load, SOURCE.Consignee_Delivery_Address, SOURCE.Consignee_Name,
# MAGIC     SOURCE.Consignee_Pickup_Address, SOURCE.Customer_Fuel_Surcharge, SOURCE.Customer_Linehaul, SOURCE.Customer_Master, SOURCE.Customer_ID,
# MAGIC     SOURCE.Customer_Name, SOURCE.Customer_Other_Fees, SOURCE.Customer_Rep_ID, SOURCE.Customer_New_Office, SOURCE.Customer_Old_Office,
# MAGIC     SOURCE.Customer_Rep, SOURCE.Days_Ahead, SOURCE.Bounced_Date, SOURCE.Cancelled_Date, SOURCE.Delivery_Pickup_Charges_Carrier,
# MAGIC     SOURCE.Delivery_Pickup_Charges_Customer, SOURCE.Delivery_Appointment_Date, SOURCE.Delivery_EndDate, SOURCE.Destination_City,
# MAGIC     SOURCE.Destination_State, SOURCE.Destination_Zip, SOURCE.DOT_Number, SOURCE.Delivery_StartDate, SOURCE.Equipment,
# MAGIC     SOURCE.Equipment_Vehicle_Charges_Carrier, SOURCE.Equipment_Vehicle_Charges_Customer, SOURCE.Expense, SOURCE.Financial_Period_Year,
# MAGIC     SOURCE.GreenScreen_Rate, SOURCE.Invoice_Amount, SOURCE.Invoiced_Date, SOURCE.Lawson_ID, SOURCE.Load_Currency,
# MAGIC     SOURCE.Load_Flag, SOURCE.Load_Lane, SOURCE.Load_Status, SOURCE.Load_Unload_Charges_Carrier, SOURCE.Load_Unload_Charges_Customer,
# MAGIC     SOURCE.Margin, SOURCE.Market_Buy_Rate, SOURCE.Market_Destination, SOURCE.Market_Lane, SOURCE.Market_Origin,
# MAGIC     SOURCE.MC_Number, SOURCE.Miles, SOURCE.Miscellaneous_Chargers_Carrier, SOURCE.Miscellaneous_Chargers_Customers, SOURCE.Mode,
# MAGIC     SOURCE.On_Time_Delivery, SOURCE.On_Time_Pickup, SOURCE.Origin_City, SOURCE.Origin_State, SOURCE.Origin_Zip,
# MAGIC     SOURCE.Permits_Compliance_Charges_Carrier, SOURCE.Permits_Compliance_Charges_Customer, SOURCE.Pickup_Appointment_Date, SOURCE.Actual_Pickup_Date, SOURCE.Pickup_EndDate,
# MAGIC     SOURCE.Pickup_StartDate, SOURCE.PO_Number, SOURCE.PreBookStatus, SOURCE.Revenue, SOURCE.Rolled_Date,
# MAGIC     SOURCE.Sales_Rep_ID, SOURCE.Sales_Rep, SOURCE.Ship_Date, SOURCE.Spot_Margin, SOURCE.Spot_Revenue,
# MAGIC     SOURCE.Tender_Source_Type, SOURCE.Tendered_Date, SOURCE.TMS_System, SOURCE.Trailer_Number, SOURCE.Transit_Routing_Chargers_Carrier,
# MAGIC     SOURCE.Transit_Routing_Chargers_Customer, SOURCE.Week_Number, SOURCE.hashkey, CURRENT_TIMESTAMP(), 'Databricks', CURRENT_TIMESTAMP(), 'Databricks', 0
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Update Is_Deleted Column

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE Gold.fact_loads_nonsplit 
# MAGIC SET Is_Deleted = 1, Last_Modified_Date = current_timestamp(), Last_Modified_By = 'Databricks' 
# MAGIC WHERE DW_Load_ID NOT IN (SELECT DW_Load_ID FROM Gold.vw_fact_loads_nonsplit_full_load) AND Is_Deleted = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update the run status

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata 
# MAGIC SET PipelineEndTime = current_timestamp(),
# MAGIC LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM Gold.fact_loads_split WHERE Is_Deleted=0),
# MAGIC PipelineRunStatus = 'Success'
# MAGIC WHERE TableID = 'GL3'