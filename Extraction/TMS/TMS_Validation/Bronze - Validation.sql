-- Databricks notebook source
use catalog brokerageprod

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##aljex_cred_debt

-- COMMAND ----------

select count(*) from bronze.aljex_cred_debt

-- COMMAND ----------

Select 'aljex_cred_debt ' as Table_Name, 'office' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_cred_debt  where office is null union
Select 'aljex_cred_debt ' as Table_Name, 'pro_num' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_cred_debt  where pro_num is null union
Select 'aljex_cred_debt ' as Table_Name, ' customer' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_cred_debt  where  customer is null union
Select 'aljex_cred_debt ' as Table_Name, ' type_of_ship' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_cred_debt  where  type_of_ship is null union
Select 'aljex_cred_debt ' as Table_Name, ' ship_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_cred_debt  where  ship_date is null union
Select 'aljex_cred_debt ' as Table_Name, ' revenue' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_cred_debt  where  revenue is null union
Select 'aljex_cred_debt ' as Table_Name, ' expense' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_cred_debt  where  expense is null

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##aljex_Customer_profiles

-- COMMAND ----------

select count(*)  as row_count from brokerageprod.bronze.aljex_customer_profiles


-- COMMAND ----------

Select 'aljex_customer_profiles' as Table_Name, 'cust_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where cust_id is null union
Select 'aljex_customer_profiles' as Table_Name, 'cust_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where cust_name is null union
Select 'aljex_customer_profiles' as Table_Name, 'sales_rep' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where sales_rep is null union
Select 'aljex_customer_profiles' as Table_Name, 'state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where state is null union
Select 'aljex_customer_profiles' as Table_Name, 'status' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where status is null union
Select 'aljex_customer_profiles' as Table_Name, 'cust_country' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where cust_country is null union
Select 'aljex_customer_profiles' as Table_Name, 'address_one' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where address_one is null union
Select 'aljex_customer_profiles' as Table_Name, 'address_two' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where address_two is null union
Select 'aljex_customer_profiles' as Table_Name, 'address_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where address_city is null union
Select 'aljex_customer_profiles' as Table_Name, 'address_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_customer_profiles where address_zip is null 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##aljex_dot_lawson_ref

-- COMMAND ----------

select count(*)  as row_count from brokerageprod.bronze.aljex_dot_lawson_ref


-- COMMAND ----------

Select 'aljex_dot_lawson_ref' as Table_Name, 'aljex_carrier_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_dot_lawson_ref where aljex_carrier_id is null union
Select 'aljex_dot_lawson_ref' as Table_Name, 'dot_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_dot_lawson_ref where dot_number is null union
Select 'aljex_dot_lawson_ref' as Table_Name, 'lawson_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_dot_lawson_ref where lawson_id is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##aljex_invoice

-- COMMAND ----------

select count(*)  as row_count 
from brokerageprod.bronze.aljex_invoice

-- COMMAND ----------

Select 'aljex_invoice ' as Table_Name, 'invoice_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_invoice  where invoice_date is null union
Select 'aljex_invoice ' as Table_Name, 'pro_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_invoice  where pro_number is null union
Select 'aljex_invoice ' as Table_Name, 'shipdate' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_invoice  where shipdate is null 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##aljex_mode_types

-- COMMAND ----------

select count(*)  as row_count 
from brokerageprod.bronze.aljex_mode_types


-- COMMAND ----------

Select 'aljex_mode_types' as Table_Name, 'equipment_type' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_mode_types where equipment_type is null union
Select 'aljex_mode_types' as Table_Name, 'equipment_mode' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.aljex_mode_types where equipment_mode is null 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##ap_lawson_appt

-- COMMAND ----------

select count(*)  as row_count 
from brokerageprod.bronze.ap_lawson_appt


-- COMMAND ----------

Select 'ap_lawson_appt' as Table_Name, 'due_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.ap_lawson_appt where due_date is null union
Select 'ap_lawson_appt' as Table_Name, 'invoice_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.ap_lawson_appt where invoice_number is null union
Select 'ap_lawson_appt' as Table_Name, 'lawson_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.ap_lawson_appt where lawson_id is null union
Select 'ap_lawson_appt' as Table_Name, 'payment_amount' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.ap_lawson_appt where payment_amount is null union
Select 'ap_lawson_appt' as Table_Name, 'payment_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.ap_lawson_appt where payment_date is null union
Select 'ap_lawson_appt' as Table_Name, 'payment_num' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.ap_lawson_appt where payment_num is null union
Select 'ap_lawson_appt' as Table_Name, 'po_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.ap_lawson_appt where po_number is null union
Select 'ap_lawson_appt' as Table_Name, 'record_status' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.ap_lawson_appt where record_status is null 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##big_export_projection

-- COMMAND ----------

select count(*)  as row_count 
from brokerageprod.bronze.big_export_projection


-- COMMAND ----------

Select 'big_export_projection' as Table_Name, 'load_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where load_number is null union
Select 'big_export_projection' as Table_Name, 'carrier_accessorial_expense' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where carrier_accessorial_expense is null union
Select 'big_export_projection' as Table_Name, 'carrier_fuel_expense' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where carrier_fuel_expense is null union
Select 'big_export_projection' as Table_Name, 'carrier_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where carrier_id is null union
Select 'big_export_projection' as Table_Name, 'carrier_linehaul_expense' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where carrier_linehaul_expense is null union
Select 'big_export_projection' as Table_Name, 'carrier_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where carrier_name is null union
Select 'big_export_projection' as Table_Name, 'carrier_pro_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where carrier_pro_number is null union
Select 'big_export_projection' as Table_Name, 'charges' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where charges is null union
Select 'big_export_projection' as Table_Name, 'consignee_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where consignee_city is null union
Select 'big_export_projection' as Table_Name, 'consignee_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where consignee_name is null union
Select 'big_export_projection' as Table_Name, 'consignee_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where consignee_state is null union
Select 'big_export_projection' as Table_Name, 'consignee_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where consignee_zip is null union
Select 'big_export_projection' as Table_Name, 'customer_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where customer_id is null union
Select 'big_export_projection' as Table_Name, 'delivered_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where delivered_date is null union
Select 'big_export_projection' as Table_Name, 'delivery_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where delivery_number is null union
Select 'big_export_projection' as Table_Name, 'dispatch_status' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where dispatch_status is null union
Select 'big_export_projection' as Table_Name, 'invoice_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where invoice_date is null union
Select 'big_export_projection' as Table_Name, 'miles' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where miles is null union
Select 'big_export_projection' as Table_Name, 'pallet_count' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where pallet_count is null union
Select 'big_export_projection' as Table_Name, 'pickup_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where pickup_city is null union
Select 'big_export_projection' as Table_Name, 'pickup_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where pickup_name is null union
Select 'big_export_projection' as Table_Name, 'pickup_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where pickup_number is null union
Select 'big_export_projection' as Table_Name, 'pickup_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where pickup_state is null union
Select 'big_export_projection' as Table_Name, 'pickup_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where pickup_zip is null union
Select 'big_export_projection' as Table_Name, 'piece_count' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where piece_count is null union
Select 'big_export_projection' as Table_Name, 'po_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where po_number is null union
Select 'big_export_projection' as Table_Name, 'projected_expense' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where projected_expense is null union
Select 'big_export_projection' as Table_Name, 'ship_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where ship_date is null union
Select 'big_export_projection' as Table_Name, 'weight' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where weight is null union
Select 'big_export_projection' as Table_Name, 'inserted_at' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where inserted_at is null union
Select 'big_export_projection' as Table_Name, 'updated_at' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.big_export_projection where updated_at is null 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##booking_carrier_planned

-- COMMAND ----------

select count(*)  as row_count 
from brokerageprod.bronze.booking_carrier_planned


-- COMMAND ----------

Select 'booking_carrier_planned' as Table_Name, 'relay_reference_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_carrier_planned where relay_reference_number is null union
Select 'booking_carrier_planned' as Table_Name, 'carrier_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_carrier_planned where carrier_id is null union
Select 'booking_carrier_planned' as Table_Name, 'managed_load_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_carrier_planned where managed_load_id is null union
Select 'booking_carrier_planned' as Table_Name, 'styimed' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_carrier_planned where styimed is null 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##booking_projection

-- COMMAND ----------

select count(*)  as row_count 
from brokerageprod.bronze.booking_projection


-- COMMAND ----------

Select 'booking_projection' as Table_Name, 'booking_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where booking_id is null union
Select 'booking_projection' as Table_Name, 'booked_carrier_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where booked_carrier_name is null union
Select 'booking_projection' as Table_Name, 'total_pallets' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where total_pallets is null union
Select 'booking_projection' as Table_Name, 'truck_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where truck_number is null union
Select 'booking_projection' as Table_Name, 'tonu_customer_money_currency' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where tonu_customer_money_currency is null union
Select 'booking_projection' as Table_Name, 'reserved_carrier_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where reserved_carrier_id is null union
Select 'booking_projection' as Table_Name, 'booked_carrier_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where booked_carrier_id is null union
Select 'booking_projection' as Table_Name, 'cargo_value_amount' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where cargo_value_amount is null union
Select 'booking_projection' as Table_Name, 'empty_city_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where empty_city_state is null union
Select 'booking_projection' as Table_Name, 'is_committed' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where is_committed is null union
Select 'booking_projection' as Table_Name, 'low_value_captured_at' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where low_value_captured_at is null union
Select 'booking_projection' as Table_Name, 'status' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where status is null union
Select 'booking_projection' as Table_Name, 'empty_date_time' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where empty_date_time is null union
Select 'booking_projection' as Table_Name, 'total_weight' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where total_weight is null union
Select 'booking_projection' as Table_Name, 'reserved_carrier_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where reserved_carrier_name is null union
Select 'booking_projection' as Table_Name, 'driver_phone_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where driver_phone_number is null union
Select 'booking_projection' as Table_Name, 'ready_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where ready_date is null union
Select 'booking_projection' as Table_Name, 'total_pieces' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where total_pieces is null union
Select 'booking_projection' as Table_Name, 'rate_con_sent_by' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where rate_con_sent_by is null union
Select 'booking_projection' as Table_Name, 'tonu_carrier_money_currency' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where tonu_carrier_money_currency is null union
Select 'booking_projection' as Table_Name, 'ready_time' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where ready_time is null union
Select 'booking_projection' as Table_Name, 'tonu_rate_con_recipients' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where tonu_rate_con_recipients is null union
Select 'booking_projection' as Table_Name, 'trailer_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where trailer_number is null union
Select 'booking_projection' as Table_Name, 'rolled_by' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where rolled_by is null union
Select 'booking_projection' as Table_Name, 'relay_reference_number' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where relay_reference_number is null union
Select 'booking_projection' as Table_Name, 'booked_by_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where booked_by_name is null union
Select 'booking_projection' as Table_Name, 'driver_info_captured_by' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where driver_info_captured_by is null union
Select 'booking_projection' as Table_Name, 'receiver_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where receiver_city is null union
Select 'booking_projection' as Table_Name, 'total_miles' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where total_miles is null union
Select 'booking_projection' as Table_Name, 'tonu_carrier_money_amount' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where tonu_carrier_money_amount is null union
Select 'booking_projection' as Table_Name, 'booked_at' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where booked_at is null union
Select 'booking_projection' as Table_Name, 'booked_total_carrier_rate_currency' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where booked_total_carrier_rate_currency is null union
Select 'booking_projection' as Table_Name, 'tonu_customer_money_amount' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where tonu_customer_money_amount is null union
Select 'booking_projection' as Table_Name, 'receiver_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where receiver_state is null union
Select 'booking_projection' as Table_Name, 'reservation_cancelled_by' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where reservation_cancelled_by is null union
Select 'booking_projection' as Table_Name, 'is_tonu_issued' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where is_tonu_issued is null union
Select 'booking_projection' as Table_Name, 'reserved_by' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where reserved_by is null union
Select 'booking_projection' as Table_Name, 'first_shipper_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_shipper_city is null union
Select 'booking_projection' as Table_Name, 'rate_cons_sent_count' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where rate_cons_sent_count is null union
Select 'booking_projection' as Table_Name, 'receiver_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where receiver_name is null union
Select 'booking_projection' as Table_Name, 'reserved_at' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where reserved_at is null union
Select 'booking_projection' as Table_Name, 'tonu_issued_by' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where tonu_issued_by is null union
Select 'booking_projection' as Table_Name, 'driver_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where driver_name is null union
Select 'booking_projection' as Table_Name, 'first_shipper_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_shipper_state is null union
Select 'booking_projection' as Table_Name, 'second_shipper_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_shipper_state is null union
Select 'booking_projection' as Table_Name, 'rate_con_recipients' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where rate_con_recipients is null union
Select 'booking_projection' as Table_Name, 'low_value_captured_by' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where low_value_captured_by is null union
Select 'booking_projection' as Table_Name, 'rolled_at' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where rolled_at is null union
Select 'booking_projection' as Table_Name, 'first_shipper_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_shipper_name is null union
Select 'booking_projection' as Table_Name, 'is_must_check_high_value' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where is_must_check_high_value is null union
Select 'booking_projection' as Table_Name, 'second_shipper_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_shipper_city is null union
Select 'booking_projection' as Table_Name, 'booked_total_carrier_rate_amount' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where booked_total_carrier_rate_amount is null union
Select 'booking_projection' as Table_Name, 'stop_count' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where stop_count is null union
Select 'booking_projection' as Table_Name, 'cargo_value_currency' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where cargo_value_currency is null union
Select 'booking_projection' as Table_Name, 'second_shipper_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_shipper_name is null union
Select 'booking_projection' as Table_Name, 'last_rate_con_sent_time' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where last_rate_con_sent_time is null union
Select 'booking_projection' as Table_Name, 'bounced_at' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where bounced_at is null union
Select 'booking_projection' as Table_Name, 'bounced_by' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where bounced_by is null union
Select 'booking_projection' as Table_Name, 'loading_type' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where loading_type is null union
Select 'booking_projection' as Table_Name, 'first_shipper_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_shipper_zip is null union
Select 'booking_projection' as Table_Name, 'first_shipper_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_shipper_id is null union
Select 'booking_projection' as Table_Name, 'second_shipper_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_shipper_zip is null union
Select 'booking_projection' as Table_Name, 'second_shipper_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_shipper_id is null union
Select 'booking_projection' as Table_Name, 'receiver_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where receiver_id is null union
Select 'booking_projection' as Table_Name, 'receiver_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where receiver_zip is null union
Select 'booking_projection' as Table_Name, 'first_intermediary_receiver_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_intermediary_receiver_id is null union
Select 'booking_projection' as Table_Name, 'first_intermediary_receiver_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_intermediary_receiver_name is null union
Select 'booking_projection' as Table_Name, 'first_intermediary_receiver_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_intermediary_receiver_city is null union
Select 'booking_projection' as Table_Name, 'first_intermediary_receiver_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_intermediary_receiver_state is null union
Select 'booking_projection' as Table_Name, 'first_intermediary_receiver_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where first_intermediary_receiver_zip is null union
Select 'booking_projection' as Table_Name, 'second_intermediary_receiver_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_intermediary_receiver_id is null union
Select 'booking_projection' as Table_Name, 'second_intermediary_receiver_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_intermediary_receiver_name is null union
Select 'booking_projection' as Table_Name, 'second_intermediary_receiver_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_intermediary_receiver_city is null union
Select 'booking_projection' as Table_Name, 'second_intermediary_receiver_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_intermediary_receiver_state is null union
Select 'booking_projection' as Table_Name, 'second_intermediary_receiver_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where second_intermediary_receiver_zip is null union
Select 'booking_projection' as Table_Name, 'pickup_count' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where pickup_count is null union
Select 'booking_projection' as Table_Name, 'delivery_count' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where delivery_count is null union
Select 'booking_projection' as Table_Name, 'tender_on_behalf_of_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where tender_on_behalf_of_id is null union
Select 'booking_projection' as Table_Name, 'tender_on_behalf_of_type' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where tender_on_behalf_of_type is null union
Select 'booking_projection' as Table_Name, 'booked_by_user_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where booked_by_user_id is null union
Select 'booking_projection' as Table_Name, 'inserted_at' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where inserted_at is null union
Select 'booking_projection' as Table_Name, 'updated_at' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.booking_projection where updated_at is null 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##cai_data

-- COMMAND ----------

select count(1)  as row_count 
from brokerageprod.bronze.cai_data


-- COMMAND ----------

Select 'cai_data' as Table_Name, 'ship_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where ship_date is null union
Select 'cai_data' as Table_Name, 'load_num' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where load_num is null union
Select 'cai_data' as Table_Name, 'ops_office' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where ops_office is null union
Select 'cai_data' as Table_Name, 'customer' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where customer is null union
Select 'cai_data' as Table_Name, 'cust_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where cust_city is null union
Select 'cai_data' as Table_Name, 'cust_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where cust_state is null union
Select 'cai_data' as Table_Name, 'cust_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where cust_zip is null union
Select 'cai_data' as Table_Name, 'cust_ref_num' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where cust_ref_num is null union
Select 'cai_data' as Table_Name, 'delivery_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where delivery_date is null union
Select 'cai_data' as Table_Name, 'total_picks' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where total_picks is null union
Select 'cai_data' as Table_Name, 'total_delvs' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where total_delvs is null union
Select 'cai_data' as Table_Name, 'status' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where status is null union
Select 'cai_data' as Table_Name, 'mode' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where mode is null union
Select 'cai_data' as Table_Name, 'shipper' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where shipper is null union
Select 'cai_data' as Table_Name, 'shipper_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where shipper_city is null union
Select 'cai_data' as Table_Name, 'shipper_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where shipper_state is null union
Select 'cai_data' as Table_Name, 'shipper_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where shipper_zip is null union
Select 'cai_data' as Table_Name, 'booked_by' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where booked_by is null union
Select 'cai_data' as Table_Name, 'salesperson' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where salesperson is null union
Select 'cai_data' as Table_Name, 'acct_mgr' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where acct_mgr is null union
Select 'cai_data' as Table_Name, 'miles' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where miles is null union
Select 'cai_data' as Table_Name, 'carrier' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where carrier is null union
Select 'cai_data' as Table_Name, 'carr_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where carr_city is null union
Select 'cai_data' as Table_Name, 'carr_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where carr_state is null union
Select 'cai_data' as Table_Name, 'carr_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where carr_zip is null union
Select 'cai_data' as Table_Name, 'carr_mc' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where carr_mc is null union
Select 'cai_data' as Table_Name, 'consignee' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where consignee is null union
Select 'cai_data' as Table_Name, 'consignee_city' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where consignee_city is null union
Select 'cai_data' as Table_Name, 'consignee_state' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where consignee_state is null union
Select 'cai_data' as Table_Name, 'consignee_zip' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where consignee_zip is null union
Select 'cai_data' as Table_Name, 'total_expense' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where total_expense is null union
Select 'cai_data' as Table_Name, 'total_revenue' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where total_revenue is null union
Select 'cai_data' as Table_Name, 'total_margin' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where total_margin is null union
Select 'cai_data' as Table_Name, 'created_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where created_date is null union
Select 'cai_data' as Table_Name, 'booked_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where booked_date is null union
Select 'cai_data' as Table_Name, 'invoice_date' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where invoice_date is null union
Select 'cai_data' as Table_Name, 'pickup_appointment' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where pickup_appointment is null union
Select 'cai_data' as Table_Name, 'delivery_appointment' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.cai_data where delivery_appointment is null

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##cai_equipment

-- COMMAND ----------

select count(1)  as row_count 
from brokerageprod.bronze.cai_equipment


-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## slack_actions_2

-- COMMAND ----------

select count(*) from brokerageprod.bronze.slack_actions_2

-- COMMAND ----------

Select 'slack_actions_2' as Table_Name, 'action_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.slack_actions_2 where action_id is null union
Select 'slack_actions_2' as Table_Name, 'action_type' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.slack_actions_2 where action_type is null union
Select 'slack_actions_2' as Table_Name, 'user_id' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.slack_actions_2 where user_id is null union
Select 'slack_actions_2' as Table_Name, 'channel_name' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.slack_actions_2 where channel_name is null union
Select 'slack_actions_2' as Table_Name, 'channel_type' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.slack_actions_2 where channel_type is null union
Select 'slack_actions_2' as Table_Name, 'datetime' as Column_Name, Count(1) as Null_Count from brokerageprod.bronze.slack_actions_2 where datetime is null 

-- COMMAND ----------

