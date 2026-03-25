-- Databricks notebook source
-- MAGIC %md
-- MAGIC Count 

-- COMMAND ----------

USE Schema Bronze;

-- COMMAND ----------

SELECT COUNT(*) FROM (
  SELECT b.booking_id FROM bronze.booking_projection b
     LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
     LEFT JOIN vendor_transaction_projection ON b.booking_id = vendor_transaction_projection.booking_id
     LEFT JOIN canada_conversions ON vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date
     LEFT JOIN bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id -- Carrier Details
  WHERE 1 = 1 AND b.status::string <> 'cancelled'::string AND (canonical_plan_projection.mode::string <> 'ltl'::string OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status::string <> 'voided'::string OR canonical_plan_projection.status IS NULL) 

  UNION 
  SELECT load_number
FROM big_export_projection b
     LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
     LEFT JOIN canada_conversions on b.ship_date = canada_conversions.ship_date
      LEFT JOIN bronze.projection_carrier on b.carrier_id = projection_carrier.id 
  WHERE 1 = 1 AND b.dispatch_status::string <> 'Cancelled'::string AND (canonical_plan_projection.mode::string = 'ltl'::string OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status::string <> 'voided'::string OR canonical_plan_projection.status IS NULL) 
)

-- COMMAND ----------

SELECT COUNT(*) FROM Silver.silver_relay_carrier_money WHERE Is_Deleted = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Duplicates

-- COMMAND ----------

SELECT Booking_ID FROM Silver.silver_relay_carrier_money WHERE Is_Deleted = 0 GROUP BY Booking_ID HAVING COUNT(*) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Null Count

-- COMMAND ----------

SELECT
  COUNT(*) FILTER (WHERE Booking_ID IS NULL) AS Booking_ID_nulls,
  COUNT(*) FILTER (WHERE Relay_Reference_Number IS NULL) AS Relay_Reference_Number_nulls,
  COUNT(*) FILTER (WHERE Carrier_ID IS NULL) AS Carrier_ID_nulls,
  COUNT(*) FILTER (WHERE Status IS NULL) AS Status_nulls,
  COUNT(*) FILTER (WHERE LTL_or_TL IS NULL) AS LTL_or_TL_nulls,
  COUNT(*) FILTER (WHERE Carrier_Linehaul_USD IS NULL) AS Carrier_Linehaul_USD_nulls,
  COUNT(*) FILTER (WHERE Carrier_Linehaul_CAD IS NULL) AS Carrier_Linehaul_CAD_nulls,
  COUNT(*) FILTER (WHERE Carrier_FuelSurcharge_USD IS NULL) AS Carrier_FuelSurcharge_USD_nulls,
  COUNT(*) FILTER (WHERE Carrier_FuelSurcharge_CAD IS NULL) AS Carrier_FuelSurcharge_CAD_nulls,
  COUNT(*) FILTER (WHERE Carrier_Accessorial_USD IS NULL) AS Carrier_Accessorial_USD_nulls,
  COUNT(*) FILTER (WHERE Carrier_Accessorial_CAD IS NULL) AS Carrier_Accessorial_CAD_nulls,
  COUNT(*) FILTER (WHERE Carrier_Final_Rate_USD IS NULL) AS Carrier_Final_Rate_USD_nulls,
  COUNT(*) FILTER (WHERE Carrier_Final_Rate_CAD IS NULL) AS Carrier_Final_Rate_CAD_nulls,
  COUNT(*) FILTER (WHERE Hashkey IS NULL) AS Hashkey_nulls,
  COUNT(*) FILTER (WHERE Created_Date IS NULL) AS Created_Date_nulls,
  COUNT(*) FILTER (WHERE Created_By IS NULL) AS Created_By_nulls,
  COUNT(*) FILTER (WHERE Last_Modified_Date IS NULL) AS Last_Modified_Date_nulls,
  COUNT(*) FILTER (WHERE Last_Modified_By IS NULL) AS Last_Modified_By_nulls,
  COUNT(*) FILTER (WHERE Is_Deleted IS NULL) AS Is_Deleted_nulls
FROM Silver.silver_relay_carrier_money WHERE IS_Deleted  = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Silver Customer Money

-- COMMAND ----------

SELECT COUNT(*) FROM Silver.silver_relay_customer_money WHERE Is_Deleted = 0

-- COMMAND ----------

 SELECT * FROM Silver.VW_silver_relay_customer_money WHERE Booking_ID IN (SELECT Booking_ID FROM Silver.VW_silver_relay_customer_money GROUP BY Booking_ID HAVING COUNT(*) > 1)

-- COMMAND ----------

SELECT
    COUNT(CASE WHEN Booking_ID IS NULL THEN 1 END) AS Booking_ID_null_count,
    COUNT(CASE WHEN Relay_Reference_Number IS NULL THEN 1 END) AS Relay_Reference_Number_null_count,
    COUNT(CASE WHEN Customer_ID IS NULL THEN 1 END) AS Customer_ID_null_count,
    COUNT(CASE WHEN Lawson_ID IS NULL THEN 1 END) AS Lawson_ID_null_count,
    COUNT(CASE WHEN Status IS NULL THEN 1 END) AS Status_null_count,
    COUNT(CASE WHEN LTL_or_TL IS NULL THEN 1 END) AS LTL_or_TL_null_count,
    COUNT(CASE WHEN Total_Customer_Rate_WO_Cred_USD IS NULL THEN 1 END) AS Total_Customer_Rate_WO_Cred_USD_null_count,
    COUNT(CASE WHEN Total_Customer_Rate_WO_Cred_CAD IS NULL THEN 1 END) AS Total_Customer_Rate_WO_Cred_CAD_null_count,
    COUNT(CASE WHEN Customer_Linehaul_USD IS NULL THEN 1 END) AS Customer_Linehaul_USD_null_count,
    COUNT(CASE WHEN Customer_Linehaul_CAD IS NULL THEN 1 END) AS Customer_Linehaul_CAD_null_count,
    COUNT(CASE WHEN Customer_FuelSurcharge_USD IS NULL THEN 1 END) AS Customer_FuelSurcharge_USD_null_count,
    COUNT(CASE WHEN Customer_FuelSurcharge_CAD IS NULL THEN 1 END) AS Customer_FuelSurcharge_CAD_null_count,
    COUNT(CASE WHEN Customer_Accessorial_USD IS NULL THEN 1 END) AS Customer_Accessorial_USD_null_count,
    COUNT(CASE WHEN Customer_Accessorial_CAD IS NULL THEN 1 END) AS Customer_Accessorial_CAD_null_count,
    COUNT(CASE WHEN Invoicing_Credits_USD IS NULL THEN 1 END) AS Invoicing_Credits_USD_null_count,
    COUNT(CASE WHEN Invoicing_Credits_CAD IS NULL THEN 1 END) AS Invoicing_Credits_CAD_null_count,
    COUNT(CASE WHEN Customer_Final_Rate_USD IS NULL THEN 1 END) AS Customer_Final_Rate_USD_null_count,
    COUNT(CASE WHEN Customer_Final_Rate_CAD IS NULL THEN 1 END) AS Customer_Final_Rate_CAD_null_count,
    COUNT(CASE WHEN Stop_Off_Acc IS NULL THEN 1 END) AS Stop_Off_Acc_null_count,
    COUNT(CASE WHEN Det_Acc IS NULL THEN 1 END) AS Det_Acc_null_count,
    COUNT(CASE WHEN Load_Currency IS NULL THEN 1 END) AS Load_Currency_null_count
FROM Silver.Silver_relay_customer_money;


-- COMMAND ----------

select * from bronze.booking_projection WHERE booking_ID IN (
7140398,
7140399,
7140400,
7140401,
7140402,
7140403,
7140404,
7140405,
7140406,
7140407)

-- COMMAND ----------

select * from silver.silver_relay_customer_money WHERE booking_ID IN (
7140398,
7140399,
7140400,
7140401,
7140402,
7140403,
7140404,
7140405,
7140406,
7140407)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Relay

-- COMMAND ----------

SELECT DISTINCT * FROM Silver.silver_RELAY Where load_id  in(SELECT LOAD_ID from silver.silver_RELAY GROUP BY Load_ID HAVING count(*) > 1) 

-- COMMAND ----------

SELECT COUNT(*) FROM
(SELECT DISTINCT relay_reference_number FROM  Bronze.booking_projection WHERE status NOT IN ('cancelled', "bounced")
union 
SELECT  DISTINCT load_number FROM Bronze.big_export_projection WHERE dispatch_status != 'Cancelled')