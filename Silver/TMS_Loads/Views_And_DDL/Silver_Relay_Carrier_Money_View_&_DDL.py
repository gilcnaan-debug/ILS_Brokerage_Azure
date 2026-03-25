# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ## Relay_Carrier_Money
# MAGIC * **Description:** To extract data from Bronze to Silver as delta file
# MAGIC * **Created Date:** 03/07/2025
# MAGIC * **Created By:** Uday
# MAGIC * **Modified Date:** 06/10/2025
# MAGIC * **Modified By:** Uday
# MAGIC * **Changes Made:** Logic Update

# COMMAND ----------

# MAGIC %md
# MAGIC ####Incremental View

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Relay_Carrier_Money AS
# MAGIC  WITH to_get_avg AS (
# MAGIC   SELECT
# MAGIC     DISTINCT canada_conversions.ship_date,
# MAGIC     canada_conversions.conversion AS us_to_cad,
# MAGIC     canada_conversions.us_to_cad AS cad_to_us
# MAGIC   FROM
# MAGIC     canada_conversions
# MAGIC   ORDER BY
# MAGIC     canada_conversions.ship_date DESC
# MAGIC   LIMIT
# MAGIC     7
# MAGIC ), average_conversions AS (
# MAGIC   SELECT
# MAGIC     avg(to_get_avg.us_to_cad) AS avg_us_to_cad,
# MAGIC     avg(to_get_avg.cad_to_us) AS avg_cad_to_us
# MAGIC   FROM
# MAGIC     to_get_avg
# MAGIC )
# MAGIC  
# MAGIC SELECT *, MD5(
# MAGIC   CONCAT(
# MAGIC     COALESCE(CAST(Booking_ID AS STRING), ''),
# MAGIC     COALESCE(CAST(Relay_Reference_Number AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_ID AS STRING), ''),
# MAGIC     COALESCE(Status, ''),
# MAGIC     COALESCE(LTL_or_TL, ''),
# MAGIC     COALESCE(CAST(Carrier_Final_Rate_USD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Final_Rate_CAD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Linehaul_USD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Linehaul_CAD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_FuelSurcharge_USD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_FuelSurcharge_CAD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Accessorial_USD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Accessorial_CAD AS STRING), '')
# MAGIC   )
# MAGIC ) AS Hashkey FROM 
# MAGIC ( SELECT DISTINCT b.Booking_ID ,
# MAGIC     b.Relay_Reference_Number,
# MAGIC     b.booked_carrier_id  AS Carrier_ID,
# MAGIC     b.Status,
# MAGIC     'TL'::string AS LTL_or_TL,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false) / 100.00::double , 0::double ) AS Carrier_Final_Rate_USD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'USD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false) / 100.00::double , 0::double ) AS Carrier_Final_Rate_CAD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'linehaul'::string) / 100.00::double , 0::double ) AS Carrier_Linehaul_USD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'USD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'linehaul'::string) / 100.00::double , 0::double ) AS Carrier_Linehaul_CAD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'fuel_surcharge'::string) / 100.00::double , 0::double ) AS Carrier_FuelSurcharge_USD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'USD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'fuel_surcharge'::string) / 100.00::double , 0::double ) AS Carrier_FuelSurcharge_CAD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND (vendor_transaction_projection.charge_code::string NOT IN ('linehaul', 'fuel_surcharge'))) / 100.00::double , 0::double ) AS Carrier_Accessorial_USD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'USD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND (vendor_transaction_projection.charge_code::string NOT IN ('linehaul', 'fuel_surcharge'))) / 100.00::double , 0::double ) AS Carrier_Accessorial_CAD
# MAGIC    
# MAGIC    FROM booking_projection b
# MAGIC      LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC      LEFT JOIN vendor_transaction_projection ON b.booking_id = vendor_transaction_projection.booking_id
# MAGIC      LEFT JOIN canada_conversions ON vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date
# MAGIC      JOIN average_conversions ON 1 = 1
# MAGIC      LEFT JOIN bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id -- Carrier Details
# MAGIC   WHERE 1 = 1 AND b.status::string <> 'cancelled'::string AND (canonical_plan_projection.mode::string <> 'ltl'::string OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status::string <> 'voided'::string OR canonical_plan_projection.status IS NULL) AND b.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R40') 
# MAGIC   GROUP BY b.booking_id, b.relay_reference_number, b.status, booked_carrier_id
# MAGIC UNION
# MAGIC  SELECT 
# MAGIC  DISTINCT 
# MAGIC     b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.Carrier_ID,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL'::string AS ltl_or_tl,
# MAGIC     b.projected_expense::float / 100.00 AS total_carrier_rate,
# MAGIC     total_carrier_rate * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double ,
# MAGIC     b.carrier_linehaul_expense::float / 100.00 AS lhc_carrier_rate,
# MAGIC     lhc_carrier_rate * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double ,
# MAGIC     b.carrier_fuel_expense::float / 100.00 as fsc_carrier_rate,
# MAGIC     fsc_carrier_rate * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double,
# MAGIC     b.carrier_accessorial_expense::float / 100.00 AS acc_carrier_rate,
# MAGIC     acc_carrier_rate * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double
# MAGIC    FROM big_export_projection b
# MAGIC      LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC      LEFT JOIN canada_conversions on b.ship_date = canada_conversions.ship_date
# MAGIC      JOIN average_conversions ON 1 = 1
# MAGIC       LEFT JOIN bronze.projection_carrier on b.carrier_id = projection_carrier.id 
# MAGIC   WHERE 1 = 1 AND b.dispatch_status::string <> 'Cancelled'::string AND (canonical_plan_projection.mode::string = 'ltl'::string OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status::string <> 'voided'::string OR canonical_plan_projection.status IS NULL) AND b.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R46') )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Load View

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Relay_Carrier_Money_Full_Load AS
# MAGIC  WITH to_get_avg AS (
# MAGIC   SELECT
# MAGIC     DISTINCT canada_conversions.ship_date,
# MAGIC     canada_conversions.conversion AS us_to_cad,
# MAGIC     canada_conversions.us_to_cad AS cad_to_us
# MAGIC   FROM
# MAGIC     canada_conversions
# MAGIC   ORDER BY
# MAGIC     canada_conversions.ship_date DESC
# MAGIC   LIMIT
# MAGIC     7
# MAGIC ), average_conversions AS (
# MAGIC   SELECT
# MAGIC     avg(to_get_avg.us_to_cad) AS avg_us_to_cad,
# MAGIC     avg(to_get_avg.cad_to_us) AS avg_cad_to_us
# MAGIC   FROM
# MAGIC     to_get_avg
# MAGIC )
# MAGIC  
# MAGIC SELECT *, MD5(
# MAGIC   CONCAT(
# MAGIC     COALESCE(CAST(Booking_ID AS STRING), ''),
# MAGIC     COALESCE(CAST(Relay_Reference_Number AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_ID AS STRING), ''),
# MAGIC     COALESCE(Status, ''),
# MAGIC     COALESCE(LTL_or_TL, ''),
# MAGIC     COALESCE(CAST(Carrier_Final_Rate_USD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Final_Rate_CAD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Linehaul_USD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Linehaul_CAD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_FuelSurcharge_USD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_FuelSurcharge_CAD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Accessorial_USD AS STRING), ''),
# MAGIC     COALESCE(CAST(Carrier_Accessorial_CAD AS STRING), '')
# MAGIC   )
# MAGIC ) AS Hashkey FROM 
# MAGIC ( SELECT DISTINCT b.Booking_ID ,
# MAGIC     b.Relay_Reference_Number,
# MAGIC     b.booked_carrier_id  AS Carrier_ID,
# MAGIC     b.Status,
# MAGIC     'TL'::string AS LTL_or_TL,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false) / 100.00::double , 0::double ) AS Carrier_Final_Rate_USD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'USD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false) / 100.00::double , 0::double ) AS Carrier_Final_Rate_CAD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'linehaul'::string) / 100.00::double , 0::double ) AS Carrier_Linehaul_USD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'USD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'linehaul'::string) / 100.00::double , 0::double ) AS Carrier_Linehaul_CAD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'fuel_surcharge'::string) / 100.00::double , 0::double ) AS Carrier_FuelSurcharge_USD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'USD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'fuel_surcharge'::string) / 100.00::double , 0::double ) AS Carrier_FuelSurcharge_CAD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND (vendor_transaction_projection.charge_code::string NOT IN ('linehaul', 'fuel_surcharge'))) / 100.00::double , 0::double ) AS Carrier_Accessorial_USD,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'USD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND (vendor_transaction_projection.charge_code::string NOT IN ('linehaul', 'fuel_surcharge'))) / 100.00::double , 0::double ) AS Carrier_Accessorial_CAD
# MAGIC    
# MAGIC    FROM booking_projection b
# MAGIC      LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC      LEFT JOIN vendor_transaction_projection ON b.booking_id = vendor_transaction_projection.booking_id
# MAGIC      LEFT JOIN canada_conversions ON vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date
# MAGIC      JOIN average_conversions ON 1 = 1
# MAGIC      LEFT JOIN bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id -- Carrier Details
# MAGIC   WHERE 1 = 1 AND b.status::string <> 'cancelled'::string AND (canonical_plan_projection.mode::string <> 'ltl'::string OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status::string <> 'voided'::string OR canonical_plan_projection.status IS NULL) 
# MAGIC   GROUP BY b.booking_id, b.relay_reference_number, b.status, booked_carrier_id
# MAGIC UNION
# MAGIC  SELECT 
# MAGIC  DISTINCT b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.Carrier_ID,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL'::string AS ltl_or_tl,
# MAGIC     b.projected_expense::float / 100.00 AS total_carrier_rate,
# MAGIC     total_carrier_rate * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double ,
# MAGIC     b.carrier_linehaul_expense::float / 100.00 AS lhc_carrier_rate,
# MAGIC     lhc_carrier_rate * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double ,
# MAGIC     b.carrier_fuel_expense::float / 100.00 as fsc_carrier_rate,
# MAGIC     fsc_carrier_rate * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double,
# MAGIC     b.carrier_accessorial_expense::float / 100.00 AS acc_carrier_rate,
# MAGIC     acc_carrier_rate * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.conversion = 0::float THEN NULL::float
# MAGIC                 ELSE canada_conversions.conversion
# MAGIC             END, average_conversions.avg_us_to_cad)::double
# MAGIC    FROM big_export_projection b
# MAGIC      LEFT JOIN canonical_plan_projection 
# MAGIC      ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC      LEFT JOIN canada_conversions 
# MAGIC      on b.ship_date = canada_conversions.ship_date
# MAGIC      JOIN average_conversions ON 1 = 1
# MAGIC       LEFT JOIN bronze.projection_carrier 
# MAGIC       on b.carrier_id = projection_carrier.id 
# MAGIC   WHERE 1 = 1 AND 
# MAGIC   b.dispatch_status::string <> 'Cancelled'::string AND 
# MAGIC   (canonical_plan_projection.mode::string = 'ltl'::string OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status::string <> 'voided'::string OR canonical_plan_projection.status IS NULL) )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver.Silver_Relay_Carrier_Money (
# MAGIC     Booking_ID BIGINT NOT NULL,
# MAGIC     Relay_Reference_Number BIGINT ,
# MAGIC     Carrier_ID VARCHAR(50),
# MAGIC     Status VARCHAR(50) ,
# MAGIC     LTL_or_TL VARCHAR(50) ,
# MAGIC     Carrier_Linehaul_USD DECIMAL(10,2),
# MAGIC     Carrier_Linehaul_CAD DECIMAL(10,2),
# MAGIC     Carrier_FuelSurcharge_USD DECIMAL(10,2),
# MAGIC     Carrier_FuelSurcharge_CAD DECIMAL(10,2),
# MAGIC     Carrier_Accessorial_USD DECIMAL(10,2),
# MAGIC     Carrier_Accessorial_CAD DECIMAL(10,2),
# MAGIC     Carrier_Final_Rate_USD DECIMAL(10,2),
# MAGIC     Carrier_Final_Rate_CAD DECIMAL(10,2),
# MAGIC     Hashkey VARCHAR(255) NOT NULL,
# MAGIC     Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Created_By VARCHAR(50) NOT NULL,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Last_Modified_By VARCHAR(50) NOT NULL,
# MAGIC     Is_Deleted SMALLINT NOT NULL,
# MAGIC
# MAGIC     CONSTRAINT PK_booking_id_2 PRIMARY KEY (Booking_ID)
# MAGIC );
# MAGIC