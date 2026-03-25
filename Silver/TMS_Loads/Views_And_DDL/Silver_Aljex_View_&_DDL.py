# Databricks notebook source
# MAGIC %md
# MAGIC ## Aljex
# MAGIC * **Description:** To extract data from Bronze to Silver as delta file
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
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Aljex AS
# MAGIC WITH to_get_avg AS (
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
# MAGIC ),
# MAGIC average_conversions AS (
# MAGIC   SELECT
# MAGIC     avg(to_get_avg.us_to_cad) AS avg_us_to_cad,
# MAGIC     avg(to_get_avg.cad_to_us) AS avg_cad_to_us
# MAGIC   FROM
# MAGIC     to_get_avg
# MAGIC ),
# MAGIC aljex_master_dates AS (
# MAGIC
# MAGIC   SELECT DISTINCT 
# MAGIC try_cast(p.load_num AS numeric) AS id,
# MAGIC p.status,
# MAGIC p.equipment,
# MAGIC COALESCE(  TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   ), 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.loaded_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.loaded_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   ), 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   )) AS Use_Date,
# MAGIC CASE
# MAGIC     WHEN dayofweek(coalesce(try_cast(p.arrive_pickup_date AS date), try_cast(p.loaded_date AS date), try_cast(p.pickup_date AS date))) = 1 THEN -- Sunday
# MAGIC     date_add(coalesce(to_date(p.arrive_pickup_date), to_date(p.loaded_date), to_date(p.pickup_date)), 6)
# MAGIC     ELSE
# MAGIC     date_add(date_trunc('week', coalesce(try_cast(p.arrive_pickup_date AS date), try_cast(p.loaded_date AS date), try_cast(p.pickup_date AS date))), 5)
# MAGIC END AS end_date,
# MAGIC CASE
# MAGIC     WHEN dayofweek(try_cast(p.key_c_date AS date)) = 1 THEN
# MAGIC     date_add(to_date(p.key_c_date), 6)
# MAGIC     ELSE
# MAGIC     date_add(date_trunc('week', try_cast(p.key_c_date AS date)), 5)
# MAGIC END AS booked_end_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.pickup_appt_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.pickup_appt_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS pu_appt_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS pu_in,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.loaded_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.loaded_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS pu_out,
# MAGIC COALESCE( 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   ), 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.loaded_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.loaded_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   )) AS Ship_Date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.consignee_appointment_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.consignee_appointment_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS del_appt_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_consignee_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_consignee_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS del_in,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.delivery_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.delivery_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS del_out,
# MAGIC COALESCE( 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_consignee_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_consignee_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   ), 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.delivery_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.delivery_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   )) AS delivered_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.key_c_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.key_c_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS booked_at,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.tag_creation_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.tag_creation_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS created_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS pickup_date,
# MAGIC     CASE
# MAGIC         WHEN dayofweek(p.pickup_date) = 1 THEN 
# MAGIC             DATE_ADD(CAST(p.pickup_date AS DATE), 6)
# MAGIC         ELSE 
# MAGIC             Try_cast(DATE_ADD(date_trunc('WEEK', Try_cast(p.pickup_date AS DATE)), 5) AS DATE)
# MAGIC     END AS pickup_date_end_date,
# MAGIC TRY_CAST(p.must_del_date AS DATE) AS must_del_date
# MAGIC
# MAGIC
# MAGIC FROM (
# MAGIC    SELECT * FROM
# MAGIC         (SELECT *, projection_load_1.id AS load_num,
# MAGIC             projection_load_1.updated_at as LMD
# MAGIC             FROM BRONZE.projection_load_1 
# MAGIC             LEFT JOIN BRONZE.projection_load_2 
# MAGIC             ON projection_load_1.id = projection_load_2.id
# MAGIC             WHERE projection_load_1.Is_Deleted = 0 AND projection_load_2.Is_Deleted = 0
# MAGIC         ) AS p
# MAGIC         WHERE p.status NOT LIKE '%VOID%'
# MAGIC         AND P.LMD > (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where Lower(SourceTableName) like 'projection_load%') 
# MAGIC ) AS p
# MAGIC WHERE p.status NOT LIKE '%VOID%'
# MAGIC        
# MAGIC
# MAGIC ),
# MAGIC aljex_data AS (
# MAGIC   SELECT
# MAGIC     DISTINCT p.load_num:: integer AS id,
# MAGIC     p.customer_id,
# MAGIC     p.ref_num AS shipment_id,
# MAGIC     p.status,
# MAGIC     CASE
# MAGIC       WHEN aljex_user_report_listing.full_name IS NULL THEN p.key_c_user
# MAGIC       ELSE aljex_user_report_listing.full_name
# MAGIC     END AS key_c_user,
# MAGIC     p.ps1,
# MAGIC     CASE
# MAGIC       WHEN dis.full_name IS NULL THEN p.key_h_user
# MAGIC       ELSE dis.full_name
# MAGIC     END AS key_h_user,
# MAGIC     CASE
# MAGIC       WHEN p.key_c_user :: string = 'IMPORT' :: string
# MAGIC       AND p.shipper :: string = 'ALTMAN PLANTS' :: string THEN 'LAX' :: string
# MAGIC       WHEN p.key_c_user :: string = 'IMPORT' :: string
# MAGIC       AND p.equipment :: string = 'LTL' :: string
# MAGIC       AND p.office IN ('64', '63', '62', '61') THEN 'TOR' :: string
# MAGIC       WHEN p.key_c_user :: string = 'IMPORT' :: string
# MAGIC       AND p.equipment :: string = 'LTL' :: string THEN 'LTL' :: string
# MAGIC       WHEN p.key_c_user :: string = 'IMPORT' :: string THEN 'DRAY' :: string
# MAGIC       WHEN p.key_c_user :: string = 'EDILCR' :: string THEN 'LTL' :: string
# MAGIC       ELSE car.new_office
# MAGIC     END AS carr_office,
# MAGIC     COALESCE(
# MAGIC       aljex_user_report_listing.pnl_code,
# MAGIC       relay_users.office_id
# MAGIC     ) :: string AS Carrier_Old_Office,
# MAGIC     p.shipper AS original_shipper,
# MAGIC     CASE
# MAGIC       WHEN customer_lookup.master_customer_name IS NULL THEN p.shipper :: string
# MAGIC       ELSE customer_lookup.master_customer_name
# MAGIC     END AS mastername,
# MAGIC     CASE
# MAGIC       WHEN TRY_CAST(
# MAGIC         COALESCE(
# MAGIC           p.arrive_pickup_date,
# MAGIC           p.loaded_date,
# MAGIC           p.pickup_date
# MAGIC         ) AS DATE
# MAGIC       ) IS NOT NULL THEN TRY_CAST(
# MAGIC         COALESCE(
# MAGIC           p.arrive_pickup_date,
# MAGIC           p.loaded_date,
# MAGIC           p.pickup_date
# MAGIC         ) AS DATE
# MAGIC       )
# MAGIC       ELSE NULL
# MAGIC     END AS use_date,
# MAGIC     CASE
# MAGIC       WHEN DAYOFWEEK(
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           ) IS NOT NULL THEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           )
# MAGIC           ELSE NULL
# MAGIC         END
# MAGIC       ) = 1 THEN CASE
# MAGIC         WHEN TRY_CAST(
# MAGIC           COALESCE(
# MAGIC             p.arrive_pickup_date,
# MAGIC             p.loaded_date,
# MAGIC             p.pickup_date
# MAGIC           ) AS DATE
# MAGIC         ) IS NOT NULL THEN TRY_CAST(
# MAGIC           COALESCE(
# MAGIC             p.arrive_pickup_date,
# MAGIC             p.loaded_date,
# MAGIC             p.pickup_date
# MAGIC           ) AS DATE
# MAGIC         )
# MAGIC         ELSE NULL
# MAGIC       END + INTERVAL 6 DAYS
# MAGIC       ELSE DATE_TRUNC(
# MAGIC         'WEEK',
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           ) IS NOT NULL THEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           )
# MAGIC           ELSE NULL
# MAGIC         END
# MAGIC       ) + INTERVAL 5 DAYS
# MAGIC     END AS end_date,
# MAGIC     CASE
# MAGIC       WHEN carrier_line_haul LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_line_haul RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_line_haul AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate1 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate1 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate1 AS DOUBLE),"0.00"::DOUBLE )
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate2 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate2 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate2 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate3 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate3 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate3 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate4 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate4 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate4 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate5 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate5 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate5 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate6 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate6 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate7 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate7 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate7 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate8 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate8 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate8 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END AS final_carrier_rate,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN p.value :: string RLIKE '^-?[0-9]+\.?[0-9]*$' :: string THEN COALESCE(TRY_CAST(p.value AS NUMERIC ), "0"::NUMERIC)
# MAGIC       ELSE 0.00
# MAGIC     END AS cargo_value,
# MAGIC     case
# MAGIC       when p.office is not null and p.office not in (
# MAGIC         '10',
# MAGIC         '34',
# MAGIC         '51',
# MAGIC         '54',
# MAGIC         '61',
# MAGIC         '62',
# MAGIC         '63',
# MAGIC         '64',
# MAGIC         '74'
# MAGIC       ) then 'USD'
# MAGIC       else coalesce(
# MAGIC         cad_currency.currency_type,
# MAGIC         aljex_customer_profiles.cust_country,
# MAGIC         'CAD'
# MAGIC       )
# MAGIC     end as cust_curr,
# MAGIC     CASE
# MAGIC       WHEN p.office in (
# MAGIC         '10',
# MAGIC         '34',
# MAGIC         '51',
# MAGIC         '54',
# MAGIC         '61',
# MAGIC         '62',
# MAGIC         '63',
# MAGIC         '64',
# MAGIC         '74'
# MAGIC       ) then CONCAT("200", Customer_ID)
# MAGIC       when p.office NOT in (
# MAGIC         '10',
# MAGIC         '34',
# MAGIC         '51',
# MAGIC         '54',
# MAGIC         '61',
# MAGIC         '62',
# MAGIC         '63',
# MAGIC         '64',
# MAGIC         '74'
# MAGIC       ) then CONCAT("880", Customer_ID)
# MAGIC       When p.office IS NULL THEN  CONCAT("200", Customer_ID)
# MAGIC       ELSE NULL
# MAGIC     -- CASE WHEN cust_curr = 'CAD' THEN CONCAT("200", Coalesce(Cust_ID, p.customer_id))
# MAGIC     -- ELSE CONCAT("880", Coalesce(Cust_ID, p.customer_id)) 
# MAGIC     END AS Lawson_ID,
# MAGIC     CASE
# MAGIC       WHEN c.name :: string like '%CAD' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like 'CANADIAN R%' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like '%CAN' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like '%(CAD)%' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like '%-C' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like '%- C%' :: string THEN 'CAD' :: string
# MAGIC       WHEN canada_carriers.canada_dot IS NOT NULL THEN 'CAD' :: string
# MAGIC       ELSE 'USD' :: string
# MAGIC     END AS carrier_curr,
# MAGIC     CASE
# MAGIC       WHEN canada_conversions.conversion IS NULL THEN average_conversions.avg_us_to_cad
# MAGIC       ELSE canada_conversions.conversion
# MAGIC     END AS conversion_rate,
# MAGIC     CASE
# MAGIC       WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us
# MAGIC       ELSE canada_conversions.us_to_cad
# MAGIC     END AS conversion_rate_cad,
# MAGIC     CASE WHEN p.office is NULL THEN 'TOR' ELSE new_office_lookup.new_office END AS customer_new_office,
# MAGIC     Coalesce(p.office, 'TOR') AS customer_old_office,
# MAGIC     p.office AS proj_load_office,
# MAGIC     case
# MAGIC       when aljex_mode_types.equipment_mode = 'F' then 'Flatbed'
# MAGIC       when aljex_mode_types.equipment_mode is null then 'Dry Van'
# MAGIC       when aljex_mode_types.equipment_mode = 'R' then 'Reefer'
# MAGIC       when aljex_mode_types.equipment_mode = 'V' then 'Dry Van'
# MAGIC       when aljex_mode_types.equipment_mode = 'PORTD' then 'Drayage'
# MAGIC       when aljex_mode_types.equipment_mode = 'POWER ONLY' then 'Power Only'
# MAGIC       when aljex_mode_types.equipment_mode = 'IMDL' then 'Intermodel'
# MAGIC       else
# MAGIC         case
# MAGIC           when p.equipment = 'RLTL' then 'Reefer'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC     end as Equipment,
# MAGIC     p.mode AS proj_load_mode,
# MAGIC     CASE
# MAGIC       WHEN p.equipment :: string LIKE '%LTL%' :: string THEN 'LTL' :: string
# MAGIC       ELSE 'TL' :: string
# MAGIC     END AS modee,
# MAGIC     p.invoice_total,
# MAGIC     c.name AS carrier_name,
# MAGIC     financial_calendar.financial_period_sorting,
# MAGIC     c.dot_num AS dot_number,
# MAGIC     p.pickup_name,
# MAGIC     p.origin_city,
# MAGIC     p.origin_state,
# MAGIC     p.pickup_zip_code AS origin_zip,
# MAGIC     p.consignee,
# MAGIC     p.consignee_address,
# MAGIC     p.pickup_address,
# MAGIC     p.dest_city,
# MAGIC     p.dest_state,
# MAGIC     p.consignee_zip_code AS dest_zip,
# MAGIC     COALESCE(TRY_CAST(p.miles AS INT), 0::INT) AS miles,                    
# MAGIC     COALESCE(TRY_CAST(REPLACE(p.weight, " ", "") AS INT), 0::INT) AS weight,
# MAGIC     p.truck_num,
# MAGIC     p.trailer_number,
# MAGIC     p.driver_cell_num,
# MAGIC     p.carrier_id,
# MAGIC     p.hazmat,
# MAGIC     COALESCE(sales.full_name, p.sales_rep) AS sales_rep,
# MAGIC     p.srv_rep as Customer_Rep_ID,
# MAGIC     CASE
# MAGIC       WHEN p.srv_rep :: string = 'BRIOP' :: string THEN 'Brianna Martinez' :: string
# MAGIC       ELSE COALESCE(am.full_name, p.srv_rep)
# MAGIC     END AS srv_rep,
# MAGIC     p.accessorial1,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate1 IS NULL THEN NULL
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate1 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate1 :: string RLIKE '-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate1 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc1_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate1 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate1 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate1 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate1 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc1_rate,
# MAGIC     p.accessorial2,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate2 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate2 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate2 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate2 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc2_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate2 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate2 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate2 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate2 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc2_rate,
# MAGIC     p.accessorial3,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate3 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate3 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate3 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate3 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc3_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate3 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate3 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate3 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate3 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc3_rate,
# MAGIC     p.accessorial4,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate4 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate4 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate4 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate4 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc4_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate4 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate4 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate4 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate4 AS numeric),"0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc4_rate,
# MAGIC     p.accessorial5,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate5 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate5 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate5 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate5 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc5_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate5 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate5 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate5 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate5 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc5_rate,
# MAGIC     p.accessorial6,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate6 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate6 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate6 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate6 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc6_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate6 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate6 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate6 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate6 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc6_rate,
# MAGIC     p.accessorial7,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate7 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate7 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate7 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate7 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc7_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate7 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate7 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate7 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate7 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc7_rate,
# MAGIC     p.accessorial8,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate8 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate8 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate8 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate8 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc8_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate8 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate8 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate8 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate8 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc8_rate,
# MAGIC     case
# MAGIC         when
# MAGIC           accessorial1 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate1 like '%:%' OR  customer_accessorial_rate1 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate1 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate1::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial2 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate2 like '%:%' OR  customer_accessorial_rate2 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate2 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate2::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial3 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate3 like '%:%' OR  customer_accessorial_rate3 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate3 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate3::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial4 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate4 like '%:%' OR  customer_accessorial_rate4 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate4 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate4::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial5 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate5 like '%:%' OR customer_accessorial_rate5 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate5 AS DATE ) IS NOT NULL then 0.00 
# MAGIC             when
# MAGIC               customer_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate5::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial6 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate6 like '%:%' OR  customer_accessorial_rate6 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate6 AS DATE ) IS NOT NULL
# MAGIC             then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate6::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial7 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate7 like '%:%' OR  customer_accessorial_rate7 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate7 AS DATE ) IS NOT NULL
# MAGIC              then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate7::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial8 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate8 like '%:%' OR  customer_accessorial_rate8 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate8 AS DATE ) IS NOT NULL
# MAGIC             then 0.00 
# MAGIC             when
# MAGIC               customer_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate8::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end as customer_accessorial,
# MAGIC case
# MAGIC         when
# MAGIC           accessorial1 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate1 like '%:%' OR  customer_accessorial_rate1 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate1 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate1::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial2 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate2 like '%:%' OR  customer_accessorial_rate2 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate2 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate2::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial3 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate3 like '%:%' OR  customer_accessorial_rate3 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate3 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate3::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial4 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate4 like '%:%' OR  customer_accessorial_rate4 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate4 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate4::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial5 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate5 like '%:%' OR  customer_accessorial_rate5 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate5 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate5::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial6 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate6 like '%:%' OR  customer_accessorial_rate6 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate6 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate6::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial7 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate7 like '%:%' OR  customer_accessorial_rate7 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate7 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate7::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial8 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate8 like '%:%' OR  customer_accessorial_rate8 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate8 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate8::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end as customer_fuel,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_line_haul :: string LIKE '%:%' :: string THEN 0.00
# MAGIC       WHEN p.carrier_line_haul :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_line_haul AS numeric),  "0"::NUMERIC)
# MAGIC       ELSE NULL :: numeric
# MAGIC     END AS carrier_linehaul,
# MAGIC     coalesce(CASE WHEN cust_curr = 'USD' THEN p.invoice_total::float ELSE p.invoice_total::float / conversion_rate::float
# MAGIC       END,0) - (CASE WHEN p.accessorial1 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc1_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial2 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc2_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial3 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc3_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial4 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc4_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial5 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc5_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial6 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc6_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial7 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc7_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial8 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc8_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END) - (CASE WHEN p.accessorial1 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc1_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial2 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc2_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial3 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc3_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial4 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc4_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial5 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc5_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial6 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc6_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial7 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc7_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial8 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc8_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END) AS Customer_Linehaul,
# MAGIC     CASE
# MAGIC       WHEN p.accessorial1 LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate1 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate1 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate1 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial2 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate2 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate2 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate2 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial3 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate3 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate3 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate3 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial4 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate4 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate4 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate4 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial5 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate5 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate5 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate5 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial6 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate6 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate6 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate6 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial7 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate7 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate7 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate7 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial8 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate8 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate8 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate8 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END AS carrier_fuel,
# MAGIC     case
# MAGIC       when
# MAGIC         accessorial1 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate1 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate1 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial2 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate2 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate2 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial3 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate3 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate3 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial4 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate4 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC            COALESCE(TRY_CAST(carrier_accessorial_rate4 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial5 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate5 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate5 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial6 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate6 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate6 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial7 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate7 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate7 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial8 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate8 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate8 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end as carrier_accessorial,    
# MAGIC     p.ref_num,
# MAGIC     p.consignee_address_line2,
# MAGIC     c.mc_num as Mc_Number,
# MAGIC     p.carrier_id,
# MAGIC     p.customer_id,
# MAGIC     p.key_c_user as carrier_rep_id
# MAGIC     
# MAGIC      FROM
# MAGIC     (   SELECT * FROM
# MAGIC         (SELECT *, projection_load_1.id AS load_num,
# MAGIC             projection_load_1.updated_at as LMD
# MAGIC             FROM BRONZE.projection_load_1 
# MAGIC             LEFT JOIN BRONZE.projection_load_2 
# MAGIC             ON projection_load_1.id = projection_load_2.id
# MAGIC             WHERE projection_load_1.Is_Deleted = 0 AND projection_load_2.Is_Deleted = 0
# MAGIC         ) AS p
# MAGIC         WHERE p.status NOT LIKE '%VOID%'
# MAGIC         AND P.LMD > (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where Lower(SourceTableName) like 'projection_load%') 
# MAGIC     ) AS p
# MAGIC     LEFT JOIN financial_calendar ON p.pickup_date :: date = financial_calendar.date
# MAGIC     LEFT JOIN new_office_lookup ON p.office :: string = new_office_lookup.old_office :: string
# MAGIC     LEFT JOIN cad_currency ON p.load_num :: string = cad_currency.pro_number :: string
# MAGIC     LEFT JOIN aljex_customer_profiles ON p.customer_id :: string = aljex_customer_profiles.cust_id :: string
# MAGIC     LEFT JOIN projection_carrier c ON p.carrier_id :: string = c.id :: string
# MAGIC     LEFT JOIN aljex_mode_types on p.equipment = aljex_mode_types.equipment_type
# MAGIC     LEFT JOIN canada_carriers ON p.carrier_id :: string = canada_carriers.canada_dot :: string
# MAGIC     LEFT JOIN customer_lookup ON left(p.shipper :: string, 32) = left(customer_lookup.aljex_customer_name, 32)
# MAGIC     JOIN average_conversions ON 1 = 1
# MAGIC     LEFT JOIN aljex_user_report_listing am ON p.srv_rep :: string = am.aljex_id :: string
# MAGIC     AND am.aljex_id IS NOT NULL
# MAGIC     LEFT JOIN aljex_user_report_listing sales ON p.sales_rep :: string = sales.sales_rep :: string
# MAGIC     AND sales.sales_rep IS NOT NULL
# MAGIC     LEFT JOIN canada_conversions ON p.pickup_date :: date = canada_conversions.ship_date
# MAGIC     LEFT JOIN aljex_user_report_listing ON p.key_c_user = aljex_user_report_listing.aljex_id
# MAGIC     AND aljex_user_report_listing.aljex_id <> 'repeat' 
# MAGIC     LEFT JOIN relay_users ON aljex_user_report_listing.full_name :: string = relay_users.full_name :: string
# MAGIC     AND relay_users.`active?` = true
# MAGIC     LEFT JOIN new_office_lookup car ON COALESCE(
# MAGIC       aljex_user_report_listing.pnl_code,
# MAGIC       relay_users.office_id
# MAGIC     ) :: string = car.old_office :: string
# MAGIC     LEFT JOIN aljex_user_report_listing dis ON p.key_h_user :: string = dis.aljex_id :: string
# MAGIC     AND dis.aljex_id :: string <> 'repeat' :: string 
# MAGIC   WHERE
# MAGIC     p.status :: string NOT LIKE '%VOID%' OR p.status::string <> 'HOLD' :: string
# MAGIC ),
# MAGIC raw_data_one AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN customer_name = 'TJX Companies c/o Blueyonder' THEN 'IL'
# MAGIC       ELSE COALESCE(
# MAGIC         relay_users.office_id,
# MAGIC         aljex_user_report_listing.pnl_code
# MAGIC       )
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     COALESCE(master_customer_name, customer_name) AS mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at
# MAGIC   FROM
# MAGIC     bronze.spot_quotes
# MAGIC     JOIN analytics.dim_date ON CAST(spot_quotes.created_at AS DATE) = dim_date.Calendar_Date
# MAGIC     LEFT JOIN bronze.relay_users ON spot_quotes.created_by = relay_users.full_name
# MAGIC     LEFT JOIN bronze.aljex_user_report_listing ON spot_quotes.created_by = aljex_user_report_listing.full_name
# MAGIC     LEFT JOIN bronze.customer_lookup ON LEFT(customer_name, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC raw_data AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC       WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC       ELSE rep_office
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at,
# MAGIC     rank() OVER (
# MAGIC       PARTITION BY Load_Number,
# MAGIC       status_str
# MAGIC       ORDER BY
# MAGIC         last_updated_at DESC
# MAGIC     ) AS rank
# MAGIC   FROM
# MAGIC     raw_data_one
# MAGIC     LEFT JOIN bronze.days_to_pay_offices ON CASE
# MAGIC       WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC       WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC       ELSE rep_office
# MAGIC     END = days_to_pay_offices.office
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC
# MAGIC pricing_nfi_load_predictions as (
# MAGIC   select
# MAGIC     external_load_id as MR_id,
# MAGIC     max(target_buy_rate_gsn) * max(miles_predicted) as Market_Buy_Rate,
# MAGIC     1 as Markey_buy_rate_flag
# MAGIC   from
# MAGIC     bronze.pricing_nfi_load_predictions
# MAGIC   group by
# MAGIC     external_load_id
# MAGIC ),
# MAGIC aljex_spot_loads as (
# MAGIC   SELECT DISTINCT
# MAGIC     CAST(p.load_num AS FLOAT) AS id,
# MAGIC     p.pickup_reference_number,
# MAGIC     p.shipper,
# MAGIC     COALESCE(cl.master_customer_name, p.shipper) AS master_customer_name,
# MAGIC     CAST(p.invoice_total AS FLOAT) AS revenue,
# MAGIC     NULL AS column_null,
# MAGIC     rd.created_at,
# MAGIC     rd.quote_id,
# MAGIC     1 AS counter,
# MAGIC     rd.quoted_price,
# MAGIC     rd.margin,
# MAGIC     rd.created_by,
# MAGIC     rd.rep_office,
# MAGIC     rd.load_number,
# MAGIC     rd.shipper_ref_number,
# MAGIC     rd.status_str,
# MAGIC     rd.last_updated_at
# MAGIC   FROM
# MAGIC     (   SELECT * FROM
# MAGIC         (SELECT *, projection_load_1.id AS load_num,
# MAGIC             projection_load_1.updated_at as LMD
# MAGIC             FROM BRONZE.projection_load_1 
# MAGIC             LEFT JOIN BRONZE.projection_load_2 
# MAGIC             ON projection_load_1.id = projection_load_2.id
# MAGIC             WHERE projection_load_1.Is_Deleted = 0 AND projection_load_2.Is_Deleted = 0
# MAGIC         ) AS p
# MAGIC         WHERE p.status NOT LIKE '%VOID%'
# MAGIC         AND P.LMD > (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where Lower(SourceTableName) like 'projection_load%') 
# MAGIC     ) p 
# MAGIC       LEFT JOIN bronze.customer_lookup cl
# MAGIC         ON LEFT(p.shipper, 32) = LEFT(cl.aljex_customer_name, 32)
# MAGIC       JOIN raw_data rd
# MAGIC         ON CAST(p.load_num AS STRING) = rd.load_number
# MAGIC         AND COALESCE(cl.master_customer_name, p.shipper) = rd.mastername
# MAGIC         AND p.origin_state = rd.shipper_state
# MAGIC         AND p.dest_state = rd.receiver_state
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     AND CAST(p.pickup_date AS DATE) >= '2021-01-01'
# MAGIC     and rd.status_str = 'WON'
# MAGIC     and rd.rank = 1
# MAGIC ),
# MAGIC
# MAGIC tender as (
# MAGIC   SELECT
# MAGIC     tender_reference_numbers_projection.relay_reference_number as id,
# MAGIC     tenderer,
# MAGIC     tendering_acceptance.accepted_at,
# MAGIC     integration_tender_mapped_projection_v2.tendered_at
# MAGIC   FROM
# MAGIC     bronze.tender_reference_numbers_projection
# MAGIC       LEFT JOIN bronze.tendering_acceptance
# MAGIC         ON tender_reference_numbers_projection.tender_id = tendering_acceptance.tender_id
# MAGIC       LEFT JOIN bronze.integration_tender_mapped_projection_v2
# MAGIC         ON tendering_acceptance.shipment_id = integration_tender_mapped_projection_v2.shipment_id
# MAGIC         AND event_type = 'tender_mapped'
# MAGIC       JOIN analytics.dim_date
# MAGIC         ON cast(accepted_At as DATE) = analytics.dim_date.Calendar_Date
# MAGIC   WHERE
# MAGIC     `accepted?` = 'true'
# MAGIC     AND is_split = 'false'
# MAGIC ),
# MAGIC max as (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     rank() OVER (PARTITION BY id ORDER BY tendered_at, accepted_at DESC) AS rank
# MAGIC   from
# MAGIC     tender
# MAGIC ),
# MAGIC tender_final AS (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     tenderer,
# MAGIC       CASE
# MAGIC         WHEN tenderer IN ('orderful', '3gtms') THEN 'EDI'
# MAGIC         WHEN tenderer = 'vooma-orderful' THEN 'Vooma'
# MAGIC         WHEN tenderer IS NULL THEN 'Manual'
# MAGIC         ELSE tenderer
# MAGIC       END AS Tender_Source_Type
# MAGIC   FROM
# MAGIC     max
# MAGIC   WHERE
# MAGIC     rank = 1
# MAGIC ),
# MAGIC
# MAGIC  Final as ( SELECT DISTINCT
# MAGIC         CONCAT("A",  aljex_data.id::INT)::STRING AS DW_Load_ID,
# MAGIC         aljex_data.id::INT AS Load_ID,
# MAGIC         sac.AdminFees_Carrier_USD AS Admin_Fees_Carrier_USD,
# MAGIC         sac.AdminFees_Carrier_CAD as Admin_Fees_Carrier_CAD,
# MAGIC         sacc.AdminFees_Cust_USD as Admin_Fees_Customer_USD,
# MAGIC         sacc.AdminFees_Cust_CAD as Admin_Fees_Customer_CAD,
# MAGIC         CASE WHEN COALESCE(aljex_invoice.invoice_date::DATE, inv_date::DATE) IS NULL THEN "Unbilled" ELSE "Billed"
# MAGIC         END AS Billing_Status,
# MAGIC         aljex_master_dates.delivered_date::TIMESTAMP_NTZ as Actual_Delivered_Date,
# MAGIC         CASE
# MAGIC           WHEN Customer_New_Office <> Carr_Office THEN 'Crossbooked'
# MAGIC           ELSE 'Same Office'
# MAGIC         END AS Booking_type,
# MAGIC         aljex_master_dates.booked_at::DATE AS Booked_Date,
# MAGIC         Carrier_Fuel as Carrier_Fuel_Surcharge_USD,
# MAGIC         Carrier_Fuel * coalesce(canada_conversions.conversion, avg_us_to_cad) as Carrier_Fuel_Surcharge_CAD,
# MAGIC         Carrier_Linehaul AS Carrier_Linehaul_USD,
# MAGIC         Carrier_Linehaul * coalesce(canada_conversions.conversion, avg_us_to_cad) AS Carrier_Linehaul_CAD,
# MAGIC         carrier_name as Carrier_Name,
# MAGIC         Carr_Office as Carrier_New_Office,
# MAGIC         Carrier_Old_Office,
# MAGIC         Carrier_accessorial as Carrier_Other_Fees_USD,
# MAGIC         Carrier_accessorial * coalesce(canada_conversions.conversion, avg_us_to_cad) AS Carrier_Other_Fees_CAD,
# MAGIC         Carrier_ID,
# MAGIC         Carrier_Rep_ID,
# MAGIC         key_c_user AS Carrier_Rep,
# MAGIC         "NO" AS Combined_Load,
# MAGIC         CONCAT(
# MAGIC           INITCAP(aljex_data.consignee_Address),
# MAGIC           ', ',
# MAGIC           INITCAP(consignee_address_line2),
# MAGIC           ', ',
# MAGIC           INITCAP(dest_city),
# MAGIC           ', ',
# MAGIC           dest_state,
# MAGIC           ', ',
# MAGIC           Dest_Zip
# MAGIC         ) AS Consignee_Delivery_Address,
# MAGIC         Consignee AS Consignee_Name,
# MAGIC         CONCAT(
# MAGIC           INITCAP(pickup_address), ', ', INITCAP(origin_city), ', ', origin_state, ', ', origin_Zip
# MAGIC         ) AS Consignee_Pickup_Address,
# MAGIC         Customer_Fuel as Customer_Fuel_Surcharge_USD ,
# MAGIC         Customer_Fuel * coalesce(canada_conversions.conversion, avg_us_to_cad) as Customer_Fule_Surcharge_CAD,
# MAGIC         Customer_Linehaul as Customer_Linehaul_USD,
# MAGIC         Customer_Linehaul * coalesce(canada_conversions.conversion, avg_us_to_cad) as Customer_Linehaul_CAD,
# MAGIC         Customer_ID,
# MAGIC         Coalesce(mastername,original_shipper) AS Customer_Master,
# MAGIC         original_shipper as Customer_Name,
# MAGIC         Customer_New_Office,
# MAGIC         Customer_Old_Office,
# MAGIC         customer_accessorial AS Customer_Other_Fees_USD,
# MAGIC         customer_accessorial * coalesce(canada_conversions.conversion, avg_us_to_cad) AS Customer_Other_Fees_CAD,
# MAGIC         Customer_Rep_ID,
# MAGIC         srv_rep AS Customer_Rep,
# MAGIC         -- DATEDIFF(CURRENT_DATE, COALESCE(aljex_master_dates.delivered_date::DATE, aljex_master_dates.pickup_date::DATE)) AS Days_Ahead,
# MAGIC         null::date as Bounced_Date,
# MAGIC         null::date as Cancelled_Date,
# MAGIC         DelandPickup_Carrier_CAD as Delivery_Pickup_Charges_Carrier_CAD,
# MAGIC         DelandPickup_Carrier_USD as Delivery_Pickup_Charges_Carrier_USD,
# MAGIC         DelandPickup_Cust_CAD as Delivery_Pickup_Charges_Customer_CAD,
# MAGIC         DelandPickup_Cust_USD as Delivery_Pickup_Charges_Customer_USD,
# MAGIC         del_appt_date as Delivery_Appointment_Date,
# MAGIC         aljex_master_dates.del_out as Delivery_EndDate,
# MAGIC         dest_city as Destination_City,
# MAGIC         dest_state as Destination_State,
# MAGIC         dest_zip as Destination_Zip,
# MAGIC         aljex_data.DOT_Number AS DOT_Number,
# MAGIC         aljex_master_dates.del_in AS Delivery_StartDate,
# MAGIC         aljex_data.Equipment, 
# MAGIC         EquipmentandVehicle_Carrier_usd as Equipment_Vehicle_Charges_Carrier_CAD,
# MAGIC         EquipmentandVehicle_Carrier_cad as Equipment_Vehicle_Charges_Carrier_USD,
# MAGIC         EquipmentandVehicle_Cust_USD as Equipment_Vehicle_Charges_Customer_CAD,
# MAGIC         EquipmentandVehicle_Cust_CAD as Equipment_Vehicle_Charges_Customer_USD, 
# MAGIC         (
# MAGIC           case
# MAGIC             when carrier_curr = 'CAD' then final_carrier_rate::float
# MAGIC             else final_carrier_rate::float * aljex_data.conversion_rate
# MAGIC           end
# MAGIC         ) as Expense_CAD,
# MAGIC         (
# MAGIC           case
# MAGIC             when carrier_curr = 'USD' then final_carrier_rate::float
# MAGIC             else final_carrier_rate::float / aljex_data.conversion_rate
# MAGIC           end
# MAGIC         ) as Expense_USD,
# MAGIC         Expense_CAD AS Carrier_Final_Rate_CAD,
# MAGIC         Expense_USD AS Carrier_Final_Rate_USD,
# MAGIC         Dim_date.period_year as Financial_Period_Year,
# MAGIC         null as GreenScreen_Rate_CAD,
# MAGIC         null as GreenScreen_Rate_USD,
# MAGIC         (
# MAGIC           case
# MAGIC             when cust_curr <> 'USD' then invoice_total::float
# MAGIC             else invoice_total::float * aljex_data.conversion_rate::float
# MAGIC           end
# MAGIC         ) as Invoice_Amount_CAD,
# MAGIC         (
# MAGIC           case
# MAGIC             when cust_curr = 'USD' then invoice_total::float
# MAGIC             else invoice_total::float /aljex_data.conversion_rate::float
# MAGIC           end
# MAGIC         ) as Invoice_Amount_USD,
# MAGIC          COALESCE(aljex_invoice.invoice_date::DATE, inv_date::DATE)::TIMESTAMP_NTZ AS Invoiced_Date,
# MAGIC         Lawson_ID,
# MAGIC         cust_curr AS Load_Currency,
# MAGIC         CONCAT(
# MAGIC           INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC         ) AS Load_Lane,
# MAGIC         UPPER(aljex_data.status) as Load_Status,
# MAGIC         Loadandunload_carrier_cad AS Load_Unload_Charges_Carrier_CAD,
# MAGIC         Loadandunload_carrier_usd AS Load_Unload_Charges_Carrier_USD,
# MAGIC         Loadandunload_Cust_CAD as Load_Unload_Charges_Customer_CAD,
# MAGIC         Loadandunload_Cust_USD as Load_Unload_Charges_Customer_USD,
# MAGIC         case
# MAGIC           when carrier_name is not null then '1'
# MAGIC           else '0'
# MAGIC         end as Load_flag,
# MAGIC         (
# MAGIC           coalesce(
# MAGIC             case
# MAGIC               when cust_curr = 'CAD' then invoice_total::float
# MAGIC               else invoice_total::float * aljex_data.conversion_rate::float
# MAGIC             end,
# MAGIC             0
# MAGIC           )
# MAGIC           - coalesce(
# MAGIC             case
# MAGIC               when carrier_curr = 'CAD' then final_carrier_rate::float
# MAGIC               else final_carrier_rate::float * aljex_data.conversion_rate
# MAGIC             end,
# MAGIC             0
# MAGIC           )
# MAGIC         ) AS Margin_CAD,
# MAGIC         (
# MAGIC           coalesce(
# MAGIC             case
# MAGIC               when cust_curr = 'USD' then invoice_total::float
# MAGIC               else invoice_total::float / aljex_data.conversion_rate::float
# MAGIC             end,
# MAGIC             0
# MAGIC           )
# MAGIC           - coalesce(
# MAGIC             case
# MAGIC               when carrier_curr = 'USD' then final_carrier_rate::float
# MAGIC               else final_carrier_rate::float / aljex_data.conversion_rate
# MAGIC             end,
# MAGIC             0
# MAGIC           )
# MAGIC         ) as Margin_USD,
# MAGIC         
# MAGIC         mr.Market_Buy_Rate / aljex_data.conversion_rate::float AS Market_Buy_Rate_CAD,
# MAGIC         mr.Market_Buy_Rate AS Market_Buy_Rate_USD,
# MAGIC         d.market AS Market_Destination,
# MAGIC         CONCAT(o.market, '>', d.market) AS Market_Lane,
# MAGIC         o.market as Market_Origin,
# MAGIC         Mc_Number,
# MAGIC         case miles Rlike '^\d+(.\d+)?$'
# MAGIC           when true then miles::float
# MAGIC           else 0
# MAGIC         end as Miles,
# MAGIC         Miscellaneous_Carrier_CAD AS Miscellaneous_Chargers_Carrier_CAD,
# MAGIC         Miscellaneous_Carrier_USD AS Miscellaneous_Chargers_Carrier_USD,
# MAGIC         Miscellaneous_Cust_CAD AS Miscellaneous_Chargers_Customers_CAD,
# MAGIC         Miscellaneous_Cust_USD AS Miscellaneous_Chargers_Customers_USD,
# MAGIC         Modee as Mode,
# MAGIC         case
# MAGIC           when delivered_date::date > aljex_master_dates.del_appt_date::date then 'Late'
# MAGIC           else 'OnTime'
# MAGIC         end as On_Time_Delivery,
# MAGIC         case
# MAGIC           when
# MAGIC             aljex_master_dates.Ship_Date::date > COALESCE(aljex_master_dates.pu_appt_date, aljex_master_dates.use_date::date)
# MAGIC           then
# MAGIC             'Late'
# MAGIC           else 'OnTime'
# MAGIC         end as On_Time_Pickup,
# MAGIC         Origin_City,
# MAGIC         Origin_State ,
# MAGIC         Origin_Zip,
# MAGIC         PermitsandCompliance_Carrier_CAD AS Permits_Compliance_Charges_Carrier_CAD,
# MAGIC         PermitsandCompliance_Carrier_USD as Permits_Compliance_Charges_Carrier_USD,
# MAGIC         PermitsandCompliance_cust_CAD AS Permits_Compliance_Charges_Customer_CAD,
# MAGIC         PermitsandCompliance_cust_USD AS Permits_Compliance_Charges_Customer_USD,
# MAGIC         aljex_master_dates.pu_appt_date::TIMESTAMP_NTZ AS Pickup_Appointment_Date,
# MAGIC         aljex_master_dates.Ship_Date::TIMESTAMP_NTZ AS Actual_Pickup_Date,
# MAGIC         aljex_master_dates.pu_out::TIMESTAMP_NTZ AS Pickup_EndDate,
# MAGIC         aljex_master_dates.pu_in::TIMESTAMP_NTZ AS Pickup_StartDate,
# MAGIC         aljex_data.ref_num AS PO_Number,
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(Booked_date AS DATE) < TRY_CAST(aljex_master_dates.Use_Date AS DATE) THEN 'PreBooked'
# MAGIC           ELSE 'SameDay'
# MAGIC         END AS PreBookStatus,
# MAGIC         (
# MAGIC           case
# MAGIC             when cust_curr = 'CAD' then invoice_total::float
# MAGIC             else invoice_total::float * aljex_data.conversion_rate::float
# MAGIC           end
# MAGIC         ) as Revenue_CAD,
# MAGIC         (
# MAGIC           case
# MAGIC             when cust_curr = 'USD' then invoice_total::float
# MAGIC             else invoice_total::float / aljex_data.conversion_rate::float
# MAGIC           end
# MAGIC         ) as Revenue_USD,
# MAGIC         null::date as Rolled_Date,  
# MAGIC         NULL as Sales_Rep_ID,      
# MAGIC         Sales_Rep AS Sales_Rep,
# MAGIC         aljex_master_dates.Use_Date AS Ship_Date,
# MAGIC         datediff(try_cast(aljex_master_dates.use_date as date), try_cast(aljex_master_dates.created_date as date)) as Days_ahead,
# MAGIC         sp.margin as Spot_Margin,
# MAGIC         sp.quoted_price as Spot_Revenue,  
# MAGIC         aljex_master_dates.created_date::DATE AS Tendered_Date,
# MAGIC         "ALJEX" as TMS_System,
# MAGIC         Trailer_Number,
# MAGIC         TransitandRouting_Carrier_CAD AS Transit_Routing_Chargers_Carrier_CAD,
# MAGIC         TransitandRouting_Carrier_USD as Transit_Routing_Chargers_Carrier_USD,
# MAGIC         TransitandRouting_Cust_CAD AS Transit_Routing_Chargers_Customer_CAD,
# MAGIC         TransitandRouting_Cust_USD AS Transit_Routing_Chargers_Customer_USD,
# MAGIC         Week_Num as Week_Number,
# MAGIC         tender_final.Tender_Source_Type
# MAGIC
# MAGIC     FROM aljex_data
# MAGIC     LEFT JOIN aljex_master_dates ON aljex_data.id::INT = aljex_master_dates.id::INT 
# MAGIC     LEFT JOIN financial_calendar ON COALESCE(delivered_date::DATE, pickup_date::DATE)::DATE = financial_calendar.date
# MAGIC     LEFT JOIN projection_invoicing ON aljex_data.id::INT = projection_invoicing.pro_num::INT
# MAGIC     LEFT JOIN customer_lookup ON LEFT(original_shipper, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC     LEFT JOIN aljex_invoice ON aljex_data.id = aljex_invoice.pro_number--::FLOAT
# MAGIC     LEFT JOIN new_office_lookup ON proj_load_office = new_office_lookup.old_office
# MAGIC     LEFT JOIN bronze.market_lookup o ON LEFT(origin_zip, 3) = o.pickup_zip
# MAGIC     LEFT JOIN bronze.market_lookup d ON LEFT(dest_zip, 3) = d.pickup_zip 
# MAGIC     LEFT JOIN aljex_spot_loads sp on aljex_data.id = sp.id -- Spot Perspective
# MAGIC     LEFT JOIN pricing_nfi_load_predictions mr on aljex_data.id = mr.MR_id
# MAGIC     LEFT JOIN analytics.dim_date ON pickup_date::DATE = analytics.dim_date.calendar_date::date
# MAGIC     LEFT JOIN canada_conversions ON pickup_date::DATE = canada_conversions.ship_date::DATE
# MAGIC     LEFT JOIN average_conversions ON 1=1
# MAGIC     LEFT JOIN superinsight.accessorial_carriers sac on aljex_data.id = sac.load_number and sac.tms = 'ALJEX'
# MAGIC     LEFT JOIN superinsight.accessorial_customers sacc on aljex_data.id = sacc.load_number and sacc.tms = 'ALJEX'
# MAGIC     left join bronze.date_range_two on aljex_master_dates.use_date::date = date_range_two.date_date::date
# MAGIC     left join tender_final on aljex_data.id = tender_final.id
# MAGIC     WHERE 
# MAGIC         key_c_user IS NOT NULL )
# MAGIC
# MAGIC   SELECT *, 
# MAGIC     SHA1(
# MAGIC   CONCAT(
# MAGIC     COALESCE(DW_Load_ID::string, ''),
# MAGIC     COALESCE(Load_ID::string, ''),
# MAGIC     COALESCE(Admin_Fees_Carrier_USD::string, ''),
# MAGIC     COALESCE(Admin_Fees_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Admin_Fees_Customer_USD::string, ''),
# MAGIC     COALESCE(Admin_Fees_Customer_CAD::string, ''),
# MAGIC     COALESCE(Billing_Status::string, ''),
# MAGIC     COALESCE(Actual_Delivered_Date::string, ''),
# MAGIC     COALESCE(Booking_Type::string, ''),
# MAGIC     COALESCE(Booked_Date::string, ''),
# MAGIC     COALESCE(Carrier_Fuel_Surcharge_CAD::string, ''),
# MAGIC     COALESCE(Carrier_Fuel_Surcharge_USD::string, ''),
# MAGIC     COALESCE(Carrier_Linehaul_CAD::string, ''),
# MAGIC     COALESCE(Carrier_Linehaul_USD::string, ''),
# MAGIC     COALESCE(Carrier_Name::string, ''),
# MAGIC     COALESCE(Carrier_New_Office::string, ''),
# MAGIC     COALESCE(Carrier_Old_Office::string, ''),
# MAGIC     COALESCE(Carrier_Other_Fees_CAD::string, ''),
# MAGIC     COALESCE(Carrier_Other_Fees_USD::string, ''),
# MAGIC     COALESCE(Carrier_ID::string, ''),
# MAGIC     COALESCE(Carrier_Rep_ID::string, ''),
# MAGIC     COALESCE(Carrier_Rep::string, ''),
# MAGIC     COALESCE(Combined_Load::string, ''),
# MAGIC     COALESCE(Consignee_Delivery_Address::string, ''),
# MAGIC     COALESCE(Consignee_Name::string, ''),
# MAGIC     COALESCE(Consignee_Pickup_Address::string, ''),
# MAGIC     COALESCE(Customer_Fuel_Surcharge_USD::string, ''),
# MAGIC     COALESCE(Customer_Fule_Surcharge_CAD::string, ''),
# MAGIC     COALESCE(Customer_Linehaul_CAD::string, ''),
# MAGIC     COALESCE(Customer_Linehaul_USD::string, ''),
# MAGIC     COALESCE(Customer_ID::string, ''),
# MAGIC     COALESCE(Customer_Master::string, ''),
# MAGIC     COALESCE(Customer_Name::string, ''),
# MAGIC     COALESCE(Customer_New_Office::string, ''),
# MAGIC     COALESCE(Customer_Old_Office::string, ''),
# MAGIC     COALESCE(Customer_Other_Fees_CAD::string, ''),
# MAGIC     COALESCE(Customer_Other_Fees_USD::string, ''),
# MAGIC     COALESCE(Customer_Rep_ID::string, ''),
# MAGIC     COALESCE(Customer_Rep::string, ''),
# MAGIC     COALESCE(Days_Ahead::string, ''),
# MAGIC     COALESCE(Bounced_Date::string, ''),
# MAGIC     COALESCE(Cancelled_Date::string, ''),
# MAGIC     COALESCE(Delivery_Pickup_Charges_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Delivery_Pickup_Charges_Carrier_USD::string, ''),
# MAGIC     COALESCE(Delivery_Pickup_Charges_Customer_CAD::string, ''),
# MAGIC     COALESCE(Delivery_Pickup_Charges_Customer_USD::string, ''),
# MAGIC     COALESCE(Delivery_Appointment_Date::string, ''),
# MAGIC     COALESCE(Delivery_EndDate::string, ''),
# MAGIC     COALESCE(Destination_City::string, ''),
# MAGIC     COALESCE(Destination_State::string, ''),
# MAGIC     COALESCE(Destination_Zip::string, ''),
# MAGIC     COALESCE(DOT_Number::string, ''),
# MAGIC     COALESCE(Delivery_StartDate::string, ''),
# MAGIC     COALESCE(Equipment::string, ''),
# MAGIC     COALESCE(Equipment_Vehicle_Charges_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Equipment_Vehicle_Charges_Carrier_USD::string, ''),
# MAGIC     COALESCE(Equipment_Vehicle_Charges_Customer_CAD::string, ''),
# MAGIC     COALESCE(Equipment_Vehicle_Charges_Customer_USD::string, ''),
# MAGIC     COALESCE(Expense_CAD::string, ''),
# MAGIC     COALESCE(Expense_USD::string, ''),
# MAGIC     COALESCE(Financial_Period_Year::string, ''),
# MAGIC     COALESCE(GreenScreen_Rate_CAD::string, ''),
# MAGIC     COALESCE(GreenScreen_Rate_USD::string, ''),
# MAGIC     COALESCE(Invoice_Amount_CAD::string, ''),
# MAGIC     COALESCE(Invoice_Amount_USD::string, ''),
# MAGIC     COALESCE(Invoiced_Date::string, ''),
# MAGIC     COALESCE(Lawson_ID::string, ''),
# MAGIC     COALESCE(Load_Currency::string, ''),
# MAGIC     COALESCE(Load_Flag::string, ''),
# MAGIC     COALESCE(Load_Lane::string, ''),
# MAGIC     COALESCE(Load_Status::string, ''),
# MAGIC     COALESCE(Load_Unload_Charges_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Load_Unload_Charges_Carrier_USD::string, ''),
# MAGIC     COALESCE(Load_Unload_Charges_Customer_CAD::string, ''),
# MAGIC     COALESCE(Load_Unload_Charges_Customer_USD::string, ''),
# MAGIC     COALESCE(Margin_CAD::string, ''),
# MAGIC     COALESCE(Margin_USD::string, ''),
# MAGIC     COALESCE(Market_Buy_Rate_CAD::string, ''),
# MAGIC     COALESCE(Market_Buy_Rate_USD::string, ''),
# MAGIC     COALESCE(Market_Destination::string, ''),
# MAGIC     COALESCE(Market_Lane::string, ''),
# MAGIC     COALESCE(Market_Origin::string, ''),
# MAGIC     COALESCE(MC_Number::string, ''),
# MAGIC     COALESCE(Miles::string, ''),
# MAGIC     COALESCE(Miscellaneous_Chargers_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Miscellaneous_Chargers_Carrier_USD::string, ''),
# MAGIC     COALESCE(Miscellaneous_Chargers_Customers_CAD::string, ''),
# MAGIC     COALESCE(Miscellaneous_Chargers_Customers_USD::string, ''),
# MAGIC     COALESCE(Mode::string, ''),
# MAGIC     COALESCE(On_Time_Delivery::string, ''),
# MAGIC     COALESCE(On_Time_Pickup::string, ''),
# MAGIC     COALESCE(Origin_City::string, ''),
# MAGIC     COALESCE(Origin_State::string, ''),
# MAGIC     COALESCE(Origin_Zip::string, ''),
# MAGIC     COALESCE(Permits_Compliance_Charges_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Permits_Compliance_Charges_Carrier_USD::string, ''),
# MAGIC     COALESCE(Permits_Compliance_Charges_Customer_CAD::string, ''),
# MAGIC     COALESCE(Permits_Compliance_Charges_Customer_USD::string, ''),
# MAGIC     COALESCE(Pickup_Appointment_Date::string, ''),
# MAGIC     COALESCE(Actual_Pickup_Date::string, ''),
# MAGIC     COALESCE(Pickup_EndDate::string, ''),
# MAGIC     COALESCE(Pickup_StartDate::string, ''),
# MAGIC     COALESCE(PO_Number::string, ''),
# MAGIC     COALESCE(PreBookStatus::string, ''),
# MAGIC     COALESCE(Revenue_CAD::string, ''),
# MAGIC     COALESCE(Revenue_USD::string, ''),
# MAGIC     COALESCE(Rolled_Date::string, ''),
# MAGIC     COALESCE(Sales_Rep_ID::string, ''),
# MAGIC     COALESCE(Sales_Rep::string, ''),
# MAGIC     COALESCE(Ship_Date::string, ''),
# MAGIC     COALESCE(Spot_Margin::string, ''),
# MAGIC     COALESCE(Spot_Revenue::string, ''),
# MAGIC     COALESCE(Tender_Source_Type::string, ''),
# MAGIC     COALESCE(Tendered_Date::string, ''),
# MAGIC     COALESCE(TMS_System::string, ''),
# MAGIC     COALESCE(Trailer_Number::string, ''),
# MAGIC     COALESCE(Transit_Routing_Chargers_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Transit_Routing_Chargers_Carrier_USD::string, ''),
# MAGIC     COALESCE(Transit_Routing_Chargers_Customer_CAD::string, ''),
# MAGIC     COALESCE(Transit_Routing_Chargers_Customer_USD::string, ''),
# MAGIC     COALESCE(Week_Number::string, ''),
# MAGIC     COALESCE(Carrier_Final_Rate_CAD::string, ''),
# MAGIC     COALESCE(Carrier_Final_Rate_USD::string, '')
# MAGIC   )
# MAGIC ) AS HashKey FROM Final;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Load View

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Aljex_Full_Load AS
# MAGIC WITH to_get_avg AS (
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
# MAGIC ),
# MAGIC average_conversions AS (
# MAGIC   SELECT
# MAGIC     avg(to_get_avg.us_to_cad) AS avg_us_to_cad,
# MAGIC     avg(to_get_avg.cad_to_us) AS avg_cad_to_us
# MAGIC   FROM
# MAGIC     to_get_avg
# MAGIC ),
# MAGIC aljex_master_dates AS (
# MAGIC
# MAGIC   SELECT DISTINCT 
# MAGIC try_cast(p.load_num AS numeric) AS id,
# MAGIC p.status,
# MAGIC p.equipment,
# MAGIC COALESCE(  TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   ), 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.loaded_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.loaded_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   ), 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   )) AS Use_Date,
# MAGIC CASE
# MAGIC     WHEN dayofweek(coalesce(try_cast(p.arrive_pickup_date AS date), try_cast(p.loaded_date AS date), try_cast(p.pickup_date AS date))) = 1 THEN -- Sunday
# MAGIC     date_add(coalesce(to_date(p.arrive_pickup_date), to_date(p.loaded_date), to_date(p.pickup_date)), 6)
# MAGIC     ELSE
# MAGIC     date_add(date_trunc('week', coalesce(try_cast(p.arrive_pickup_date AS date), try_cast(p.loaded_date AS date), try_cast(p.pickup_date AS date))), 5)
# MAGIC END AS end_date,
# MAGIC CASE
# MAGIC     WHEN dayofweek(try_cast(p.key_c_date AS date)) = 1 THEN
# MAGIC     date_add(to_date(p.key_c_date), 6)
# MAGIC     ELSE
# MAGIC     date_add(date_trunc('week', try_cast(p.key_c_date AS date)), 5)
# MAGIC END AS booked_end_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.pickup_appt_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.pickup_appt_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS pu_appt_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS pu_in,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.loaded_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.loaded_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS pu_out,
# MAGIC COALESCE( 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   ), 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.loaded_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.loaded_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   )) AS Ship_Date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.consignee_appointment_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.consignee_appointment_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS del_appt_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_consignee_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_consignee_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS del_in,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.delivery_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.delivery_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS del_out,
# MAGIC COALESCE( 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.arrive_consignee_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.arrive_consignee_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   ), 
# MAGIC   TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.delivery_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.delivery_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ
# MAGIC   )) AS delivered_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.key_c_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.key_c_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS booked_at,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.tag_creation_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.tag_creation_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS created_date,
# MAGIC TRY_CAST(
# MAGIC     CONCAT(
# MAGIC       TRY_CAST(p.pickup_date AS DATE), ' ', 
# MAGIC       COALESCE(
# MAGIC         date_format(try_cast(p.pickup_time AS timestamp), 'HH:mm:ss'), '00:00:00')
# MAGIC     ) AS TIMESTAMP_NTZ) AS pickup_date,
# MAGIC     CASE
# MAGIC         WHEN dayofweek(p.pickup_date) = 1 THEN 
# MAGIC             DATE_ADD(CAST(p.pickup_date AS DATE), 6)
# MAGIC         ELSE 
# MAGIC             Try_cast(DATE_ADD(date_trunc('WEEK', Try_cast(p.pickup_date AS DATE)), 5) AS DATE)
# MAGIC     END AS pickup_date_end_date,
# MAGIC TRY_CAST(p.must_del_date AS DATE) AS must_del_date
# MAGIC
# MAGIC
# MAGIC FROM (
# MAGIC    SELECT * FROM
# MAGIC         (SELECT *, projection_load_1.id AS load_num,
# MAGIC             projection_load_1.updated_at as LMD
# MAGIC             FROM BRONZE.projection_load_1 
# MAGIC             LEFT JOIN BRONZE.projection_load_2 
# MAGIC             ON projection_load_1.id = projection_load_2.id
# MAGIC             WHERE projection_load_1.Is_Deleted = 0 AND projection_load_2.Is_Deleted = 0
# MAGIC         ) AS p
# MAGIC         WHERE p.status NOT LIKE '%VOID%'
# MAGIC ) AS p
# MAGIC WHERE p.status NOT LIKE '%VOID%'
# MAGIC        
# MAGIC
# MAGIC ),
# MAGIC aljex_data AS (
# MAGIC   SELECT
# MAGIC     DISTINCT p.load_num:: integer AS id,
# MAGIC     p.customer_id,
# MAGIC     p.ref_num AS shipment_id,
# MAGIC     p.status,
# MAGIC     CASE
# MAGIC       WHEN aljex_user_report_listing.full_name IS NULL THEN p.key_c_user
# MAGIC       ELSE aljex_user_report_listing.full_name
# MAGIC     END AS key_c_user,
# MAGIC     p.ps1,
# MAGIC     CASE
# MAGIC       WHEN dis.full_name IS NULL THEN p.key_h_user
# MAGIC       ELSE dis.full_name
# MAGIC     END AS key_h_user,
# MAGIC     CASE
# MAGIC       WHEN p.key_c_user :: string = 'IMPORT' :: string
# MAGIC       AND p.shipper :: string = 'ALTMAN PLANTS' :: string THEN 'LAX' :: string
# MAGIC       WHEN p.key_c_user :: string = 'IMPORT' :: string
# MAGIC       AND p.equipment :: string = 'LTL' :: string
# MAGIC       AND p.office IN ('64', '63', '62', '61') THEN 'TOR' :: string
# MAGIC       WHEN p.key_c_user :: string = 'IMPORT' :: string
# MAGIC       AND p.equipment :: string = 'LTL' :: string THEN 'LTL' :: string
# MAGIC       WHEN p.key_c_user :: string = 'IMPORT' :: string THEN 'DRAY' :: string
# MAGIC       WHEN p.key_c_user :: string = 'EDILCR' :: string THEN 'LTL' :: string
# MAGIC       ELSE car.new_office
# MAGIC     END AS carr_office,
# MAGIC     COALESCE(
# MAGIC       aljex_user_report_listing.pnl_code,
# MAGIC       relay_users.office_id
# MAGIC     ) :: string AS Carrier_Old_Office,
# MAGIC     p.shipper AS original_shipper,
# MAGIC     CASE
# MAGIC       WHEN customer_lookup.master_customer_name IS NULL THEN p.shipper :: string
# MAGIC       ELSE customer_lookup.master_customer_name
# MAGIC     END AS mastername,
# MAGIC     CASE
# MAGIC       WHEN TRY_CAST(
# MAGIC         COALESCE(
# MAGIC           p.arrive_pickup_date,
# MAGIC           p.loaded_date,
# MAGIC           p.pickup_date
# MAGIC         ) AS DATE
# MAGIC       ) IS NOT NULL THEN TRY_CAST(
# MAGIC         COALESCE(
# MAGIC           p.arrive_pickup_date,
# MAGIC           p.loaded_date,
# MAGIC           p.pickup_date
# MAGIC         ) AS DATE
# MAGIC       )
# MAGIC       ELSE NULL
# MAGIC     END AS use_date,
# MAGIC     CASE
# MAGIC       WHEN DAYOFWEEK(
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           ) IS NOT NULL THEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           )
# MAGIC           ELSE NULL
# MAGIC         END
# MAGIC       ) = 1 THEN CASE
# MAGIC         WHEN TRY_CAST(
# MAGIC           COALESCE(
# MAGIC             p.arrive_pickup_date,
# MAGIC             p.loaded_date,
# MAGIC             p.pickup_date
# MAGIC           ) AS DATE
# MAGIC         ) IS NOT NULL THEN TRY_CAST(
# MAGIC           COALESCE(
# MAGIC             p.arrive_pickup_date,
# MAGIC             p.loaded_date,
# MAGIC             p.pickup_date
# MAGIC           ) AS DATE
# MAGIC         )
# MAGIC         ELSE NULL
# MAGIC       END + INTERVAL 6 DAYS
# MAGIC       ELSE DATE_TRUNC(
# MAGIC         'WEEK',
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           ) IS NOT NULL THEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           )
# MAGIC           ELSE NULL
# MAGIC         END
# MAGIC       ) + INTERVAL 5 DAYS
# MAGIC     END AS end_date,
# MAGIC     CASE
# MAGIC       WHEN carrier_line_haul LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_line_haul RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_line_haul AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate1 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate1 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate1 AS DOUBLE),"0.00"::DOUBLE )
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate2 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate2 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate2 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate3 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate3 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate3 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate4 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate4 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate4 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate5 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate5 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate5 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate6 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate6 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate7 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate7 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate7 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN carrier_accessorial_rate8 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate8 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN COALESCE(TRY_CAST(carrier_accessorial_rate8 AS DOUBLE), "0.00"::DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END AS final_carrier_rate,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN p.value :: string RLIKE '^-?[0-9]+\.?[0-9]*$' :: string THEN COALESCE(TRY_CAST(p.value AS NUMERIC ), "0"::NUMERIC)
# MAGIC       ELSE 0.00
# MAGIC     END AS cargo_value,
# MAGIC     case
# MAGIC       when p.office is not null and p.office not in (
# MAGIC         '10',
# MAGIC         '34',
# MAGIC         '51',
# MAGIC         '54',
# MAGIC         '61',
# MAGIC         '62',
# MAGIC         '63',
# MAGIC         '64',
# MAGIC         '74'
# MAGIC       ) then 'USD'
# MAGIC       else coalesce(
# MAGIC         cad_currency.currency_type,
# MAGIC         aljex_customer_profiles.cust_country,
# MAGIC         'CAD'
# MAGIC       )
# MAGIC     end as cust_curr,
# MAGIC     CASE
# MAGIC       WHEN p.office in (
# MAGIC         '10',
# MAGIC         '34',
# MAGIC         '51',
# MAGIC         '54',
# MAGIC         '61',
# MAGIC         '62',
# MAGIC         '63',
# MAGIC         '64',
# MAGIC         '74'
# MAGIC       ) then CONCAT("200", Customer_ID)
# MAGIC       when p.office NOT in (
# MAGIC         '10',
# MAGIC         '34',
# MAGIC         '51',
# MAGIC         '54',
# MAGIC         '61',
# MAGIC         '62',
# MAGIC         '63',
# MAGIC         '64',
# MAGIC         '74'
# MAGIC       ) then CONCAT("880", Customer_ID)
# MAGIC       When p.office IS NULL THEN  CONCAT("200", Customer_ID)
# MAGIC       ELSE NULL
# MAGIC     -- CASE WHEN cust_curr = 'CAD' THEN CONCAT("200", Coalesce(Cust_ID, p.customer_id))
# MAGIC     -- ELSE CONCAT("880", Coalesce(Cust_ID, p.customer_id)) 
# MAGIC     END AS Lawson_ID,
# MAGIC     CASE
# MAGIC       WHEN c.name :: string like '%CAD' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like 'CANADIAN R%' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like '%CAN' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like '%(CAD)%' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like '%-C' :: string THEN 'CAD' :: string
# MAGIC       WHEN c.name :: string like '%- C%' :: string THEN 'CAD' :: string
# MAGIC       WHEN canada_carriers.canada_dot IS NOT NULL THEN 'CAD' :: string
# MAGIC       ELSE 'USD' :: string
# MAGIC     END AS carrier_curr,
# MAGIC     CASE
# MAGIC       WHEN canada_conversions.conversion IS NULL THEN average_conversions.avg_us_to_cad
# MAGIC       ELSE canada_conversions.conversion
# MAGIC     END AS conversion_rate,
# MAGIC     CASE
# MAGIC       WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us
# MAGIC       ELSE canada_conversions.us_to_cad
# MAGIC     END AS conversion_rate_cad,
# MAGIC     CASE WHEN p.office is NULL THEN 'TOR' ELSE new_office_lookup.new_office END AS customer_new_office,
# MAGIC     Coalesce(p.office, 'TOR') AS customer_old_office,
# MAGIC     p.office AS proj_load_office,
# MAGIC     case
# MAGIC       when aljex_mode_types.equipment_mode = 'F' then 'Flatbed'
# MAGIC       when aljex_mode_types.equipment_mode is null then 'Dry Van'
# MAGIC       when aljex_mode_types.equipment_mode = 'R' then 'Reefer'
# MAGIC       when aljex_mode_types.equipment_mode = 'V' then 'Dry Van'
# MAGIC       when aljex_mode_types.equipment_mode = 'PORTD' then 'Drayage'
# MAGIC       when aljex_mode_types.equipment_mode = 'POWER ONLY' then 'Power Only'
# MAGIC       when aljex_mode_types.equipment_mode = 'IMDL' then 'Intermodel'
# MAGIC       else
# MAGIC         case
# MAGIC           when p.equipment = 'RLTL' then 'Reefer'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC     end as Equipment,
# MAGIC     p.mode AS proj_load_mode,
# MAGIC     CASE
# MAGIC       WHEN p.equipment :: string LIKE '%LTL%' :: string THEN 'LTL' :: string
# MAGIC       ELSE 'TL' :: string
# MAGIC     END AS modee,
# MAGIC     p.invoice_total,
# MAGIC     c.name AS carrier_name,
# MAGIC     financial_calendar.financial_period_sorting,
# MAGIC     c.dot_num AS dot_number,
# MAGIC     p.pickup_name,
# MAGIC     p.origin_city,
# MAGIC     p.origin_state,
# MAGIC     p.pickup_zip_code AS origin_zip,
# MAGIC     p.consignee,
# MAGIC     p.consignee_address,
# MAGIC     p.pickup_address,
# MAGIC     p.dest_city,
# MAGIC     p.dest_state,
# MAGIC     p.consignee_zip_code AS dest_zip,
# MAGIC     COALESCE(TRY_CAST(p.miles AS INT), 0::INT) AS miles,                    
# MAGIC     COALESCE(TRY_CAST(REPLACE(p.weight, " ", "") AS INT), 0::INT) AS weight,
# MAGIC     p.truck_num,
# MAGIC     p.trailer_number,
# MAGIC     p.driver_cell_num,
# MAGIC     p.carrier_id,
# MAGIC     p.hazmat,
# MAGIC     COALESCE(sales.full_name, p.sales_rep) AS sales_rep,
# MAGIC     p.srv_rep as Customer_Rep_ID,
# MAGIC     CASE
# MAGIC       WHEN p.srv_rep :: string = 'BRIOP' :: string THEN 'Brianna Martinez' :: string
# MAGIC       ELSE COALESCE(am.full_name, p.srv_rep)
# MAGIC     END AS srv_rep,
# MAGIC     p.accessorial1,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate1 IS NULL THEN NULL
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate1 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate1 :: string RLIKE '-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate1 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc1_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate1 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate1 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate1 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate1 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc1_rate,
# MAGIC     p.accessorial2,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate2 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate2 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate2 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate2 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc2_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate2 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate2 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate2 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate2 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc2_rate,
# MAGIC     p.accessorial3,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate3 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate3 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate3 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate3 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc3_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate3 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate3 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate3 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate3 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc3_rate,
# MAGIC     p.accessorial4,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate4 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate4 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate4 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate4 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc4_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate4 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate4 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate4 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate4 AS numeric),"0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc4_rate,
# MAGIC     p.accessorial5,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate5 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate5 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate5 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate5 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc5_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate5 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate5 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate5 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate5 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc5_rate,
# MAGIC     p.accessorial6,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate6 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate6 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate6 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate6 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc6_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate6 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate6 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate6 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate6 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc6_rate,
# MAGIC     p.accessorial7,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate7 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate7 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate7 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate7 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc7_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate7 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate7 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate7 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate7 AS numeric), "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc7_rate,
# MAGIC     p.accessorial8,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_accessorial_rate8 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.carrier_accessorial_rate8 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate8 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate8 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS carr_acc8_rate,
# MAGIC     CASE
# MAGIC       WHEN p.customer_accessorial_rate8 IS NULL THEN NULL :: DOUBLE
# MAGIC       ELSE CASE
# MAGIC         WHEN p.customer_accessorial_rate8 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.customer_accessorial_rate8 :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.customer_accessorial_rate8 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END :: DOUBLE
# MAGIC     END AS cust_acc8_rate,
# MAGIC     case
# MAGIC         when
# MAGIC           accessorial1 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate1 like '%:%' OR  customer_accessorial_rate1 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate1 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate1::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial2 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate2 like '%:%' OR  customer_accessorial_rate2 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate2 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate2::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial3 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate3 like '%:%' OR  customer_accessorial_rate3 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate3 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate3::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial4 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate4 like '%:%' OR  customer_accessorial_rate4 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate4 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate4::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial5 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate5 like '%:%' OR customer_accessorial_rate5 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate5 AS DATE ) IS NOT NULL then 0.00 
# MAGIC             when
# MAGIC               customer_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate5::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial6 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate6 like '%:%' OR  customer_accessorial_rate6 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate6 AS DATE ) IS NOT NULL
# MAGIC             then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate6::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial7 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate7 like '%:%' OR  customer_accessorial_rate7 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate7 AS DATE ) IS NOT NULL
# MAGIC              then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?' 
# MAGIC             then
# MAGIC               customer_accessorial_rate7::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial8 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate8 like '%:%' OR  customer_accessorial_rate8 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate8 AS DATE ) IS NOT NULL
# MAGIC             then 0.00 
# MAGIC             when
# MAGIC               customer_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate8::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end as customer_accessorial,
# MAGIC case
# MAGIC         when
# MAGIC           accessorial1 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate1 like '%:%' OR  customer_accessorial_rate1 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate1 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate1::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial2 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate2 like '%:%' OR  customer_accessorial_rate2 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate2 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate2::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial3 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate3 like '%:%' OR  customer_accessorial_rate3 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate3 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate3::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial4 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate4 like '%:%' OR  customer_accessorial_rate4 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate4 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate4::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial5 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate5 like '%:%' OR  customer_accessorial_rate5 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate5 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate5::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial6 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate6 like '%:%' OR  customer_accessorial_rate6 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate6 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate6::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial7 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate7 like '%:%' OR  customer_accessorial_rate7 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate7 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate7::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial8 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate8 like '%:%' OR  customer_accessorial_rate8 RLIKE '[A-Za-z]' OR  try_cast(customer_accessorial_rate8 AS DATE ) IS NOT NULL then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate8::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end as customer_fuel,
# MAGIC     CASE
# MAGIC       WHEN p.carrier_line_haul :: string LIKE '%:%' :: string THEN 0.00
# MAGIC       WHEN p.carrier_line_haul :: string RLIKE '^-?[0-9]+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_line_haul AS numeric),  "0"::NUMERIC)
# MAGIC       ELSE NULL :: numeric
# MAGIC     END AS carrier_linehaul,
# MAGIC     coalesce(CASE WHEN cust_curr = 'USD' THEN p.invoice_total::float ELSE p.invoice_total::float / conversion_rate::float
# MAGIC       END,0) - (CASE WHEN p.accessorial1 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc1_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial2 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc2_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial3 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc3_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial4 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc4_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial5 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc5_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial6 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc6_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial7 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc7_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial8 LIKE 'FUE%' THEN coalesce(try_cast(cust_acc8_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END) - (CASE WHEN p.accessorial1 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc1_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial2 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc2_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial3 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc3_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial4 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc4_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial5 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc5_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial6 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc6_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial7 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc7_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END + CASE WHEN p.accessorial8 NOT LIKE 'FUE%' THEN coalesce(try_cast(cust_acc8_rate as numeric), 0)::numeric ELSE 0
# MAGIC       END) AS Customer_Linehaul,
# MAGIC     CASE
# MAGIC       WHEN p.accessorial1 LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate1 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate1 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate1 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial2 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate2 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate2 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate2 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial3 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate3 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate3 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate3 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial4 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate4 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate4 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate4 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial5 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate5 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate5 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate5 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial6 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate6 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate6 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate6 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial7 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate7 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate7 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate7 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END + CASE
# MAGIC       WHEN p.accessorial8 :: string LIKE 'FUE%' :: string THEN CASE
# MAGIC         WHEN p.carrier_accessorial_rate8 :: string LIKE '%:%' :: string THEN 0.00
# MAGIC         WHEN p.carrier_accessorial_rate8 :: string RLIKE '\\d+(\\.[0-9]+)?' :: string THEN COALESCE(TRY_CAST(p.carrier_accessorial_rate8 AS numeric),  "0"::NUMERIC)
# MAGIC         ELSE 0.00
# MAGIC       END
# MAGIC       ELSE 0.00
# MAGIC     END AS carrier_fuel,
# MAGIC     case
# MAGIC       when
# MAGIC         accessorial1 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate1 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate1 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial2 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate2 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate2 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial3 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate3 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate3 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial4 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate4 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC            COALESCE(TRY_CAST(carrier_accessorial_rate4 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial5 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate5 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate5 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial6 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate6 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate6 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial7 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate7 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate7 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial8 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate8 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             COALESCE(TRY_CAST(carrier_accessorial_rate8 AS numeric),  "0"::NUMERIC)
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end as carrier_accessorial,    
# MAGIC     p.ref_num,
# MAGIC     p.consignee_address_line2,
# MAGIC     c.mc_num as Mc_Number,
# MAGIC     p.carrier_id,
# MAGIC     p.customer_id,
# MAGIC     p.key_c_user as carrier_rep_id
# MAGIC     
# MAGIC      FROM
# MAGIC     (   SELECT * FROM
# MAGIC         (SELECT *, projection_load_1.id AS load_num,
# MAGIC             projection_load_1.updated_at as LMD
# MAGIC             FROM BRONZE.projection_load_1 
# MAGIC             LEFT JOIN BRONZE.projection_load_2 
# MAGIC             ON projection_load_1.id = projection_load_2.id
# MAGIC             WHERE projection_load_1.Is_Deleted = 0 AND projection_load_2.Is_Deleted = 0
# MAGIC         ) AS p
# MAGIC         WHERE p.status NOT LIKE '%VOID%'
# MAGIC
# MAGIC     ) AS p
# MAGIC     LEFT JOIN financial_calendar ON p.pickup_date :: date = financial_calendar.date
# MAGIC     LEFT JOIN new_office_lookup ON p.office :: string = new_office_lookup.old_office :: string
# MAGIC     LEFT JOIN cad_currency ON p.load_num :: string = cad_currency.pro_number :: string
# MAGIC     LEFT JOIN aljex_customer_profiles ON p.customer_id :: string = aljex_customer_profiles.cust_id :: string
# MAGIC     LEFT JOIN projection_carrier c ON p.carrier_id :: string = c.id :: string
# MAGIC     LEFT JOIN aljex_mode_types on p.equipment = aljex_mode_types.equipment_type
# MAGIC     LEFT JOIN canada_carriers ON p.carrier_id :: string = canada_carriers.canada_dot :: string
# MAGIC     LEFT JOIN customer_lookup ON left(p.shipper :: string, 32) = left(customer_lookup.aljex_customer_name, 32)
# MAGIC     JOIN average_conversions ON 1 = 1
# MAGIC     LEFT JOIN aljex_user_report_listing am ON p.srv_rep :: string = am.aljex_id :: string
# MAGIC     AND am.aljex_id IS NOT NULL
# MAGIC     LEFT JOIN aljex_user_report_listing sales ON p.sales_rep :: string = sales.sales_rep :: string
# MAGIC     AND sales.sales_rep IS NOT NULL
# MAGIC     LEFT JOIN canada_conversions ON p.pickup_date :: date = canada_conversions.ship_date
# MAGIC     LEFT JOIN aljex_user_report_listing ON p.key_c_user = aljex_user_report_listing.aljex_id
# MAGIC     AND aljex_user_report_listing.aljex_id <> 'repeat' 
# MAGIC     LEFT JOIN relay_users ON aljex_user_report_listing.full_name :: string = relay_users.full_name :: string
# MAGIC     AND relay_users.`active?` = true
# MAGIC     LEFT JOIN new_office_lookup car ON COALESCE(
# MAGIC       aljex_user_report_listing.pnl_code,
# MAGIC       relay_users.office_id
# MAGIC     ) :: string = car.old_office :: string
# MAGIC     LEFT JOIN aljex_user_report_listing dis ON p.key_h_user :: string = dis.aljex_id :: string
# MAGIC     AND dis.aljex_id :: string <> 'repeat' :: string 
# MAGIC   WHERE
# MAGIC     p.status::string <> 'HOLD' :: string
# MAGIC ),
# MAGIC raw_data_one AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN customer_name = 'TJX Companies c/o Blueyonder' THEN 'IL'
# MAGIC       ELSE COALESCE(
# MAGIC         relay_users.office_id,
# MAGIC         aljex_user_report_listing.pnl_code
# MAGIC       )
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     COALESCE(master_customer_name, customer_name) AS mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at
# MAGIC   FROM
# MAGIC     bronze.spot_quotes
# MAGIC     JOIN analytics.dim_date ON CAST(spot_quotes.created_at AS DATE) = dim_date.Calendar_Date
# MAGIC     LEFT JOIN bronze.relay_users ON spot_quotes.created_by = relay_users.full_name
# MAGIC     LEFT JOIN bronze.aljex_user_report_listing ON spot_quotes.created_by = aljex_user_report_listing.full_name
# MAGIC     LEFT JOIN bronze.customer_lookup ON LEFT(customer_name, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC raw_data AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC       WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC       ELSE rep_office
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at,
# MAGIC     rank() OVER (
# MAGIC       PARTITION BY Load_Number,
# MAGIC       status_str
# MAGIC       ORDER BY
# MAGIC         last_updated_at DESC
# MAGIC     ) AS rank
# MAGIC   FROM
# MAGIC     raw_data_one
# MAGIC     LEFT JOIN bronze.days_to_pay_offices ON CASE
# MAGIC       WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC       WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC       ELSE rep_office
# MAGIC     END = days_to_pay_offices.office
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC
# MAGIC pricing_nfi_load_predictions as (
# MAGIC   select
# MAGIC     external_load_id as MR_id,
# MAGIC     max(target_buy_rate_gsn) * max(miles_predicted) as Market_Buy_Rate,
# MAGIC     1 as Markey_buy_rate_flag
# MAGIC   from
# MAGIC     bronze.pricing_nfi_load_predictions
# MAGIC   group by
# MAGIC     external_load_id
# MAGIC ),
# MAGIC aljex_spot_loads as (
# MAGIC   SELECT DISTINCT
# MAGIC     CAST(p.load_num AS FLOAT) AS id,
# MAGIC     p.pickup_reference_number,
# MAGIC     p.shipper,
# MAGIC     COALESCE(cl.master_customer_name, p.shipper) AS master_customer_name,
# MAGIC     CAST(p.invoice_total AS FLOAT) AS revenue,
# MAGIC     NULL AS column_null,
# MAGIC     rd.created_at,
# MAGIC     rd.quote_id,
# MAGIC     1 AS counter,
# MAGIC     rd.quoted_price,
# MAGIC     rd.margin,
# MAGIC     rd.created_by,
# MAGIC     rd.rep_office,
# MAGIC     rd.load_number,
# MAGIC     rd.shipper_ref_number,
# MAGIC     rd.status_str,
# MAGIC     rd.last_updated_at
# MAGIC   FROM
# MAGIC     (   SELECT * FROM
# MAGIC         (SELECT *, projection_load_1.id AS load_num,
# MAGIC             projection_load_1.updated_at as LMD
# MAGIC             FROM BRONZE.projection_load_1 
# MAGIC             LEFT JOIN BRONZE.projection_load_2 
# MAGIC             ON projection_load_1.id = projection_load_2.id
# MAGIC             WHERE projection_load_1.Is_Deleted = 0 AND projection_load_2.Is_Deleted = 0
# MAGIC         ) AS p
# MAGIC         WHERE p.status NOT LIKE '%VOID%'
# MAGIC     ) p 
# MAGIC       LEFT JOIN bronze.customer_lookup cl
# MAGIC         ON LEFT(p.shipper, 32) = LEFT(cl.aljex_customer_name, 32)
# MAGIC       JOIN raw_data rd
# MAGIC         ON CAST(p.load_num AS STRING) = rd.load_number
# MAGIC         AND COALESCE(cl.master_customer_name, p.shipper) = rd.mastername
# MAGIC         AND p.origin_state = rd.shipper_state
# MAGIC         AND p.dest_state = rd.receiver_state
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     AND CAST(p.pickup_date AS DATE) >= '2021-01-01'
# MAGIC     and rd.status_str = 'WON'
# MAGIC     and rd.rank = 1
# MAGIC ),
# MAGIC
# MAGIC tender as (
# MAGIC   SELECT
# MAGIC     tender_reference_numbers_projection.relay_reference_number as id,
# MAGIC     tenderer,
# MAGIC     tendering_acceptance.accepted_at,
# MAGIC     integration_tender_mapped_projection_v2.tendered_at
# MAGIC   FROM
# MAGIC     bronze.tender_reference_numbers_projection
# MAGIC       LEFT JOIN bronze.tendering_acceptance
# MAGIC         ON tender_reference_numbers_projection.tender_id = tendering_acceptance.tender_id
# MAGIC       LEFT JOIN bronze.integration_tender_mapped_projection_v2
# MAGIC         ON tendering_acceptance.shipment_id = integration_tender_mapped_projection_v2.shipment_id
# MAGIC         AND event_type = 'tender_mapped'
# MAGIC       JOIN analytics.dim_date
# MAGIC         ON cast(accepted_At as DATE) = analytics.dim_date.Calendar_Date
# MAGIC   WHERE
# MAGIC     `accepted?` = 'true'
# MAGIC     AND is_split = 'false'
# MAGIC ),
# MAGIC max as (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     rank() OVER (PARTITION BY id ORDER BY tendered_at, accepted_at DESC) AS rank
# MAGIC   from
# MAGIC     tender
# MAGIC ),
# MAGIC tender_final AS (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     tenderer,
# MAGIC       CASE
# MAGIC         WHEN tenderer IN ('orderful', '3gtms') THEN 'EDI'
# MAGIC         WHEN tenderer = 'vooma-orderful' THEN 'Vooma'
# MAGIC         WHEN tenderer IS NULL THEN 'Manual'
# MAGIC         ELSE tenderer
# MAGIC       END AS Tender_Source_Type
# MAGIC   FROM
# MAGIC     max
# MAGIC   WHERE
# MAGIC     rank = 1
# MAGIC ),
# MAGIC
# MAGIC  Final as ( SELECT DISTINCT
# MAGIC         CONCAT("A",  aljex_data.id::INT)::STRING AS DW_Load_ID,
# MAGIC         aljex_data.id::INT AS Load_ID,
# MAGIC         sac.AdminFees_Carrier_USD AS Admin_Fees_Carrier_USD,
# MAGIC         sac.AdminFees_Carrier_CAD as Admin_Fees_Carrier_CAD,
# MAGIC         sacc.AdminFees_Cust_USD as Admin_Fees_Customer_USD,
# MAGIC         sacc.AdminFees_Cust_CAD as Admin_Fees_Customer_CAD,
# MAGIC         CASE WHEN COALESCE(aljex_invoice.invoice_date::DATE, inv_date::DATE) IS NULL THEN "Unbilled" ELSE "Billed"
# MAGIC         END AS Billing_Status,
# MAGIC         aljex_master_dates.delivered_date::TIMESTAMP_NTZ as Actual_Delivered_Date,
# MAGIC         CASE
# MAGIC           WHEN Customer_New_Office <> Carr_Office THEN 'Crossbooked'
# MAGIC           ELSE 'Same Office'
# MAGIC         END AS Booking_type,
# MAGIC         aljex_master_dates.booked_at::DATE AS Booked_Date,
# MAGIC         Carrier_Fuel as Carrier_Fuel_Surcharge_USD,
# MAGIC         Carrier_Fuel * coalesce(canada_conversions.conversion, avg_us_to_cad) as Carrier_Fuel_Surcharge_CAD,
# MAGIC         Carrier_Linehaul AS Carrier_Linehaul_USD,
# MAGIC         Carrier_Linehaul * coalesce(canada_conversions.conversion, avg_us_to_cad) AS Carrier_Linehaul_CAD,
# MAGIC         carrier_name as Carrier_Name,
# MAGIC         Carr_Office as Carrier_New_Office,
# MAGIC         Carrier_Old_Office,
# MAGIC         Carrier_accessorial as Carrier_Other_Fees_USD,
# MAGIC         Carrier_accessorial * coalesce(canada_conversions.conversion, avg_us_to_cad) AS Carrier_Other_Fees_CAD,
# MAGIC         Carrier_ID,
# MAGIC         Carrier_Rep_ID,
# MAGIC         key_c_user AS Carrier_Rep,
# MAGIC         "NO" AS Combined_Load,
# MAGIC         CONCAT(
# MAGIC           INITCAP(aljex_data.consignee_Address),
# MAGIC           ', ',
# MAGIC           INITCAP(consignee_address_line2),
# MAGIC           ', ',
# MAGIC           INITCAP(dest_city),
# MAGIC           ', ',
# MAGIC           dest_state,
# MAGIC           ', ',
# MAGIC           Dest_Zip
# MAGIC         ) AS Consignee_Delivery_Address,
# MAGIC         Consignee AS Consignee_Name,
# MAGIC         CONCAT(
# MAGIC           INITCAP(pickup_address), ', ', INITCAP(origin_city), ', ', origin_state, ', ', origin_Zip
# MAGIC         ) AS Consignee_Pickup_Address,
# MAGIC         Customer_Fuel as Customer_Fuel_Surcharge_USD ,
# MAGIC         Customer_Fuel * coalesce(canada_conversions.conversion, avg_us_to_cad) as Customer_Fule_Surcharge_CAD,
# MAGIC         Customer_Linehaul as Customer_Linehaul_USD,
# MAGIC         Customer_Linehaul * coalesce(canada_conversions.conversion, avg_us_to_cad) as Customer_Linehaul_CAD,
# MAGIC         Customer_ID,
# MAGIC         Coalesce(mastername,original_shipper) AS Customer_Master,
# MAGIC         original_shipper as Customer_Name,
# MAGIC         Customer_New_Office,
# MAGIC         Customer_Old_Office,
# MAGIC         customer_accessorial AS Customer_Other_Fees_USD,
# MAGIC         customer_accessorial * coalesce(canada_conversions.conversion, avg_us_to_cad) AS Customer_Other_Fees_CAD,
# MAGIC         Customer_Rep_ID,
# MAGIC         srv_rep AS Customer_Rep,
# MAGIC         -- DATEDIFF(CURRENT_DATE, COALESCE(aljex_master_dates.delivered_date::DATE, aljex_master_dates.pickup_date::DATE)) AS Days_Ahead,
# MAGIC         null::date as Bounced_Date,
# MAGIC         null::date as Cancelled_Date,
# MAGIC         DelandPickup_Carrier_CAD as Delivery_Pickup_Charges_Carrier_CAD,
# MAGIC         DelandPickup_Carrier_USD as Delivery_Pickup_Charges_Carrier_USD,
# MAGIC         DelandPickup_Cust_CAD as Delivery_Pickup_Charges_Customer_CAD,
# MAGIC         DelandPickup_Cust_USD as Delivery_Pickup_Charges_Customer_USD,
# MAGIC         del_appt_date as Delivery_Appointment_Date,
# MAGIC         aljex_master_dates.del_out as Delivery_EndDate,
# MAGIC         dest_city as Destination_City,
# MAGIC         dest_state as Destination_State,
# MAGIC         dest_zip as Destination_Zip,
# MAGIC         aljex_data.DOT_Number AS DOT_Number,
# MAGIC         aljex_master_dates.del_in AS Delivery_StartDate,
# MAGIC         aljex_data.Equipment, 
# MAGIC         EquipmentandVehicle_Carrier_usd as Equipment_Vehicle_Charges_Carrier_CAD,
# MAGIC         EquipmentandVehicle_Carrier_cad as Equipment_Vehicle_Charges_Carrier_USD,
# MAGIC         EquipmentandVehicle_Cust_USD as Equipment_Vehicle_Charges_Customer_CAD,
# MAGIC         EquipmentandVehicle_Cust_CAD as Equipment_Vehicle_Charges_Customer_USD, 
# MAGIC         (
# MAGIC           case
# MAGIC             when carrier_curr = 'CAD' then final_carrier_rate::float
# MAGIC             else final_carrier_rate::float * aljex_data.conversion_rate
# MAGIC           end
# MAGIC         ) as Expense_CAD,
# MAGIC         (
# MAGIC           case
# MAGIC             when carrier_curr = 'USD' then final_carrier_rate::float
# MAGIC             else final_carrier_rate::float / aljex_data.conversion_rate
# MAGIC           end
# MAGIC         ) as Expense_USD,
# MAGIC         Expense_CAD AS Carrier_Final_Rate_CAD,
# MAGIC         Expense_USD AS Carrier_Final_Rate_USD,
# MAGIC         Dim_date.period_year as Financial_Period_Year,
# MAGIC         null as GreenScreen_Rate_CAD,
# MAGIC         null as GreenScreen_Rate_USD,
# MAGIC         (
# MAGIC           case
# MAGIC             when cust_curr <> 'USD' then invoice_total::float
# MAGIC             else invoice_total::float * aljex_data.conversion_rate::float
# MAGIC           end
# MAGIC         ) as Invoice_Amount_CAD,
# MAGIC         (
# MAGIC           case
# MAGIC             when cust_curr = 'USD' then invoice_total::float
# MAGIC             else invoice_total::float /aljex_data.conversion_rate::float
# MAGIC           end
# MAGIC         ) as Invoice_Amount_USD,
# MAGIC          COALESCE(aljex_invoice.invoice_date::DATE, inv_date::DATE)::TIMESTAMP_NTZ AS Invoiced_Date,
# MAGIC         Lawson_ID,
# MAGIC         cust_curr AS Load_Currency,
# MAGIC         CONCAT(
# MAGIC           INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC         ) AS Load_Lane,
# MAGIC         UPPER(aljex_data.status) as Load_Status,
# MAGIC         Loadandunload_carrier_cad AS Load_Unload_Charges_Carrier_CAD,
# MAGIC         Loadandunload_carrier_usd AS Load_Unload_Charges_Carrier_USD,
# MAGIC         Loadandunload_Cust_CAD as Load_Unload_Charges_Customer_CAD,
# MAGIC         Loadandunload_Cust_USD as Load_Unload_Charges_Customer_USD,
# MAGIC         case
# MAGIC           when carrier_name is not null then '1'
# MAGIC           else '0'
# MAGIC         end as Load_flag,
# MAGIC         (
# MAGIC           coalesce(
# MAGIC             case
# MAGIC               when cust_curr = 'CAD' then invoice_total::float
# MAGIC               else invoice_total::float * aljex_data.conversion_rate::float
# MAGIC             end,
# MAGIC             0
# MAGIC           )
# MAGIC           - coalesce(
# MAGIC             case
# MAGIC               when carrier_curr = 'CAD' then final_carrier_rate::float
# MAGIC               else final_carrier_rate::float * aljex_data.conversion_rate
# MAGIC             end,
# MAGIC             0
# MAGIC           )
# MAGIC         ) AS Margin_CAD,
# MAGIC         (
# MAGIC           coalesce(
# MAGIC             case
# MAGIC               when cust_curr = 'USD' then invoice_total::float
# MAGIC               else invoice_total::float / aljex_data.conversion_rate::float
# MAGIC             end,
# MAGIC             0
# MAGIC           )
# MAGIC           - coalesce(
# MAGIC             case
# MAGIC               when carrier_curr = 'USD' then final_carrier_rate::float
# MAGIC               else final_carrier_rate::float / aljex_data.conversion_rate
# MAGIC             end,
# MAGIC             0
# MAGIC           )
# MAGIC         ) as Margin_USD,
# MAGIC         
# MAGIC         mr.Market_Buy_Rate / aljex_data.conversion_rate::float AS Market_Buy_Rate_CAD,
# MAGIC         mr.Market_Buy_Rate AS Market_Buy_Rate_USD,
# MAGIC         d.market AS Market_Destination,
# MAGIC         CONCAT(o.market, '>', d.market) AS Market_Lane,
# MAGIC         o.market as Market_Origin,
# MAGIC         Mc_Number,
# MAGIC         case miles Rlike '^\d+(.\d+)?$'
# MAGIC           when true then miles::float
# MAGIC           else 0
# MAGIC         end as Miles,
# MAGIC         Miscellaneous_Carrier_CAD AS Miscellaneous_Chargers_Carrier_CAD,
# MAGIC         Miscellaneous_Carrier_USD AS Miscellaneous_Chargers_Carrier_USD,
# MAGIC         Miscellaneous_Cust_CAD AS Miscellaneous_Chargers_Customers_CAD,
# MAGIC         Miscellaneous_Cust_USD AS Miscellaneous_Chargers_Customers_USD,
# MAGIC         Modee as Mode,
# MAGIC         case
# MAGIC           when delivered_date::date > aljex_master_dates.del_appt_date::date then 'Late'
# MAGIC           else 'OnTime'
# MAGIC         end as On_Time_Delivery,
# MAGIC         case
# MAGIC           when
# MAGIC             aljex_master_dates.Ship_Date::date > COALESCE(aljex_master_dates.pu_appt_date, aljex_master_dates.use_date::date)
# MAGIC           then
# MAGIC             'Late'
# MAGIC           else 'OnTime'
# MAGIC         end as On_Time_Pickup,
# MAGIC         Origin_City,
# MAGIC         Origin_State ,
# MAGIC         Origin_Zip,
# MAGIC         PermitsandCompliance_Carrier_CAD AS Permits_Compliance_Charges_Carrier_CAD,
# MAGIC         PermitsandCompliance_Carrier_USD as Permits_Compliance_Charges_Carrier_USD,
# MAGIC         PermitsandCompliance_cust_CAD AS Permits_Compliance_Charges_Customer_CAD,
# MAGIC         PermitsandCompliance_cust_USD AS Permits_Compliance_Charges_Customer_USD,
# MAGIC         aljex_master_dates.pu_appt_date::TIMESTAMP_NTZ AS Pickup_Appointment_Date,
# MAGIC         aljex_master_dates.Ship_Date::TIMESTAMP_NTZ AS Actual_Pickup_Date,
# MAGIC         aljex_master_dates.pu_out::TIMESTAMP_NTZ AS Pickup_EndDate,
# MAGIC         aljex_master_dates.pu_in::TIMESTAMP_NTZ AS Pickup_StartDate,
# MAGIC         aljex_data.ref_num AS PO_Number,
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(Booked_date AS DATE) < TRY_CAST(aljex_master_dates.Use_Date AS DATE) THEN 'PreBooked'
# MAGIC           ELSE 'SameDay'
# MAGIC         END AS PreBookStatus,
# MAGIC         (
# MAGIC           case
# MAGIC             when cust_curr = 'CAD' then invoice_total::float
# MAGIC             else invoice_total::float * aljex_data.conversion_rate::float
# MAGIC           end
# MAGIC         ) as Revenue_CAD,
# MAGIC         (
# MAGIC           case
# MAGIC             when cust_curr = 'USD' then invoice_total::float
# MAGIC             else invoice_total::float / aljex_data.conversion_rate::float
# MAGIC           end
# MAGIC         ) as Revenue_USD,
# MAGIC         null::date as Rolled_Date,  
# MAGIC         NULL as Sales_Rep_ID,      
# MAGIC         Sales_Rep AS Sales_Rep,
# MAGIC         aljex_master_dates.Use_Date AS Ship_Date,
# MAGIC         datediff(try_cast(aljex_master_dates.use_date as date), try_cast(aljex_master_dates.created_date as date)) as Days_ahead,
# MAGIC         sp.margin as Spot_Margin,
# MAGIC         sp.quoted_price as Spot_Revenue,  
# MAGIC         aljex_master_dates.created_date::DATE AS Tendered_Date,
# MAGIC         "ALJEX" as TMS_System,
# MAGIC         Trailer_Number,
# MAGIC         TransitandRouting_Carrier_CAD AS Transit_Routing_Chargers_Carrier_CAD,
# MAGIC         TransitandRouting_Carrier_USD as Transit_Routing_Chargers_Carrier_USD,
# MAGIC         TransitandRouting_Cust_CAD AS Transit_Routing_Chargers_Customer_CAD,
# MAGIC         TransitandRouting_Cust_USD AS Transit_Routing_Chargers_Customer_USD,
# MAGIC         Week_Num as Week_Number,
# MAGIC         tender_final.Tender_Source_Type
# MAGIC
# MAGIC     FROM aljex_data
# MAGIC     LEFT JOIN aljex_master_dates ON aljex_data.id::INT = aljex_master_dates.id::INT 
# MAGIC     LEFT JOIN financial_calendar ON COALESCE(delivered_date::DATE, pickup_date::DATE)::DATE = financial_calendar.date
# MAGIC     LEFT JOIN projection_invoicing ON aljex_data.id::INT = projection_invoicing.pro_num::INT
# MAGIC     LEFT JOIN customer_lookup ON LEFT(original_shipper, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC     LEFT JOIN aljex_invoice ON aljex_data.id = aljex_invoice.pro_number--::FLOAT
# MAGIC     LEFT JOIN new_office_lookup ON proj_load_office = new_office_lookup.old_office
# MAGIC     LEFT JOIN bronze.market_lookup o ON LEFT(origin_zip, 3) = o.pickup_zip
# MAGIC     LEFT JOIN bronze.market_lookup d ON LEFT(dest_zip, 3) = d.pickup_zip 
# MAGIC     LEFT JOIN aljex_spot_loads sp on aljex_data.id = sp.id -- Spot Perspective
# MAGIC     LEFT JOIN pricing_nfi_load_predictions mr on aljex_data.id = mr.MR_id
# MAGIC     LEFT JOIN analytics.dim_date ON pickup_date::DATE = analytics.dim_date.calendar_date::date
# MAGIC     LEFT JOIN canada_conversions ON pickup_date::DATE = canada_conversions.ship_date::DATE
# MAGIC     LEFT JOIN average_conversions ON 1=1
# MAGIC     LEFT JOIN superinsight.accessorial_carriers sac on aljex_data.id = sac.load_number and sac.tms = 'ALJEX'
# MAGIC     LEFT JOIN superinsight.accessorial_customers sacc on aljex_data.id = sacc.load_number and sacc.tms = 'ALJEX'
# MAGIC     left join bronze.date_range_two on aljex_master_dates.use_date::date = date_range_two.date_date::date
# MAGIC     left join tender_final on aljex_data.id = tender_final.id
# MAGIC     WHERE 
# MAGIC         key_c_user IS NOT NULL )
# MAGIC
# MAGIC   SELECT *, 
# MAGIC     SHA1(
# MAGIC   CONCAT(
# MAGIC     COALESCE(DW_Load_ID::string, ''),
# MAGIC     COALESCE(Load_ID::string, ''),
# MAGIC     COALESCE(Admin_Fees_Carrier_USD::string, ''),
# MAGIC     COALESCE(Admin_Fees_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Admin_Fees_Customer_USD::string, ''),
# MAGIC     COALESCE(Admin_Fees_Customer_CAD::string, ''),
# MAGIC     COALESCE(Billing_Status::string, ''),
# MAGIC     COALESCE(Actual_Delivered_Date::string, ''),
# MAGIC     COALESCE(Booking_Type::string, ''),
# MAGIC     COALESCE(Booked_Date::string, ''),
# MAGIC     COALESCE(Carrier_Fuel_Surcharge_CAD::string, ''),
# MAGIC     COALESCE(Carrier_Fuel_Surcharge_USD::string, ''),
# MAGIC     COALESCE(Carrier_Linehaul_CAD::string, ''),
# MAGIC     COALESCE(Carrier_Linehaul_USD::string, ''),
# MAGIC     COALESCE(Carrier_Name::string, ''),
# MAGIC     COALESCE(Carrier_New_Office::string, ''),
# MAGIC     COALESCE(Carrier_Old_Office::string, ''),
# MAGIC     COALESCE(Carrier_Other_Fees_CAD::string, ''),
# MAGIC     COALESCE(Carrier_Other_Fees_USD::string, ''),
# MAGIC     COALESCE(Carrier_ID::string, ''),
# MAGIC     COALESCE(Carrier_Rep_ID::string, ''),
# MAGIC     COALESCE(Carrier_Rep::string, ''),
# MAGIC     COALESCE(Combined_Load::string, ''),
# MAGIC     COALESCE(Consignee_Delivery_Address::string, ''),
# MAGIC     COALESCE(Consignee_Name::string, ''),
# MAGIC     COALESCE(Consignee_Pickup_Address::string, ''),
# MAGIC     COALESCE(Customer_Fuel_Surcharge_USD::string, ''),
# MAGIC     COALESCE(Customer_Fule_Surcharge_CAD::string, ''),
# MAGIC     COALESCE(Customer_Linehaul_CAD::string, ''),
# MAGIC     COALESCE(Customer_Linehaul_USD::string, ''),
# MAGIC     COALESCE(Customer_ID::string, ''),
# MAGIC     COALESCE(Customer_Master::string, ''),
# MAGIC     COALESCE(Customer_Name::string, ''),
# MAGIC     COALESCE(Customer_New_Office::string, ''),
# MAGIC     COALESCE(Customer_Old_Office::string, ''),
# MAGIC     COALESCE(Customer_Other_Fees_CAD::string, ''),
# MAGIC     COALESCE(Customer_Other_Fees_USD::string, ''),
# MAGIC     COALESCE(Customer_Rep_ID::string, ''),
# MAGIC     COALESCE(Customer_Rep::string, ''),
# MAGIC     COALESCE(Days_Ahead::string, ''),
# MAGIC     COALESCE(Bounced_Date::string, ''),
# MAGIC     COALESCE(Cancelled_Date::string, ''),
# MAGIC     COALESCE(Delivery_Pickup_Charges_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Delivery_Pickup_Charges_Carrier_USD::string, ''),
# MAGIC     COALESCE(Delivery_Pickup_Charges_Customer_CAD::string, ''),
# MAGIC     COALESCE(Delivery_Pickup_Charges_Customer_USD::string, ''),
# MAGIC     COALESCE(Delivery_Appointment_Date::string, ''),
# MAGIC     COALESCE(Delivery_EndDate::string, ''),
# MAGIC     COALESCE(Destination_City::string, ''),
# MAGIC     COALESCE(Destination_State::string, ''),
# MAGIC     COALESCE(Destination_Zip::string, ''),
# MAGIC     COALESCE(DOT_Number::string, ''),
# MAGIC     COALESCE(Delivery_StartDate::string, ''),
# MAGIC     COALESCE(Equipment::string, ''),
# MAGIC     COALESCE(Equipment_Vehicle_Charges_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Equipment_Vehicle_Charges_Carrier_USD::string, ''),
# MAGIC     COALESCE(Equipment_Vehicle_Charges_Customer_CAD::string, ''),
# MAGIC     COALESCE(Equipment_Vehicle_Charges_Customer_USD::string, ''),
# MAGIC     COALESCE(Expense_CAD::string, ''),
# MAGIC     COALESCE(Expense_USD::string, ''),
# MAGIC     COALESCE(Financial_Period_Year::string, ''),
# MAGIC     COALESCE(GreenScreen_Rate_CAD::string, ''),
# MAGIC     COALESCE(GreenScreen_Rate_USD::string, ''),
# MAGIC     COALESCE(Invoice_Amount_CAD::string, ''),
# MAGIC     COALESCE(Invoice_Amount_USD::string, ''),
# MAGIC     COALESCE(Invoiced_Date::string, ''),
# MAGIC     COALESCE(Lawson_ID::string, ''),
# MAGIC     COALESCE(Load_Currency::string, ''),
# MAGIC     COALESCE(Load_Flag::string, ''),
# MAGIC     COALESCE(Load_Lane::string, ''),
# MAGIC     COALESCE(Load_Status::string, ''),
# MAGIC     COALESCE(Load_Unload_Charges_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Load_Unload_Charges_Carrier_USD::string, ''),
# MAGIC     COALESCE(Load_Unload_Charges_Customer_CAD::string, ''),
# MAGIC     COALESCE(Load_Unload_Charges_Customer_USD::string, ''),
# MAGIC     COALESCE(Margin_CAD::string, ''),
# MAGIC     COALESCE(Margin_USD::string, ''),
# MAGIC     COALESCE(Market_Buy_Rate_CAD::string, ''),
# MAGIC     COALESCE(Market_Buy_Rate_USD::string, ''),
# MAGIC     COALESCE(Market_Destination::string, ''),
# MAGIC     COALESCE(Market_Lane::string, ''),
# MAGIC     COALESCE(Market_Origin::string, ''),
# MAGIC     COALESCE(MC_Number::string, ''),
# MAGIC     COALESCE(Miles::string, ''),
# MAGIC     COALESCE(Miscellaneous_Chargers_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Miscellaneous_Chargers_Carrier_USD::string, ''),
# MAGIC     COALESCE(Miscellaneous_Chargers_Customers_CAD::string, ''),
# MAGIC     COALESCE(Miscellaneous_Chargers_Customers_USD::string, ''),
# MAGIC     COALESCE(Mode::string, ''),
# MAGIC     COALESCE(On_Time_Delivery::string, ''),
# MAGIC     COALESCE(On_Time_Pickup::string, ''),
# MAGIC     COALESCE(Origin_City::string, ''),
# MAGIC     COALESCE(Origin_State::string, ''),
# MAGIC     COALESCE(Origin_Zip::string, ''),
# MAGIC     COALESCE(Permits_Compliance_Charges_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Permits_Compliance_Charges_Carrier_USD::string, ''),
# MAGIC     COALESCE(Permits_Compliance_Charges_Customer_CAD::string, ''),
# MAGIC     COALESCE(Permits_Compliance_Charges_Customer_USD::string, ''),
# MAGIC     COALESCE(Pickup_Appointment_Date::string, ''),
# MAGIC     COALESCE(Actual_Pickup_Date::string, ''),
# MAGIC     COALESCE(Pickup_EndDate::string, ''),
# MAGIC     COALESCE(Pickup_StartDate::string, ''),
# MAGIC     COALESCE(PO_Number::string, ''),
# MAGIC     COALESCE(PreBookStatus::string, ''),
# MAGIC     COALESCE(Revenue_CAD::string, ''),
# MAGIC     COALESCE(Revenue_USD::string, ''),
# MAGIC     COALESCE(Rolled_Date::string, ''),
# MAGIC     COALESCE(Sales_Rep_ID::string, ''),
# MAGIC     COALESCE(Sales_Rep::string, ''),
# MAGIC     COALESCE(Ship_Date::string, ''),
# MAGIC     COALESCE(Spot_Margin::string, ''),
# MAGIC     COALESCE(Spot_Revenue::string, ''),
# MAGIC     COALESCE(Tender_Source_Type::string, ''),
# MAGIC     COALESCE(Tendered_Date::string, ''),
# MAGIC     COALESCE(TMS_System::string, ''),
# MAGIC     COALESCE(Trailer_Number::string, ''),
# MAGIC     COALESCE(Transit_Routing_Chargers_Carrier_CAD::string, ''),
# MAGIC     COALESCE(Transit_Routing_Chargers_Carrier_USD::string, ''),
# MAGIC     COALESCE(Transit_Routing_Chargers_Customer_CAD::string, ''),
# MAGIC     COALESCE(Transit_Routing_Chargers_Customer_USD::string, ''),
# MAGIC     COALESCE(Week_Number::string, ''),
# MAGIC     COALESCE(Carrier_Final_Rate_CAD::string, ''),
# MAGIC     COALESCE(Carrier_Final_Rate_USD::string, '')
# MAGIC   )
# MAGIC ) AS HashKey FROM Final;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Aljex (
# MAGIC     DW_Load_ID  VARCHAR(255) NOT NULL PRIMARY KEY,
# MAGIC     Load_Number BIGINT NOT NULL ,
# MAGIC     Admin_Fees_Carrier_USD DECIMAL(10,2),
# MAGIC     Admin_Fees_Carrier_CAD DECIMAL(10,2),
# MAGIC     Admin_Fees_Customer_USD DECIMAL(10,2),
# MAGIC     Admin_Fees_Customer_CAD DECIMAL(10,2),
# MAGIC     Billing_Status VARCHAR(255),
# MAGIC     Actual_Delivered_Date TIMESTAMP_NTZ,
# MAGIC     Booking_Type VARCHAR(255),
# MAGIC     Booked_Date TIMESTAMP_NTZ,
# MAGIC     Carrier_Fuel_Surcharge_CAD DECIMAL(10,2),
# MAGIC     Carrier_Fuel_Surcharge_USD DECIMAL(10,2),
# MAGIC     Carrier_Linehaul_CAD DECIMAL(10,2),
# MAGIC     Carrier_Linehaul_USD DECIMAL(10,2),
# MAGIC     Carrier_Name VARCHAR(255),
# MAGIC     Carrier_New_Office VARCHAR(255),
# MAGIC     Carrier_Old_Office VARCHAR(255),
# MAGIC     Carrier_Other_Fees_CAD DECIMAL(10,2),
# MAGIC     Carrier_Other_Fees_USD DECIMAL(10,2),
# MAGIC     Carrier_ID VARCHAR(255),
# MAGIC     Carrier_Rep_ID VARCHAR(255),
# MAGIC     Carrier_Rep VARCHAR(255),
# MAGIC     Combined_Load VARCHAR(255),
# MAGIC     Consignee_Delivery_Address VARCHAR(255),
# MAGIC     Consignee_Name VARCHAR(255),
# MAGIC     Consignee_Pickup_Address VARCHAR(255),
# MAGIC     Customer_Fuel_Surcharge_USD DECIMAL(10,2),
# MAGIC     Customer_Fule_Surcharge_CAD DECIMAL(10,2),
# MAGIC     Customer_Linehaul_CAD DECIMAL(10,2),
# MAGIC     Customer_Linehaul_USD DECIMAL(10,2),
# MAGIC     Customer_ID VARCHAR(255),
# MAGIC     Customer_Master VARCHAR(255),
# MAGIC     Customer_Name VARCHAR(255),
# MAGIC     Customer_New_Office VARCHAR(255),
# MAGIC     Customer_Old_Office VARCHAR(255),
# MAGIC     Customer_Other_Fees_CAD DECIMAL(10,2),
# MAGIC     Customer_Other_Fees_USD DECIMAL(10,2),
# MAGIC     Customer_Rep_ID VARCHAR(255),
# MAGIC     Customer_Rep VARCHAR(255),
# MAGIC     Days_Ahead VARCHAR(255),
# MAGIC     Bounced_Date TIMESTAMP_NTZ,
# MAGIC     Cancelled_Date TIMESTAMP_NTZ,
# MAGIC     Delivery_Pickup_Charges_Carrier_CAD DECIMAL(10,2),
# MAGIC     Delivery_Pickup_Charges_Carrier_USD DECIMAL(10,2),
# MAGIC     Delivery_Pickup_Charges_Customer_CAD DECIMAL(10,2),
# MAGIC     Delivery_Pickup_Charges_Customer_USD DECIMAL(10,2),
# MAGIC     Delivery_Appointment_Date TIMESTAMP_NTZ,
# MAGIC     Delivery_EndDate TIMESTAMP_NTZ,
# MAGIC     Destination_City VARCHAR(255),
# MAGIC     Destination_State VARCHAR(255),
# MAGIC     Destination_Zip VARCHAR(255),
# MAGIC     DOT_Number VARCHAR(255),
# MAGIC     Delivery_StartDate TIMESTAMP_NTZ,
# MAGIC     Equipment VARCHAR(255),
# MAGIC     Equipment_Vehicle_Charges_Carrier_CAD DECIMAL(10,2),
# MAGIC     Equipment_Vehicle_Charges_Carrier_USD DECIMAL(10,2),
# MAGIC     Equipment_Vehicle_Charges_Customer_CAD DECIMAL(10,2),
# MAGIC     Equipment_Vehicle_Charges_Customer_USD DECIMAL(10,2),
# MAGIC     Expense_CAD DECIMAL(10,2),
# MAGIC     Expense_USD DECIMAL(10,2),
# MAGIC     Financial_Period_Year VARCHAR(255),
# MAGIC     GreenScreen_Rate_CAD DECIMAL(10,2),
# MAGIC     GreenScreen_Rate_USD DECIMAL(10,2),
# MAGIC     Invoice_Amount_CAD DECIMAL(10,2),
# MAGIC     Invoice_Amount_USD DECIMAL(10,2),
# MAGIC     Invoiced_Date TIMESTAMP_NTZ,
# MAGIC     Lawson_ID VARCHAR(50),
# MAGIC     Load_Currency VARCHAR(255),
# MAGIC     Load_Flag VARCHAR(255),
# MAGIC     Load_Lane VARCHAR(255),
# MAGIC     Load_Status VARCHAR(255),
# MAGIC     Load_Unload_Charges_Carrier_CAD DECIMAL(10,2),
# MAGIC     Load_Unload_Charges_Carrier_USD DECIMAL(10,2),
# MAGIC     Load_Unload_Charges_Customer_CAD DECIMAL(10,2),
# MAGIC     Load_Unload_Charges_Customer_USD DECIMAL(10,2),
# MAGIC     Margin_CAD DECIMAL(10,2),
# MAGIC     Margin_USD DECIMAL(10,2),
# MAGIC     Market_Buy_Rate_CAD DECIMAL(10,2),
# MAGIC     Market_Buy_Rate_USD DECIMAL(10,2),
# MAGIC     Market_Destination VARCHAR(255),
# MAGIC     Market_Lane VARCHAR(255),
# MAGIC     Market_Origin VARCHAR(255),
# MAGIC     MC_Number VARCHAR(255),
# MAGIC     Miles VARCHAR(255),
# MAGIC     Miscellaneous_Chargers_Carrier_CAD DECIMAL(10,2),
# MAGIC     Miscellaneous_Chargers_Carrier_USD DECIMAL(10,2),
# MAGIC     Miscellaneous_Chargers_Customers_CAD DECIMAL(10,2),
# MAGIC     Miscellaneous_Chargers_Customers_USD DECIMAL(10,2),
# MAGIC     Mode VARCHAR(255),
# MAGIC     On_Time_Delivery VARCHAR(255),
# MAGIC     On_Time_Pickup VARCHAR(255),
# MAGIC     Origin_City VARCHAR(255),
# MAGIC     Origin_State VARCHAR(255),
# MAGIC     Origin_Zip VARCHAR(255),
# MAGIC     Permits_Compliance_Charges_Carrier_CAD DECIMAL(10,2),
# MAGIC     Permits_Compliance_Charges_Carrier_USD DECIMAL(10,2),
# MAGIC     Permits_Compliance_Charges_Customer_CAD DECIMAL(10,2),
# MAGIC     Permits_Compliance_Charges_Customer_USD DECIMAL(10,2),
# MAGIC     Pickup_Appointment_Date TIMESTAMP_NTZ,
# MAGIC     Actual_Pickup_Date TIMESTAMP_NTZ,
# MAGIC     Pickup_EndDate TIMESTAMP_NTZ,
# MAGIC     Pickup_StartDate TIMESTAMP_NTZ,
# MAGIC     PO_Number VARCHAR(255),
# MAGIC     PreBookStatus VARCHAR(255),
# MAGIC     Revenue_CAD DECIMAL(10,2),
# MAGIC     Revenue_USD DECIMAL(10,2),
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
# MAGIC     Transit_Routing_Chargers_Carrier_CAD DECIMAL(10,2),
# MAGIC     Transit_Routing_Chargers_Carrier_USD DECIMAL(10,2),
# MAGIC     Transit_Routing_Chargers_Customer_CAD DECIMAL(10,2),
# MAGIC     Transit_Routing_Chargers_Customer_USD DECIMAL(10,2),
# MAGIC     Week_Number VARCHAR(10),
# MAGIC     Carrier_Final_Rate_CAD DECIMAL(10,2),
# MAGIC     Carrier_Final_Rate_USD DECIMAL(10,2),
# MAGIC     Hashkey STRING NOT NULL,
# MAGIC     Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Created_By VARCHAR(50) NOT NULL,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Last_Modified_By VARCHAR(50) NOT NULL,
# MAGIC     Is_Deleted SMALLINT NOT NULL
# MAGIC );