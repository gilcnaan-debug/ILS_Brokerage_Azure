# Databricks notebook source
# MAGIC %md
# MAGIC ## Canned Reprorts
# MAGIC * **Description:** To Replicate Metabase Canned Reports for Niffy
# MAGIC * **Created Date:** 11/25/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 11/25/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating View

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC  SET ansi_mode = false;
# MAGIC create or replace view bronze.JohnnyH_Summary_Report_Genai
# MAGIC  as( 
# MAGIC   WITH final_carrier_rate AS (
# MAGIC   SELECT
# MAGIC   p1.id,
# MAGIC   CAST(pickup_date AS DATE) AS pickup_date,
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_line_haul LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_line_haul RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_line_haul AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate1 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate1 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate1 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate2 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate2 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate2 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate3 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate3 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate3 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate4 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate4 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate4 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate5 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate5 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate5 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate6 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate6 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate7 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate7 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate7 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate8 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate8 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate8 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) AS final_carrier_rate
# MAGIC FROM projection_load_1 p1 
# MAGIC LEFT JOIN projection_load_2 p2 ON p1.id = p2.id
# MAGIC LEFT JOIN analytics.dim_financial_calendar ON CAST(p1.pickup_date AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE)
# MAGIC WHERE 1=1
# MAGIC   AND p2.status NOT LIKE '%VOID%'
# MAGIC ),
# MAGIC
# MAGIC for_mc AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     dot_num,
# MAGIC     MAX(mc_num) AS `for_mc`
# MAGIC   FROM projection_carrier
# MAGIC   GROUP BY dot_num
# MAGIC ),
# MAGIC
# MAGIC new_pu_appt AS (
# MAGIC   SELECT DISTINCT
# MAGIC     b.relay_reference_number,
# MAGIC     CAST(p.appointment_datetime AS TIMESTAMP) AS `pickup_proj_appt_datetime`,
# MAGIC     CAST(s.appointment_datetime AS TIMESTAMP) AS `planning_appt_datetime`,
# MAGIC     CAST(s.window_start_datetime AS TIMESTAMP) AS `window_start_datetime`,
# MAGIC     CAST(s.window_end_datetime AS TIMESTAMP) AS `window_end_datetime`,
# MAGIC     COALESCE(
# MAGIC       CAST(s.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(s.window_start_datetime AS TIMESTAMP),
# MAGIC       CAST(s.window_end_datetime AS TIMESTAMP),
# MAGIC       CAST(p.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(b.ready_date AS DATE)
# MAGIC     ) AS `new_pu_appt`
# MAGIC   FROM booking_projection b
# MAGIC   LEFT JOIN pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC     AND b.first_shipper_name = p.shipper_name
# MAGIC     AND p.sequence_number = 1
# MAGIC   LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC     AND s.stop_type = 'pickup'
# MAGIC     AND s.sequence_number = 1
# MAGIC     AND s.`removed?` = 'false'
# MAGIC   WHERE 1=1
# MAGIC     AND b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
# MAGIC max_time AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     relay_reference_number,
# MAGIC     MAX(last_update_date_time) AS `max_time`
# MAGIC   FROM truckload_projection
# MAGIC   GROUP BY relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC relay_status AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     truckload_projection.relay_reference_number,
# MAGIC     last_update_date_time,
# MAGIC     last_update_event_name AS `relay_status`
# MAGIC   FROM truckload_projection
# MAGIC   JOIN max_time ON truckload_projection.relay_reference_number = max_time.relay_reference_number
# MAGIC     AND truckload_projection.last_update_date_time = max_time.max_time
# MAGIC ),
# MAGIC
# MAGIC to_get_aljex_am AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     shipper,
# MAGIC     CASE WHEN master_customer_name IS NULL THEN shipper ELSE master_customer_name END AS `mastername`,
# MAGIC     COALESCE(aljex_user_report_listing.full_name, srv_rep) AS `aljex_rep`,
# MAGIC     aljex_user_report_listing.full_name,
# MAGIC     srv_rep,
# MAGIC     MAX(pickup_date) AS `max_date`,
# MAGIC     COUNT(DISTINCT p.id) AS `volume`,
# MAGIC     RANK() OVER (PARTITION BY CASE WHEN master_customer_name IS NULL THEN shipper ELSE master_customer_name END ORDER BY MAX(pickup_date) DESC) AS `ranker`
# MAGIC   FROM projection_load_1 p 
# MAGIC   left join projection_load_2 p2 on p.id = p2.id
# MAGIC   LEFT JOIN customer_lookup ON LEFT(shipper, 32) = LEFT(aljex_customer_name, 32)
# MAGIC   LEFT JOIN new_office_lookup ON p.office = new_office_lookup.old_office
# MAGIC   LEFT JOIN aljex_user_report_listing ON srv_rep = aljex_user_report_listing.aljex_id
# MAGIC   WHERE 1=1
# MAGIC   -- [[AND {{office}}]]
# MAGIC   -- AND srv_rep IS NOT NULL
# MAGIC   GROUP BY shipper, master_customer_name, srv_rep, aljex_user_report_listing.full_name
# MAGIC   ORDER BY shipper,max_date DESC
# MAGIC ),
# MAGIC
# MAGIC aljex_am AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     mastername,
# MAGIC     aljex_rep AS `aljex_am`
# MAGIC   FROM to_get_aljex_am
# MAGIC   WHERE 1=1 AND ranker = 1
# MAGIC   ORDER BY mastername
# MAGIC ),
# MAGIC
# MAGIC relay_am AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     profit_center,
# MAGIC     customer_name,
# MAGIC     CASE WHEN master_customer_name IS NULL THEN customer_name ELSE master_customer_name END AS `mastername`,
# MAGIC     relay_users.full_name AS `relay_am`,
# MAGIC     s.full_name AS `relay_sales`
# MAGIC   FROM customer_profile_projection
# MAGIC   LEFT JOIN customer_lookup ON LEFT(customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC   LEFT JOIN relay_users ON customer_profile_projection.primary_relay_user_id = relay_users.user_id AND relay_users.`active?` = 'true'
# MAGIC   LEFT JOIN relay_users s ON customer_profile_projection.sales_relay_user_id = s.user_id AND s.`active?` = 'true'
# MAGIC   LEFT JOIN new_office_lookup ON profit_center = new_office_lookup.old_office
# MAGIC   WHERE 1=1
# MAGIC     AND status = 'published'
# MAGIC ),
# MAGIC
# MAGIC to_get_avg AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     ship_date,
# MAGIC     conversion AS `us_to_cad`,
# MAGIC     us_to_cad AS `cad_to_us`
# MAGIC   FROM canada_conversions
# MAGIC   ORDER BY ship_date DESC
# MAGIC   LIMIT 7
# MAGIC ),
# MAGIC
# MAGIC average_conversions AS (
# MAGIC   SELECT  
# MAGIC     AVG(us_to_cad) AS `avg_us_to_cad`,
# MAGIC     AVG(cad_to_us) AS `avg_cad_to_us`
# MAGIC   FROM to_get_avg
# MAGIC ),
# MAGIC system_union AS (
# MAGIC   SELECT DISTINCT
# MAGIC     'ALJEX' AS `systemm`,
# MAGIC     tag_created_by,
# MAGIC     k.full_name AS `booked_by`,
# MAGIC     CONCAT(CAST(tag_creation_date AS DATE), 'T', CAST(tag_creation_time AS TIMESTAMP)) AS `create_date`,
# MAGIC     CAST(p.id AS FLOAT) AS `loadnum`,
# MAGIC     NULL AS booking_id,  -- NULL with explicit type FLOAT
# MAGIC     p.office AS `office`,
# MAGIC     CAST(p.pickup_date AS DATE) AS `shipdate`,
# MAGIC     CASE
# MAGIC       WHEN customer_lookup.master_customer_name IS NULL THEN shipper
# MAGIC       ELSE customer_lookup.master_customer_name
# MAGIC     END AS `mastername`,
# MAGIC     aljex_am.aljex_am AS `system_am`,
# MAGIC     relay_am.relay_am,
# MAGIC     s.full_name AS `system_sales_rep`,
# MAGIC     relay_am.relay_sales,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     pickup_zip_code AS `origin_zip`,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     consignee_zip_code AS `dest_zip`,
# MAGIC     CAST(invoice_total AS FLOAT) AS `revenue`,
# MAGIC     CASE 
# MAGIC             WHEN (c.name LIKE '%CAD' OR c.id IN ('300023338','300022987') or canada_carriers.canada_dot is not null) THEN 
# MAGIC                 CASE 
# MAGIC                     WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE (final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                     ELSE TRY_DIVIDE (final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC             ELSE final_carrier_rate::FLOAT 
# MAGIC         END AS `expense`,
# MAGIC     INITCAP(c.name) AS `carrier_name`,
# MAGIC     CAST(c.dot_num AS STRING) AS `dot_num`,
# MAGIC     CAST(c.mc_num AS STRING) AS `mc_num`,
# MAGIC     equipment AS `equip`,
# MAGIC     p2.status AS `status`,
# MAGIC     CAST(miles AS FLOAT),
# MAGIC     aljex_mode_types.equipment_mode,
# MAGIC     (
# MAGIC       (CASE WHEN ps1 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps2 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps3 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps4 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps5 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps6 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps7 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps8 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps9 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps10 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps11 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps12 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps13 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps14 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps15 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps16 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps17 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps18 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps19 = 'S' THEN 1 ELSE 0 END) 
# MAGIC     --   (CASE WHEN ps20 = 'S' THEN 1 ELSE 0 END)
# MAGIC     )::FLOAT + 1  AS `num_drops`
# MAGIC   FROM projection_load_1 p 
# MAGIC   left join projection_load_2 p2 on p.id = p2.id
# MAGIC   LEFT JOIN final_carrier_rate ON p.id = final_carrier_rate.id 
# MAGIC   LEFT JOIN canada_conversions ON CAST(p.pickup_date AS DATE) = CAST(canada_conversions.ship_date AS DATE)
# MAGIC   LEFT JOIN customer_lookup ON LEFT(shipper, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC   LEFT JOIN analytics.dim_financial_calendar ON CAST(p.pickup_date AS DATE) = analytics.dim_financial_calendar.date 
# MAGIC   LEFT JOIN aljex_mode_types ON p.equipment = aljex_mode_types.equipment_type
# MAGIC   LEFT JOIN projection_carrier c ON p.carrier_id = c.id 
# MAGIC   JOIN average_conversions ON 1=1 
# MAGIC   LEFT JOIN aljex_am_by_acct ON CAST(p2.customer_id AS FLOAT) = CAST(aljex_am_by_acct.account_num AS FLOAT)
# MAGIC   LEFT JOIN relay_am ON customer_lookup.master_customer_name = relay_am.mastername
# MAGIC   LEFT JOIN canada_carriers ON CAST(p.carrier_id AS STRING) = CAST(canada_carriers.canada_dot AS STRING)
# MAGIC   LEFT JOIN aljex_user_report_listing k ON p.act_disp = k.aljex_id
# MAGIC   LEFT JOIN aljex_am ON customer_lookup.master_customer_name = aljex_am.mastername 
# MAGIC   LEFT JOIN aljex_user_report_listing ON COALESCE(relay_am, aljex_am) = aljex_user_report_listing.full_name 
# MAGIC   LEFT JOIN aljex_user_report_listing s ON p2.sales_rep = s.sales_rep
# MAGIC   WHERE 1=1 
# MAGIC     AND p2.status NOT LIKE 'VOID%'
# MAGIC     AND tag_creation_date NOT LIKE '%:%'
# MAGIC     AND p.office NOT IN ('10', '34', '51', '54', '61', '62', '63', '64', '74')
# MAGIC   
# MAGIC   UNION 
# MAGIC   
# MAGIC   
# MAGIC   SELECT DISTINCT
# MAGIC     'ALJEX',
# MAGIC     tag_created_by,
# MAGIC     k.full_name,
# MAGIC     CONCAT(CAST(tag_creation_date AS DATE), 'T', CAST(tag_creation_time AS TIMESTAMP)) AS `create_date`,
# MAGIC     CAST(p.id AS FLOAT),
# MAGIC     NULL AS `booking_id`,
# MAGIC     p.office,
# MAGIC     CAST(p.pickup_date AS DATE),
# MAGIC     CASE
# MAGIC       WHEN customer_lookup.master_customer_name IS NULL THEN shipper
# MAGIC       ELSE customer_lookup.master_customer_name
# MAGIC     END AS `mastername`,
# MAGIC     aljex_am.aljex_am,
# MAGIC     relay_am.relay_am,
# MAGIC     s.full_name AS `system_sales_rep`,
# MAGIC     relay_am.relay_sales,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     pickup_zip_code AS `origin_zip`,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     consignee_zip_code AS `dest_zip`,
# MAGIC     CASE
# MAGIC       WHEN cad_currency.currency_type = 'USD' THEN CAST(invoice_total AS FLOAT)
# MAGIC       WHEN aljex_customer_profiles.cust_country = 'USD' THEN CAST(invoice_total AS FLOAT)
# MAGIC       ELSE CAST(invoice_total AS FLOAT) / (
# MAGIC         CASE
# MAGIC           WHEN canada_conversions.conversion IS NULL THEN average_conversions.avg_us_to_cad
# MAGIC           ELSE CAST(canada_conversions.conversion AS FLOAT)
# MAGIC         END
# MAGIC       )
# MAGIC     END AS `revenue`,
# MAGIC 	 CASE 
# MAGIC             WHEN c.name LIKE '%CAD'  THEN 
# MAGIC                 CASE 
# MAGIC                     WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                     ELSE TRY_DIVIDE (final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC 		    WHEN canada_carriers.canada_dot IS NOT NULL THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC 			WHEN c.name RLIKE 'CANADIAN R%' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC 			WHEN c.name RLIKE '%CAN' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC 			WHEN c.name RLIKE '%(CAD)%' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC             
# MAGIC 			WHEN c.name RLIKE '%-C' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC  
# MAGIC 			WHEN c.name RLIKE '%- C%' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC       ELSE final_carrier_rate
# MAGIC     END AS `expense`,
# MAGIC     INITCAP(c.name) AS `carrier_name`,
# MAGIC     CAST(c.dot_num AS STRING) AS `dot_num`,
# MAGIC     CAST(c.mc_num AS STRING) AS `mc_num`,
# MAGIC     equipment,
# MAGIC     p2.status,
# MAGIC     CAST(miles AS FLOAT),
# MAGIC     aljex_mode_types.equipment_mode,
# MAGIC     (
# MAGIC       (CASE WHEN ps1 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps2 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps3 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps4 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps5 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps6 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps7 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps8 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps9 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps10 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps11 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps12 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps13 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps14 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps15 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps16 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps17 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps18 = 'S' THEN 1 ELSE 0 END) +
# MAGIC       (CASE WHEN ps19 = 'S' THEN 1 ELSE 0 END) 
# MAGIC     --   (CASE WHEN ps20 = 'S' THEN 1 ELSE 0 END)
# MAGIC     )::FLOAT + 1 AS `num_drops`
# MAGIC   FROM projection_load_1 p 
# MAGIC   left join projection_load_2 p2 on p.id = p2.id 
# MAGIC   LEFT JOIN canada_conversions ON CAST(p.pickup_date AS DATE) = CAST(canada_conversions.ship_date AS DATE)
# MAGIC   LEFT JOIN customer_lookup ON LEFT(shipper, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC   LEFT JOIN analytics.dim_financial_calendar ON CAST(p.pickup_date AS DATE) = analytics.dim_financial_calendar.date
# MAGIC   LEFT JOIN cad_currency ON CAST(p.id AS STRING) = CAST(cad_currency.pro_number AS STRING)
# MAGIC   LEFT JOIN final_carrier_rate ON p.id = final_carrier_rate.id 
# MAGIC   LEFT JOIN aljex_mode_types ON p.equipment = aljex_mode_types.equipment_type
# MAGIC   LEFT JOIN projection_carrier c ON p.carrier_id = c.id 
# MAGIC   LEFT JOIN aljex_user_report_listing s ON p2.sales_rep = s.sales_rep
# MAGIC   LEFT JOIN canada_carriers ON CAST(p.carrier_id AS STRING) = CAST(canada_carriers.canada_dot AS STRING)
# MAGIC   LEFT JOIN aljex_user_report_listing k ON p.act_disp = k.aljex_id
# MAGIC   LEFT JOIN relay_am ON customer_lookup.master_customer_name = relay_am.mastername
# MAGIC   JOIN average_conversions ON 1=1 
# MAGIC   LEFT JOIN aljex_am ON customer_lookup.master_customer_name = aljex_am.mastername
# MAGIC   LEFT JOIN aljex_customer_profiles ON CAST(p2.customer_id AS STRING) = CAST(aljex_customer_profiles.cust_id AS STRING)
# MAGIC   LEFT JOIN aljex_user_report_listing ON COALESCE(relay_am, aljex_am) = aljex_user_report_listing.full_name
# MAGIC   WHERE 1=1 
# MAGIC     AND p2.status NOT LIKE 'VOID%'
# MAGIC     AND tag_creation_date NOT LIKE '%:%'
# MAGIC     AND p.office IN ('10', '34', '51', '54', '61', '62', '63', '64', '74')
# MAGIC   UNION
# MAGIC   
# MAGIC   SELECT DISTINCT
# MAGIC     'RELAY',
# MAGIC     t.full_name AS `tag_created_by`,
# MAGIC     b.booked_by_name AS `booked_by`,
# MAGIC     CAST(tendering_acceptance.accepted_at AS TIMESTAMP),
# MAGIC     CAST(b.relay_reference_number AS FLOAT),
# MAGIC     CAST(b.booking_id AS FLOAT),
# MAGIC     r.profit_center,
# MAGIC     CAST(new_pu_appt.new_pu_appt AS DATE) AS `new_pu_appt_date`,
# MAGIC     CASE
# MAGIC       WHEN customer_lookup.master_customer_name IS NULL THEN r.customer_name
# MAGIC       ELSE master_customer_name
# MAGIC     END AS `mastername`,
# MAGIC     am.full_name,
# MAGIC     am.full_name,
# MAGIC     s.full_name,
# MAGIC     s.full_name,
# MAGIC     b.first_shipper_city,
# MAGIC     b.first_shipper_state,
# MAGIC     b.first_shipper_zip,
# MAGIC     b.receiver_city,
# MAGIC     b.receiver_state,
# MAGIC     b.receiver_zip,
# MAGIC     (
# MAGIC       (COALESCE(m.fuel_surcharge_amount, 0.00) / 100) +
# MAGIC       (COALESCE(m.linehaul_amount, 0.00) / 100) +
# MAGIC       (COALESCE(m.accessorial_amount, 0.00) / 100)
# MAGIC     ) AS `revenue`,
# MAGIC     b.booked_total_carrier_rate_amount / 100 AS `expense`,
# MAGIC     carrier_projection.carrier_name,
# MAGIC     CAST(carrier_projection.dot_number AS STRING),
# MAGIC     CAST(for_mc.for_mc AS STRING),
# MAGIC     CASE
# MAGIC       WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'V'
# MAGIC       WHEN tendering_service_line.service_line_type = 'flatbed' THEN 'F'
# MAGIC       WHEN tendering_service_line.service_line_type = 'refrigerated' THEN 'R'
# MAGIC     END AS `equip`,
# MAGIC     relay_status.relay_status,
# MAGIC     CAST(distance_projection.total_distance AS FLOAT),
# MAGIC     'TL',
# MAGIC     b.delivery_count
# MAGIC   FROM booking_projection b 
# MAGIC   LEFT JOIN tendering_acceptance ON b.relay_reference_number = tendering_acceptance.relay_reference_number
# MAGIC   LEFT JOIN relay_users t ON CAST(tendering_acceptance.accepted_by AS FLOAT) = t.user_id AND t.`active?` = 'true'
# MAGIC   LEFT JOIN customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC   LEFT JOIN new_pu_appt ON b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC   LEFT JOIN customer_lookup ON LEFT(r.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC   LEFT JOIN carrier_projection ON b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC   LEFT JOIN tendering_service_line ON b.relay_reference_number = tendering_service_line.relay_reference_number
# MAGIC   LEFT JOIN distance_projection ON b.relay_reference_number = distance_projection.relay_reference_number
# MAGIC   LEFT JOIN analytics.dim_financial_calendar ON CAST(new_pu_appt.new_pu_appt AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE)
# MAGIC   LEFT JOIN relay_users s ON r.sales_relay_user_id = s.user_id AND s.`active?` = 'true'
# MAGIC   LEFT JOIN relay_users am ON r.primary_relay_user_id = am.user_id AND am.`active?` = 'true'
# MAGIC   LEFT JOIN customer_money m ON b.relay_reference_number = m.relay_reference_number
# MAGIC   LEFT JOIN for_mc ON CAST(carrier_projection.dot_number AS STRING) = CAST(for_mc.dot_num AS STRING)
# MAGIC   LEFT JOIN aljex_user_report_listing ON am.full_name = aljex_user_report_listing.full_name 
# MAGIC   LEFT JOIN relay_status ON b.relay_reference_number = relay_status.relay_reference_number
# MAGIC   WHERE 1=1 
# MAGIC     AND b.status = 'booked'
# MAGIC     AND tendering_acceptance.`accepted?` = 'true'
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     'RELAY' AS systemm,
# MAGIC     t.full_name AS tag_created_by,
# MAGIC     'LTL Team' AS booked_by,
# MAGIC     CAST(tendering_acceptance.accepted_at AS TIMESTAMP) AS create_date,
# MAGIC     CAST(load_number AS FLOAT) AS loadnum,
# MAGIC     NULL AS booking_id,
# MAGIC     r.profit_center AS office,
# MAGIC     CAST(ship_date AS DATE) AS shipdate,
# MAGIC     CASE 
# MAGIC       WHEN customer_lookup.master_customer_name IS NULL THEN r.customer_name 
# MAGIC       ELSE master_customer_name 
# MAGIC     END AS mastername,
# MAGIC     am.full_name AS primary_am,
# MAGIC     am.full_name AS secondary_am,
# MAGIC     s.full_name AS sales_rep,
# MAGIC     s.full_name AS secondary_sales_rep,
# MAGIC     pickup_city,
# MAGIC     pickup_state,
# MAGIC     pickup_zip AS origin_zip,
# MAGIC     consignee_city,
# MAGIC     consignee_state,
# MAGIC     consignee_zip AS dest_zip,
# MAGIC     (CAST(c.linehaul_amount AS FLOAT) + CAST(c.fuel_surcharge_amount AS FLOAT) + CAST(c.accessorial_amount AS FLOAT)) / 100.00 AS revenue,
# MAGIC     projected_expense / 100.0 AS expense,
# MAGIC     e.carrier_name,
# MAGIC     CAST(projection_carrier.dot_num AS STRING) AS dot_num,
# MAGIC     CAST(projection_carrier.mc_num AS STRING) AS mc_num,
# MAGIC     CASE 
# MAGIC       WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'V'
# MAGIC       WHEN tendering_service_line.service_line_type = 'flatbed' THEN 'F'
# MAGIC       WHEN tendering_service_line.service_line_type = 'refrigerated' THEN 'R' 
# MAGIC     END AS equip,
# MAGIC     e.dispatch_status AS status,
# MAGIC     CAST(distance_projection.total_distance AS FLOAT) AS total_distance,
# MAGIC     'LTL' AS mode,
# MAGIC     b.delivery_count
# MAGIC   FROM big_export_projection e 
# MAGIC   LEFT JOIN tendering_acceptance ON CAST(e.load_number AS FLOAT) = CAST(tendering_acceptance.relay_reference_number AS FLOAT)
# MAGIC   LEFT JOIN relay_users t ON tendering_acceptance.accepted_by = t.user_id
# MAGIC   LEFT JOIN customer_profile_projection r ON e.customer_id = r.customer_slug
# MAGIC   LEFT JOIN customer_money c ON c.relay_reference_number = e.load_number
# MAGIC   LEFT JOIN projection_carrier ON e.carrier_id = projection_carrier.id
# MAGIC   LEFT JOIN relay_users am ON r.primary_relay_user_id = am.user_id AND am.`active?` = 'true'
# MAGIC   LEFT JOIN customer_lookup ON LEFT(r.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC   LEFT JOIN tendering_service_line ON CAST(e.load_number AS FLOAT) = CAST(tendering_service_line.relay_reference_number AS FLOAT)
# MAGIC   LEFT JOIN distance_projection ON CAST(e.load_number AS FLOAT) = distance_projection.relay_reference_number
# MAGIC   LEFT JOIN analytics.dim_financial_calendar ON CAST(e.ship_date AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE)
# MAGIC   LEFT JOIN relay_users s ON r.sales_relay_user_id = s.user_id AND s.`active?` = 'true'
# MAGIC   LEFT JOIN aljex_user_report_listing ON am.full_name = aljex_user_report_listing.full_name
# MAGIC   LEFT JOIN booking_projection b ON CAST(e.load_number AS FLOAT) = b.relay_reference_number AND b.status = 'cancelled'
# MAGIC   WHERE 1=1
# MAGIC     AND e.carrier_name IS NOT NULL
# MAGIC     AND e.customer_id NOT IN ('hain', 'deb', 'roland')
# MAGIC     AND dispatch_status NOT IN ('Cancelled', 'Open')
# MAGIC   
# MAGIC   UNION
# MAGIC   
# MAGIC   SELECT DISTINCT 
# MAGIC     'RELAY' AS systemm,
# MAGIC     t.full_name AS tag_created_by,
# MAGIC     'LTL Team' AS booked_by,
# MAGIC     CAST(tendering_acceptance.accepted_at AS TIMESTAMP) AS create_date,
# MAGIC     CAST(load_number AS FLOAT) AS loadnum,
# MAGIC     NULL AS booking_id,
# MAGIC     CASE 
# MAGIC       WHEN customer_id = 'roland' THEN 'LTL' 
# MAGIC       ELSE 'NJ' 
# MAGIC     END AS office,
# MAGIC     CAST(ship_date AS DATE) AS shipdate,
# MAGIC     CASE 
# MAGIC       WHEN customer_id = 'deb' THEN 'Paul Jensen' 
# MAGIC       WHEN customer_id = 'hain' THEN 'Paul Jensen' 
# MAGIC       WHEN customer_id = 'roland' THEN 'Manny Galace' 
# MAGIC     END AS `Customer Sales Rep`,
# MAGIC     CASE 
# MAGIC       WHEN customer_id = 'deb' THEN 'Paul Jensen' 
# MAGIC       WHEN customer_id = 'hain' THEN 'Paul Jensen' 
# MAGIC       WHEN customer_id = 'roland' THEN 'Manny Galace' 
# MAGIC     END AS `Customer acct mgr`,
# MAGIC     CASE 
# MAGIC       WHEN customer_id = 'deb' THEN 'Paul Jensen' 
# MAGIC       WHEN customer_id = 'hain' THEN 'Paul Jensen' 
# MAGIC       WHEN customer_id = 'roland' THEN 'Manny Galace' 
# MAGIC     END AS `Customer acct mgr`,
# MAGIC     CASE 
# MAGIC       WHEN customer_id = 'deb' THEN 'Paul Jensen' 
# MAGIC       WHEN customer_id = 'hain' THEN 'Paul Jensen' 
# MAGIC       WHEN customer_id = 'roland' THEN 'Manny Galace' 
# MAGIC     END AS `Customer acct mgr`,
# MAGIC     CASE 
# MAGIC       WHEN customer_id = 'deb' THEN 'SC JOHNSON' 
# MAGIC       WHEN customer_id = 'hain' THEN 'HAIN CELESTIAL' 
# MAGIC       WHEN customer_id = 'roland' THEN 'ROLAND CORPORATION' 
# MAGIC     END AS mastername,
# MAGIC     pickup_city,
# MAGIC     pickup_state,
# MAGIC     pickup_zip AS origin_zip,
# MAGIC     consignee_city,
# MAGIC     consignee_state,
# MAGIC     consignee_zip AS dest_zip,
# MAGIC     (carrier_accessorial_expense +
# MAGIC       CEIL(
# MAGIC         CASE 
# MAGIC           WHEN customer_id = 'deb' THEN 
# MAGIC             CASE 
# MAGIC               WHEN carrier_id = '300003979' THEN GREATEST(carrier_linehaul_expense * 0.10, 1500)
# MAGIC               ELSE 
# MAGIC                 CASE 
# MAGIC                   WHEN CAST(ship_date AS DATE) < '2019-03-09' THEN GREATEST(carrier_linehaul_expense * 0.08, 1000)
# MAGIC                   ELSE GREATEST(carrier_linehaul_expense * 0.09, 1000)
# MAGIC                 END
# MAGIC             END
# MAGIC           WHEN customer_id = 'hain' THEN GREATEST(carrier_linehaul_expense * 0.10, 1000)
# MAGIC           WHEN customer_id = 'roland' THEN carrier_linehaul_expense * 0.1365
# MAGIC           ELSE 0 
# MAGIC         END + carrier_linehaul_expense
# MAGIC       ) +
# MAGIC       CEIL(
# MAGIC         CEIL(
# MAGIC           CASE 
# MAGIC             WHEN customer_id = 'deb' THEN 
# MAGIC               CASE 
# MAGIC                 WHEN carrier_id = '300003979' THEN GREATEST(carrier_linehaul_expense * 0.10, 1500)
# MAGIC                 ELSE 
# MAGIC                   CASE 
# MAGIC                     WHEN CAST(ship_date AS DATE) < '2019-03-09' THEN GREATEST(carrier_linehaul_expense * 0.08, 1000)
# MAGIC                     ELSE GREATEST(carrier_linehaul_expense * 0.09, 1000)
# MAGIC                   END
# MAGIC               END
# MAGIC             WHEN customer_id = 'hain' THEN GREATEST(carrier_linehaul_expense * 0.10, 1000)
# MAGIC             WHEN customer_id = 'roland' THEN carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0 
# MAGIC           END + carrier_linehaul_expense
# MAGIC         ) * 
# MAGIC         CASE 
# MAGIC           WHEN carrier_linehaul_expense = 0 THEN 0 
# MAGIC           ELSE carrier_fuel_expense / carrier_linehaul_expense 
# MAGIC         END
# MAGIC       )
# MAGIC     ) / 100 AS revenue,
# MAGIC     projected_expense / 100.0 AS expense,
# MAGIC     e.carrier_name,
# MAGIC     CAST(projection_carrier.dot_num AS STRING) AS dot_num,
# MAGIC     CAST(projection_carrier.mc_num AS STRING) AS mc_num,
# MAGIC     CASE 
# MAGIC       WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'V'
# MAGIC       WHEN tendering_service_line.service_line_type = 'flatbed' THEN 'F'
# MAGIC       WHEN tendering_service_line.service_line_type = 'refrigerated' THEN 'R' 
# MAGIC     END AS equip,
# MAGIC     e.dispatch_status AS status,
# MAGIC     CAST(distance_projection.total_distance AS FLOAT) AS total_distance,
# MAGIC     'LTL' AS mode,
# MAGIC     b.delivery_count
# MAGIC   FROM big_export_projection e 
# MAGIC   LEFT JOIN tendering_acceptance ON CAST(e.load_number AS FLOAT) = CAST(tendering_acceptance.relay_reference_number AS FLOAT) 
# MAGIC   LEFT JOIN relay_users t ON tendering_acceptance.accepted_by = t.user_id 
# MAGIC   LEFT JOIN customer_profile_projection r ON e.customer_id = r.customer_slug 
# MAGIC   LEFT JOIN customer_money c ON c.relay_reference_number = e.load_number
# MAGIC   LEFT JOIN projection_carrier ON e.carrier_id = projection_carrier.id 
# MAGIC   LEFT JOIN relay_users a ON r.primary_relay_user_id = a.user_id 
# MAGIC   LEFT JOIN relay_users s ON r.sales_relay_user_id = s.user_id 
# MAGIC   LEFT JOIN customer_lookup ON LEFT(r.customer_name, 32) = LEFT(aljex_customer_name, 32) 
# MAGIC   LEFT JOIN tendering_service_line ON CAST(e.load_number AS FLOAT) = CAST(tendering_service_line.relay_reference_number AS FLOAT)
# MAGIC   LEFT JOIN distance_projection ON CAST(e.load_number AS FLOAT) = distance_projection.relay_reference_number
# MAGIC   LEFT JOIN analytics.dim_financial_calendar ON CAST(e.ship_date AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE) 
# MAGIC   LEFT JOIN booking_projection b ON CAST(e.load_number AS FLOAT) = b.relay_reference_number AND b.status = 'cancelled'
# MAGIC   LEFT JOIN aljex_user_report_listing ON 
# MAGIC     CASE 
# MAGIC       WHEN customer_id = 'deb' THEN 'Paul Jensen'
# MAGIC       WHEN customer_id = 'hain' THEN 'Paul Jensen'
# MAGIC       WHEN customer_id = 'roland' THEN 'Manny Galace' 
# MAGIC     END = aljex_user_report_listing.full_name
# MAGIC   WHERE 1=1 
# MAGIC     AND e.carrier_name IS NOT NULL 
# MAGIC     AND e.customer_id IN ('hain', 'deb', 'roland')
# MAGIC     AND dispatch_status NOT IN ('Cancelled', 'Open')
# MAGIC )
# MAGIC ,
# MAGIC final_output as (
# MAGIC SELECT DISTINCT 
# MAGIC   systemm AS TMS,
# MAGIC   tag_created_by AS Create_User,
# MAGIC   booked_by AS Actual_Dispatcher,
# MAGIC   create_date AS Create_Date,
# MAGIC   loadnum AS Load_Number,
# MAGIC   booking_id AS Booking_ID,
# MAGIC   office AS Ops_Office,
# MAGIC   shipdate AS Ship_Date,
# MAGIC   COALESCE(relay_sales, system_sales_rep) AS Salesperson,
# MAGIC   COALESCE(relay_am, system_am) AS Acct_Mgr,
# MAGIC   mastername AS Customer,
# MAGIC   INITCAP(origin_city) AS Shipper_City,
# MAGIC   origin_state AS Shipper_State,
# MAGIC   origin_zip AS Shipper_Zip,
# MAGIC   INITCAP(dest_city) AS Consignee_City,
# MAGIC   dest_state AS Consignee_State,
# MAGIC   dest_zip AS Consignee_Zip,
# MAGIC   revenue AS Total_AR,
# MAGIC   expense AS Total_AP,
# MAGIC   CASE 
# MAGIC     WHEN revenue IS NULL THEN 0 
# MAGIC     ELSE revenue - COALESCE(expense, 0) 
# MAGIC   END AS Total_Margin,
# MAGIC   CASE 
# MAGIC     WHEN revenue IS NULL THEN 0 
# MAGIC     WHEN revenue = 0 THEN 0 
# MAGIC     ELSE (COALESCE(revenue, 0) - COALESCE(expense, 0)) / revenue 
# MAGIC   END AS Total_Margin_Percentage,
# MAGIC   carrier_name AS Carrier,
# MAGIC   dot_num AS Carrier_DOT,
# MAGIC   mc_num AS Carr_MC,
# MAGIC   equip AS Equipment,
# MAGIC   status AS Status,
# MAGIC   miles AS Miles,
# MAGIC   equipment_mode AS Mode,
# MAGIC   num_drops AS Total_Delvs
# MAGIC FROM system_union 
# MAGIC LEFT JOIN aljex_user_report_listing ON COALESCE(relay_sales, system_sales_rep) = aljex_user_report_listing.full_name 
# MAGIC LEFT JOIN relay_users ON booked_by = relay_users.full_name 
# MAGIC ORDER BY Ship_Date DESC)
# MAGIC select * from final_output
# MAGIC
# MAGIC     
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Shipment_Summary_Report_Temp;
# MAGIC
# MAGIC INSERT INTO analytics.Shipment_Summary_Report_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.JohnnyH_Summary_Report_Genai

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Shipment_Summary_Report;
# MAGIC
# MAGIC INSERT INTO analytics.Shipment_Summary_Report
# MAGIC SELECT *
# MAGIC FROM analytics.Shipment_Summary_Report_Temp