# Databricks notebook source
# MAGIC %md
# MAGIC ## Canned Reprorts
# MAGIC * **Description:** To Replicate Metabase Canned Reports for Niffy
# MAGIC * **Created Date:** 10/15/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 10/15/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating View

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC create or replace view bronze.USPS_Carrier_Vetting_GenAI AS ( WITH new_pu_appt AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.relay_reference_number,
# MAGIC         p.appointment_datetime::TIMESTAMP AS pickup_proj_appt_datetime,
# MAGIC         s.appointment_datetime::TIMESTAMP AS planning_appt_datetime,
# MAGIC         s.window_start_datetime::TIMESTAMP,
# MAGIC         s.window_end_datetime::TIMESTAMP,
# MAGIC         COALESCE(
# MAGIC             s.appointment_datetime::TIMESTAMP,
# MAGIC             s.window_start_datetime::TIMESTAMP,
# MAGIC             s.window_end_datetime::TIMESTAMP,
# MAGIC             p.appointment_datetime::TIMESTAMP,
# MAGIC             b.ready_date::DATE
# MAGIC         ) AS new_pu_appt
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN pickup_projection p 
# MAGIC         ON b.relay_reference_number = p.relay_reference_number
# MAGIC         AND b.first_shipper_name = p.shipper_name
# MAGIC         AND p.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule s 
# MAGIC         ON b.relay_reference_number = s.relay_reference_number
# MAGIC         AND s.stop_type = 'pickup'
# MAGIC         AND s.sequence_number = 1
# MAGIC         AND s.`removed?` = 'false'
# MAGIC     WHERE b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC system_union AS (
# MAGIC     SELECT  
# MAGIC         'ALJEX' AS systemm,
# MAGIC         p.office AS office,
# MAGIC         projection_load_2.status AS status,
# MAGIC         p.id::FLOAT AS loadnum,
# MAGIC         shipper AS customer,
# MAGIC         CASE 
# MAGIC             WHEN master_customer_name IS NULL THEN projection_load_2.shipper 
# MAGIC             ELSE master_customer_name 
# MAGIC         END AS mastername,
# MAGIC         p.pickup_date::DATE AS shipdate,
# MAGIC         c.dot_num::STRING,
# MAGIC         mc_num::STRING
# MAGIC     FROM projection_load_1 p 
# MAGIC     LEFT JOIN projection_load_2 ON p.id = projection_load_2.id
# MAGIC     LEFT JOIN customer_lookup 
# MAGIC         ON LEFT(projection_load_2.shipper, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN projection_carrier c 
# MAGIC         ON p.carrier_id = c.id 
# MAGIC     WHERE p.pickup_date::DATE BETWEEN CURRENT_DATE - INTERVAL 1 YEAR AND CURRENT_DATE
# MAGIC         AND projection_load_2.status NOT LIKE 'VOID%'
# MAGIC         AND p.office NOT IN ('10', '34', '51', '54', '61', '62', '63', '64', '74')
# MAGIC        --- [[AND c.dot_num = {{dotnum}}]]
# MAGIC       ---  [[AND c.mc_num = {{mc}}]]
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         'RELAY',
# MAGIC         r.profit_center AS office,
# MAGIC         b.status AS status,
# MAGIC         b.relay_reference_number::FLOAT AS loadnum,
# MAGIC         r.customer_name,
# MAGIC         CASE 
# MAGIC             WHEN master_customer_name IS NULL THEN r.customer_name 
# MAGIC             ELSE master_customer_name 
# MAGIC         END AS mastername,
# MAGIC         new_pu_appt.new_pu_appt::DATE AS datee,
# MAGIC         carrier_projection_new.dot_number::STRING,
# MAGIC         mc_number::STRING
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN new_pu_appt 
# MAGIC         ON b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC     LEFT JOIN customer_profile_projection r 
# MAGIC         ON b.tender_on_behalf_of_id = r.customer_slug
# MAGIC     LEFT JOIN carrier_projection_new 
# MAGIC         ON b.booked_carrier_id = carrier_projection_new.carrier_id 
# MAGIC     LEFT JOIN customer_lookup 
# MAGIC         ON LEFT(r.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     WHERE new_pu_appt.new_pu_appt::DATE BETWEEN CURRENT_DATE - INTERVAL 1 YEAR AND CURRENT_DATE
# MAGIC         AND b.status = 'booked'
# MAGIC         AND new_pu_appt.new_pu_appt::DATE <= CURRENT_DATE
# MAGIC       ---  [[AND carrier_projection_new.dot_number = {{dotnum}}]]
# MAGIC       ---  [[AND carrier_projection_new.mc_number = {{mc}}]]
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT  
# MAGIC         'EDGE',
# MAGIC         ops_office,
# MAGIC         cai_data.status,
# MAGIC         load_num::FLOAT,
# MAGIC         customer,
# MAGIC         CASE 
# MAGIC             WHEN master_customer_name IS NULL THEN customer 
# MAGIC             ELSE master_customer_name 
# MAGIC         END AS mastername,
# MAGIC         ship_date::DATE,
# MAGIC         c.dot_num::STRING,
# MAGIC         mc_num::STRING
# MAGIC     FROM cai_data 
# MAGIC     LEFT JOIN projection_carrier c 
# MAGIC         ON carr_mc::STRING = c.mc_num::STRING 
# MAGIC     LEFT JOIN customer_lookup 
# MAGIC         ON LEFT(customer, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     WHERE ship_date::DATE BETWEEN CURRENT_DATE - INTERVAL 1 YEAR AND CURRENT_DATE
# MAGIC       ---  [[AND c.dot_num = {{dotnum}}]]
# MAGIC        --- [[AND c.mc_num = {{mc}}]]
# MAGIC ),
# MAGIC
# MAGIC carrier_name AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         system_union.dot_num AS dotnum,
# MAGIC         system_union.mc_num AS mc_number,
# MAGIC         CASE 
# MAGIC             WHEN MAX(pc.name) IS NULL THEN MAX(cp.carrier_name) 
# MAGIC             ELSE MAX(pc.name) 
# MAGIC         END AS carrier_name
# MAGIC     FROM system_union
# MAGIC     LEFT JOIN projection_carrier pc 
# MAGIC         ON system_union.dot_num::STRING = pc.dot_num::STRING
# MAGIC     LEFT JOIN carrier_projection_new cp 
# MAGIC         ON system_union.dot_num::STRING = cp.dot_number::STRING
# MAGIC     GROUP BY system_union.dot_num, system_union.mc_num
# MAGIC ),
# MAGIC
# MAGIC summary_data AS (
# MAGIC     SELECT DISTINCT
# MAGIC         dot_num,
# MAGIC         mc_num,
# MAGIC         carrier_name.carrier_name,
# MAGIC         mastername,
# MAGIC         shipdate,
# MAGIC         DATE_FORMAT(shipdate, 'yyyy-MM') AS year_month,
# MAGIC         loadnum 
# MAGIC     FROM system_union
# MAGIC     LEFT JOIN carrier_name 
# MAGIC         ON system_union.dot_num::STRING = carrier_name.dotnum::STRING
# MAGIC ),
# MAGIC
# MAGIC USPS_Data AS (
# MAGIC     SELECT 
# MAGIC         summary_data.dot_num,
# MAGIC         summary_data.mc_num,
# MAGIC         summary_data.carrier_name AS Carrier,
# MAGIC         year_month AS Month,
# MAGIC         COUNT(DISTINCT loadnum) AS Volume,
# MAGIC         COUNT(DISTINCT loadnum) FILTER (WHERE mastername = 'USPS') AS USPS_Vol,
# MAGIC         CASE 
# MAGIC             WHEN COUNT(DISTINCT loadnum) = 0 THEN 0 
# MAGIC             ELSE COUNT(DISTINCT loadnum) FILTER (WHERE mastername = 'USPS')::FLOAT / COUNT(DISTINCT loadnum)::FLOAT 
# MAGIC         END AS percent_USPS,
# MAGIC         NULL::DATE AS Approved_At,
# MAGIC         1 AS orderbyyyy
# MAGIC     FROM summary_data
# MAGIC     LEFT JOIN carrier_projection_new 
# MAGIC         ON summary_data.dot_num::STRING = carrier_projection_new.dot_number::STRING 
# MAGIC         AND carrier_projection_new.is_overridden = 'false'
# MAGIC     GROUP BY summary_data.dot_num, summary_data.mc_num, summary_data.carrier_name, year_month
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT  
# MAGIC         carrier_projection_new.dot_number::STRING AS dot_num,
# MAGIC         carrier_projection_new.mc_number::STRING AS mc_number,
# MAGIC         'Totals',
# MAGIC         NULL,
# MAGIC         COUNT(DISTINCT loadnum) AS Volume,
# MAGIC         COUNT(DISTINCT loadnum) FILTER (WHERE mastername = 'USPS') AS USPS_Vol,
# MAGIC         CASE 
# MAGIC             WHEN COUNT(DISTINCT loadnum) = 0 THEN 0 
# MAGIC             ELSE COUNT(DISTINCT loadnum) FILTER (WHERE mastername = 'USPS')::FLOAT / COUNT(DISTINCT loadnum)::FLOAT 
# MAGIC         END AS percent_USPS,
# MAGIC         carrier_projection_new.approved_at::DATE,
# MAGIC         2 AS orderby
# MAGIC     FROM summary_data
# MAGIC     LEFT JOIN carrier_projection_new 
# MAGIC         ON summary_data.dot_num::STRING = carrier_projection_new.dot_number::STRING 
# MAGIC         AND carrier_projection_new.is_overridden = 'false'
# MAGIC     GROUP BY carrier_projection_new.approved_at, carrier_projection_new.dot_number, carrier_projection_new.mc_number
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         carrier_projection_new.dot_number::STRING AS dot_num,
# MAGIC         carrier_projection_new.mc_number::STRING AS mc_number,
# MAGIC         'Actual DOT #?',
# MAGIC         COALESCE(carrier_projection_new.dot_number, 'Doesn''t Exist'),
# MAGIC         0,
# MAGIC         0,
# MAGIC         0,
# MAGIC         NULL,
# MAGIC         3
# MAGIC     FROM carrier_projection_new
# MAGIC     WHERE 1=1
# MAGIC     -- [[AND carrier_projection_new.dot_number = {{dotnum}}]]
# MAGIC     -- [[AND carrier_projection_new.mc_number = {{mc}}]]
# MAGIC     ORDER BY orderbyyyy, Month
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM USPS_Data where volume >= '5'
# MAGIC
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.USPS_Carrier_Vetting_GenAI_Temp;
# MAGIC INSERT INTO analytics.USPS_Carrier_Vetting_GenAI_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.USPS_Carrier_Vetting_GenAI

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.USPS_Carrier_Vetting_GenAI;
# MAGIC INSERT INTO analytics.USPS_Carrier_Vetting_GenAI
# MAGIC SELECT *
# MAGIC FROM analytics.USPS_Carrier_Vetting_GenAI_Temp