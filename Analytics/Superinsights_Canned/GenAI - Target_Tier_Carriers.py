# Databricks notebook source
# MAGIC %md
# MAGIC ## Canned Reprorts
# MAGIC * **Description:** To Replicate Metabase Canned Reports for Niffy
# MAGIC * **Created Date:** 10/15/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 15/15/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating View

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC create or replace view bronze.Target_Tier_Carriers  as ( 
# MAGIC WITH use_dates AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         DATE_FORMAT(date, 'yyyy-MM') AS use_dates
# MAGIC     FROM analytics.dim_financial_calendar
# MAGIC     WHERE date <= CURRENT_DATE()
# MAGIC     ORDER BY use_dates DESC 
# MAGIC     LIMIT 12
# MAGIC ),
# MAGIC month_rank AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         use_dates,
# MAGIC         RANK() OVER (ORDER BY use_dates) AS month_rank
# MAGIC     FROM use_dates
# MAGIC     ORDER BY use_dates
# MAGIC ),
# MAGIC new_pu_appt AS (
# MAGIC     SELECT
# MAGIC         DISTINCT b.relay_reference_number,
# MAGIC         MAX(COALESCE(
# MAGIC             TRY_CAST(s.appointment_datetime AS TIMESTAMP),
# MAGIC             TRY_CAST(s.window_start_datetime AS TIMESTAMP),
# MAGIC             TRY_CAST(s.window_end_datetime AS TIMESTAMP),
# MAGIC             TRY_CAST(p.appointment_datetime AS TIMESTAMP),
# MAGIC             TRY_CAST(b.ready_date AS TIMESTAMP)
# MAGIC         )) AS new_pu_appt
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC         AND b.first_shipper_name = p.shipper_name
# MAGIC         AND p.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC         AND s.stop_type = 'pickup'
# MAGIC         AND s.sequence_number = 1
# MAGIC         AND s.`removed?` = 'false'
# MAGIC     WHERE 1=1
# MAGIC         AND b.status NOT IN ('cancelled', 'bounced')
# MAGIC         AND b.tender_on_behalf_of_id = 'target'
# MAGIC     GROUP BY b.relay_reference_number
# MAGIC ),
# MAGIC actual_pu AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.booking_id, 
# MAGIC         b.relay_reference_number,
# MAGIC         MAX(COALESCE(TRY_CAST(canonical_stop.in_date_time AS TIMESTAMP), TRY_CAST(canonical_stop.out_date_time AS TIMESTAMP))) AS actual_pu
# MAGIC     FROM booking_projection b 
# MAGIC     LEFT JOIN canonical_stop ON b.booking_id = canonical_stop.booking_id AND b.first_shipper_id = canonical_stop.facility_id
# MAGIC     WHERE 1=1 
# MAGIC         AND b.status = 'booked'
# MAGIC         AND b.tender_on_behalf_of_id = 'target'
# MAGIC         AND canonical_stop.`stale?` = 'false'
# MAGIC         AND canonical_stop.stop_type = 'pickup'
# MAGIC     GROUP BY b.booking_id, b.relay_reference_number
# MAGIC ),
# MAGIC summary_data AS (
# MAGIC     SELECT DISTINCT
# MAGIC         CAST(b.relay_reference_number AS STRING) AS loadnum,
# MAGIC         CAST(COALESCE(actual_pu.actual_pu, new_pu_appt.new_pu_appt) AS DATE) AS datee,
# MAGIC         DATE_FORMAT(analytics.dim_financial_calendar.date, 'yyyy-MM') AS monthh,
# MAGIC         month_rank.month_rank,
# MAGIC         CAST(c.dot_number AS STRING) AS dot_number,
# MAGIC         receiver_state,
# MAGIC         'TL' AS modee
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN new_pu_appt ON b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC     LEFT JOIN actual_pu ON b.booking_id = actual_pu.booking_id
# MAGIC     LEFT JOIN analytics.dim_financial_calendar ON COALESCE(TRY_CAST(actual_pu.actual_pu AS DATE), TRY_CAST(new_pu_appt.new_pu_appt AS DATE)) = analytics.dim_financial_calendar.date
# MAGIC     JOIN use_dates ON DATE_FORMAT(analytics.dim_financial_calendar.date, 'yyyy-MM') = use_dates.use_dates
# MAGIC     LEFT JOIN carrier_projection c ON b.booked_carrier_id = c.carrier_id
# MAGIC     LEFT JOIN month_rank ON DATE_FORMAT(analytics.dim_financial_calendar.date, 'yyyy-MM') = month_rank.use_dates
# MAGIC     WHERE 1=1
# MAGIC         --[[AND receiver_state = {{crossdock}}]]
# MAGIC         AND b.status = 'booked'
# MAGIC         AND b.tender_on_behalf_of_id = 'target'
# MAGIC         AND COALESCE(TRY_CAST(new_pu_appt.new_pu_appt AS DATE), TRY_CAST(actual_pu.actual_pu AS DATE)) <= CURRENT_DATE()
# MAGIC     UNION DISTINCT 
# MAGIC     SELECT DISTINCT
# MAGIC         CAST(e.load_number AS STRING) AS loadnum,
# MAGIC         CAST(e.ship_date AS DATE) AS datee,
# MAGIC         DATE_FORMAT(analytics.dim_financial_calendar.date, 'yyyy-MM') AS monthh,
# MAGIC         month_rank.month_rank,
# MAGIC         CAST(c.dot_num AS STRING) AS dot_number,
# MAGIC         consignee_state,
# MAGIC         'LTL' AS modee
# MAGIC     FROM big_export_projection e
# MAGIC     LEFT JOIN analytics.dim_financial_calendar ON TRY_CAST(e.ship_date AS DATE) = analytics.dim_financial_calendar.date
# MAGIC     JOIN use_dates ON DATE_FORMAT(analytics.dim_financial_calendar.date, 'yyyy-MM') = use_dates.use_dates
# MAGIC     LEFT JOIN month_rank ON DATE_FORMAT(analytics.dim_financial_calendar.date, 'yyyy-MM') = month_rank.use_dates
# MAGIC     LEFT JOIN projection_carrier c ON e.carrier_id = c.id
# MAGIC     WHERE 1=1
# MAGIC         --[[AND consignee_state = {{crossdock}}]]
# MAGIC         AND e.customer_id = 'target'
# MAGIC         AND e.dispatch_status <> 'Cancelled'
# MAGIC         AND e.carrier_name IS NOT NULL 
# MAGIC         AND TRY_CAST(e.ship_date AS DATE) <= CURRENT_DATE()
# MAGIC ),
# MAGIC
# MAGIC     final_data as (SELECT DISTINCT 
# MAGIC         loadnum,
# MAGIC         datee,
# MAGIC         monthh,
# MAGIC         month_rank,
# MAGIC         summary_data.dot_number,
# MAGIC         receiver_state,
# MAGIC         target_tiers.carrier,
# MAGIC         target_tiers.state
# MAGIC     FROM summary_data
# MAGIC     LEFT JOIN target_tiers ON CAST(summary_data.dot_number AS STRING) = CAST(target_tiers.dot_number AS STRING) 
# MAGIC         AND summary_data.modee = target_tiers.mode )
# MAGIC
# MAGIC 	select * FROM final_data
# MAGIC
# MAGIC
# MAGIC
# MAGIC     
# MAGIC )
# MAGIC 	
# MAGIC 	

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Target_Tier_Carriers;
# MAGIC
# MAGIC INSERT INTO analytics.Target_Tier_Carriers
# MAGIC SELECT *
# MAGIC FROM bronze.Target_Tier_Carriers