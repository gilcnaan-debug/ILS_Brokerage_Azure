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
# MAGIC
# MAGIC create or replace view bronze.Target_Updates_GenAI as (
# MAGIC WITH first_loaded_tracking AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     truck_load_thing_id,
# MAGIC     MIN(at) AS first_loaded
# MAGIC   FROM tracking_activity_v2 
# MAGIC   WHERE type = 'Tracking.TruckLoadThing.MarkedLoaded'
# MAGIC   GROUP BY truck_load_thing_id
# MAGIC ),
# MAGIC combined_loads AS (
# MAGIC   SELECT DISTINCT 
# MAGIC   booking_projection.tender_on_behalf_of_id,
# MAGIC     canonical_stop.stop_id,
# MAGIC     canonical_stop.facility_name,
# MAGIC     canonical_stop.in_date_time,
# MAGIC     canonical_stop.out_date_time,
# MAGIC     relay_reference_number_one,
# MAGIC     relay_reference_number_two,
# MAGIC     canonical_plan_projection.relay_reference_number AS combine_new_rrn,
# MAGIC     booking_projection.booking_id,
# MAGIC     booking_projection.booked_by_name,
# MAGIC     booking_projection.booked_carrier_name,
# MAGIC     CONCAT(a.original_shipment_id, ', ', z.original_shipment_id) AS shipment_ids,
# MAGIC     CONCAT('https://relaytms.com/tracking_board?modal_params[title]=Mark+loaded&modal_params[truck_load_thing_id]=', t.truck_load_thing_id, '&show_modal=mark_loaded&filters[booked_by_id]=&filters[customer_id]=&filters[search_term]=', booking_projection.booking_id, '&filters[status]=all&sort_by[1][direction]=desc&sort_by[1][field]=booking_id') AS update_url,
# MAGIC     canonical_stop.stop_type,
# MAGIC     t.last_update_event_name,
# MAGIC     COALESCE(pickup_projection.appointment_datetime, p.appointment_datetime, p.window_end_datetime, p.window_start_datetime) AS p_appt,
# MAGIC     'COMBINED' AS combined,
# MAGIC     first_loaded_tracking.first_loaded
# MAGIC   FROM plan_combination_projection
# MAGIC   JOIN canonical_plan_projection ON resulting_plan_id = canonical_plan_projection.plan_id
# MAGIC   LEFT JOIN booking_projection ON canonical_plan_projection.relay_reference_number = booking_projection.relay_reference_number
# MAGIC   LEFT JOIN canonical_stop ON booking_projection.booking_id = canonical_stop.booking_id 
# MAGIC   LEFT JOIN truckload_projection t ON booking_projection.booking_id = t.booking_id
# MAGIC   LEFT JOIN first_loaded_tracking ON t.truck_load_thing_id = first_loaded_tracking.truck_load_thing_id
# MAGIC   LEFT JOIN tender_reference_numbers_projection a ON relay_reference_number_one = a.relay_reference_number
# MAGIC   LEFT JOIN tender_reference_numbers_projection z ON relay_reference_number_two = z.relay_reference_number
# MAGIC   LEFT JOIN pickup_projection ON booking_projection.relay_reference_number = pickup_projection.relay_reference_number
# MAGIC     AND pickup_projection.shipper_id = canonical_stop.facility_id
# MAGIC   LEFT JOIN planning_stop_schedule p ON canonical_stop.stop_id = p.stop_id 
# MAGIC   LEFT JOIN financial_calendar ON CAST(COALESCE(pickup_projection.appointment_datetime, p.appointment_datetime, p.window_end_datetime, p.window_start_datetime) AS DATE) = financial_calendar.date
# MAGIC   WHERE is_combined = true
# MAGIC     AND canonical_stop.stop_type = 'pickup'
# MAGIC     AND canonical_stop.`stale?` = false
# MAGIC     AND booking_projection.status = 'booked'
# MAGIC     AND p.`removed?` = false
# MAGIC     AND receiver_state IN ('IL', 'PA')
# MAGIC     AND (
# MAGIC       COALESCE(CAST(first_loaded_tracking.first_loaded AS DATE), CAST(in_date_time AS DATE)) IS NULL
# MAGIC       OR COALESCE(CAST(first_loaded_tracking.first_loaded AS DATE), CAST(out_date_time AS DATE)) IS NULL
# MAGIC     )
# MAGIC     AND booking_projection.tender_on_behalf_of_id = 'target' -- Changes
# MAGIC
# MAGIC   ORDER BY combine_new_rrn
# MAGIC ),
# MAGIC system_union as (
# MAGIC SELECT DISTINCT
# MAGIC   customer_profile_projection.customer_name AS Customer,
# MAGIC   tendering_acceptance.shipment_id AS Shipment_ID,
# MAGIC   booking_projection.relay_reference_number AS Load_Number,
# MAGIC   s.booking_id AS Booking_ID,
# MAGIC   booking_projection.booked_by_name AS Booked_By,
# MAGIC   booking_projection.booked_carrier_name AS Booked_Carrier,
# MAGIC   s.stop_type AS Stop_Type,
# MAGIC   s.facility_name AS Facility_Name,
# MAGIC   COALESCE(pickup_projection.appointment_datetime, p.appointment_datetime, p.window_end_datetime, p.window_start_datetime) AS Appointment,
# MAGIC   s.in_date_time AS In_Time,
# MAGIC   s.out_date_time AS Out_Time,
# MAGIC   t.last_update_event_name AS Last_Status,
# MAGIC   CONCAT('https://relaytms.com/tracking_board?modal_params[title]=Mark+loaded&modal_params[truck_load_thing_id]=', t.truck_load_thing_id, '&show_modal=mark_loaded&filters[booked_by_id]=&filters[customer_id]=&filters[search_term]=', booking_projection.booking_id, '&filters[status]=all&sort_by[1][direction]=desc&sort_by[1][field]=booking_id') AS Update_Link,
# MAGIC   first_loaded_tracking.first_loaded AS Marked_Loaded_At
# MAGIC FROM canonical_stop s
# MAGIC JOIN booking_projection ON booking_projection.booking_id = s.booking_id
# MAGIC LEFT JOIN tendering_acceptance ON booking_projection.relay_reference_number = tendering_acceptance.relay_reference_number
# MAGIC LEFT JOIN customer_profile_projection ON booking_projection.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC JOIN truckload_projection t ON t.booking_id = s.booking_id
# MAGIC LEFT JOIN planning_stop_schedule p ON s.stop_id = p.stop_id 
# MAGIC LEFT JOIN pickup_projection ON booking_projection.relay_reference_number = pickup_projection.relay_reference_number
# MAGIC   AND pickup_projection.shipper_id = s.facility_id
# MAGIC LEFT JOIN financial_calendar ON CAST(COALESCE(pickup_projection.appointment_datetime, p.appointment_datetime, p.window_end_datetime, p.window_start_datetime) AS DATE) = financial_calendar.date
# MAGIC LEFT JOIN combined_loads ON s.booking_id = combined_loads.booking_id 
# MAGIC LEFT JOIN first_loaded_tracking ON t.truck_load_thing_id = first_loaded_tracking.truck_load_thing_id
# MAGIC WHERE s.`stale?` = false
# MAGIC   AND combined_loads.combined IS NULL 
# MAGIC   AND p.`removed?` = false
# MAGIC   AND s.stop_type = 'pickup'
# MAGIC   AND receiver_state IN ('IL', 'PA')
# MAGIC   AND (
# MAGIC     COALESCE(CAST(first_loaded_tracking.first_loaded AS DATE), CAST(s.in_date_time AS DATE)) IS NULL
# MAGIC     OR COALESCE(CAST(first_loaded_tracking.first_loaded AS DATE), CAST(s.out_date_time AS DATE)) IS NULL
# MAGIC   )
# MAGIC   AND booking_projection.status = 'booked'
# MAGIC  AND customer_name = 'Target' 
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   combined_loads.tender_on_behalf_of_id AS Customer, -- Changes added
# MAGIC   shipment_ids AS Shipment_ID,
# MAGIC   combine_new_rrn AS Load_Number,
# MAGIC   booking_id AS Booking_ID,
# MAGIC   booked_by_name AS Booked_By,
# MAGIC   booked_carrier_name AS Booked_Carrier,
# MAGIC   'pickup' AS Stop_Type,
# MAGIC   facility_name AS Facility_Name,
# MAGIC   p_appt AS Appointment,
# MAGIC   in_date_time AS In_Time,
# MAGIC   out_date_time AS Out_Time,
# MAGIC   last_update_event_name AS Last_Status,
# MAGIC   update_url AS Update_Link,
# MAGIC   from_utc_timestamp(first_loaded, 'America/New_York') AS Marked_Loaded_At
# MAGIC FROM combined_loads
# MAGIC )
# MAGIC select * from system_union
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Target_Updates_GenAI_Temp;
# MAGIC INSERT INTO analytics.Target_Updates_GenAI_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Target_Updates_GenAI

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC truncate table analytics.Target_Updates_GenAI;
# MAGIC
# MAGIC INSERT INTO analytics.Target_Updates_GenAI
# MAGIC SELECT *
# MAGIC FROM analytics.Target_Updates_GenAI_Temp
# MAGIC