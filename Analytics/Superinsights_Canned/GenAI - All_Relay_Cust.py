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
# MAGIC create or replace view bronze.All_Relay_Cust_GenAI as (
# MAGIC     WITH new_pu_appt AS (
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
# MAGIC             b.ready_date::date
# MAGIC         ) AS new_pu_appt,
# MAGIC         schedule_type
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC         AND b.first_shipper_name = p.shipper_name
# MAGIC         AND p.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC         AND s.stop_type = 'pickup'
# MAGIC         AND s.sequence_number = 1
# MAGIC         AND s.`removed?` = 'false'
# MAGIC     WHERE b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
# MAGIC new_del_appt AS (
# MAGIC     SELECT DISTINCT
# MAGIC         b.relay_reference_number,
# MAGIC         CASE 
# MAGIC             WHEN d.appointment_time_local IS NULL THEN d.appointment_date::date 
# MAGIC             ELSE (d.appointment_date || 'T' || d.appointment_time_local)::timestamp 
# MAGIC         END AS del_proj_appt_datetime,
# MAGIC         planning_stop_schedule.appointment_datetime::timestamp AS planning_delappt_datetime,
# MAGIC         window_start_datetime::timestamp,
# MAGIC         window_end_datetime::timestamp,
# MAGIC         COALESCE(
# MAGIC             planning_stop_schedule.appointment_datetime::timestamp, 
# MAGIC             window_start_datetime::timestamp, 
# MAGIC             window_end_datetime::timestamp, 
# MAGIC             CASE 
# MAGIC                 WHEN d.appointment_time_local IS NULL THEN d.appointment_date::date 
# MAGIC                 ELSE (d.appointment_date || 'T' || d.appointment_time_local)::timestamp 
# MAGIC             END
# MAGIC         ) AS new_del_appt,
# MAGIC         schedule_type
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN delivery_projection d ON b.relay_reference_number = d.relay_reference_number 
# MAGIC         AND b.receiver_id = d.receiver_id
# MAGIC     LEFT JOIN planning_stop_schedule ON b.relay_reference_number = planning_stop_schedule.relay_reference_number 
# MAGIC         AND b.receiver_name = planning_stop_schedule.stop_name 
# MAGIC         AND planning_stop_schedule.stop_type = 'delivery'
# MAGIC         AND planning_stop_schedule.`removed?` = 'false'
# MAGIC     WHERE b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
# MAGIC actual_pu AS (
# MAGIC     SELECT DISTINCT
# MAGIC         b.booking_id,
# MAGIC         b.relay_reference_number,
# MAGIC         in_date_time::timestamp AS in_pu_date,
# MAGIC         out_date_time::timestamp AS out_pu_date,
# MAGIC         COALESCE(in_date_time::timestamp, out_date_time::timestamp) AS actual_pu
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC     LEFT JOIN canonical_stop ON b.booking_id = canonical_stop.booking_id 
# MAGIC         AND b.first_shipper_id = canonical_stop.facility_id
# MAGIC     WHERE b.status = 'booked'
# MAGIC         AND canonical_stop.stop_type = 'pickup'
# MAGIC ),
# MAGIC
# MAGIC final_del_seq AS (
# MAGIC     SELECT DISTINCT
# MAGIC         planning_stop_schedule.relay_reference_number,
# MAGIC         MAX(sequence_number) AS final_del_seq
# MAGIC     FROM planning_stop_schedule
# MAGIC     LEFT JOIN booking_projection b ON planning_stop_schedule.relay_reference_number = b.relay_reference_number
# MAGIC     LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC     WHERE planning_stop_schedule.stop_type = 'delivery'
# MAGIC         AND planning_stop_schedule.`removed?` = 'false'
# MAGIC     GROUP BY planning_stop_schedule.relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC final_del_stop_id AS (
# MAGIC     SELECT DISTINCT
# MAGIC         final_del_seq.relay_reference_number,
# MAGIC         final_del_seq,
# MAGIC         planning_stop_schedule.stop_id AS final_del_stop_id
# MAGIC     FROM final_del_seq
# MAGIC     LEFT JOIN planning_stop_schedule ON final_del_seq.relay_reference_number = planning_stop_schedule.relay_reference_number
# MAGIC         AND final_del_seq.final_del_seq = planning_stop_schedule.sequence_number
# MAGIC         AND planning_stop_schedule.`removed?` = 'false'
# MAGIC ),
# MAGIC
# MAGIC actual_del_date AS (
# MAGIC     SELECT DISTINCT
# MAGIC         b.booking_id,
# MAGIC         b.ready_date,
# MAGIC         c.reason_code,
# MAGIC         c.stop_id,
# MAGIC         c.in_date_time::timestamp AS in_del_time,
# MAGIC         c.out_date_time::timestamp AS out_del_time,
# MAGIC         COALESCE(c.in_date_time::timestamp, c.out_date_time::timestamp) AS actual_del_date
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN canonical_stop c ON b.booking_id = c.booking_id 
# MAGIC         AND b.receiver_id = c.facility_id
# MAGIC         AND c.stop_type = 'delivery'
# MAGIC         AND c.`stale?` = 'false'
# MAGIC     JOIN final_del_stop_id ON c.stop_id = final_del_stop_id.final_del_stop_id
# MAGIC     WHERE b.status = 'booked'
# MAGIC ),
# MAGIC
# MAGIC max_time AS (
# MAGIC     SELECT DISTINCT
# MAGIC         relay_reference_number,
# MAGIC         MAX(last_update_date_time) AS max_time
# MAGIC     FROM truckload_projection
# MAGIC     LEFT JOIN customer_profile_projection ON truckload_projection.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC     GROUP BY relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC max_del_code AS (
# MAGIC     SELECT 
# MAGIC         status_datetime, 
# MAGIC         status_reason_code, 
# MAGIC         relay_reference_number 
# MAGIC     FROM integration_tracking_notification_triggered
# MAGIC     WHERE status_type = 'delivery_appointment'
# MAGIC         AND status_reason_code != 'normal_appointment'
# MAGIC ),
# MAGIC
# MAGIC max_pu_code AS (
# MAGIC     SELECT 
# MAGIC         status_datetime, 
# MAGIC         status_reason_code, 
# MAGIC         relay_reference_number 
# MAGIC     FROM integration_tracking_notification_triggered
# MAGIC     WHERE status_type = 'pickup_appointment'
# MAGIC         AND status_reason_code != 'normal_appointment'
# MAGIC ),
# MAGIC
# MAGIC final_status AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t.relay_reference_number,
# MAGIC         t.last_update_date_time,
# MAGIC         t.last_update_event_name AS final_status_original,
# MAGIC         t.status,
# MAGIC         CASE 
# MAGIC             WHEN t.status = 'Delivered' THEN 'Marked Delivered' 
# MAGIC             ELSE t.last_update_event_name 
# MAGIC         END AS final_status
# MAGIC     FROM truckload_projection t
# MAGIC     JOIN max_time ON t.relay_reference_number = max_time.relay_reference_number
# MAGIC         AND t.last_update_date_time = max_time.max_time
# MAGIC ),
# MAGIC
# MAGIC tracking_activity_fourkites AS (
# MAGIC     SELECT DISTINCT
# MAGIC         t.relay_reference_number,
# MAGIC         'tracked_on_fourkites' AS tracked_on_fourkites,
# MAGIC         tracking_activity_v2.truck_load_thing_id
# MAGIC     FROM tracking_activity_v2
# MAGIC     LEFT JOIN truckload_projection t ON tracking_activity_v2.truck_load_thing_id = t.truck_load_thing_id
# MAGIC     WHERE by_system_id = 'four_kites'
# MAGIC         AND t.status != 'CancelledOrBounced'
# MAGIC ),
# MAGIC
# MAGIC system_union AS (
# MAGIC     SELECT DISTINCT
# MAGIC         customer_profile_projection.profit_center as Office,
# MAGIC         customer_profile_projection.customer_name AS `Customer`,
# MAGIC         CAST(b.relay_reference_number AS FLOAT) AS `Load_Number`,
# MAGIC         tendering_acceptance.shipment_id AS `Shipment_ID`,
# MAGIC         b.booking_id AS `Booking_ID`,
# MAGIC         b.booked_by_name AS `Booking_Rep`,
# MAGIC         relay_users.office_id AS `Booking_Office`,
# MAGIC         CAST(b.booked_at AS TIMESTAMP) AS `Booked_At`,
# MAGIC         tendering_service_line.service_line_type AS `Equip`,
# MAGIC         b.booked_carrier_name AS `Carrier`,
# MAGIC         CASE 
# MAGIC             WHEN fourkite_carriers.network_status = 'Connected' THEN 'yes'  
# MAGIC             WHEN tracking_activity_fourkites.tracked_on_fourkites = 'tracked_on_fourkites' THEN 'yes' 
# MAGIC             ELSE 'no' 
# MAGIC         END AS `Connected_to_4Kites`,
# MAGIC         b.status AS `Load Status`,
# MAGIC         final_status.final_status AS `Last_Update`,
# MAGIC         CAST(final_status.last_update_date_time AS TIMESTAMP) AS `Last_Update_Time`,
# MAGIC         b.first_shipper_name AS `Shipper`,
# MAGIC         CONCAT(INITCAP(b.first_shipper_city), ',', first_shipper_state) AS `Origin`,
# MAGIC         b.receiver_name AS `Receiver`,
# MAGIC         CONCAT(INITCAP(b.receiver_city), ',', receiver_state) AS `Destination`,
# MAGIC         DATE_FORMAT(CAST(b.ready_date AS DATE), 'yyyy-MM-dd') AS `Ready_Date`,
# MAGIC         new_pu_appt.schedule_type AS `PU_Schedule_Type`,
# MAGIC         DATE_FORMAT(CAST(new_pu_appt AS DATE), 'yyyy-MM-dd') AS `PU_Appt_Date`,
# MAGIC         DATE_FORMAT(CAST(new_pu_appt AS TIMESTAMP), 'HH:mm') AS `PU_Appt_Time`,
# MAGIC         DATE_FORMAT(COALESCE(CAST(actual_pu.in_pu_date AS DATE), CAST(actual_pu.out_pu_date AS DATE)), 'yyyy-MM-dd') AS `PU_Date`,
# MAGIC         DATE_FORMAT(CAST(actual_pu.in_pu_date AS TIMESTAMP), 'HH:mm') AS `PU_In_Time`,
# MAGIC         DATE_FORMAT(CAST(actual_pu.out_pu_date AS TIMESTAMP), 'HH:mm') AS `PU_Out_Time`,
# MAGIC         CASE 
# MAGIC             WHEN CAST(actual_pu AS TIMESTAMP) > CAST(new_pu_appt AS TIMESTAMP) THEN 'Late' 
# MAGIC             ELSE 'On-time' 
# MAGIC         END AS `PU_Status`,
# MAGIC         new_del_appt.schedule_type AS `Deliv_Schedule_Type`,
# MAGIC         DATE_FORMAT(CAST(hain_tracking_accuracy_projection.delivery_by_date_at_tender AS DATE), 'yyyy-MM-dd') AS `Must_Del_By`,
# MAGIC         DATE_FORMAT(CAST(new_del_appt AS DATE), 'yyyy-MM-dd') AS `Del_Appt_Date`,
# MAGIC         DATE_FORMAT(CAST(new_del_appt AS TIMESTAMP), 'HH:mm') AS `Del_Appt_Time`,
# MAGIC         DATE_FORMAT(COALESCE(CAST(actual_del_date.in_del_time AS DATE), CAST(actual_del_date.out_del_time AS DATE)), 'yyyy-MM-dd') AS `DEL_Date`,
# MAGIC         DATE_FORMAT(CAST(actual_del_date.in_del_time AS TIMESTAMP), 'HH:mm') AS `DEL_In_Time`,
# MAGIC         DATE_FORMAT(CAST(actual_del_date.out_del_time AS TIMESTAMP), 'HH:mm') AS `DEL_Out_Time`,
# MAGIC         CASE 
# MAGIC             WHEN CAST(actual_del_date AS TIMESTAMP) > CAST(new_del_appt AS TIMESTAMP) THEN 'Late' 
# MAGIC             ELSE 'On-time' 
# MAGIC         END AS `Deliv_Status`,
# MAGIC         max_pu_code.status_reason_code AS `PU_Appt_Reason_Code`,
# MAGIC         max_del_code.status_reason_code AS `Del_Appt_Reason_Code`,
# MAGIC         pnc.internal_note AS `Internal_Note`,
# MAGIC         tendering_orders_product_descriptions.orders_product_descriptions AS `Commodity`
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN actual_pu ON b.booking_id = actual_pu.booking_id
# MAGIC     LEFT JOIN new_pu_appt ON b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC     LEFT JOIN new_del_appt ON b.relay_reference_number = new_del_appt.relay_reference_number
# MAGIC     LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC     LEFT JOIN carrier_projection c ON b.booked_carrier_id = c.carrier_id 
# MAGIC     LEFT JOIN tendering_service_line ON b.relay_reference_number = tendering_service_line.relay_reference_number
# MAGIC     LEFT JOIN actual_del_date ON b.booking_id = actual_del_date.booking_id
# MAGIC     LEFT JOIN tracking_activity_fourkites ON b.relay_reference_number = tracking_activity_fourkites.relay_reference_number
# MAGIC     LEFT JOIN relay_users ON b.booked_by_name = relay_users.full_name AND relay_users.`active?` = TRUE
# MAGIC     LEFT JOIN tendering_acceptance ON b.relay_reference_number = tendering_acceptance.relay_reference_number AND tendering_acceptance.`accepted?` = TRUE
# MAGIC     LEFT JOIN final_status ON b.relay_reference_number = final_status.relay_reference_number
# MAGIC     LEFT JOIN fourkite_carriers ON CAST(c.dot_number AS STRING) = fourkite_carriers.us_dot
# MAGIC     LEFT JOIN planning_note_captured pnc ON pnc.relay_reference_number = b.relay_reference_number
# MAGIC     LEFT JOIN tendering_orders_product_descriptions ON CAST(b.relay_reference_number AS INT) = CAST(tendering_orders_product_descriptions.relay_reference_number AS INT)
# MAGIC     LEFT JOIN hain_tracking_accuracy_projection ON b.relay_reference_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC     LEFT JOIN max_del_code ON b.relay_reference_number = max_del_code.relay_reference_number
# MAGIC     LEFT JOIN max_pu_code ON b.relay_reference_number = max_pu_code.relay_reference_number
# MAGIC     WHERE b.status = 'booked'
# MAGIC     ORDER BY `PU_Appt_Date` DESC  
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM system_union 
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table  analytics.all_relay_cust_genai_temp;
# MAGIC INSERT INTO analytics.all_relay_cust_genai_temp
# MAGIC SELECT *
# MAGIC FROM bronze.All_Relay_Cust_GenAI

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.All_Relay_Cust_GenAI;
# MAGIC
# MAGIC INSERT INTO analytics.All_Relay_Cust_GenAI
# MAGIC SELECT *
# MAGIC FROM analytics.all_relay_cust_genai_temp

# COMMAND ----------

