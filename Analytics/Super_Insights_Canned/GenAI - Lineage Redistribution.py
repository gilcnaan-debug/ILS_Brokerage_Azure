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
# MAGIC create or replace view bronze.Relay_Daily_Report_GenAI as ( 
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
# MAGIC     WHERE 1=1
# MAGIC         AND b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC  new_del_appt AS (
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
# MAGIC         schedule_type,
# MAGIC         planning_stop_schedule.reference_numbers -- Include reference_numbers here
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN delivery_projection d ON b.relay_reference_number = d.relay_reference_number 
# MAGIC         AND b.receiver_id = d.receiver_id
# MAGIC     LEFT JOIN planning_stop_schedule ON b.relay_reference_number = planning_stop_schedule.relay_reference_number 
# MAGIC         AND b.receiver_name = planning_stop_schedule.stop_name 
# MAGIC         AND planning_stop_schedule.stop_type = 'delivery'
# MAGIC         AND planning_stop_schedule.`removed?` = 'false'
# MAGIC     WHERE 1=1 
# MAGIC         AND b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
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
# MAGIC     WHERE 1=1 
# MAGIC         -- [[AND {{customer}}]]
# MAGIC         -- [[AND {{office}}]]
# MAGIC         AND b.status = 'booked'
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
# MAGIC     WHERE 1=1 
# MAGIC         -- [[AND {{customer}}]]
# MAGIC         -- [[AND {{office}}]]
# MAGIC         AND planning_stop_schedule.stop_type = 'delivery'
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
# MAGIC     WHERE 1=1 
# MAGIC         AND b.status = 'booked'
# MAGIC ),
# MAGIC     max_time AS (
# MAGIC     SELECT DISTINCT
# MAGIC         relay_reference_number,
# MAGIC         MAX(last_update_date_time) AS max_time
# MAGIC     FROM truckload_projection
# MAGIC     LEFT JOIN customer_profile_projection ON truckload_projection.tender_on_behalf_of_id = customer_profile_projection.customer_slug AND customer_profile_projection.status = 'published'
# MAGIC     WHERE 1=1 
# MAGIC         -- [[AND {{customer}}]]
# MAGIC         -- [[AND {{office}}]]
# MAGIC     GROUP BY relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC max_del_code AS (
# MAGIC     SELECT 
# MAGIC         status_datetime, 
# MAGIC         status_reason_code, 
# MAGIC         relay_reference_number 
# MAGIC     FROM integration_tracking_notification_triggered 
# MAGIC     JOIN customer_profile_projection on customer_slug = tender_on_behalf_of_id AND customer_profile_projection.status = 'published'
# MAGIC     WHERE status_type = 'delivery_appointment'
# MAGIC       AND status_reason_code != 'normal_appointment'
# MAGIC ),
# MAGIC  max_schedule as
# MAGIC (SELECT DISTINCT 
# MAGIC     relay_reference_number,
# MAGIC     max(cast(scheduled_at as timestamp)) AS max_schedule
# MAGIC FROM 
# MAGIC     pickup_projection
# MAGIC GROUP BY 
# MAGIC     relay_reference_number),
# MAGIC
# MAGIC max_pu_code AS (
# MAGIC     SELECT 
# MAGIC         status_datetime, 
# MAGIC         status_reason_code, 
# MAGIC         relay_reference_number 
# MAGIC     FROM integration_tracking_notification_triggered
# MAGIC     JOIN customer_profile_projection on customer_slug = tender_on_behalf_of_id AND customer_profile_projection.status = 'published'
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
# MAGIC         AND t.last_update_date_time = max_time.max_time),
# MAGIC max_schedule_del AS (
# MAGIC SELECT
# MAGIC     relay_reference_number,
# MAGIC     MAX(CAST(scheduled_at AS timestamp)) AS max_schedule
# MAGIC FROM 
# MAGIC     bronze.pickup_projection 
# MAGIC GROUP BY 
# MAGIC     relay_reference_number
# MAGIC )
# MAGIC
# MAGIC , last_delivery AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         relay_reference_number,
# MAGIC         MAX(sequence_number) AS last_delivery
# MAGIC     FROM planning_stop_schedule
# MAGIC     WHERE `removed?` = false
# MAGIC     GROUP BY relay_reference_number
# MAGIC )
# MAGIC , truckload_proj_del AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         booking_id,
# MAGIC         MAX(CAST(last_update_date_time AS timestamp)) AS truckload_proj_del
# MAGIC     FROM truckload_projection
# MAGIC     WHERE last_update_event_name = 'MarkedDelivered'
# MAGIC     GROUP BY booking_id),
# MAGIC master_date AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.booking_id,
# MAGIC         b.relay_reference_number,
# MAGIC         b.status,
# MAGIC         'TL' AS ltl_or_tl,
# MAGIC         COALESCE(
# MAGIC             c.in_date_time, 
# MAGIC             c.out_date_time, 
# MAGIC             pss.appointment_datetime, 
# MAGIC             pss.window_end_datetime, 
# MAGIC             pss.window_start_datetime, 
# MAGIC             pp.appointment_datetime,
# MAGIC             CASE
# MAGIC                 WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC             END
# MAGIC         ) AS use_date,
# MAGIC         CASE
# MAGIC             WHEN DATE_FORMAT(COALESCE(
# MAGIC                 c.in_date_time, 
# MAGIC                 c.out_date_time, 
# MAGIC                 pss.appointment_datetime, 
# MAGIC                 pss.window_end_datetime, 
# MAGIC                 pss.window_start_datetime, 
# MAGIC                 pp.appointment_datetime,
# MAGIC                 CASE
# MAGIC                     WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC                     ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC                 END
# MAGIC             ), 'E') = 'Sun' THEN DATE_ADD(DATE(COALESCE(
# MAGIC                 c.in_date_time, 
# MAGIC                 c.out_date_time, 
# MAGIC                 pss.appointment_datetime, 
# MAGIC                 pss.window_end_datetime, 
# MAGIC                 pss.window_start_datetime, 
# MAGIC                 pp.appointment_datetime,
# MAGIC                 CASE
# MAGIC                     WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC                     ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC                 END
# MAGIC             )), 6)
# MAGIC             ELSE DATE_ADD(DATE_TRUNC('week', COALESCE(
# MAGIC                 c.in_date_time, 
# MAGIC                 c.out_date_time, 
# MAGIC                 pss.appointment_datetime, 
# MAGIC                 pss.window_end_datetime, 
# MAGIC                 pss.window_start_datetime, 
# MAGIC                 pp.appointment_datetime,
# MAGIC                 CASE
# MAGIC                     WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC                     ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC                 END
# MAGIC             )), 5)
# MAGIC         END AS end_date,
# MAGIC         CASE
# MAGIC             WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC             ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC         END AS ready_date,
# MAGIC         COALESCE(pss.window_start_datetime, pss.appointment_datetime, pp.appointment_datetime) AS pu_appt_start_datetime,
# MAGIC         pss.window_end_datetime AS pu_appt_end_datetime,
# MAGIC         COALESCE(pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime) AS pu_appt_date,
# MAGIC         pss.schedule_type AS pu_schedule_type,
# MAGIC         c.in_date_time AS pickup_in,
# MAGIC         c.out_date_time AS pickup_out,
# MAGIC         COALESCE(c.in_date_time, c.out_date_time) AS pickup_datetime,
# MAGIC         COALESCE(dpss.window_start_datetime, dpss.appointment_datetime,
# MAGIC             CASE
# MAGIC                 WHEN dp.appointment_time_local IS NULL THEN TO_TIMESTAMP(dp.appointment_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(dp.appointment_date, ' ', dp.appointment_time_local))
# MAGIC             END
# MAGIC         ) AS del_appt_start_datetime,
# MAGIC         dpss.window_end_datetime AS del_appt_end_datetime,
# MAGIC         COALESCE(dpss.appointment_datetime, dpss.window_end_datetime, dpss.window_start_datetime,
# MAGIC             CASE
# MAGIC                 WHEN dp.appointment_time_local IS NULL THEN TO_TIMESTAMP(dp.appointment_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(dp.appointment_date, ' ', dp.appointment_time_local))
# MAGIC             END
# MAGIC         ) AS del_appt_date,
# MAGIC         dpss.schedule_type AS del_schedule_type,
# MAGIC         DATE(hain_tracking_accuracy_projection.delivery_by_date_at_tender) AS Delivery_By_Date,
# MAGIC         cd.in_date_time AS delivery_in,
# MAGIC         cd.out_date_time AS delivery_out,
# MAGIC         COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC         t.truckload_proj_del AS truckload_proj_delivered,
# MAGIC         b.booked_at AS booked_at
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN max_schedule ON b.relay_reference_number = max_schedule.relay_reference_number
# MAGIC     LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC     LEFT JOIN pickup_projection pp ON b.booking_id = pp.booking_id AND b.first_shipper_name = pp.shipper_name AND max_schedule.max_schedule = pp.scheduled_at AND pp.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule pss ON b.relay_reference_number = pss.relay_reference_number AND b.first_shipper_name = pss.stop_name AND pss.sequence_number = 1 AND pss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop c ON pss.stop_id = c.stop_id AND c.`stale?` = false AND c.stop_type = 'pickup'
# MAGIC     LEFT JOIN last_delivery ON b.relay_reference_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number AND b.receiver_name = dp.receiver_name AND max_schedule_del.max_schedule = dp.scheduled_at
# MAGIC     LEFT JOIN planning_stop_schedule dpss ON b.relay_reference_number = dpss.relay_reference_number AND b.receiver_name = dpss.stop_name AND b.receiver_city = dpss.locality AND last_delivery.last_delivery = dpss.sequence_number AND dpss.stop_type = 'delivery' AND dpss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop cd ON dpss.stop_id = cd.stop_id AND b.receiver_city = cd.locality AND cd.`stale?` = false AND cd.stop_type = 'delivery'
# MAGIC     LEFT JOIN hain_tracking_accuracy_projection ON b.relay_reference_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC     LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC     LEFT JOIN truckload_proj_del t ON b.booking_id = t.booking_id
# MAGIC     WHERE b.status != 'cancelled' AND (canonical_plan_projection.mode != 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC     UNION ALL
# MAGIC     SELECT DISTINCT 
# MAGIC         b.load_number AS booking_id,
# MAGIC         b.load_number AS relay_reference_number,
# MAGIC         b.dispatch_status AS status,
# MAGIC         'LTL' AS ltl_or_tl,
# MAGIC         COALESCE(b.ship_date, c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime) AS use_date,
# MAGIC         CASE
# MAGIC             WHEN DATE_FORMAT(COALESCE(b.ship_date, c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime), 'u') = '1' 
# MAGIC             THEN DATE_ADD(DATE(COALESCE(b.ship_date, c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime)), 6)
# MAGIC             ELSE DATE_ADD(DATE_TRUNC('week', COALESCE(b.ship_date, c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime)), 5)
# MAGIC         END AS end_date,
# MAGIC         NULL AS ready_date,
# MAGIC         COALESCE(pss.window_start_datetime, pss.appointment_datetime, pp.appointment_datetime) AS pu_appt_start_datetime,
# MAGIC         pss.window_end_datetime AS pu_appt_end_datetime,
# MAGIC         COALESCE(pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime) AS pu_appt_date,
# MAGIC         pss.schedule_type AS pu_schedule_type,
# MAGIC         COALESCE(b.ship_date, c.in_date_time) AS pickup_in,
# MAGIC         c.out_date_time AS pickup_out,
# MAGIC         COALESCE(b.ship_date, c.in_date_time, c.out_date_time) AS pickup_datetime,
# MAGIC         COALESCE(dpss.window_start_datetime, dpss.appointment_datetime,
# MAGIC             CASE
# MAGIC                 WHEN dp.appointment_time_local IS NULL THEN TO_TIMESTAMP(dp.appointment_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(dp.appointment_date, ' ', dp.appointment_time_local))
# MAGIC             END
# MAGIC         ) AS del_appt_start_datetime,
# MAGIC         dpss.window_end_datetime AS del_appt_end_datetime,
# MAGIC         COALESCE(dpss.appointment_datetime, dpss.window_end_datetime, dpss.window_start_datetime,
# MAGIC             CASE
# MAGIC                 WHEN dp.appointment_time_local IS NULL THEN TO_TIMESTAMP(dp.appointment_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(dp.appointment_date, ' ', dp.appointment_time_local))
# MAGIC             END
# MAGIC         ) AS del_appt_date,
# MAGIC         dpss.schedule_type AS del_schedule_type,
# MAGIC         DATE(hain_tracking_accuracy_projection.delivery_by_date_at_tender) AS Delivery_By_Date,
# MAGIC         COALESCE(b.delivered_date, cd.in_date_time) AS delivery_in,
# MAGIC         cd.out_date_time AS delivery_out,
# MAGIC         COALESCE(b.delivered_date, cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC         NULL AS truckload_proj_delivered,
# MAGIC         NULL AS booked_at
# MAGIC     FROM big_export_projection b
# MAGIC     LEFT JOIN max_schedule ON b.load_number = max_schedule.relay_reference_number
# MAGIC     LEFT JOIN max_schedule_del ON b.load_number = max_schedule_del.relay_reference_number
# MAGIC     LEFT JOIN pickup_projection pp ON b.load_number = pp.relay_reference_number AND b.pickup_name = pp.shipper_name AND b.weight = pp.weight_to_pickup_amount AND b.piece_count = pp.pieces_to_pickup_count AND max_schedule.max_schedule = pp.scheduled_at AND pp.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule pss ON b.load_number = pss.relay_reference_number AND b.pickup_name = pss.stop_name AND pss.sequence_number = 1 AND pss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop c ON pss.stop_id = c.stop_id
# MAGIC     LEFT JOIN delivery_projection dp ON b.load_number = dp.relay_reference_number AND b.consignee_name = dp.receiver_name AND max_schedule_del.max_schedule = dp.scheduled_at
# MAGIC     LEFT JOIN last_delivery ON b.load_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN planning_stop_schedule dpss ON b.load_number = dpss.relay_reference_number AND b.consignee_name = dpss.stop_name AND b.consignee_city = dpss.locality AND last_delivery.last_delivery = dpss.sequence_number AND dpss.stop_type = 'delivery' AND dpss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop cd ON dpss.stop_id = cd.stop_id
# MAGIC     LEFT JOIN hain_tracking_accuracy_projection ON b.load_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC     LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC     WHERE b.dispatch_status != 'Cancelled' AND (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC ),
# MAGIC
# MAGIC system_union as (
# MAGIC SELECT DISTINCT
# MAGIC   customer_profile_projection.customer_name as Customer,
# MAGIC   CAST(b.relay_reference_number AS FLOAT) AS Load_Number,
# MAGIC   tendering_acceptance.shipment_id AS Shipment_ID,
# MAGIC   CAST(tendering_acceptance.accepted_at AS DATE) AS Tender_Accepted,
# MAGIC   b.first_shipper_name AS Shipper,
# MAGIC   CONCAT(INITCAP(b.first_shipper_city), ',', first_shipper_state) AS Origin,
# MAGIC   b.receiver_name AS Receiver,
# MAGIC   CONCAT(INITCAP(b.receiver_city), ',', receiver_state) AS Destination,
# MAGIC   DATE_FORMAT(CAST(b.ready_date AS DATE), 'yyyy-MM-dd') AS Ready_Date,
# MAGIC   DATE_FORMAT(CAST(pu_appt_date AS DATE), 'yyyy-MM-dd') AS PU_Appt_Date,
# MAGIC   DATE_FORMAT(CAST(pickup_datetime AS DATE), 'yyyy-MM-dd') AS Actual_PU,
# MAGIC   DATE_FORMAT(CAST(delivery_by_date_at_tender AS DATE), 'yyyy-MM-dd') AS Must_Del_By,
# MAGIC   DATE_FORMAT(CAST(del_appt_date AS DATE), 'yyyy-MM-dd') AS Del_Appt_Date,
# MAGIC   stop_reference_numbers AS Delivery_Number,
# MAGIC   max_pu_code.status_reason_code AS PU_Appt_Reason_Code,
# MAGIC   max_del_code.status_reason_code AS Del_Appt_Reason_Code,
# MAGIC   tendering_orders_product_descriptions.orders_product_descriptions AS Commodity,
# MAGIC   COALESCE(dp.total_distance, b.total_miles) AS Miles
# MAGIC FROM booking_projection b
# MAGIC LEFT JOIN master_date ON master_date.booking_id = b.booking_id
# MAGIC LEFT JOIN financial_calendar ON CAST(COALESCE(b.ready_date, use_date) AS DATE) = financial_calendar.date 
# MAGIC LEFT JOIN tendering_service_line ON b.relay_reference_number = tendering_service_line.relay_reference_number
# MAGIC LEFT JOIN tendering_acceptance ON b.relay_reference_number = tendering_acceptance.relay_reference_number AND tendering_acceptance.`accepted?` = TRUE
# MAGIC LEFT JOIN final_status ON b.relay_reference_number = final_status.relay_reference_number
# MAGIC LEFT JOIN tendering_orders_product_descriptions ON CAST(b.relay_reference_number AS INT) = CAST(tendering_orders_product_descriptions.relay_reference_number AS INT) 
# MAGIC LEFT JOIN hain_tracking_accuracy_projection hj ON hj.relay_reference_number = b.relay_reference_number
# MAGIC LEFT JOIN actual_del_date ON b.booking_id = actual_del_date.booking_id 
# MAGIC LEFT JOIN max_del_code ON b.relay_reference_number = max_del_code.relay_reference_number
# MAGIC LEFT JOIN max_pu_code ON b.relay_reference_number = max_pu_code.relay_reference_number
# MAGIC LEFT JOIN distance_projection dp ON dp.relay_reference_number = b.relay_reference_number
# MAGIC LEFT JOIN canonical_stop c on b.booking_id = c.booking_id 
# MAGIC     and b.receiver_id = c.facility_id
# MAGIC     and stop_type = 'delivery'
# MAGIC     and `stale?` = 'false'
# MAGIC JOIN customer_profile_projection on customer_profile_projection.customer_slug = b.tender_on_behalf_of_id AND customer_profile_projection.status = 'published'
# MAGIC WHERE b.status IN ('booked', 'eligible')
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM system_union 
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SET legacy_time_parser_policy = LEGACY;
# MAGIC
# MAGIC TRUNCATE TABLE analytics.Relay_Daily_Report_GenAI_Temp;
# MAGIC
# MAGIC INSERT INTO analytics.Relay_Daily_Report_GenAI_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Relay_Daily_Report_GenAI;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE table analytics.Relay_Daily_Report_GenAI;
# MAGIC
# MAGIC INSERT INTO analytics.Relay_Daily_Report_GenAI
# MAGIC SELECT *
# MAGIC FROM analytics.Relay_Daily_Report_GenAI_Temp