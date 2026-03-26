# Databricks notebook source
# MAGIC %md
# MAGIC ## Canned Reprorts
# MAGIC * **Description:** To Replicate Metabase Canned Reports for Niffy
# MAGIC * **Created Date:** 01/15/2025
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 01/15/2025
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating View

# COMMAND ----------

# MAGIC %sql
# MAGIC SET ANSI_MODE = false;
# MAGIC USE SCHEMA bronze;
# MAGIC create or replace view bronze.Relay_Aljex_Status_Genai
# MAGIC  as(
# MAGIC WITH new_pu_appt AS (
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
# MAGIC     LEFT JOIN pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC       AND b.first_shipper_name = p.shipper_name
# MAGIC       AND p.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC       AND s.stop_type = 'pickup'
# MAGIC       AND s.sequence_number = 1
# MAGIC       AND s.`removed?` = 'false'
# MAGIC     WHERE b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
# MAGIC new_del_appt AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.relay_reference_number,
# MAGIC         CASE WHEN d.appointment_time_local IS NULL THEN d.appointment_date::DATE ELSE 
# MAGIC             (d.appointment_date || 'T' || d.appointment_time_local)::TIMESTAMP END AS del_proj_appt_datetime,
# MAGIC         planning_stop_schedule.appointment_datetime::TIMESTAMP AS planning_delappt_datetime,
# MAGIC         window_start_datetime::TIMESTAMP,
# MAGIC         window_end_datetime::TIMESTAMP,
# MAGIC         COALESCE(
# MAGIC             planning_stop_schedule.appointment_datetime::TIMESTAMP,
# MAGIC             window_start_datetime::TIMESTAMP,
# MAGIC             window_end_datetime::TIMESTAMP,
# MAGIC             CASE WHEN d.appointment_time_local IS NULL THEN d.appointment_date::DATE ELSE 
# MAGIC                 (d.appointment_date || 'T' || d.appointment_time_local)::TIMESTAMP END
# MAGIC         ) AS new_del_appt
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN delivery_projection d ON b.relay_reference_number = d.relay_reference_number 
# MAGIC       AND b.receiver_id = d.receiver_id 
# MAGIC     LEFT JOIN planning_stop_schedule ON b.relay_reference_number = planning_stop_schedule.relay_reference_number 
# MAGIC       AND b.receiver_name = planning_stop_schedule.stop_name 
# MAGIC       AND planning_stop_schedule.stop_type = 'delivery'
# MAGIC       AND planning_stop_schedule.`removed?` = 'false'
# MAGIC     WHERE b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
# MAGIC actual_pu AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.booking_id, 
# MAGIC         b.relay_reference_number,
# MAGIC         in_date_time::TIMESTAMP AS in_pu_date,
# MAGIC         out_date_time::TIMESTAMP AS out_pu_date,
# MAGIC         COALESCE(in_date_time::TIMESTAMP, out_date_time::TIMESTAMP) AS actual_pu
# MAGIC     FROM booking_projection b 
# MAGIC     LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug 
# MAGIC     LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN canonical_stop ON b.booking_id = canonical_stop.booking_id AND b.first_shipper_id = canonical_stop.facility_id
# MAGIC     LEFT JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC     WHERE b.status = 'booked' 
# MAGIC       AND canonical_stop.stop_type = 'pickup'
# MAGIC       -- [[AND {{customer}}]]
# MAGIC       -- [[AND {{office}}]]
# MAGIC ),
# MAGIC
# MAGIC final_del_seq AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         planning_stop_schedule.relay_reference_number,
# MAGIC         MAX(sequence_number) AS final_del_seq
# MAGIC     FROM planning_stop_schedule
# MAGIC     LEFT JOIN booking_projection b ON planning_stop_schedule.relay_reference_number = b.relay_reference_number
# MAGIC     LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug 
# MAGIC     LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC     WHERE planning_stop_schedule.stop_type = 'delivery'
# MAGIC       AND planning_stop_schedule.`removed?` = 'false'
# MAGIC     GROUP BY planning_stop_schedule.relay_reference_number
# MAGIC     -- [[AND {{customer}}]]
# MAGIC     -- [[AND {{office}}]]
# MAGIC ),
# MAGIC
# MAGIC final_del_stop_id AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         final_del_seq.relay_reference_number,
# MAGIC         final_del_seq,
# MAGIC         planning_stop_schedule.stop_id AS final_del_stop_id
# MAGIC     FROM final_del_seq
# MAGIC     LEFT JOIN planning_stop_schedule ON final_del_seq.relay_reference_number = planning_stop_schedule.relay_reference_number
# MAGIC       AND final_del_seq.final_del_seq = planning_stop_schedule.sequence_number
# MAGIC       AND planning_stop_schedule.`removed?` = 'false'
# MAGIC ),
# MAGIC
# MAGIC actual_del_date AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.booking_id,
# MAGIC         b.ready_date,
# MAGIC         c.stop_id,
# MAGIC         COALESCE(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) AS actual_del_date
# MAGIC     FROM booking_projection b 
# MAGIC     LEFT JOIN canonical_stop c ON b.booking_id = c.booking_id 
# MAGIC       AND b.receiver_id = c.facility_id
# MAGIC       AND stop_type = 'delivery'
# MAGIC       AND c.`stale?` = 'false'
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
# MAGIC     LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC     GROUP BY relay_reference_number
# MAGIC     -- [[AND {{customer}}]]
# MAGIC     -- [[AND {{office}}]]
# MAGIC ),
# MAGIC
# MAGIC final_status AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         t.relay_reference_number,
# MAGIC         t.last_update_date_time,
# MAGIC         t.last_update_event_name AS final_status_original,
# MAGIC         t.status,
# MAGIC         CASE WHEN t.status = 'Delivered' THEN 'Marked Delivered' ELSE t.last_update_event_name END AS final_status
# MAGIC     FROM truckload_projection t
# MAGIC     JOIN max_time ON t.relay_reference_number = max_time.relay_reference_number
# MAGIC       AND t.last_update_date_time = max_time.max_time
# MAGIC )
# MAGIC ,
# MAGIC -- Anchoring the main selection
# MAGIC system_union as(
# MAGIC SELECT DISTINCT
# MAGIC     new_office_lookup.new_office AS Office,
# MAGIC     CASE WHEN master_customer_name IS NULL THEN customer_profile_projection.customer_name ELSE master_customer_name END AS Customer,
# MAGIC     new_office_lookup_dup.new_office_dup AS Booking_Office,
# MAGIC     b.booked_by_name AS Booking_Rep,
# MAGIC     aljex_user_report_listing.full_name AS Sales_Rep,
# MAGIC     b.relay_reference_number::FLOAT AS Load_Num,
# MAGIC     b.booking_id::FLOAT AS Booking_ID,
# MAGIC     tender_reference_numbers_projection.nfi_pro_number::STRING AS Ref_Num,
# MAGIC     tendering_service_line.service_line_type AS Equipment,
# MAGIC     b.booked_carrier_name AS Carrier,
# MAGIC     COALESCE(b.driver_name, t.driver_name) AS Driver_Name,
# MAGIC     t.driver_phone_number AS Driver_Phone_Num,
# MAGIC     t.trailer_number AS Trailer_Num,
# MAGIC     t.truck_number AS Truck_Num,
# MAGIC     b.status AS Load_Status,
# MAGIC     final_status.final_status AS Last_Update,
# MAGIC     DATE_FORMAT(TRY_CAST(from_utc_timestamp(final_status.last_update_date_time, 'America/Chicago') AS TIMESTAMP), 'yyyy-MM-dd HH:mm') AS Last_Update_Time, 
# MAGIC     b.first_shipper_name AS Shipper,
# MAGIC     CONCAT(INITCAP(b.first_shipper_city), ',', first_shipper_state) AS Origin,
# MAGIC     b.receiver_name AS Receiver,
# MAGIC     CONCAT(INITCAP(b.receiver_city), ',', receiver_state) AS Destination,
# MAGIC     TO_CHAR(new_pu_appt::DATE, 'yyyy-MM-dd') AS PU_Appt_Date,
# MAGIC     new_pu_appt AS PU_Appt_Time,
# MAGIC     TO_CHAR(actual_pu.in_pu_date::DATE, 'yyyy-MM-dd') AS PU_In_Date,
# MAGIC     actual_pu.in_pu_date AS PU_In_Time,
# MAGIC     TO_CHAR(actual_pu.out_pu_date::DATE, 'yyyy-MM-dd') AS PU_Out_Date,
# MAGIC     actual_pu.out_pu_date AS PU_Out_Time,
# MAGIC     TO_CHAR(new_del_appt::DATE, 'yyyy-MM-dd') AS Del_Appt_Date,
# MAGIC     new_del_appt AS Del_Appt_Time,
# MAGIC     CONCAT('https://relaytms.com/tracking/tracking_load_detail/', b.booking_id) AS Relay_Tracking_Board
# MAGIC FROM booking_projection b
# MAGIC LEFT JOIN actual_pu ON b.booking_id = actual_pu.booking_id
# MAGIC LEFT JOIN new_pu_appt ON b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC LEFT JOIN new_del_appt ON b.relay_reference_number = new_del_appt.relay_reference_number
# MAGIC LEFT JOIN financial_calendar ON COALESCE(actual_pu::DATE, new_pu_appt::DATE) = financial_calendar.date 
# MAGIC LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC LEFT JOIN carrier_projection c ON b.booked_carrier_id = c.carrier_id 
# MAGIC LEFT JOIN truckload_projection t ON b.relay_reference_number = t.relay_reference_number AND t.status != 'CancelledOrBounced'
# MAGIC LEFT JOIN tendering_service_line ON b.relay_reference_number = tendering_service_line.relay_reference_number
# MAGIC LEFT JOIN actual_del_date ON b.booking_id = actual_del_date.booking_id
# MAGIC LEFT JOIN final_status ON b.relay_reference_number = final_status.relay_reference_number
# MAGIC LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32) 
# MAGIC LEFT JOIN tender_reference_numbers_projection ON b.relay_reference_number = tender_reference_numbers_projection.relay_reference_number
# MAGIC LEFT JOIN relay_users ON customer_profile_projection.sales_relay_user_id = relay_users.user_id 
# MAGIC LEFT JOIN aljex_user_report_listing ON relay_users.full_name = aljex_user_report_listing.full_name
# MAGIC LEFT JOIN relay_users ru ON booked_by_name = ru.full_name AND ru.`active?` IS TRUE
# MAGIC LEFT JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC LEFT JOIN new_office_lookup_dup ON new_office_lookup_dup.Old_Office_Dub = ru.office_id
# MAGIC WHERE master_customer_name != 'WCD LOGISTICS LLC'
# MAGIC   AND b.status NOT IN ('cancelled', 'bounced')
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     new_office_lookup.new_office AS office,
# MAGIC     CASE WHEN master_customer_name IS NULL THEN p1.shipper ELSE master_customer_name END AS mastername,
# MAGIC     CASE WHEN key_c_user = 'IMPORT' THEN 'DRAY' ELSE new_office_lookup_dup.new_office_dup END AS pnl_code,
# MAGIC     CASE WHEN r.full_name IS NULL THEN p.key_c_user ELSE r.full_name END AS rep,
# MAGIC     aljex_user_report_listing.full_name AS salesrep,
# MAGIC     p.id::FLOAT,
# MAGIC     NULL::FLOAT,
# MAGIC     p1.ref_num::STRING,
# MAGIC     equipment,
# MAGIC     c.name,
# MAGIC     NULL,
# MAGIC     p.driver_cell_num,
# MAGIC     p1.trailer_number,
# MAGIC     p1.truck_num,
# MAGIC     p1.status,
# MAGIC     NULL,
# MAGIC     NULL::TIMESTAMP,
# MAGIC     pickup_name,
# MAGIC     CONCAT(INITCAP(origin_city), ',', origin_state) AS origin,
# MAGIC     p.consignee,
# MAGIC     CONCAT(INITCAP(dest_city), ',', dest_state) AS destination,
# MAGIC     TO_CHAR(TRY_CAST(p.pickup_appt_date AS DATE), 'yyyy-MM-dd') AS pu_appt_date,
# MAGIC     pickup_appt_time AS pu_appt_time,
# MAGIC     TO_CHAR(TRY_CAST(pickup_date AS DATE), 'yyyy-MM-dd') AS Pu_in_date,
# MAGIC    pickup_time  AS pu_in_time,
# MAGIC     TO_CHAR(TRY_CAST(pickup_date AS DATE), 'yyyy-MM-dd') AS Pu_in_date,
# MAGIC    pickup_time  AS pu_in_time,
# MAGIC     TO_CHAR(TRY_CAST(consignee_appointment_date AS DATE), 'yyyy-MM-dd') AS del_Appt_date,
# MAGIC     consignee_appointment_time AS del_appt_time,
# MAGIC     NULL
# MAGIC FROM projection_load_1 p 
# MAGIC left join projection_load_2 p1 ON p.id = p1.id 
# MAGIC LEFT JOIN financial_calendar ON p.pickup_date::DATE = financial_calendar.date::DATE 
# MAGIC LEFT JOIN customer_lookup ON LEFT(shipper, 32) = LEFT(aljex_customer_name, 32) 
# MAGIC LEFT JOIN aljex_user_report_listing ON p1.sales_rep = aljex_user_report_listing.sales_rep
# MAGIC LEFT JOIN aljex_user_report_listing r ON p.key_c_user = r.aljex_id 
# MAGIC LEFT JOIN projection_carrier c ON p.carrier_id = c.id 
# MAGIC LEFT JOIN new_office_lookup ON p.office = new_office_lookup.old_office
# MAGIC LEFT JOIN new_office_lookup_dup ON new_office_lookup_dup.old_office_dub = r.pnl_code
# MAGIC WHERE office IS NOT NULL 
# MAGIC   AND p1.status NOT LIKE '%VOID%'
# MAGIC   AND master_customer_name != 'WCD LOGISTICS LLC'
# MAGIC
# MAGIC ORDER BY PU_Appt_Date DESC)
# MAGIC select * from system_union 
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Relay_Aljex_Status_Genai_Temp;
# MAGIC INSERT INTO analytics.Relay_Aljex_Status_Genai_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Relay_Aljex_Status_Genai
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Relay_Aljex_Status_Genai;
# MAGIC INSERT INTO analytics.Relay_Aljex_Status_Genai
# MAGIC SELECT *
# MAGIC FROM analytics.Relay_Aljex_Status_Genai_Temp