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
# MAGIC create or replace view bronze.Dry_Reefer_Loads_Genai as(
# MAGIC WITH final_carrier_rate AS  (
# MAGIC     SELECT  
# MAGIC         p.id,
# MAGIC         pickup_date::DATE,
# MAGIC         COALESCE(try_cast(NULLIF(carrier_line_haul, ':') AS NUMERIC), 0) +
# MAGIC         COALESCE(try_cast(NULLIF(carrier_accessorial_rate1, ':') AS NUMERIC), 0) +
# MAGIC         COALESCE(try_cast(NULLIF(carrier_accessorial_rate2, ':') AS NUMERIC), 0) +
# MAGIC         COALESCE(try_cast(NULLIF(carrier_accessorial_rate3, ':') AS NUMERIC), 0) +
# MAGIC         COALESCE(try_cast(NULLIF(carrier_accessorial_rate4, ':') AS NUMERIC), 0) +
# MAGIC         COALESCE(try_cast(NULLIF(carrier_accessorial_rate5, ':') AS NUMERIC), 0) +
# MAGIC         COALESCE(try_cast(NULLIF(carrier_accessorial_rate6, ':') AS NUMERIC), 0) +
# MAGIC         COALESCE(try_cast(NULLIF(carrier_accessorial_rate7, ':') AS NUMERIC), 0) +
# MAGIC         COALESCE(try_cast(NULLIF(carrier_accessorial_rate8, ':') AS NUMERIC), 0) AS final_carrier_rate
# MAGIC     FROM projection_load_1 p 
# MAGIC     LEFT JOIN projection_load_2 p1 on p.id = p1.id
# MAGIC     LEFT JOIN financial_calendar ON p.pickup_date::DATE = financial_calendar.date::DATE 
# MAGIC     WHERE p1.status NOT LIKE '%VOID%'
# MAGIC       AND p.pickup_date::DATE BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '1 week'
# MAGIC ),
# MAGIC
# MAGIC new_pu_appt AS  (
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
# MAGIC             b.ready_date::TIMESTAMP
# MAGIC         ) AS new_pu_appt
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC         AND b.first_shipper_name = p.shipper_name
# MAGIC         AND p.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC         AND s.stop_type = 'pickup'
# MAGIC         AND s.sequence_number = 1
# MAGIC         AND `removed?` = 'false'
# MAGIC     WHERE b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
# MAGIC assignment_start AS  (
# MAGIC     SELECT DISTINCT 
# MAGIC         relay_reference_number AS loadnum,
# MAGIC         MAX(assigned_at) AS max_assigned
# MAGIC     FROM planning_current_assignment
# MAGIC     WHERE assigned_to IS NOT NULL 
# MAGIC     GROUP BY relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC assigned_to AS  (
# MAGIC     SELECT DISTINCT 
# MAGIC         pca.relay_reference_number,
# MAGIC         pca.assigned_to AS assigned_id,
# MAGIC         pca.assigned_at,
# MAGIC         ru.full_name AS assigned_to,
# MAGIC         ru.office_id
# MAGIC     FROM planning_current_assignment pca
# MAGIC     JOIN assignment_start ast ON pca.relay_reference_number = ast.loadnum
# MAGIC         AND pca.assigned_at = ast.max_assigned
# MAGIC     LEFT JOIN relay_users ru ON pca.assigned_to = ru.user_id AND ru.`active?` = 'true'
# MAGIC     WHERE pca.assigned_to IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC new_del_appt AS  (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.relay_reference_number,
# MAGIC         COALESCE(
# MAGIC             CASE WHEN d.appointment_time_local IS NULL THEN d.appointment_date::DATE ELSE 
# MAGIC             (d.appointment_date || 'T' || d.appointment_time_local)::TIMESTAMP END,
# MAGIC             planning_stop_schedule.appointment_datetime::TIMESTAMP,
# MAGIC             planning_stop_schedule.window_start_datetime::TIMESTAMP,
# MAGIC             planning_stop_schedule.window_end_datetime::TIMESTAMP
# MAGIC         ) AS new_del_appt
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN delivery_projection d ON b.relay_reference_number = d.relay_reference_number 
# MAGIC         AND b.receiver_id = d.receiver_id 
# MAGIC     LEFT JOIN planning_stop_schedule ON b.relay_reference_number = planning_stop_schedule.relay_reference_number 
# MAGIC         AND b.receiver_name = planning_stop_schedule.stop_name 
# MAGIC         AND planning_stop_schedule.stop_type = 'delivery'
# MAGIC         AND `removed?` = 'false'
# MAGIC     WHERE b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
# MAGIC drop_picks AS  (
# MAGIC     SELECT DISTINCT 
# MAGIC         ps.relay_reference_number,
# MAGIC         'Drop Trailer Pick' AS drop_trailer_status
# MAGIC     FROM planning_stop_schedule ps
# MAGIC     LEFT JOIN booking_projection bp ON ps.relay_reference_number = bp.relay_reference_number
# MAGIC     WHERE bp.status IN ('available', 'eligible')
# MAGIC       AND ps.schedule_type LIKE '%drop_trailer%'
# MAGIC       AND ps.`removed?` = 'false'
# MAGIC ),
# MAGIC
# MAGIC relay_stop_count AS  (
# MAGIC     SELECT DISTINCT 
# MAGIC         ps.relay_reference_number,
# MAGIC         MAX(sequence_number) FILTER (WHERE ps.stop_type = 'pickup') AS num_picks,
# MAGIC         COUNT(ps.relay_reference_number) FILTER (WHERE ps.stop_type = 'delivery') AS num_drops
# MAGIC     FROM planning_stop_schedule ps
# MAGIC     GROUP BY ps.relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC system_union as (SELECT DISTINCT 
# MAGIC     p.id::STRING AS Load_Num,
# MAGIC     aljex_mode_types.equipment_mode AS Shipment_Type,
# MAGIC     1 + CASE WHEN ps1 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps2 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps3 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps4 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps5 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps6 = 'P' THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN ps7 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps8 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps9 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps10 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps11 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps12 = 'P' THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN ps13 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps14 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps15 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps16 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps17 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps18 = 'P' THEN 1 ELSE 0 END 
# MAGIC     -- CASE WHEN ps19 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps20 = 'P' THEN 1 ELSE 0 END 
# MAGIC     AS Num_Picks,
# MAGIC     
# MAGIC     p.pickup_appt_date::DATE AS PU_Appt_Date,
# MAGIC     p.pickup_appt_time::STRING AS PU_Appt_Time,
# MAGIC     INITCAP(origin_city) AS Origin_City,
# MAGIC     origin_state AS Origin_State,
# MAGIC
# MAGIC     1 + CASE WHEN ps1 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps2 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps3 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps4 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps5 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps6 = 'S' THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN ps7 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps8 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps9 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps10 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps11 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps12 = 'S' THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN ps13 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps14 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps15 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps16 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps17 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps18 = 'S' THEN 1 ELSE 0 END 
# MAGIC     -- CASE WHEN ps19 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps20 = 'S' THEN 1 ELSE 0 END 
# MAGIC     AS Num_Drops,
# MAGIC
# MAGIC     p.consignee_appointment_date::DATE AS Delivery_Appt_Date,
# MAGIC     p.consignee_appointment_time::STRING AS Delivery_Appt_Time,
# MAGIC     INITCAP(dest_city) AS Dest_City,
# MAGIC     dest_state AS Dest_State,
# MAGIC
# MAGIC     CASE WHEN weight REGEXP '^[0-9]+(\.[0-9]+)?$' THEN weight::INTEGER
# MAGIC              ELSE 0.00 END AS weight,
# MAGIC
# MAGIC         CASE WHEN miles REGEXP '^[0-9]+(\.[0-9]+)?$' THEN miles::INTEGER
# MAGIC              ELSE 0.00 END AS miles
# MAGIC
# MAGIC FROM projection_load_1 p 
# MAGIC Left join projection_load_2 p1 on p.id = p1.id
# MAGIC LEFT JOIN financial_calendar ON p.pickup_appt_date::DATE = financial_calendar.date 
# MAGIC LEFT JOIN aljex_mode_types ON p.equipment = aljex_mode_types.equipment_type 
# MAGIC LEFT JOIN state_lookup ON state_lookup.state = origin_state
# MAGIC LEFT JOIN state_lookup_two ON state_lookup_two.state_two = dest_state
# MAGIC WHERE 1=1 
# MAGIC --   [[AND {{datee}}]]
# MAGIC --   [[AND {{origin}}]]
# MAGIC --   [[AND {{dest}}]]
# MAGIC --   [[AND aljex_mode_types.equipment_mode = {{modee}}]]
# MAGIC   AND p.pickup_appt_date::DATE BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL 1 WEEK
# MAGIC   AND aljex_mode_types.equipment_mode IN ('V', 'R')
# MAGIC   AND UPPER(p.pickup_appt_time::STRING) NOT LIKE '%DROP%'
# MAGIC   AND p1.status IN ('OPEN', 'ASSIGNED')
# MAGIC   AND p.consignee_appointment_time IS NOT NULL 
# MAGIC   AND p.pickup_appt_time IS NOT NULL 
# MAGIC   AND p.office NOT IN ('10', '34', '51', '54', '61', '62', '63', '64', '74', 'TG')
# MAGIC   AND p.consignee_appointment_date::DATE IS NOT NULL 
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     p.id::STRING AS Load_Num,
# MAGIC     aljex_mode_types.equipment_mode AS Shipment_Type,
# MAGIC     1 + CASE WHEN ps1 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps2 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps3 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps4 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps5 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps6 = 'P' THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN ps7 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps8 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps9 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps10 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps11 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps12 = 'P' THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN ps13 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps14 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps15 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps16 = 'P' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps17 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps18 = 'P' THEN 1 ELSE 0 END 
# MAGIC     -- CASE WHEN ps19 = 'P' THEN 1 ELSE 0 END + CASE WHEN ps20 = 'P' THEN 1 ELSE 0 END 
# MAGIC     AS Num_Picks,
# MAGIC
# MAGIC     p.pickup_appt_date::DATE AS PU_Appt_Date,
# MAGIC     p.pickup_appt_time::STRING AS PU_Appt_Time,
# MAGIC     INITCAP(origin_city) AS Origin_City,
# MAGIC     origin_state AS Origin_State,
# MAGIC
# MAGIC     1 + CASE WHEN ps1 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps2 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps3 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps4 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps5 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps6 = 'S' THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN ps7 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps8 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps9 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps10 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps11 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps12 = 'S' THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN ps13 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps14 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps15 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps16 = 'S' THEN 1 ELSE 0 END + 
# MAGIC     CASE WHEN ps17 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps18 = 'S' THEN 1 ELSE 0 END 
# MAGIC     -- CASE WHEN ps19 = 'S' THEN 1 ELSE 0 END + CASE WHEN ps20 = 'S' THEN 1 ELSE 0 END 
# MAGIC     AS Num_Drops,
# MAGIC
# MAGIC     p.consignee_appointment_date::DATE AS Delivery_Appt_Date,
# MAGIC     p.consignee_appointment_time::STRING AS Delivery_Appt_Time,
# MAGIC     INITCAP(dest_city) AS Dest_City,
# MAGIC     dest_state AS Dest_State,
# MAGIC
# MAGIC     CASE WHEN weight REGEXP '^[0-9]+(\.[0-9]+)?$' THEN weight::INTEGER
# MAGIC              ELSE 0.00 END AS weight,
# MAGIC
# MAGIC         CASE WHEN miles REGEXP '^[0-9]+(\.[0-9]+)?$' THEN miles::INTEGER
# MAGIC              ELSE 0.00 END AS miles
# MAGIC
# MAGIC FROM projection_load_1 p 
# MAGIC left join projection_load_2 p1 on p.id = p1.id
# MAGIC LEFT JOIN financial_calendar ON p.pickup_appt_date::DATE = financial_calendar.date 
# MAGIC LEFT JOIN aljex_mode_types ON p.equipment = aljex_mode_types.equipment_type 
# MAGIC LEFT JOIN state_lookup ON state_lookup.state = origin_state
# MAGIC LEFT JOIN state_lookup_two ON state_lookup_two.state_two = dest_state
# MAGIC WHERE 1=1 
# MAGIC --   [[AND aljex_mode_types.equipment_mode = {{modee}}]]
# MAGIC --   [[AND {{datee}}]]
# MAGIC --   [[AND {{origin}}]]
# MAGIC --   [[AND {{dest}}]]
# MAGIC   AND p.pickup_appt_date::DATE BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL 1 WEEK
# MAGIC   AND p1.status IN ('OPEN', 'ASSIGNED')
# MAGIC   AND UPPER(p.pickup_appt_time::STRING) NOT LIKE '%DROP%'
# MAGIC   AND p.consignee_appointment_time IS NOT NULL 
# MAGIC   AND p.pickup_appt_time IS NOT NULL 
# MAGIC   AND aljex_mode_types.equipment_mode IN ('V', 'R')
# MAGIC   AND p.office IN ('10', '34', '51', '54', '61', '62', '63', '64', '74')
# MAGIC   AND p.consignee_appointment_date::DATE IS NOT NULL 
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     b.relay_reference_number::STRING AS Load_Num,
# MAGIC     CASE WHEN tendering_service_line.service_line_type LIKE 'ref%' THEN 'R'
# MAGIC          WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'V'
# MAGIC          ELSE 'F' END AS Shipment_Type,
# MAGIC
# MAGIC     relay_stop_count.num_picks AS Num_Picks,
# MAGIC     new_pu_appt.new_pu_appt::DATE AS PU_Appt_Date,
# MAGIC     TO_CHAR(new_pu_appt::TIMESTAMP, 'HH:mm') AS PU_Appt_Time,
# MAGIC     INITCAP(b.first_shipper_city) AS Origin_City,
# MAGIC     b.first_shipper_state AS Origin_State,
# MAGIC
# MAGIC     relay_stop_count.num_drops AS Num_Drops,
# MAGIC     new_del_appt::DATE AS Delivery_Appt_Date,
# MAGIC     TO_CHAR(new_del_appt::TIMESTAMP, 'HH:mm') AS Delivery_Appt_Time,
# MAGIC
# MAGIC     INITCAP(b.receiver_city) AS Dest_City,
# MAGIC     b.receiver_state AS Dest_State,
# MAGIC     total_weight::INTEGER AS Weight,
# MAGIC     distance_projection.total_distance::INTEGER AS Miles 
# MAGIC
# MAGIC FROM booking_projection b
# MAGIC LEFT JOIN new_pu_appt ON b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC LEFT JOIN financial_calendar ON new_pu_appt.new_pu_appt::DATE = financial_calendar.date 
# MAGIC LEFT JOIN customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug AND r.status = 'published'
# MAGIC LEFT JOIN tendering_service_line ON b.relay_reference_number = tendering_service_line.relay_reference_number
# MAGIC LEFT JOIN assigned_to ON b.relay_reference_number = assigned_to.relay_reference_number
# MAGIC LEFT JOIN new_del_appt ON b.relay_reference_number = new_del_appt.relay_reference_number
# MAGIC LEFT JOIN distance_projection ON b.relay_reference_number = distance_projection.relay_reference_number
# MAGIC LEFT JOIN drop_picks ON b.relay_reference_number = drop_picks.relay_reference_number
# MAGIC LEFT JOIN relay_stop_count ON b.relay_reference_number = relay_stop_count.relay_reference_number
# MAGIC LEFT JOIN state_lookup ON state_lookup.state = first_shipper_state
# MAGIC LEFT JOIN state_lookup_two ON state_lookup_two.state_two = receiver_state
# MAGIC WHERE 1=1
# MAGIC
# MAGIC   AND b.tender_on_behalf_of_id NOT IN ('usps', 'target')
# MAGIC     AND CASE WHEN tendering_service_line.service_line_type LIKE 'ref%' THEN 'R'
# MAGIC              WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'V'
# MAGIC              ELSE 'F' END IN ('R', 'V','F') 
# MAGIC   AND new_pu_appt::DATE BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL 1 WEEK
# MAGIC   AND tendering_service_line.service_line_type IN ('dry_van', 'refrigerated')
# MAGIC   AND b.status IN ('available', 'eligible')
# MAGIC   AND drop_trailer_status IS NULL 
# MAGIC   AND new_del_appt IS NOT NULL 
# MAGIC   AND TO_CHAR(new_pu_appt::TIMESTAMP, 'HH:mm') != '00:00'
# MAGIC --   [[AND {{datee}}]]
# MAGIC --   [[AND {{origin}}]]
# MAGIC --   [[AND {{dest}}]]
# MAGIC
# MAGIC ORDER BY PU_Appt_Date, PU_Appt_Time)
# MAGIC select * from system_union
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Dry_Reefer_Loads_Genai_Temp;
# MAGIC INSERT INTO analytics.Dry_Reefer_Loads_Genai_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Dry_Reefer_Loads_Genai
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Dry_Reefer_Loads_Genai;
# MAGIC INSERT INTO analytics.Dry_Reefer_Loads_Genai
# MAGIC SELECT *
# MAGIC FROM analytics.Dry_Reefer_Loads_Genai_Temp