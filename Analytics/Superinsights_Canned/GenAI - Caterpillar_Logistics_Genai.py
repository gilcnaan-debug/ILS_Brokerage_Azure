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
# MAGIC create or replace view bronze.Caterpillar_logistics_Genai as(
# MAGIC   
# MAGIC      
# MAGIC WITH new_pu_appt AS (
# MAGIC     SELECT DISTINCT
# MAGIC         b.relay_reference_number,
# MAGIC         p.appointment_datetime AS pickup_proj_appt_datetime,
# MAGIC         s.appointment_datetime AS planning_appt_datetime,
# MAGIC         s.window_start_datetime,
# MAGIC         s.window_end_datetime,
# MAGIC         COALESCE(
# MAGIC             s.appointment_datetime,
# MAGIC             s.window_start_datetime,
# MAGIC             s.window_end_datetime,
# MAGIC             p.appointment_datetime,
# MAGIC             b.ready_date::DATE
# MAGIC         ) AS new_pu_appt
# MAGIC     FROM
# MAGIC         booking_projection b
# MAGIC     LEFT JOIN pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC         AND b.first_shipper_name = p.shipper_name
# MAGIC         AND p.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC         AND s.stop_type = 'pickup'
# MAGIC         AND s.sequence_number = 1
# MAGIC         AND `removed?` = 'false'
# MAGIC     WHERE
# MAGIC             b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
# MAGIC actual_pu AS (
# MAGIC     SELECT DISTINCT
# MAGIC         b.booking_id,
# MAGIC         b.relay_reference_number,
# MAGIC         in_date_time AS in_pu_date,
# MAGIC         out_date_time AS out_pu_date,
# MAGIC         COALESCE(in_date_time::DATE, out_date_time::DATE) AS actual_pu
# MAGIC     FROM
# MAGIC         booking_projection b
# MAGIC     LEFT JOIN canonical_stop ON b.booking_id = canonical_stop.booking_id AND b.first_shipper_id = canonical_stop.facility_id
# MAGIC     WHERE
# MAGIC         b.status = 'booked'
# MAGIC         AND b.tender_on_behalf_of_id = 'caterpillar_logistics'
# MAGIC         AND canonical_stop.stop_type = 'pickup'
# MAGIC ),
# MAGIC
# MAGIC final_del_seq AS (
# MAGIC     SELECT DISTINCT
# MAGIC         planning_stop_schedule.relay_reference_number,
# MAGIC         MAX(sequence_number) AS final_del_seq
# MAGIC     FROM
# MAGIC         planning_stop_schedule
# MAGIC     WHERE
# MAGIC         planning_stop_schedule.stop_type = 'delivery'
# MAGIC         AND planning_stop_schedule.`removed?` = 'false'
# MAGIC     GROUP BY
# MAGIC         planning_stop_schedule.relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC final_del_stop_id AS (
# MAGIC     SELECT DISTINCT
# MAGIC         final_del_seq.relay_reference_number,
# MAGIC         final_del_seq.final_del_seq,
# MAGIC         planning_stop_schedule.stop_id AS final_del_stop_id
# MAGIC     FROM
# MAGIC         final_del_seq
# MAGIC     LEFT JOIN planning_stop_schedule ON final_del_seq.relay_reference_number = planning_stop_schedule.relay_reference_number
# MAGIC         AND final_del_seq.final_del_seq = planning_stop_schedule.sequence_number
# MAGIC         AND planning_stop_schedule.`removed?` = 'false'
# MAGIC ),
# MAGIC
# MAGIC actual_del_date AS (
# MAGIC     SELECT DISTINCT
# MAGIC         b.booking_id,
# MAGIC         b.ready_date,
# MAGIC         c.stop_id,
# MAGIC         c.in_date_time AS in_del_time,
# MAGIC         c.out_date_time AS out_del_time,
# MAGIC         COALESCE(c.in_date_time::DATE, c.out_date_time::DATE) AS actual_del_date
# MAGIC     FROM
# MAGIC         booking_projection b
# MAGIC     LEFT JOIN canonical_stop c ON b.booking_id = c.booking_id AND b.receiver_id = c.facility_id AND stop_type = 'delivery' AND c.`stale?` = 'false'
# MAGIC     JOIN final_del_stop_id ON c.stop_id = final_del_stop_id.final_del_stop_id
# MAGIC     WHERE
# MAGIC         b.status = 'booked'
# MAGIC ),
# MAGIC
# MAGIC delivery_start AS (
# MAGIC     SELECT DISTINCT
# MAGIC         p.relay_reference_number,
# MAGIC         b.booking_id,
# MAGIC         p.stop_id,
# MAGIC         p.sequence_number,
# MAGIC         c.in_date_time,
# MAGIC         c.out_date_time,
# MAGIC         RANK() OVER (PARTITION BY p.relay_reference_number ORDER BY p.sequence_number) AS ranker
# MAGIC     FROM
# MAGIC         planning_stop_schedule p
# MAGIC     LEFT JOIN booking_projection b ON p.relay_reference_number = b.relay_reference_number
# MAGIC     LEFT JOIN canonical_stop c ON b.booking_id::FLOAT = c.booking_id AND p.stop_id = c.stop_id
# MAGIC     WHERE
# MAGIC         p.stop_type = 'delivery'
# MAGIC         AND b.tender_on_behalf_of_id = 'caterpillar_logistics'
# MAGIC         AND b.status NOT IN ('cancelled', 'bounced')
# MAGIC     ORDER BY
# MAGIC         p.relay_reference_number, sequence_number
# MAGIC ),
# MAGIC
# MAGIC first_delivery AS (
# MAGIC     SELECT DISTINCT
# MAGIC         relay_reference_number,
# MAGIC         stop_id,
# MAGIC         sequence_number,
# MAGIC         ranker,
# MAGIC         in_date_time,
# MAGIC         out_date_time
# MAGIC     FROM
# MAGIC         delivery_start
# MAGIC     WHERE
# MAGIC         ranker = 1
# MAGIC ),
# MAGIC
# MAGIC second_delivery AS (
# MAGIC     SELECT DISTINCT
# MAGIC         relay_reference_number,
# MAGIC         stop_id,
# MAGIC         sequence_number,
# MAGIC         ranker,
# MAGIC         in_date_time,
# MAGIC         out_date_time
# MAGIC     FROM
# MAGIC         delivery_start
# MAGIC     WHERE
# MAGIC         ranker = 2
# MAGIC ),
# MAGIC
# MAGIC system_union AS (
# MAGIC     SELECT DISTINCT
# MAGIC         'RELAY' AS relay,
# MAGIC         r.profit_center AS office,
# MAGIC         UPPER(b.status) AS status,
# MAGIC         b.booked_by_name,
# MAGIC         b.relay_reference_number::FLOAT AS nfi_pro_num,
# MAGIC         tender_reference_numbers_projection.nfi_pro_number AS cat_ref_num,
# MAGIC         new_pu_appt.new_pu_appt::DATE,
# MAGIC         INITCAP(b.first_shipper_city) AS origin_city,
# MAGIC         b.first_shipper_state AS origin_state,
# MAGIC         first_shipper_zip AS origin_zip,
# MAGIC         INITCAP(b.receiver_city) AS dest_city,
# MAGIC         b.receiver_state AS dest_state,
# MAGIC         receiver_zip AS dest_zip,
# MAGIC         CONCAT(INITCAP(b.first_shipper_city), ',', first_shipper_state, '>', INITCAP(b.receiver_city), ',', b.receiver_state) AS lane,
# MAGIC         actual_pu.actual_pu::TIMESTAMP,
# MAGIC         actual_pu.in_pu_date::TIMESTAMP,
# MAGIC         actual_pu.out_pu_date::TIMESTAMP,
# MAGIC         first_delivery.in_date_time AS first_del_in,
# MAGIC         first_delivery.out_date_time AS first_del_out,
# MAGIC         second_delivery.in_date_time AS second_del_in,
# MAGIC         second_delivery.out_date_time AS second_del_out
# MAGIC     FROM
# MAGIC         booking_projection b
# MAGIC     LEFT JOIN new_pu_appt ON b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC     LEFT JOIN actual_del_date ON b.booking_id = actual_del_date.booking_id
# MAGIC     LEFT JOIN financial_calendar ON actual_del_date.actual_del_date = financial_calendar.date
# MAGIC     LEFT JOIN customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug
# MAGIC     LEFT JOIN actual_pu ON b.booking_id = actual_pu.booking_id
# MAGIC     LEFT JOIN first_delivery ON b.relay_reference_number = first_delivery.relay_reference_number
# MAGIC     LEFT JOIN second_delivery ON b.relay_reference_number = second_delivery.relay_reference_number
# MAGIC     LEFT JOIN tender_reference_numbers_projection ON b.relay_reference_number = tender_reference_numbers_projection.relay_reference_number
# MAGIC     WHERE
# MAGIC         b.status = 'booked'
# MAGIC         AND b.first_shipper_state IN ('TX', 'CO', 'PA', 'IL', 'OH')
# MAGIC         AND b.receiver_state IN ('TX', 'OK', 'KS', 'WA', 'WY', 'GA', 'CA')
# MAGIC         AND b.tender_on_behalf_of_id = 'caterpillar_logistics'
# MAGIC ),
# MAGIC
# MAGIC final_code AS (
# MAGIC     SELECT DISTINCT
# MAGIC         nfi_pro_num AS NFI_Pro_Num,
# MAGIC         cat_ref_num AS CAT_Ref_Num,
# MAGIC         CONCAT(origin_city, ',', origin_state) AS Origin,
# MAGIC         CONCAT(dest_city, ',', dest_state) AS Destination,
# MAGIC         new_pu_appt AS PU_Appt,
# MAGIC         in_pu_date AS PU_In_Time,
# MAGIC         out_pu_date AS PU_Out_Time,
# MAGIC         first_del_in AS Del_1_In_Time,
# MAGIC         DATE(first_del_in) AS Del_1_In_Date,
# MAGIC         first_del_out AS Del_1_Out_Time,
# MAGIC         DATE(first_del_out) AS Del_1_Out_Date,
# MAGIC         second_del_in AS Del_2_In_Time,
# MAGIC         second_del_out AS Del_2_Out_Time
# MAGIC     FROM
# MAGIC         system_union
# MAGIC     WHERE
# MAGIC         lane IN (
# MAGIC             'Waco,TX>San Antonio,TX', 'Waco,TX>Wichita,KS', 'Waco,TX>Houston,TX',
# MAGIC             'Denver,CO>Spokane,WA', 'Waco,TX>Oklahoma City,OK', 'Denver,CO>Cheyenne,WY',
# MAGIC             'York,PA>Arvin,CA', 'Champaign,IL>Seguin,TX', 'Clayton,OH>Spokane,WA',
# MAGIC             'Morton,IL>Spokane,WA', 'Morton,IL>Atlanta,GA', 'Denver,CO>Gillette,WY'
# MAGIC         )
# MAGIC )
# MAGIC select * from final_code  order by PU_Appt 
# MAGIC
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Caterpillar_logistics_Genai_Temp;
# MAGIC INSERT INTO analytics.caterpillar_logistics_genai_temp
# MAGIC SELECT *
# MAGIC FROM bronze.Caterpillar_logistics_Genai
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Caterpillar_logistics_Genai;
# MAGIC INSERT INTO analytics.Caterpillar_logistics_Genai
# MAGIC SELECT *
# MAGIC FROM analytics.caterpillar_logistics_genai_temp