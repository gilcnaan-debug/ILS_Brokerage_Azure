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
# MAGIC USE SCHEMA Bronze;
# MAGIC Create or replace view bronze.Wilsonart_Activity_Report_GenAI as ( 
# MAGIC WITH new_pu_appt_wilson AS (
# MAGIC   SELECT
# MAGIC     b.relay_reference_number,
# MAGIC     COALESCE(appointment_datetime, s.window_start_datetime, s.window_end_datetime) AS new_pu_appt,
# MAGIC     CAST(s.window_start_datetime AS TIMESTAMP) AS pu_appt_start,
# MAGIC     CAST(s.window_end_datetime AS TIMESTAMP) AS pu_appt_end
# MAGIC   FROM
# MAGIC     booking_projection b
# MAGIC     LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC       AND s.stop_type = 'pickup'
# MAGIC       AND s.sequence_number = 1
# MAGIC       AND s.`removed?` = FALSE
# MAGIC   WHERE b.status NOT IN ('cancelled', 'bounced')
# MAGIC     --AND b.tender_on_behalf_of_id = 'wilsonart' -- Changes
# MAGIC ),
# MAGIC
# MAGIC new_del_appt_wilson AS (                   
# MAGIC   SELECT  
# MAGIC     b.relay_reference_number,
# MAGIC     COALESCE(appointment_datetime, window_start_datetime, window_end_datetime) AS new_del_appt,
# MAGIC     CAST(window_start_datetime AS TIMESTAMP) AS del_appt_start,
# MAGIC     CAST(window_end_datetime AS TIMESTAMP) AS del_appt_end
# MAGIC   FROM booking_projection b
# MAGIC   LEFT JOIN planning_stop_schedule ON b.relay_reference_number = planning_stop_schedule.relay_reference_number 
# MAGIC     AND b.receiver_name = planning_stop_schedule.stop_name 
# MAGIC     AND INITCAP(b.receiver_city) = INITCAP(planning_stop_schedule.locality)
# MAGIC     AND planning_stop_schedule.stop_type = 'delivery'
# MAGIC     AND planning_stop_schedule.`removed?` = FALSE
# MAGIC   WHERE b.status NOT IN ('cancelled','bounced')
# MAGIC     --AND b.tender_on_behalf_of_id = 'wilsonart' - Changes
# MAGIC ),
# MAGIC
# MAGIC system_union AS (
# MAGIC   SELECT DISTINCT
# MAGIC     'RELAY' AS system,
# MAGIC     CAST(b.relay_reference_number AS INT) AS loadnum,
# MAGIC     new_pu_appt,
# MAGIC     new_del_appt,
# MAGIC     first_shipper_name,
# MAGIC     `team_driver?`,
# MAGIC     INITCAP(b.first_shipper_city) AS origin_city,
# MAGIC     b.first_shipper_state AS origin_state,
# MAGIC     first_shipper_zip AS origin_zip,
# MAGIC     receiver_name,
# MAGIC     INITCAP(b.receiver_city) AS receiver_city,
# MAGIC     b.receiver_state,
# MAGIC     receiver_zip AS dest_zip,
# MAGIC     tender_reference_numbers_projection.original_shipment_id,
# MAGIC     tender_reference_numbers_projection.target_shipment_id,
# MAGIC     service_line_type,
# MAGIC     CAST(invoiced_at AS DATE) AS invoiced_at,
# MAGIC     CAST(new_pu_appt AS STRING) AS pu_appt_start_final,
# MAGIC     CAST(pu_appt_end AS STRING) AS pu_appt_end,
# MAGIC     CAST(new_del_appt AS STRING) AS del_appt_start_final,
# MAGIC     CAST(del_appt_end AS STRING) AS del_appt_end,
# MAGIC     truckload_projection.status,
# MAGIC     INITCAP(b.status) AS booking_status
# MAGIC   FROM booking_projection b
# MAGIC   LEFT JOIN new_pu_appt_wilson ON b.relay_reference_number = new_pu_appt_wilson.relay_reference_number
# MAGIC   LEFT JOIN new_del_appt_wilson ON new_del_appt_wilson.relay_reference_number = b.relay_reference_number
# MAGIC   LEFT JOIN financial_calendar ON CAST(new_pu_appt_wilson.new_pu_appt AS DATE) = financial_calendar.date 
# MAGIC   LEFT JOIN planning_team_driver_marked ON b.relay_reference_number = planning_team_driver_marked.relay_reference_number
# MAGIC   LEFT JOIN tender_reference_numbers_projection ON tender_reference_numbers_projection.relay_reference_number = b.relay_reference_number
# MAGIC   LEFT JOIN tendering_service_line ON tendering_service_line.relay_reference_number = b.relay_reference_number
# MAGIC   LEFT JOIN tl_invoice_projection ON tl_invoice_projection.relay_reference_number = b.relay_reference_number
# MAGIC   LEFT JOIN truckload_projection ON b.booking_id = truckload_projection.booking_id
# MAGIC   WHERE b.status NOT IN ('cancelled', 'bounced')
# MAGIC     -- ${datee}
# MAGIC     --AND b.tender_on_behalf_of_id = 'wilsonart'-- Changes
# MAGIC ), final_code as (SELECT
# MAGIC     CASE WHEN invoiced_at IS NOT NULL THEN 'Invoiced' ELSE COALESCE(status, booking_status) END AS Status,
# MAGIC     loadnum AS Load_Number,
# MAGIC     CASE WHEN service_line_type = 'dry_van' THEN 'dry van' ELSE service_line_type END AS Equipment,
# MAGIC     CASE WHEN `team_driver?` IS TRUE THEN 'Yes' ELSE 'No' END AS `Team_Driver?`,
# MAGIC     original_shipment_id AS Shipment_ID,
# MAGIC     target_shipment_id AS PO_Number,
# MAGIC     INITCAP(first_shipper_name) AS Shipper,
# MAGIC     origin_city AS PU_City,
# MAGIC     origin_state AS PU_State,
# MAGIC     CAST(new_pu_appt AS DATE) AS PU_Appt_Day,
# MAGIC     CASE 
# MAGIC       WHEN pu_appt_end IS NULL THEN DATE_FORMAT(pu_appt_start_final, 'HH:mm:ss')
# MAGIC       ELSE CONCAT(DATE_FORMAT(pu_appt_start_final, 'HH:mm:ss'), '-', DATE_FORMAT(pu_appt_end, 'HH:mm:ss'))
# MAGIC     END AS PU_Appt_Time,
# MAGIC     INITCAP(receiver_name) AS Receiver_Name,
# MAGIC     receiver_city AS Receiver_City,
# MAGIC     receiver_state AS Receiver_State,
# MAGIC     CAST(new_del_appt AS DATE) AS Del_Appt_Day,
# MAGIC     CASE 
# MAGIC       WHEN del_appt_end IS NULL THEN DATE_FORMAT(del_appt_start_final, 'HH:mm:ss')
# MAGIC       ELSE CONCAT(DATE_FORMAT(del_appt_start_final, 'HH:mm:ss'), '-', DATE_FORMAT(del_appt_end, 'HH:mm:ss'))
# MAGIC     END AS Del_Appt_Time
# MAGIC   FROM
# MAGIC     system_union
# MAGIC   ORDER BY
# MAGIC     new_pu_appt)
# MAGIC 	
# MAGIC 	select * from final_code 
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Wilsonart_Activity_Report_GenAI;
# MAGIC INSERT INTO analytics.Wilsonart_Activity_Report_GenAI
# MAGIC SELECT *
# MAGIC FROM bronze.Wilsonart_Activity_Report_GenAI