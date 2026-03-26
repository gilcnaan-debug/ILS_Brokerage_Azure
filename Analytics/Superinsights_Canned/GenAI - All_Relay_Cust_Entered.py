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
# MAGIC create or replace view bronze.All_Relay_Cust_Entered_GenAI as (
# MAGIC     WITH delivery AS (
# MAGIC   SELECT 
# MAGIC     tender_on_behalf_of_id,
# MAGIC     b.relay_reference_number AS relay_ref_number,
# MAGIC     b.booking_id AS booking_id,
# MAGIC     CAST(s.in_date_time AS TIMESTAMP) AS delivery_time_in,
# MAGIC     CAST(s.out_date_time AS TIMESTAMP) AS delivery_time_out,
# MAGIC     facility_id
# MAGIC   FROM booking_projection b 
# MAGIC   LEFT JOIN canonical_stop s ON b.booking_id = s.booking_id AND b.receiver_name = s.facility_name 
# MAGIC   LEFT JOIN customer_profile_projection ON tender_on_behalf_of_id = customer_profile_projection.customer_slug 
# MAGIC   WHERE 1=1 
# MAGIC     --[[AND {{customer}}]]
# MAGIC     AND s.stop_type = 'delivery'
# MAGIC     AND b.status = 'booked'
# MAGIC     AND s.`stale?` = 'false'
# MAGIC ),
# MAGIC
# MAGIC pickup AS (
# MAGIC   SELECT 
# MAGIC     tender_on_behalf_of_id,
# MAGIC     b.relay_reference_number AS relay_ref_number,
# MAGIC     b.booking_id AS booking_id,
# MAGIC     CAST(s.in_date_time AS TIMESTAMP) AS pickup_time_in,
# MAGIC     CAST(s.out_date_time AS TIMESTAMP) AS pickup_time_out,
# MAGIC     facility_id
# MAGIC   FROM booking_projection b 
# MAGIC   LEFT JOIN canonical_stop s ON b.booking_id = s.booking_id AND b.first_shipper_name = s.facility_name 
# MAGIC   LEFT JOIN customer_profile_projection ON tender_on_behalf_of_id = customer_profile_projection.customer_slug 
# MAGIC   WHERE 1=1 
# MAGIC     --[[AND {{customer}}]]
# MAGIC     AND s.stop_type = 'pickup'
# MAGIC     AND b.status = 'booked'
# MAGIC     AND s.`stale?` = 'false'
# MAGIC ),
# MAGIC
# MAGIC new_pu_appt AS (
# MAGIC   SELECT
# MAGIC     b.relay_reference_number,
# MAGIC     CAST(p.appointment_datetime AS TIMESTAMP) AS pickup_proj_appt_datetime,
# MAGIC     CAST(s.appointment_datetime AS TIMESTAMP) AS planning_appt_datetime,
# MAGIC     CAST(s.window_start_datetime AS TIMESTAMP) AS window_start_datetime,
# MAGIC     CAST(s.window_end_datetime AS TIMESTAMP) AS window_end_datetime,
# MAGIC     COALESCE(
# MAGIC       CAST(s.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(s.window_start_datetime AS TIMESTAMP),
# MAGIC       CAST(s.window_end_datetime AS TIMESTAMP),
# MAGIC       CAST(p.appointment_datetime AS TIMESTAMP),
# MAGIC       b.ready_date
# MAGIC     ) AS new_pu_appt
# MAGIC   FROM booking_projection b
# MAGIC   LEFT JOIN pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC     AND b.first_shipper_name = p.shipper_name
# MAGIC     AND p.sequence_number = 1
# MAGIC   LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC     AND s.stop_type = 'pickup'
# MAGIC     AND s.sequence_number = 1
# MAGIC     AND s.`removed?` = 'false'
# MAGIC   LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC   WHERE 1=1
# MAGIC     --[[AND {{customer}}]]
# MAGIC     AND b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC
# MAGIC tracking_incomplete_uninvoiced_loads AS (
# MAGIC   SELECT
# MAGIC     DISTINCT b.relay_reference_number
# MAGIC   FROM
# MAGIC     booking_projection b
# MAGIC     JOIN canonical_stop c USING (booking_id)
# MAGIC   WHERE
# MAGIC     (
# MAGIC       c.in_date_time IS NULL
# MAGIC       OR c.out_date_time IS NULL
# MAGIC     )
# MAGIC     AND c.`stale?` = 'false'
# MAGIC ),
# MAGIC   
# MAGIC finished_loads AS (
# MAGIC   SELECT
# MAGIC     t.relay_reference_number AS loadnum
# MAGIC   FROM
# MAGIC     tendering_acceptance t
# MAGIC     JOIN booking_projection b USING (relay_reference_number)
# MAGIC     LEFT JOIN tl_invoice_projection i USING (relay_reference_number)
# MAGIC     LEFT JOIN tracking_incomplete_uninvoiced_loads x USING (relay_reference_number)
# MAGIC   WHERE
# MAGIC     b.status = 'booked'
# MAGIC     AND x.relay_reference_number IS NULL
# MAGIC     AND i.invoiced_at IS NULL
# MAGIC     AND t.`accepted?` = TRUE
# MAGIC ),
# MAGIC   
# MAGIC final_Code as ( 
# MAGIC   SELECT 
# MAGIC   DISTINCT booking_projection.booking_id,
# MAGIC   customer_profile_projection.profit_center AS Office,
# MAGIC   loadnum AS Load_Number,
# MAGIC   tendering_acceptance.shipment_id AS Cust_Ref_Num,
# MAGIC   customer_profile_projection.customer_name AS Customer,
# MAGIC   booking_projection.booked_by_name AS Booked_By,
# MAGIC   CAST(new_pu_appt.new_pu_appt AS DATE) AS PU_Appt,
# MAGIC   booking_projection.first_shipper_name AS First_Shipper_Name,
# MAGIC   booking_projection.receiver_name AS Last_Receiver_Name,
# MAGIC   CAST(pickup.pickup_time_in AS TIMESTAMP) AS PU_Time_In,
# MAGIC   CAST(pickup.pickup_time_out AS TIMESTAMP) AS PU_Time_Out,
# MAGIC   CAST(delivery.delivery_time_in AS TIMESTAMP) AS Del_Time_In,
# MAGIC   CAST(delivery.delivery_time_out AS TIMESTAMP) AS Del_Time_Out
# MAGIC FROM 
# MAGIC   finished_loads
# MAGIC   LEFT JOIN booking_projection ON loadnum = booking_projection.relay_reference_number
# MAGIC   LEFT JOIN customer_profile_projection ON booking_projection.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC   LEFT JOIN new_pu_appt ON loadnum = new_pu_appt.relay_reference_number
# MAGIC   LEFT JOIN delivery ON booking_projection.booking_id = delivery.booking_id
# MAGIC   LEFT JOIN pickup ON booking_projection.booking_id = pickup.booking_id
# MAGIC   LEFT JOIN tendering_acceptance ON loadnum::BIGINT = tendering_acceptance.relay_reference_number::BIGINT 
# MAGIC WHERE 
# MAGIC   1=1 
# MAGIC   --[[AND {{customer}}]]
# MAGIC   --[[AND {{office}}]]
# MAGIC  --[[AND {{custrefnum}}]]
# MAGIC   --[[AND {{relay_reference_number}}]]
# MAGIC   --[[AND {{rep}}]]
# MAGIC   AND pickup.pickup_time_in IS NOT NULL 
# MAGIC   AND pickup.pickup_time_out IS NOT NULL 
# MAGIC   AND delivery.delivery_time_out IS NOT NULL 
# MAGIC   AND delivery.delivery_time_in IS NOT NULL 
# MAGIC   AND booking_projection.status = 'booked'
# MAGIC ORDER BY 
# MAGIC   PU_Appt, Load_Number)
# MAGIC
# MAGIC  select * from final_code 
# MAGIC
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table  analytics.All_Relay_Cust_Entered_GenAI_Temp;
# MAGIC INSERT INTO analytics.All_Relay_Cust_Entered_GenAI_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.All_Relay_Cust_Entered_GenAI

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.All_Relay_Cust_Entered_GenAI;
# MAGIC
# MAGIC INSERT INTO analytics.All_Relay_Cust_Entered_GenAI
# MAGIC SELECT *
# MAGIC FROM analytics.All_Relay_Cust_Entered_GenAI_Temp

# COMMAND ----------

