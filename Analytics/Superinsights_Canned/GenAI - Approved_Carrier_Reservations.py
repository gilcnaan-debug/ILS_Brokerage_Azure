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
# MAGIC create or replace view bronze.Approved_Carrier_Reservations_Genai
# MAGIC  as (
# MAGIC     
# MAGIC with try as ( SELECT
# MAGIC   CAST(reserved_at AS TIMESTAMP) AS reserved_at,
# MAGIC     CAST(reserved_at AS DATE) AS reserved_date,
# MAGIC
# MAGIC   booking_projection.status,
# MAGIC   booking_projection.relay_reference_number AS load_number,
# MAGIC   c.primary_relay_user_id,
# MAGIC --   cu.full_name AS primary_am,
# MAGIC   booking_projection.booking_id,
# MAGIC   ca.full_name AS reserved_by,
# MAGIC   carrier_projection.carrier_name,
# MAGIC   COALESCE(CAST(appointment_datetime AS STRING), CONCAT(window_start_datetime, '-', window_end_datetime)) AS pickup_schedule,
# MAGIC   CONCAT(INITCAP(first_shipper_city), ', ', UPPER(first_shipper_state)) AS origin,
# MAGIC   CONCAT(INITCAP(receiver_city), ', ', UPPER(receiver_state)) AS destination
# MAGIC FROM
# MAGIC   booking_projection
# MAGIC   JOIN planning_stop_schedule USING (relay_reference_number)
# MAGIC   JOIN carrier_projection ON booking_projection.reserved_carrier_id = carrier_projection.carrier_id
# MAGIC   JOIN relay_users ca ON booking_projection.reserved_by = ca.user_id
# MAGIC   JOIN customer_profile_projection ON booking_projection.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC   left join bronze.customer_profile_projection c 
# MAGIC on booking_projection.tender_on_behalf_of_id = c.customer_slug
# MAGIC --   JOIN relay_users cu ON booking_projection.primary_relay_user_id = cu.user_id
# MAGIC   LEFT JOIN analytics.dim_financial_calendar ON dim_financial_calendar.date = CAST(booking_projection.reserved_at AS DATE)
# MAGIC WHERE
# MAGIC   planning_stop_schedule.sequence_number = 1
# MAGIC   /*AND booking_projection.status = 'reserved'*/
# MAGIC   AND carrier_projection.status = 'approved'
# MAGIC   AND booking_projection.tender_on_behalf_of_id <> 'target'
# MAGIC -- [[AND {{reserved}}]]
# MAGIC ORDER BY
# MAGIC   reserved_at DESC),
# MAGIC
# MAGIC   final_reserved as ( select 
# MAGIC   reserved_at,
# MAGIC reserved_date,
# MAGIC status,
# MAGIC  load_number,
# MAGIC   cu.full_name AS primary_am,
# MAGIC  booking_id,
# MAGIC   reserved_by,
# MAGIC carrier_name,
# MAGIC  pickup_schedule,
# MAGIC  origin,
# MAGIC  destination from try JOIN relay_users cu ON try.primary_relay_user_id = cu.user_id
# MAGIC  )
# MAGIC  select * from final_reserved
# MAGIC   
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Approved_Carrier_Reservations;
# MAGIC INSERT INTO analytics.Approved_Carrier_Reservations
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bronze.Approved_Carrier_Reservations_Genai
# MAGIC