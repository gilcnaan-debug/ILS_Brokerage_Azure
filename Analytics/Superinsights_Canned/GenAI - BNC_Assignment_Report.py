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
# MAGIC create or replace view bronze.bnc_assignment_genai  as ( 
# MAGIC     
# MAGIC     SELECT
# MAGIC     o.new_office as office,
# MAGIC     p.relay_reference_number,
# MAGIC     booking_id,
# MAGIC     full_name as assigned_to_rep,
# MAGIC     first_shipper_city || ', ' || first_shipper_state as origin,
# MAGIC     coalesce(p.appointment_datetime::date, p.window_start_datetime::date) as pickup_date,
# MAGIC     receiver_city || ', ' || receiver_state as destination,
# MAGIC     coalesce(d.appointment_datetime::date, d.window_start_datetime::date) as delivery_date
# MAGIC FROM
# MAGIC 	booking_projection
# MAGIC 	LEFT JOIN planning_current_assignment a USING(relay_reference_number)
# MAGIC 	LEFT JOIN planning_stop_schedule p on p.relay_reference_number = a.relay_reference_number and p.sequence_number = 1
# MAGIC 	LEFT JOIN planning_stop_schedule d on d.relay_reference_number = a.relay_reference_number and d.sequence_number = stop_count
# MAGIC 	LEFT JOIN relay_users ON assigned_to = user_id
# MAGIC   Left join new_office_lookup_w_tgt o on o.old_office = relay_users.office_id
# MAGIC WHERE
# MAGIC 	status IN (
# MAGIC 		'committed', 'reserved', 'available',
# MAGIC 		'eligible'
# MAGIC 	)
# MAGIC 	AND coalesce(p.appointment_datetime::date, p.window_start_datetime::date) >= CURRENT_DATE
# MAGIC 	
# MAGIC ORDER BY
# MAGIC     pickup_date,
# MAGIC     assigned_to_rep,
# MAGIC     relay_reference_number
# MAGIC )
# MAGIC 	
# MAGIC 	

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.bnc_assignment_genai;
# MAGIC
# MAGIC INSERT INTO analytics.bnc_assignment_genai
# MAGIC SELECT *
# MAGIC FROM bronze.bnc_assignment_genai