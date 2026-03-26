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
# MAGIC create or replace view bronze.Target_LTL_GenAI as (
# MAGIC WITH Target_LTL AS (
# MAGIC SELECT
# MAGIC 	load_number,
# MAGIC 	po_number,
# MAGIC 	
# MAGIC 	carrier_name,
# MAGIC 	carrier_pro_number,
# MAGIC 	pickup_confirmation_number,
# MAGIC 	dispatch_status,
# MAGIC 	pickup_name,
# MAGIC 	pickup_city || ', ' || pickup_state AS pickup_city_state,
# MAGIC 	pickup_zip,
# MAGIC 	consignee_state,
# MAGIC 	COALESCE(window_end_datetime::date, appointment_datetime::date, expected_ship_date::date) AS expected_ship_date,
# MAGIC 	ship_date
# MAGIC FROM
# MAGIC 	big_export_projection
# MAGIC 	JOIN canonical_plan_projection ON load_number = canonical_plan_projection.relay_reference_number
# MAGIC 	LEFT JOIN planning_stop_schedule ON load_number = planning_stop_schedule.relay_reference_number AND stop_type = 'pickup'
# MAGIC 	LEFT JOIN hain_tracking_accuracy_projection ON load_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC 	LEFT JOIN integration_p44_create_shipment_succeeded ON integration_p44_create_shipment_succeeded.relay_reference_number = big_export_projection.load_number
# MAGIC WHERE
# MAGIC 	customer_id = 'target'
# MAGIC 	AND status = 'determined'
# MAGIC 	AND MODE = 'ltl'
# MAGIC 	AND dispatch_status NOT IN ('Delivered', 'Cancelled')
# MAGIC 	AND delivered_date IS NULL
# MAGIC 	AND LEFT(delivery_number, 1) <> '8'
# MAGIC ORDER BY
# MAGIC 	dispatch_status,
# MAGIC 	carrier_id
# MAGIC 	)
# MAGIC select * from Target_LTL
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Target_LTL_GenAI_Temp;
# MAGIC INSERT INTO analytics.Target_LTL_GenAI_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Target_LTL_GenAI

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Target_LTL_GenAI;
# MAGIC INSERT INTO analytics.Target_LTL_GenAI
# MAGIC SELECT *
# MAGIC FROM analytics.Target_LTL_GenAI_Temp