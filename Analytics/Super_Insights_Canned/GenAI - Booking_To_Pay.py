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
# MAGIC create or replace view bronze.Booking_To_Pay_GenAI as(
# MAGIC   
# MAGIC      with booking_to_pay AS (
# MAGIC     SELECT distinct
# MAGIC         CAST (booking_id AS BIGINT) AS Booking_ID,
# MAGIC         booked_by_name AS Booked_By,
# MAGIC         carrier_name AS Booked_Carrier,
# MAGIC         carrier_projection.dot_number AS DOT_Number,
# MAGIC         l.lawson_id AS Lawson_ID,
# MAGIC         CASE 
# MAGIC             WHEN event_at IS NULL THEN 'Not Yet Approved'
# MAGIC             ELSE DATE_FORMAT(event_at, 'yyyy-MM-dd')
# MAGIC         END AS Invoice_Last_Approved_On,
# MAGIC         date_to_pay AS Expected_Payment_Date,
# MAGIC         LEFT(integration_hubtran_vendor_invoice_approved.invoice_number, 22) AS Invoice_Number,
# MAGIC         CASE WHEN notes LIKE '%efs%' THEN TRUE ELSE FALSE END AS `Notes_Contain_EFS?`
# MAGIC         -- CAST(booking_id AS BIGINT) AS Booking_ID
# MAGIC     FROM
# MAGIC         booking_projection
# MAGIC     JOIN carrier_projection ON booking_projection.booked_carrier_id = carrier_projection.carrier_id
# MAGIC     LEFT JOIN aljex_dot_lawson_reference l ON carrier_projection.dot_number = CAST(l.dot_number AS STRING)
# MAGIC     LEFT JOIN integration_hubtran_vendor_invoice_approved USING (booking_id)
# MAGIC     WHERE
# MAGIC         booked_by_name IS NOT NULL
# MAGIC         AND booking_projection.status NOT IN ('cancelled', 'bounced')
# MAGIC         -- [[AND {{booking_id}}]]
# MAGIC         -- [[AND {{dot_number}}]]
# MAGIC     ORDER BY
# MAGIC         Booking_ID
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM booking_to_pay 
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Booking_To_Pay_GenAI_Temp;
# MAGIC INSERT INTO analytics.Booking_To_Pay_GenAI_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Booking_To_Pay_GenAI
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Booking_To_Pay_GenAI;
# MAGIC INSERT INTO analytics.Booking_To_Pay_GenAI
# MAGIC SELECT *
# MAGIC FROM analytics.Booking_To_Pay_GenAI_Temp