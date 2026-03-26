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
# MAGIC create or replace view bronze.Carrier_Destination_Market_Genai
# MAGIC  as( 
# MAGIC SELECT
# MAGIC     date_format(from_utc_timestamp(booking_projection.booked_at, 'UTC'), 'yyyy-MM-dd HH.mm') AS booked_at_utc,
# MAGIC   booking_projection.ready_date AS ready_date,
# MAGIC   booking_projection.booked_carrier_id AS booked_carrier_id,
# MAGIC   booking_projection.relay_reference_number AS relay_reference_number,
# MAGIC   SUBSTRING(booking_projection.receiver_zip, 1, 3) AS Destination_Zip_3,
# MAGIC   market_lookup_two.pickup_zip AS Market_Lookup_Two_pickup_zip,
# MAGIC   market_lookup_two.market AS Market_Lookup_Two_market,
# MAGIC   market_lookup_two.dat_name_short AS Market_Lookup_Two_dat_name_short,
# MAGIC   carrier_projection.carrier_id AS Carrier_Projection_carrier_id,
# MAGIC   carrier_projection.carrier_profile_id AS Carrier_Projection_carrier_profile_id,
# MAGIC   Question_8248.carrier_profile_id AS Question_8248_carrier_profile_id,
# MAGIC   Question_8248.dot_number AS Question_8248_dot_number,
# MAGIC   Question_8248.carrier_name AS Question_8248_carrier_name,
# MAGIC   Question_8248.Market_Lookup_Two_2_market AS Question_8248_Market_Lookup_Two_2_market
# MAGIC FROM
# MAGIC   bronze.booking_projection
# MAGIC   INNER JOIN bronze.market_lookup_two AS market_lookup_two 
# MAGIC     ON SUBSTRING(booking_projection.receiver_zip, 1, 3) = market_lookup_two.pickup_zip
# MAGIC   INNER JOIN bronze.carrier_projection AS carrier_projection 
# MAGIC     ON booking_projection.booked_carrier_id = carrier_projection.carrier_id
# MAGIC   INNER JOIN (
# MAGIC     SELECT
# MAGIC       carrier_profile.carrier_profile_id AS carrier_profile_id,
# MAGIC       carrier_profile.dot_number AS dot_number,
# MAGIC       carrier_profile.mc_number AS mc_number,
# MAGIC       carrier_profile.carrier_name AS carrier_name,
# MAGIC       carrier_profile.status AS status,
# MAGIC       carrier_profile.scac AS scac,
# MAGIC       carrier_profile.enabled_by AS enabled_by,
# MAGIC       carrier_profile.enabled_at AS enabled_at,
# MAGIC       carrier_profile.disabled_by AS disabled_by,
# MAGIC       carrier_profile.disabled_at AS disabled_at,
# MAGIC       carrier_profile.default_rate_con_recipients AS default_rate_con_recipients,
# MAGIC       carrier_profile.cargo_insurances AS cargo_insurances,
# MAGIC       carrier_profile.tax_identifier AS tax_identifier,
# MAGIC       carrier_profile.address_1 AS address_1,
# MAGIC       carrier_profile.address_2 AS address_2,
# MAGIC       carrier_profile.city AS city,
# MAGIC       carrier_profile.state AS state,
# MAGIC       carrier_profile.zip_code AS zip_code,
# MAGIC       carrier_profile.phone_number AS phone_number,
# MAGIC       carrier_profile.highway_id AS highway_id,
# MAGIC       SUBSTRING(carrier_profile.zip_code, 1, 3) AS Zip3,
# MAGIC       market_lookup_two_2.market AS Market_Lookup_Two_2_market,
# MAGIC       market_lookup_two_2.dat_name_short AS Market_Lookup_Two_2_dat_name_short
# MAGIC     FROM
# MAGIC       bronze.carrier_profile
# MAGIC       LEFT JOIN bronze.market_lookup_two AS market_lookup_two_2 
# MAGIC         ON SUBSTRING(carrier_profile.zip_code, 1, 3) = market_lookup_two_2.pickup_zip
# MAGIC   ) AS Question_8248 
# MAGIC   ON carrier_projection.carrier_profile_id = Question_8248.carrier_profile_id
# MAGIC WHERE
# MAGIC   booking_projection.ready_date BETWEEN DATE_SUB(CURRENT_DATE(), 30) AND DATE_SUB(CURRENT_DATE(), 1) 
# MAGIC LIMIT 1048575
# MAGIC     
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Carrier_Destination_Market_Genai_Temp;
# MAGIC
# MAGIC INSERT INTO analytics.Carrier_Destination_Market_Genai_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Carrier_Destination_Market_Genai

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Carrier_Destination_Market_Genai;
# MAGIC
# MAGIC INSERT INTO analytics.Carrier_Destination_Market_Genai
# MAGIC SELECT *
# MAGIC FROM analytics.Carrier_Destination_Market_Genai_Temp