# Databricks notebook source
# MAGIC %md
# MAGIC ## Aljex Touches
# MAGIC * **Description:** To extract data from Bronze to Silver as delta file
# MAGIC * **Created Date:** 10/09/2025
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 10/09/2025
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** Logic Update

# COMMAND ----------

# MAGIC %md
# MAGIC Incremental Load

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Aljex_Touches AS
# MAGIC WITH use_dates AS (
# MAGIC   SELECT DISTINCT
# MAGIC     Calendar_date AS use_dates,
# MAGIC 	period_year As period_year,
# MAGIC 	Week_End_Date As Week_End_Date
# MAGIC   FROM analytics.dim_date
# MAGIC   WHERE 1 = 1 
# MAGIC ),
# MAGIC
# MAGIC Silver_Aljex_Touches (
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Booked a Load' AS Activity_Type, -- Not Used 
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_c_time IS NULL 
# MAGIC           OR p.key_c_time = '' 
# MAGIC           OR NOT p.key_c_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_c_date IS NULL 
# MAGIC               OR p.key_c_date = '' 
# MAGIC               OR NOT p.key_c_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_c_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_c_date, ' ', p.key_c_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_c_time IS NULL 
# MAGIC           OR p.key_c_time = '' 
# MAGIC           OR NOT p.key_c_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_c_date IS NULL 
# MAGIC               OR p.key_c_date = '' 
# MAGIC               OR NOT p.key_c_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_c_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_c_date, ' ', p.key_c_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_c_time IS NULL 
# MAGIC           OR p.key_c_time = '' 
# MAGIC           OR NOT p.key_c_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_c_date IS NULL 
# MAGIC               OR p.key_c_date = '' 
# MAGIC               OR NOT p.key_c_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_c_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_c_date, ' ', p.key_c_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS Activity_Date,
# MAGIC   aljex_user_report_listing.full_name As Employee_Name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as Office,
# MAGIC  -- master_am_list.office as office_gs,
# MAGIC   --  master_am_list.job_title,
# MAGIC   --  master_am_list.lss_y_or_no,
# MAGIC   --  master_am_list.email_address,
# MAGIC     p.id::string as Activity_Id,
# MAGIC     1 as Touch_Type,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC     --'no' as contains_hubspot
# MAGIC   ---new_office,
# MAGIC   --p.id,
# MAGIC FROM  bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_c_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_c_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_c_user IS NOT NULL 
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Dispatched Load', -- Not Used
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_h_time IS NULL 
# MAGIC           OR p.key_h_time = '' 
# MAGIC           OR NOT p.key_h_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_h_date IS NULL 
# MAGIC               OR p.key_h_date = '' 
# MAGIC               OR NOT p.key_h_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_h_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_h_date, ' ', p.key_h_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_h_time IS NULL 
# MAGIC           OR p.key_h_time = '' 
# MAGIC           OR NOT p.key_h_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_h_date IS NULL 
# MAGIC               OR p.key_h_date = '' 
# MAGIC               OR NOT p.key_h_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_h_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_h_date, ' ', p.key_h_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_h_time IS NULL 
# MAGIC           OR p.key_h_time = '' 
# MAGIC           OR NOT p.key_h_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_h_date IS NULL 
# MAGIC               OR p.key_h_date = '' 
# MAGIC               OR NOT p.key_h_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_h_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_h_date, ' ', p.key_h_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC     aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as Office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC   --  master_am_list.job_title,
# MAGIC    -- master_am_list.lss_y_or_no,
# MAGIC    -- master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC     --'no' as contains_hubspot
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_h_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_h_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_h_user IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Marked Loaded', -- Not Used
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_l_time IS NULL 
# MAGIC           OR p.key_l_time = '' 
# MAGIC           OR NOT p.key_l_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_l_date IS NULL 
# MAGIC               OR p.key_l_date = '' 
# MAGIC               OR NOT p.key_l_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_l_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_l_date, ' ', p.key_l_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_l_time IS NULL 
# MAGIC           OR p.key_l_time = '' 
# MAGIC           OR NOT p.key_l_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_l_date IS NULL 
# MAGIC               OR p.key_l_date = '' 
# MAGIC               OR NOT p.key_l_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_l_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_l_date, ' ', p.key_l_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_l_time IS NULL 
# MAGIC           OR p.key_l_time = '' 
# MAGIC           OR NOT p.key_l_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_l_date IS NULL 
# MAGIC               OR p.key_l_date = '' 
# MAGIC               OR NOT p.key_l_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_l_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_l_date, ' ', p.key_l_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC    aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as Office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC    -- master_am_list.job_title,
# MAGIC    -- master_am_list.lss_y_or_no,
# MAGIC     --master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC    -- 'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_l_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_l_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_l_user IS NOT NULL
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Will Pick Up', -- Not Used
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_w_time IS NULL 
# MAGIC           OR p.key_w_time = '' 
# MAGIC           OR NOT p.key_w_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_w_date IS NULL 
# MAGIC               OR p.key_w_date = '' 
# MAGIC               OR NOT p.key_w_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_w_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_w_date, ' ', p.key_w_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_w_time IS NULL 
# MAGIC           OR p.key_w_time = '' 
# MAGIC           OR NOT p.key_w_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_w_date IS NULL 
# MAGIC               OR p.key_w_date = '' 
# MAGIC               OR NOT p.key_w_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_w_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_w_date, ' ', p.key_w_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_w_time IS NULL 
# MAGIC           OR p.key_w_time = '' 
# MAGIC           OR NOT p.key_w_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_w_date IS NULL 
# MAGIC               OR p.key_w_date = '' 
# MAGIC               OR NOT p.key_w_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_w_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_w_date, ' ', p.key_w_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC  aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC   --  master_am_list.job_title,
# MAGIC   --  master_am_list.lss_y_or_no,
# MAGIC   --  master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC    -- 'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_w_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_w_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_w_user IS NOT NULL
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Marked Delivered',
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_d_time IS NULL 
# MAGIC           OR p.key_d_time = '' 
# MAGIC           OR NOT p.key_d_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_d_date IS NULL 
# MAGIC               OR p.key_d_date = '' 
# MAGIC               OR NOT p.key_d_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_d_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_d_date, ' ', p.key_d_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_d_time IS NULL 
# MAGIC           OR p.key_d_time = '' 
# MAGIC           OR NOT p.key_d_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_d_date IS NULL 
# MAGIC               OR p.key_d_date = '' 
# MAGIC               OR NOT p.key_d_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_d_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_d_date, ' ', p.key_d_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_d_time IS NULL 
# MAGIC           OR p.key_d_time = '' 
# MAGIC           OR NOT p.key_d_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_d_date IS NULL 
# MAGIC               OR p.key_d_date = '' 
# MAGIC               OR NOT p.key_d_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_d_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_d_date, ' ', p.key_d_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC    aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC    --- master_am_list.job_title,
# MAGIC     --master_am_list.lss_y_or_no,
# MAGIC     --master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC     --'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_d_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_d_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_d_user IS NOT NULL
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Released a Load',
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_r_time IS NULL 
# MAGIC           OR p.key_r_time = '' 
# MAGIC           OR NOT p.key_r_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_r_date IS NULL 
# MAGIC               OR p.key_r_date = '' 
# MAGIC               OR NOT p.key_r_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_r_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_r_date, ' ', p.key_r_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_r_time IS NULL 
# MAGIC           OR p.key_r_time = '' 
# MAGIC           OR NOT p.key_r_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_r_date IS NULL 
# MAGIC               OR p.key_r_date = '' 
# MAGIC               OR NOT p.key_r_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_r_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_r_date, ' ', p.key_r_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_r_time IS NULL 
# MAGIC           OR p.key_r_time = '' 
# MAGIC           OR NOT p.key_r_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_r_date IS NULL 
# MAGIC               OR p.key_r_date = '' 
# MAGIC               OR NOT p.key_r_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_r_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_r_date, ' ', p.key_r_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC        aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as office,
# MAGIC  -- master_am_list.office as office_gs,
# MAGIC    -- master_am_list.job_title,
# MAGIC    -- master_am_list.lss_y_or_no,
# MAGIC    -- master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC    -- 'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_r_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_r_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_r_user IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Created Load',
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If tag_created_by is null/empty/invalid…
# MAGIC         WHEN p2.tag_creation_time IS NULL 
# MAGIC           OR p2.tag_creation_time = '' 
# MAGIC           OR NOT p2.tag_creation_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p2.tag_creation_date IS NULL 
# MAGIC               OR p2.tag_creation_date = '' 
# MAGIC               OR NOT p2.tag_creation_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p2.tag_creation_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p2.tag_creation_date, ' ', p2.tag_creation_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p2.tag_creation_time IS NULL 
# MAGIC           OR p2.tag_creation_time = '' 
# MAGIC           OR NOT p2.tag_creation_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p2.tag_creation_date IS NULL 
# MAGIC               OR p2.tag_creation_date = '' 
# MAGIC               OR NOT p2.tag_creation_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p2.tag_creation_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p2.tag_creation_date, ' ', p2.tag_creation_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p2.tag_creation_time IS NULL 
# MAGIC           OR p2.tag_creation_time = '' 
# MAGIC           OR NOT p2.tag_creation_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p2.tag_creation_date IS NULL 
# MAGIC               OR p2.tag_creation_date = '' 
# MAGIC               OR NOT p2.tag_creation_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p2.tag_creation_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p2.tag_creation_date, ' ', p2.tag_creation_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC      aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC   --  master_am_list.job_title,
# MAGIC    -- master_am_list.lss_y_or_no,
# MAGIC    -- master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC     --'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 as p2 ON p.id = p2.id
# MAGIC left JOIN use_dates ON try_cast(p2.tag_creation_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p2.tag_created_by = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p2.tag_created_by IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Load Pays', -- Not Used
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_p_time IS NULL 
# MAGIC           OR p.key_p_time = '' 
# MAGIC           OR NOT p.key_p_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_p_date IS NULL 
# MAGIC               OR p.key_p_date = '' 
# MAGIC               OR NOT p.key_p_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_p_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_p_date, ' ', p.key_p_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_p_time IS NULL 
# MAGIC           OR p.key_p_time = '' 
# MAGIC           OR NOT p.key_p_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_p_date IS NULL 
# MAGIC               OR p.key_p_date = '' 
# MAGIC               OR NOT p.key_p_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_p_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_p_date, ' ', p.key_p_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_p_time IS NULL 
# MAGIC           OR p.key_p_time = '' 
# MAGIC           OR NOT p.key_p_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_p_date IS NULL 
# MAGIC               OR p.key_p_date = '' 
# MAGIC               OR NOT p.key_p_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_p_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_p_date, ' ', p.key_h_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC     aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as Office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC     --master_am_list.job_title,
# MAGIC     --master_am_list.lss_y_or_no,
# MAGIC     --master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC    -- 'no' as contains_hubspot
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_h_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_h_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_p_user IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC Select Distinct 
# MAGIC  'Aljex - Dispatched_Load',
# MAGIC  try_cast(p.Dispatched_date AS Date) AS Dispatched_date,
# MAGIC  al.full_name AS full_name,
# MAGIC  Period_Year,
# MAGIC  Week_End_Date,
# MAGIC  new_office as Office,
# MAGIC  p.id :: String,
# MAGIC  1 as counter,
# MAGIC  'Aljex' as Source_System_Type
# MAGIC   FROM bronze.projection_load_1 p
# MAGIC   LEFT JOIN bronze.projection_load_2 p2 ON p.id = p2.id
# MAGIC   LEFT JOIN bronze.aljex_user_report_listing al ON p.key_c_user = al.aljex_id
# MAGIC   left JOIN use_dates ON try_cast(p.key_h_date as date )= use_dates.use_dates
# MAGIC   LEFT JOIN bronze.new_office_lookup ON al.pnl_code = new_office_lookup.old_office
# MAGIC   where Dispatched_date IS NOT NULL and full_name is not null
# MAGIC
# MAGIC ),
# MAGIC Final_Code as (
# MAGIC Select 
# MAGIC Concat_ws(Activity_Type,Activity_Date,Activity_Id) As DW_Touch_Id,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touch_Type,
# MAGIC period_year,
# MAGIC Week_End_Date,
# MAGIC Source_System_Type
# MAGIC  from Silver_Aljex_Touches )
# MAGIC
# MAGIC  Select 
# MAGIC  *,
# MAGIC  SHA1(
# MAGIC   Concat(
# MAGIC     COALESCE(Activity_Id::string,''),
# MAGIC     COALESCE(Activity_Type::String,''),
# MAGIC     COALESCE(Activity_Date::String,''),
# MAGIC     COALESCE(Office::String,''),
# MAGIC     COALESCE(Employee_Name::String,''),
# MAGIC     COALESCE(Touch_Type::String,''),
# MAGIC     COALESCE(period_year::String,''),
# MAGIC     COALESCE(Week_End_Date::String,''),
# MAGIC     COALESCE(Source_System_Type,'') 
# MAGIC   )
# MAGIC  )As HashKey from Final_Code

# COMMAND ----------

# MAGIC %md
# MAGIC Full Load

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Aljex_Touches_Full_Load AS
# MAGIC WITH use_dates AS (
# MAGIC   SELECT DISTINCT
# MAGIC     Calendar_date AS use_dates,
# MAGIC 	period_year As period_year,
# MAGIC 	Week_End_Date As Week_End_Date
# MAGIC   FROM analytics.dim_date
# MAGIC   WHERE 1 = 1 
# MAGIC ),
# MAGIC
# MAGIC Silver_Aljex_Touches (
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Booked a Load' AS Activity_Type, -- Not Used 
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_c_time IS NULL 
# MAGIC           OR p.key_c_time = '' 
# MAGIC           OR NOT p.key_c_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_c_date IS NULL 
# MAGIC               OR p.key_c_date = '' 
# MAGIC               OR NOT p.key_c_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_c_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_c_date, ' ', p.key_c_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_c_time IS NULL 
# MAGIC           OR p.key_c_time = '' 
# MAGIC           OR NOT p.key_c_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_c_date IS NULL 
# MAGIC               OR p.key_c_date = '' 
# MAGIC               OR NOT p.key_c_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_c_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_c_date, ' ', p.key_c_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_c_time IS NULL 
# MAGIC           OR p.key_c_time = '' 
# MAGIC           OR NOT p.key_c_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_c_date IS NULL 
# MAGIC               OR p.key_c_date = '' 
# MAGIC               OR NOT p.key_c_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_c_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_c_date, ' ', p.key_c_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS Activity_Date,
# MAGIC   aljex_user_report_listing.full_name As Employee_Name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as Office,
# MAGIC  -- master_am_list.office as office_gs,
# MAGIC   --  master_am_list.job_title,
# MAGIC   --  master_am_list.lss_y_or_no,
# MAGIC   --  master_am_list.email_address,
# MAGIC     p.id::string as Activity_Id,
# MAGIC     1 as Touch_Type,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC     --'no' as contains_hubspot
# MAGIC   ---new_office,
# MAGIC   --p.id,
# MAGIC FROM  bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_c_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_c_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_c_user IS NOT NULL 
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Dispatched Load', -- Not Used
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_h_time IS NULL 
# MAGIC           OR p.key_h_time = '' 
# MAGIC           OR NOT p.key_h_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_h_date IS NULL 
# MAGIC               OR p.key_h_date = '' 
# MAGIC               OR NOT p.key_h_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_h_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_h_date, ' ', p.key_h_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_h_time IS NULL 
# MAGIC           OR p.key_h_time = '' 
# MAGIC           OR NOT p.key_h_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_h_date IS NULL 
# MAGIC               OR p.key_h_date = '' 
# MAGIC               OR NOT p.key_h_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_h_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_h_date, ' ', p.key_h_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_h_time IS NULL 
# MAGIC           OR p.key_h_time = '' 
# MAGIC           OR NOT p.key_h_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_h_date IS NULL 
# MAGIC               OR p.key_h_date = '' 
# MAGIC               OR NOT p.key_h_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_h_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_h_date, ' ', p.key_h_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC     aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as Office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC   --  master_am_list.job_title,
# MAGIC    -- master_am_list.lss_y_or_no,
# MAGIC    -- master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC     --'no' as contains_hubspot
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_h_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_h_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_h_user IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Marked Loaded', -- Not Used
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_l_time IS NULL 
# MAGIC           OR p.key_l_time = '' 
# MAGIC           OR NOT p.key_l_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_l_date IS NULL 
# MAGIC               OR p.key_l_date = '' 
# MAGIC               OR NOT p.key_l_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_l_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_l_date, ' ', p.key_l_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_l_time IS NULL 
# MAGIC           OR p.key_l_time = '' 
# MAGIC           OR NOT p.key_l_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_l_date IS NULL 
# MAGIC               OR p.key_l_date = '' 
# MAGIC               OR NOT p.key_l_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_l_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_l_date, ' ', p.key_l_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_l_time IS NULL 
# MAGIC           OR p.key_l_time = '' 
# MAGIC           OR NOT p.key_l_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_l_date IS NULL 
# MAGIC               OR p.key_l_date = '' 
# MAGIC               OR NOT p.key_l_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_l_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_l_date, ' ', p.key_l_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC    aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as Office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC    -- master_am_list.job_title,
# MAGIC    -- master_am_list.lss_y_or_no,
# MAGIC     --master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC    -- 'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_l_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_l_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_l_user IS NOT NULL
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Will Pick Up', -- Not Used
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_w_time IS NULL 
# MAGIC           OR p.key_w_time = '' 
# MAGIC           OR NOT p.key_w_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_w_date IS NULL 
# MAGIC               OR p.key_w_date = '' 
# MAGIC               OR NOT p.key_w_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_w_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_w_date, ' ', p.key_w_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_w_time IS NULL 
# MAGIC           OR p.key_w_time = '' 
# MAGIC           OR NOT p.key_w_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_w_date IS NULL 
# MAGIC               OR p.key_w_date = '' 
# MAGIC               OR NOT p.key_w_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_w_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_w_date, ' ', p.key_w_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_w_time IS NULL 
# MAGIC           OR p.key_w_time = '' 
# MAGIC           OR NOT p.key_w_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_w_date IS NULL 
# MAGIC               OR p.key_w_date = '' 
# MAGIC               OR NOT p.key_w_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_w_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_w_date, ' ', p.key_w_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC  aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC   --  master_am_list.job_title,
# MAGIC   --  master_am_list.lss_y_or_no,
# MAGIC   --  master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC    -- 'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_w_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_w_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_w_user IS NOT NULL
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Marked Delivered',
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_d_time IS NULL 
# MAGIC           OR p.key_d_time = '' 
# MAGIC           OR NOT p.key_d_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_d_date IS NULL 
# MAGIC               OR p.key_d_date = '' 
# MAGIC               OR NOT p.key_d_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_d_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_d_date, ' ', p.key_d_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_d_time IS NULL 
# MAGIC           OR p.key_d_time = '' 
# MAGIC           OR NOT p.key_d_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_d_date IS NULL 
# MAGIC               OR p.key_d_date = '' 
# MAGIC               OR NOT p.key_d_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_d_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_d_date, ' ', p.key_d_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_d_time IS NULL 
# MAGIC           OR p.key_d_time = '' 
# MAGIC           OR NOT p.key_d_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_d_date IS NULL 
# MAGIC               OR p.key_d_date = '' 
# MAGIC               OR NOT p.key_d_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_d_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_d_date, ' ', p.key_d_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC    aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC    --- master_am_list.job_title,
# MAGIC     --master_am_list.lss_y_or_no,
# MAGIC     --master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC     --'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_d_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_d_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_d_user IS NOT NULL
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Released a Load',
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_r_time IS NULL 
# MAGIC           OR p.key_r_time = '' 
# MAGIC           OR NOT p.key_r_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_r_date IS NULL 
# MAGIC               OR p.key_r_date = '' 
# MAGIC               OR NOT p.key_r_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_r_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_r_date, ' ', p.key_r_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_r_time IS NULL 
# MAGIC           OR p.key_r_time = '' 
# MAGIC           OR NOT p.key_r_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_r_date IS NULL 
# MAGIC               OR p.key_r_date = '' 
# MAGIC               OR NOT p.key_r_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_r_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_r_date, ' ', p.key_r_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_r_time IS NULL 
# MAGIC           OR p.key_r_time = '' 
# MAGIC           OR NOT p.key_r_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_r_date IS NULL 
# MAGIC               OR p.key_r_date = '' 
# MAGIC               OR NOT p.key_r_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_r_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_r_date, ' ', p.key_r_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC        aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as office,
# MAGIC  -- master_am_list.office as office_gs,
# MAGIC    -- master_am_list.job_title,
# MAGIC    -- master_am_list.lss_y_or_no,
# MAGIC    -- master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC    -- 'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_r_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_r_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_r_user IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Created Load',
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If tag_created_by is null/empty/invalid…
# MAGIC         WHEN p2.tag_creation_time IS NULL 
# MAGIC           OR p2.tag_creation_time = '' 
# MAGIC           OR NOT p2.tag_creation_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p2.tag_creation_date IS NULL 
# MAGIC               OR p2.tag_creation_date = '' 
# MAGIC               OR NOT p2.tag_creation_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p2.tag_creation_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p2.tag_creation_date, ' ', p2.tag_creation_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p2.tag_creation_time IS NULL 
# MAGIC           OR p2.tag_creation_time = '' 
# MAGIC           OR NOT p2.tag_creation_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p2.tag_creation_date IS NULL 
# MAGIC               OR p2.tag_creation_date = '' 
# MAGIC               OR NOT p2.tag_creation_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p2.tag_creation_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p2.tag_creation_date, ' ', p2.tag_creation_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p2.tag_creation_time IS NULL 
# MAGIC           OR p2.tag_creation_time = '' 
# MAGIC           OR NOT p2.tag_creation_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p2.tag_creation_date IS NULL 
# MAGIC               OR p2.tag_creation_date = '' 
# MAGIC               OR NOT p2.tag_creation_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p2.tag_creation_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p2.tag_creation_date, ' ', p2.tag_creation_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC      aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC   --  master_am_list.job_title,
# MAGIC    -- master_am_list.lss_y_or_no,
# MAGIC    -- master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC     --'no' as contains_hubspot
# MAGIC
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 as p2 ON p.id = p2.id
# MAGIC left JOIN use_dates ON try_cast(p2.tag_creation_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p2.tag_created_by = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p2.tag_created_by IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC   'Aljex - Load Pays', -- Not Used
# MAGIC   CASE
# MAGIC     WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC       CASE
# MAGIC         -- If key_c_time is null/empty/invalid…
# MAGIC         WHEN p.key_p_time IS NULL 
# MAGIC           OR p.key_p_time = '' 
# MAGIC           OR NOT p.key_p_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           -- ...check the date validity; if invalid, default to 1978-01-01 else use the date
# MAGIC           CASE
# MAGIC             WHEN p.key_p_date IS NULL 
# MAGIC               OR p.key_p_date = '' 
# MAGIC               OR NOT p.key_p_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_p_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           -- If the time is valid, attempt to build the timestamp by concatenating the date and time.
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_p_date, ' ', p.key_p_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 1 HOUR
# MAGIC       END 
# MAGIC     WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC       CASE
# MAGIC         WHEN p.key_p_time IS NULL 
# MAGIC           OR p.key_p_time = '' 
# MAGIC           OR NOT p.key_p_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_p_date IS NULL 
# MAGIC               OR p.key_p_date = '' 
# MAGIC               OR NOT p.key_p_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_p_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_p_date, ' ', p.key_p_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           ) - INTERVAL 2 HOURS
# MAGIC       END 
# MAGIC     ELSE
# MAGIC       CASE
# MAGIC         WHEN p.key_p_time IS NULL 
# MAGIC           OR p.key_p_time = '' 
# MAGIC           OR NOT p.key_p_time RLIKE '^[0-9]{1,2}:[0-9]{2}$' 
# MAGIC         THEN 
# MAGIC           CASE
# MAGIC             WHEN p.key_p_date IS NULL 
# MAGIC               OR p.key_p_date = '' 
# MAGIC               OR NOT p.key_p_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
# MAGIC             THEN DATE '1978-01-01'
# MAGIC             ELSE TRY_CAST(p.key_p_date AS DATE) -- Use TRY_CAST to avoid errors
# MAGIC           END
# MAGIC         ELSE 
# MAGIC           COALESCE(
# MAGIC             TRY_CAST(CONCAT(p.key_p_date, ' ', p.key_h_time) AS TIMESTAMP), 
# MAGIC             TIMESTAMP '1970-01-01 23:59:00'
# MAGIC           )
# MAGIC       END
# MAGIC   END AS event_timestamp,
# MAGIC     aljex_user_report_listing.full_name,
# MAGIC   period_year,
# MAGIC   Week_End_Date,
# MAGIC   new_office as Office,
# MAGIC   --master_am_list.office as office_gs,
# MAGIC     --master_am_list.job_title,
# MAGIC     --master_am_list.lss_y_or_no,
# MAGIC     --master_am_list.email_address,
# MAGIC     p.id::string,
# MAGIC     1 as counter,
# MAGIC 	'Aljex' as Source_System_Type
# MAGIC    -- 'no' as contains_hubspot
# MAGIC FROM bronze.projection_load_1 p
# MAGIC LEFT JOIN bronze.projection_load_2 ON p.id = projection_load_2.id
# MAGIC left JOIN use_dates ON try_cast(p.key_h_date as date )= use_dates.use_dates -- Use TRY_CAST here as well
# MAGIC LEFT JOIN bronze.aljex_user_report_listing ON p.key_h_user = aljex_user_report_listing.aljex_id
# MAGIC  AND aljex_id != 'repeat'
# MAGIC LEFT JOIN bronze.new_office_lookup ON aljex_user_report_listing.pnl_code = new_office_lookup.old_office
# MAGIC LEFT JOIN bronze.relay_users ON UPPER(aljex_user_report_listing.full_name) = UPPER(relay_users.full_name) 
# MAGIC AND relay_users.`active?` = 'true'
# MAGIC join bronze.master_am_list on relay_users.user_id::string = master_am_list.am_relay_id::string
# MAGIC WHERE p.key_p_user IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC Select Distinct 
# MAGIC  'Aljex - Dispatched_Load',
# MAGIC  try_cast(p.Dispatched_date AS Date) AS Dispatched_date,
# MAGIC  al.full_name AS full_name,
# MAGIC  Period_Year,
# MAGIC  Week_End_Date,
# MAGIC  new_office as Office,
# MAGIC  p.id :: String,
# MAGIC  1 as counter,
# MAGIC  'Aljex' as Source_System_Type
# MAGIC   FROM bronze.projection_load_1 p
# MAGIC   LEFT JOIN bronze.projection_load_2 p2 ON p.id = p2.id
# MAGIC   LEFT JOIN bronze.aljex_user_report_listing al ON p.key_c_user = al.aljex_id
# MAGIC   left JOIN use_dates ON try_cast(p.key_h_date as date )= use_dates.use_dates
# MAGIC   LEFT JOIN bronze.new_office_lookup ON al.pnl_code = new_office_lookup.old_office
# MAGIC   where Dispatched_date IS NOT NULL and full_name is not null
# MAGIC
# MAGIC ),
# MAGIC Final_Code as (
# MAGIC Select 
# MAGIC Concat_ws(Activity_Type,Activity_Date,Activity_Id) As DW_Touch_Id,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touch_Type,
# MAGIC period_year,
# MAGIC Week_End_Date,
# MAGIC Source_System_Type
# MAGIC  from Silver_Aljex_Touches )
# MAGIC
# MAGIC  Select 
# MAGIC  *,
# MAGIC  SHA1(
# MAGIC   Concat(
# MAGIC     COALESCE(Activity_Id::string,''),
# MAGIC     COALESCE(Activity_Type::String,''),
# MAGIC     COALESCE(Activity_Date::String,''),
# MAGIC     COALESCE(Office::String,''),
# MAGIC     COALESCE(Employee_Name::String,''),
# MAGIC     COALESCE(Touch_Type::String,''),
# MAGIC     COALESCE(period_year::String,''),
# MAGIC     COALESCE(Week_End_Date::String,''),
# MAGIC     COALESCE(Source_System_Type,'') 
# MAGIC   )
# MAGIC  )As HashKey from Final_Code

# COMMAND ----------

# MAGIC %md
# MAGIC DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Silver.Sliver_Touch_Aljex (
# MAGIC     DW_Touches_ID VARCHAR(255)NOT NULL PRIMARY KEY,
# MAGIC     Activity_Id VARCHAR (255),
# MAGIC     Activity_Type VARCHAR (255),
# MAGIC     Activity_Date DATE,
# MAGIC     Office VARCHAR (255),
# MAGIC     Employee_Name VARCHAR (255),
# MAGIC     Touches_Type INT,
# MAGIC     Period_Year VARCHAR (255),
# MAGIC     Week_end_date DATE,
# MAGIC     Source_System_Type VARCHAR(255),
# MAGIC     Hash_key VARCHAR (255),
# MAGIC     Created_Date TIMESTAMP_NTZ,
# MAGIC     Created_By VARCHAR(50) NOT NULL,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ,
# MAGIC     Last_Modified_by VARCHAR(50) NOT NULL,
# MAGIC     Is_Deleted SMALLINT
# MAGIC );