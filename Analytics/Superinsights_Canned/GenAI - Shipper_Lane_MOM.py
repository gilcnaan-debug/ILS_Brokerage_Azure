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
# MAGIC SET ansi_mode = false;
# MAGIC create or replace view bronze.Shipper_Lane_MOM as( 
# MAGIC WITH final_carrier_rate AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     p.id,
# MAGIC     CAST(pickup_date AS DATE) AS pickup_date,
# MAGIC     CASE 
# MAGIC       WHEN carrier_line_haul RLIKE '%:%' THEN 0.00 
# MAGIC       WHEN carrier_line_haul RLIKE '^\d+(\.\d+)?$' THEN CAST(carrier_line_haul AS FLOAT)
# MAGIC       ELSE 0.00 
# MAGIC     END +
# MAGIC     CASE 
# MAGIC       WHEN carrier_accessorial_rate1 RLIKE '%:%' THEN 0.00 
# MAGIC       WHEN carrier_accessorial_rate1 RLIKE '^\d+(\.\d+)?$' THEN CAST(carrier_accessorial_rate1 AS FLOAT)
# MAGIC       ELSE 0.00 
# MAGIC     END +
# MAGIC     CASE 
# MAGIC       WHEN carrier_accessorial_rate2 RLIKE '%:%' THEN 0.00 
# MAGIC       WHEN carrier_accessorial_rate2 RLIKE '^\d+(\.\d+)?$' THEN CAST(carrier_accessorial_rate2 AS FLOAT)
# MAGIC       ELSE 0.00 
# MAGIC     END +
# MAGIC     CASE 
# MAGIC       WHEN carrier_accessorial_rate3 RLIKE '%:%' THEN 0.00 
# MAGIC       WHEN carrier_accessorial_rate3 RLIKE '^\d+(\.\d+)?$' THEN CAST(carrier_accessorial_rate3 AS FLOAT)
# MAGIC       ELSE 0.00 
# MAGIC     END +
# MAGIC     CASE 
# MAGIC       WHEN carrier_accessorial_rate4 RLIKE '%:%' THEN 0.00 
# MAGIC       WHEN carrier_accessorial_rate4 RLIKE '^\d+(\.\d+)?$' THEN CAST(carrier_accessorial_rate4 AS FLOAT)
# MAGIC       ELSE 0.00 
# MAGIC     END +
# MAGIC     CASE 
# MAGIC       WHEN carrier_accessorial_rate5 RLIKE '%:%' THEN 0.00 
# MAGIC       WHEN carrier_accessorial_rate5 RLIKE '^\d+(\.\d+)?$' THEN CAST(carrier_accessorial_rate5 AS FLOAT)
# MAGIC       ELSE 0.00 
# MAGIC     END +
# MAGIC     CASE 
# MAGIC       WHEN carrier_accessorial_rate6 RLIKE '%:%' THEN 0.00 
# MAGIC       WHEN carrier_accessorial_rate6 RLIKE '^\d+(\.\d+)?$' THEN CAST(carrier_accessorial_rate6 AS FLOAT)
# MAGIC       ELSE 0.00 
# MAGIC     END +
# MAGIC     CASE 
# MAGIC       WHEN carrier_accessorial_rate7 RLIKE '%:%' THEN 0.00 
# MAGIC       WHEN carrier_accessorial_rate7 RLIKE '^\d+(\.\d+)?$' THEN CAST(carrier_accessorial_rate7 AS FLOAT)
# MAGIC       ELSE 0.00 
# MAGIC     END +
# MAGIC     CASE 
# MAGIC       WHEN carrier_accessorial_rate8 RLIKE '%:%' THEN 0.00 
# MAGIC       WHEN carrier_accessorial_rate8 RLIKE '^\d+(\.\d+)?$' THEN CAST(carrier_accessorial_rate8 AS FLOAT)
# MAGIC       ELSE 0.00 
# MAGIC     END AS `final_carrier_rate`
# MAGIC   FROM projection_load_1 p 
# MAGIC   left join projection_load_2 p2 on p.id = p2.id
# MAGIC   LEFT JOIN analytics.dim_financial_calendar ON CAST(p.pickup_date AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE)
# MAGIC   WHERE 1=1
# MAGIC     AND p2.status NOT LIKE '%VOID%'
# MAGIC ),
# MAGIC new_pu_appt AS (
# MAGIC   SELECT DISTINCT
# MAGIC     b.relay_reference_number,
# MAGIC     CAST(p.appointment_datetime AS TIMESTAMP) AS `pickup_proj_appt_datetime`,
# MAGIC     CAST(s.appointment_datetime AS TIMESTAMP) AS `planning_appt_datetime`,
# MAGIC     CAST(s.window_start_datetime AS TIMESTAMP) AS `window_start_datetime`,
# MAGIC     CAST(s.window_end_datetime AS TIMESTAMP) AS `window_end_datetime`,
# MAGIC     COALESCE(
# MAGIC       CAST(s.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(s.window_start_datetime AS TIMESTAMP),
# MAGIC       CAST(s.window_end_datetime AS TIMESTAMP),
# MAGIC       CAST(p.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(b.ready_date AS DATE)
# MAGIC     ) AS `new_pu_appt`
# MAGIC   FROM booking_projection b
# MAGIC   LEFT JOIN pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC     AND b.first_shipper_name = p.shipper_name
# MAGIC     AND p.sequence_number = 1
# MAGIC   LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC     AND s.stop_type = 'pickup'
# MAGIC     AND s.sequence_number = 1
# MAGIC     AND s.`removed?` = 'false'
# MAGIC   WHERE 1=1
# MAGIC     AND b.status NOT IN ('cancelled', 'bounced')
# MAGIC ),
# MAGIC to_get_avg AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     ship_date,
# MAGIC     conversion AS `us_to_cad`,
# MAGIC     us_to_cad AS `cad_to_us`
# MAGIC   FROM canada_conversions
# MAGIC   ORDER BY ship_date DESC
# MAGIC   LIMIT 7
# MAGIC ),
# MAGIC
# MAGIC average_conversions AS (
# MAGIC   SELECT  
# MAGIC     AVG(us_to_cad) AS `avg_us_to_cad`,
# MAGIC     AVG(cad_to_us) AS `avg_cad_to_us`
# MAGIC   FROM to_get_avg
# MAGIC ),
# MAGIC new_customer_money_two AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         incurred_at,
# MAGIC         `voided?` AS voided,
# MAGIC         m.currency AS cust_currency_type,
# MAGIC         m.relay_reference_number AS loadnum,
# MAGIC         charge_code,
# MAGIC         amount::float / 100 AS amount
# MAGIC     FROM moneying_billing_party_transaction m
# MAGIC     WHERE 1=1 
# MAGIC     AND `voided?` = 'false'
# MAGIC     ORDER BY loadnum
# MAGIC ),
# MAGIC
# MAGIC invoicing_credits AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         relay_reference_number,
# MAGIC         SUM(total_amount) FILTER (WHERE currency = 'USD')::float / 100.00 AS usd_cred,
# MAGIC         SUM(total_amount) FILTER (WHERE currency = 'CAD')::float / 100.00 AS cad_cred
# MAGIC     FROM invoicing_credits
# MAGIC     GROUP BY relay_reference_number
# MAGIC ),
# MAGIC customer_money_final AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         loadnum,
# MAGIC         COALESCE(SUM(amount) FILTER (WHERE cust_currency_type = 'USD'), 0) AS total_usd,
# MAGIC         COALESCE(SUM(amount) FILTER (WHERE cust_currency_type = 'CAD'), 0) AS total_cad,
# MAGIC         COALESCE(SUM(amount) FILTER (WHERE cust_currency_type = 'USD' AND charge_code = 'linehaul'), 0) AS usd_lhc,
# MAGIC         COALESCE(SUM(amount) FILTER (WHERE cust_currency_type = 'USD' AND charge_code = 'fuel_surcharge'), 0) AS usd_fsc,
# MAGIC         COALESCE(SUM(amount) FILTER (WHERE cust_currency_type = 'USD' AND charge_code NOT IN ('linehaul','fuel_surcharge')), 0) AS usd_acc,
# MAGIC         COALESCE(SUM(amount) FILTER (WHERE cust_currency_type = 'CAD' AND charge_code = 'linehaul'), 0) AS cad_lhc,
# MAGIC         COALESCE(SUM(amount) FILTER (WHERE cust_currency_type = 'CAD' AND charge_code = 'fuel_surcharge'), 0) AS cad_fsc,
# MAGIC         COALESCE(SUM(amount) FILTER (WHERE cust_currency_type = 'CAD' AND charge_code NOT IN ('linehaul','fuel_surcharge')), 0) AS cad_acc,
# MAGIC         COALESCE(invoicing_credits.usd_cred, 0) AS usd_cred,
# MAGIC         COALESCE(invoicing_credits.cad_cred, 0) AS cad_cred
# MAGIC     FROM new_customer_money_two
# MAGIC     LEFT JOIN invoicing_credits ON loadnum = invoicing_credits.relay_reference_number
# MAGIC     GROUP BY loadnum, usd_cred, cad_cred
# MAGIC ),
# MAGIC carrier_charges_one AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         relay_reference_number,
# MAGIC         incurred_at,
# MAGIC         COALESCE(
# MAGIC             CASE 
# MAGIC                 WHEN currency = 'USD' THEN SUM(amount) FILTER (WHERE charge_code = 'linehaul')::FLOAT / 100.00 
# MAGIC                 ELSE (SUM(amount) FILTER (WHERE charge_code = 'linehaul')::FLOAT / 100.00) * 
# MAGIC                     (CASE 
# MAGIC                         WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us 
# MAGIC                         ELSE canada_conversions.us_to_cad 
# MAGIC                     END)::FLOAT 
# MAGIC             END, 0) AS carr_lhc,
# MAGIC         COALESCE(
# MAGIC             CASE 
# MAGIC                 WHEN currency = 'USD' THEN SUM(amount) FILTER (WHERE charge_code = 'fuel_surcharge')::FLOAT / 100.00 
# MAGIC                 ELSE (SUM(amount) FILTER (WHERE charge_code = 'fuel_surcharge')::FLOAT / 100.00) * 
# MAGIC                     (CASE 
# MAGIC                         WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us 
# MAGIC                         ELSE canada_conversions.us_to_cad 
# MAGIC                     END)::FLOAT 
# MAGIC             END, 0) AS carr_fsc,
# MAGIC         COALESCE(
# MAGIC             CASE 
# MAGIC                 WHEN currency = 'USD' THEN SUM(amount) FILTER (WHERE charge_code NOT IN ('fuel_surcharge','linehaul'))::FLOAT / 100.00 
# MAGIC                 ELSE (SUM(amount) FILTER (WHERE charge_code NOT IN ('fuel_surcharge','linehaul'))::FLOAT / 100.00) * 
# MAGIC                     (CASE 
# MAGIC                         WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us 
# MAGIC                         ELSE canada_conversions.us_to_cad 
# MAGIC                     END)::FLOAT 
# MAGIC             END, 0) AS carr_acc
# MAGIC     FROM vendor_transaction_projection
# MAGIC     LEFT JOIN canada_conversions ON vendor_transaction_projection.incurred_at::DATE = canada_conversions.ship_date::DATE 
# MAGIC     JOIN average_conversions ON 1=1 
# MAGIC     WHERE "voided?" = 'false'
# MAGIC     GROUP BY relay_reference_number, currency, us_to_cad, avg_cad_to_us, incurred_at
# MAGIC ),
# MAGIC
# MAGIC carrier_charges AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         relay_reference_number,
# MAGIC         SUM(carr_lhc) AS carr_lhc,
# MAGIC         SUM(carr_fsc) AS carr_fsc,
# MAGIC         SUM(carr_acc) AS carr_acc,
# MAGIC         SUM(carr_acc)::FLOAT + SUM(carr_fsc)::FLOAT + SUM(carr_lhc)::FLOAT AS total_carrier_cost
# MAGIC     FROM carrier_charges_one
# MAGIC     GROUP BY relay_reference_number
# MAGIC ),
# MAGIC relay_sales AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         profit_center,
# MAGIC         new_office,
# MAGIC         customer_name,
# MAGIC         COALESCE(master_customer_name, customer_name) AS mastername,
# MAGIC         relay_users.full_name AS relay_sales
# MAGIC     FROM customer_profile_projection 
# MAGIC     LEFT JOIN relay_users ON sales_relay_user_id = relay_users.user_id AND `active?` = 'true'
# MAGIC     LEFT JOIN customer_lookup ON LEFT(customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN new_office_lookup ON profit_center = old_office 
# MAGIC     WHERE customer_profile_projection.status = 'published'
# MAGIC ),
# MAGIC
# MAGIC aljex_start AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         new_office,
# MAGIC         COALESCE(master_customer_name, shipper) AS mastername,
# MAGIC         COALESCE(aljex_user_report_listing.full_name, p2.sales_rep) AS sales_rep,
# MAGIC         p.id,
# MAGIC         pickup_date
# MAGIC     FROM projection_load_1 p 
# MAGIC   left join projection_load_2 p2 on p.id = p2.id
# MAGIC     LEFT JOIN customer_lookup ON LEFT(shipper, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN aljex_user_report_listing ON p2.sales_rep = aljex_user_report_listing.sales_rep 
# MAGIC     LEFT JOIN new_office_lookup ON office = old_office 
# MAGIC     WHERE pickup_date BETWEEN CURRENT_DATE - INTERVAL 2 YEARS AND CURRENT_DATE
# MAGIC       AND p2.sales_rep IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC aljex_end AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         new_office,
# MAGIC         mastername,
# MAGIC         sales_rep,
# MAGIC         COUNT(DISTINCT id) AS volume,
# MAGIC         MAX(pickup_date) AS last_shipment,
# MAGIC         RANK() OVER (PARTITION BY mastername, new_office ORDER BY COUNT(DISTINCT id) DESC, MAX(pickup_date) DESC, sales_rep) AS ranker
# MAGIC     FROM aljex_start 
# MAGIC     GROUP BY new_office, mastername, sales_rep
# MAGIC     ORDER BY mastername ,volume desc, last_shipment DESC
# MAGIC ),
# MAGIC
# MAGIC aljex_sales AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         new_office,
# MAGIC         mastername,
# MAGIC         sales_rep AS aljex_sales
# MAGIC     FROM aljex_end
# MAGIC     WHERE ranker = 1 
# MAGIC       AND new_office IS NOT NULL
# MAGIC ),
# MAGIC  system_union AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         'ALJEX US' AS systemm,
# MAGIC         new_office_lookup.new_office,
# MAGIC         p2.status AS status,
# MAGIC         COALESCE(aljex_user_report_listing.full_name, p.key_c_user) AS key_c_user,
# MAGIC         COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) AS carr_office,
# MAGIC         co.new_office AS new_carr_office,
# MAGIC         CAST(p.id AS FLOAT) AS loadnum,
# MAGIC         shipper AS customer,
# MAGIC         COALESCE(customer_lookup.master_customer_name, shipper) AS mastername,
# MAGIC         COALESCE(relay_sales.relay_sales, aljex_sales.aljex_sales) AS sales_rep,
# MAGIC         equipment AS equip,
# MAGIC         CAST(p.pickup_date AS DATE) AS shipdate,
# MAGIC     cast(analytics.dim_financial_calendar.financial_period_Description as string) AS period,
# MAGIC         CAST(invoice_total AS FLOAT) AS revenue,
# MAGIC         CASE 
# MAGIC             WHEN c.name LIKE '%CAD' OR c.id IN ('300023338') THEN 
# MAGIC                 COALESCE(CAST(final_carrier_rate AS FLOAT) / canada_conversions.conversion::NUMERIC, CAST(final_carrier_rate AS FLOAT) / 1.3333) 
# MAGIC             ELSE CAST(final_carrier_rate AS FLOAT)
# MAGIC         END AS expense,
# MAGIC         origin_city,
# MAGIC         origin_state,
# MAGIC         pickup_zip_code AS origin_zip,
# MAGIC         dest_city,
# MAGIC         dest_state,
# MAGIC         consignee_zip_code AS dest_zip,
# MAGIC         dim_financial_calendar.Financial_Year
# MAGIC 	FROM projection_load_1 p 
# MAGIC 	left join projection_load_2 p2 on p.id = p2.id    LEFT JOIN analytics.dim_financial_calendar ON CAST(p.pickup_date AS DATE) = analytics.dim_financial_calendar.date 
# MAGIC     LEFT JOIN final_carrier_rate ON p.id = final_carrier_rate.id 
# MAGIC     LEFT JOIN canada_conversions ON CAST(p.pickup_date AS DATE) = CAST(canada_conversions.ship_date AS DATE)
# MAGIC     LEFT JOIN customer_lookup ON LEFT(shipper, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC     LEFT JOIN projection_carrier c ON p.carrier_id = c.id 
# MAGIC     LEFT JOIN aljex_user_report_listing ON p.key_c_user = aljex_user_report_listing.aljex_id 
# MAGIC     LEFT JOIN new_office_lookup ON p.office = new_office_lookup.old_office 
# MAGIC     LEFT JOIN relay_users ON aljex_user_report_listing.full_name = relay_users.full_name AND relay_users.`active?` = 'true'
# MAGIC     LEFT JOIN new_office_lookup co ON COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) = co.old_office 
# MAGIC     LEFT JOIN relay_sales ON customer_lookup.master_customer_name = relay_sales.mastername 
# MAGIC         AND new_office_lookup.new_office = relay_sales.new_office 
# MAGIC     LEFT JOIN aljex_sales ON customer_lookup.master_customer_name = aljex_sales.mastername 
# MAGIC         AND new_office_lookup.new_office = aljex_sales.new_office 
# MAGIC     WHERE p2.status NOT LIKE 'VOID%'
# MAGIC     AND p.office NOT IN ('10', '34', '51', '54', '61', '62', '63', '64', '74')
# MAGIC     -- Uncomment and adjust the following lines if needed
# MAGIC     -- [[AND {{cust_office}}]]
# MAGIC     -- [[AND {{shipdate}}]]
# MAGIC     -- [[AND {{shipyear}}]]
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     'ALJEX CAD' AS systemm,
# MAGIC     new_office_lookup.new_office,
# MAGIC     p2.status AS status,
# MAGIC     COALESCE(aljex_user_report_listing.full_name, p.key_c_user) AS key_c_user,
# MAGIC     COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) AS carr_office,
# MAGIC     co.new_office AS new_carr_office,
# MAGIC     CAST(p.id AS FLOAT) AS loadnum,
# MAGIC     shipper AS customer,
# MAGIC     COALESCE(customer_lookup.master_customer_name, shipper) AS mastername,
# MAGIC     COALESCE(relay_sales.relay_sales, aljex_sales.aljex_sales) AS sales_rep,
# MAGIC     equipment AS equip,
# MAGIC     CAST(p.pickup_date AS DATE) AS shipdate,
# MAGIC     cast(analytics.dim_financial_calendar.financial_period_Description as string) AS period,
# MAGIC     CASE 
# MAGIC         WHEN cad_currency.currency_type = 'USD' THEN CAST(invoice_total AS FLOAT)
# MAGIC         WHEN aljex_customer_profiles.cust_country = 'USD' THEN CAST(invoice_total AS FLOAT)
# MAGIC         ELSE CAST(invoice_total AS FLOAT) / COALESCE(CAST(canada_conversions.conversion AS NUMERIC), 1.33333)
# MAGIC     END AS revenue,
# MAGIC     CAST(final_carrier_rate AS FLOAT) / COALESCE(CAST(canada_conversions.conversion AS NUMERIC), 1.33333) AS expense,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     pickup_zip_code AS origin_zip,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     consignee_zip_code AS dest_zip,
# MAGIC     dim_financial_calendar.Financial_Year
# MAGIC FROM projection_load_1 p 
# MAGIC   left join projection_load_2 p2 on p.id = p2.id
# MAGIC   LEFT JOIN analytics.dim_financial_calendar ON CAST(p.pickup_date AS DATE) = analytics.dim_financial_calendar.date 
# MAGIC LEFT JOIN final_carrier_rate ON p.id = final_carrier_rate.id 
# MAGIC LEFT JOIN canada_conversions ON CAST(p.pickup_date AS DATE) = CAST(canada_conversions.ship_date AS DATE)
# MAGIC LEFT JOIN customer_lookup ON LEFT(shipper, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC LEFT JOIN projection_carrier c ON p.carrier_id = c.id 
# MAGIC LEFT JOIN aljex_user_report_listing ON p.key_c_user = aljex_user_report_listing.aljex_id 
# MAGIC LEFT JOIN new_office_lookup ON p.office = new_office_lookup.old_office 
# MAGIC LEFT JOIN relay_users ON aljex_user_report_listing.full_name = relay_users.full_name AND relay_users.`active?` = 'true'
# MAGIC LEFT JOIN new_office_lookup co ON COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) = co.old_office 
# MAGIC LEFT JOIN cad_currency ON CAST(p.id AS STRING) = CAST(cad_currency.pro_number AS STRING)
# MAGIC LEFT JOIN aljex_customer_profiles ON CAST(p2.customer_id AS STRING) = CAST(aljex_customer_profiles.cust_id AS STRING)
# MAGIC LEFT JOIN relay_sales ON customer_lookup.master_customer_name = relay_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = relay_sales.new_office 
# MAGIC LEFT JOIN aljex_sales ON customer_lookup.master_customer_name = aljex_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = aljex_sales.new_office 
# MAGIC WHERE p2.status NOT LIKE 'VOID%'
# MAGIC AND p.office IN ('10', '34', '51', '54', '61', '62', '63', '64', '74')
# MAGIC -- Uncomment and adjust the following lines if needed
# MAGIC -- [[AND {{cust_office}}]]
# MAGIC -- [[AND {{shipdate}}]]
# MAGIC -- [[AND {{shipyear}}]]
# MAGIC
# MAGIC union
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     'RELAY' AS systemm,
# MAGIC     new_office_lookup.new_office,
# MAGIC     UPPER(b.status) AS status,
# MAGIC     b.booked_by_name,
# MAGIC     COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) AS carr_office,
# MAGIC     co.new_office AS new_carr_office,
# MAGIC     CAST(b.relay_reference_number AS FLOAT) AS loadnum,
# MAGIC     r.customer_name,
# MAGIC     CASE 
# MAGIC         WHEN customer_lookup.master_customer_name IS NULL THEN UPPER(r.customer_name)
# MAGIC         ELSE customer_lookup.master_customer_name
# MAGIC     END AS customer,
# MAGIC     COALESCE(relay_sales.relay_sales, aljex_sales.aljex_sales) AS sales_rep,
# MAGIC     'TL' AS equip,
# MAGIC     CAST(new_pu_appt.new_pu_appt AS DATE) AS datee,
# MAGIC     cast(analytics.dim_financial_calendar.financial_period_Description as string) AS period,
# MAGIC     (CAST(customer_money_final.total_usd AS FLOAT) +
# MAGIC     (customer_money_final.total_cad * COALESCE(CAST(canada_conversions.us_to_cad AS FLOAT), CAST(average_conversions.avg_cad_to_us AS FLOAT)))) - 
# MAGIC     (CAST(customer_money_final.usd_cred AS FLOAT) +
# MAGIC     (customer_money_final.cad_cred * COALESCE(CAST(canada_conversions.us_to_cad AS FLOAT), CAST(average_conversions.avg_cad_to_us AS FLOAT)))) AS revenue,
# MAGIC     carrier_charges.total_carrier_cost AS expense,
# MAGIC     UPPER(b.first_shipper_city) AS origin_city,
# MAGIC     b.first_shipper_state AS origin_state,
# MAGIC     first_shipper_zip AS origin_zip,
# MAGIC     UPPER(b.receiver_city) AS dest_city,
# MAGIC     b.receiver_state AS dest_state,
# MAGIC     receiver_zip AS dest_zip,
# MAGIC     dim_financial_calendar.Financial_Year
# MAGIC
# MAGIC FROM booking_projection b
# MAGIC LEFT JOIN new_pu_appt ON b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC LEFT JOIN analytics.dim_financial_calendar ON CAST(new_pu_appt.new_pu_appt AS DATE) = analytics.dim_financial_calendar.date 
# MAGIC LEFT JOIN customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug AND r.status = 'published'
# MAGIC LEFT JOIN carrier_charges ON b.relay_reference_number = carrier_charges.relay_reference_number
# MAGIC LEFT JOIN customer_lookup ON r.customer_name = aljex_customer_name
# MAGIC LEFT JOIN carrier_projection c ON b.booked_carrier_id = c.carrier_id 
# MAGIC LEFT JOIN customer_money_final ON b.relay_reference_number = customer_money_final.loadnum
# MAGIC LEFT JOIN new_office_lookup ON r.profit_center = new_office_lookup.old_office 
# MAGIC LEFT JOIN aljex_user_report_listing ON b.booked_by_name = aljex_user_report_listing.full_name 
# MAGIC LEFT JOIN relay_users ON b.booked_by_name = relay_users.full_name AND relay_users.`active?` = 'true'
# MAGIC LEFT JOIN new_office_lookup co ON COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) = co.old_office 
# MAGIC JOIN average_conversions ON 1=1 
# MAGIC LEFT JOIN canada_conversions ON CAST(new_pu_appt.new_pu_appt AS DATE) = CAST(canada_conversions.ship_date AS DATE)
# MAGIC LEFT JOIN relay_sales ON customer_lookup.master_customer_name = relay_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = relay_sales.new_office 
# MAGIC LEFT JOIN aljex_sales ON customer_lookup.master_customer_name = aljex_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = aljex_sales.new_office 
# MAGIC WHERE CAST(new_pu_appt.new_pu_appt AS DATE) <= CURRENT_DATE
# MAGIC AND b.status = 'booked'
# MAGIC -- Uncomment and adjust the following lines if needed
# MAGIC -- [[AND {{cust_office}}]]
# MAGIC -- [[AND {{shipdate}}]]
# MAGIC -- [[AND {{shipyear}}]]
# MAGIC
# MAGIC union
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     'RELAY LTL' AS systemm,
# MAGIC     CASE 
# MAGIC         WHEN customer_id IN ('hain', 'deb') THEN 'PHI' 
# MAGIC         ELSE 'LTL' 
# MAGIC     END AS office,
# MAGIC     dispatch_status,
# MAGIC     'ltl_team' AS team,
# MAGIC     'LTL' AS business_unit,
# MAGIC     'LTL' AS service_type,
# MAGIC     CAST(load_number AS FLOAT) AS loadnum,
# MAGIC     CASE 
# MAGIC         WHEN customer_id = 'hain' THEN 'HAIN CELESTIAL'
# MAGIC         WHEN customer_id = 'deb' THEN 'SC JOHNSON'
# MAGIC         ELSE 'ROLAND CORPORATION' 
# MAGIC     END AS mastername1,
# MAGIC     CASE 
# MAGIC         WHEN customer_id = 'hain' THEN 'HAIN CELESTIAL'
# MAGIC         WHEN customer_id = 'deb' THEN 'SC JOHNSON'
# MAGIC         ELSE 'ROLAND CORPORATION' 
# MAGIC     END AS mastername2,
# MAGIC     CASE 
# MAGIC         WHEN customer_id IN ('deb','hain') THEN 'Paul Jensen' 
# MAGIC         ELSE 'Manny Galace' 
# MAGIC     END AS sales_rep,
# MAGIC     'LTL' AS equipment,
# MAGIC     CAST(ship_date AS DATE) AS shipdate,
# MAGIC     cast(analytics.dim_financial_calendar.financial_period_Description as string) AS period,
# MAGIC     (carrier_accessorial_expense +
# MAGIC         CEILING(
# MAGIC             CASE customer_id
# MAGIC                 WHEN 'deb' THEN 
# MAGIC                     CASE carrier_id 
# MAGIC                         WHEN '300003979' THEN GREATEST(carrier_linehaul_expense * 0.10, 1500) 
# MAGIC                         ELSE 
# MAGIC                             CASE 
# MAGIC                                 WHEN CAST(ship_date AS DATE) < '2019-03-09' THEN GREATEST(carrier_linehaul_expense * 0.08, 1000)
# MAGIC                                 ELSE GREATEST(carrier_linehaul_expense * 0.09, 1000)
# MAGIC                             END
# MAGIC                     END
# MAGIC                 WHEN 'hain' THEN GREATEST(carrier_linehaul_expense * 0.10, 1000)
# MAGIC                 WHEN 'roland' THEN carrier_linehaul_expense * 0.1365
# MAGIC                 ELSE 0
# MAGIC             END + carrier_linehaul_expense
# MAGIC         ) 
# MAGIC         + CEILING(
# MAGIC             CEILING(
# MAGIC                 CASE customer_id
# MAGIC                     WHEN 'deb' THEN 
# MAGIC                         CASE carrier_id 
# MAGIC                             WHEN '300003979' THEN GREATEST(carrier_linehaul_expense * 0.10, 1500)
# MAGIC                             ELSE 
# MAGIC                                 CASE 
# MAGIC                                     WHEN CAST(ship_date AS DATE) < '2019-03-09' THEN GREATEST(carrier_linehaul_expense * 0.08, 1000)
# MAGIC                                     ELSE GREATEST(carrier_linehaul_expense * 0.09, 1000)
# MAGIC                                 END
# MAGIC                         END
# MAGIC                     WHEN 'hain' THEN GREATEST(carrier_linehaul_expense * 0.10, 1000)
# MAGIC                     WHEN 'roland' THEN carrier_linehaul_expense * 0.1365
# MAGIC                     ELSE 0
# MAGIC                 END + carrier_linehaul_expense
# MAGIC             ) 
# MAGIC             * (CASE carrier_linehaul_expense WHEN 0 THEN 0 ELSE carrier_fuel_expense::FLOAT / carrier_linehaul_expense::FLOAT END)
# MAGIC         )
# MAGIC     ) / 100 AS projected_revenue,
# MAGIC     projected_expense::FLOAT / 100.0 AS expense,
# MAGIC     pickup_city AS Pickup_City,
# MAGIC     pickup_state AS Pickup_State,
# MAGIC     pickup_zip AS Pickup_Zip,
# MAGIC     consignee_city AS Consignee_City,
# MAGIC     consignee_state AS Consignee_State,
# MAGIC     consignee_zip AS Consignee_Zip,
# MAGIC      dim_financial_calendar.Financial_Year
# MAGIC FROM big_export_projection 
# MAGIC LEFT JOIN analytics.dim_financial_calendar ON CAST(ship_date AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE)
# MAGIC LEFT JOIN projection_carrier c ON big_export_projection.carrier_id = c.id 
# MAGIC LEFT JOIN new_office_lookup ON CASE 
# MAGIC     WHEN customer_id IN ('deb', 'hain') THEN 'NJ' 
# MAGIC     ELSE 'LTL' 
# MAGIC END = new_office_lookup.old_office 
# MAGIC WHERE 
# MAGIC     1 = 1 
# MAGIC     -- Uncomment and adjust the following lines if needed
# MAGIC     -- [[AND {{cust_office}}]]
# MAGIC     AND CAST(ship_date AS DATE) <= CURRENT_DATE
# MAGIC     AND customer_id IN ('deb', 'hain', 'roland')
# MAGIC     AND dispatch_status <> 'Cancelled'
# MAGIC     AND carrier_name IS NOT NULL
# MAGIC     -- Uncomment and adjust the following lines if needed
# MAGIC     -- [[AND {{shipdate}}]]
# MAGIC     -- [[AND {{shipyear}}]]
# MAGIC union
# MAGIC 	
# MAGIC SELECT DISTINCT 
# MAGIC     'SCR' AS system,
# MAGIC     'SEA' AS office,
# MAGIC     NULL AS status,
# MAGIC     covered,
# MAGIC     'SEA' AS team,
# MAGIC     'SEA' AS business_unit,
# MAGIC     CAST(pro_number AS FLOAT) AS loadnum,
# MAGIC     scr_data.customer_name,
# MAGIC     CASE 
# MAGIC         WHEN master_customer_name IS NULL THEN scr_data.customer_name 
# MAGIC         ELSE master_customer_name 
# MAGIC     END AS mastername,
# MAGIC     COALESCE(relay_sales.relay_sales, aljex_sales.aljex_sales) AS sales_rep,
# MAGIC     shipment_type,
# MAGIC     CAST(ship_date AS DATE) AS shipdate,
# MAGIC     cast(analytics.dim_financial_calendar.financial_period_Description as string) AS period,
# MAGIC     CAST(revenue AS FLOAT) AS revenue,
# MAGIC     CAST(expense AS FLOAT) AS expense,
# MAGIC     pickup_city AS Pickup_City,
# MAGIC     pickup_state AS Pickup_State,
# MAGIC     pickup_zip AS Pickup_Zip,
# MAGIC     consignee_city AS Consignee_City,
# MAGIC     consignee_state AS Consignee_State,
# MAGIC     consignee_zip AS Consignee_Zip,
# MAGIC      dim_financial_calendar.Financial_Year
# MAGIC
# MAGIC FROM scr_data
# MAGIC LEFT JOIN analytics.dim_financial_calendar ON CAST(ship_date AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE)
# MAGIC LEFT JOIN customer_lookup ON LEFT(scr_data.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC LEFT JOIN new_office_lookup ON CASE 
# MAGIC     WHEN pro_number IS NOT NULL THEN 'SEA' 
# MAGIC     ELSE NULL 
# MAGIC END = new_office_lookup.old_office
# MAGIC LEFT JOIN relay_sales ON customer_lookup.master_customer_name = relay_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = relay_sales.new_office 
# MAGIC LEFT JOIN aljex_sales ON customer_lookup.master_customer_name = aljex_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = aljex_sales.new_office 
# MAGIC WHERE 1 = 1 
# MAGIC AND covered != ''
# MAGIC AND covered != ' '
# MAGIC -- Uncomment and adjust the following lines if needed
# MAGIC -- [[AND {{cust_office}}]]
# MAGIC -- [[AND {{shipdate}}]]
# MAGIC -- [[AND {{shipyear}}]]
# MAGIC
# MAGIC -- Second part of the union: big_export_projection
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     'RELAY' AS system,
# MAGIC     new_office_lookup.new_office AS office,
# MAGIC     dispatch_status AS status,
# MAGIC     'ltl_team' AS team,
# MAGIC     'LTL' AS business_unit,
# MAGIC     'LTL' AS service_type,
# MAGIC     CAST(load_number AS FLOAT) AS loadnum,
# MAGIC     r.customer_name AS customer_name,
# MAGIC     CASE 
# MAGIC         WHEN master_customer_name IS NULL THEN r.customer_name 
# MAGIC         ELSE master_customer_name 
# MAGIC     END AS mastername,
# MAGIC     COALESCE(relay_sales.relay_sales, aljex_sales.aljex_sales) AS sales_rep,
# MAGIC     'LTL' AS equipment,
# MAGIC     CAST(big_export_projection.ship_date AS DATE) AS shipdate,
# MAGIC     cast(analytics.dim_financial_calendar.financial_period_Description as string) AS period,
# MAGIC     (CAST(customer_money_final.total_usd AS FLOAT) +
# MAGIC     (customer_money_final.total_cad * COALESCE(CAST(canada_conversions.us_to_cad AS FLOAT), CAST(average_conversions.avg_cad_to_us AS FLOAT)))) - 
# MAGIC     (CAST(customer_money_final.usd_cred AS FLOAT) +
# MAGIC     (customer_money_final.cad_cred * COALESCE(CAST(canada_conversions.us_to_cad AS FLOAT), CAST(average_conversions.avg_cad_to_us AS FLOAT)))) AS revenue,
# MAGIC     projected_expense::FLOAT / 100.0 AS expense,
# MAGIC     pickup_city AS Pickup_City,
# MAGIC     pickup_state AS Pickup_State,
# MAGIC     pickup_zip AS Pickup_Zip,
# MAGIC     consignee_city AS Consignee_City,
# MAGIC     consignee_state AS Consignee_State,
# MAGIC     consignee_zip AS Consignee_Zip,
# MAGIC      dim_financial_calendar.Financial_Year
# MAGIC FROM big_export_projection 
# MAGIC LEFT JOIN analytics.dim_financial_calendar ON CAST(big_export_projection.ship_date AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE) 
# MAGIC LEFT JOIN customer_profile_projection r ON customer_id = r.customer_slug
# MAGIC LEFT JOIN customer_money_final ON big_export_projection.load_number = customer_money_final.loadnum
# MAGIC LEFT JOIN customer_lookup ON r.customer_name = aljex_customer_name
# MAGIC LEFT JOIN projection_carrier c ON big_export_projection.carrier_id = c.id 
# MAGIC LEFT JOIN new_office_lookup ON r.profit_center = new_office_lookup.old_office
# MAGIC JOIN average_conversions ON 1 = 1 
# MAGIC LEFT JOIN canada_conversions ON CAST(big_export_projection.ship_date AS DATE) = CAST(canada_conversions.ship_date AS DATE)
# MAGIC LEFT JOIN relay_sales ON customer_lookup.master_customer_name = relay_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = relay_sales.new_office 
# MAGIC LEFT JOIN aljex_sales ON customer_lookup.master_customer_name = aljex_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = aljex_sales.new_office 
# MAGIC WHERE 1 = 1 
# MAGIC -- Uncomment and adjust the following lines if needed
# MAGIC -- [[AND {{shipdate}}]]
# MAGIC -- [[AND {{shipyear}}]]
# MAGIC AND customer_id NOT IN ('deb', 'hain', 'roland')
# MAGIC AND CAST(big_export_projection.ship_date AS DATE) <= CURRENT_DATE
# MAGIC -- Uncomment and adjust the following lines if needed
# MAGIC -- [[AND {{cust_office}}]]
# MAGIC AND big_export_projection.load_number != '2352492'
# MAGIC AND dispatch_status <> 'Cancelled'
# MAGIC AND carrier_name IS NOT NULL
# MAGIC
# MAGIC union
# MAGIC
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     'EDGE' AS systemm,
# MAGIC     new_office_lookup.new_office AS office,
# MAGIC     NULL AS status,
# MAGIC     booked_by,
# MAGIC     COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) AS carr_office,
# MAGIC     co.new_office AS new_carr_office,
# MAGIC     load_num,
# MAGIC     customer,
# MAGIC     CASE 
# MAGIC         WHEN master_customer_name IS NULL THEN customer 
# MAGIC         ELSE master_customer_name 
# MAGIC     END AS mastername,
# MAGIC     COALESCE(relay_sales.relay_sales, aljex_sales.aljex_sales) AS sales_rep,
# MAGIC     mode,
# MAGIC     CAST(ship_date AS DATE) AS shipdate,
# MAGIC     cast(analytics.dim_financial_calendar.financial_period_Description as string) AS period,
# MAGIC     total_revenue,
# MAGIC     total_expense,
# MAGIC     shipper_city,
# MAGIC     shipper_state,
# MAGIC     shipper_zip,
# MAGIC     carr_city,
# MAGIC     carr_state,
# MAGIC     carr_zip,
# MAGIC     dim_financial_calendar.Financial_Year
# MAGIC FROM cai_data 
# MAGIC LEFT JOIN analytics.dim_financial_calendar ON CAST(ship_date AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE)
# MAGIC LEFT JOIN new_office_lookup ON ops_office = new_office_lookup.old_office 
# MAGIC LEFT JOIN aljex_user_report_listing ON booked_by = aljex_user_report_listing.full_name 
# MAGIC LEFT JOIN relay_users ON booked_by = relay_users.full_name AND relay_users.`active?` = 'true'
# MAGIC LEFT JOIN new_office_lookup co ON COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) = co.old_office
# MAGIC LEFT JOIN customer_lookup ON LEFT(customer, 32) = LEFT(aljex_customer_name, 32)
# MAGIC LEFT JOIN relay_sales ON customer_lookup.master_customer_name = relay_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = relay_sales.new_office 
# MAGIC LEFT JOIN aljex_sales ON customer_lookup.master_customer_name = aljex_sales.mastername 
# MAGIC     AND new_office_lookup.new_office = aljex_sales.new_office 
# MAGIC WHERE 1 = 1 
# MAGIC AND mode = 'OTR'
# MAGIC AND ops_office IN ('WEST', 'EAST', 'SCR AIR SERVICES, INC.', 'PORTLAND')
# MAGIC -- Uncomment and adjust the following lines if needed
# MAGIC -- [[AND {{cust_office}}]]
# MAGIC -- [[AND {{shipdate}}]]
# MAGIC -- [[AND {{shipyear}}]]
# MAGIC ),
# MAGIC
# MAGIC    Final_Data as ( SELECT DISTINCT 
# MAGIC         period,
# MAGIC         system_union.sales_rep,
# MAGIC         system_union.new_office,
# MAGIC         shipdate,
# MAGIC          Financial_Year,
# MAGIC         CASE 
# MAGIC             WHEN RIGHT(period, 2) = 'P1' THEN '1'
# MAGIC             WHEN RIGHT(period, 2) = 'P2' THEN '2'
# MAGIC             WHEN RIGHT(period, 2) = 'P3' THEN '3'
# MAGIC             WHEN RIGHT(period, 2) = 'P4' THEN '4'
# MAGIC             WHEN RIGHT(period, 2) = 'P5' THEN '5'
# MAGIC             WHEN RIGHT(period, 2) = 'P6' THEN '6'
# MAGIC             WHEN RIGHT(period, 2) = 'P7' THEN '7'
# MAGIC             WHEN RIGHT(period, 2) = 'P8' THEN '8'
# MAGIC             WHEN RIGHT(period, 2) = 'P9' THEN '9'
# MAGIC             WHEN RIGHT(period, 3) = 'P10' THEN '10'
# MAGIC             WHEN RIGHT(period, 3) = 'P11' THEN '11'
# MAGIC             WHEN RIGHT(period, 3) = 'P12' THEN '12'
# MAGIC         END AS period_grouping,
# MAGIC         mastername,
# MAGIC         loadnum,
# MAGIC         revenue,
# MAGIC         expense,
# MAGIC         (COALESCE(revenue, 0) - COALESCE(expense, 0)) AS margin,
# MAGIC         CONCAT(INITCAP(origin_city), ', ', origin_state, ' > ', INITCAP(dest_city), ', ', dest_state) AS lane,
# MAGIC         origin_zip,
# MAGIC         dest_zip
# MAGIC     FROM system_union 
# MAGIC     LEFT JOIN customer_lookup ON mastername = customer_lookup.master_customer_name
# MAGIC     LEFT JOIN aljex_user_report_listing ON system_union.sales_rep = aljex_user_report_listing.full_name
# MAGIC 	)
# MAGIC     select * from final_data
# MAGIC  
# MAGIC     
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.shipper_lane_mom_Temp;
# MAGIC
# MAGIC INSERT INTO analytics.shipper_lane_mom_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.shipper_lane_mom

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.shipper_lane_mom;
# MAGIC
# MAGIC INSERT INTO analytics.shipper_lane_mom
# MAGIC SELECT *
# MAGIC FROM analytics.shipper_lane_mom_Temp