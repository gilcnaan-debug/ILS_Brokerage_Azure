# Databricks notebook source
# MAGIC %md
# MAGIC ## Canned Reprorts
# MAGIC * **Description:** To Replicate Metabase Canned Reports for Niffy
# MAGIC * **Created Date:** 01/15/2025
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 01/15/2025
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating View

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC SET ansi_mode = false;
# MAGIC create or replace view bronze.Master_Customer_Report_Genai
# MAGIC  as(
# MAGIC
# MAGIC WITH final_carrier_rate AS (
# MAGIC   SELECT
# MAGIC   p1.id,
# MAGIC   CAST(pickup_date AS DATE) AS pickup_date,
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_line_haul LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_line_haul RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_line_haul AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate1 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate1 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate1 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate2 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate2 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate2 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate3 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate3 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate3 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate4 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate4 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate4 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate5 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate5 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate5 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate6 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate6 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate7 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate7 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate7 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) +
# MAGIC   COALESCE(
# MAGIC     CASE
# MAGIC       WHEN carrier_accessorial_rate8 LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_accessorial_rate8 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN try_cast(carrier_accessorial_rate8 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END, 0.00
# MAGIC   ) AS final_carrier_rate
# MAGIC FROM projection_load_1 p1 
# MAGIC LEFT JOIN projection_load_2 p2 ON p1.id = p2.id
# MAGIC LEFT JOIN analytics.dim_financial_calendar ON CAST(p1.pickup_date AS DATE) = CAST(analytics.dim_financial_calendar.date AS DATE)
# MAGIC WHERE 1=1
# MAGIC   AND p2.status NOT LIKE '%VOID%'
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC new_pu_appt AS (
# MAGIC     SELECT DISTINCT
# MAGIC         b.relay_reference_number,
# MAGIC         MAX(COALESCE(
# MAGIC             CAST(s.appointment_datetime AS TIMESTAMP),
# MAGIC             CAST(s.window_start_datetime AS TIMESTAMP),
# MAGIC             CAST(s.window_end_datetime AS TIMESTAMP),
# MAGIC             CAST(p.appointment_datetime AS TIMESTAMP),
# MAGIC             CAST(b.ready_date AS date)
# MAGIC         )) AS new_pu_appt
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC         AND b.first_shipper_name = p.shipper_name
# MAGIC         AND p.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC         AND s.stop_type = 'pickup'
# MAGIC         AND s.sequence_number = 1
# MAGIC         AND s.`removed?` = 'false'
# MAGIC     WHERE 1=1
# MAGIC     AND b.status NOT IN ('cancelled', 'bounced')
# MAGIC     GROUP BY b.relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC to_get_avg AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         ship_date,
# MAGIC         conversion AS us_to_cad,
# MAGIC         us_to_cad AS cad_to_us
# MAGIC     FROM canada_conversions
# MAGIC     ORDER BY ship_date DESC 
# MAGIC     LIMIT 7
# MAGIC ),
# MAGIC
# MAGIC average_conversions AS (
# MAGIC     SELECT  
# MAGIC         AVG(us_to_cad) AS avg_us_to_cad,
# MAGIC         AVG(cad_to_us) AS avg_cad_to_us
# MAGIC     FROM to_get_avg
# MAGIC ),
# MAGIC
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
# MAGIC
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
# MAGIC     WHERE `voided?` = 'false'
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
# MAGIC
# MAGIC actual_pu AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.booking_id, 
# MAGIC         b.relay_reference_number,
# MAGIC         MAX(COALESCE(in_date_time::TIMESTAMP, out_date_time::TIMESTAMP)) AS actual_pu
# MAGIC     FROM booking_projection b 
# MAGIC     LEFT JOIN canonical_stop ON b.booking_id = canonical_stop.booking_id AND b.first_shipper_id = canonical_stop.facility_id
# MAGIC     WHERE b.status = 'booked'
# MAGIC     AND `stale?` = 'false'
# MAGIC     AND canonical_stop.stop_type = 'pickup'
# MAGIC     GROUP BY b.booking_id, b.relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC system_union AS (
# MAGIC         SELECT DISTINCT 
# MAGIC         'ALJEX' AS systemm,
# MAGIC         new_office_lookup.new_office,
# MAGIC         new_office_dup AS booking_office,
# MAGIC         p1.status AS status,
# MAGIC         key_c_user,
# MAGIC         p.id::FLOAT AS loadnum,
# MAGIC         shipper AS customer,
# MAGIC         CASE 
# MAGIC             WHEN customer_lookup.master_customer_name IS NULL THEN shipper 
# MAGIC             ELSE customer_lookup.master_customer_name 
# MAGIC         END AS mastername,
# MAGIC         equipment AS equip,
# MAGIC         p.pickup_date::DATE AS shipdate,
# MAGIC         invoice_total::FLOAT AS revenue,
# MAGIC          CASE 
# MAGIC             WHEN (c.name LIKE '%CAD' OR c.id IN ('300023338','300022987')) THEN 
# MAGIC                 CASE 
# MAGIC                     WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                     ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC             ELSE final_carrier_rate::FLOAT 
# MAGIC         END AS expense,
# MAGIC         c.dot_num::FLOAT
# MAGIC     FROM projection_load_1 p left join projection_load_2 p1 on p.id = p1.id
# MAGIC     LEFT JOIN final_carrier_rate ON p.id = final_carrier_rate.id 
# MAGIC     LEFT JOIN canada_conversions ON p.pickup_date::DATE = canada_conversions.ship_date::DATE 
# MAGIC     LEFT JOIN customer_lookup ON LEFT(shipper,32) = LEFT(customer_lookup.aljex_customer_name,32)
# MAGIC     LEFT JOIN Analytics.dim_financial_calendar ON p.pickup_date::DATE = Analytics.dim_financial_calendar.date 
# MAGIC     LEFT JOIN projection_carrier c ON p.carrier_id = c.id 
# MAGIC     JOIN average_conversions ON 1=1 
# MAGIC     LEFT JOIN new_office_lookup ON p.office = new_office_lookup.old_office 
# MAGIC     LEFT JOIN aljex_user_report_listing ON aljex_user_report_listing.aljex_id = key_c_user
# MAGIC     LEFT JOIN new_office_lookup_dup ON new_office_lookup_dup.Old_Office_Dub = aljex_user_report_listing.pnl_code
# MAGIC     WHERE p1.status NOT LIKE 'VOID%'
# MAGIC     AND p.office NOT IN ('10','34','51','54','61','62','63','64','74')
# MAGIC     
# MAGIC
# MAGIC     UNION 
# MAGIC
# MAGIC     SELECT DISTINCT 
# MAGIC         'ALJEX',
# MAGIC         new_office_lookup.new_office,
# MAGIC         new_office_dup AS booking_office,
# MAGIC         p1.status,
# MAGIC         key_c_user,
# MAGIC         p.id::FLOAT,
# MAGIC         shipper,
# MAGIC         CASE 
# MAGIC             WHEN customer_lookup.master_customer_name IS NULL THEN shipper 
# MAGIC             ELSE customer_lookup.master_customer_name 
# MAGIC         END AS mastername,
# MAGIC         equipment,
# MAGIC         p.pickup_date::DATE,
# MAGIC         CASE 
# MAGIC             WHEN cad_currency.currency_type = 'USD' THEN invoice_total::FLOAT 
# MAGIC             WHEN aljex_customer_profiles.cust_country = 'USD' THEN invoice_total::FLOAT 
# MAGIC             ELSE invoice_total::FLOAT / 
# MAGIC                 (CASE 
# MAGIC                     WHEN canada_conversions.conversion IS NULL THEN average_conversions.avg_us_to_cad::FLOAT 
# MAGIC                     ELSE canada_conversions.conversion::NUMERIC 
# MAGIC                 END) 
# MAGIC         END AS revenue,
# MAGIC         CASE 
# MAGIC             WHEN c.name LIKE '%CAD'  THEN 
# MAGIC                 CASE 
# MAGIC                     WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                     ELSE TRY_DIVIDE (final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC 		    WHEN canada_carriers.canada_dot IS NOT NULL THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC 			WHEN c.name RLIKE 'CANADIAN R%' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC 			WHEN c.name RLIKE '%CAN' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC 			WHEN c.name RLIKE '%(CAD)%' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC             
# MAGIC 			WHEN c.name RLIKE '%-C' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC  
# MAGIC 			WHEN c.name RLIKE '%- C%' THEN 
# MAGIC 				CASE
# MAGIC 				  WHEN canada_conversions.conversion IS NULL THEN TRY_DIVIDE(final_carrier_rate,average_conversions.avg_us_to_cad)
# MAGIC                   ELSE TRY_DIVIDE(final_carrier_rate,canada_conversions.conversion)
# MAGIC                 END 
# MAGIC       ELSE final_carrier_rate
# MAGIC     END AS expense,
# MAGIC         c.dot_num::FLOAT
# MAGIC    FROM projection_load_1 p left join projection_load_2 p1 on p.id = p1.id
# MAGIC     LEFT JOIN canada_conversions ON p.pickup_date::DATE = canada_conversions.ship_date::DATE 
# MAGIC     LEFT JOIN customer_lookup ON LEFT(shipper,32) = LEFT(customer_lookup.aljex_customer_name,32)
# MAGIC     LEFT JOIN Analytics.dim_financial_calendar ON p.pickup_date::DATE = Analytics.dim_financial_calendar.date 
# MAGIC     LEFT JOIN aljex_customer_profiles ON p1.customer_id::STRING = aljex_customer_profiles.cust_id::STRING 
# MAGIC     LEFT JOIN final_carrier_rate ON p.id = final_carrier_rate.id 
# MAGIC     LEFT JOIN projection_carrier c ON p.carrier_id = c.id 
# MAGIC     LEFT JOIN cad_currency ON p.id::STRING = cad_currency.pro_number::STRING
# MAGIC     LEFT JOIN canada_carriers ON p.carrier_id::STRING = canada_carriers.canada_dot::STRING
# MAGIC     LEFT JOIN new_office_lookup ON p.office = new_office_lookup.old_office 
# MAGIC     JOIN average_conversions ON 1=1 
# MAGIC     LEFT JOIN aljex_user_report_listing ON aljex_user_report_listing.aljex_id = key_c_user
# MAGIC LEFT JOIN new_office_lookup_dup ON new_office_lookup_dup.Old_Office_Dub = aljex_user_report_listing.pnl_code
# MAGIC     WHERE p1.status NOT LIKE 'VOID%'
# MAGIC     AND p.office IN ('10','34','51','54','61','62','63','64','74')
# MAGIC     
# MAGIC     UNION 
# MAGIC
# MAGIC     SELECT DISTINCT 
# MAGIC         'RELAY',
# MAGIC         new_office_lookup.new_office,
# MAGIC         new_office_dup AS booking_office,
# MAGIC         UPPER(b.status) AS status,
# MAGIC         b.booked_by_name,
# MAGIC         b.relay_reference_number::FLOAT AS loadnum,
# MAGIC         r.customer_name,
# MAGIC         CASE 
# MAGIC             WHEN customer_lookup.master_customer_name IS NULL THEN UPPER(r.customer_name) 
# MAGIC             ELSE customer_lookup.master_customer_name 
# MAGIC         END AS customer,
# MAGIC         'TL',
# MAGIC         new_pu_appt.new_pu_appt::DATE AS datee,
# MAGIC         (customer_money_final.total_usd::FLOAT +
# MAGIC         (customer_money_final.total_cad * 
# MAGIC             (CASE 
# MAGIC                 WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us 
# MAGIC                 ELSE canada_conversions.us_to_cad 
# MAGIC             END)::FLOAT)) - 
# MAGIC         (customer_money_final.usd_cred::FLOAT +
# MAGIC         (customer_money_final.cad_cred * 
# MAGIC             (CASE 
# MAGIC                 WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us 
# MAGIC                 ELSE canada_conversions.us_to_cad 
# MAGIC             END)::FLOAT)) AS revenue,
# MAGIC         carrier_charges.total_carrier_cost AS expense,
# MAGIC         c.dot_number::FLOAT
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN new_pu_appt ON b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC     LEFT JOIN actual_pu ON b.booking_id = actual_pu.booking_id
# MAGIC     LEFT JOIN Analytics.dim_financial_calendar ON COALESCE(actual_pu::DATE, new_pu_appt.new_pu_appt::DATE) = Analytics.dim_financial_calendar.date 
# MAGIC     LEFT JOIN customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug AND r.status = 'published'
# MAGIC     LEFT JOIN carrier_charges ON b.relay_reference_number = carrier_charges.relay_reference_number
# MAGIC     LEFT JOIN customer_lookup ON LEFT(r.customer_name,32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN carrier_projection c ON b.booked_carrier_id = c.carrier_id 
# MAGIC     LEFT JOIN new_office_lookup ON r.profit_center = new_office_lookup.old_office 
# MAGIC     LEFT JOIN customer_money_final ON b.relay_reference_number = customer_money_final.loadnum
# MAGIC     JOIN average_conversions ON 1=1 
# MAGIC     LEFT JOIN canada_conversions ON new_pu_appt::DATE = canada_conversions.ship_date::DATE 
# MAGIC     LEFT JOIN relay_users ON relay_users.user_id = booked_by_user_id
# MAGIC     LEFT JOIN new_office_lookup_dup ON new_office_lookup_dup.Old_Office_Dub = relay_users.office_id
# MAGIC     WHERE b.status = 'booked'
# MAGIC
# MAGIC     UNION 
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC         'RELAY',
# MAGIC         new_office_lookup.new_office,
# MAGIC         new_office_dup AS booking_office,
# MAGIC         dispatch_status,
# MAGIC         'ltl_team',
# MAGIC         load_number::FLOAT,
# MAGIC         r.customer_name,
# MAGIC         CASE 
# MAGIC             WHEN master_customer_name IS NULL THEN r.customer_name 
# MAGIC             ELSE master_customer_name 
# MAGIC         END AS mastername,
# MAGIC         'LTL',
# MAGIC         big_export_projection.ship_date::DATE,
# MAGIC         (customer_money_final.total_usd::FLOAT +
# MAGIC         (customer_money_final.total_cad * 
# MAGIC             (CASE 
# MAGIC                 WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us 
# MAGIC                 ELSE canada_conversions.us_to_cad 
# MAGIC             END)::FLOAT)) - 
# MAGIC         (customer_money_final.usd_cred::FLOAT +
# MAGIC         (customer_money_final.cad_cred * 
# MAGIC             (CASE 
# MAGIC                 WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us 
# MAGIC                 ELSE canada_conversions.us_to_cad 
# MAGIC             END)::FLOAT)) AS revenue,
# MAGIC         projected_expense::FLOAT / 100.0 AS expense,
# MAGIC         c.dot_num::FLOAT
# MAGIC     FROM big_export_projection 
# MAGIC     LEFT JOIN Analytics.dim_financial_calendar ON ship_date::DATE = Analytics.dim_financial_calendar.date::DATE 
# MAGIC     LEFT JOIN customer_profile_projection r ON customer_id = r.customer_slug
# MAGIC     LEFT JOIN customer_money_final ON big_export_projection.load_number = customer_money_final.loadnum
# MAGIC     LEFT JOIN customer_lookup ON LEFT(r.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN projection_carrier c ON big_export_projection.carrier_id = c.id 
# MAGIC     LEFT JOIN new_office_lookup ON r.profit_center = new_office_lookup.old_office 
# MAGIC     JOIN average_conversions ON 1=1 
# MAGIC     LEFT JOIN canada_conversions ON big_export_projection.ship_date::DATE = canada_conversions.ship_date::DATE 
# MAGIC     LEFT JOIN new_office_lookup_dup ON Old_Office_Dub = CASE WHEN load_number IS NOT NULL THEN 'LTL' END 
# MAGIC     WHERE customer_id NOT IN ('deb', 'hain', 'roland')
# MAGIC     AND big_export_projection.load_number != '2352492'
# MAGIC     AND dispatch_status <> 'Cancelled'
# MAGIC     AND carrier_name IS NOT NULL 
# MAGIC     --[[AND {{office}}]]
# MAGIC     --[[AND {{carrier_office}}]]
# MAGIC     --[[AND {{cust}}]]
# MAGIC     --[[AND NOT {{cust_ex}}]]
# MAGIC
# MAGIC     UNION 
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC         'RELAY',
# MAGIC         new_office_lookup.new_office,
# MAGIC         new_office_dup AS booking_office,
# MAGIC         dispatch_status,
# MAGIC         'ltl_team',
# MAGIC         load_number::FLOAT,
# MAGIC         CASE 
# MAGIC             WHEN customer_id = 'hain' THEN 'HAIN CELESTIAL'
# MAGIC             WHEN customer_id = 'deb' THEN 'SC JOHNSON'
# MAGIC             ELSE 'ROLAND CORPORATION' 
# MAGIC         END AS mastername,
# MAGIC         CASE 
# MAGIC             WHEN customer_id = 'hain' THEN 'HAIN CELESTIAL'
# MAGIC             WHEN customer_id = 'deb' THEN 'SC JOHNSON'
# MAGIC             ELSE 'ROLAND CORPORATION' 
# MAGIC         END AS mastername,
# MAGIC         'LTL',
# MAGIC         ship_date::DATE,
# MAGIC         (carrier_accessorial_expense +
# MAGIC         CEILING(CASE customer_id 
# MAGIC             WHEN 'deb' THEN 
# MAGIC                 CASE carrier_id WHEN '300003979' 
# MAGIC                     THEN GREATEST(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE WHEN ship_date::DATE < '2019-03-09' 
# MAGIC                             THEN GREATEST(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END 
# MAGIC                 END 
# MAGIC             WHEN 'hain' 
# MAGIC                 THEN GREATEST(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland'
# MAGIC                 THEN carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0 
# MAGIC         END + carrier_linehaul_expense) 
# MAGIC         + CEILING(CEILING(CASE customer_id 
# MAGIC             WHEN 'deb' THEN 
# MAGIC                 CASE carrier_id WHEN '300003979' 
# MAGIC                     THEN GREATEST(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE WHEN ship_date::DATE < '2019-03-09' 
# MAGIC                             THEN GREATEST(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END 
# MAGIC                 END 
# MAGIC             WHEN 'hain' 
# MAGIC                 THEN GREATEST(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland'
# MAGIC                 THEN carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0 
# MAGIC         END + carrier_linehaul_expense) * 
# MAGIC         (CASE carrier_linehaul_expense WHEN 0 THEN 0 ELSE carrier_fuel_expense::FLOAT / carrier_linehaul_expense::FLOAT END))) / 100 AS projected_rvenue, 
# MAGIC         projected_expense::FLOAT / 100.0 AS expense,
# MAGIC         c.dot_num::FLOAT
# MAGIC     FROM big_export_projection 
# MAGIC     LEFT JOIN Analytics.dim_financial_calendar ON ship_date::DATE = Analytics.dim_financial_calendar.date::DATE 
# MAGIC     LEFT JOIN projection_carrier c ON big_export_projection.carrier_id = c.id 
# MAGIC     LEFT JOIN new_office_lookup ON CASE WHEN customer_id = 'roland' THEN 'LT' ELSE 'NJ' END = new_office_lookup.old_office 
# MAGIC     LEFT JOIN customer_lookup ON customer_id = customer_lookup.aljex_customer_name
# MAGIC     LEFT JOIN new_office_lookup_dup ON Old_Office_Dub = CASE WHEN load_number IS NOT NULL THEN 'LTL' END 
# MAGIC     WHERE customer_id IN ('deb', 'hain', 'roland')
# MAGIC     AND big_export_projection.load_number != '2352492'
# MAGIC    AND dispatch_status <> 'Cancelled'
# MAGIC     AND carrier_name IS NOT NULL 
# MAGIC     
# MAGIC )
# MAGIC ,summary_data AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         loadnum,
# MAGIC         shipdate,
# MAGIC         new_office,
# MAGIC         booking_office,
# MAGIC         mastername,
# MAGIC         revenue,
# MAGIC         expense,
# MAGIC         CASE 
# MAGIC             WHEN revenue IS NULL THEN 0 
# MAGIC             ELSE revenue 
# MAGIC         END - CASE 
# MAGIC             WHEN expense IS NULL THEN 0 
# MAGIC             ELSE expense 
# MAGIC         END AS margin
# MAGIC     FROM system_union
# MAGIC )
# MAGIC select * from summary_data
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Master_Customer_Report_Genai_Temp;
# MAGIC INSERT INTO analytics.Master_Customer_Report_Genai_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Master_Customer_Report_Genai
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Master_Customer_Report_Genai;
# MAGIC INSERT INTO analytics.Master_Customer_Report_Genai
# MAGIC SELECT *
# MAGIC FROM analytics.Master_Customer_Report_Genai_Temp