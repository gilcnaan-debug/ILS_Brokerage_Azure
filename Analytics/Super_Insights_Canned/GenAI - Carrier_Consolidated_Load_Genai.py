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
# MAGIC SET legacy_time_parser_policy = LEGACY;
# MAGIC create or replace view bronze.Carrier_Consolidated_Load_Genai
# MAGIC  as (
# MAGIC with max_schedule as
# MAGIC (SELECT DISTINCT 
# MAGIC     relay_reference_number,
# MAGIC     max(cast(scheduled_at as timestamp)) AS max_schedule
# MAGIC FROM 
# MAGIC     pickup_projection
# MAGIC GROUP BY 
# MAGIC     relay_reference_number)
# MAGIC , to_get_avg as  (    
# MAGIC select distinct 
# MAGIC ship_date,
# MAGIC conversion as us_to_cad,
# MAGIC us_to_cad as cad_to_us
# MAGIC from bronze.canada_conversions
# MAGIC order by ship_date desc 
# MAGIC limit 7 )
# MAGIC
# MAGIC
# MAGIC , average_conversions as  (
# MAGIC select  
# MAGIC avg(us_to_cad) as avg_us_to_cad,
# MAGIC avg(cad_to_us) as avg_cad_to_us
# MAGIC from to_get_avg)
# MAGIC
# MAGIC ,aljex_data as (SELECT DISTINCT 
# MAGIC     CAST(p1.id AS INTEGER) AS id,
# MAGIC     p2.ref_num AS shipment_id,
# MAGIC     p2.status,
# MAGIC     CASE
# MAGIC         WHEN aljex_user_report_listing.full_name IS NULL THEN p1.key_c_user
# MAGIC         ELSE aljex_user_report_listing.full_name
# MAGIC     END AS key_c_user,
# MAGIC     CASE
# MAGIC         WHEN dis.full_name IS NULL THEN p1.key_h_user
# MAGIC         ELSE dis.full_name
# MAGIC     END AS key_h_user,
# MAGIC     CASE
# MAGIC         WHEN p1.key_c_user = 'IMPORT' THEN 'DRAY'
# MAGIC         WHEN p1.key_c_user = 'EDILCR' THEN 'LTL'
# MAGIC         ELSE car.new_office
# MAGIC     END AS carr_office,
# MAGIC     p2.shipper AS original_shipper,
# MAGIC     CASE
# MAGIC         WHEN customer_lookup.master_customer_name IS NULL THEN p2.shipper
# MAGIC         ELSE customer_lookup.master_customer_name
# MAGIC     END AS mastername,
# MAGIC     CAST(p1.pickup_date AS DATE) AS use_date,
# MAGIC     CASE
# MAGIC         WHEN EXTRACT(dow FROM CAST(p1.pickup_date AS DATE)) = 0 THEN DATE_ADD(CAST(p1.pickup_date AS DATE), 6)
# MAGIC         ELSE DATE_ADD(date_trunc('week', p1.pickup_date), 5)
# MAGIC     END AS end_date,
# MAGIC     (CASE
# MAGIC         WHEN p1.carrier_line_haul LIKE '%:%' THEN 0.00
# MAGIC         WHEN p1.carrier_line_haul RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p1.carrier_line_haul AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END +
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate1 LIKE '%:%' THEN 0.00
# MAGIC         WHEN p2.carrier_accessorial_rate1 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate1 AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END +
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate2 LIKE '%:%' THEN 0.00
# MAGIC         WHEN p2.carrier_accessorial_rate2 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate2 AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END +
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate3 LIKE '%:%' THEN 0.00
# MAGIC         WHEN p2.carrier_accessorial_rate3 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate3 AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END +
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate4 LIKE '%:%' THEN 0.00
# MAGIC         WHEN p2.carrier_accessorial_rate4 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate4 AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END +
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate5 LIKE '%:%' THEN 0.00
# MAGIC         WHEN p2.carrier_accessorial_rate5 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate5 AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END +
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate6 LIKE '%:%' THEN 0.00
# MAGIC         WHEN p2.carrier_accessorial_rate6 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate6 AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END +
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate7 LIKE '%:%' THEN 0.00
# MAGIC         WHEN p2.carrier_accessorial_rate7 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate7 AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END +
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate8 LIKE '%:%' THEN 0.00
# MAGIC         WHEN p2.carrier_accessorial_rate8 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate8 AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END) AS final_carrier_rate,
# MAGIC     CASE
# MAGIC         WHEN p2.value RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p2.value AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END AS cargo_value,
# MAGIC     CASE
# MAGIC         WHEN p1.office NOT IN ('10', '34', '51', '54', '61', '62', '63', '64', '74') THEN 'USD'
# MAGIC         ELSE COALESCE(cad_currency.currency_type, aljex_customer_profiles.cust_country, 'CAD')
# MAGIC     END AS cust_curr,
# MAGIC     CASE
# MAGIC         WHEN c.name LIKE '%CAD' THEN 'CAD'
# MAGIC         WHEN c.name LIKE 'CANADIAN R%' THEN 'CAD'
# MAGIC         WHEN c.name LIKE '%CAN' THEN 'CAD'
# MAGIC         WHEN c.name LIKE '%(CAD)%' THEN 'CAD'
# MAGIC         WHEN c.name LIKE '%-C' THEN 'CAD'
# MAGIC         WHEN c.name LIKE '%- C%' THEN 'CAD'
# MAGIC         WHEN canada_carriers.canada_dot IS NOT NULL THEN 'CAD'
# MAGIC         ELSE 'USD'
# MAGIC     END AS carrier_curr,
# MAGIC     CASE
# MAGIC         WHEN canada_conversions.conversion IS NULL THEN average_conversions.avg_us_to_cad
# MAGIC         ELSE canada_conversions.conversion
# MAGIC     END AS conversion_rate,
# MAGIC     CASE
# MAGIC         WHEN canada_conversions.us_to_cad IS NULL THEN average_conversions.avg_cad_to_us
# MAGIC         ELSE canada_conversions.us_to_cad
# MAGIC     END AS conversion_rate_cad,
# MAGIC     new_office_lookup.new_office AS customer_office,
# MAGIC     p1.office AS proj_load_office,
# MAGIC     p1.equipment,
# MAGIC     p1.mode AS proj_load_mode,
# MAGIC     CASE
# MAGIC         WHEN p1.equipment LIKE '%LTL%' THEN 'LTL'
# MAGIC         ELSE 'TL'
# MAGIC     END AS modee,
# MAGIC     p1.invoice_total,
# MAGIC     c.name AS carrier_name,
# MAGIC     analytics.dim_financial_calendar.financial_period_sorting,
# MAGIC     c.dot_num AS dot_number,
# MAGIC     p1.pickup_name,
# MAGIC     p1.origin_city,
# MAGIC     p1.origin_state,
# MAGIC     p1.pickup_zip_code AS origin_zip,
# MAGIC     p1.consignee,
# MAGIC     p1.consignee_address,
# MAGIC     p1.pickup_address,
# MAGIC     p1.dest_city,
# MAGIC     p1.dest_state,
# MAGIC     p1.consignee_zip_code AS dest_zip,
# MAGIC     CASE
# MAGIC         WHEN p1.miles RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p1.miles AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END AS miles,
# MAGIC     CASE
# MAGIC         WHEN p2.weight RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.weight AS DECIMAL)
# MAGIC         ELSE 0.00
# MAGIC     END AS weight,
# MAGIC     p2.truck_num,
# MAGIC     p2.trailer_number,
# MAGIC     p1.driver_cell_num,
# MAGIC     p1.carrier_id,
# MAGIC     COALESCE(sales.full_name, p2.sales_rep) AS sales_rep,
# MAGIC     COALESCE(am.full_name, p2.srv_rep) AS srv_rep,
# MAGIC     p1.accessorial1,
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate1 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.carrier_accessorial_rate1 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.carrier_accessorial_rate1 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS carr_acc1_rate,
# MAGIC     CASE
# MAGIC         WHEN p2.customer_accessorial_rate1 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.customer_accessorial_rate1 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.customer_accessorial_rate1 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.customer_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS cust_acc1_rate,
# MAGIC     p1.accessorial2,
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate2 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.carrier_accessorial_rate2 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.carrier_accessorial_rate2 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS carr_acc2_rate,
# MAGIC     CASE
# MAGIC         WHEN p2.customer_accessorial_rate2 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.customer_accessorial_rate2 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.customer_accessorial_rate2 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.customer_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS cust_acc2_rate,
# MAGIC     p1.accessorial3,
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate3 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.carrier_accessorial_rate3 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.carrier_accessorial_rate3 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS carr_acc3_rate,
# MAGIC     CASE
# MAGIC         WHEN p2.customer_accessorial_rate3 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.customer_accessorial_rate3 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.customer_accessorial_rate3 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.customer_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS cust_acc3_rate,
# MAGIC     p1.accessorial4,
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate4 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.carrier_accessorial_rate4 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.carrier_accessorial_rate4 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS carr_acc4_rate,
# MAGIC     CASE
# MAGIC         WHEN p2.customer_accessorial_rate4 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.customer_accessorial_rate4 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.customer_accessorial_rate4 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.customer_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS cust_acc4_rate,
# MAGIC     p1.accessorial5,
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate5 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.carrier_accessorial_rate5 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.carrier_accessorial_rate5 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS carr_acc5_rate,
# MAGIC     CASE
# MAGIC         WHEN p2.customer_accessorial_rate5 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.customer_accessorial_rate5 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.customer_accessorial_rate5 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.customer_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS cust_acc5_rate,
# MAGIC     p1.accessorial6,
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate6 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.carrier_accessorial_rate6 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.carrier_accessorial_rate6 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS carr_acc6_rate,
# MAGIC     CASE
# MAGIC         WHEN p2.customer_accessorial_rate6 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.customer_accessorial_rate6 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.customer_accessorial_rate6 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.customer_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS cust_acc6_rate,
# MAGIC     p1.accessorial7,
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate7 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.carrier_accessorial_rate7 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.carrier_accessorial_rate7 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate7 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS carr_acc7_rate,
# MAGIC     CASE
# MAGIC         WHEN p2.customer_accessorial_rate7 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.customer_accessorial_rate7 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.customer_accessorial_rate7 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.customer_accessorial_rate7 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS cust_acc7_rate,
# MAGIC     p1.accessorial8,
# MAGIC     CASE
# MAGIC         WHEN p2.carrier_accessorial_rate8 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.carrier_accessorial_rate8 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.carrier_accessorial_rate8 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.carrier_accessorial_rate8 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS carr_acc8_rate,
# MAGIC     CASE
# MAGIC         WHEN p2.customer_accessorial_rate8 IS NULL THEN NULL
# MAGIC         ELSE
# MAGIC         CASE
# MAGIC             WHEN p2.customer_accessorial_rate8 LIKE '%:%' THEN 0.00
# MAGIC             WHEN p2.customer_accessorial_rate8 RLIKE '^-?[0-9]\\d*(\\.\\d+)?$' THEN CAST(p2.customer_accessorial_rate8 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     END AS cust_acc8_rate
# MAGIC from bronze.projection_load_1 p1
# MAGIC Left Join bronze.projection_load_2 p2 on p1.id = p2.id 
# MAGIC JOIN analytics.dim_financial_calendar ON CAST(p1.pickup_date AS DATE) = analytics.dim_financial_calendar.date
# MAGIC JOIN new_office_lookup ON p1.office = new_office_lookup.old_office
# MAGIC LEFT JOIN cad_currency ON p2.id = cad_currency.pro_number
# MAGIC LEFT JOIN aljex_customer_profiles ON p2.customer_id = aljex_customer_profiles.cust_id
# MAGIC LEFT JOIN projection_carrier c ON p1.carrier_id = c.id
# MAGIC LEFT JOIN canada_carriers ON p1.carrier_id = canada_carriers.canada_dot
# MAGIC LEFT JOIN customer_lookup ON LEFT(p2.shipper, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC JOIN average_conversions ON TRUE
# MAGIC LEFT JOIN aljex_user_report_listing am ON p2.srv_rep = am.aljex_id AND am.aljex_id IS NOT NULL
# MAGIC LEFT JOIN aljex_user_report_listing sales ON p2.sales_rep = sales.sales_rep AND sales.sales_rep IS NOT NULL
# MAGIC LEFT JOIN canada_conversions ON CAST(p1.pickup_date AS DATE) = canada_conversions.ship_date
# MAGIC LEFT JOIN aljex_user_report_listing ON p1.key_c_user = aljex_user_report_listing.aljex_id AND aljex_user_report_listing.aljex_id <> 'repeat'
# MAGIC LEFT JOIN relay_users ON aljex_user_report_listing.full_name = relay_users.full_name AND relay_users.`active?` = true
# MAGIC LEFT JOIN new_office_lookup car ON COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) = car.old_office
# MAGIC LEFT JOIN aljex_user_report_listing dis ON p1.key_h_user = dis.aljex_id AND dis.aljex_id <> 'repeat'
# MAGIC WHERE p2.status NOT LIKE '%VOID%')
# MAGIC ,relay_carrier_money as( 
# MAGIC SELECT
# MAGIC   booking_id,
# MAGIC   relay_reference_number, 
# MAGIC   status,
# MAGIC   ltl_or_tl,
# MAGIC   total_carrier_rate,
# MAGIC   lhc_carrier_rate,
# MAGIC   fsc_carrier_rate,
# MAGIC   acc_carrier_rate
# MAGIC FROM (
# MAGIC   SELECT DISTINCT
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC       CASE
# MAGIC         WHEN vtp.currency = 'CAD' THEN vtp.amount * COALESCE(NULLIF(cc.us_to_cad, 0), avg_conv.avg_cad_to_us)
# MAGIC         ELSE vtp.amount
# MAGIC       END) FILTER (WHERE vtp.`voided?` = false) / 100.0, 0) AS total_carrier_rate,
# MAGIC     COALESCE(SUM(
# MAGIC       CASE
# MAGIC         WHEN vtp.currency = 'CAD' THEN vtp.amount * COALESCE(NULLIF(cc.us_to_cad, 0), avg_conv.avg_cad_to_us)
# MAGIC         ELSE vtp.amount
# MAGIC       END) FILTER (WHERE vtp.`voided?` = false AND vtp.charge_code = 'linehaul') / 100.0, 0) AS lhc_carrier_rate,
# MAGIC     COALESCE(SUM(
# MAGIC       CASE
# MAGIC         WHEN vtp.currency = 'CAD' THEN vtp.amount * COALESCE(NULLIF(cc.us_to_cad, 0), avg_conv.avg_cad_to_us)
# MAGIC         ELSE vtp.amount
# MAGIC       END) FILTER (WHERE vtp.`voided?` = false AND vtp.charge_code = 'fuel_surcharge') / 100.0, 0) AS fsc_carrier_rate,
# MAGIC     COALESCE(SUM(
# MAGIC       CASE
# MAGIC         WHEN vtp.currency = 'CAD' THEN vtp.amount * COALESCE(NULLIF(cc.us_to_cad, 0), avg_conv.avg_cad_to_us)
# MAGIC         ELSE vtp.amount
# MAGIC       END) FILTER (WHERE vtp.`voided?` = false AND vtp.charge_code NOT IN ('linehaul', 'fuel_surcharge')) / 100.0, 0) AS acc_carrier_rate
# MAGIC   FROM
# MAGIC     booking_projection b
# MAGIC     LEFT JOIN canonical_plan_projection cpp ON b.relay_reference_number = cpp.relay_reference_number
# MAGIC     LEFT JOIN vendor_transaction_projection vtp ON b.booking_id = vtp.booking_id
# MAGIC     LEFT JOIN canada_conversions cc ON DATE(vtp.incurred_at) = cc.ship_date
# MAGIC     CROSS JOIN average_conversions avg_conv
# MAGIC   WHERE
# MAGIC     b.status != 'cancelled'
# MAGIC     AND (cpp.mode != 'ltl' OR cpp.mode IS NULL)
# MAGIC     AND (cpp.status != 'voided' OR cpp.status IS NULL)
# MAGIC   GROUP BY b.booking_id, b.relay_reference_number, b.status
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC     b.projected_expense / 100.0 AS total_carrier_rate,
# MAGIC     b.carrier_linehaul_expense / 100.0 AS lhc_carrier_rate,
# MAGIC     b.carrier_fuel_expense / 100.0 AS fsc_carrier_rate,
# MAGIC     b.carrier_accessorial_expense / 100.0 AS acc_carrier_rate
# MAGIC   FROM
# MAGIC     big_export_projection b
# MAGIC     LEFT JOIN canonical_plan_projection cpp ON b.load_number = cpp.relay_reference_number
# MAGIC   WHERE
# MAGIC     b.dispatch_status != 'Cancelled'
# MAGIC     AND (cpp.mode = 'ltl' OR cpp.mode IS NULL)
# MAGIC     AND (cpp.status != 'voided' OR cpp.status IS NULL)
# MAGIC ))
# MAGIC ,max_schedule_del AS (
# MAGIC SELECT
# MAGIC     relay_reference_number,
# MAGIC     MAX(CAST(scheduled_at AS timestamp)) AS max_schedule
# MAGIC FROM 
# MAGIC     bronze.pickup_projection 
# MAGIC GROUP BY 
# MAGIC     relay_reference_number
# MAGIC )
# MAGIC
# MAGIC , last_delivery AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         relay_reference_number,
# MAGIC         MAX(sequence_number) AS last_delivery
# MAGIC     FROM planning_stop_schedule
# MAGIC     WHERE `removed?` = false
# MAGIC     GROUP BY relay_reference_number
# MAGIC )
# MAGIC , truckload_proj_del AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         booking_id,
# MAGIC         MAX(CAST(last_update_date_time AS timestamp)) AS truckload_proj_del
# MAGIC     FROM truckload_projection
# MAGIC     WHERE last_update_event_name = 'MarkedDelivered'
# MAGIC     GROUP BY booking_id)
# MAGIC , master_date AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.booking_id,
# MAGIC         b.relay_reference_number,
# MAGIC         b.status,
# MAGIC         'TL' AS ltl_or_tl,
# MAGIC         COALESCE(
# MAGIC             c.in_date_time, 
# MAGIC             c.out_date_time, 
# MAGIC             pss.appointment_datetime, 
# MAGIC             pss.window_end_datetime, 
# MAGIC             pss.window_start_datetime, 
# MAGIC             pp.appointment_datetime,
# MAGIC             CASE
# MAGIC                 WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC             END
# MAGIC         ) AS use_date,
# MAGIC         CASE
# MAGIC             WHEN DATE_FORMAT(COALESCE(
# MAGIC                 c.in_date_time, 
# MAGIC                 c.out_date_time, 
# MAGIC                 pss.appointment_datetime, 
# MAGIC                 pss.window_end_datetime, 
# MAGIC                 pss.window_start_datetime, 
# MAGIC                 pp.appointment_datetime,
# MAGIC                 CASE
# MAGIC                     WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC                     ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC                 END
# MAGIC             ), 'E') = 'Sun' THEN DATE_ADD(DATE(COALESCE(
# MAGIC                 c.in_date_time, 
# MAGIC                 c.out_date_time, 
# MAGIC                 pss.appointment_datetime, 
# MAGIC                 pss.window_end_datetime, 
# MAGIC                 pss.window_start_datetime, 
# MAGIC                 pp.appointment_datetime,
# MAGIC                 CASE
# MAGIC                     WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC                     ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC                 END
# MAGIC             )), 6)
# MAGIC             ELSE DATE_ADD(DATE_TRUNC('week', COALESCE(
# MAGIC                 c.in_date_time, 
# MAGIC                 c.out_date_time, 
# MAGIC                 pss.appointment_datetime, 
# MAGIC                 pss.window_end_datetime, 
# MAGIC                 pss.window_start_datetime, 
# MAGIC                 pp.appointment_datetime,
# MAGIC                 CASE
# MAGIC                     WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC                     ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC                 END
# MAGIC             )), 5)
# MAGIC         END AS end_date,
# MAGIC         CASE
# MAGIC             WHEN b.ready_time IS NULL THEN TO_TIMESTAMP(b.ready_date)
# MAGIC             ELSE TO_TIMESTAMP(CONCAT(b.ready_date, ' ', b.ready_time))
# MAGIC         END AS ready_date,
# MAGIC         COALESCE(pss.window_start_datetime, pss.appointment_datetime, pp.appointment_datetime) AS pu_appt_start_datetime,
# MAGIC         pss.window_end_datetime AS pu_appt_end_datetime,
# MAGIC         COALESCE(pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime) AS pu_appt_date,
# MAGIC         pss.schedule_type AS pu_schedule_type,
# MAGIC         c.in_date_time AS pickup_in,
# MAGIC         c.out_date_time AS pickup_out,
# MAGIC         COALESCE(c.in_date_time, c.out_date_time) AS pickup_datetime,
# MAGIC         COALESCE(dpss.window_start_datetime, dpss.appointment_datetime,
# MAGIC             CASE
# MAGIC                 WHEN dp.appointment_time_local IS NULL THEN TO_TIMESTAMP(dp.appointment_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(dp.appointment_date, ' ', dp.appointment_time_local))
# MAGIC             END
# MAGIC         ) AS del_appt_start_datetime,
# MAGIC         dpss.window_end_datetime AS del_appt_end_datetime,
# MAGIC         COALESCE(dpss.appointment_datetime, dpss.window_end_datetime, dpss.window_start_datetime,
# MAGIC             CASE
# MAGIC                 WHEN dp.appointment_time_local IS NULL THEN TO_TIMESTAMP(dp.appointment_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(dp.appointment_date, ' ', dp.appointment_time_local))
# MAGIC             END
# MAGIC         ) AS del_appt_date,
# MAGIC         dpss.schedule_type AS del_schedule_type,
# MAGIC         DATE(hain_tracking_accuracy_projection.delivery_by_date_at_tender) AS Delivery_By_Date,
# MAGIC         cd.in_date_time AS delivery_in,
# MAGIC         cd.out_date_time AS delivery_out,
# MAGIC         COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC         t.truckload_proj_del AS truckload_proj_delivered,
# MAGIC         b.booked_at AS booked_at
# MAGIC     FROM booking_projection b
# MAGIC     LEFT JOIN max_schedule ON b.relay_reference_number = max_schedule.relay_reference_number
# MAGIC     LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC     LEFT JOIN pickup_projection pp ON b.booking_id = pp.booking_id AND b.first_shipper_name = pp.shipper_name AND max_schedule.max_schedule = pp.scheduled_at AND pp.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule pss ON b.relay_reference_number = pss.relay_reference_number AND b.first_shipper_name = pss.stop_name AND pss.sequence_number = 1 AND pss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop c ON pss.stop_id = c.stop_id AND c.`stale?` = false AND c.stop_type = 'pickup'
# MAGIC     LEFT JOIN last_delivery ON b.relay_reference_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number AND b.receiver_name = dp.receiver_name AND max_schedule_del.max_schedule = dp.scheduled_at
# MAGIC     LEFT JOIN planning_stop_schedule dpss ON b.relay_reference_number = dpss.relay_reference_number AND b.receiver_name = dpss.stop_name AND b.receiver_city = dpss.locality AND last_delivery.last_delivery = dpss.sequence_number AND dpss.stop_type = 'delivery' AND dpss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop cd ON dpss.stop_id = cd.stop_id AND b.receiver_city = cd.locality AND cd.`stale?` = false AND cd.stop_type = 'delivery'
# MAGIC     LEFT JOIN hain_tracking_accuracy_projection ON b.relay_reference_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC     LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC     LEFT JOIN truckload_proj_del t ON b.booking_id = t.booking_id
# MAGIC     WHERE b.status != 'cancelled' AND (canonical_plan_projection.mode != 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC     UNION ALL
# MAGIC     SELECT DISTINCT 
# MAGIC         b.load_number AS booking_id,
# MAGIC         b.load_number AS relay_reference_number,
# MAGIC         b.dispatch_status AS status,
# MAGIC         'LTL' AS ltl_or_tl,
# MAGIC         COALESCE(b.ship_date, c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime) AS use_date,
# MAGIC         CASE
# MAGIC             WHEN DATE_FORMAT(COALESCE(b.ship_date, c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime), 'u') = '1' 
# MAGIC             THEN DATE_ADD(DATE(COALESCE(b.ship_date, c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime)), 6)
# MAGIC             ELSE DATE_ADD(DATE_TRUNC('week', COALESCE(b.ship_date, c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime)), 5)
# MAGIC         END AS end_date,
# MAGIC         NULL AS ready_date,
# MAGIC         COALESCE(pss.window_start_datetime, pss.appointment_datetime, pp.appointment_datetime) AS pu_appt_start_datetime,
# MAGIC         pss.window_end_datetime AS pu_appt_end_datetime,
# MAGIC         COALESCE(pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime) AS pu_appt_date,
# MAGIC         pss.schedule_type AS pu_schedule_type,
# MAGIC         COALESCE(b.ship_date, c.in_date_time) AS pickup_in,
# MAGIC         c.out_date_time AS pickup_out,
# MAGIC         COALESCE(b.ship_date, c.in_date_time, c.out_date_time) AS pickup_datetime,
# MAGIC         COALESCE(dpss.window_start_datetime, dpss.appointment_datetime,
# MAGIC             CASE
# MAGIC                 WHEN dp.appointment_time_local IS NULL THEN TO_TIMESTAMP(dp.appointment_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(dp.appointment_date, ' ', dp.appointment_time_local))
# MAGIC             END
# MAGIC         ) AS del_appt_start_datetime,
# MAGIC         dpss.window_end_datetime AS del_appt_end_datetime,
# MAGIC         COALESCE(dpss.appointment_datetime, dpss.window_end_datetime, dpss.window_start_datetime,
# MAGIC             CASE
# MAGIC                 WHEN dp.appointment_time_local IS NULL THEN TO_TIMESTAMP(dp.appointment_date)
# MAGIC                 ELSE TO_TIMESTAMP(CONCAT(dp.appointment_date, ' ', dp.appointment_time_local))
# MAGIC             END
# MAGIC         ) AS del_appt_date,
# MAGIC         dpss.schedule_type AS del_schedule_type,
# MAGIC         DATE(hain_tracking_accuracy_projection.delivery_by_date_at_tender) AS Delivery_By_Date,
# MAGIC         COALESCE(b.delivered_date, cd.in_date_time) AS delivery_in,
# MAGIC         cd.out_date_time AS delivery_out,
# MAGIC         COALESCE(b.delivered_date, cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC         NULL AS truckload_proj_delivered,
# MAGIC         NULL AS booked_at
# MAGIC     FROM big_export_projection b
# MAGIC     LEFT JOIN max_schedule ON b.load_number = max_schedule.relay_reference_number
# MAGIC     LEFT JOIN max_schedule_del ON b.load_number = max_schedule_del.relay_reference_number
# MAGIC     LEFT JOIN pickup_projection pp ON b.load_number = pp.relay_reference_number AND b.pickup_name = pp.shipper_name AND b.weight = pp.weight_to_pickup_amount AND b.piece_count = pp.pieces_to_pickup_count AND max_schedule.max_schedule = pp.scheduled_at AND pp.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule pss ON b.load_number = pss.relay_reference_number AND b.pickup_name = pss.stop_name AND pss.sequence_number = 1 AND pss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop c ON pss.stop_id = c.stop_id
# MAGIC     LEFT JOIN delivery_projection dp ON b.load_number = dp.relay_reference_number AND b.consignee_name = dp.receiver_name AND max_schedule_del.max_schedule = dp.scheduled_at
# MAGIC     LEFT JOIN last_delivery ON b.load_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN planning_stop_schedule dpss ON b.load_number = dpss.relay_reference_number AND b.consignee_name = dpss.stop_name AND b.consignee_city = dpss.locality AND last_delivery.last_delivery = dpss.sequence_number AND dpss.stop_type = 'delivery' AND dpss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop cd ON dpss.stop_id = cd.stop_id
# MAGIC     LEFT JOIN hain_tracking_accuracy_projection ON b.load_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC     LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC     WHERE b.dispatch_status != 'Cancelled' AND (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC ),
# MAGIC
# MAGIC   system_union AS (
# MAGIC     SELECT DISTINCT
# MAGIC       aljex_data.status AS `status`,
# MAGIC       aljex_data.key_c_user,
# MAGIC       TRY_CAST(aljex_data.id AS FLOAT) AS `loadnum`,
# MAGIC       aljex_data.original_shipper AS `customer`,
# MAGIC       aljex_data.mastername AS `mastername`,
# MAGIC       CAST(aljex_data.use_date AS DATE) AS `shipdate`,
# MAGIC       analytics.dim_financial_calendar.financial_period AS `period`,
# MAGIC       analytics.dim_financial_calendar.date AS `Date`,
# MAGIC       aljex_data.origin_city,
# MAGIC       aljex_data.origin_state,
# MAGIC       aljex_data.origin_zip,
# MAGIC       aljex_data.dest_city,
# MAGIC       aljex_data.dest_state,
# MAGIC       aljex_data.dest_zip,
# MAGIC       aljex_data.dot_number AS `dot_num`,
# MAGIC       CAST(aljex_data.use_date AS DATE) AS last_ship
# MAGIC     FROM
# MAGIC       aljex_data
# MAGIC       LEFT JOIN customer_lookup ON aljex_data.mastername = customer_lookup.master_customer_name
# MAGIC       LEFT JOIN analytics.dim_financial_calendar ON CAST(aljex_data.use_date AS DATE) = analytics.dim_financial_calendar.date
# MAGIC     WHERE
# MAGIC       -- financial_calendar.date BETWEEN (CURRENT_DATE - INTERVAL 90 DAY) AND CURRENT_DATE
# MAGIC        CAST(aljex_data.use_date AS DATE) < CURRENT_TIMESTAMP
# MAGIC       AND aljex_data.equipment != 'TORD'
# MAGIC     UNION
# MAGIC     SELECT DISTINCT
# MAGIC       b.status AS `status`,
# MAGIC       b.booked_by_name,
# MAGIC       TRY_CAST(b.relay_reference_number AS FLOAT) AS `loadnum`,
# MAGIC       r.customer_name AS `customer`,
# MAGIC       customer_lookup.master_customer_name AS `mastername`,
# MAGIC       CAST(COALESCE(master_date.pu_appt_date, master_date.ready_date) AS DATE) AS `shipdate`,
# MAGIC       analytics.dim_financial_calendar.financial_period AS `period`,
# MAGIC       analytics.dim_financial_calendar.date AS `Date`,
# MAGIC       UPPER(b.first_shipper_city) AS `origin_city`,
# MAGIC       b.first_shipper_state AS `origin_state`,
# MAGIC       b.first_shipper_zip AS `origin_zip`,
# MAGIC       UPPER(b.receiver_city) AS `dest_city`,
# MAGIC       b.receiver_state AS `dest_state`,
# MAGIC       b.receiver_zip AS `dest_zip`,
# MAGIC       c.dot_number AS `dot_num`,
# MAGIC             COALESCE(
# MAGIC         CAST(master_date.pu_appt_start_datetime AS DATE),
# MAGIC         CAST(master_date.use_date AS DATE)
# MAGIC       ) AS last_ship
# MAGIC     FROM
# MAGIC       booking_projection b
# MAGIC       LEFT JOIN master_date ON b.booking_id = master_date.booking_id
# MAGIC       LEFT JOIN  analytics.dim_financial_calendar ON CAST(COALESCE(master_date.pu_appt_date, master_date.ready_date) AS DATE) =  analytics.dim_financial_calendar.date
# MAGIC       LEFT JOIN customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       LEFT JOIN carrier_projection c ON b.booked_carrier_id = c.carrier_id
# MAGIC       LEFT JOIN customer_lookup ON r.customer_name = aljex_customer_name
# MAGIC       LEFT JOIN relay_users ON b.booked_by_name = relay_users.full_name
# MAGIC         AND relay_users.`active?` = 'true'
# MAGIC     WHERE
# MAGIC       COALESCE(master_date.pu_appt_date, master_date.ready_date) IS NOT NULL
# MAGIC       AND b.status = 'booked'
# MAGIC       AND CAST(COALESCE(master_date.pu_appt_date, master_date.ready_date) AS DATE) < CURRENT_TIMESTAMP
# MAGIC       -- AND financial_calendar.date BETWEEN (CURRENT_DATE - INTERVAL 90 DAY) AND CURRENT_DATE
# MAGIC   ),
# MAGIC   carrier_name AS (
# MAGIC     SELECT DISTINCT
# MAGIC       system_union.dot_num AS `dotnum`,
# MAGIC       CASE
# MAGIC         WHEN MAX(pc.name) IS NULL THEN MAX(cp.carrier_name)
# MAGIC         ELSE MAX(pc.name)
# MAGIC       END AS `carrier_name`,
# MAGIC       MAX(pc.mc_num) AS mc_num,
# MAGIC       MAX(pc.phone) AS phone,
# MAGIC       COALESCE(
# MAGIC         MAX(pc.email1),
# MAGIC         MAX(pc.email2),
# MAGIC         MAX(cp.default_rate_con_recipients)
# MAGIC       ) AS email,
# MAGIC       pc.state
# MAGIC     FROM
# MAGIC       system_union
# MAGIC       LEFT JOIN projection_carrier pc ON TRY_CAST(system_union.dot_num AS STRING) = TRY_CAST(pc.dot_num AS STRING)
# MAGIC       LEFT JOIN carrier_projection cp ON TRY_CAST(system_union.dot_num AS STRING) = TRY_CAST(cp.dot_number AS STRING)
# MAGIC     GROUP BY
# MAGIC       system_union.dot_num,
# MAGIC       pc.state
# MAGIC   ),
# MAGIC   summary_data AS (
# MAGIC     SELECT DISTINCT
# MAGIC       loadnum,
# MAGIC      system_union. Date,
# MAGIC       dot_num,
# MAGIC       COALESCE(
# MAGIC         carrier_name.mc_num,
# MAGIC         TRY_CAST(caam_list.mc_number AS STRING),
# MAGIC         TRY_CAST(new_carrier_ownership.mc_number AS STRING)
# MAGIC       ) AS mc_number,
# MAGIC       carrier_name.email,
# MAGIC       carrier_name.phone,
# MAGIC       carrier_name.carrier_name,
# MAGIC       mastername,
# MAGIC       key_c_user,
# MAGIC       shipdate,
# MAGIC       carrier_name.state
# MAGIC     FROM
# MAGIC       system_union
# MAGIC       LEFT JOIN carrier_name ON TRY_CAST(dot_num AS INT) = TRY_CAST(carrier_name.dotnum AS INT)
# MAGIC       LEFT JOIN caam_list ON TRY_CAST(caam_list.dot_number AS INT) = TRY_CAST(system_union.dot_num AS INT)
# MAGIC       LEFT JOIN new_carrier_ownership ON TRY_CAST(new_carrier_ownership.dot_number AS INT) = TRY_CAST(system_union.dot_num AS INT)
# MAGIC     WHERE
# MAGIC       dot_num IS NOT NULL
# MAGIC       -- AND TRY_CAST(dot_num AS INT) != 0 
# MAGIC       -- [[AND system_union.dot_num = $dot]]
# MAGIC       -- [[AND COALESCE(carrier_name.mc_num, TRY_CAST(caam_list.mc_number AS STRING), TRY_CAST(new_carrier_ownership.mc_number AS STRING)) = $mc]]
# MAGIC       -- [[AND carrier_name.state = $state]]
# MAGIC   )
# MAGIC ,Final_code as ( SELECT DISTINCT
# MAGIC   CASE
# MAGIC     WHEN dot_num IN ('1486168', '2242124', '85840') THEN 'NFI Transportation'
# MAGIC     ELSE carrier_name
# MAGIC   END AS `Carrier`,
# MAGIC   Date,
# MAGIC   state AS `Carrier_State`,
# MAGIC   COUNT(DISTINCT loadnum) AS `Volume`,
# MAGIC   dot_num AS `DOT_Num`,
# MAGIC   mc_number AS `MC_Num`,
# MAGIC   phone AS `Phone`,
# MAGIC   email AS `Email`,
# MAGIC   CONCAT(
# MAGIC     'https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string=',
# MAGIC     dot_num
# MAGIC   ) AS `Link_to_Further_Info`
# MAGIC FROM
# MAGIC   summary_data 
# MAGIC   -- where 
# MAGIC   -- date BETWEEN DATE_SUB(CURRENT_DATE, 5) AND CURRENT_DATE
# MAGIC GROUP BY
# MAGIC   carrier_name,
# MAGIC   dot_num,
# MAGIC   mc_number,
# MAGIC   phone,
# MAGIC   email,
# MAGIC   state,
# MAGIC   Date
# MAGIC HAVING
# MAGIC   COUNT(DISTINCT loadnum) >= 5
# MAGIC ORDER BY
# MAGIC   `Volume` DESC)
# MAGIC   SELECT * FROM Final_code 
# MAGIC
# MAGIC
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Carrier_Consolidated_Load_Genai_Temp;
# MAGIC INSERT INTO analytics.Carrier_Consolidated_Load_Genai_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Carrier_Consolidated_Load_Genai

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Carrier_Consolidated_Load_Genai;
# MAGIC INSERT INTO analytics.Carrier_Consolidated_Load_Genai
# MAGIC
# MAGIC SELECT *
# MAGIC FROM analytics.Carrier_Consolidated_Load_Genai_Temp