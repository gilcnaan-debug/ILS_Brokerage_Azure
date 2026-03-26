# Databricks notebook source
# MAGIC %md
# MAGIC ##Creation Of Spot Data Table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC SET ansi_mode = false;
# MAGIC create or replace table analytics.spot_data as 
# MAGIC WITH to_get_avg AS (
# MAGIC   SELECT DISTINCT
# MAGIC     ship_date,
# MAGIC     conversion AS us_to_cad,
# MAGIC     us_to_cad AS cad_to_us
# MAGIC   FROM
# MAGIC     brokerageprod.bronze.canada_conversions
# MAGIC   ORDER BY
# MAGIC     ship_date DESC
# MAGIC   LIMIT 7
# MAGIC ),
# MAGIC average_conversions AS (
# MAGIC   SELECT
# MAGIC     AVG(us_to_cad) AS avg_us_to_cad,
# MAGIC     AVG(cad_to_us) AS avg_cad_to_us
# MAGIC   FROM
# MAGIC     to_get_avg
# MAGIC ),
# MAGIC invoicing_cred AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     invoicing_credits.relay_reference_number,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN invoicing_credits.currency = 'CAD' THEN invoicing_credits.total_amount * COALESCE(
# MAGIC           CASE 
# MAGIC             WHEN canada_conversions.us_to_cad = 0 THEN NULL 
# MAGIC             ELSE canada_conversions.us_to_cad 
# MAGIC           END, average_conversions.avg_cad_to_us)
# MAGIC         ELSE invoicing_credits.total_amount
# MAGIC       END
# MAGIC     ) / 100.00 AS invoicing_cred
# MAGIC   FROM
# MAGIC     invoicing_credits
# MAGIC   LEFT JOIN canada_conversions 
# MAGIC     ON CAST(invoicing_credits.credited_at AS DATE) = canada_conversions.ship_date
# MAGIC   JOIN average_conversions ON TRUE
# MAGIC   GROUP BY invoicing_credits.relay_reference_number
# MAGIC ),
# MAGIC projection_load as (Select p1.*,  p2.ps_arrive_code_1 ,
# MAGIC         p2.ps_arrive_code_2          ,
# MAGIC         p2.ps_arrive_code_3          ,
# MAGIC         p2.ps_arrive_code_4          ,
# MAGIC         p2.ps_arrive_code_5          ,
# MAGIC         p2.ps_arrive_code_6          ,
# MAGIC         p2.ps_arrive_code_7          ,
# MAGIC         p2.ps_arrive_code_8          ,
# MAGIC         p2.ps_arrive_code_9          ,
# MAGIC         p2.ps_arrive_code_10         ,
# MAGIC         p2.ps_arrive_code_11         ,
# MAGIC         p2.ps_arrive_code_12         ,
# MAGIC         p2.ps_arrive_code_16         ,
# MAGIC         p2.ps_arrive_date_1          ,
# MAGIC         p2.ps_arrive_date_2          ,
# MAGIC         p2.ps_arrive_date_3          ,
# MAGIC         p2.ps_arrive_date_4          ,
# MAGIC         p2.ps_arrive_date_5          ,
# MAGIC         p2.ps_arrive_date_6          ,
# MAGIC         p2.ps_arrive_date_7          ,
# MAGIC         p2.ps_arrive_date_8          ,
# MAGIC         p2.ps_arrive_date_9          ,
# MAGIC         p2.ps_arrive_date_10         ,
# MAGIC         p2.ps_arrive_date_11         ,
# MAGIC         p2.ps_arrive_date_12         ,
# MAGIC         p2.ps_arrive_time_1          ,
# MAGIC         p2.ps_arrive_time_2          ,
# MAGIC         p2.ps_arrive_time_3          ,
# MAGIC         p2.ps_arrive_time_4          ,
# MAGIC         p2.ps_arrive_time_5          ,
# MAGIC         p2.ps_arrive_time_6          ,
# MAGIC         p2.ps_arrive_time_7          ,
# MAGIC         p2.ps_arrive_time_8          ,
# MAGIC         p2.ps_arrive_time_9          ,
# MAGIC         p2.ps_arrive_time_10         ,
# MAGIC         p2.ps_arrive_time_11         ,
# MAGIC         p2.ps_arrive_time_12         ,
# MAGIC         p2.ps_arrive_time_20         ,
# MAGIC         p2.ps_city_1                 ,
# MAGIC         p2.ps_city_2                 ,
# MAGIC         p2.ps_city_3                 ,
# MAGIC         p2.ps_city_4                 ,
# MAGIC         p2.ps_city_5                 ,
# MAGIC         p2.ps_city_6                 ,
# MAGIC         p2.ps_city_7                 ,
# MAGIC         p2.ps_city_8                 ,
# MAGIC         p2.ps_city_9                 ,
# MAGIC         p2.ps_city_10                ,
# MAGIC         p2.ps_city_11                ,
# MAGIC         p2.ps_city_12                ,
# MAGIC         p2.ps_city_13                ,
# MAGIC         p2.ps_city_14                ,
# MAGIC         p2.ps_city_15                ,
# MAGIC         p2.ps_city_16                ,
# MAGIC         p2.ps_city_17                ,
# MAGIC         p2.ps_city_18                ,
# MAGIC         p2.ps_city_19                ,
# MAGIC         p2.ps_company_1              ,
# MAGIC         p2.ps_company_2              ,
# MAGIC         p2.ps_company_3              ,
# MAGIC         p2.ps_company_4              ,
# MAGIC         p2.ps_company_5              ,
# MAGIC         p2.ps_company_6              ,
# MAGIC         p2.ps_company_7              ,
# MAGIC         p2.ps_company_8              ,
# MAGIC         p2.ps_company_9              ,
# MAGIC         p2.ps_company_10             ,
# MAGIC         p2.ps_company_11             ,
# MAGIC         p2.ps_company_12             ,
# MAGIC         p2.ps_company_13             ,
# MAGIC         p2.ps_company_14             ,
# MAGIC         p2.ps_company_15             ,
# MAGIC         p2.ps_company_16             ,
# MAGIC         p2.ps_company_17             ,
# MAGIC         p2.ps_company_18             ,
# MAGIC         p2.ps_company_19             ,
# MAGIC         p2.ps_depart_code_1          ,
# MAGIC         p2.ps_depart_code_2          ,
# MAGIC         p2.ps_depart_code_3          ,
# MAGIC         p2.ps_depart_code_4          ,
# MAGIC         p2.ps_depart_code_5          ,
# MAGIC         p2.ps_depart_code_6          ,
# MAGIC         p2.ps_depart_code_7          ,
# MAGIC         p2.ps_depart_code_8          ,
# MAGIC         p2.ps_depart_code_9          ,
# MAGIC         p2.ps_depart_code_10         ,
# MAGIC         p2.ps_depart_code_11         ,
# MAGIC         p2.ps_depart_code_12         ,
# MAGIC         p2.ps_depart_code_14         ,
# MAGIC         p2.ps_depart_date_1          ,
# MAGIC         p2.ps_depart_date_2          ,
# MAGIC         p2.ps_depart_date_3          ,
# MAGIC         p2.ps_depart_date_4          ,
# MAGIC         p2.ps_depart_date_5          ,
# MAGIC         p2.ps_depart_date_6          ,
# MAGIC         p2.ps_depart_date_7          ,
# MAGIC         p2.ps_depart_date_8          ,
# MAGIC         p2.ps_depart_date_9          ,
# MAGIC         p2.ps_depart_date_10         ,
# MAGIC         p2.ps_depart_date_11         ,
# MAGIC         p2.ps_depart_date_12         ,
# MAGIC         p2.ps_depart_time_1          ,
# MAGIC         p2.ps_depart_time_2          ,
# MAGIC         p2.ps_depart_time_3          ,
# MAGIC         p2.ps_depart_time_4          ,
# MAGIC         p2.ps_depart_time_5          ,
# MAGIC         p2.ps_depart_time_6          ,
# MAGIC         p2.ps_depart_time_7          ,
# MAGIC         p2.ps_depart_time_8          ,
# MAGIC         p2.ps_depart_time_9          ,
# MAGIC         p2.ps_depart_time_10         ,
# MAGIC         p2.ps_depart_time_11         ,
# MAGIC         p2.ps_depart_time_12         ,
# MAGIC         p2.ps_ref_1                  ,
# MAGIC         p2.ps_ref_2                  ,
# MAGIC         p2.ps_ref_3                  ,
# MAGIC         p2.ps_ref_4                  ,
# MAGIC         p2.ps_ref_5                  ,
# MAGIC         p2.ps_ref_6                  ,
# MAGIC         p2.ps_ref_7                  ,
# MAGIC         p2.ps_ref_8                  ,
# MAGIC         p2.ps_ref_9                  ,
# MAGIC         p2.ps_ref_10                 ,
# MAGIC         p2.ps_ref_11                 ,
# MAGIC         p2.ps_ref_12                 ,
# MAGIC         p2.ps_ref_13                 ,
# MAGIC         p2.ps_ref_14                 ,
# MAGIC         p2.ps_ref_15                 ,
# MAGIC         p2.ps_ref_16                 ,
# MAGIC         p2.ps_ref_17                 ,
# MAGIC         p2.ps_ref_18                 ,
# MAGIC         p2.ps_ref_19                 ,
# MAGIC         p2.ps_state_1                ,
# MAGIC         p2.ps_state_2                ,
# MAGIC         p2.ps_state_3                ,
# MAGIC         p2.ps_state_4                ,
# MAGIC         p2.ps_state_5                ,
# MAGIC         p2.ps_state_6                ,
# MAGIC         p2.ps_state_7                ,
# MAGIC         p2.ps_state_8                ,
# MAGIC         p2.ps_state_9                ,
# MAGIC         p2.ps_state_10               ,
# MAGIC         p2.ps_state_11               ,
# MAGIC         p2.ps_state_12               ,
# MAGIC         p2.ps_state_13               ,
# MAGIC         p2.ps_state_14               ,
# MAGIC         p2.ps_state_15               ,
# MAGIC         p2.ps_state_16               ,
# MAGIC         p2.ps_state_17               ,
# MAGIC         p2.ps_state_18               ,
# MAGIC         p2.ps_state_19               ,
# MAGIC         p2.ps_zip_1                  ,
# MAGIC         p2.ps_zip_2                  ,
# MAGIC         p2.ps_zip_3                  ,
# MAGIC         p2.ps_zip_4                  ,
# MAGIC         p2.ps_zip_5                  ,
# MAGIC         p2.ps_zip_6                  ,
# MAGIC         p2.ps_zip_7                  ,
# MAGIC         p2.ps_zip_8                  ,
# MAGIC         p2.ps_zip_9                  ,
# MAGIC         p2.ps_zip_10                 ,
# MAGIC         p2.ps_zip_11                 ,
# MAGIC         p2.ps_zip_12                 ,
# MAGIC         p2.ps_zip_13                 ,
# MAGIC         p2.ps_zip_14                 ,
# MAGIC         p2.ps_zip_15                 ,
# MAGIC         p2.ps_zip_16                 ,
# MAGIC         p2.ps_zip_17                 ,
# MAGIC         p2.ps_zip_18                 ,
# MAGIC         p2.ps_zip_19                 ,
# MAGIC         p2.Ps1                       ,
# MAGIC         p2.Ps2                       ,
# MAGIC         p2.Ps3                       ,
# MAGIC         p2.Ps4                       ,
# MAGIC         p2.Ps5                       ,
# MAGIC         p2.Ps6                       ,
# MAGIC         p2.Ps7                       ,
# MAGIC         p2.Ps8                       ,
# MAGIC         p2.Ps9                       ,
# MAGIC         p2.Ps10                      ,
# MAGIC         p2.Ps11                      ,
# MAGIC         p2.Ps12                      ,
# MAGIC         p2.Ps13                      ,
# MAGIC         p2.Ps14                      ,
# MAGIC         p2.Ps15                      ,
# MAGIC         p2.Ps16                      ,
# MAGIC         p2.Ps17                      ,
# MAGIC         p2.Ps18                      ,
# MAGIC         p2.Ps19                      ,
# MAGIC         p2.quote_number              ,
# MAGIC         p2.ref_num                   ,
# MAGIC         p2.release_date              ,
# MAGIC         p2.sales_rep                 ,
# MAGIC         p2.sales_team                ,
# MAGIC         p2.seal                      ,
# MAGIC         p2.shipper                   ,
# MAGIC         p2.signed_by                 ,
# MAGIC         p2.sls_pct                   ,
# MAGIC         p2.srv_rep                   ,
# MAGIC         p2.status                    ,
# MAGIC         p2.straps_chains             ,
# MAGIC         p2.tag_created_by            ,
# MAGIC         p2.tag_creation_date         ,
# MAGIC         p2.tag_creation_time         ,
# MAGIC         p2.tariff_num                ,
# MAGIC         p2.tarp_required             ,
# MAGIC         p2.tarp_size                 ,
# MAGIC         p2.temp                      ,
# MAGIC         p2.temp_ctrl                 ,
# MAGIC         p2.time                      ,
# MAGIC         p2.Tord                      ,
# MAGIC         p2.trailer_number            ,
# MAGIC         p2.truck_num                 ,
# MAGIC         p2.value                     ,
# MAGIC         p2.web_post                  ,
# MAGIC         p2.web_sync_action           ,
# MAGIC         p2.web_sync_table_name       ,
# MAGIC         p2.weight                    ,
# MAGIC         p2.will_pickup_date          ,
# MAGIC         p2.will_pickup_time          ,
# MAGIC         p2.asg_disp                  ,
# MAGIC         p2.bl_ref                    ,
# MAGIC         p2.carrier_accessorial_rate1 ,
# MAGIC         p2.carrier_accessorial_rate2 ,
# MAGIC         p2.carrier_accessorial_rate3 ,
# MAGIC         p2.carrier_accessorial_rate4 ,
# MAGIC         p2.carrier_accessorial_rate5 ,
# MAGIC         p2.carrier_accessorial_rate6 ,
# MAGIC         p2.carrier_accessorial_rate7 ,
# MAGIC         p2.carrier_accessorial_rate8 ,
# MAGIC         p2.customer_accessorial_rate1,
# MAGIC         p2.customer_accessorial_rate2,
# MAGIC         p2.customer_accessorial_rate3,
# MAGIC         p2.customer_accessorial_rate4,
# MAGIC         p2.customer_accessorial_rate5,
# MAGIC         p2.customer_accessorial_rate6,
# MAGIC         p2.customer_accessorial_rate7,
# MAGIC         p2.customer_accessorial_rate8,
# MAGIC         p2.customer_id               ,
# MAGIC         p2.nmfc_num  from bronze.projection_load_1 as p1 LEFT JOIN bronze.projection_load_2 as p2 ON p1.id = p2.id),
# MAGIC aljex_data as ( 
# MAGIC SELECT DISTINCT CAST(p.id AS INT) AS id,
# MAGIC     p.customer_id,
# MAGIC     p.ref_num AS shipment_id,
# MAGIC     p.status,
# MAGIC         CASE
# MAGIC             WHEN aljex_user_report_listing.full_name IS NULL THEN p.key_c_user
# MAGIC             ELSE aljex_user_report_listing.full_name
# MAGIC         END AS key_c_user,
# MAGIC     p.ps1,
# MAGIC         CASE
# MAGIC             WHEN dis.full_name IS NULL THEN p.key_h_user
# MAGIC             ELSE dis.full_name
# MAGIC         END AS key_h_user,
# MAGIC         CASE
# MAGIC             WHEN p.key_c_user = 'IMPORT' AND p.shipper = 'ALTMAN PLANTS' THEN 'LAX'
# MAGIC             WHEN p.key_c_user = 'IMPORT' AND p.equipment = 'LTL' AND p.office IN ('64', '63', '62', '61') THEN 'TOR'
# MAGIC             WHEN p.key_c_user = 'IMPORT' AND p.equipment = 'LTL' THEN 'LTL'
# MAGIC             WHEN p.key_c_user = 'IMPORT' THEN 'DRAY'
# MAGIC             WHEN p.key_c_user = 'EDILCR' THEN 'LTL'
# MAGIC             ELSE car.new_office
# MAGIC         END AS carr_office,
# MAGIC     p.shipper AS original_shipper,
# MAGIC         CASE
# MAGIC             WHEN customer_lookup.master_customer_name IS NULL THEN p.shipper
# MAGIC             ELSE customer_lookup.master_customer_name
# MAGIC         END AS mastername,
# MAGIC     CASE
# MAGIC       WHEN TRY_CAST(
# MAGIC         COALESCE(
# MAGIC           p.arrive_pickup_date,
# MAGIC           p.loaded_date,
# MAGIC           p.pickup_date
# MAGIC         ) AS DATE
# MAGIC       ) IS NOT NULL THEN TRY_CAST(
# MAGIC         COALESCE(
# MAGIC           p.arrive_pickup_date,
# MAGIC           p.loaded_date,
# MAGIC           p.pickup_date
# MAGIC         ) AS DATE
# MAGIC       )
# MAGIC       ELSE NULL
# MAGIC     END AS use_date,
# MAGIC     CASE
# MAGIC       WHEN DAYOFWEEK(
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           ) IS NOT NULL THEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           )
# MAGIC           ELSE NULL
# MAGIC         END
# MAGIC       ) = 1 THEN CASE
# MAGIC         WHEN TRY_CAST(
# MAGIC           COALESCE(
# MAGIC             p.arrive_pickup_date,
# MAGIC             p.loaded_date,
# MAGIC             p.pickup_date
# MAGIC           ) AS DATE
# MAGIC         ) IS NOT NULL THEN TRY_CAST(
# MAGIC           COALESCE(
# MAGIC             p.arrive_pickup_date,
# MAGIC             p.loaded_date,
# MAGIC             p.pickup_date
# MAGIC           ) AS DATE
# MAGIC         )
# MAGIC         ELSE NULL
# MAGIC       END + INTERVAL 6 DAYS
# MAGIC       ELSE DATE_TRUNC(
# MAGIC         'WEEK',
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           ) IS NOT NULL THEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           )
# MAGIC           ELSE NULL
# MAGIC         END
# MAGIC       ) + INTERVAL 5 DAYS
# MAGIC     END AS end_date,
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_line_haul AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate1 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate2 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate3 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate4 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate5 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate6 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate7 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate8 AS STRING), ''), 0.00) AS final_carrier_rate,
# MAGIC         CASE
# MAGIC             WHEN p.value RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.value AS FLOAT)
# MAGIC             ELSE 0.00
# MAGIC         END AS cargo_value,
# MAGIC         CASE
# MAGIC             WHEN p.office NOT IN ('10', '34', '51', '54', '61', '62', '63', '64', '74') THEN 'USD'
# MAGIC             ELSE COALESCE(cad_currency.currency_type, aljex_customer_profiles.cust_country, 'CAD')
# MAGIC         END AS cust_curr,
# MAGIC         CASE
# MAGIC             WHEN c.name RLIKE '%CAD' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE 'CANADIAN R%' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE '%CAN' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE '%(CAD)%' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE '%-C' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE '%- C%' THEN 'CAD'
# MAGIC             WHEN canada_carriers.canada_dot IS NOT NULL THEN 'CAD'
# MAGIC             ELSE 'USD'
# MAGIC         END AS carrier_curr,
# MAGIC         COALESCE(canada_conversions.conversion, average_conversions.avg_us_to_cad) AS conversion_rate,
# MAGIC         COALESCE(canada_conversions.us_to_cad, average_conversions.avg_cad_to_us) AS conversion_rate_cad,
# MAGIC     new_office_lookup.new_office AS customer_office,
# MAGIC     p.office AS proj_load_office,
# MAGIC     p.equipment,
# MAGIC     p.mode AS proj_load_mode,
# MAGIC         CASE
# MAGIC             WHEN p.equipment RLIKE '%LTL%' THEN 'LTL'
# MAGIC             ELSE 'TL'
# MAGIC         END AS modee,
# MAGIC     p.invoice_total,
# MAGIC     c.name AS carrier_name,
# MAGIC     Analytics.Dim_financial_calendar.financial_period_sorting,
# MAGIC     c.dot_num AS dot_number,
# MAGIC     p.pickup_name,
# MAGIC     p.origin_city,
# MAGIC     p.origin_state,
# MAGIC     p.pickup_zip_code AS origin_zip,
# MAGIC     p.consignee,
# MAGIC     p.consignee_address,
# MAGIC     p.pickup_address,
# MAGIC     p.dest_city,
# MAGIC     p.dest_state,
# MAGIC     p.consignee_zip_code AS dest_zip,
# MAGIC         CASE
# MAGIC             WHEN p.miles RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.miles AS FLOAT)
# MAGIC             ELSE 0.0
# MAGIC         END AS miles,
# MAGIC         CASE
# MAGIC             WHEN p.weight RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.weight AS FLOAT)
# MAGIC             ELSE 0.0
# MAGIC         END AS weight,
# MAGIC     p.truck_num,
# MAGIC     p.trailer_number,
# MAGIC     p.driver_cell_num,
# MAGIC     p.carrier_id,
# MAGIC     p.hazmat,
# MAGIC     COALESCE(sales.full_name, p.sales_rep) AS sales_rep,
# MAGIC         CASE
# MAGIC             WHEN p.srv_rep = 'BRIOP' THEN 'Brianna Martinez'
# MAGIC             ELSE COALESCE(am.full_name, p.srv_rep)
# MAGIC         END AS srv_rep,
# MAGIC     p.accessorial1,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate1 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate1 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate1 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc1_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate1 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc1_rate,
# MAGIC
# MAGIC p.accessorial2,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate2 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate2 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate2 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc2_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate2 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc2_rate,
# MAGIC
# MAGIC p.accessorial3,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate3 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate3 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate3 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc3_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate3 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc3_rate,
# MAGIC
# MAGIC p.accessorial4,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate4 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate4 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate4 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc4_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate4 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc4_rate,
# MAGIC
# MAGIC p.accessorial5,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate5 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate5 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate5 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc5_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate5 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc5_rate,
# MAGIC
# MAGIC p.accessorial6,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate6 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate6 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate6 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc6_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate6 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc6_rate,
# MAGIC
# MAGIC p.accessorial7,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate7 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate7 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate7 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate7 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc7_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate7 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate7 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc7_rate,
# MAGIC
# MAGIC p.accessorial8,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate8 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate8 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate8 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate8 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END 
# MAGIC END AS carr_acc8_rate,
# MAGIC
# MAGIC CASE 
# MAGIC     WHEN p.customer_accessorial_rate8 IS NULL THEN NULL 
# MAGIC     ELSE 
# MAGIC         CASE 
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate8 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC END AS cust_acc8_rate,
# MAGIC CASE
# MAGIC     WHEN p.accessorial1 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial2 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial3 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial4 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial5 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial6 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial7 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate7 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial8 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate8 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END AS customer_accessorial,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.accessorial1 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial2 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial3 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial4 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial5 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial6 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial7 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate7 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial8 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate8 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END AS customer_fuel,
# MAGIC
# MAGIC CASE 
# MAGIC     WHEN p.carrier_line_haul RLIKE '%:%' THEN 0.00 
# MAGIC     WHEN p.carrier_line_haul RLIKE '^-?\\d+(\\.\\d+)?$' THEN CAST(p.carrier_line_haul AS DOUBLE) 
# MAGIC     ELSE NULL 
# MAGIC END AS carrier_linehaul,
# MAGIC
# MAGIC CASE 
# MAGIC     WHEN p.accessorial1 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate1 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate1 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate1 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial2 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate2 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate2 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate2 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial3 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate3 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate3 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate3 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial4 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate4 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate4 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate4 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial5 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate5 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate5 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate5 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial6 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate6 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate6 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate6 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial7 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate7 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate7 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate7 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial8 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate8 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate8 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate8 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END AS carrier_fuel,
# MAGIC
# MAGIC p.ref_num
# MAGIC
# MAGIC FROM projection_load p
# MAGIC
# MAGIC LEFT JOIN Analytics.Dim_financial_calendar ON DATE(p.pickup_date) = Analytics.Dim_financial_calendar.date
# MAGIC
# MAGIC JOIN new_office_lookup ON CAST(p.office AS STRING) = new_office_lookup.old_office
# MAGIC
# MAGIC LEFT JOIN cad_currency ON CAST(p.id AS STRING) = cad_currency.pro_number
# MAGIC
# MAGIC LEFT JOIN aljex_customer_profiles ON CAST(p.customer_id AS STRING) = aljex_customer_profiles.cust_id
# MAGIC
# MAGIC LEFT JOIN projection_carrier c ON CAST(p.carrier_id AS STRING) = c.id
# MAGIC
# MAGIC LEFT JOIN canada_carriers ON CAST(p.carrier_id AS STRING) = canada_carriers.canada_dot
# MAGIC
# MAGIC LEFT JOIN customer_lookup ON LEFT(CAST(p.shipper AS STRING), 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC
# MAGIC JOIN average_conversions ON TRUE
# MAGIC
# MAGIC LEFT JOIN aljex_user_report_listing am ON CAST(p.srv_rep AS STRING) = am.aljex_id AND am.aljex_id IS NOT NULL
# MAGIC
# MAGIC LEFT JOIN aljex_user_report_listing sales ON CAST(p.sales_rep AS STRING) = sales.sales_rep AND sales.sales_rep IS NOT NULL
# MAGIC
# MAGIC LEFT JOIN canada_conversions ON DATE(p.pickup_date) = canada_conversions.ship_date
# MAGIC
# MAGIC LEFT JOIN aljex_user_report_listing ON CAST(p.key_c_user AS STRING) = aljex_user_report_listing.aljex_id AND aljex_user_report_listing.aljex_id <> 'repeat'
# MAGIC
# MAGIC LEFT JOIN relay_users ON aljex_user_report_listing.full_name = relay_users.full_name AND relay_users.`active?` = TRUE
# MAGIC
# MAGIC LEFT JOIN new_office_lookup car ON COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) = car.old_office
# MAGIC
# MAGIC LEFT JOIN aljex_user_report_listing dis ON CAST(p.key_h_user AS STRING) = dis.aljex_id AND dis.aljex_id <> 'repeat'
# MAGIC
# MAGIC WHERE NOT (p.status LIKE '%VOID%')
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC combined_loads_mine as ( SELECT DISTINCT 
# MAGIC     p.relay_reference_number_one,
# MAGIC     p.relay_reference_number_two,
# MAGIC     p.resulting_plan_id,
# MAGIC     c.relay_reference_number AS combine_new_rrn,
# MAGIC     c.mode,
# MAGIC     b.tender_on_behalf_of_id,
# MAGIC     'COMBINED' AS combined,
# MAGIC     COALESCE(one_money.one_money, 0.0) AS one_money,
# MAGIC     COALESCE(one_lhc_money.one_lhc_money, 0.0) AS one_lhc_money,
# MAGIC     COALESCE(one_fuel_money.one_fuel_money, 0.0) AS one_fuel_money,
# MAGIC     COALESCE(two_money.two_money, 0.0) AS two_money,
# MAGIC     COALESCE(two_lhc_money.two_lhc_money, 0.0) AS two_lhc_money,
# MAGIC     COALESCE(two_fuel_money.two_fuel_money, 0.0) AS two_fuel_money,
# MAGIC     COALESCE(one_money.one_money, 0.0) + COALESCE(two_money.two_money, 0.0) AS total_load_rev,
# MAGIC     COALESCE(one_lhc_money.one_lhc_money, 0.0) + COALESCE(two_lhc_money.two_lhc_money, 0.0) AS total_load_lhc_rev,
# MAGIC     COALESCE(one_fuel_money.one_fuel_money, 0.0) + COALESCE(two_fuel_money.two_fuel_money, 0.0) AS total_load_fuel_rev,
# MAGIC     COALESCE(carrier_money.carrier_money, 0.0) AS final_carrier_rate,
# MAGIC     COALESCE(carrier_linehaul.carrier_linehaul, 0.0) AS carrier_linehaul,
# MAGIC     COALESCE(fuel_surcharge.fuel_surcharge, 0.0) AS carrier_fuel_surcharge
# MAGIC FROM plan_combination_projection p
# MAGIC JOIN canonical_plan_projection c ON p.resulting_plan_id = c.plan_id
# MAGIC LEFT JOIN booking_projection b ON c.relay_reference_number = b.relay_reference_number
# MAGIC
# MAGIC -- One Money
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT SUM(m.amount) / 100.00 AS one_money
# MAGIC     FROM moneying_billing_party_transaction m
# MAGIC     WHERE p.relay_reference_number_one = m.relay_reference_number AND m.`voided?` = false
# MAGIC ) one_money ON true
# MAGIC
# MAGIC -- One Fuel Money
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT SUM(m.amount) / 100.00 AS one_fuel_money
# MAGIC     FROM moneying_billing_party_transaction m
# MAGIC     WHERE p.relay_reference_number_one = m.relay_reference_number AND m.`voided?` = false AND m.charge_code = 'fuel_surcharge'
# MAGIC ) one_fuel_money ON true
# MAGIC
# MAGIC -- One LHC Money
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT SUM(m.amount) / 100.00 AS one_lhc_money
# MAGIC     FROM moneying_billing_party_transaction m
# MAGIC     WHERE p.relay_reference_number_one = m.relay_reference_number AND m.`voided?` = false AND m.charge_code = 'linehaul'
# MAGIC ) one_lhc_money ON true
# MAGIC
# MAGIC -- Two LHC Money
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT SUM(m.amount) / 100.00 AS two_lhc_money
# MAGIC     FROM moneying_billing_party_transaction m
# MAGIC     WHERE p.relay_reference_number_two = m.relay_reference_number AND m.`voided?` = false AND m.charge_code = 'linehaul'
# MAGIC ) two_lhc_money ON true
# MAGIC
# MAGIC -- Two Fuel Money
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT SUM(m.amount) / 100.00 AS two_fuel_money
# MAGIC     FROM moneying_billing_party_transaction m
# MAGIC     WHERE p.relay_reference_number_two = m.relay_reference_number AND m.`voided?` = false AND m.charge_code = 'fuel_surcharge'
# MAGIC ) two_fuel_money ON true
# MAGIC
# MAGIC -- Two Money
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT SUM(m.amount) / 100.00 AS two_money
# MAGIC     FROM moneying_billing_party_transaction m
# MAGIC     WHERE p.relay_reference_number_two = m.relay_reference_number AND m.`voided?` = false
# MAGIC ) two_money ON true
# MAGIC
# MAGIC -- Carrier Money
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT SUM(v.amount) / 100.00 AS carrier_money
# MAGIC     FROM vendor_transaction_projection v
# MAGIC     WHERE c.relay_reference_number = v.relay_reference_number AND v.`voided?` = false
# MAGIC ) carrier_money ON true
# MAGIC
# MAGIC -- Carrier Linehaul
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT SUM(v.amount) / 100.00 AS carrier_linehaul
# MAGIC     FROM vendor_transaction_projection v
# MAGIC     WHERE c.relay_reference_number = v.relay_reference_number AND v.`voided?` = false AND v.charge_code = 'linehaul'
# MAGIC ) carrier_linehaul ON true
# MAGIC
# MAGIC -- Carrier Fuel Surcharge
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT SUM(v.amount) / 100.00 AS fuel_surcharge
# MAGIC     FROM vendor_transaction_projection v
# MAGIC     WHERE c.relay_reference_number = v.relay_reference_number AND v.`voided?` = false AND v.charge_code = 'fuel_surcharge'
# MAGIC ) fuel_surcharge ON true
# MAGIC
# MAGIC WHERE p.is_combined = true),
# MAGIC
# MAGIC
# MAGIC relay_customer_money as (SELECT DISTINCT 
# MAGIC   b.booking_id,
# MAGIC   b.relay_reference_number,
# MAGIC   b.status,
# MAGIC   CAST('TL' AS STRING) AS ltl_or_tl,
# MAGIC   COALESCE(SUM(
# MAGIC     CASE 
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC         CASE 
# MAGIC           WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC           ELSE cm.us_to_cad 
# MAGIC         END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END
# MAGIC   ), 0) / 100.00 AS total_cust_rate_wo_cred,
# MAGIC   COALESCE(SUM(
# MAGIC     CASE 
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC         CASE 
# MAGIC           WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC           ELSE cm.us_to_cad 
# MAGIC         END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END
# MAGIC     * CASE WHEN m.charge_code = 'linehaul' AND m.`voided?` = FALSE THEN 1 ELSE 0 END
# MAGIC   ), 0) / 100.00 AS customer_lhc,
# MAGIC   COALESCE(SUM(
# MAGIC     CASE 
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC         CASE 
# MAGIC           WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC           ELSE cm.us_to_cad 
# MAGIC         END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END
# MAGIC     * CASE WHEN m.charge_code = 'fuel_surcharge' AND m.`voided?` = FALSE THEN 1 ELSE 0 END
# MAGIC   ), 0) / 100.00 AS customer_fsc,
# MAGIC   COALESCE(SUM(
# MAGIC     CASE 
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC         CASE 
# MAGIC           WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC           ELSE cm.us_to_cad 
# MAGIC         END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END
# MAGIC     * CASE WHEN m.charge_code NOT IN ('linehaul', 'fuel_surcharge') AND m.`voided?` = FALSE THEN 1 ELSE 0 END
# MAGIC   ), 0) / 100.00 AS customer_acc,
# MAGIC   COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
# MAGIC   COALESCE(SUM(
# MAGIC     CASE 
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC         CASE 
# MAGIC           WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC           ELSE cm.us_to_cad 
# MAGIC         END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END
# MAGIC   ), 0) / 100.00 - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
# MAGIC   SUM(CASE WHEN m.charge_code = 'stop_off' THEN m.amount ELSE 0 END) AS stop_off_acc,
# MAGIC   SUM(CASE WHEN m.charge_code IN ('detention', 'detention_at_origin', 'detention_at_destination') THEN m.amount ELSE 0 END) AS det_acc
# MAGIC FROM
# MAGIC   booking_projection b
# MAGIC LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
# MAGIC LEFT JOIN canada_conversions cm ON CAST(m.incurred_at AS DATE) = cm.ship_date
# MAGIC JOIN average_conversions ON TRUE
# MAGIC LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC WHERE
# MAGIC   b.status <> 'cancelled'
# MAGIC   AND (canonical_plan_projection.mode <> 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC   AND (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC   AND b.booking_id <> 7370325
# MAGIC   AND m.`voided?` = FALSE
# MAGIC GROUP BY
# MAGIC   b.booking_id, b.relay_reference_number, b.status, invoicing_cred.invoicing_cred
# MAGIC UNION 
# MAGIC SELECT
# MAGIC     DISTINCT b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.dispatch_status AS status,
# MAGIC     CAST('LTL' AS STRING) AS ltl_or_tl,
# MAGIC
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE 
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC                     ELSE cm.us_to_cad 
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END
# MAGIC     ), 0) / 100.00 AS total_cust_rate_wo_cred,
# MAGIC
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE 
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC                     ELSE cm.us_to_cad 
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END * CASE WHEN m.charge_code = 'linehaul' AND m.`voided?` = FALSE THEN 1 ELSE 0 END
# MAGIC     ), 0) / 100.00 AS customer_lhc,
# MAGIC
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE 
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC                     ELSE cm.us_to_cad 
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END * CASE WHEN m.charge_code = 'fuel_surcharge' AND m.`voided?` = FALSE THEN 1 ELSE 0 END
# MAGIC     ), 0) / 100.00 AS customer_fsc,
# MAGIC
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE 
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC                     ELSE cm.us_to_cad 
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END * CASE WHEN m.charge_code NOT IN ('linehaul', 'fuel_surcharge') AND m.`voided?` = FALSE THEN 1 ELSE 0 END
# MAGIC     ), 0) / 100.00 AS customer_acc,
# MAGIC
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
# MAGIC
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE 
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL 
# MAGIC                     ELSE cm.us_to_cad 
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END
# MAGIC     ), 0) / 100.00 - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
# MAGIC
# MAGIC     SUM(CASE WHEN m.charge_code = 'stop_off' THEN m.amount ELSE 0 END) AS stop_off_acc,
# MAGIC
# MAGIC     SUM(CASE WHEN m.charge_code IN ('detention', 'detention_at_origin', 'detention_at_destination') THEN m.amount ELSE 0 END) AS det_acc
# MAGIC
# MAGIC FROM big_export_projection b
# MAGIC LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT JOIN moneying_billing_party_transaction m ON b.load_number = m.relay_reference_number
# MAGIC LEFT JOIN canada_conversions cm ON CAST(m.incurred_at AS DATE) = cm.ship_date
# MAGIC JOIN average_conversions ON TRUE
# MAGIC LEFT JOIN invoicing_cred ON b.load_number = invoicing_cred.relay_reference_number
# MAGIC
# MAGIC WHERE b.dispatch_status <> 'Cancelled'
# MAGIC   AND (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC   AND (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC   AND b.customer_id NOT IN ('hain', 'deb', 'roland')
# MAGIC   AND m.`voided?` = FALSE
# MAGIC
# MAGIC GROUP BY b.load_number, b.dispatch_status, invoicing_cred.invoicing_cred
# MAGIC union
# MAGIC SELECT DISTINCT 
# MAGIC     e.load_number AS booking_id,
# MAGIC     e.load_number AS relay_reference_number,
# MAGIC     e.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC     (e.carrier_accessorial_expense + CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) + CEIL(CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) *
# MAGIC         CASE 
# MAGIC             WHEN e.carrier_linehaul_expense = 0 THEN 0
# MAGIC             ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
# MAGIC         END)) / 100 AS total_cust_rate_wo_cred,
# MAGIC     
# MAGIC     CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) / 100 AS customer_lhc,
# MAGIC
# MAGIC     CEIL(CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) *
# MAGIC         CASE 
# MAGIC             WHEN e.carrier_linehaul_expense = 0 THEN 0
# MAGIC             ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
# MAGIC         END) / 100 AS customer_fsc,
# MAGIC
# MAGIC     e.carrier_accessorial_expense / 100 AS customer_acc,
# MAGIC     0 AS invoicing_cred,
# MAGIC     
# MAGIC     (e.carrier_accessorial_expense + CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) + CEIL(CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) *
# MAGIC         CASE 
# MAGIC             WHEN e.carrier_linehaul_expense = 0 THEN 0
# MAGIC             ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
# MAGIC         END)) / 100 AS final_customer_rate,
# MAGIC
# MAGIC     NULL AS stop_off_acc,
# MAGIC     NULL AS det_acc
# MAGIC
# MAGIC FROM big_export_projection e
# MAGIC LEFT JOIN canonical_plan_projection 
# MAGIC     ON e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC
# MAGIC WHERE 
# MAGIC     e.dispatch_status <> 'Cancelled'
# MAGIC     AND (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC     AND (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC     AND (e.customer_id IN ('hain', 'deb', 'roland'))
# MAGIC union
# MAGIC SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) AS total_cust_rate_wo_cred,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.00, 0) AS customer_lhc,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.00, 0) AS customer_fsc,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND (m.charge_code NOT IN ('linehaul', 'fuel_surcharge'))) / 100.00, 0) AS customer_acc,
# MAGIC     
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
# MAGIC     
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code = 'stop_off') AS stop_off_acc,
# MAGIC     
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code IN ('detention', 'detention_at_origin', 'detention_at_destination')) AS det_acc
# MAGIC
# MAGIC FROM booking_projection b
# MAGIC JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_one
# MAGIC LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
# MAGIC LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
# MAGIC JOIN average_conversions ON TRUE
# MAGIC LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC
# MAGIC WHERE m.`voided?` = false
# MAGIC
# MAGIC GROUP BY b.booking_id, b.relay_reference_number, b.status, invoicing_cred.invoicing_cred
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) AS total_cust_rate_wo_cred,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.00, 0) AS customer_lhc,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.00, 0) AS customer_fsc,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND (m.charge_code NOT IN ('linehaul', 'fuel_surcharge'))) / 100.00, 0) AS customer_acc,
# MAGIC     
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE cm.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
# MAGIC     
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code = 'stop_off') AS stop_off_acc,
# MAGIC     
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code IN ('detention', 'detention_at_origin', 'detention_at_destination')) AS det_acc
# MAGIC
# MAGIC FROM booking_projection b
# MAGIC JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_two
# MAGIC LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
# MAGIC LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
# MAGIC JOIN average_conversions ON TRUE
# MAGIC LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC
# MAGIC WHERE m.`voided?` = false
# MAGIC
# MAGIC GROUP BY b.booking_id, b.relay_reference_number, b.status, invoicing_cred.invoicing_cred),
# MAGIC
# MAGIC
# MAGIC relay_carrier_money as (
# MAGIC SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency = 'CAD' THEN vendor_transaction_projection.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN canada_conversions.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE canada_conversions.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE vendor_transaction_projection.amount
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false) / 100.00, 0) AS total_carrier_rate,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency = 'CAD' THEN vendor_transaction_projection.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN canada_conversions.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE canada_conversions.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE vendor_transaction_projection.amount
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code = 'linehaul') / 100.00, 0) AS lhc_carrier_rate,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency = 'CAD' THEN vendor_transaction_projection.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN canada_conversions.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE canada_conversions.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE vendor_transaction_projection.amount
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code = 'fuel_surcharge') / 100.00, 0) AS fsc_carrier_rate,
# MAGIC     
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency = 'CAD' THEN vendor_transaction_projection.amount * COALESCE(
# MAGIC                 CASE
# MAGIC                     WHEN canada_conversions.us_to_cad = 0 THEN NULL
# MAGIC                     ELSE canada_conversions.us_to_cad
# MAGIC                 END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE vendor_transaction_projection.amount
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false AND (vendor_transaction_projection.charge_code NOT IN ('linehaul', 'fuel_surcharge'))) / 100.00, 0) AS acc_carrier_rate
# MAGIC
# MAGIC FROM booking_projection b
# MAGIC LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT JOIN vendor_transaction_projection ON b.booking_id = vendor_transaction_projection.booking_id
# MAGIC LEFT JOIN canada_conversions ON DATE(vendor_transaction_projection.incurred_at) = canada_conversions.ship_date
# MAGIC JOIN average_conversions ON TRUE
# MAGIC
# MAGIC WHERE b.status <> 'cancelled' 
# MAGIC AND (canonical_plan_projection.mode <> 'ltl' OR canonical_plan_projection.mode IS NULL) 
# MAGIC AND (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC
# MAGIC GROUP BY b.booking_id, b.relay_reference_number, b.status
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC     b.projected_expense / 100.00 AS total_carrier_rate,
# MAGIC     b.carrier_linehaul_expense / 100.00 AS lhc_carrier_rate,
# MAGIC     b.carrier_fuel_expense / 100.00 AS fsc_carrier_rate,
# MAGIC     b.carrier_accessorial_expense / 100.00 AS acc_carrier_rate
# MAGIC
# MAGIC FROM big_export_projection b
# MAGIC LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC
# MAGIC WHERE b.dispatch_status <> 'Cancelled' 
# MAGIC AND (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL) 
# MAGIC AND (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC ),
# MAGIC  system_union AS (
# MAGIC     SELECT
# MAGIC         spot_quotes.quote_id,
# MAGIC         'Relay1' AS TMS,
# MAGIC         b.relay_reference_number AS loadnum,
# MAGIC         spot_quotes.customer_name AS customer,
# MAGIC         spot_quotes.quote_source_str,
# MAGIC         spot_quotes.status_str AS status,
# MAGIC         from_utc_timestamp(t.accepted_at, 'EST') AS accepted_at,
# MAGIC         b.booked_at,
# MAGIC         (relay_customer_money.final_customer_rate - relay_carrier_money.total_carrier_rate) AS margin,
# MAGIC         relay_customer_money.final_customer_rate AS revenue,
# MAGIC         relay_carrier_money.total_carrier_rate AS cost
# MAGIC     FROM
# MAGIC         tendering_acceptance t
# MAGIC     JOIN spot_quotes ON spot_quotes.load_number = t.relay_reference_number
# MAGIC     JOIN booking_projection b ON t.relay_reference_number = b.relay_reference_number
# MAGIC     JOIN analytics.Dim_financial_calendar ON analytics.dim_financial_calendar.date = CAST(b.booked_at AS DATE)
# MAGIC     JOIN relay_customer_money ON relay_customer_money.booking_id = b.booking_id
# MAGIC     JOIN relay_carrier_money ON relay_carrier_money.booking_id = b.booking_id
# MAGIC     LEFT JOIN new_office_lookup ON spot_quotes.profit_center = old_office
# MAGIC     LEFT JOIN aljex_user_report_listing ON b.booked_by_name = aljex_user_report_listing.full_name
# MAGIC     WHERE 
# MAGIC         b.booked_at > '2023-01-01'
# MAGIC         AND b.booked_at <= CURRENT_DATE 
# MAGIC         AND status_str = 'WON' 
# MAGIC         AND b.status = 'booked'
# MAGIC         
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT DISTINCT
# MAGIC         spot_quotes.quote_id,
# MAGIC         'Relay2' AS TMS,
# MAGIC         b.relay_reference_number,
# MAGIC         spot_quotes.customer_name,
# MAGIC         spot_quotes.quote_source_str,
# MAGIC         spot_quotes.status_str AS status,
# MAGIC         from_utc_timestamp(t.accepted_at, 'EST') AS accepted_at,
# MAGIC         b.booked_at,
# MAGIC         (relay_customer_money.final_customer_rate - relay_carrier_money.total_carrier_rate) AS margin,
# MAGIC         relay_customer_money.final_customer_rate AS revenue,
# MAGIC         relay_carrier_money.total_carrier_rate AS cost
# MAGIC     FROM
# MAGIC         tendering_acceptance t
# MAGIC     JOIN spot_quotes ON t.shipment_id = spot_quotes.shipper_ref_number
# MAGIC     JOIN booking_projection b ON t.relay_reference_number = b.relay_reference_number AND b.status = 'booked'
# MAGIC     JOIN Analytics.Dim_financial_calendar ON analytics.dim_financial_calendar.date = CAST(b.booked_at AS DATE)
# MAGIC     JOIN relay_customer_money ON relay_customer_money.booking_id = b.booking_id
# MAGIC     JOIN relay_carrier_money ON relay_carrier_money.booking_id = b.booking_id
# MAGIC      JOIN new_office_lookup ON spot_quotes.profit_center = old_office
# MAGIC     LEFT JOIN aljex_user_report_listing ON b.booked_by_name = aljex_user_report_listing.full_name
# MAGIC     WHERE 
# MAGIC         spot_quotes.load_number IS NULL
# MAGIC         AND b.booked_at > '2023-01-01'
# MAGIC         AND b.booked_at <= CURRENT_DATE 
# MAGIC         AND status_str = 'WON' 
# MAGIC        
# MAGIC         
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         spot_quotes.quote_id,
# MAGIC         'Aljex',
# MAGIC         p.id AS loadnum,
# MAGIC         spot_quotes.customer_name,
# MAGIC         spot_quotes.quote_source_str,
# MAGIC         spot_quotes.status_str AS status,
# MAGIC         p.tag_creation_date AS accepted_at,
# MAGIC         p.key_c_date AS booked_at,
# MAGIC         (aljex_data.invoice_total - aljex_data.final_carrier_rate) AS margin,
# MAGIC         aljex_data.invoice_total AS revenue,
# MAGIC         aljex_data.final_carrier_rate AS cost
# MAGIC     FROM 
# MAGIC         projection_load p
# MAGIC     JOIN spot_quotes ON p.ref_num = spot_quotes.shipper_ref_number
# MAGIC     JOIN aljex_data ON p.id = aljex_data.id
# MAGIC     LEFT JOIN analytics.dim_financial_calendar ON analytics.dim_financial_calendar.date = CAST(p.key_c_date AS DATE)
# MAGIC     LEFT JOIN projection_carrier c ON p.carrier_id = c.id
# MAGIC     LEFT JOIN new_office_lookup ON spot_quotes.profit_center = old_office
# MAGIC     LEFT JOIN aljex_user_report_listing ON p.key_c_user = aljex_user_report_listing.aljex_id
# MAGIC     WHERE 
# MAGIC         CAST(p.key_c_date AS DATE) BETWEEN '2023-01-01' AND CURRENT_DATE 
# MAGIC         AND status_str = 'WON'
# MAGIC         AND p.status NOT ILIKE '%void%'
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT
# MAGIC         spot_quotes.quote_id,
# MAGIC         'Aljex',
# MAGIC         p.id AS loadnum,
# MAGIC         spot_quotes.customer_name,
# MAGIC         spot_quotes.quote_source_str,
# MAGIC         spot_quotes.status_str AS status,
# MAGIC         p.tag_creation_date AS accepted_at,
# MAGIC         p.key_c_date AS booked_at,
# MAGIC         (aljex_data.invoice_total - aljex_data.final_carrier_rate) AS margin,
# MAGIC         aljex_data.invoice_total AS revenue,
# MAGIC         aljex_data.final_carrier_rate AS cost
# MAGIC     FROM 
# MAGIC         projection_load p
# MAGIC     JOIN spot_quotes ON p.id = spot_quotes.load_number
# MAGIC     JOIN aljex_data ON p.id = aljex_data.id
# MAGIC     LEFT JOIN analytics.dim_financial_calendar ON analytics.dim_financial_calendar.date = CAST(p.key_c_date AS DATE)
# MAGIC     LEFT JOIN projection_carrier c ON p.carrier_id = c.id
# MAGIC     LEFT JOIN new_office_lookup ON spot_quotes.profit_center = old_office
# MAGIC     LEFT JOIN aljex_user_report_listing ON p.key_c_user = aljex_user_report_listing.aljex_id
# MAGIC     WHERE 
# MAGIC         CAST(p.key_c_date AS DATE) BETWEEN '2023-01-01' AND CURRENT_DATE 
# MAGIC         AND status_str = 'WON'
# MAGIC         AND p.status NOT ILIKE '%void%'
# MAGIC ),
# MAGIC
# MAGIC summary_data AS (
# MAGIC     SELECT DISTINCT
# MAGIC     quote_id,
# MAGIC         loadnum,
# MAGIC         customer,
# MAGIC         margin,
# MAGIC         revenue,
# MAGIC         cost,
# MAGIC         CAST(booked_at AS DATE) AS booked_at
# MAGIC     FROM system_union
# MAGIC ),
# MAGIC
# MAGIC loads_quoted AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         customer_name,
# MAGIC         COUNT(DISTINCT quote_id) AS loads_quoted,
# MAGIC         COUNT(DISTINCT quote_id) FILTER (WHERE status_str = 'WON') AS won_quotes
# MAGIC     FROM 
# MAGIC         spot_quotes
# MAGIC     LEFT JOIN new_office_lookup ON spot_quotes.profit_center = old_office
# MAGIC     JOIN analytics.dim_financial_calendar ON analytics.dim_financial_calendar.date = COALESCE(responded_at, created_at)
# MAGIC     WHERE 
# MAGIC        COALESCE(responded_at, created_at) > '2023-01-01'
# MAGIC
# MAGIC     GROUP BY customer_name 
# MAGIC ),
# MAGIC
# MAGIC for_total_quoted AS (
# MAGIC     SELECT DISTINCT 
# MAGIC        SUM(loads_quoted) AS for_total_quoted,
# MAGIC        SUM(won_quotes) AS total_won_quotes
# MAGIC     FROM loads_quoted 
# MAGIC )
# MAGIC
# MAGIC -- SELECT count(*),loadnum
# MAGIC -- FROM summary_data group by loadnum having count(*)>1
# MAGIC -- group by loadnum having count(*)>1
# MAGIC select * from summary_data --where booked_at between '2025-07-27' and '2025-08-23'
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creation Of AM Dates Table

# COMMAND ----------

# MAGIC %sql
# MAGIC set ansi_mode = false;
# MAGIC use schema bronze;
# MAGIC create or replace table analytics.Am_dates as
# MAGIC WITH to_get_avg AS (
# MAGIC   SELECT DISTINCT
# MAGIC     ship_date,
# MAGIC     conversion AS us_to_cad,
# MAGIC     us_to_cad AS cad_to_us
# MAGIC   FROM
# MAGIC     brokerageprod.bronze.canada_conversions
# MAGIC   ORDER BY
# MAGIC     ship_date DESC
# MAGIC   LIMIT 7
# MAGIC ),
# MAGIC average_conversions AS (
# MAGIC   SELECT
# MAGIC     AVG(us_to_cad) AS avg_us_to_cad,
# MAGIC     AVG(cad_to_us) AS avg_cad_to_us
# MAGIC   FROM
# MAGIC     to_get_avg
# MAGIC ),
# MAGIC invoicing_cred AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     invoicing_credits.relay_reference_number,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN invoicing_credits.currency = 'CAD' THEN invoicing_credits.total_amount * COALESCE(
# MAGIC           CASE 
# MAGIC             WHEN canada_conversions.us_to_cad = 0 THEN NULL 
# MAGIC             ELSE canada_conversions.us_to_cad 
# MAGIC           END, average_conversions.avg_cad_to_us)
# MAGIC         ELSE invoicing_credits.total_amount
# MAGIC       END
# MAGIC     ) / 100.00 AS invoicing_cred
# MAGIC   FROM
# MAGIC     invoicing_credits
# MAGIC   LEFT JOIN canada_conversions 
# MAGIC     ON CAST(invoicing_credits.credited_at AS DATE) = canada_conversions.ship_date
# MAGIC   JOIN average_conversions ON TRUE
# MAGIC   GROUP BY invoicing_credits.relay_reference_number
# MAGIC ),
# MAGIC projection_load as (Select p1.*,  p2.ps_arrive_code_1 ,
# MAGIC         p2.ps_arrive_code_2          ,
# MAGIC         p2.ps_arrive_code_3          ,
# MAGIC         p2.ps_arrive_code_4          ,
# MAGIC         p2.ps_arrive_code_5          ,
# MAGIC         p2.ps_arrive_code_6          ,
# MAGIC         p2.ps_arrive_code_7          ,
# MAGIC         p2.ps_arrive_code_8          ,
# MAGIC         p2.ps_arrive_code_9          ,
# MAGIC         p2.ps_arrive_code_10         ,
# MAGIC         p2.ps_arrive_code_11         ,
# MAGIC         p2.ps_arrive_code_12         ,
# MAGIC         p2.ps_arrive_code_16         ,
# MAGIC         p2.ps_arrive_date_1          ,
# MAGIC         p2.ps_arrive_date_2          ,
# MAGIC         p2.ps_arrive_date_3          ,
# MAGIC         p2.ps_arrive_date_4          ,
# MAGIC         p2.ps_arrive_date_5          ,
# MAGIC         p2.ps_arrive_date_6          ,
# MAGIC         p2.ps_arrive_date_7          ,
# MAGIC         p2.ps_arrive_date_8          ,
# MAGIC         p2.ps_arrive_date_9          ,
# MAGIC         p2.ps_arrive_date_10         ,
# MAGIC         p2.ps_arrive_date_11         ,
# MAGIC         p2.ps_arrive_date_12         ,
# MAGIC         p2.ps_arrive_time_1          ,
# MAGIC         p2.ps_arrive_time_2          ,
# MAGIC         p2.ps_arrive_time_3          ,
# MAGIC         p2.ps_arrive_time_4          ,
# MAGIC         p2.ps_arrive_time_5          ,
# MAGIC         p2.ps_arrive_time_6          ,
# MAGIC         p2.ps_arrive_time_7          ,
# MAGIC         p2.ps_arrive_time_8          ,
# MAGIC         p2.ps_arrive_time_9          ,
# MAGIC         p2.ps_arrive_time_10         ,
# MAGIC         p2.ps_arrive_time_11         ,
# MAGIC         p2.ps_arrive_time_12         ,
# MAGIC         p2.ps_arrive_time_20         ,
# MAGIC         p2.ps_city_1                 ,
# MAGIC         p2.ps_city_2                 ,
# MAGIC         p2.ps_city_3                 ,
# MAGIC         p2.ps_city_4                 ,
# MAGIC         p2.ps_city_5                 ,
# MAGIC         p2.ps_city_6                 ,
# MAGIC         p2.ps_city_7                 ,
# MAGIC         p2.ps_city_8                 ,
# MAGIC         p2.ps_city_9                 ,
# MAGIC         p2.ps_city_10                ,
# MAGIC         p2.ps_city_11                ,
# MAGIC         p2.ps_city_12                ,
# MAGIC         p2.ps_city_13                ,
# MAGIC         p2.ps_city_14                ,
# MAGIC         p2.ps_city_15                ,
# MAGIC         p2.ps_city_16                ,
# MAGIC         p2.ps_city_17                ,
# MAGIC         p2.ps_city_18                ,
# MAGIC         p2.ps_city_19                ,
# MAGIC         p2.ps_company_1              ,
# MAGIC         p2.ps_company_2              ,
# MAGIC         p2.ps_company_3              ,
# MAGIC         p2.ps_company_4              ,
# MAGIC         p2.ps_company_5              ,
# MAGIC         p2.ps_company_6              ,
# MAGIC         p2.ps_company_7              ,
# MAGIC         p2.ps_company_8              ,
# MAGIC         p2.ps_company_9              ,
# MAGIC         p2.ps_company_10             ,
# MAGIC         p2.ps_company_11             ,
# MAGIC         p2.ps_company_12             ,
# MAGIC         p2.ps_company_13             ,
# MAGIC         p2.ps_company_14             ,
# MAGIC         p2.ps_company_15             ,
# MAGIC         p2.ps_company_16             ,
# MAGIC         p2.ps_company_17             ,
# MAGIC         p2.ps_company_18             ,
# MAGIC         p2.ps_company_19             ,
# MAGIC         p2.ps_depart_code_1          ,
# MAGIC         p2.ps_depart_code_2          ,
# MAGIC         p2.ps_depart_code_3          ,
# MAGIC         p2.ps_depart_code_4          ,
# MAGIC         p2.ps_depart_code_5          ,
# MAGIC         p2.ps_depart_code_6          ,
# MAGIC         p2.ps_depart_code_7          ,
# MAGIC         p2.ps_depart_code_8          ,
# MAGIC         p2.ps_depart_code_9          ,
# MAGIC         p2.ps_depart_code_10         ,
# MAGIC         p2.ps_depart_code_11         ,
# MAGIC         p2.ps_depart_code_12         ,
# MAGIC         p2.ps_depart_code_14         ,
# MAGIC         p2.ps_depart_date_1          ,
# MAGIC         p2.ps_depart_date_2          ,
# MAGIC         p2.ps_depart_date_3          ,
# MAGIC         p2.ps_depart_date_4          ,
# MAGIC         p2.ps_depart_date_5          ,
# MAGIC         p2.ps_depart_date_6          ,
# MAGIC         p2.ps_depart_date_7          ,
# MAGIC         p2.ps_depart_date_8          ,
# MAGIC         p2.ps_depart_date_9          ,
# MAGIC         p2.ps_depart_date_10         ,
# MAGIC         p2.ps_depart_date_11         ,
# MAGIC         p2.ps_depart_date_12         ,
# MAGIC         p2.ps_depart_time_1          ,
# MAGIC         p2.ps_depart_time_2          ,
# MAGIC         p2.ps_depart_time_3          ,
# MAGIC         p2.ps_depart_time_4          ,
# MAGIC         p2.ps_depart_time_5          ,
# MAGIC         p2.ps_depart_time_6          ,
# MAGIC         p2.ps_depart_time_7          ,
# MAGIC         p2.ps_depart_time_8          ,
# MAGIC         p2.ps_depart_time_9          ,
# MAGIC         p2.ps_depart_time_10         ,
# MAGIC         p2.ps_depart_time_11         ,
# MAGIC         p2.ps_depart_time_12         ,
# MAGIC         p2.ps_ref_1                  ,
# MAGIC         p2.ps_ref_2                  ,
# MAGIC         p2.ps_ref_3                  ,
# MAGIC         p2.ps_ref_4                  ,
# MAGIC         p2.ps_ref_5                  ,
# MAGIC         p2.ps_ref_6                  ,
# MAGIC         p2.ps_ref_7                  ,
# MAGIC         p2.ps_ref_8                  ,
# MAGIC         p2.ps_ref_9                  ,
# MAGIC         p2.ps_ref_10                 ,
# MAGIC         p2.ps_ref_11                 ,
# MAGIC         p2.ps_ref_12                 ,
# MAGIC         p2.ps_ref_13                 ,
# MAGIC         p2.ps_ref_14                 ,
# MAGIC         p2.ps_ref_15                 ,
# MAGIC         p2.ps_ref_16                 ,
# MAGIC         p2.ps_ref_17                 ,
# MAGIC         p2.ps_ref_18                 ,
# MAGIC         p2.ps_ref_19                 ,
# MAGIC         p2.ps_state_1                ,
# MAGIC         p2.ps_state_2                ,
# MAGIC         p2.ps_state_3                ,
# MAGIC         p2.ps_state_4                ,
# MAGIC         p2.ps_state_5                ,
# MAGIC         p2.ps_state_6                ,
# MAGIC         p2.ps_state_7                ,
# MAGIC         p2.ps_state_8                ,
# MAGIC         p2.ps_state_9                ,
# MAGIC         p2.ps_state_10               ,
# MAGIC         p2.ps_state_11               ,
# MAGIC         p2.ps_state_12               ,
# MAGIC         p2.ps_state_13               ,
# MAGIC         p2.ps_state_14               ,
# MAGIC         p2.ps_state_15               ,
# MAGIC         p2.ps_state_16               ,
# MAGIC         p2.ps_state_17               ,
# MAGIC         p2.ps_state_18               ,
# MAGIC         p2.ps_state_19               ,
# MAGIC         p2.ps_zip_1                  ,
# MAGIC         p2.ps_zip_2                  ,
# MAGIC         p2.ps_zip_3                  ,
# MAGIC         p2.ps_zip_4                  ,
# MAGIC         p2.ps_zip_5                  ,
# MAGIC         p2.ps_zip_6                  ,
# MAGIC         p2.ps_zip_7                  ,
# MAGIC         p2.ps_zip_8                  ,
# MAGIC         p2.ps_zip_9                  ,
# MAGIC         p2.ps_zip_10                 ,
# MAGIC         p2.ps_zip_11                 ,
# MAGIC         p2.ps_zip_12                 ,
# MAGIC         p2.ps_zip_13                 ,
# MAGIC         p2.ps_zip_14                 ,
# MAGIC         p2.ps_zip_15                 ,
# MAGIC         p2.ps_zip_16                 ,
# MAGIC         p2.ps_zip_17                 ,
# MAGIC         p2.ps_zip_18                 ,
# MAGIC         p2.ps_zip_19                 ,
# MAGIC         p2.Ps1                       ,
# MAGIC         p2.Ps2                       ,
# MAGIC         p2.Ps3                       ,
# MAGIC         p2.Ps4                       ,
# MAGIC         p2.Ps5                       ,
# MAGIC         p2.Ps6                       ,
# MAGIC         p2.Ps7                       ,
# MAGIC         p2.Ps8                       ,
# MAGIC         p2.Ps9                       ,
# MAGIC         p2.Ps10                      ,
# MAGIC         p2.Ps11                      ,
# MAGIC         p2.Ps12                      ,
# MAGIC         p2.Ps13                      ,
# MAGIC         p2.Ps14                      ,
# MAGIC         p2.Ps15                      ,
# MAGIC         p2.Ps16                      ,
# MAGIC         p2.Ps17                      ,
# MAGIC         p2.Ps18                      ,
# MAGIC         p2.Ps19                      ,
# MAGIC         p2.quote_number              ,
# MAGIC         p2.ref_num                   ,
# MAGIC         p2.release_date              ,
# MAGIC         p2.sales_rep                 ,
# MAGIC         p2.sales_team                ,
# MAGIC         p2.seal                      ,
# MAGIC         p2.shipper                   ,
# MAGIC         p2.signed_by                 ,
# MAGIC         p2.sls_pct                   ,
# MAGIC         p2.srv_rep                   ,
# MAGIC         p2.status                    ,
# MAGIC         p2.straps_chains             ,
# MAGIC         p2.tag_created_by            ,
# MAGIC         p2.tag_creation_date         ,
# MAGIC         p2.tag_creation_time         ,
# MAGIC         p2.tariff_num                ,
# MAGIC         p2.tarp_required             ,
# MAGIC         p2.tarp_size                 ,
# MAGIC         p2.temp                      ,
# MAGIC         p2.temp_ctrl                 ,
# MAGIC         p2.time                      ,
# MAGIC         p2.Tord                      ,
# MAGIC         p2.trailer_number            ,
# MAGIC         p2.truck_num                 ,
# MAGIC         p2.value                     ,
# MAGIC         p2.web_post                  ,
# MAGIC         p2.web_sync_action           ,
# MAGIC         p2.web_sync_table_name       ,
# MAGIC         p2.weight                    ,
# MAGIC         p2.will_pickup_date          ,
# MAGIC         p2.will_pickup_time          ,
# MAGIC         p2.asg_disp                  ,
# MAGIC         p2.bl_ref                    ,
# MAGIC         p2.carrier_accessorial_rate1 ,
# MAGIC         p2.carrier_accessorial_rate2 ,
# MAGIC         p2.carrier_accessorial_rate3 ,
# MAGIC         p2.carrier_accessorial_rate4 ,
# MAGIC         p2.carrier_accessorial_rate5 ,
# MAGIC         p2.carrier_accessorial_rate6 ,
# MAGIC         p2.carrier_accessorial_rate7 ,
# MAGIC         p2.carrier_accessorial_rate8 ,
# MAGIC         p2.customer_accessorial_rate1,
# MAGIC         p2.customer_accessorial_rate2,
# MAGIC         p2.customer_accessorial_rate3,
# MAGIC         p2.customer_accessorial_rate4,
# MAGIC         p2.customer_accessorial_rate5,
# MAGIC         p2.customer_accessorial_rate6,
# MAGIC         p2.customer_accessorial_rate7,
# MAGIC         p2.customer_accessorial_rate8,
# MAGIC         p2.customer_id               ,
# MAGIC         p2.nmfc_num  from bronze.projection_load_1 as p1 LEFT JOIN bronze.projection_load_2 as p2 ON p1.id = p2.id),
# MAGIC aljex_master_dates as ( 
# MAGIC SELECT DISTINCT 
# MAGIC   CAST(p.id AS DECIMAL) AS id,
# MAGIC   p.status,
# MAGIC   p.equipment,
# MAGIC
# MAGIC   -- use_date
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN COALESCE(p.arrive_pickup_time, p.loaded_time, p.pickup_time) IS NULL THEN TO_TIMESTAMP(COALESCE(p.arrive_pickup_date, p.loaded_date, p.pickup_date))
# MAGIC         ELSE TO_TIMESTAMP(COALESCE(p.arrive_pickup_date, p.loaded_date, p.pickup_date)) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(COALESCE(p.arrive_pickup_time, p.loaded_time, p.pickup_time), 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(COALESCE(p.arrive_pickup_time, p.loaded_time, p.pickup_time), 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS use_date,
# MAGIC
# MAGIC   -- end_date
# MAGIC   CASE
# MAGIC     WHEN DAYOFWEEK(TO_DATE(COALESCE(p.arrive_pickup_date, p.loaded_date, p.pickup_date))) = 1 THEN DATE_ADD(TO_DATE(COALESCE(p.arrive_pickup_date, p.loaded_date, p.pickup_date)), 6)
# MAGIC     ELSE DATE_ADD(DATE_TRUNC('WEEK', TO_DATE(COALESCE(p.arrive_pickup_date, p.loaded_date, p.pickup_date))), 5)
# MAGIC   END AS end_date,
# MAGIC
# MAGIC   -- booked_end_date
# MAGIC   CASE
# MAGIC     WHEN DAYOFWEEK(TO_DATE(p.key_c_date)) = 1 THEN DATE_ADD(TO_DATE(p.key_c_date), 6)
# MAGIC     ELSE DATE_ADD(DATE_TRUNC('WEEK', TO_DATE(p.key_c_date)), 5)
# MAGIC   END AS booked_end_date,
# MAGIC
# MAGIC   -- pu_appt_date
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN p.pickup_appt_time IS NULL THEN TO_TIMESTAMP(p.pickup_appt_date)
# MAGIC         ELSE TO_TIMESTAMP(p.pickup_appt_date) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.pickup_appt_time, 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.pickup_appt_time, 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS pu_appt_date,
# MAGIC
# MAGIC   -- pu_in
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN p.arrive_pickup_time IS NULL THEN TO_TIMESTAMP(p.arrive_pickup_date)
# MAGIC         ELSE TO_TIMESTAMP(p.arrive_pickup_date) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.arrive_pickup_time, 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.arrive_pickup_time, 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS pu_in,
# MAGIC
# MAGIC   -- pu_out
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN p.loaded_time IS NULL THEN TO_TIMESTAMP(p.loaded_date)
# MAGIC         ELSE TO_TIMESTAMP(p.loaded_date) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.loaded_time, 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.loaded_time, 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS pu_out,
# MAGIC
# MAGIC   -- ship_date
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN COALESCE(p.arrive_pickup_time, p.loaded_time) IS NULL THEN TO_TIMESTAMP(COALESCE(p.arrive_pickup_date, p.loaded_date))
# MAGIC         ELSE TO_TIMESTAMP(COALESCE(p.arrive_pickup_date, p.loaded_date)) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(COALESCE(p.arrive_pickup_time, p.loaded_time), 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(COALESCE(p.arrive_pickup_time, p.loaded_time), 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS ship_date,
# MAGIC
# MAGIC   -- del_appt_date
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN p.consignee_appointment_time IS NULL THEN TRY_CAST(p.consignee_appointment_date AS TIMESTAMP)
# MAGIC         ELSE TO_TIMESTAMP(p.consignee_appointment_date) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.consignee_appointment_time, 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.consignee_appointment_time, 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS del_appt_date,
# MAGIC
# MAGIC   -- del_in
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN p.arrive_consignee_time IS NULL THEN TRY_CAST(p.arrive_consignee_date AS TIMESTAMP)
# MAGIC         ELSE TO_TIMESTAMP(p.arrive_consignee_date) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.arrive_consignee_time, 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.arrive_consignee_time, 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS del_in,
# MAGIC
# MAGIC   -- del_out
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN p.delivery_time IS NULL THEN TO_TIMESTAMP(p.delivery_date)
# MAGIC         ELSE TO_TIMESTAMP(p.delivery_date) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.delivery_time, 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.delivery_time, 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS del_out,
# MAGIC
# MAGIC   -- delivered_date
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN COALESCE(p.arrive_consignee_time, p.delivery_time) IS NULL THEN TO_TIMESTAMP(COALESCE(p.arrive_consignee_date, p.delivery_date))
# MAGIC         ELSE TO_TIMESTAMP(COALESCE(p.arrive_consignee_date, p.delivery_date)) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(COALESCE(p.arrive_consignee_time, p.delivery_time), 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(COALESCE(p.arrive_consignee_time, p.delivery_time), 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS delivered_date,
# MAGIC
# MAGIC   -- booked_at
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN p.key_c_time IS NULL THEN TO_TIMESTAMP(p.key_c_date)
# MAGIC         ELSE TO_TIMESTAMP(p.key_c_date) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.key_c_time, 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.key_c_time, 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS booked_at,
# MAGIC
# MAGIC   -- created_date
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN p.tag_creation_time IS NULL THEN TO_TIMESTAMP(p.tag_creation_date)
# MAGIC         ELSE TO_TIMESTAMP(p.tag_creation_date) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.tag_creation_time, 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.tag_creation_time, 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS created_date,
# MAGIC
# MAGIC   -- pickup_date
# MAGIC   
# MAGIC     CASE
# MAGIC         WHEN p.pickup_time IS NULL THEN TO_TIMESTAMP(p.pickup_date)
# MAGIC         ELSE TO_TIMESTAMP(p.pickup_date) + 
# MAGIC     MAKE_INTERVAL(
# MAGIC         0, 0, 0, 0,
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.pickup_time, 1, 2) AS INT), 0),
# MAGIC         COALESCE(TRY_CAST(SUBSTRING(p.pickup_time, 4, 2) AS INT), 1),
# MAGIC         0
# MAGIC     )
# MAGIC     
# MAGIC     END
# MAGIC      AS pickup_date,
# MAGIC
# MAGIC   -- pickup_date_end_date
# MAGIC   CASE
# MAGIC     WHEN DAYOFWEEK(TO_DATE(p.pickup_date)) = 1 THEN DATE_ADD(TO_DATE(p.pickup_date), 6)
# MAGIC     ELSE DATE_ADD(DATE_TRUNC('WEEK', TO_DATE(p.pickup_date)), 5)
# MAGIC   END AS pickup_date_end_date,
# MAGIC
# MAGIC   -- must_del_date
# MAGIC   TO_DATE(p.must_del_date) AS must_del_date
# MAGIC
# MAGIC FROM projection_load p
# MAGIC WHERE p.status NOT LIKE '%VOID%'
# MAGIC ),
# MAGIC aljex_data as ( 
# MAGIC SELECT DISTINCT CAST(p.id AS INT) AS id,
# MAGIC     p.customer_id,
# MAGIC     p.ref_num AS shipment_id,
# MAGIC     p.status,
# MAGIC         CASE
# MAGIC             WHEN aljex_user_report_listing.full_name IS NULL THEN p.key_c_user
# MAGIC             ELSE aljex_user_report_listing.full_name
# MAGIC         END AS key_c_user,
# MAGIC     p.ps1,
# MAGIC         CASE
# MAGIC             WHEN dis.full_name IS NULL THEN p.key_h_user
# MAGIC             ELSE dis.full_name
# MAGIC         END AS key_h_user,
# MAGIC         CASE
# MAGIC             WHEN p.key_c_user = 'IMPORT' AND p.shipper = 'ALTMAN PLANTS' THEN 'LAX'
# MAGIC             WHEN p.key_c_user = 'IMPORT' AND p.equipment = 'LTL' AND p.office IN ('64', '63', '62', '61') THEN 'TOR'
# MAGIC             WHEN p.key_c_user = 'IMPORT' AND p.equipment = 'LTL' THEN 'LTL'
# MAGIC             WHEN p.key_c_user = 'IMPORT' THEN 'DRAY'
# MAGIC             WHEN p.key_c_user = 'EDILCR' THEN 'LTL'
# MAGIC             ELSE car.new_office
# MAGIC         END AS carr_office,
# MAGIC     p.shipper AS original_shipper,
# MAGIC         CASE
# MAGIC             WHEN customer_lookup.master_customer_name IS NULL THEN p.shipper
# MAGIC             ELSE customer_lookup.master_customer_name
# MAGIC         END AS mastername,
# MAGIC     CASE
# MAGIC       WHEN TRY_CAST(
# MAGIC         COALESCE(
# MAGIC           p.arrive_pickup_date,
# MAGIC           p.loaded_date,
# MAGIC           p.pickup_date
# MAGIC         ) AS DATE
# MAGIC       ) IS NOT NULL THEN TRY_CAST(
# MAGIC         COALESCE(
# MAGIC           p.arrive_pickup_date,
# MAGIC           p.loaded_date,
# MAGIC           p.pickup_date
# MAGIC         ) AS DATE
# MAGIC       )
# MAGIC       ELSE NULL
# MAGIC     END AS use_date,
# MAGIC     CASE
# MAGIC       WHEN DAYOFWEEK(
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           ) IS NOT NULL THEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           )
# MAGIC           ELSE NULL
# MAGIC         END
# MAGIC       ) = 1 THEN CASE
# MAGIC         WHEN TRY_CAST(
# MAGIC           COALESCE(
# MAGIC             p.arrive_pickup_date,
# MAGIC             p.loaded_date,
# MAGIC             p.pickup_date
# MAGIC           ) AS DATE
# MAGIC         ) IS NOT NULL THEN TRY_CAST(
# MAGIC           COALESCE(
# MAGIC             p.arrive_pickup_date,
# MAGIC             p.loaded_date,
# MAGIC             p.pickup_date
# MAGIC           ) AS DATE
# MAGIC         )
# MAGIC         ELSE NULL
# MAGIC       END + INTERVAL 6 DAYS
# MAGIC       ELSE DATE_TRUNC(
# MAGIC         'WEEK',
# MAGIC         CASE
# MAGIC           WHEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           ) IS NOT NULL THEN TRY_CAST(
# MAGIC             COALESCE(
# MAGIC               p.arrive_pickup_date,
# MAGIC               p.loaded_date,
# MAGIC               p.pickup_date
# MAGIC             ) AS DATE
# MAGIC           )
# MAGIC           ELSE NULL
# MAGIC         END
# MAGIC       ) + INTERVAL 5 DAYS
# MAGIC     END AS end_date,
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_line_haul AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate1 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate2 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate3 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate4 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate5 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate6 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate7 AS STRING), ''), 0.00) +
# MAGIC         COALESCE(NULLIF(CAST(p.carrier_accessorial_rate8 AS STRING), ''), 0.00) AS final_carrier_rate,
# MAGIC         CASE
# MAGIC             WHEN p.value RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.value AS FLOAT)
# MAGIC             ELSE 0.00
# MAGIC         END AS cargo_value,
# MAGIC         CASE
# MAGIC             WHEN p.office NOT IN ('10', '34', '51', '54', '61', '62', '63', '64', '74') THEN 'USD'
# MAGIC             ELSE COALESCE(cad_currency.currency_type, aljex_customer_profiles.cust_country, 'CAD')
# MAGIC         END AS cust_curr,
# MAGIC         CASE
# MAGIC             WHEN c.name RLIKE '%CAD' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE 'CANADIAN R%' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE '%CAN' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE '%(CAD)%' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE '%-C' THEN 'CAD'
# MAGIC             WHEN c.name RLIKE '%- C%' THEN 'CAD'
# MAGIC             WHEN canada_carriers.canada_dot IS NOT NULL THEN 'CAD'
# MAGIC             ELSE 'USD'
# MAGIC         END AS carrier_curr,
# MAGIC         COALESCE(canada_conversions.conversion, average_conversions.avg_us_to_cad) AS conversion_rate,
# MAGIC         COALESCE(canada_conversions.us_to_cad, average_conversions.avg_cad_to_us) AS conversion_rate_cad,
# MAGIC     new_office_lookup.new_office AS customer_office,
# MAGIC     p.office AS proj_load_office,
# MAGIC     p.equipment,
# MAGIC     p.mode AS proj_load_mode,
# MAGIC         CASE
# MAGIC             WHEN p.equipment LIKE '%LTL%' THEN 'LTL'
# MAGIC             ELSE 'TL'
# MAGIC         END AS modee,
# MAGIC     p.invoice_total,
# MAGIC     c.name AS carrier_name,
# MAGIC     bronze.financial_calendar.financial_period_sorting,
# MAGIC     c.dot_num AS dot_number,
# MAGIC     p.pickup_name,
# MAGIC     p.origin_city,
# MAGIC     p.origin_state,
# MAGIC     p.pickup_zip_code AS origin_zip,
# MAGIC     p.consignee,
# MAGIC     p.consignee_address,
# MAGIC     p.pickup_address,
# MAGIC     p.dest_city,
# MAGIC     p.dest_state,
# MAGIC     p.consignee_zip_code AS dest_zip,
# MAGIC         CASE
# MAGIC             WHEN p.miles RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.miles AS FLOAT)
# MAGIC             ELSE 0.0
# MAGIC         END AS miles,
# MAGIC         CASE
# MAGIC             WHEN p.weight RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.weight AS FLOAT)
# MAGIC             ELSE 0.0
# MAGIC         END AS weight,
# MAGIC     p.truck_num,
# MAGIC     p.trailer_number,
# MAGIC     p.driver_cell_num,
# MAGIC     p.carrier_id,
# MAGIC     p.hazmat,
# MAGIC     COALESCE(sales.full_name, p.sales_rep) AS sales_rep,
# MAGIC         CASE
# MAGIC             WHEN p.srv_rep = 'BRIOP' THEN 'Brianna Martinez'
# MAGIC             ELSE COALESCE(am.full_name, p.srv_rep)
# MAGIC         END AS srv_rep,
# MAGIC     p.accessorial1,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate1 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate1 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate1 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc1_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate1 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc1_rate,
# MAGIC
# MAGIC p.accessorial2,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate2 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate2 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate2 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc2_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate2 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc2_rate,
# MAGIC
# MAGIC p.accessorial3,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate3 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate3 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate3 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc3_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate3 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc3_rate,
# MAGIC
# MAGIC p.accessorial4,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate4 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate4 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate4 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc4_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate4 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc4_rate,
# MAGIC
# MAGIC p.accessorial5,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate5 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate5 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate5 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc5_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate5 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc5_rate,
# MAGIC
# MAGIC p.accessorial6,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate6 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate6 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate6 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc6_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate6 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc6_rate,
# MAGIC
# MAGIC p.accessorial7,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate7 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate7 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate7 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate7 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS carr_acc7_rate,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.customer_accessorial_rate7 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate7 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC END AS cust_acc7_rate,
# MAGIC
# MAGIC p.accessorial8,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.carrier_accessorial_rate8 IS NULL THEN NULL
# MAGIC     ELSE
# MAGIC         CASE
# MAGIC             WHEN p.carrier_accessorial_rate8 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.carrier_accessorial_rate8 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.carrier_accessorial_rate8 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END 
# MAGIC END AS carr_acc8_rate,
# MAGIC
# MAGIC CASE 
# MAGIC     WHEN p.customer_accessorial_rate8 IS NULL THEN NULL 
# MAGIC     ELSE 
# MAGIC         CASE 
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p.customer_accessorial_rate8 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC END AS cust_acc8_rate,
# MAGIC CASE
# MAGIC     WHEN p.accessorial1 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial2 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial3 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial4 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial5 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial6 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial7 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate7 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial8 NOT RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate8 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END AS customer_accessorial,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN p.accessorial1 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate1 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate1 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial2 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate2 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate2 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial3 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate3 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate3 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial4 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate4 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate4 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial5 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate5 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate5 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END
# MAGIC     ELSE 0.00
# MAGIC END +
# MAGIC CASE
# MAGIC     WHEN p.accessorial6 RLIKE 'FUE%' THEN
# MAGIC         CASE
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '%:%' THEN 0.00
# MAGIC             WHEN p.customer_accessorial_rate6 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate6 AS DOUBLE)
# MAGIC             ELSE 0.00
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial7 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.customer_accessorial_rate7 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate7 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial8 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.customer_accessorial_rate8 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.customer_accessorial_rate8 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END AS customer_fuel,
# MAGIC
# MAGIC CASE 
# MAGIC     WHEN p.carrier_line_haul RLIKE '%:%' THEN 0.00 
# MAGIC     WHEN p.carrier_line_haul RLIKE '^-?\\d+(\\.\\d+)?$' THEN CAST(p.carrier_line_haul AS DOUBLE) 
# MAGIC     ELSE NULL 
# MAGIC END AS carrier_linehaul,
# MAGIC
# MAGIC CASE 
# MAGIC     WHEN p.accessorial1 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate1 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate1 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate1 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial2 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate2 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate2 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate2 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial3 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate3 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate3 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate3 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial4 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate4 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate4 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate4 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial5 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate5 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate5 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate5 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial6 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate6 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate6 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate6 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial7 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate7 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate7 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate7 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN p.accessorial8 RLIKE 'FUE%' THEN 
# MAGIC         CASE 
# MAGIC             WHEN p.carrier_accessorial_rate8 RLIKE '%:%' THEN 0.00 
# MAGIC             WHEN p.carrier_accessorial_rate8 RLIKE '^\\d+(\\.\\d+)?$' THEN CAST(p.carrier_accessorial_rate8 AS DOUBLE) 
# MAGIC             ELSE 0.00 
# MAGIC         END 
# MAGIC     ELSE 0.00 
# MAGIC END AS carrier_fuel,
# MAGIC
# MAGIC p.ref_num
# MAGIC
# MAGIC FROM projection_load p
# MAGIC
# MAGIC LEFT JOIN bronze.financial_calendar ON DATE(p.pickup_date) = bronze.financial_calendar.date
# MAGIC
# MAGIC JOIN new_office_lookup ON CAST(p.office AS STRING) = new_office_lookup.old_office
# MAGIC
# MAGIC LEFT JOIN cad_currency ON CAST(p.id AS STRING) = cad_currency.pro_number
# MAGIC
# MAGIC LEFT JOIN aljex_customer_profiles ON CAST(p.customer_id AS STRING) = aljex_customer_profiles.cust_id
# MAGIC
# MAGIC LEFT JOIN projection_carrier c ON CAST(p.carrier_id AS STRING) = c.id
# MAGIC
# MAGIC LEFT JOIN canada_carriers ON CAST(p.carrier_id AS STRING) = canada_carriers.canada_dot
# MAGIC
# MAGIC LEFT JOIN customer_lookup ON LEFT(CAST(p.shipper AS STRING), 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC
# MAGIC JOIN average_conversions ON TRUE
# MAGIC
# MAGIC LEFT JOIN aljex_user_report_listing am ON CAST(p.srv_rep AS STRING) = am.aljex_id AND am.aljex_id IS NOT NULL
# MAGIC
# MAGIC LEFT JOIN aljex_user_report_listing sales ON CAST(p.sales_rep AS STRING) = sales.sales_rep AND sales.sales_rep IS NOT NULL
# MAGIC
# MAGIC LEFT JOIN canada_conversions ON DATE(p.pickup_date) = canada_conversions.ship_date
# MAGIC
# MAGIC LEFT JOIN aljex_user_report_listing ON CAST(p.key_c_user AS STRING) = aljex_user_report_listing.aljex_id AND aljex_user_report_listing.aljex_id <> 'repeat'
# MAGIC
# MAGIC LEFT JOIN relay_users ON aljex_user_report_listing.full_name = relay_users.full_name AND relay_users.`active?` = TRUE
# MAGIC
# MAGIC LEFT JOIN new_office_lookup car ON COALESCE(aljex_user_report_listing.pnl_code, relay_users.office_id) = car.old_office
# MAGIC
# MAGIC LEFT JOIN aljex_user_report_listing dis ON CAST(p.key_h_user AS STRING) = dis.aljex_id AND dis.aljex_id <> 'repeat'
# MAGIC
# MAGIC WHERE NOT (p.status LIKE '%VOID%')
# MAGIC ),
# MAGIC max_schedule_del as (
# MAGIC 		 SELECT DISTINCT delivery_projection.relay_reference_number,
# MAGIC     max(delivery_projection.scheduled_at::timestamp) AS max_schedule_del
# MAGIC    FROM brokerageprod.bronze.delivery_projection
# MAGIC   GROUP BY delivery_projection.relay_reference_number),
# MAGIC last_delivery as (
# MAGIC          SELECT DISTINCT planning_stop_schedule.relay_reference_number,
# MAGIC     max(planning_stop_schedule.sequence_number) AS last_delivery
# MAGIC    FROM brokerageprod.bronze.planning_stop_schedule
# MAGIC   WHERE planning_stop_schedule.`removed?` = false
# MAGIC   GROUP BY planning_stop_schedule.relay_reference_number
# MAGIC  ),
# MAGIC  max_schedule as (SELECT DISTINCT pickup_projection.relay_reference_number,
# MAGIC     max(pickup_projection.scheduled_at::timestamp_ntz) AS max_schedule
# MAGIC    FROM pickup_projection
# MAGIC   GROUP BY pickup_projection.relay_reference_number),
# MAGIC   truckload_proj_del as ( SELECT DISTINCT truckload_projection.booking_id,
# MAGIC     max(CAST(truckload_projection.last_update_date_time AS STRING)) AS truckload_proj_del
# MAGIC    FROM truckload_projection
# MAGIC   WHERE truckload_projection.last_update_event_name::string = 'MarkedDelivered'::string
# MAGIC   GROUP BY truckload_projection.booking_id),
# MAGIC
# MAGIC   Master_date as (
# MAGIC SELECT DISTINCT b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     
# MAGIC 	COALESCE(
# MAGIC 	    c.in_date_time,
# MAGIC 	    c.out_date_time,
# MAGIC 	    pss.appointment_datetime,
# MAGIC 	    pss.window_end_datetime,
# MAGIC 	    pss.window_start_datetime,
# MAGIC 	    pp.appointment_datetime,
# MAGIC 	    b.ready_date
# MAGIC 	    
# MAGIC 	) AS use_date,
# MAGIC
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN dayofweek(COALESCE(c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime, b.ready_date)) = 1 THEN date_add(COALESCE(c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime, b.ready_date), 6)
# MAGIC         ELSE date_add(date_trunc('week', COALESCE(c.in_date_time, c.out_date_time, pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime, b.ready_date)), 5)
# MAGIC     END AS end_date,
# MAGIC     CASE
# MAGIC         WHEN dayofweek(b.booked_at) = 1 THEN date_add(b.booked_at, 6)
# MAGIC         ELSE date_add(date_trunc('week', b.booked_at), 5)
# MAGIC     END AS booked_end_date,
# MAGIC     
# MAGIC 	CASE
# MAGIC 	    WHEN TRY_CAST((b.ready_time) AS timestamp) IS NULL THEN
# MAGIC 	        COALESCE(TRY_CAST((b.ready_date) AS date), DATE '1978-01-01')
# MAGIC 	    ELSE
# MAGIC 	        COALESCE(TRY_CAST((b.ready_date) AS date), DATE '1978-01-01')
# MAGIC 	END AS ready_date,
# MAGIC
# MAGIC     COALESCE(pss.window_start_datetime, pss.appointment_datetime, pp.appointment_datetime) AS pu_appt_start_datetime,
# MAGIC     pss.window_end_datetime AS pu_appt_end_datetime,
# MAGIC     COALESCE(pss.appointment_datetime, pss.window_end_datetime, pss.window_start_datetime, pp.appointment_datetime) AS pu_appt_date,
# MAGIC     pss.schedule_type AS pu_schedule_type,
# MAGIC     c.in_date_time AS pickup_in,
# MAGIC     c.out_date_time AS pickup_out,
# MAGIC     COALESCE(c.in_date_time, c.out_date_time) AS pickup_datetime,
# MAGIC 	COALESCE(
# MAGIC 		dpss.window_start_datetime,
# MAGIC 		dpss.appointment_datetime,
# MAGIC 		CASE
# MAGIC 			WHEN TRY_CAST((dp.appointment_time_local) AS timestamp) IS NULL THEN
# MAGIC 				COALESCE(TRY_CAST((dp.appointment_date) AS date), DATE '1978-01-01')
# MAGIC 			ELSE
# MAGIC 				COALESCE(TRY_CAST((dp.appointment_date) AS date), DATE '1978-01-01')
# MAGIC 				--  +
# MAGIC 				-- COALESCE(TRY_CAST(NULLIF(dp.appointment_time_local, '') AS timestamp), TIMEstamp '00:00:00')
# MAGIC 		END
# MAGIC 	) AS del_appt_start_datetime,
# MAGIC
# MAGIC     dpss.window_end_datetime AS del_appt_end_datetime,
# MAGIC 	COALESCE(
# MAGIC 		dpss.appointment_datetime,
# MAGIC 		dpss.window_end_datetime,
# MAGIC 		dpss.window_start_datetime,
# MAGIC         dp.appointment_time_local,
# MAGIC         dp.appointment_date
# MAGIC
# MAGIC 		-- CASE
# MAGIC 		-- 	WHEN TRY_CAST((dp.appointment_time_local) AS timestamp) IS NULL THEN
# MAGIC 		-- 		COALESCE(TRY_CAST((dp.appointment_date) AS date), DATE '1978-01-01')
# MAGIC 		-- 	ELSE
# MAGIC 		-- 		COALESCE(TRY_CAST((dp.appointment_date) AS date), DATE '1978-01-01') 
# MAGIC 		-- 		-- +
# MAGIC 		-- 		-- COALESCE(TRY_CAST(NULLIF(dp.appointment_time_local, '') AS timestamp), TIMEstamp '00:00:00')
# MAGIC 		-- END
# MAGIC 	) AS del_appt_date,
# MAGIC
# MAGIC     dpss.schedule_type AS del_schedule_type,
# MAGIC     hain_tracking_accuracy_projection.delivery_by_date_at_tender AS Delivery_By_Date,
# MAGIC     cd.in_date_time AS delivery_in,
# MAGIC     cd.out_date_time AS delivery_out,
# MAGIC     COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC     t.truckload_proj_del AS truckload_proj_delivered,
# MAGIC     b.booked_at AS booked_at
# MAGIC FROM booking_projection b
# MAGIC LEFT JOIN max_schedule ON b.relay_reference_number = max_schedule.relay_reference_number
# MAGIC LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC LEFT JOIN pickup_projection pp ON b.booking_id = pp.booking_id AND b.first_shipper_name = pp.shipper_name AND max_schedule.max_schedule = pp.scheduled_at AND pp.sequence_number = 1
# MAGIC LEFT JOIN planning_stop_schedule pss ON b.relay_reference_number = pss.relay_reference_number AND b.first_shipper_name = pss.stop_name AND pss.sequence_number = 1 AND pss.`removed?` = false
# MAGIC LEFT JOIN canonical_stop c ON pss.stop_id = c.stop_id AND c.`stale?` = false AND c.stop_type = 'pickup'
# MAGIC LEFT JOIN last_delivery ON b.relay_reference_number = last_delivery.relay_reference_number
# MAGIC LEFT JOIN delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number AND b.receiver_name = dp.receiver_name AND max_schedule_del.max_schedule_del = dp.scheduled_at
# MAGIC LEFT JOIN planning_stop_schedule dpss ON b.relay_reference_number = dpss.relay_reference_number AND b.receiver_name = dpss.stop_name AND b.receiver_city = dpss.locality AND last_delivery.last_delivery = dpss.sequence_number AND dpss.stop_type = 'delivery' AND dpss.`removed?` = false
# MAGIC LEFT JOIN canonical_stop cd ON dpss.stop_id = cd.stop_id AND b.receiver_city = cd.locality AND cd.`stale?` = false AND cd.stop_type = 'delivery'
# MAGIC LEFT JOIN hain_tracking_accuracy_projection ON b.relay_reference_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT JOIN truckload_proj_del t ON b.booking_id = t.booking_id
# MAGIC WHERE b.status <> 'cancelled' AND (canonical_plan_projection.mode <> 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(b.ship_date AS TIMESTAMP),
# MAGIC         CAST(c.in_date_time AS TIMESTAMP),
# MAGIC         CAST(c.out_date_time AS TIMESTAMP),
# MAGIC         CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC         CAST(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS use_date,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN DAYOFWEEK(
# MAGIC             CAST(COALESCE(
# MAGIC                 b.ship_date,
# MAGIC                 c.in_date_time,
# MAGIC                 c.out_date_time,
# MAGIC                 pss.appointment_datetime,
# MAGIC                 pss.window_end_datetime,
# MAGIC                 pss.window_start_datetime,
# MAGIC                 pp.appointment_datetime
# MAGIC             ) AS DATE)
# MAGIC         ) = 1 THEN DATE_ADD(
# MAGIC             CAST(COALESCE(
# MAGIC                 b.ship_date,
# MAGIC                 c.in_date_time,
# MAGIC                 c.out_date_time,
# MAGIC                 pss.appointment_datetime,
# MAGIC                 pss.window_end_datetime,
# MAGIC                 pss.window_start_datetime,
# MAGIC                 pp.appointment_datetime
# MAGIC             ) AS DATE),
# MAGIC             6
# MAGIC         )
# MAGIC         ELSE DATE_ADD(
# MAGIC             DATE_TRUNC('WEEK', CAST(COALESCE(
# MAGIC                 b.ship_date,
# MAGIC                 c.in_date_time,
# MAGIC                 c.out_date_time,
# MAGIC                 pss.appointment_datetime,
# MAGIC                 pss.window_end_datetime,
# MAGIC                 pss.window_start_datetime,
# MAGIC                 pp.appointment_datetime
# MAGIC             ) AS TIMESTAMP)),
# MAGIC             5
# MAGIC         )
# MAGIC     END AS end_date,
# MAGIC
# MAGIC     NULL AS booked_end_date,
# MAGIC     NULL AS ready_date,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_start_datetime,
# MAGIC
# MAGIC     CAST(pss.window_end_datetime AS TIMESTAMP) AS pu_appt_end_datetime,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC         CAST(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_date,
# MAGIC
# MAGIC     pss.schedule_type AS pu_schedule_type,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(b.ship_date AS TIMESTAMP),
# MAGIC         CAST(c.in_date_time AS TIMESTAMP)
# MAGIC     ) AS pickup_in,
# MAGIC
# MAGIC     CAST(c.out_date_time AS TIMESTAMP) AS pickup_out,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(b.ship_date AS TIMESTAMP),
# MAGIC         CAST(c.in_date_time AS TIMESTAMP),
# MAGIC         CAST(c.out_date_time AS TIMESTAMP)
# MAGIC     ) AS pickup_datetime,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC         CAST(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC         CASE
# MAGIC             WHEN TRY_CAST((dp.appointment_time_local) AS timestamp) IS NULL THEN
# MAGIC                 COALESCE(TRY_CAST((dp.appointment_date) AS DATE), DATE '1978-01-01')
# MAGIC             ELSE
# MAGIC                 COALESCE(TRY_CAST((dp.appointment_date) AS DATE), DATE '1978-01-01') 
# MAGIC -- +
# MAGIC --                 COALESCE(TRY_CAST(NULLIF(dp.appointment_time_local, '') AS TIMEstamp), TIMEstamp '00:00:00')
# MAGIC         END
# MAGIC     ) AS del_appt_start_datetime,
# MAGIC
# MAGIC     CAST(dpss.window_end_datetime AS TIMESTAMP) AS del_appt_end_datetime,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(dpss.window_end_datetime AS TIMESTAMP),
# MAGIC         CAST(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC         dp.appointment_date,
# MAGIC         dp.appointment_time_local
# MAGIC --         CASE
# MAGIC --             WHEN TRY_CAST((dp.appointment_time_local) AS timestamp) IS NULL THEN
# MAGIC --                 COALESCE(TRY_CAST((dp.appointment_date) AS DATE), DATE '1978-01-01')
# MAGIC --             ELSE
# MAGIC --                 COALESCE(TRY_CAST((dp.appointment_date) AS DATE), DATE '1978-01-01') 
# MAGIC -- -- +
# MAGIC -- --                 COALESCE(TRY_CAST(NULLIF(dp.appointment_time_local, '') AS TIMEstamp), TIMEstamp '00:00:00')
# MAGIC --         END
# MAGIC     ) AS del_appt_date,
# MAGIC
# MAGIC     dpss.schedule_type AS del_schedule_type,
# MAGIC
# MAGIC     CAST(hain_tracking_accuracy_projection.delivery_by_date_at_tender AS DATE) AS Delivery_By_Date,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(b.delivered_date AS TIMESTAMP),
# MAGIC         CAST(cd.in_date_time AS TIMESTAMP)
# MAGIC     ) AS delivery_in,
# MAGIC
# MAGIC     CAST(cd.out_date_time AS TIMESTAMP) AS delivery_out,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(b.delivered_date AS TIMESTAMP),
# MAGIC         CAST(cd.in_date_time AS TIMESTAMP),
# MAGIC         CAST(cd.out_date_time AS TIMESTAMP)
# MAGIC     ) AS delivery_datetime,
# MAGIC
# MAGIC     NULL AS truckload_proj_delivered,
# MAGIC     NULL AS booked_at
# MAGIC
# MAGIC FROM big_export_projection b
# MAGIC LEFT JOIN max_schedule ON b.load_number = max_schedule.relay_reference_number
# MAGIC LEFT JOIN max_schedule_del ON b.load_number = max_schedule_del.relay_reference_number
# MAGIC LEFT JOIN pickup_projection pp ON b.load_number = pp.relay_reference_number 
# MAGIC     AND b.pickup_name = pp.shipper_name 
# MAGIC     AND b.weight = pp.weight_to_pickup_amount 
# MAGIC     AND b.piece_count = pp.pieces_to_pickup_count 
# MAGIC     AND max_schedule.max_schedule = pp.scheduled_at 
# MAGIC     AND pp.sequence_number = 1
# MAGIC LEFT JOIN planning_stop_schedule pss ON b.load_number = pss.relay_reference_number 
# MAGIC     AND b.pickup_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = false
# MAGIC LEFT JOIN canonical_stop c ON pss.stop_id = c.stop_id
# MAGIC LEFT JOIN delivery_projection dp ON b.load_number = dp.relay_reference_number 
# MAGIC     AND b.consignee_name = dp.receiver_name 
# MAGIC     AND max_schedule_del.max_schedule_del = dp.scheduled_at
# MAGIC LEFT JOIN last_delivery ON b.load_number = last_delivery.relay_reference_number
# MAGIC LEFT JOIN planning_stop_schedule dpss ON b.load_number = dpss.relay_reference_number 
# MAGIC     AND b.consignee_name = dpss.stop_name 
# MAGIC     AND b.consignee_city = dpss.locality 
# MAGIC     AND last_delivery.last_delivery = dpss.sequence_number 
# MAGIC     AND dpss.stop_type = 'delivery' 
# MAGIC     AND dpss.`removed?` = false
# MAGIC LEFT JOIN canonical_stop cd ON dpss.stop_id = cd.stop_id
# MAGIC LEFT JOIN hain_tracking_accuracy_projection ON b.load_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC
# MAGIC WHERE b.dispatch_status != 'Cancelled'
# MAGIC   AND (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC ),
# MAGIC
# MAGIC use_dates AS (
# MAGIC   SELECT DISTINCT
# MAGIC     date AS use_dates
# MAGIC   FROM brokerageprod.bronze.financial_calendar
# MAGIC   WHERE 1 = 1
# MAGIC   ORDER BY use_dates DESC
# MAGIC ),
# MAGIC
# MAGIC system_union AS (
# MAGIC   SELECT DISTINCT
# MAGIC     'Relay' AS tms,
# MAGIC     m.booking_id,
# MAGIC     m.relay_reference_number,
# MAGIC     m.use_date,
# MAGIC     COALESCE(DATE(pu_appt_date), DATE(m.ready_date)) AS pu_appt_date,
# MAGIC     DATE(pickup_datetime) AS pickup_datetime,
# MAGIC     CASE
# MAGIC       WHEN pickup_datetime IS NULL AND COALESCE(DATE(pu_appt_date), DATE(m.ready_date)) > CURRENT_TIMESTAMP THEN 'ontime'
# MAGIC       WHEN pickup_datetime IS NULL AND COALESCE(DATE(pu_appt_date), DATE(m.ready_date)) < CURRENT_TIMESTAMP THEN 'late'
# MAGIC       ELSE CASE
# MAGIC         WHEN DATE_TRUNC('minute', DATE(pickup_datetime)) > COALESCE(DATE(pu_appt_date), DATE(m.ready_date)) THEN 'late'
# MAGIC         ELSE 'ontime'
# MAGIC       END
# MAGIC     END AS ot_pu,
# MAGIC     DATE(del_appt_date) AS del_appt_date,
# MAGIC     DATE(delivery_datetime) AS delivery_datetime,
# MAGIC     CASE
# MAGIC       WHEN delivery_datetime IS NULL AND DATE(del_appt_date) > CURRENT_TIMESTAMP THEN 'ontime'
# MAGIC       WHEN delivery_datetime IS NULL AND DATE(del_appt_date) < CURRENT_TIMESTAMP THEN 'late'
# MAGIC       ELSE CASE
# MAGIC         WHEN DATE_TRUNC('minute', DATE(delivery_datetime)) > DATE(del_appt_date) THEN 'late'
# MAGIC         ELSE 'ontime'
# MAGIC       END
# MAGIC     END AS ot_del,
# MAGIC     CASE
# MAGIC       WHEN master_customer_name IS NULL THEN customer_profile_projection.customer_name
# MAGIC       ELSE master_customer_name
# MAGIC     END AS mastername,
# MAGIC     bronze.financial_calendar.financial_period_sorting,
# MAGIC     new_office_lookup.new_office,
# MAGIC     CONCAT(INITCAP(b.first_shipper_city), ',', first_shipper_state) AS origin,
# MAGIC     CONCAT(INITCAP(b.receiver_city), ',', receiver_state) AS destination,
# MAGIC     CONCAT(INITCAP(b.first_shipper_city), ',', first_shipper_state, '>', INITCAP(b.receiver_city), ',', receiver_state) AS lane
# MAGIC   FROM master_date m
# MAGIC   JOIN use_dates ON DATE(m.use_date) = DATE(use_dates.use_dates)
# MAGIC   JOIN bronze.financial_calendar ON DATE(m.use_date) = DATE(bronze.financial_calendar.date)
# MAGIC   JOIN booking_projection b ON m.booking_id = b.booking_id
# MAGIC   JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC   JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC   LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC   WHERE 1 = 1
# MAGIC     AND m.status = 'booked'
# MAGIC     AND ltl_or_tl = 'TL'
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     'Relay' AS tms,
# MAGIC     m.booking_id,
# MAGIC     m.relay_reference_number,
# MAGIC     m.use_date,
# MAGIC     COALESCE(DATE(pu_appt_date), DATE(m.ready_date)),
# MAGIC     DATE(pickup_datetime),
# MAGIC     CASE
# MAGIC       WHEN pickup_datetime IS NULL AND COALESCE(DATE(pu_appt_date), DATE(m.ready_date)) > CURRENT_TIMESTAMP THEN 'ontime'
# MAGIC       WHEN pickup_datetime IS NULL AND COALESCE(DATE(pu_appt_date), DATE(m.ready_date)) < CURRENT_TIMESTAMP THEN 'late'
# MAGIC       ELSE CASE
# MAGIC         WHEN DATE_TRUNC('minute', DATE(pickup_datetime)) > COALESCE(DATE(pu_appt_date), DATE(m.ready_date)) THEN 'late'
# MAGIC         ELSE 'ontime'
# MAGIC       END
# MAGIC     END AS ot_pu,
# MAGIC     DATE(del_appt_date),
# MAGIC     DATE(delivery_datetime),
# MAGIC     CASE
# MAGIC       WHEN delivery_datetime IS NULL AND DATE(del_appt_date) > CURRENT_TIMESTAMP THEN 'ontime'
# MAGIC       WHEN delivery_datetime IS NULL AND DATE(del_appt_date) < CURRENT_TIMESTAMP THEN 'late'
# MAGIC       ELSE CASE
# MAGIC         WHEN DATE_TRUNC('minute', DATE(delivery_datetime)) > DATE(del_appt_date) THEN 'late'
# MAGIC         ELSE 'ontime'
# MAGIC       END
# MAGIC     END AS ot_del,
# MAGIC     CASE
# MAGIC       WHEN master_customer_name IS NULL THEN customer_profile_projection.customer_name
# MAGIC       ELSE master_customer_name
# MAGIC     END AS mastername,
# MAGIC     bronze.financial_calendar.financial_period_sorting,
# MAGIC     new_office_lookup.new_office,
# MAGIC     CONCAT(INITCAP(b.pickup_city), ',', pickup_state) AS origin,
# MAGIC     CONCAT(INITCAP(b.consignee_city), ',', consignee_state) AS destination,
# MAGIC     CONCAT(INITCAP(b.pickup_city), ',', pickup_state, '>', INITCAP(b.consignee_city), ',', consignee_state) AS lane
# MAGIC   FROM master_date m
# MAGIC   JOIN use_dates ON DATE(m.use_date) = DATE(use_dates.use_dates)
# MAGIC   JOIN bronze.financial_calendar ON DATE(m.use_date) = DATE(bronze.financial_calendar.date)
# MAGIC   JOIN big_export_projection b ON m.booking_id = b.load_number
# MAGIC   JOIN customer_profile_projection ON b.customer_id = customer_profile_projection.customer_slug
# MAGIC   JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC   LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC   WHERE 1 = 1
# MAGIC     AND b.carrier_name IS NOT NULL
# MAGIC     AND ltl_or_tl != 'TL'
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT DISTINCT
# MAGIC     'Aljex' AS tms,
# MAGIC     m.id,
# MAGIC     m.id,
# MAGIC     m.use_date,
# MAGIC     aljex_master_dates.pu_appt_date,
# MAGIC     aljex_master_dates.use_date AS new_pu_time,
# MAGIC     CASE
# MAGIC       WHEN aljex_master_dates.pu_appt_date IS NULL THEN 'late'
# MAGIC       WHEN DATE(aljex_master_dates.use_date) IS NULL AND DATE(aljex_master_dates.pu_appt_date) > CURRENT_TIMESTAMP THEN 'ontime'
# MAGIC       WHEN DATE(aljex_master_dates.use_date) IS NULL AND DATE(aljex_master_dates.pu_appt_date) < CURRENT_TIMESTAMP THEN 'late'
# MAGIC       ELSE CASE
# MAGIC         WHEN DATE_TRUNC('minute', DATE(aljex_master_dates.use_date)) > DATE(aljex_master_dates.pu_appt_date) THEN 'late'
# MAGIC         ELSE 'ontime'
# MAGIC       END
# MAGIC     END AS ot_pu,
# MAGIC     aljex_master_dates.del_appt_date,
# MAGIC     aljex_master_dates.delivered_date,
# MAGIC     CASE
# MAGIC       WHEN aljex_master_dates.del_appt_date IS NULL THEN 'late'
# MAGIC       WHEN DATE(aljex_master_dates.delivered_date) IS NULL AND DATE(aljex_master_dates.del_appt_date) > CURRENT_TIMESTAMP THEN 'ontime'
# MAGIC       WHEN DATE(aljex_master_dates.delivered_date) IS NULL AND DATE(aljex_master_dates.del_appt_date) < CURRENT_TIMESTAMP THEN 'late'
# MAGIC       ELSE CASE
# MAGIC         WHEN DATE_TRUNC('minute', DATE(aljex_master_dates.delivered_date)) > DATE(aljex_master_dates.del_appt_date) THEN 'late'
# MAGIC         ELSE 'ontime'
# MAGIC       END
# MAGIC     END AS ot_del,
# MAGIC     m.mastername,
# MAGIC     m.financial_period_sorting,
# MAGIC     new_office_lookup.new_office,
# MAGIC     CONCAT(INITCAP(m.origin_city), ',', origin_state) AS origin,
# MAGIC     CONCAT(INITCAP(m.dest_city), ',', dest_state) AS destination,
# MAGIC     CONCAT(INITCAP(m.origin_city), ',', origin_state, '>', INITCAP(m.dest_city), ',', dest_state) AS lane
# MAGIC   FROM aljex_data m
# MAGIC   JOIN use_dates ON DATE(m.use_date) = DATE(use_dates.use_dates)
# MAGIC   JOIN aljex_master_dates ON m.id = aljex_master_dates.id
# MAGIC   JOIN bronze.financial_calendar ON DATE(m.use_date) = DATE(bronze.financial_calendar.date)
# MAGIC   JOIN new_office_lookup ON m.customer_office = new_office_lookup.new_office
# MAGIC   LEFT JOIN customer_lookup ON m.mastername = customer_lookup.master_customer_name
# MAGIC   WHERE m.equipment NOT IN ('TORD', 'STOR')
# MAGIC ),
# MAGIC
# MAGIC casted as (SELECT 
# MAGIC     tms,
# MAGIC     booking_id,
# MAGIC     relay_reference_number,
# MAGIC     cast(use_date as date),
# MAGIC     cast(pu_appt_date as date),
# MAGIC     pickup_datetime,
# MAGIC     ot_pu,
# MAGIC     cast(del_appt_date as date),
# MAGIC     delivery_datetime,
# MAGIC     ot_del,
# MAGIC     mastername,
# MAGIC     financial_period_sorting,
# MAGIC     new_office,
# MAGIC     origin,
# MAGIC     destination,
# MAGIC     lane
# MAGIC FROM system_union)
# MAGIC
# MAGIC select * from casted

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creation Of Fact AM Table

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC  SET ansi_mode = false; 
# MAGIC create or replace table Analytics.fact_am AS 
# MAGIC   WITH new_customer_money_sp AS (
# MAGIC     SELECT
# MAGIC       DISTINCT m.relay_reference_number AS loadnum,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN m.charge_code = 'linehaul' THEN m.amount
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) / 100.0 AS linehaul_cust_money,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN m.charge_code = 'fuel_surcharge' THEN m.amount
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) / 100.0 AS fuel_cust_money,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN m.charge_code NOT IN ('fuel_surcharge', 'linehaul') THEN m.amount
# MAGIC           ELSE 0
# MAGIC         END
# MAGIC       ) / 100.0 AS acc_cust_money,
# MAGIC       SUM(m.amount) / 100.0 AS total_cust_amount,
# MAGIC       SUM(DISTINCT COALESCE(ic.total_amount, 0)) / 100.0 AS inv_cred_amt
# MAGIC     FROM
# MAGIC       brokerageprod.bronze.moneying_billing_party_transaction AS m
# MAGIC       LEFT JOIN brokerageprod.bronze.invoicing_credits AS ic ON CAST(m.relay_reference_number AS FLOAT) = CAST(ic.relay_reference_number AS FLOAT)
# MAGIC     WHERE
# MAGIC       m.`voided?` = 'false'
# MAGIC     GROUP BY
# MAGIC       m.relay_reference_number
# MAGIC   ),
# MAGIC   -- Select * from new_customer_money_sp
# MAGIC   raw_data_one AS (
# MAGIC     SELECT
# MAGIC       CAST(created_at AS DATE) AS created_at,
# MAGIC       quote_id,
# MAGIC       created_by,
# MAGIC       CASE
# MAGIC         WHEN customer_name = 'TJX Companies c/o Blueyonder' THEN 'IL'
# MAGIC         ELSE COALESCE(
# MAGIC           relay_users.office_id,
# MAGIC           aljex_user_report_listing.pnl_code
# MAGIC         )
# MAGIC       END AS rep_office,
# MAGIC       load_number,
# MAGIC       shipper_ref_number,
# MAGIC       1 AS counter,
# MAGIC       customer_name,
# MAGIC       COALESCE(master_customer_name, customer_name) AS mastername,
# MAGIC       quoted_price,
# MAGIC       margin,
# MAGIC       shipper_state,
# MAGIC       status_str,
# MAGIC       receiver_state,
# MAGIC       last_updated_at
# MAGIC     FROM
# MAGIC       brokerageprod.bronze.spot_quotes
# MAGIC       JOIN analytics.dim_date ON CAST(spot_quotes.created_at AS DATE) = dim_date.Calendar_Date
# MAGIC       LEFT JOIN brokerageprod.bronze.relay_users ON spot_quotes.created_by = relay_users.full_name
# MAGIC       LEFT JOIN brokerageprod.bronze.aljex_user_report_listing ON spot_quotes.created_by = aljex_user_report_listing.full_name
# MAGIC       LEFT JOIN brokerageprod.bronze.customer_lookup ON LEFT(customer_name, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC     WHERE
# MAGIC       1 = 1
# MAGIC     ORDER BY
# MAGIC       created_at DESC
# MAGIC   ),
# MAGIC   -- Select * from raw_data_one
# MAGIC   raw_data AS (
# MAGIC     SELECT
# MAGIC       CAST(created_at AS DATE) AS created_at,
# MAGIC       quote_id,
# MAGIC       created_by,
# MAGIC       CASE
# MAGIC         WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC         WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC         ELSE rep_office
# MAGIC       END AS rep_office,
# MAGIC       load_number,
# MAGIC       shipper_ref_number,
# MAGIC       1 AS counter,
# MAGIC       customer_name,
# MAGIC       mastername,
# MAGIC       quoted_price,
# MAGIC       margin,
# MAGIC       shipper_state,
# MAGIC       status_str,
# MAGIC       receiver_state,
# MAGIC       last_updated_at,
# MAGIC       rank() OVER (
# MAGIC         PARTITION BY Load_Number,
# MAGIC         status_str
# MAGIC         ORDER BY
# MAGIC           last_updated_at DESC
# MAGIC       ) AS rank
# MAGIC     FROM
# MAGIC       raw_data_one
# MAGIC       LEFT JOIN bronze.days_to_pay_offices ON CASE
# MAGIC         WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC         WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC         ELSE rep_office
# MAGIC       END = days_to_pay_offices.office
# MAGIC     WHERE
# MAGIC       1 = 1
# MAGIC     ORDER BY
# MAGIC       created_at DESC
# MAGIC   ),
# MAGIC   -- Select * from raw_data
# MAGIC   relay_spot_number AS (
# MAGIC     SELECT
# MAGIC       DISTINCT b.relay_reference_number,
# MAGIC       t.shipment_id,
# MAGIC       r.customer_name,
# MAGIC       master_customer_name,
# MAGIC       COALESCE(ncm.total_cust_amount, 0.0) - COALESCE(ncm.inv_cred_amt, 0.0) AS revenue,
# MAGIC       NULL AS column_null,
# MAGIC       rd.created_at,
# MAGIC       rd.quote_id,
# MAGIC       1 AS counter,
# MAGIC       rd.quoted_price,
# MAGIC       rd.margin,
# MAGIC       rd.created_by,
# MAGIC       rd.rep_office,
# MAGIC       rd.load_number,
# MAGIC       rd.shipper_ref_number,
# MAGIC       rd.status_str,
# MAGIC       rd.last_updated_at
# MAGIC     FROM
# MAGIC       brokerageprod.bronze.booking_projection AS b
# MAGIC       LEFT JOIN brokerageprod.bronze.customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       LEFT JOIN brokerageprod.bronze.customer_lookup AS cl ON LEFT(r.customer_name, 32) = LEFT(cl.aljex_customer_name, 32)
# MAGIC       LEFT JOIN brokerageprod.bronze.tendering_acceptance AS t ON b.relay_reference_number = t.relay_reference_number
# MAGIC       JOIN raw_data AS rd ON CAST(b.relay_reference_number AS STRING) = rd.load_number
# MAGIC       AND master_customer_name = rd.mastername
# MAGIC       AND b.first_shipper_state = rd.shipper_state
# MAGIC       AND b.receiver_state = rd.receiver_state
# MAGIC       LEFT JOIN new_customer_money_sp AS ncm ON b.relay_reference_number = ncm.loadnum
# MAGIC     WHERE
# MAGIC       1 = 1
# MAGIC       and rd.status_str = 'WON'
# MAGIC       and rd.rank = 1
# MAGIC   ),
# MAGIC   -- Select * from relay_spot_number
# MAGIC   aljex_spot_loads as (
# MAGIC     SELECT
# MAGIC       DISTINCT CAST(p.id AS FLOAT) AS id,
# MAGIC       p.pickup_reference_number,
# MAGIC       p1.shipper,
# MAGIC       COALESCE(cl.master_customer_name, p1.shipper) AS master_customer_name,
# MAGIC       CAST(p.invoice_total AS FLOAT) AS revenue,
# MAGIC       NULL AS column_null,
# MAGIC       rd.created_at,
# MAGIC       rd.quote_id,
# MAGIC       1 AS counter,
# MAGIC       rd.quoted_price,
# MAGIC       rd.margin,
# MAGIC       rd.created_by,
# MAGIC       rd.rep_office,
# MAGIC       rd.load_number,
# MAGIC       rd.shipper_ref_number,
# MAGIC       rd.status_str,
# MAGIC       rd.last_updated_at
# MAGIC     FROM
# MAGIC       brokerageprod.bronze.projection_load_1 p
# MAGIC       left join brokerageprod.bronze.projection_load_2 p1 on p.id = p1.id
# MAGIC       LEFT JOIN brokerageprod.bronze.customer_lookup cl ON LEFT(p1.shipper, 32) = LEFT(cl.aljex_customer_name, 32)
# MAGIC       JOIN raw_data rd ON CAST(p.id AS STRING) = rd.load_number
# MAGIC       AND COALESCE(cl.master_customer_name, p1.shipper) = rd.mastername
# MAGIC       AND p.origin_state = rd.shipper_state
# MAGIC       AND p.dest_state = rd.receiver_state
# MAGIC     WHERE
# MAGIC       1 = 1
# MAGIC       AND CAST(p.pickup_date AS DATE) >= '2021-01-01'
# MAGIC       and rd.status_str = 'WON'
# MAGIC       and rd.rank = 1
# MAGIC   ),
# MAGIC   -- Select * from aljex_spot_loads
# MAGIC   combined_loads_mine as (
# MAGIC     select
# MAGIC       distinct relay_reference_number_one,
# MAGIC       relay_reference_number_two,
# MAGIC       resulting_plan_id,
# MAGIC       canonical_plan_projection.relay_reference_number as combine_new_rrn,
# MAGIC       canonical_plan_projection.mode,
# MAGIC       b.tender_on_behalf_of_id,
# MAGIC       'COMBINED' as combined
# MAGIC     from
# MAGIC       brokerageprod.bronze.plan_combination_projection
# MAGIC       join brokerageprod.bronze.canonical_plan_projection on resulting_plan_id = canonical_plan_projection.plan_id
# MAGIC       left join brokerageprod.bronze.booking_projection b on canonical_plan_projection.relay_reference_number = b.relay_reference_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and is_combined = 'true'
# MAGIC   ),
# MAGIC   -- Select * from combined_loads_mine
# MAGIC   max_schedule_del as (
# MAGIC     SELECT
# MAGIC       DISTINCT delivery_projection.relay_reference_number,
# MAGIC       max(delivery_projection.scheduled_at :: timestamp) AS max_schedule_del
# MAGIC     FROM
# MAGIC       brokerageprod.bronze.delivery_projection
# MAGIC     GROUP BY
# MAGIC       delivery_projection.relay_reference_number
# MAGIC   ),
# MAGIC   last_delivery as (
# MAGIC     SELECT
# MAGIC       DISTINCT planning_stop_schedule.relay_reference_number,
# MAGIC       max(planning_stop_schedule.sequence_number) AS last_delivery
# MAGIC     FROM
# MAGIC       brokerageprod.bronze.planning_stop_schedule
# MAGIC     WHERE
# MAGIC       planning_stop_schedule.`removed?` = false
# MAGIC     GROUP BY
# MAGIC       planning_stop_schedule.relay_reference_number
# MAGIC   ),
# MAGIC   pricing_nfi_load_predictions as (
# MAGIC     select
# MAGIC       external_load_id as MR_id,
# MAGIC       max(target_buy_rate_gsn) * max(miles_predicted) as Market_Buy_Rate,
# MAGIC       1 as Markey_buy_rate_flag
# MAGIC     from
# MAGIC       `brokerageprod`.`bronze`.`pricing_nfi_load_predictions`
# MAGIC     group by
# MAGIC       external_load_id
# MAGIC   ),
# MAGIC   to_get_avg as (
# MAGIC     select
# MAGIC       distinct ship_date,
# MAGIC       conversion as us_to_cad,
# MAGIC       us_to_cad as cad_to_us
# MAGIC     from
# MAGIC       brokerageprod.bronze.canada_conversions
# MAGIC     order by
# MAGIC       ship_date desc
# MAGIC     limit
# MAGIC       7
# MAGIC   ), average_conversions as (
# MAGIC     select
# MAGIC       avg(us_to_cad) as avg_us_to_cad,
# MAGIC       avg(cad_to_us) as avg_cad_to_us
# MAGIC     from
# MAGIC       to_get_avg
# MAGIC   ),
# MAGIC   use_relay_loads as (
# MAGIC     select
# MAGIC       distinct canonical_plan_projection.mode as Mode_Filter,
# MAGIC       case
# MAGIC         when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC         else case
# MAGIC           when canonical_plan_projection.mode = 'imdl' then 'IMDL'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC       end as modee,
# MAGIC       'TL' as equipment,
# MAGIC       b.relay_reference_number as relay_load,
# MAGIC       'NO' as combined_load,
# MAGIC       b.booking_id,
# MAGIC       b.booked_by_name as Carrier_Rep,
# MAGIC       carr.new_office as carr_office,
# MAGIC       cust.new_office as customer_office,
# MAGIC       b.ready_date As Ready_Date,
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as pu_appt_date,
# MAGIC       --b.booked_at as Booked_Date,
# MAGIC       --    coalesce(
# MAGIC       --     (coalesce(s.appointment_datetime, s.window_start_datetime, s.window_end_datetime, p.appointment_datetime, b.ready_date::timestamp)::timestamp at time zone shippers.time_zone at time zone 'america/chicago')::timestamp,
# MAGIC       --     (b.ready_date::timestamp at time zone shippers.time_zone at time zone 'america/chicago')::timestamp
# MAGIC       -- --   ) as new_pu_appt,
# MAGIC       --    coalesce(
# MAGIC       --     COALESCE(s.appointment_datetime, s.window_start_datetime, s.window_end_datetime, p.appointment_datetime, b.ready_date::timestamp) at time zone shippers.time_zone at time zone 'america/chicago',
# MAGIC       --     b.ready_date::timestamp at time zone shippers.time_zone at time zone 'america/chicago'  -- making sure ready_date fallback also has timezone conversion
# MAGIC       --   ) as new_pu_appt,
# MAGIC       COALESCE(
# MAGIC         CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC         CAST(pp.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(b.ready_date AS TIMESTAMP)
# MAGIC       ) AS new_pu_appt,
# MAGIC       c.in_date_time :: TIMESTAMP As pickup_in,
# MAGIC       c.out_date_time :: TIMESTAMP As pickup_out,
# MAGIC       coalesce(
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as pickup_datetime,
# MAGIC       coalesce(
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as canonical_stop,
# MAGIC       coalesce(
# MAGIC         c.in_date_time :: date,
# MAGIC         c.out_date_time :: date,
# MAGIC         pss.appointment_datetime :: date,
# MAGIC         pss.window_end_datetime :: date,
# MAGIC         pss.window_start_datetime :: date,
# MAGIC         pp.appointment_datetime :: date,
# MAGIC         b.ready_date :: date
# MAGIC       ) as use_date,
# MAGIC       case
# MAGIC         when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC         else customer_lookup.master_customer_name
# MAGIC       end as mastername,
# MAGIC       r.customer_name as customer_name,
# MAGIC       z.full_name as customer_rep,
# MAGIC       coalesce(
# MAGIC         tendering_planned_distance.planned_distance_amount :: float,
# MAGIC         b.total_miles :: float
# MAGIC       ) as total_miles,
# MAGIC       upper(b.status) as status,
# MAGIC       initcap(b.first_shipper_city) as origin_city,
# MAGIC       upper(b.first_shipper_state) as origin_state,
# MAGIC       initcap(b.receiver_city) as dest_city,
# MAGIC       upper(b.receiver_state) as dest_state,
# MAGIC       b.first_shipper_zip as origin_zip,
# MAGIC       b.receiver_zip as dest_zip,
# MAGIC       o.market as market_origin,
# MAGIC       d.market AS market_dest,
# MAGIC       CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC       --financial_calendar.financial_period_sorting,
# MAGIC       --financial_calendar.financial_year,
# MAGIC       carrier_projection.dot_number :: string as dot_num,
# MAGIC       coalesce(a.in_date_time, a.out_date_time) :: date as del_date,
# MAGIC       rolled_at :: date as rolled_at,
# MAGIC       b.booked_at as Booked_Date,
# MAGIC       ta.accepted_at :: string as Tendering_Date,
# MAGIC       b.bounced_at :: date as bounced_date,
# MAGIC       b.bounced_at :: date + 2 as cancelled_date,
# MAGIC       cd.in_date_time AS delivery_in,
# MAGIC       cd.out_date_time AS delivery_out,
# MAGIC       COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC       CASE
# MAGIC         WHEN date_format(
# MAGIC           cast(
# MAGIC             nullif(dp.appointment_time_local, '') as timestamp
# MAGIC           ),
# MAGIC           'HH:mm'
# MAGIC         ) IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(
# MAGIC           date_format(
# MAGIC             cast(
# MAGIC               nullif(dp.appointment_time_local, '') as timestamp
# MAGIC             ),
# MAGIC             'HH:mm'
# MAGIC           ) as interval
# MAGIC         )
# MAGIC       END AS del_appt_date,
# MAGIC       --pickup.Days_ahead as Days_Ahead,
# MAGIC       --pickup.prebookable as prebookable,
# MAGIC       --papp.PickUp_Start,
# MAGIC       --papp.PickUp_End,
# MAGIC       --dapp.Drop_Start,
# MAGIC       --dapp.Drop_End,
# MAGIC       b.booked_carrier_name as carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC       b.cargo_value_amount / 100.00 as Cargo_Value,
# MAGIC       b.total_weight as Weight,
# MAGIC       mx.max_buy :: float / 100.00 as max_buy,
# MAGIC       mr.Market_Buy_Rate,
# MAGIC       sp.quoted_price as Spot_Revenue,
# MAGIC       sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       brokerageprod.bronze.booking_projection b
# MAGIC       left join relay_spot_number sp on b.relay_reference_number = sp.relay_reference_number
# MAGIC       left join bronze.sourcing_max_buy_v2 mx on b.relay_reference_number = mx.relay_reference_number
# MAGIC       left join pricing_nfi_load_predictions mr on b.relay_reference_number = mr.MR_id ------ Market rate
# MAGIC       --   Left Join brokerageprod.bronze.spot_quotes spot on b.relay_reference_number::string = spot.load_number::string and spot.status_str = 'WON'
# MAGIC       left join brokerageprod.bronze.truckload_projection t on b.relay_reference_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced'
# MAGIC       and t.trailer_number is not null
# MAGIC       LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC       LEFT join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN brokerageprod.bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       left join brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join brokerageprod.bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true'
# MAGIC       left join brokerageprod.bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC       JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office ----- Need to change to new_office_lookup_w_tgt later
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup d ON LEFT(b.receiver_zip, 3) = d.pickup_zip
# MAGIC       LEFT join brokerageprod.bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC       AND relay_users.`active?` = 'true'
# MAGIC       LEFT join brokerageprod.bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
# MAGIC       Left Join last_delivery on b.relay_reference_number = last_delivery.relay_reference_number ---- Delivery date JOIN
# MAGIC       LEFT JOIN brokerageprod.bronze.delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number
# MAGIC       AND b.receiver_name :: string = dp.receiver_name :: string
# MAGIC       AND max_schedule_del.max_schedule_del = dp.scheduled_at --::timestamp --without time zone
# MAGIC       LEFT JOIN brokerageprod.bronze.planning_stop_schedule dpss ON b.relay_reference_number = dpss.relay_reference_number
# MAGIC       AND b.receiver_name :: string = dpss.stop_name :: string
# MAGIC       AND b.receiver_city :: string = dpss.locality :: string
# MAGIC       AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC       AND dpss.stop_type :: string = 'delivery' :: string
# MAGIC       AND dpss.`removed?` = false
# MAGIC       LEFT JOIN brokerageprod.bronze.canonical_stop cd ON dpss.stop_id :: string = cd.stop_id :: string
# MAGIC       AND b.receiver_city :: string = cd.locality :: string
# MAGIC       AND cd.`stale?` = false
# MAGIC       AND cd.stop_type :: string = 'delivery' :: string ---- Delivery Detail End
# MAGIC       LEFT join brokerageprod.bronze.pickup_projection pp on (
# MAGIC         b.booking_id = pp.booking_id
# MAGIC         AND b.first_shipper_name = pp.shipper_name
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join brokerageprod.bronze.planning_stop_schedule pss on (
# MAGIC         b.relay_reference_number = pss.relay_reference_number
# MAGIC         AND b.first_shipper_name = pss.stop_name
# MAGIC         AND pss.sequence_number = 1
# MAGIC         AND pss.`removed?` = 'false'
# MAGIC       )
# MAGIC       LEFT join brokerageprod.bronze.canonical_stop c on (
# MAGIC         pss.stop_id = c.stop_id
# MAGIC         and c.`stale?` = 'false'
# MAGIC         and c.stop_type = 'pickup'
# MAGIC       )
# MAGIC       left join brokerageprod.bronze.canonical_stop a on b.booking_id = a.booking_id
# MAGIC       and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC       and upper(b.receiver_city) = upper(a.locality)
# MAGIC       and a.stop_type = 'delivery'
# MAGIC       and a.`stale?` = 'false'
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           system,
# MAGIC           max(load_number) as max_loadnumber,
# MAGIC           max(Pickup_date) as Pickup_date,
# MAGIC           max(prebookable) as prebookable,
# MAGIC           Max(days_ahead) as days_ahead
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_pickup
# MAGIC         group by
# MAGIC           system
# MAGIC       ) as pickup ON b.relay_reference_number = pickup.max_loadnumber
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp ON b.relay_reference_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON b.relay_reference_number = dapp.loadnum --left join bounce_data on b.relay_reference_number = bounce_data.relay_reference_number
# MAGIC       --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       --JOIN brokerageprod.bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC       left join combined_loads_mine on b.relay_reference_number = combined_loads_mine.combine_new_rrn
# MAGIC       left join brokerageprod.bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join brokerageprod.bronze.tendering_planned_distance on b.relay_reference_number = tendering_planned_distance.relay_reference_number
# MAGIC       left join brokerageprod.bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC     where
# MAGIC       1 = 1 -- [[and '000']]
# MAGIC       -- and t.trailer_number is not null
# MAGIC       and b.status = 'booked'
# MAGIC       and canonical_plan_projection.mode not like '%ltl%'
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and combined is null -- Select * from use_relay_loads  limit 100
# MAGIC     Union
# MAGIC     select
# MAGIC       distinct canonical_plan_projection.mode as Mode_Filter,
# MAGIC       case
# MAGIC         when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC         else case
# MAGIC           when canonical_plan_projection.mode = 'imdl' then 'IMDL'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC       end as Modee,
# MAGIC       'TL' as Equipment,
# MAGIC       combined_loads_mine.relay_reference_number_one as relay_load,
# MAGIC       CAST(combined_loads_mine.combine_new_rrn As string) as combo_load,
# MAGIC       b.booking_id,
# MAGIC       b.booked_by_name as Carrier_Rep,
# MAGIC       carr.new_office as carr_office,
# MAGIC       cust.new_office as customer_office,
# MAGIC       b.ready_date As Ready_Date,
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as pu_appt_date,
# MAGIC       COALESCE(
# MAGIC         CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC         CAST(pp.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(b.ready_date AS TIMESTAMP)
# MAGIC       ) AS new_pu_appt,
# MAGIC       c.in_date_time :: TIMESTAMP As pickup_in,
# MAGIC       c.out_date_time :: TIMESTAMP As pickup_out,
# MAGIC       coalesce(
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as pickup_datetime,
# MAGIC       coalesce(
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as canonical_stop,
# MAGIC       coalesce(
# MAGIC         c.in_date_time :: date,
# MAGIC         c.out_date_time :: date,
# MAGIC         pss.appointment_datetime :: date,
# MAGIC         pss.window_end_datetime :: date,
# MAGIC         pss.window_start_datetime :: date,
# MAGIC         pp.appointment_datetime :: date,
# MAGIC         b.ready_date :: date
# MAGIC       ) as use_date,
# MAGIC       case
# MAGIC         when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC         else customer_lookup.master_customer_name
# MAGIC       end as mastername,
# MAGIC       r.customer_name as customer_name,
# MAGIC       z.full_name as customer_rep,
# MAGIC       coalesce(
# MAGIC         tendering_planned_distance.planned_distance_amount :: float,
# MAGIC         b.total_miles :: float
# MAGIC       ) as total_miles,
# MAGIC       upper(b.status) as status,
# MAGIC       initcap(b.first_shipper_city) as origin_city,
# MAGIC       upper(b.first_shipper_state) as origin_state,
# MAGIC       initcap(b.receiver_city) as dest_city,
# MAGIC       upper(b.receiver_state) as dest_state,
# MAGIC       b.first_shipper_zip as origin_zip,
# MAGIC       b.receiver_zip as dest_zip,
# MAGIC       o.market as market_origin,
# MAGIC       d.market AS market_dest,
# MAGIC       CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC       --dim_financial_calendar.financial_period_sorting,
# MAGIC       --dim_financial_calendar.financial_year,
# MAGIC       carrier_projection.dot_number :: string,
# MAGIC       coalesce(a.in_date_time, a.out_date_time) :: date as del_date,
# MAGIC       rolled_at :: date as rolled_at,
# MAGIC       booked_at as Booked_Date,
# MAGIC       ta.accepted_at :: string as Tendering_Date,
# MAGIC       b.bounced_at :: date as bounced_date,
# MAGIC       b.bounced_at :: date + 2 as cancelled_date,
# MAGIC       cd.in_date_time AS delivery_in,
# MAGIC       cd.out_date_time AS delivery_out,
# MAGIC       COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC       CASE
# MAGIC         WHEN date_format(
# MAGIC           cast(
# MAGIC             nullif(dp.appointment_time_local, '') as timestamp
# MAGIC           ),
# MAGIC           'HH:mm'
# MAGIC         ) IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(
# MAGIC           date_format(
# MAGIC             cast(
# MAGIC               nullif(dp.appointment_time_local, '') as timestamp
# MAGIC             ),
# MAGIC             'HH:mm'
# MAGIC           ) as interval
# MAGIC         )
# MAGIC       END AS del_appt_date,
# MAGIC       --   pickup.Days_ahead as Days_Ahead,
# MAGIC       --   pickup.prebookable as prebookable,
# MAGIC       --   papp.PickUp_Start,
# MAGIC       --   papp.PickUp_End,
# MAGIC       --   dapp.Drop_Start,
# MAGIC       --   dapp.Drop_End,
# MAGIC       b.booked_carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC       b.cargo_value_amount / 100.00 as Cargo_Value,
# MAGIC       b.total_weight as Weight,
# MAGIC       mx.max_buy :: float / 100.00 as max_buy,
# MAGIC       mr.Market_Buy_Rate,
# MAGIC       sp.quoted_price as Spot_Revenue,
# MAGIC       sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       combined_loads_mine
# MAGIC       JOIN brokerageprod.bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC       left join relay_spot_number sp on b.relay_reference_number = sp.relay_reference_number
# MAGIC       left join bronze.sourcing_max_buy_v2 mx on b.relay_reference_number = mx.relay_reference_number
# MAGIC       left join pricing_nfi_load_predictions mr on b.relay_reference_number = mr.MR_id ----- market_Rate
# MAGIC       LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number --   Left Join brokerageprod.bronze.spot_quotes spot on b.relay_reference_number = spot.load_number and spot.status_str = 'WON'
# MAGIC       left join brokerageprod.bronze.truckload_projection t on b.relay_reference_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced'
# MAGIC       Left Join last_delivery on b.relay_reference_number = last_delivery.relay_reference_number ---- Delivery date JOIN
# MAGIC       LEFT JOIN brokerageprod.bronze.delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number
# MAGIC       AND b.receiver_name :: string = dp.receiver_name :: string
# MAGIC       AND max_schedule_del.max_schedule_del = dp.scheduled_at --::timestamp --without time zone
# MAGIC       LEFT JOIN brokerageprod.bronze.planning_stop_schedule dpss ON b.relay_reference_number = dpss.relay_reference_number
# MAGIC       AND b.receiver_name :: string = dpss.stop_name :: string
# MAGIC       AND b.receiver_city :: string = dpss.locality :: string
# MAGIC       AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC       AND dpss.stop_type :: string = 'delivery' :: string
# MAGIC       AND dpss.`removed?` = false
# MAGIC       LEFT JOIN brokerageprod.bronze.canonical_stop cd ON dpss.stop_id :: string = cd.stop_id :: string
# MAGIC       AND b.receiver_city :: string = cd.locality :: string
# MAGIC       AND cd.`stale?` = false
# MAGIC       AND cd.stop_type :: string = 'delivery' :: string ---- Delivery Detail End
# MAGIC       LEFT join brokerageprod.bronze.pickup_projection pp on (
# MAGIC         b.booking_id = pp.booking_id
# MAGIC         AND b.first_shipper_name = pp.shipper_name
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join brokerageprod.bronze.planning_stop_schedule pss on (
# MAGIC         b.relay_reference_number = pss.relay_reference_number
# MAGIC         AND b.first_shipper_name = pss.stop_name
# MAGIC         AND pss.sequence_number = 1
# MAGIC         AND pss.`removed?` = 'false'
# MAGIC       )
# MAGIC       LEFT join brokerageprod.bronze.canonical_stop c on (
# MAGIC         b.booking_id = c.booking_id
# MAGIC         AND b.first_shipper_id = c.facility_id
# MAGIC         and c.`stale?` = 'false'
# MAGIC         and c.stop_type = 'pickup'
# MAGIC       )
# MAGIC       left join brokerageprod.bronze.canonical_stop a on b.booking_id = a.booking_id
# MAGIC       and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC       and upper(b.receiver_city) = upper(a.locality)
# MAGIC       and a.stop_type = 'delivery'
# MAGIC       and a.`stale?` = 'false'
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           system,
# MAGIC           max(load_number) as max_loadnumber,
# MAGIC           max(Pickup_date) as Pickup_date,
# MAGIC           max(prebookable) as prebookable,
# MAGIC           Max(days_ahead) as days_ahead
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_pickup
# MAGIC         group by
# MAGIC           system
# MAGIC       ) as pickup ON b.relay_reference_number = pickup.max_loadnumber
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp ON b.relay_reference_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON b.relay_reference_number = dapp.loadnum --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       --LEFT JOIN brokerageprod.analytics.dim_financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = dim_financial_calendar.date::date
# MAGIC       JOIN brokerageprod.bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup d ON LEFT(b.receiver_zip, 3) = d.pickup_zip
# MAGIC       left join brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join brokerageprod.bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true'
# MAGIC       left join brokerageprod.bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC       left join brokerageprod.bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join brokerageprod.bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_one = tendering_planned_distance.relay_reference_number
# MAGIC       left join brokerageprod.bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC       LEFT join brokerageprod.bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC       AND relay_users.`active?` = 'true'
# MAGIC       LEFT join brokerageprod.bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC     where
# MAGIC       1 = 1 --[[and '000']]
# MAGIC       --and t.trailer_number is not null
# MAGIC       and b.status = 'booked'
# MAGIC       and canonical_plan_projection.mode not like '%ltl%'
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held') --Select * from use_relay_loads limit 100
# MAGIC       --- 3rd update-- Delivery
# MAGIC     Union
# MAGIC     select
# MAGIC       distinct concat(canonical_plan_projection.mode, '-combo2') as Mode_Filter,
# MAGIC       case
# MAGIC         when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC         else case
# MAGIC           when canonical_plan_projection.mode = 'imdl' then 'IMDL'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC       end as Equipment,
# MAGIC       'TL' as modee,
# MAGIC       combined_loads_mine.relay_reference_number_two as relay_load,
# MAGIC       CAST(combined_loads_mine.combine_new_rrn As string) as combo_load,
# MAGIC       b.booking_id,
# MAGIC       b.booked_by_name,
# MAGIC       carr.new_office as carr_office,
# MAGIC       cust.new_office as customer_office,
# MAGIC       b.ready_date As Ready_Date,
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as pu_appt_date,
# MAGIC       COALESCE(
# MAGIC         CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC         CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC         CAST(pp.appointment_datetime AS TIMESTAMP),
# MAGIC         CAST(b.ready_date AS TIMESTAMP)
# MAGIC       ) AS new_pu_appt,
# MAGIC       c.in_date_time :: TIMESTAMP As pickup_in,
# MAGIC       c.out_date_time :: TIMESTAMP As pickup_out,
# MAGIC       coalesce(
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as pickup_datetime,
# MAGIC       coalesce(
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as canonical_stop,
# MAGIC       coalesce(
# MAGIC         c.in_date_time :: date,
# MAGIC         c.out_date_time :: date,
# MAGIC         pss.appointment_datetime :: date,
# MAGIC         pss.window_end_datetime :: date,
# MAGIC         pss.window_start_datetime :: date,
# MAGIC         pp.appointment_datetime :: date,
# MAGIC         b.ready_date :: date
# MAGIC       ) as use_date,
# MAGIC       case
# MAGIC         when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC         else customer_lookup.master_customer_name
# MAGIC       end as mastername,
# MAGIC       r.customer_name as customer_name,
# MAGIC       z.full_name as customer_rep,
# MAGIC       coalesce(
# MAGIC         tendering_planned_distance.planned_distance_amount :: float,
# MAGIC         b.total_miles :: float
# MAGIC       ) as total_miles,
# MAGIC       b.status,
# MAGIC       initcap(b.first_shipper_city) as origin_city,
# MAGIC       upper(b.first_shipper_state) as origin_state,
# MAGIC       initcap(b.receiver_city) as dest_city,
# MAGIC       upper(b.receiver_state) as dest_state,
# MAGIC       b.first_shipper_zip as origin_zip,
# MAGIC       b.receiver_zip as dest_zip,
# MAGIC       o.market as market_origin,
# MAGIC       d.market AS market_dest,
# MAGIC       CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC       --financial_calendar.financial_period_sorting,
# MAGIC       --financial_calendar.financial_year,
# MAGIC       carrier_projection.dot_number :: string,
# MAGIC       coalesce(a.in_date_time, a.out_date_time) :: date as del_date,
# MAGIC       rolled_at :: date as rolled_at,
# MAGIC       booked_at as Booked_Date,
# MAGIC       ta.accepted_at :: string as Tendering_Date,
# MAGIC       b.bounced_at :: date as bounced_date,
# MAGIC       b.bounced_at :: date + 2 as cancelled_date,
# MAGIC       cd.in_date_time AS delivery_in,
# MAGIC       cd.out_date_time AS delivery_out,
# MAGIC       COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC       CASE
# MAGIC         WHEN date_format(
# MAGIC           cast(
# MAGIC             nullif(dp.appointment_time_local, '') as timestamp
# MAGIC           ),
# MAGIC           'HH:mm'
# MAGIC         ) IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(
# MAGIC           date_format(
# MAGIC             cast(
# MAGIC               nullif(dp.appointment_time_local, '') as timestamp
# MAGIC             ),
# MAGIC             'HH:mm'
# MAGIC           ) as interval
# MAGIC         )
# MAGIC       END AS del_appt_date,
# MAGIC       --   pickup.Days_ahead as Days_Ahead,
# MAGIC       --   pickup.prebookable as prebookable,
# MAGIC       --   papp.PickUp_Start,
# MAGIC       --   papp.PickUp_End,
# MAGIC       --   dapp.Drop_Start,
# MAGIC       --   dapp.Drop_End,
# MAGIC       b.booked_carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC       b.cargo_value_amount / 100.00 as Cargo_Value,
# MAGIC       b.total_weight as Weight,
# MAGIC       mx.max_buy :: float / 100.00 as max_buy,
# MAGIC       mr.Market_Buy_Rate,
# MAGIC       sp.quoted_price as Spot_Revenue,
# MAGIC       sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       combined_loads_mine
# MAGIC       JOIN brokerageprod.bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC       left join relay_spot_number sp on b.relay_reference_number = sp.relay_reference_number
# MAGIC       left join bronze.sourcing_max_buy_v2 mx on b.relay_reference_number = mx.relay_reference_number
# MAGIC       left join pricing_nfi_load_predictions mr on b.relay_reference_number = mr.MR_id --- Market rate
# MAGIC       LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number --   Left Join brokerageprod.bronze.spot_quotes spot on b.relay_reference_number = spot.load_number and spot.status_str = 'WON'
# MAGIC       left join brokerageprod.bronze.truckload_projection t on b.relay_reference_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced'
# MAGIC       and t.trailer_number is not null
# MAGIC       Left Join last_delivery on b.relay_reference_number = last_delivery.relay_reference_number ---- Delivery date JOIN
# MAGIC       LEFT JOIN brokerageprod.bronze.delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number
# MAGIC       AND b.receiver_name :: string = dp.receiver_name :: string
# MAGIC       AND max_schedule_del.max_schedule_del = dp.scheduled_at --::timestamp --without time zone
# MAGIC       LEFT JOIN brokerageprod.bronze.planning_stop_schedule dpss ON b.relay_reference_number = dpss.relay_reference_number
# MAGIC       AND b.receiver_name :: string = dpss.stop_name :: string
# MAGIC       AND b.receiver_city :: string = dpss.locality :: string
# MAGIC       AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC       AND dpss.stop_type :: string = 'delivery' :: string
# MAGIC       AND dpss.`removed?` = false
# MAGIC       LEFT JOIN brokerageprod.bronze.canonical_stop cd ON dpss.stop_id :: string = cd.stop_id :: string
# MAGIC       AND b.receiver_city :: string = cd.locality :: string
# MAGIC       AND cd.`stale?` = false
# MAGIC       AND cd.stop_type :: string = 'delivery' :: string ---- Delivery Detail End
# MAGIC       LEFT join brokerageprod.bronze.pickup_projection pp on (
# MAGIC         b.booking_id = pp.booking_id
# MAGIC         AND b.first_shipper_name = pp.shipper_name
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join brokerageprod.bronze.planning_stop_schedule pss on (
# MAGIC         b.relay_reference_number = pss.relay_reference_number
# MAGIC         AND b.first_shipper_name = pss.stop_name
# MAGIC         AND pss.sequence_number = 1
# MAGIC         AND pss.`removed?` = 'false'
# MAGIC       )
# MAGIC       LEFT join brokerageprod.bronze.canonical_stop c on (
# MAGIC         b.booking_id = c.booking_id
# MAGIC         AND b.first_shipper_id = c.facility_id
# MAGIC         and c.`stale?` = 'false'
# MAGIC         and c.stop_type = 'pickup'
# MAGIC       )
# MAGIC       left join brokerageprod.bronze.canonical_stop a on b.booking_id = a.booking_id
# MAGIC       and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC       and upper(b.receiver_city) = upper(a.locality)
# MAGIC       and a.stop_type = 'delivery'
# MAGIC       and a.`stale?` = 'false'
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           system,
# MAGIC           max(load_number) as max_loadnumber,
# MAGIC           max(Pickup_date) as Pickup_date,
# MAGIC           max(prebookable) as prebookable,
# MAGIC           Max(days_ahead) as days_ahead
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_pickup
# MAGIC         group by
# MAGIC           system
# MAGIC       ) as pickup ON b.relay_reference_number = pickup.max_loadnumber
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp ON b.relay_reference_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON b.relay_reference_number = dapp.loadnum --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       --JOIN brokerageprod.bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC       JOIN brokerageprod.bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup d ON LEFT(b.receiver_zip, 3) = d.pickup_zip
# MAGIC       left join brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join brokerageprod.bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true'
# MAGIC       left join brokerageprod.bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC       left join brokerageprod.bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join brokerageprod.bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_two = tendering_planned_distance.relay_reference_number
# MAGIC       left join brokerageprod.bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC       LEFT join brokerageprod.bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC       AND relay_users.`active?` = 'true'
# MAGIC       LEFT join brokerageprod.bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
# MAGIC     where
# MAGIC       1 = 1 --[[and '000']]
# MAGIC       --and and t.trailer_number is not null
# MAGIC       and b.status = 'booked'
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and canonical_plan_projection.mode not like '%ltl%'
# MAGIC     union
# MAGIC     select
# MAGIC       distinct canonical_plan_projection.mode as Mode_Filter,
# MAGIC       case
# MAGIC         when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC         else case
# MAGIC           when canonical_plan_projection.mode = 'imdl' then 'IMDL'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC       end as Equipment,
# MAGIC       'LTL' as modee,
# MAGIC       e.load_number,
# MAGIC       'NO' as combined_load,
# MAGIC       null :: float,
# MAGIC       'ltl_team' as booked_by,
# MAGIC       'LTL',
# MAGIC       cust.new_office as customer_office,
# MAGIC       null :: date as ready_date,
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as planning_stop_sched,
# MAGIC       null :: date as new_pu_appt,
# MAGIC       coalesce(
# MAGIC         e.ship_date :: timestamp,
# MAGIC         c.in_date_time :: TIMESTAMP
# MAGIC       ) as pickup_in,
# MAGIC       c.out_date_time :: TIMESTAMP as pickup_out,
# MAGIC       coalesce(
# MAGIC         e.ship_date :: timestamp,
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as pickup_datetime,
# MAGIC       coalesce(
# MAGIC         e.ship_date :: timestamp,
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as canonical_stop,
# MAGIC       coalesce(
# MAGIC         e.ship_date :: date,
# MAGIC         c.in_date_time :: date,
# MAGIC         c.out_date_time :: date,
# MAGIC         pss.appointment_datetime :: date,
# MAGIC         pss.window_end_datetime :: date,
# MAGIC         pss.window_start_datetime :: date,
# MAGIC         pp.appointment_datetime :: date
# MAGIC       ) as use_date,
# MAGIC       case
# MAGIC         when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC         else customer_lookup.master_customer_name
# MAGIC       end as mastername,
# MAGIC       r.customer_name as customer_name,
# MAGIC       z.full_name as customer_rep,
# MAGIC       e.miles :: float,
# MAGIC       e.dispatch_status,
# MAGIC       initcap(e.pickup_city) as origin_city,
# MAGIC       upper(e.pickup_state) as origin_state,
# MAGIC       initcap(e.consignee_city) as dest_city,
# MAGIC       upper(e.consignee_state) as dest_state,
# MAGIC       e.pickup_zip as Pickup_Zip,
# MAGIC       e.consignee_zip as Consignee_Zip,
# MAGIC       o.market as market_origin,
# MAGIC       d.market AS market_dest,
# MAGIC       CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC       --financial_calendar.financial_period_sorting,
# MAGIC       --financial_calendar.financial_year,
# MAGIC       projection_carrier.dot_num :: string,
# MAGIC       cast(delivered_date as date) :: date as delivery_date,
# MAGIC       null :: date as rolled_out,
# MAGIC       null as Booked_Date,
# MAGIC       null :: string as Tender_Date,
# MAGIC       null :: date as bounced_date,
# MAGIC       null :: date as cancelled_date,
# MAGIC       COALESCE(
# MAGIC         e.delivered_date :: timestamp,
# MAGIC         cd.in_date_time :: timestamp
# MAGIC       ) AS delivery_in,
# MAGIC       cd.out_date_time :: timestamp AS delivery_out,
# MAGIC       COALESCE(
# MAGIC         e.delivered_date :: timestamp,
# MAGIC         cd.in_date_time :: timestamp,
# MAGIC         cd.out_date_time :: timestamp
# MAGIC       ) AS delivery_datetime,
# MAGIC       CASE
# MAGIC         WHEN date_format(
# MAGIC           cast(
# MAGIC             nullif(dp.appointment_time_local, '') as timestamp
# MAGIC           ),
# MAGIC           'HH:mm'
# MAGIC         ) IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(
# MAGIC           date_format(
# MAGIC             cast(
# MAGIC               nullif(dp.appointment_time_local, '') as timestamp
# MAGIC             ),
# MAGIC             'HH:mm'
# MAGIC           ) as interval
# MAGIC         )
# MAGIC       END AS del_appt_date,
# MAGIC       --   pickup.Days_ahead as Days_Ahead,
# MAGIC       --   pickup.prebookable as prebookable,
# MAGIC       --   papp.PickUp_Start,
# MAGIC       --   papp.PickUp_End,
# MAGIC       --   dapp.Drop_Start,
# MAGIC       --   dapp.Drop_End,
# MAGIC       e.carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC       Null AS Cargo_value,
# MAGIC       e.weight,
# MAGIC       mx.max_buy :: float / 100.00 as max_buy,
# MAGIC       mr.Market_Buy_Rate,
# MAGIC       sp.quoted_price as Spot_Revenue,
# MAGIC       sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       brokerageprod.bronze.big_export_projection e
# MAGIC       left join relay_spot_number sp on e.load_number = sp.relay_reference_number --   Left Join brokerageprod.bronze.spot_quotes spot on e.load_number = spot.load_number and spot.status_str = 'WON'
# MAGIC       left join brokerageprod.bronze.truckload_projection t on e.load_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced' --and t.trailer_number is not null
# MAGIC       LEFT JOIN max_schedule_del ON e.load_number = max_schedule_del.relay_reference_number
# MAGIC       left join pricing_nfi_load_predictions mr on e.load_number = mr.MR_id
# MAGIC       left join bronze.sourcing_max_buy_v2 mx on e.load_number = mx.relay_reference_number
# MAGIC       LEFT join brokerageprod.bronze.customer_profile_projection r on e.customer_id = r.customer_slug
# MAGIC       left join brokerageprod.bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       LEFT JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip
# MAGIC       left join brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join brokerageprod.bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true'
# MAGIC       JOIN brokerageprod.bronze.canonical_plan_projection on e.load_number = canonical_plan_projection.relay_reference_number ---- Delivery Date Start---
# MAGIC       LEFT JOIN brokerageprod.bronze.delivery_projection dp ON e.load_number = dp.relay_reference_number
# MAGIC       AND e.consignee_name = dp.receiver_name
# MAGIC       AND max_schedule_del.max_schedule_del = cast(dp.scheduled_at as timestamp)
# MAGIC       LEFT JOIN last_delivery ON e.load_number = last_delivery.relay_reference_number
# MAGIC       LEFT JOIN brokerageprod.bronze.planning_stop_schedule dpss ON e.load_number = dpss.relay_reference_number
# MAGIC       AND e.consignee_name :: string = dpss.stop_name :: string
# MAGIC       AND e.consignee_city :: string = dpss.locality :: string
# MAGIC       AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC       AND dpss.stop_type :: string = 'delivery' :: string
# MAGIC       AND dpss.`removed?` = false
# MAGIC       LEFT JOIN brokerageprod.bronze.canonical_stop cd ON dpss.stop_id :: string = cd.stop_id :: string --- Delivery Date end-----
# MAGIC       LEFT join brokerageprod.bronze.pickup_projection pp on (
# MAGIC         e.load_number = pp.relay_reference_number
# MAGIC         AND e.pickup_name = pp.shipper_name
# MAGIC         AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS?
# MAGIC         AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join brokerageprod.bronze.planning_stop_schedule pss on (
# MAGIC         e.load_number = pss.relay_reference_number
# MAGIC         AND e.pickup_name = pss.stop_name
# MAGIC         AND pss.sequence_number = 1
# MAGIC         AND pss.`removed?` = 'false'
# MAGIC       )
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           system,
# MAGIC           max(load_number) as max_loadnumber,
# MAGIC           max(Pickup_date) as Pickup_date,
# MAGIC           max(prebookable) as prebookable,
# MAGIC           Max(days_ahead) as days_ahead
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_pickup
# MAGIC         group by
# MAGIC           system
# MAGIC       ) as pickup ON e.load_number = pickup.max_loadnumber
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp ON e.load_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON e.load_number = dapp.loadnum --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       LEFT join brokerageprod.bronze.canonical_stop c on pss.stop_id = c.stop_id --JOIN brokerageprod.bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC       left join brokerageprod.bronze.projection_carrier on e.carrier_id = projection_carrier.id
# MAGIC     where
# MAGIC       1 = 1 --[[and '000']]
# MAGIC       --and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and canonical_plan_projection.mode = 'ltl'
# MAGIC       and e.customer_id NOT in ('roland', 'hain', 'deb')
# MAGIC     union
# MAGIC     select
# MAGIC       distinct 'ltl- (hain,deb,roland)',
# MAGIC       case
# MAGIC         when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC         else case
# MAGIC           when canonical_plan_projection.mode = 'imdl' then 'IMDL'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC       end as Equipment,
# MAGIC       'LTL' as modee,
# MAGIC       e.load_number,
# MAGIC       'NO' as combined_load,
# MAGIC       null :: float,
# MAGIC       'ltl_team' as booked_by,
# MAGIC       'LTL',
# MAGIC       cust.new_office as customer_office,
# MAGIC       null :: date as ready_date,
# MAGIC       --   null::date as new_pu_appt,
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as planning_stop_sched,
# MAGIC       Null :Date as new_pu_appt,
# MAGIC       coalesce(
# MAGIC         e.ship_date :: timestamp,
# MAGIC         c.in_date_time :: TIMESTAMP
# MAGIC       ) as pickup_in,
# MAGIC       c.out_date_time :: TIMESTAMP as pickup_out,
# MAGIC       coalesce(
# MAGIC         e.ship_date :: timestamp,
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as pickup_datetime,
# MAGIC       coalesce(
# MAGIC         e.ship_date :: timestamp,
# MAGIC         c.in_date_time :: TIMESTAMP,
# MAGIC         c.out_date_time :: TIMESTAMP
# MAGIC       ) as canonical_stop,
# MAGIC       coalesce(
# MAGIC         e.ship_date :: date,
# MAGIC         c.in_date_time :: date,
# MAGIC         c.out_date_time :: date,
# MAGIC         pss.appointment_datetime :: date,
# MAGIC         pss.window_end_datetime :: date,
# MAGIC         pss.window_start_datetime :: date,
# MAGIC         pp.appointment_datetime :: date
# MAGIC       ) as use_date,
# MAGIC       case
# MAGIC         when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC         else customer_lookup.master_customer_name
# MAGIC       end as mastername,
# MAGIC       r.customer_name as customer_name,
# MAGIC       z.full_name as customer_rep,
# MAGIC       e.miles :: float,
# MAGIC       e.dispatch_status,
# MAGIC       initcap(e.pickup_city) as origin_city,
# MAGIC       upper(e.pickup_state) as origin_state,
# MAGIC       initcap(e.consignee_city) as dest_city,
# MAGIC       upper(e.consignee_state) as dest_state,
# MAGIC       e.pickup_zip as Pickup_Zip,
# MAGIC       e.consignee_zip as Consignee_Zip,
# MAGIC       o.market as market_origin,
# MAGIC       d.market AS market_dest,
# MAGIC       CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC       --financial_calendar.financial_period_sorting,
# MAGIC       --financial_calendar.financial_year,
# MAGIC       projection_carrier.dot_num :: string,
# MAGIC       cast(delivered_date as date) :: date as delivery_date,
# MAGIC       null :: date as rolled_out,
# MAGIC       null as Booked_Date,
# MAGIC       null :: string as Tender_Date,
# MAGIC       null :: date as bounced_date,
# MAGIC       null :: date as cancelled_date,
# MAGIC       COALESCE(
# MAGIC         e.delivered_date :: timestamp,
# MAGIC         cd.in_date_time :: timestamp
# MAGIC       ) AS delivery_in,
# MAGIC       cd.out_date_time :: timestamp AS delivery_out,
# MAGIC       COALESCE(
# MAGIC         e.delivered_date :: timestamp,
# MAGIC         cd.in_date_time :: timestamp,
# MAGIC         cd.out_date_time :: timestamp
# MAGIC       ) AS delivery_datetime,
# MAGIC       CASE
# MAGIC         WHEN date_format(
# MAGIC           cast(
# MAGIC             nullif(dp.appointment_time_local, '') as timestamp
# MAGIC           ),
# MAGIC           'HH:mm'
# MAGIC         ) IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(
# MAGIC           date_format(
# MAGIC             cast(
# MAGIC               nullif(dp.appointment_time_local, '') as timestamp
# MAGIC             ),
# MAGIC             'HH:mm'
# MAGIC           ) as interval
# MAGIC         )
# MAGIC       END AS del_appt_date,
# MAGIC       --   pickup.Days_ahead as Days_Ahead,
# MAGIC       --   pickup.prebookable as prebookable,
# MAGIC       --   papp.PickUp_Start,
# MAGIC       --   papp.PickUp_End,
# MAGIC       --   dapp.Drop_Start,
# MAGIC       --   dapp.Drop_End,
# MAGIC       e.carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC       Null as Cargo_Value,
# MAGIC       e.weight as weight,
# MAGIC       mx.max_buy :: float / 100.00 as max_buy,
# MAGIC       mr.Market_Buy_Rate,
# MAGIC       sp.quoted_price as Spot_Revenue,
# MAGIC       sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       brokerageprod.bronze.big_export_projection e
# MAGIC       left join relay_spot_number sp on e.load_number = sp.relay_reference_number
# MAGIC       left join bronze.sourcing_max_buy_v2 mx on e.load_number = mx.relay_reference_number --    Left Join brokerageprod.bronze.spot_quotes spot on e.load_number = spot.load_number and spot.status_str = 'WON'
# MAGIC       left join brokerageprod.bronze.truckload_projection t on e.load_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced' --and t.trailer_number is not null
# MAGIC       LEFT JOIN max_schedule_del ON e.load_number = max_schedule_del.relay_reference_number
# MAGIC       left join pricing_nfi_load_predictions mr on e.load_number = mr.MR_id
# MAGIC       LEFT join brokerageprod.bronze.customer_profile_projection r on e.customer_id = r.customer_slug
# MAGIC       left join brokerageprod.bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       LEFT JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip
# MAGIC       left join brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join brokerageprod.bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true' ---- Delivery Date Start---
# MAGIC       LEFT JOIN brokerageprod.bronze.delivery_projection dp ON e.load_number = dp.relay_reference_number
# MAGIC       AND e.consignee_name = dp.receiver_name
# MAGIC       AND max_schedule_del.max_schedule_del = cast(dp.scheduled_at as timestamp)
# MAGIC       LEFT JOIN last_delivery ON e.load_number = last_delivery.relay_reference_number
# MAGIC       LEFT JOIN brokerageprod.bronze.planning_stop_schedule dpss ON e.load_number = dpss.relay_reference_number
# MAGIC       AND e.consignee_name :: string = dpss.stop_name :: string
# MAGIC       AND e.consignee_city :: string = dpss.locality :: string
# MAGIC       AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC       AND dpss.stop_type :: string = 'delivery' :: string
# MAGIC       AND dpss.`removed?` = false
# MAGIC       LEFT JOIN brokerageprod.bronze.canonical_stop cd ON dpss.stop_id :: string = cd.stop_id :: string --- Delivery Date end-----
# MAGIC       JOIN brokerageprod.bronze.canonical_plan_projection on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT join brokerageprod.bronze.pickup_projection pp on (
# MAGIC         e.load_number = pp.relay_reference_number
# MAGIC         AND e.pickup_name = pp.shipper_name
# MAGIC         AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS?
# MAGIC         AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join brokerageprod.bronze.planning_stop_schedule pss on (
# MAGIC         e.load_number = pss.relay_reference_number
# MAGIC         AND e.pickup_name = pss.stop_name
# MAGIC         AND pss.sequence_number = 1
# MAGIC         AND pss.`removed?` = 'false'
# MAGIC       )
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           system,
# MAGIC           max(load_number) as max_loadnumber,
# MAGIC           max(Pickup_date) as Pickup_date,
# MAGIC           max(prebookable) as prebookable,
# MAGIC           Max(days_ahead) as days_ahead
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_pickup
# MAGIC         group by
# MAGIC           system
# MAGIC       ) as pickup ON e.load_number = pickup.max_loadnumber
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp ON e.load_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON e.load_number = dapp.loadnum --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       LEFT join brokerageprod.bronze.canonical_stop c on pss.stop_id = c.stop_id --JOIN brokerageprod.bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC       left join brokerageprod.bronze.projection_carrier on e.carrier_id = projection_carrier.id
# MAGIC     where
# MAGIC       1 = 1 --[[and '000']]
# MAGIC       --and t.trailer_number is not null
# MAGIC       and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and canonical_plan_projection.mode = 'ltl'
# MAGIC       and e.customer_id in ('roland', 'hain', 'deb')
# MAGIC   ),
# MAGIC   total_carrier_rate as (
# MAGIC     select
# MAGIC       distinct 'non_combo' as typee,
# MAGIC       relay_reference_number :: string,
# MAGIC       sum(
# MAGIC         case
# MAGIC           when currency = 'CAD' then amount :: float * (
# MAGIC             coalesce(
# MAGIC               case
# MAGIC                 when us_to_cad = 0 then null
# MAGIC                 else canada_conversions.us_to_cad
# MAGIC               end,
# MAGIC               average_conversions.avg_cad_to_us
# MAGIC             )
# MAGIC           ) :: float
# MAGIC           else amount :: float
# MAGIC         end
# MAGIC       ) :: float / 100.00 as total_carrier_rate
# MAGIC     from
# MAGIC       brokerageprod.bronze.vendor_transaction_projection
# MAGIC       JOIN use_relay_loads on relay_reference_number = use_relay_loads.relay_load
# MAGIC       left join brokerageprod.bronze.canada_conversions on vendor_transaction_projection.incurred_at :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and `voided?` = 'false'
# MAGIC       and combined_load = 'NO'
# MAGIC     group by
# MAGIC       relay_reference_number
# MAGIC     union
# MAGIC     select
# MAGIC       distinct 'combo',
# MAGIC       combined_load,
# MAGIC       sum(
# MAGIC         case
# MAGIC           when currency = 'CAD' then amount :: float * (
# MAGIC             coalesce(
# MAGIC               case
# MAGIC                 when us_to_cad = 0 then null
# MAGIC                 else canada_conversions.us_to_cad
# MAGIC               end,
# MAGIC               average_conversions.avg_cad_to_us
# MAGIC             )
# MAGIC           ) :: float
# MAGIC           else amount :: float
# MAGIC         end
# MAGIC       ) :: float / 100.00 as total_carrier_rate
# MAGIC     from
# MAGIC       brokerageprod.bronze.vendor_transaction_projection
# MAGIC       JOIN use_relay_loads on relay_reference_number :: string = use_relay_loads.combined_load :: string
# MAGIC       left join brokerageprod.bronze.canada_conversions on vendor_transaction_projection.incurred_at :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and `voided?` = 'false'
# MAGIC       and combined_load != 'NO'
# MAGIC     group by
# MAGIC       combined_load
# MAGIC   ),
# MAGIC   invoicing_cred as (
# MAGIC     select
# MAGIC       distinct relay_reference_number,
# MAGIC       sum(
# MAGIC         case
# MAGIC           when currency = 'CAD' then total_amount :: float * (
# MAGIC             coalesce(
# MAGIC               case
# MAGIC                 when us_to_cad = 0 then null
# MAGIC                 else canada_conversions.us_to_cad
# MAGIC               end,
# MAGIC               average_conversions.avg_cad_to_us
# MAGIC             )
# MAGIC           ) :: float
# MAGIC           else total_amount :: float
# MAGIC         end
# MAGIC       ) :: float / 100.00 as invoicing_cred
# MAGIC     from
# MAGIC       brokerageprod.bronze.invoicing_credits
# MAGIC       JOIN use_relay_loads on relay_reference_number = use_relay_loads.relay_load
# MAGIC       left join brokerageprod.bronze.canada_conversions on invoicing_credits.credited_at :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1
# MAGIC     where
# MAGIC       1 = 1
# MAGIC     group by
# MAGIC       relay_reference_number
# MAGIC   ),
# MAGIC   total_cust_rate as (
# MAGIC     select
# MAGIC       distinct m.relay_reference_number :: string,
# MAGIC       coalesce(invoicing_cred.invoicing_cred, 0) as credit_amt,
# MAGIC       (
# MAGIC         sum(
# MAGIC           case
# MAGIC             when m.currency = 'CAD' then amount :: float * (
# MAGIC               coalesce(
# MAGIC                 case
# MAGIC                   when us_to_cad = 0 then null
# MAGIC                   else canada_conversions.us_to_cad
# MAGIC                 end,
# MAGIC                 average_conversions.avg_cad_to_us
# MAGIC               )
# MAGIC             ) :: float
# MAGIC             else amount :: float
# MAGIC           end
# MAGIC         ) :: float / 100.00
# MAGIC       ) - coalesce(invoicing_cred.invoicing_cred, 0) as total_cust_rate
# MAGIC     from
# MAGIC       brokerageprod.bronze.moneying_billing_party_transaction m
# MAGIC       JOIN use_relay_loads on m.relay_reference_number = use_relay_loads.relay_load
# MAGIC       left join brokerageprod.bronze.canada_conversions on m.incurred_at :: date = canada_conversions.ship_date :: date
# MAGIC       LEFT join invoicing_cred on m.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC       join average_conversions on 1 = 1
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and m.`voided?` = 'false'
# MAGIC     group by
# MAGIC       m.relay_reference_number,
# MAGIC       invoicing_cred
# MAGIC   ),
# MAGIC   new_customer_money as (
# MAGIC     select
# MAGIC       distinct moneying_billing_party_transaction.relay_reference_number as loadnum,
# MAGIC       sum(amount) filter (
# MAGIC         where
# MAGIC           charge_code = 'linehaul'
# MAGIC       ) :: float / 100 as linehaul_cust_money,
# MAGIC       sum(amount) filter (
# MAGIC         where
# MAGIC           charge_code = 'fuel_surcharge'
# MAGIC       ) :: float / 100 as fuel_cust_money,
# MAGIC       sum(amount) filter (
# MAGIC         where
# MAGIC           charge_code not in ('fuel_surcharge', 'linehaul')
# MAGIC       ) :: float / 100 as acc_cust_money,
# MAGIC       sum(amount) :: float / 100 as total_cust_amount,
# MAGIC       sum(distinct invoicing_credits.total_amount) :: float / 100 as inv_cred_amt
# MAGIC     from
# MAGIC       brokerageprod.bronze.moneying_billing_party_transaction
# MAGIC       left join brokerageprod.bronze.invoicing_credits on moneying_billing_party_transaction.relay_reference_number :: float = invoicing_credits.relay_reference_number :: float
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and "voided?" = 'false'
# MAGIC     group by
# MAGIC       moneying_billing_party_transaction.relay_reference_number
# MAGIC   ),
# MAGIC   invoiced_amts as (
# MAGIC     select
# MAGIC       distinct relay_reference_number,
# MAGIC       min(invoiced_at :: date) as invoiced_at,
# MAGIC       case
# MAGIC         when (sum(amount) :: float / 100) = 0 then 0
# MAGIC         else (sum(amount) :: float / 100)
# MAGIC       end - case
# MAGIC         when new_customer_money.inv_cred_amt is null then 0
# MAGIC         else new_customer_money.inv_cred_amt :: float
# MAGIC       end as total_recievables,
# MAGIC       (
# MAGIC         sum(amount) filter (
# MAGIC           where
# MAGIC             "invoiced?" = 'true'
# MAGIC         ) :: float / 100
# MAGIC       ) :: float - case
# MAGIC         when new_customer_money.inv_cred_amt is null then 0
# MAGIC         else new_customer_money.inv_cred_amt :: float
# MAGIC       end as invoiced_amt,
# MAGIC       sum(amount) filter (
# MAGIC         where
# MAGIC           "invoiced?" = 'false'
# MAGIC       ) :: float / 100 as non_invoiced_amt
# MAGIC     from
# MAGIC       brokerageprod.bronze.moneying_billing_party_transaction
# MAGIC       left join new_customer_money on moneying_billing_party_transaction.relay_reference_number = new_customer_money.loadnum
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and "voided?" = 'false'
# MAGIC     group by
# MAGIC       relay_reference_number,
# MAGIC       new_customer_money.inv_cred_amt
# MAGIC   ),
# MAGIC   invoice_details as (
# MAGIC     select
# MAGIC       distinct relay_reference_number,
# MAGIC       sum(amount) :: float / 100.0 as invoice_amt,
# MAGIC       sum(amount) filter (
# MAGIC         where
# MAGIC           `invoiced?` = 'true'
# MAGIC       ) :: float / 100.0 as invoiced,
# MAGIC       sum(amount) filter (
# MAGIC         where
# MAGIC           `invoiced?` = 'false'
# MAGIC       ) :: float / 100.0 as not_invoiced
# MAGIC     from
# MAGIC       brokerageprod.bronze.moneying_billing_party_transaction
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and `voided?` = 'false'
# MAGIC     group by
# MAGIC       relay_reference_number
# MAGIC   ) -------------------- Carrier Adding the Linehaul, Fuelsurcharge and Other expoense CTE ----------------------------------------------
# MAGIC ,
# MAGIC   carrier_charges_one as (
# MAGIC     select
# MAGIC       distinct relay_reference_number,
# MAGIC       incurred_at,
# MAGIC       coalesce(
# MAGIC         case
# MAGIC           when currency = 'USD' then sum(amount) filter (
# MAGIC             where
# MAGIC               charge_code = 'linehaul'
# MAGIC           ) :: float / 100.00
# MAGIC           else (
# MAGIC             sum(amount) filter (
# MAGIC               where
# MAGIC                 charge_code = 'linehaul'
# MAGIC             ) :: float / 100.00
# MAGIC           ) * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         end,
# MAGIC         0
# MAGIC       ) as carr_lhc,
# MAGIC       coalesce(
# MAGIC         case
# MAGIC           when currency = 'USD' then sum(amount) filter (
# MAGIC             where
# MAGIC               charge_code = 'fuel_surcharge'
# MAGIC           ) :: float / 100.00
# MAGIC           else (
# MAGIC             sum(amount) filter (
# MAGIC               where
# MAGIC                 charge_code = 'fuel_surcharge'
# MAGIC             ) :: float / 100.00
# MAGIC           ) * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         end,
# MAGIC         0
# MAGIC       ) as carr_fsc,
# MAGIC       coalesce(
# MAGIC         case
# MAGIC           when currency = 'USD' then sum(amount) filter (
# MAGIC             where
# MAGIC               charge_code not in ('fuel_surcharge', 'linehaul')
# MAGIC           ) :: float / 100.00
# MAGIC           else (
# MAGIC             sum(amount) filter (
# MAGIC               where
# MAGIC                 charge_code not in ('fuel_surcharge', 'linehaul')
# MAGIC             ) :: float / 100.00
# MAGIC           ) * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         end,
# MAGIC         0
# MAGIC       ) as carr_acc
# MAGIC     from
# MAGIC       brokerageprod.bronze.vendor_transaction_projection
# MAGIC       left join brokerageprod.bronze.canada_conversions on vendor_transaction_projection.incurred_at :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and `voided?` = 'false'
# MAGIC     group by
# MAGIC       relay_reference_number,
# MAGIC       currency,
# MAGIC       us_to_cad,
# MAGIC       avg_cad_to_us,
# MAGIC       incurred_at
# MAGIC   ),
# MAGIC   carrier_charges as (
# MAGIC     select
# MAGIC       distinct relay_reference_number,
# MAGIC       sum(carr_lhc) as carr_lhc,
# MAGIC       sum(carr_fsc) as carr_fsc,
# MAGIC       sum(carr_acc) as carr_acc,
# MAGIC       sum(carr_acc) :: float + sum(carr_fsc) :: float + sum(carr_lhc) :: float as total_carrier_cost
# MAGIC     from
# MAGIC       carrier_charges_one
# MAGIC     group by
# MAGIC       relay_reference_number
# MAGIC   ),
# MAGIC   ------------------------------ Customer Linehual , Fuel Surcharge and Accessorial Rate CTE --------------------------------
# MAGIC   new_customer_money_two as (
# MAGIC     select
# MAGIC       distinct incurred_at,
# MAGIC       `voided?`,
# MAGIC       moneying_billing_party_transaction.currency as cust_currency_type,
# MAGIC       moneying_billing_party_transaction.relay_reference_number as loadnum,
# MAGIC       charge_code,
# MAGIC       amount :: float / 100 as amount
# MAGIC     from
# MAGIC       brokerageprod.bronze.moneying_billing_party_transaction
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and `voided?` = 'false'
# MAGIC     order by
# MAGIC       loadnum
# MAGIC   ),
# MAGIC   invoicing_credits as (
# MAGIC     select
# MAGIC       distinct relay_reference_number,
# MAGIC       sum(total_amount) filter (
# MAGIC         where
# MAGIC           currency = 'USD'
# MAGIC       ) :: float / 100.00 as usd_cred,
# MAGIC       sum(total_amount) filter (
# MAGIC         where
# MAGIC           currency = 'CAD'
# MAGIC       ) :: float / 100.00 as cad_cred
# MAGIC     from
# MAGIC       brokerageprod.bronze.invoicing_credits
# MAGIC     group by
# MAGIC       relay_reference_number
# MAGIC   ),
# MAGIC   customer_money_final as (
# MAGIC     select
# MAGIC       distinct loadnum,
# MAGIC       case
# MAGIC         when sum(amount) filter (
# MAGIC           where
# MAGIC             cust_currency_type = 'USD'
# MAGIC         ) is null then 0
# MAGIC         else sum(amount) filter (
# MAGIC           where
# MAGIC             cust_currency_type = 'USD'
# MAGIC         )
# MAGIC       end as total_usd,
# MAGIC       case
# MAGIC         when sum(amount) filter (
# MAGIC           where
# MAGIC             cust_currency_type = 'CAD'
# MAGIC         ) is null then 0
# MAGIC         else sum(amount) filter (
# MAGIC           where
# MAGIC             cust_currency_type = 'CAD'
# MAGIC         )
# MAGIC       end as total_cad,
# MAGIC       case
# MAGIC         when sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'USD'
# MAGIC             and charge_code = 'linehaul'
# MAGIC         ) is null then 0
# MAGIC         else sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'USD'
# MAGIC             and charge_code = 'linehaul'
# MAGIC         )
# MAGIC       end as usd_lhc,
# MAGIC       case
# MAGIC         when sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'USD'
# MAGIC             and charge_code = 'fuel_surcharge'
# MAGIC         ) is null then 0
# MAGIC         else sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'USD'
# MAGIC             and charge_code = 'fuel_surcharge'
# MAGIC         )
# MAGIC       end as usd_fsc,
# MAGIC       case
# MAGIC         when sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'USD'
# MAGIC             and charge_code not in ('linehaul', 'fuel_surcharge')
# MAGIC         ) is null then 0
# MAGIC         else sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'USD'
# MAGIC             and charge_code not in ('linehaul', 'fuel_surcharge')
# MAGIC         )
# MAGIC       end as usd_acc,
# MAGIC       case
# MAGIC         when sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'CAD'
# MAGIC             and charge_code = 'linehaul'
# MAGIC         ) is null then 0
# MAGIC         else sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'CAD'
# MAGIC             and charge_code = 'linehaul'
# MAGIC         )
# MAGIC       end as cad_lhc,
# MAGIC       case
# MAGIC         when sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'CAD'
# MAGIC             and charge_code = 'fuel_surcharge'
# MAGIC         ) is null then 0
# MAGIC         else sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'CAD'
# MAGIC             and charge_code = 'fuel_surcharge'
# MAGIC         )
# MAGIC       end as cad_fsc,
# MAGIC       case
# MAGIC         when sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'CAD'
# MAGIC             and charge_code not in ('linehaul', 'fuel_surcharge')
# MAGIC         ) is null then 0
# MAGIC         else sum(amount) filter (
# MAGIC           where
# MAGIC             1 = 1
# MAGIC             and cust_currency_type = 'CAD'
# MAGIC             and charge_code not in ('linehaul', 'fuel_surcharge')
# MAGIC         )
# MAGIC       end as cad_acc,
# MAGIC       case
# MAGIC         when invoicing_credits.usd_cred is null then 0
# MAGIC         else invoicing_credits.usd_cred
# MAGIC       end as usd_cred,
# MAGIC       case
# MAGIC         when invoicing_credits.cad_cred is null then 0
# MAGIC         else invoicing_credits.cad_cred
# MAGIC       end as cad_cred
# MAGIC     from
# MAGIC       new_customer_money_two
# MAGIC       left join invoicing_credits on loadnum = invoicing_credits.relay_reference_number
# MAGIC     group by
# MAGIC       loadnum,
# MAGIC       usd_cred,
# MAGIC       cad_cred
# MAGIC   ),
# MAGIC   aljex_data as (
# MAGIC     select
# MAGIC       p1.id,
# MAGIC       case
# MAGIC         when aljex_user_report_listing.full_name is null then key_c_user
# MAGIC         else aljex_user_report_listing.full_name
# MAGIC       end as key_c_user,
# MAGIC       case
# MAGIC         when customer_lookup.master_customer_name is null then p2.shipper
# MAGIC         else customer_lookup.master_customer_name
# MAGIC       end as mastername,
# MAGIC       p2.shipper as customer_name,
# MAGIC       case
# MAGIC         when z.full_name is null then p2.srv_rep
# MAGIC         else z.full_name
# MAGIC       end as customer_rep,
# MAGIC       coalesce(p1.arrive_pickup_date, p1.loaded_date, p1.pickup_date) as use_date,
# MAGIC       p2.weight as Weight,
# MAGIC       CASE
# MAGIC         WHEN carrier_line_haul LIKE '%:%' THEN 0.00
# MAGIC         WHEN carrier_line_haul RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_line_haul AS DOUBLE)
# MAGIC         ELSE 0.00
# MAGIC       END + CASE
# MAGIC         WHEN carrier_accessorial_rate1 LIKE '%:%' THEN 0.00
# MAGIC         WHEN carrier_accessorial_rate1 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate1 AS DOUBLE)
# MAGIC         ELSE 0.00
# MAGIC       END + CASE
# MAGIC         WHEN carrier_accessorial_rate2 LIKE '%:%' THEN 0.00
# MAGIC         WHEN carrier_accessorial_rate2 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate2 AS DOUBLE)
# MAGIC         ELSE 0.00
# MAGIC       END + CASE
# MAGIC         WHEN carrier_accessorial_rate3 LIKE '%:%' THEN 0.00
# MAGIC         WHEN carrier_accessorial_rate3 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate3 AS DOUBLE)
# MAGIC         ELSE 0.00
# MAGIC       END + CASE
# MAGIC         WHEN carrier_accessorial_rate4 LIKE '%:%' THEN 0.00
# MAGIC         WHEN carrier_accessorial_rate4 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate4 AS DOUBLE)
# MAGIC         ELSE 0.00
# MAGIC       END + CASE
# MAGIC         WHEN carrier_accessorial_rate5 LIKE '%:%' THEN 0.00
# MAGIC         WHEN carrier_accessorial_rate5 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate5 AS DOUBLE)
# MAGIC         ELSE 0.00
# MAGIC       END + CASE
# MAGIC         WHEN carrier_accessorial_rate6 LIKE '%:%' THEN 0.00
# MAGIC         WHEN carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate6 AS DOUBLE)
# MAGIC         ELSE 0.00
# MAGIC       END + CASE
# MAGIC         WHEN carrier_accessorial_rate7 LIKE '%:%' THEN 0.00
# MAGIC         WHEN carrier_accessorial_rate7 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate7 AS DOUBLE)
# MAGIC         ELSE 0.00
# MAGIC       END + CASE
# MAGIC         WHEN carrier_accessorial_rate8 LIKE '%:%' THEN 0.00
# MAGIC         WHEN carrier_accessorial_rate8 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate8 AS DOUBLE)
# MAGIC         ELSE 0.00
# MAGIC       END AS final_carrier_rate,
# MAGIC       case
# MAGIC         when carrier_line_haul like '%:%' then 0.00
# MAGIC         when carrier_line_haul RLIKE '\\d+(\\.[0-9]+)?' then carrier_line_haul :: numeric
# MAGIC         else 0.00
# MAGIC       end as carrier_lhc,
# MAGIC       case
# MAGIC         when accessorial1 like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate1 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate1 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial2 like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate2 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate2 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial3 like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate3 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate3 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial4 like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate4 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate4 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial5 like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate5 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate5 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial6 like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate6 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate6 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial7 like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate7 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate7 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial8 like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate8 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate8 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end as carrier_fuel,
# MAGIC       case
# MAGIC         when accessorial1 NOT like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate1 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate1 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial2 NOT like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate2 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate2 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial3 NOT like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate3 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate3 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial4 NOT like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate4 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate4 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial5 NOT like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate5 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate5 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial6 NOT like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate6 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate6 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial7 NOT like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate7 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate7 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial8 NOT like 'FUE%' then case
# MAGIC           when carrier_accessorial_rate8 like '%:%' then 0.00
# MAGIC           when carrier_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate8 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end as carrier_acc,
# MAGIC       case
# MAGIC         when accessorial1 like 'FUE%' then case
# MAGIC           when customer_accessorial_rate1 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate1 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial2 like 'FUE%' then case
# MAGIC           when customer_accessorial_rate2 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate2 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial3 like 'FUE%' then case
# MAGIC           when customer_accessorial_rate3 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate3 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial4 like 'FUE%' then case
# MAGIC           when customer_accessorial_rate4 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate4 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial5 like 'FUE%' then case
# MAGIC           when customer_accessorial_rate5 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate5 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial6 like 'FUE%' then case
# MAGIC           when customer_accessorial_rate6 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate6 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial7 like 'FUE%' then case
# MAGIC           when customer_accessorial_rate7 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate7 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial8 like 'FUE%' then case
# MAGIC           when customer_accessorial_rate8 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate8 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end as customer_fuel,
# MAGIC       case
# MAGIC         when accessorial1 not like 'FUE%' then case
# MAGIC           when customer_accessorial_rate1 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate1 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial2 not like 'FUE%' then case
# MAGIC           when customer_accessorial_rate2 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate2 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial3 not like 'FUE%' then case
# MAGIC           when customer_accessorial_rate3 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate3 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial4 not like 'FUE%' then case
# MAGIC           when customer_accessorial_rate4 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate4 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial5 not like 'FUE%' then case
# MAGIC           when customer_accessorial_rate5 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate5 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial6 not like 'FUE%' then case
# MAGIC           when customer_accessorial_rate6 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate6 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial7 not like 'FUE%' then case
# MAGIC           when customer_accessorial_rate7 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate7 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end + case
# MAGIC         when accessorial8 not like 'FUE%' then case
# MAGIC           when customer_accessorial_rate8 like '%:%' then 0.00
# MAGIC           when customer_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?' then customer_accessorial_rate8 :: numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC         else 0.00
# MAGIC       end as customer_acc,
# MAGIC       case
# MAGIC         when p1.office not in (
# MAGIC           '10',
# MAGIC           '34',
# MAGIC           '51',
# MAGIC           '54',
# MAGIC           '61',
# MAGIC           '62',
# MAGIC           '63',
# MAGIC           '64',
# MAGIC           '74'
# MAGIC         ) then 'USD'
# MAGIC         else coalesce(
# MAGIC           cad_currency.currency_type,
# MAGIC           aljex_customer_profiles.cust_country,
# MAGIC           'CAD'
# MAGIC         )
# MAGIC       end as cust_curr,
# MAGIC       case
# MAGIC         when c.name like '%CAD' then 'CAD'
# MAGIC         when c.name like 'CANADIAN R%' then 'CAD'
# MAGIC         when c.name like '%CAN' then 'CAD'
# MAGIC         when c.name like '%(CAD)%' then 'CAD'
# MAGIC         when c.name like '%-C' then 'CAD'
# MAGIC         when c.name like '%- C%' then 'CAD'
# MAGIC         when canada_carriers.canada_dot is not null then 'CAD'
# MAGIC         ELSE 'USD'
# MAGIC       end as carrier_curr,
# MAGIC       case
# MAGIC         when canada_conversions.conversion is null then average_conversions.avg_us_to_cad
# MAGIC         else canada_conversions.conversion
# MAGIC       end as conversion_rate,
# MAGIC       new_office_lookup_w_tgt.new_office as customer_office,
# MAGIC       case
# MAGIC         when p1.key_c_user = 'IMPORT' then case
# MAGIC           when customer_lookup.master_customer_name = 'ALTMAN PLANTS' then 'LAX'
# MAGIC           else 'DRAY'
# MAGIC         end
# MAGIC         when p1.key_c_user = 'EDILCR' then 'LTL'
# MAGIC         when p1.key_c_user is null then new_office_lookup_w_tgt.new_office
# MAGIC         else car.new_office
# MAGIC       end as carr_office,
# MAGIC       case
# MAGIC         when p1.equipment like '%LTL%' then 'LTL'
# MAGIC         else 'TL'
# MAGIC       end as tl_or_ltl,
# MAGIC       case
# MAGIC         when aljex_mode_types.equipment_mode = 'F' then 'Flatbed'
# MAGIC         when aljex_mode_types.equipment_mode is null then 'Dry Van'
# MAGIC         when aljex_mode_types.equipment_mode = 'R' then 'Reefer'
# MAGIC         when aljex_mode_types.equipment_mode = 'V' then 'Dry Van'
# MAGIC         when aljex_mode_types.equipment_mode = 'PORTD' then 'PORTD'
# MAGIC         when aljex_mode_types.equipment_mode = 'POWER ONLY' then 'POWER ONLY'
# MAGIC         when aljex_mode_types.equipment_mode = 'IMDL' then 'IMDL'
# MAGIC         else case
# MAGIC           when p1.equipment = 'RLTL' then 'Reefer'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC       end as modee,
# MAGIC       invoice_total,
# MAGIC       p1.miles,
# MAGIC       p2.status,
# MAGIC       initcap(p1.origin_city) as origin_city,
# MAGIC       upper(p1.origin_state) as origin_state,
# MAGIC       initcap(p1.dest_city) as dest_city,
# MAGIC       upper(p1.dest_state) as dest_state,
# MAGIC       pickup_zip_code as origin_zip,
# MAGIC       consignee_zip_code as dest_zip,
# MAGIC       o.market as market_origin,
# MAGIC       d.market AS market_dest,
# MAGIC       CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC       --financial_calendar.financial_period_sorting,
# MAGIC       --financial_calendar.financial_year,
# MAGIC       c.dot_num :: string,
# MAGIC       p1.delivery_date :: date as delivery_date,
# MAGIC       null :: date as bounced_date,
# MAGIC       null :: date as rolled_date,
# MAGIC       p1.key_c_date as Booked_Date,
# MAGIC       p2.tag_created_by :: string as Tender_Date,
# MAGIC       null :: date as cancelled_date,
# MAGIC       pickup.Days_ahead as Days_Ahead,
# MAGIC       pickup.prebookable as prebookable,
# MAGIC       p1.Pickup_Date as PickUp_Start,
# MAGIC       p1.Pickup_Date as PickUp_End,
# MAGIC       p1.Delivery_Date as Drop_Start,
# MAGIC       p1.Delivery_Date as Drop_End,
# MAGIC       P1.pickup_Appt_Date as Pickup_Appointment_Date,
# MAGIC       P1.pickup_Appt_Date as New_Pickup_Appointment_Date,
# MAGIC       c.name as carrier_name,
# MAGIC       p2.sales_rep as salesrep,
# MAGIC       CASE
# MAGIC         WHEN p2.value REGEXP '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p2.value AS DECIMAL(10, 2))
# MAGIC         ELSE CAST(0.00 AS DECIMAL(10, 2))
# MAGIC       END AS cargo_value,
# MAGIC       p2.trailer_number as Trailer_Num,
# MAGIC       mr.Market_Buy_Rate,
# MAGIC       sp.quoted_price as Spot_Revenue,
# MAGIC       sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       brokerageprod.bronze.projection_load_1 p1
# MAGIC       Left Join brokerageprod.bronze.projection_load_2 p2 on p1.id = p2.id
# MAGIC       left join aljex_spot_loads sp on p1.id = sp.id ---join brokerageprod.analytics.financial_calendar on p.pickup_date::date = financial_calendar.date::date
# MAGIC       left join pricing_nfi_load_predictions mr on p1.id = mr.MR_id
# MAGIC       join brokerageprod.bronze.new_office_lookup_w_tgt on p1.office = new_office_lookup_w_tgt.old_office
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup o ON LEFT(p1.pickup_zip_code, 3) = o.pickup_zip
# MAGIC       LEFT JOIN brokerageprod.bronze.market_lookup d ON LEFT(p1.consignee_zip_code, 3) = d.pickup_zip
# MAGIC       left join brokerageprod.bronze.cad_currency on p1.id :: string = cad_currency.pro_number :: string
# MAGIC       left join brokerageprod.bronze.aljex_customer_profiles on p2.customer_id :: string = aljex_customer_profiles.cust_id :: string
# MAGIC       left join brokerageprod.bronze.projection_carrier c on p1.carrier_id = c.id
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           system,
# MAGIC           max(load_number) as max_loadnumber,
# MAGIC           max(Pickup_date) as Pickup_date,
# MAGIC           max(prebookable) as prebookable,
# MAGIC           Max(days_ahead) as days_ahead
# MAGIC         from
# MAGIC           brokerageprod.bronze.load_pickup
# MAGIC         group by
# MAGIC           system
# MAGIC       ) as pickup ON p1.id = pickup.max_loadnumber
# MAGIC       left join brokerageprod.bronze.canada_carriers on p1.carrier_id :: string = canada_carriers.canada_dot :: string
# MAGIC       left join brokerageprod.bronze.customer_lookup on left(p2.shipper, 32) = left(customer_lookup.aljex_customer_name, 32)
# MAGIC       left join brokerageprod.bronze.aljex_mode_types on p1.equipment = brokerageprod.bronze.aljex_mode_types.equipment_type
# MAGIC       join average_conversions on 1 = 1
# MAGIC       left join brokerageprod.bronze.canada_conversions on p1.pickup_date :: date = canada_conversions.ship_date :: date
# MAGIC       left join brokerageprod.bronze.aljex_user_report_listing on p1.key_c_user = aljex_user_report_listing.aljex_id
# MAGIC       left join brokerageprod.bronze.aljex_user_report_listing z on p2.srv_rep = z.aljex_id
# MAGIC       left join brokerageprod.bronze.relay_users on aljex_user_report_listing.full_name = relay_users.full_name
# MAGIC       left join brokerageprod.bronze.new_office_lookup_w_tgt car on coalesce(
# MAGIC         aljex_user_report_listing.pnl_code,
# MAGIC         relay_users.office_id
# MAGIC       ) = car.old_office
# MAGIC     where
# MAGIC       1 = 1 --[[and '000']]
# MAGIC       and p2.status not in ('OPEN', 'ASSIGNED')
# MAGIC       and p2.status not like '%VOID%'
# MAGIC       and p1.key_c_user is not null
# MAGIC   ),
# MAGIC   ----------------------------------------------------------------- System Union Code ----------------------------------------------------------
# MAGIC   System_Union as (
# MAGIC     Select
# MAGIC       -------- Relay ----------------------------------------------------------Relay----------------------- Changing this now ---------------------
# MAGIC       distinct relay_load as Load_Number,
# MAGIC       --0.5 as Load_Counter,
# MAGIC       use_date as Ship_Date,
# MAGIC       dim_date.period_year,
# MAGIC       dim_date.Month_Year,
# MAGIC       Equipment as Mode,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else modee
# MAGIC       End as Equipment,
# MAGIC       --modee as Mode,
# MAGIC       --mode as Equipment,
# MAGIC       date_range_two.week_num as Week_Num,
# MAGIC       Case
# MAGIC         when use_relay_loads.customer_office = 'TGT' then 'PHI'
# MAGIC         else use_relay_loads.customer_office
# MAGIC       end as customer_office,
# MAGIC       --customer_office as Customer_Office,
# MAGIC       Case
# MAGIC         when use_relay_loads.carr_office = 'TGT' then 'PHI'
# MAGIC         else use_relay_loads.carr_office
# MAGIC       end as Carrier_Office,
# MAGIC       --carr_office as Carrier_Office,
# MAGIC       --'Shipper' as Office_Type,
# MAGIC       use_relay_loads.Carrier_Rep as Booked_By,
# MAGIC       --carr_office as Carr_Office,
# MAGIC       --financial_period_sorting as "Financial Period",
# MAGIC       --financial_year as "Financial Yr",
# MAGIC       use_relay_loads.mastername as customer_master,
# MAGIC       use_relay_loads.customer_name,
# MAGIC       use_relay_loads.customer_rep,
# MAGIC       salesrep,
# MAGIC       status as load_status,
# MAGIC       use_relay_loads.origin_city as Origin_City,
# MAGIC       use_relay_loads.origin_state as Origin_State,
# MAGIC       use_relay_loads.dest_city as Dest_City,
# MAGIC       use_relay_loads.dest_state as Dest_State,
# MAGIC       use_relay_loads.origin_zip as Origin_Zip,
# MAGIC       use_relay_loads.dest_zip as Dest_Zip,
# MAGIC       CONCAT(
# MAGIC         INITCAP(use_relay_loads.origin_city),
# MAGIC         ',',
# MAGIC         use_relay_loads.origin_state,
# MAGIC         '>',
# MAGIC         INITCAP(use_relay_loads.dest_city),
# MAGIC         ',',
# MAGIC         use_relay_loads.dest_state
# MAGIC       ) AS load_lane,
# MAGIC       use_relay_loads.market_origin as Market_origin,
# MAGIC       use_relay_loads.market_dest as Market_Deat,
# MAGIC       use_relay_loads.market_lane as Market_Lane,
# MAGIC       use_relay_loads.del_date as Delivery_Date,
# MAGIC       use_relay_loads.rolled_at as rolled_date,
# MAGIC       use_relay_loads.Booked_Date as Booked_Date,
# MAGIC       use_relay_loads.Tendering_Date as Tendered_Date,
# MAGIC       use_relay_loads.bounced_date as bounced_date,
# MAGIC       use_relay_loads.cancelled_date as Cancelled_Date,
# MAGIC       pu_appt_date as Pickup_Appointment_Date,
# MAGIC       new_pu_appt as New_Pickup_Appointment_Date,
# MAGIC       pickup_in as Pickup_in,
# MAGIC       pickup_out as Pickup_Out,
# MAGIC       pickup_datetime as pickup_datetime,
# MAGIC       case
# MAGIC         when pickup_datetime :: date > COALESCE(pu_appt_date :: date, ready_date :: date) then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_pu,
# MAGIC       delivery_in as Delivery_In,
# MAGIC       delivery_out as Delivery_out,
# MAGIC       delivery_datetime as Delivery_Datetime,
# MAGIC       case
# MAGIC         when delivery_datetime :: date > del_appt_date :: date then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_del,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN use_relay_loads.customer_office <> use_relay_loads.carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when use_relay_loads.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       use_relay_loads.dot_num as DOT_Num,
# MAGIC       use_relay_loads.carrier_name as Carrier,
# MAGIC       use_relay_loads.total_miles :: float as Miles,
# MAGIC       total_cust_rate.total_cust_rate :: float as Revenue,
# MAGIC       total_carrier_rate.total_carrier_rate :: float as Expense,
# MAGIC       (
# MAGIC         coalesce(total_cust_rate.total_cust_rate, 0) :: float - coalesce(total_carrier_rate.total_carrier_rate, 0) :: float
# MAGIC       ) as Margin,
# MAGIC       CASE
# MAGIC         WHEN new_pu_appt IS NOT NULL
# MAGIC         AND Booked_Date IS NOT NULL THEN DATEDIFF(day, Booked_Date, new_pu_appt)
# MAGIC         ELSE NULL
# MAGIC       END AS Lead_Time,
# MAGIC       invoice_details.invoice_amt as invoiceable_amt,
# MAGIC       invoice_details.invoiced as amt_already_invoiced,
# MAGIC       invoice_details.not_invoiced as outstanindg_amt,
# MAGIC       carrier_charges.carr_lhc as Linehaul,
# MAGIC       carrier_charges.carr_fsc as Fuel_Surcharge,
# MAGIC       carrier_charges.carr_acc as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_lhc,
# MAGIC       use_relay_loads.Trailer_Num,
# MAGIC       use_relay_loads.Cargo_Value,
# MAGIC       use_relay_loads.Weight,
# MAGIC       use_relay_loads.max_buy,
# MAGIC       use_relay_loads.market_buy_rate,
# MAGIC       use_relay_loads.Spot_Revenue,
# MAGIC       use_relay_loads.Spot_Margin,
# MAGIC       d.Is_Awarded,
# MAGIC       'RELAY' as TMS_System
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number
# MAGIC       left join total_carrier_rate on use_relay_loads.relay_load = total_carrier_rate.relay_reference_number
# MAGIC       left join brokerageprod.bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join brokerageprod.bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1
# MAGIC       left join invoice_details on use_relay_loads.relay_load = invoice_details.relay_reference_number
# MAGIC       left join brokerageprod.analytics.Dim_date dim_date on dim_date.calendar_date = use_relay_loads.use_date
# MAGIC       left join brokerageprod.gold.fact_awards_summary d on use_relay_loads.relay_load = d.Load_ID and d.Is_deleted = 0 and d.Is_Awarded = 'Awarded'
# MAGIC       left join carrier_charges on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC       and combined_load = 'NO' --UNION
# MAGIC     Union
# MAGIC     select
# MAGIC       distinct ---- Shipper - Hain Roloadn and Deb
# MAGIC       relay_load as relay_ref_num,
# MAGIC       --0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       dim_date.period_year,
# MAGIC       dim_date.Month_Year,
# MAGIC       Equipment as Mode,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else modee
# MAGIC       End as Equipment,
# MAGIC       date_range_two.week_num,
# MAGIC       Case
# MAGIC         when use_relay_loads.customer_office = 'TGT' then 'PHI'
# MAGIC         else use_relay_loads.customer_office
# MAGIC       end as customer_office,
# MAGIC       --customer_office as Customer_Office,
# MAGIC       Case
# MAGIC         when use_relay_loads.carr_office = 'TGT' then 'PHI'
# MAGIC         else use_relay_loads.carr_office
# MAGIC       end as Carrier_Office,
# MAGIC       --carr_office as Carrier_Office,
# MAGIC       --'Shipper',
# MAGIC       use_relay_loads.Carrier_Rep,
# MAGIC       --carr_office,
# MAGIC       ---financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       use_relay_loads.mastername,
# MAGIC       use_relay_loads.customer_name,
# MAGIC       use_relay_loads.customer_rep,
# MAGIC       salesrep,
# MAGIC       status as load_status,
# MAGIC       use_relay_loads.origin_city,
# MAGIC       use_relay_loads.origin_state,
# MAGIC       use_relay_loads.dest_city,
# MAGIC       use_relay_loads.dest_state,
# MAGIC       use_relay_loads.origin_zip as Origin_Zip,
# MAGIC       use_relay_loads.dest_zip as Dest_Zip,
# MAGIC       CONCAT(
# MAGIC         INITCAP(use_relay_loads.origin_city),
# MAGIC         ',',
# MAGIC         use_relay_loads.origin_state,
# MAGIC         '>',
# MAGIC         INITCAP(use_relay_loads.dest_city),
# MAGIC         ',',
# MAGIC         use_relay_loads.dest_state
# MAGIC       ) AS load_lane,
# MAGIC       use_relay_loads.market_origin as Market_origin,
# MAGIC       use_relay_loads.market_dest as Market_Deat,
# MAGIC       use_relay_loads.market_lane as Market_Lane,
# MAGIC       use_relay_loads.del_date as Delivery_Date,
# MAGIC       use_relay_loads.rolled_at as rolled_date,
# MAGIC       use_relay_loads.Booked_Date as Booked_Date,
# MAGIC       use_relay_loads.Tendering_Date as Tendered_Date,
# MAGIC       use_relay_loads.bounced_date as bounced_date,
# MAGIC       use_relay_loads.cancelled_date as Cancelled_Date,
# MAGIC       pu_appt_date as Pickup_Appointment_Date,
# MAGIC       new_pu_appt as New_Pickup_Appointment_Date,
# MAGIC       pickup_in as Pickup_in,
# MAGIC       pickup_out as Pickup_Out,
# MAGIC       pickup_datetime as pickup_datetime,
# MAGIC       case
# MAGIC         when pickup_datetime :: date > COALESCE(pu_appt_date :: date, ready_date :: date) then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_pu,
# MAGIC       delivery_in as Delivery_In,
# MAGIC       delivery_out as Delivery_out,
# MAGIC       delivery_datetime as Delivery_Datetime,
# MAGIC       case
# MAGIC         when delivery_datetime :: date > del_appt_date :: date then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_del,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN use_relay_loads.customer_office <> use_relay_loads.carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when e.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       use_relay_loads.dot_num as DOT_Number,
# MAGIC       e.carrier_name as Carrier,
# MAGIC       use_relay_loads.total_miles :: float,
# MAGIC       total_cust_rate.total_cust_rate :: float as SplitRev,
# MAGIC       (projected_expense :: float / 100.0) as splitexp,
# MAGIC       (
# MAGIC         coalesce(total_cust_rate.total_cust_rate, 0) :: float - coalesce(projected_expense :: float / 100.0, 0) :: float
# MAGIC       ) as splitmargin,
# MAGIC       CASE
# MAGIC         WHEN new_pu_appt IS NOT NULL
# MAGIC         AND Booked_Date IS NOT NULL THEN DATEDIFF(day, Booked_Date, new_pu_appt)
# MAGIC         ELSE NULL
# MAGIC       END AS Lead_Time,
# MAGIC       invoice_details.invoice_amt as invoiceable_amt,
# MAGIC       invoice_details.invoiced as amt_already_invoiced,
# MAGIC       invoice_details.not_invoiced as outstanindg_amt,
# MAGIC       carrier_charges.carr_lhc as Linehaul,
# MAGIC       carrier_charges.carr_fsc as Fuel_Surcharge,
# MAGIC       carrier_charges.carr_acc as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_lhc,
# MAGIC       use_relay_loads.Trailer_Num,
# MAGIC       use_relay_loads.Cargo_Value,
# MAGIC       use_relay_loads.Weight,
# MAGIC       use_relay_loads.max_buy,
# MAGIC       use_relay_loads.Market_Buy_Rate,
# MAGIC       use_relay_loads.Spot_Revenue,
# MAGIC       use_relay_loads.Spot_Margin,
# MAGIC       d.Is_Awarded,
# MAGIC       'RELAY' as tms
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join brokerageprod.bronze.big_export_projection e on use_relay_loads.relay_load :: float = e.load_number :: float
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load :: float = total_cust_rate.relay_reference_number :: float
# MAGIC       left join brokerageprod.bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join brokerageprod.bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1
# MAGIC       left join invoice_details on use_relay_loads.relay_load = invoice_details.relay_reference_number
# MAGIC       left join brokerageprod.analytics.Dim_date dim_date on dim_date.calendar_date = use_relay_loads.use_date
# MAGIC       left join brokerageprod.gold.fact_awards_summary d on use_relay_loads.relay_load = d.Load_ID and d.Is_deleted = 0 and d.Is_Awarded = 'Awarded'
# MAGIC       left join carrier_charges on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter = 'ltl'
# MAGIC       and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and e.customer_id not in ('hain', 'roland', 'deb')
# MAGIC     Union
# MAGIC     select
# MAGIC       distinct -- Shipper
# MAGIC       relay_load as relay_ref_num,
# MAGIC       --0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       dim_date.period_year,
# MAGIC       dim_date.Month_Year,
# MAGIC       Equipment as Mode,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else modee
# MAGIC       End as Equipment,
# MAGIC       date_range_two.week_num,
# MAGIC       Case
# MAGIC         when use_relay_loads.customer_office = 'TGT' then 'PHI'
# MAGIC         else use_relay_loads.customer_office
# MAGIC       end as customer_office,
# MAGIC       --customer_office as Customer_Office,
# MAGIC       Case
# MAGIC         when use_relay_loads.carr_office = 'TGT' then 'PHI'
# MAGIC         else use_relay_loads.carr_office
# MAGIC       end as Carrier_Office,
# MAGIC       --carr_office as Carrier_Office,
# MAGIC       --'Shipper',
# MAGIC       use_relay_loads.Carrier_Rep,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       use_relay_loads.mastername,
# MAGIC       use_relay_loads.customer_name,
# MAGIC       use_relay_loads.customer_rep,
# MAGIC       salesrep,
# MAGIC       status as load_status,
# MAGIC       use_relay_loads.origin_city,
# MAGIC       use_relay_loads.origin_state,
# MAGIC       use_relay_loads.dest_city,
# MAGIC       use_relay_loads.dest_state,
# MAGIC       use_relay_loads.origin_zip as Origin_Zip,
# MAGIC       use_relay_loads.dest_zip as Dest_Zip,
# MAGIC       CONCAT(
# MAGIC         INITCAP(use_relay_loads.origin_city),
# MAGIC         ',',
# MAGIC         use_relay_loads.origin_state,
# MAGIC         '>',
# MAGIC         INITCAP(use_relay_loads.dest_city),
# MAGIC         ',',
# MAGIC         use_relay_loads.dest_state
# MAGIC       ) AS load_lane,
# MAGIC       use_relay_loads.market_origin as Market_origin,
# MAGIC       use_relay_loads.market_dest as Market_Deat,
# MAGIC       use_relay_loads.market_lane as Market_Lane,
# MAGIC       use_relay_loads.del_date as Delivery_Date,
# MAGIC       use_relay_loads.rolled_at as rolled_date,
# MAGIC       use_relay_loads.Booked_Date as Booked_Date,
# MAGIC       use_relay_loads.Tendering_Date as Tendered_Date,
# MAGIC       use_relay_loads.bounced_date as bounced_date,
# MAGIC       use_relay_loads.cancelled_date as Cancelled_Date,
# MAGIC       pu_appt_date as Pickup_Appointment_Date,
# MAGIC       new_pu_appt as New_Pickup_Appointment_Date,
# MAGIC       pickup_in as Pickup_in,
# MAGIC       pickup_out as Pickup_Out,
# MAGIC       pickup_datetime as pickup_datetime,
# MAGIC       case
# MAGIC         when pickup_datetime :: date > COALESCE(pu_appt_date :: date, ready_date :: date) then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_pu,
# MAGIC       delivery_in as Delivery_In,
# MAGIC       delivery_out as Delivery_out,
# MAGIC       delivery_datetime as Delivery_Datetime,
# MAGIC       case
# MAGIC         when delivery_datetime :: date > del_appt_date :: date then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_del,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN use_relay_loads.customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when e.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       use_relay_loads.dot_num as DOT_Number,
# MAGIC       e.carrier_name as Carrier,
# MAGIC       use_relay_loads.total_miles :: float,
# MAGIC       (
# MAGIC         (
# MAGIC           carrier_accessorial_expense + ceiling(
# MAGIC             case
# MAGIC               customer_id
# MAGIC               when 'deb' then case
# MAGIC                 carrier_id
# MAGIC                 when '300003979' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                 else case
# MAGIC                   when e.ship_date :: date < '2019-03-09' then greatest(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                   else greatest(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                 end
# MAGIC               end
# MAGIC               when 'hain' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC               when 'roland' then carrier_linehaul_expense * 0.1365
# MAGIC               else 0
# MAGIC             end + carrier_linehaul_expense
# MAGIC           ) + ceiling(
# MAGIC             ceiling(
# MAGIC               case
# MAGIC                 customer_id
# MAGIC                 when 'deb' then case
# MAGIC                   carrier_id
# MAGIC                   when '300003979' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                   else case
# MAGIC                     when e.ship_date :: date < '2019-03-09' then greatest(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                     else greatest(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                   end
# MAGIC                 end
# MAGIC                 when 'hain' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC                 when 'roland' then carrier_linehaul_expense * 0.1365
# MAGIC                 else 0
# MAGIC               end + carrier_linehaul_expense
# MAGIC             ) * (
# MAGIC               case
# MAGIC                 carrier_linehaul_expense
# MAGIC                 when 0 then 0
# MAGIC                 else carrier_fuel_expense :: float / carrier_linehaul_expense :: float
# MAGIC               end
# MAGIC             )
# MAGIC           )
# MAGIC         ) / 100
# MAGIC       ) :: float as split_rev,
# MAGIC       (projected_expense :: float / 100.0) as split_exp,
# MAGIC       (
# MAGIC         coalesce(
# MAGIC           (
# MAGIC             (
# MAGIC               carrier_accessorial_expense + ceiling(
# MAGIC                 case
# MAGIC                   customer_id
# MAGIC                   when 'deb' then case
# MAGIC                     carrier_id
# MAGIC                     when '300003979' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     else case
# MAGIC                       when e.ship_date :: date < '2019-03-09' then greatest(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                       else greatest(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                     end
# MAGIC                   end
# MAGIC                   when 'hain' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC                   when 'roland' then carrier_linehaul_expense * 0.1365
# MAGIC                   else 0
# MAGIC                 end + carrier_linehaul_expense
# MAGIC               ) + ceiling(
# MAGIC                 ceiling(
# MAGIC                   case
# MAGIC                     customer_id
# MAGIC                     when 'deb' then case
# MAGIC                       carrier_id
# MAGIC                       when '300003979' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                       else case
# MAGIC                         when e.ship_date :: date < '2019-03-09' then greatest(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                         else greatest(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                       end
# MAGIC                     end
# MAGIC                     when 'hain' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC                     when 'roland' then carrier_linehaul_expense * 0.1365
# MAGIC                     else 0
# MAGIC                   end + carrier_linehaul_expense
# MAGIC                 ) * (
# MAGIC                   case
# MAGIC                     carrier_linehaul_expense
# MAGIC                     when 0 then 0
# MAGIC                     else carrier_fuel_expense :: float / carrier_linehaul_expense :: float
# MAGIC                   end
# MAGIC                 )
# MAGIC               )
# MAGIC             ) / 100
# MAGIC           ),
# MAGIC           0
# MAGIC         ) :: float - coalesce((projected_expense :: float / 100.0), 0)
# MAGIC       ) :: float as split_margin,
# MAGIC       CASE
# MAGIC         WHEN new_pu_appt IS NOT NULL
# MAGIC         AND Booked_Date IS NOT NULL THEN DATEDIFF(day, Booked_Date, new_pu_appt)
# MAGIC         ELSE NULL
# MAGIC       END AS Lead_Time,
# MAGIC       invoice_details.invoice_amt as invoiceable_amt,
# MAGIC       invoice_details.invoiced as amt_already_invoiced,
# MAGIC       invoice_details.not_invoiced as outstanindg_amt,
# MAGIC       carrier_charges.carr_lhc as Linehaul,
# MAGIC       carrier_charges.carr_fsc as Fuel_Surcharge,
# MAGIC       carrier_charges.carr_acc as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_lhc,
# MAGIC       use_relay_loads.Trailer_Num,
# MAGIC       use_relay_loads.Cargo_Value,
# MAGIC       use_relay_loads.Weight,
# MAGIC       use_relay_loads.max_buy,
# MAGIC       use_relay_loads.Market_Buy_Rate,
# MAGIC       use_relay_loads.Spot_Revenue,
# MAGIC       use_relay_loads.Spot_Margin,
# MAGIC       d.Is_Awarded,
# MAGIC       'RELAY' as tms
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join brokerageprod.bronze.big_export_projection e on use_relay_loads.relay_load :: float = e.load_number :: float
# MAGIC       left join brokerageprod.bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join brokerageprod.bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1
# MAGIC       left join invoice_details on use_relay_loads.relay_load = invoice_details.relay_reference_number
# MAGIC       left join brokerageprod.analytics.Dim_date dim_date on dim_date.calendar_date = use_relay_loads.use_date
# MAGIC       left join brokerageprod.gold.fact_awards_summary d on use_relay_loads.relay_load = d.Load_ID and d.Is_deleted = 0 and d.Is_Awarded = 'Awarded'
# MAGIC       left join carrier_charges on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter = 'ltl- (hain,deb,roland)'
# MAGIC       and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and e.customer_id in ('hain', 'roland', 'deb')	  --UNION
# MAGIC     Union
# MAGIC       ------------------------- Need to take from here ------------------------
# MAGIC     select
# MAGIC       distinct -- Shipper
# MAGIC       relay_load as relay_ref_num,
# MAGIC       --0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       dim_date.period_year,
# MAGIC       dim_date.Month_Year,
# MAGIC       Equipment as Mode,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else modee
# MAGIC       End as Equipment,
# MAGIC       date_range_two.week_num,
# MAGIC       Case
# MAGIC         when use_relay_loads.customer_office = 'TGT' then 'PHI'
# MAGIC         else use_relay_loads.customer_office
# MAGIC       end as customer_office,
# MAGIC       --customer_office as Customer_Office,
# MAGIC       Case
# MAGIC         when use_relay_loads.carr_office = 'TGT' then 'PHI'
# MAGIC         else use_relay_loads.carr_office
# MAGIC       end as Carrier_Office,
# MAGIC       --carr_office as Carrier_Office,
# MAGIC       --'Shipper',
# MAGIC       use_relay_loads.Carrier_Rep,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       use_relay_loads.mastername,
# MAGIC       use_relay_loads.customer_name,
# MAGIC       use_relay_loads.customer_rep,
# MAGIC       salesrep,
# MAGIC       status as load_status,
# MAGIC       use_relay_loads.origin_city,
# MAGIC       use_relay_loads.origin_state,
# MAGIC       use_relay_loads.dest_city,
# MAGIC       use_relay_loads.dest_state,
# MAGIC       use_relay_loads.origin_zip as Origin_Zip,
# MAGIC       use_relay_loads.dest_zip as Dest_Zip,
# MAGIC       CONCAT(
# MAGIC         INITCAP(use_relay_loads.origin_city),
# MAGIC         ',',
# MAGIC         use_relay_loads.origin_state,
# MAGIC         '>',
# MAGIC         INITCAP(use_relay_loads.dest_city),
# MAGIC         ',',
# MAGIC         use_relay_loads.dest_state
# MAGIC       ) AS load_lane,
# MAGIC       use_relay_loads.market_origin as Market_origin,
# MAGIC       use_relay_loads.market_dest as Market_Deat,
# MAGIC       use_relay_loads.market_lane as Market_Lane,
# MAGIC       use_relay_loads.del_date as Delivery_Date,
# MAGIC       use_relay_loads.rolled_at as rolled_date,
# MAGIC       use_relay_loads.Booked_Date as Booked_Date,
# MAGIC       use_relay_loads.Tendering_Date as Tendered_Date,
# MAGIC       use_relay_loads.bounced_date as bounced_date,
# MAGIC       use_relay_loads.cancelled_date as Cancelled_Date,
# MAGIC       pu_appt_date as Pickup_Appointment_Date,
# MAGIC       new_pu_appt as New_Pickup_Appointment_Date,
# MAGIC       pickup_in as Pickup_in,
# MAGIC       pickup_out as Pickup_Out,
# MAGIC       pickup_datetime as pickup_datetime,
# MAGIC       case
# MAGIC         when pickup_datetime :: date > COALESCE(pu_appt_date :: date, ready_date :: date) then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_pu,
# MAGIC       delivery_in as Delivery_In,
# MAGIC       delivery_out as Delivery_out,
# MAGIC       delivery_datetime as Delivery_Datetime,
# MAGIC       case
# MAGIC         when delivery_datetime :: date > del_appt_date :: date then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_del,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN use_relay_loads.customer_office <> use_relay_loads.carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when use_relay_loads.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       use_relay_loads.dot_num as DOT_Number,
# MAGIC       use_relay_loads.carrier_name as Carrier,
# MAGIC       use_relay_loads.total_miles :: float,
# MAGIC       total_cust_rate.total_cust_rate :: float as split_rev,
# MAGIC       total_carrier_rate.total_carrier_rate :: float as split_exp,
# MAGIC       coalesce(total_cust_rate.total_cust_rate, 0) :: float - coalesce(
# MAGIC         (
# MAGIC           total_carrier_rate.total_carrier_rate :: float / 2
# MAGIC         ),
# MAGIC         0
# MAGIC       ) :: float as split_margin,
# MAGIC       CASE
# MAGIC         WHEN new_pu_appt IS NOT NULL
# MAGIC         AND Booked_Date IS NOT NULL THEN DATEDIFF(day, Booked_Date, new_pu_appt)
# MAGIC         ELSE NULL
# MAGIC       END AS Lead_Time,
# MAGIC       invoice_details.invoice_amt as invoiceable_amt,
# MAGIC       invoice_details.invoiced as amt_already_invoiced,
# MAGIC       invoice_details.not_invoiced as outstanindg_amt,
# MAGIC       carrier_charges.carr_lhc as Linehaul,
# MAGIC       carrier_charges.carr_fsc as Fuel_Surcharge,
# MAGIC       carrier_charges.carr_acc as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) as customer_lhc,
# MAGIC       use_relay_loads.Trailer_Num,
# MAGIC       use_relay_loads.Cargo_Value,
# MAGIC       use_relay_loads.Weight,
# MAGIC       use_relay_loads.max_buy,
# MAGIC       use_relay_loads.Market_Buy_Rate,
# MAGIC       use_relay_loads.Spot_Revenue,
# MAGIC       use_relay_loads.Spot_Margin,
# MAGIC       d.Is_Awarded,
# MAGIC       'RELAY' as tms
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number
# MAGIC       left join total_carrier_rate on use_relay_loads.combined_load = total_carrier_rate.relay_reference_number
# MAGIC       left join combined_loads_mine on use_relay_loads.relay_load :: float = combined_loads_mine.combine_new_rrn
# MAGIC       left join brokerageprod.bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join brokerageprod.bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1
# MAGIC       left join invoice_details on use_relay_loads.relay_load = invoice_details.relay_reference_number
# MAGIC       left join brokerageprod.analytics.Dim_date dim_date on dim_date.calendar_date = use_relay_loads.use_date
# MAGIC       left join brokerageprod.gold.fact_awards_summary d on use_relay_loads.relay_load = d.Load_ID and d.Is_deleted = 0 and d.Is_Awarded = 'Awarded'
# MAGIC       left join carrier_charges on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC       and combined_load != 'NO' --UNION
# MAGIC   ),
# MAGIC   Ordered as (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       rank() OVER (
# MAGIC         PARTITION BY Load_Number
# MAGIC         ORDER BY
# MAGIC           Pickup_Appointment_Date, Delivery_Date, Tendered_Date DESC
# MAGIC       ) AS rank
# MAGIC     from
# MAGIC       system_union
# MAGIC   ),
# MAGIC   -- Select * from Ordered
# MAGIC   final as (
# MAGIC     select
# MAGIC       Load_Number,
# MAGIC       Ship_Date,
# MAGIC       period_year,
# MAGIC       Month_Year,
# MAGIC       Mode,
# MAGIC       Equipment,
# MAGIC       Week_Num,
# MAGIC       customer_office,
# MAGIC       Carrier_Office,
# MAGIC       Booked_By,
# MAGIC       customer_master,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       salesrep,
# MAGIC       load_status,
# MAGIC       Origin_City,
# MAGIC       Origin_State,
# MAGIC       Dest_City,
# MAGIC       Dest_State,
# MAGIC       Origin_Zip,
# MAGIC       Dest_Zip,
# MAGIC       load_lane,
# MAGIC       Market_origin,
# MAGIC       Market_Deat,
# MAGIC       Market_Lane,
# MAGIC       Delivery_Date,
# MAGIC       rolled_date,
# MAGIC       Booked_Date,
# MAGIC       Tendered_Date,
# MAGIC       bounced_date,
# MAGIC       Cancelled_Date,
# MAGIC       Pickup_Appointment_Date,
# MAGIC       New_Pickup_Appointment_Date,
# MAGIC       Pickup_in,
# MAGIC       Pickup_Out,
# MAGIC       AM_Dates.pickup_datetime,
# MAGIC       AM_Dates.ot_pu,
# MAGIC       Delivery_In,
# MAGIC       Delivery_out,
# MAGIC       AM_Dates.Delivery_Datetime,
# MAGIC       AM_Dates.ot_del,
# MAGIC       invoiced_Date,
# MAGIC       Booking_type,
# MAGIC       Load_flag,
# MAGIC       DOT_Num,
# MAGIC       Carrier,
# MAGIC       Miles,
# MAGIC       ordered.Revenue,
# MAGIC       Expense,
# MAGIC       ordered.Margin,
# MAGIC       Lead_Time,
# MAGIC       invoiceable_amt,
# MAGIC       amt_already_invoiced,
# MAGIC       outstanindg_amt,
# MAGIC       Linehaul,
# MAGIC       Fuel_Surcharge,
# MAGIC       Other_Fess,
# MAGIC       customer_fuel,
# MAGIC       customer_acc,
# MAGIC       customer_lhc,
# MAGIC       Trailer_Num,
# MAGIC       Cargo_Value,
# MAGIC       case
# MAGIC         when Cargo_Value :: float / 100 > 100000 then 'YES'
# MAGIC         else 'NO'
# MAGIC       end as High_Value,
# MAGIC       Weight,
# MAGIC       max_buy,
# MAGIC       market_buy_rate,
# MAGIC       spot_data.revenue as Spot_Revenue,
# MAGIC       spot_data.margin as Spot_Margin,
# MAGIC 	  spot_data.cost as Spot_Cost,
# MAGIC 	  -- Spot_data.Won_quotes as Spot_Won_Cost,
# MAGIC       Is_Awarded,
# MAGIC       TMS_System
# MAGIC     from
# MAGIC       Ordered
# MAGIC 	  left join analytics.AM_Dates on Ordered.load_number = AM_Dates.relay_reference_number and AM_Dates.tms = 'Relay'
# MAGIC       left join analytics.spot_data on  Ordered.load_number = spot_data.Loadnum
# MAGIC     where
# MAGIC       rank = 1
# MAGIC     union
# MAGIC     select
# MAGIC       distinct -- Shipper  ----- Need to change from here.
# MAGIC       id as relay_ref_num,
# MAGIC       --0.5 as Load_Counter,
# MAGIC       aljex_data.use_date,
# MAGIC       dim_date.period_year,
# MAGIC       dim_date.Month_Year,
# MAGIC       tl_or_ltl as Mode,
# MAGIC       Case
# MAGIC         when modee = 'Dry Van' then 'Dry Van'
# MAGIC         when modee = 'Reefer' then 'Reefer'
# MAGIC         when modee = 'Flatbed' then 'Flatbed'
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else null
# MAGIC       end as Equipment,
# MAGIC       date_range_two.week_num,
# MAGIC       Case
# MAGIC         when aljex_data.customer_office = 'TGT' then 'PHI'
# MAGIC         else aljex_data.customer_office
# MAGIC       end as customer_office,
# MAGIC       --customer_office as Customer_Office,
# MAGIC       Case
# MAGIC         when aljex_data.carr_office = 'TGT' then 'PHI'
# MAGIC         else aljex_data.carr_office
# MAGIC       end as Carrier_Office,
# MAGIC       --carr_office as Carrier_Office,
# MAGIC       --'Shipper',
# MAGIC       key_c_user,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       aljex_data.mastername,
# MAGIC       aljex_data.customer_name,
# MAGIC       aljex_data.customer_rep,
# MAGIC       aljex_data.salesrep,
# MAGIC       aljex_data.status as load_status,
# MAGIC       aljex_data.origin_city,
# MAGIC       aljex_data.origin_state,
# MAGIC       aljex_data.dest_city,
# MAGIC       aljex_data.dest_state,
# MAGIC       aljex_data.origin_zip as Origin_Zip,
# MAGIC       aljex_data.dest_zip as Dest_Zip,
# MAGIC       CONCAT(
# MAGIC         INITCAP(aljex_data.origin_city),
# MAGIC         ',',
# MAGIC         aljex_data.origin_state,
# MAGIC         '>',
# MAGIC         INITCAP(aljex_data.dest_city),
# MAGIC         ',',
# MAGIC         aljex_data.dest_state
# MAGIC       ) AS load_lane,
# MAGIC       aljex_data.market_origin as Market_origin,
# MAGIC       aljex_data.market_dest as Market_Deat,
# MAGIC       aljex_data.market_lane as Market_Lane,
# MAGIC       aljex_data.delivery_date as Delivery_Date,
# MAGIC       aljex_data.rolled_date as Delivery_Date,
# MAGIC       aljex_data.Booked_Date as Booked_Date,
# MAGIC       aljex_data.Tender_Date as Tendered_Date,
# MAGIC       aljex_data.bounced_date as bounced_date,
# MAGIC       aljex_data.cancelled_date as Cancelled_Date,
# MAGIC       --Days_ahead as Days_Ahead,
# MAGIC       --prebookable,
# MAGIC       aljex_data.Pickup_Appointment_Date,
# MAGIC       aljex_data.New_Pickup_Appointment_Date,
# MAGIC       aljex_data.PickUp_Start as pickup_in,
# MAGIC       aljex_data.PickUp_End as Pickup_Out,
# MAGIC       AM_Dates.pickup_datetime as pickup_datetime,
# MAGIC       AM_Dates.ot_pu,
# MAGIC       Drop_Start as Delivery_In,
# MAGIC       Drop_End as Delivery_Out,
# MAGIC       AM_Dates.Delivery_Datetime,
# MAGIC       AM_Dates.ot_del,
# MAGIC       null :: date as invoiced_Date,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN aljex_data.customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when aljex_data.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       aljex_data.dot_num as DOT_Number,
# MAGIC       aljex_data.carrier_name as Carrier,
# MAGIC       case
# MAGIC         aljex_data.miles Rlike '^\d+(.\d+)?$'
# MAGIC         when true then aljex_data.miles :: float
# MAGIC         else 0
# MAGIC       end as miles,
# MAGIC       (
# MAGIC         case
# MAGIC           when aljex_data.cust_curr = 'USD' then invoice_total :: float
# MAGIC           else invoice_total :: float / aljex_data.conversion_rate :: float
# MAGIC         end
# MAGIC       ) as split_rev,
# MAGIC       (
# MAGIC         case
# MAGIC           when carrier_curr = 'USD' then final_carrier_rate :: float
# MAGIC           else final_carrier_rate :: float / aljex_data.conversion_rate
# MAGIC         end
# MAGIC       ) as split_exp,
# MAGIC       (
# MAGIC         coalesce(
# MAGIC           case
# MAGIC             when cust_curr = 'USD' then invoice_total :: float
# MAGIC             else invoice_total :: float / aljex_data.conversion_rate :: float
# MAGIC           end,
# MAGIC           0
# MAGIC         ) - coalesce(
# MAGIC           case
# MAGIC             when aljex_data.carrier_curr = 'USD' then final_carrier_rate :: float
# MAGIC             else final_carrier_rate :: float / aljex_data.conversion_rate
# MAGIC           end,
# MAGIC           0
# MAGIC         )
# MAGIC       ) as split_margin,
# MAGIC       CASE
# MAGIC         WHEN New_Pickup_Appointment_Date IS NOT NULL
# MAGIC         AND Booked_Date IS NOT NULL THEN DATEDIFF(day, Booked_Date, New_Pickup_Appointment_Date)
# MAGIC         ELSE NULL
# MAGIC       END AS Lead_Time,
# MAGIC       invoice_details.invoice_amt as invoiceable_amt,
# MAGIC       invoice_details.invoiced as amt_already_invoiced,
# MAGIC       invoice_details.not_invoiced as outstanindg_amt,
# MAGIC       aljex_data.carrier_lhc as Linehaul,
# MAGIC       aljex_data.carrier_fuel as Fuel_Surcharge,
# MAGIC       aljex_data.carrier_acc as Other_Fess,
# MAGIC       (aljex_data.customer_fuel) as customer_fuel,
# MAGIC       (aljex_data.customer_acc) as customer_acc,
# MAGIC       (
# MAGIC         aljex_data.invoice_total :: float - aljex_data.customer_fuel :: float - aljex_data.customer_acc :: float
# MAGIC       ) as customer_lhc,
# MAGIC       aljex_data.Trailer_Num,
# MAGIC       Cargo_Value,
# MAGIC       case
# MAGIC         when Cargo_Value > 100000 Then 'YES'
# MAGIC         else 'NO'
# MAGIC       end as High_Value,
# MAGIC       aljex_data.Weight,
# MAGIC       null as max_buy,
# MAGIC       aljex_data.Market_Buy_Rate,
# MAGIC       spot_data.revenue as Spot_Revenue,
# MAGIC 	  spot_data.margin as Spot_Margin,
# MAGIC 	  spot_data.cost as Spot_Cost,
# MAGIC 	  -- Spot_data.Won_quotes as Spot_Won_Cost,
# MAGIC       d.Is_Awarded,
# MAGIC       'ALJEX' as tms
# MAGIC     from
# MAGIC       aljex_data
# MAGIC 	  left join analytics.spot_data on  aljex_data.id = spot_data.Loadnum
# MAGIC       left join analytics.AM_Dates on aljex_data.id = AM_Dates.relay_reference_number and AM_Dates.tms = 'Aljex'
# MAGIC       left join brokerageprod.bronze.date_range_two on aljex_data.use_date :: date = date_range_two.date_date :: date --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC       left join brokerageprod.gold.fact_awards_summary d on aljex_data.id = d.Load_ID and d.Is_deleted = 0 and Is_Awarded = 'Awarded'
# MAGIC       left join brokerageprod.analytics.Dim_date dim_date on dim_date.calendar_date = aljex_data.use_date
# MAGIC       left join invoice_details on aljex_data.id = invoice_details.relay_reference_number
# MAGIC     union
# MAGIC     select
# MAGIC       distinct -- Shipper  ----- Need to change from here.
# MAGIC       Loadnum as relay_ref_num,
# MAGIC       --0.5 as Load_Counter,
# MAGIC       coalesce(
# MAGIC         arrive_pu_date :: date,
# MAGIC         depart_pu_date :: date,
# MAGIC         pu_appt :: date
# MAGIC       ) as use_date,
# MAGIC       dim_date.period_year,
# MAGIC       dim_date.Month_Year,
# MAGIC       load_mode as Mode,
# MAGIC       null as Equipment,
# MAGIC       date_range_two.week_num as week_num,
# MAGIC       'DBG' as customer_office,
# MAGIC       --customer_office as Customer_Office,
# MAGIC       'DBG' as Carrier_Office,
# MAGIC       --carr_office as Carrier_Office,
# MAGIC       --'Shipper',
# MAGIC       null as key_c_user,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       Shipper as mastername,
# MAGIC       shipper as customer_name,
# MAGIC       Null as customer_rep,
# MAGIC       Null as salesrep,
# MAGIC       Load_status as load_status,
# MAGIC       origin_city,
# MAGIC       origin_state,
# MAGIC       dest_city,
# MAGIC       dest_state,
# MAGIC       origin_zip as Origin_Zip,
# MAGIC       dest_zip as Dest_Zip,
# MAGIC       CONCAT(
# MAGIC         INITCAP(origin_city),
# MAGIC         ',',
# MAGIC         origin_state,
# MAGIC         '>',
# MAGIC         INITCAP(dest_city),
# MAGIC         ',',
# MAGIC         dest_state
# MAGIC       ) AS load_lane,
# MAGIC       Null as Market_origin,
# MAGIC       Null as Market_Deat,
# MAGIC       Null as Market_Lane,
# MAGIC       del_appt as Delivery_Date,
# MAGIC       rolled_date as rolled_date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tendered_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       Cancelled_Date as Cancelled_Date,
# MAGIC       pu_appt as Pickup_Appointment_Date,
# MAGIC       null as New_Pickup_Appointment_Date,
# MAGIC       arrive_pu_date as Pickup_in,
# MAGIC       depart_pu_date as Pickup_Out,
# MAGIC       CAST(Pickup_Appointment_Date AS TIMESTAMP) as pickup_datetime,
# MAGIC       case
# MAGIC         when pickup_datetime :: date > COALESCE(arrive_pu_date :: date, pu_appt :: date) then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_pu,
# MAGIC       arrive_del_date as Delivery_In,
# MAGIC       depart_del_date as Delivery_out,
# MAGIC       cast(depart_del_date as TIMESTAMP) as Delivery_Datetime,
# MAGIC       case
# MAGIC         when delivery_datetime :: date > del_appt :: date then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_del,
# MAGIC       finance_date as invoiced_Date,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       Null as Booking_type,
# MAGIC       case
# MAGIC         when carrier is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot as DOT_Number,
# MAGIC       transfix_data.carrier as Carrier,
# MAGIC       calc_miles :: float,
# MAGIC       transfix_data.total_rev :: float as split_rev,
# MAGIC       transfix_data.total_exp :: float :: float as split_exp,
# MAGIC       coalesce(transfix_data.total_rev, 0) :: float - coalesce(
# MAGIC         (transfix_data.total_exp :: float),
# MAGIC         0
# MAGIC       ) :: float as split_margin,
# MAGIC       CASE
# MAGIC         WHEN pu_appt IS NOT NULL
# MAGIC         AND Booked_Date IS NOT NULL THEN DATEDIFF(day, Booked_Date, pu_appt)
# MAGIC         ELSE NULL
# MAGIC       END AS Lead_Time,
# MAGIC       null as invoiceable_amt,
# MAGIC       null as amt_already_invoiced,
# MAGIC       null as outstanindg_amt,
# MAGIC       transfix_data.carr_lhc as Linehaul,
# MAGIC       transfix_data.carr_fuel as Fuel_Surcharge,
# MAGIC       transfix_data.carr_acc as Other_Fess,
# MAGIC       transfix_data.cust_lhc,
# MAGIC       transfix_data.cust_fuel,
# MAGIC       transfix_data.cust_acc,
# MAGIC       Null as Trailer_Num,
# MAGIC       null as Cargo_Value,
# MAGIC       null as High_Value,
# MAGIC       transfix_data.Weight,
# MAGIC       Null as max_buy,
# MAGIC       null as Market_Buy_Rate,
# MAGIC       null as Spot_Revenue,
# MAGIC       null as Spot_Margin,
# MAGIC 	  null  as Spot_Cost,
# MAGIC 	  -- null as Spot_Won_Cost,
# MAGIC       null as Is_Awarded,
# MAGIC       'Transfix' as tms
# MAGIC     from
# MAGIC       brokerageprod.bronze.transfix_data
# MAGIC       join brokerageprod.analytics.dim_date on coalesce(
# MAGIC         arrive_pu_date :: date,
# MAGIC         depart_pu_date :: date,
# MAGIC         pu_appt :: date
# MAGIC       ) :: date = brokerageprod.analytics.dim_date.calendar_date :: date
# MAGIC       left join brokerageprod.bronze.date_range_two on brokerageprod.analytics.dim_date.calendar_date :: date = date_range_two.date_date :: date
# MAGIC   ),
# MAGIC   -- Select * from final
# MAGIC   tender as (
# MAGIC     SELECT
# MAGIC       tender_reference_numbers_projection.relay_reference_number as id,
# MAGIC       tenderer,
# MAGIC       tendering_acceptance.accepted_at,
# MAGIC       integration_tender_mapped_projection_v2.tendered_at
# MAGIC     FROM
# MAGIC       bronze.tender_reference_numbers_projection
# MAGIC       LEFT JOIN brokerageprod.bronze.tendering_acceptance ON tender_reference_numbers_projection.tender_id = tendering_acceptance.tender_id
# MAGIC       LEFT JOIN brokerageprod.bronze.integration_tender_mapped_projection_v2 ON tendering_acceptance.shipment_id = integration_tender_mapped_projection_v2.shipment_id
# MAGIC       AND event_type = 'tender_mapped'
# MAGIC       JOIN analytics.dim_date ON cast(accepted_At as DATE) = analytics.dim_date.Calendar_Date
# MAGIC     WHERE
# MAGIC       `accepted?` = 'true'
# MAGIC       AND is_split = 'false'
# MAGIC   ),
# MAGIC   max as (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       rank() OVER (
# MAGIC         PARTITION BY id
# MAGIC         ORDER BY
# MAGIC           tendered_at,
# MAGIC           accepted_at DESC
# MAGIC       ) AS rank
# MAGIC     from
# MAGIC       tender
# MAGIC   ),
# MAGIC   tender_final AS (
# MAGIC     SELECT
# MAGIC       id,
# MAGIC       tenderer,
# MAGIC       CASE
# MAGIC         WHEN tenderer IN ('orderful', '3gtms') THEN 'EDI'
# MAGIC         WHEN tenderer = 'vooma-orderful' THEN 'Vooma'
# MAGIC         WHEN tenderer IS NULL THEN 'Manual'
# MAGIC         ELSE tenderer
# MAGIC       END AS Tender_Source_Type
# MAGIC     FROM
# MAGIC       max
# MAGIC     WHERE
# MAGIC       rank = 1
# MAGIC   ),
# MAGIC   final_ranked_loads as ( select
# MAGIC     *
# MAGIC   from
# MAGIC     final
# MAGIC     left join tender_final on final.load_number = tender_final.id)
# MAGIC 	select  *  from final_ranked_loads 

# COMMAND ----------

# MAGIC %md
# MAGIC # Creation of Same Day Bounce Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE TABLE Analytics.Fact_Same_Day_Bounce_Loads AS
# MAGIC With to_get_avg as (
# MAGIC  SELECT DISTINCT canada_conversions.ship_date,
# MAGIC     canada_conversions.conversion AS us_to_cad,
# MAGIC     canada_conversions.us_to_cad AS cad_to_us
# MAGIC    FROM canada_conversions
# MAGIC   ORDER BY canada_conversions.ship_date DESC
# MAGIC  LIMIT 7),
# MAGIC
# MAGIC
# MAGIC average_conversions as (
# MAGIC  SELECT avg(to_get_avg.us_to_cad) AS avg_us_to_cad,
# MAGIC     avg(to_get_avg.cad_to_us) AS avg_cad_to_us
# MAGIC    FROM to_get_avg
# MAGIC    ),
# MAGIC
# MAGIC Combined_Loads_mine as (
# MAGIC    SELECT DISTINCT plan_combination_projection.relay_reference_number_one,
# MAGIC     plan_combination_projection.relay_reference_number_two,
# MAGIC     plan_combination_projection.resulting_plan_id,
# MAGIC     canonical_plan_projection.relay_reference_number AS combine_new_rrn,
# MAGIC     canonical_plan_projection.mode,
# MAGIC     b.tender_on_behalf_of_id,
# MAGIC     'COMBINED'::string AS combined,
# MAGIC     COALESCE(one_money.one_money, 0::double) AS one_money,
# MAGIC     COALESCE(one_lhc_money.one_lhc_money, 0::double) AS one_lhc_money,
# MAGIC     COALESCE(one_fuel_money.one_fuel_money, 0::double) AS one_fuel_money,
# MAGIC     COALESCE(two_money.two_money, 0::double ) AS two_money,
# MAGIC     COALESCE(two_lhc_money.two_lhc_money, 0::double) AS two_lhc_money,
# MAGIC     COALESCE(two_fuel_money.two_fuel_money, 0::double) AS two_fuel_money,
# MAGIC     COALESCE(one_money.one_money, 0::double) + COALESCE(two_money.two_money, 0::double) AS total_load_rev,
# MAGIC     COALESCE(one_lhc_money.one_lhc_money, 0::double) + COALESCE(two_lhc_money.two_lhc_money, 0::double) AS total_load_lhc_rev,
# MAGIC     COALESCE(one_fuel_money.one_fuel_money, 0::double) + COALESCE(two_fuel_money.two_fuel_money, 0::double) AS total_load_fuel_rev,
# MAGIC     COALESCE(carrier_money.carrier_money, 0::double) AS final_carrier_rate,
# MAGIC     COALESCE(carrier_linehaul.carrier_linehaul, 0::double) AS carrier_linehaul,
# MAGIC     COALESCE(fuel_surcharge.fuel_surcharge, 0::double) AS carrier_fuel_surcharge
# MAGIC    FROM plan_combination_projection
# MAGIC      JOIN canonical_plan_projection ON plan_combination_projection.resulting_plan_id::string = canonical_plan_projection.plan_id::string
# MAGIC      LEFT JOIN booking_projection b ON canonical_plan_projection.relay_reference_number = b.relay_reference_number
# MAGIC      LEFT JOIN LATERAL ( SELECT sum(moneying_billing_party_transaction.amount) FILTER (WHERE moneying_billing_party_transaction.`voided?` = false)::double / 100.00::double AS one_money,
# MAGIC             moneying_billing_party_transaction.relay_reference_number
# MAGIC            FROM moneying_billing_party_transaction
# MAGIC           WHERE plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC           GROUP BY moneying_billing_party_transaction.relay_reference_number) one_money ON true
# MAGIC      LEFT JOIN LATERAL ( SELECT sum(moneying_billing_party_transaction.amount) FILTER (WHERE 1 = 1 AND moneying_billing_party_transaction.charge_code::string = 'fuel_surcharge'::string AND moneying_billing_party_transaction.`voided?` = false)::double / 100.00::double AS one_fuel_money,
# MAGIC             moneying_billing_party_transaction.relay_reference_number
# MAGIC            FROM moneying_billing_party_transaction
# MAGIC           WHERE plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC           GROUP BY moneying_billing_party_transaction.relay_reference_number) one_fuel_money ON true
# MAGIC      LEFT JOIN LATERAL ( SELECT sum(moneying_billing_party_transaction.amount) FILTER (WHERE 1 = 1 AND moneying_billing_party_transaction.charge_code::string = 'linehaul'::string AND moneying_billing_party_transaction.`voided?` = false)::double / 100.00::double AS one_lhc_money,
# MAGIC             moneying_billing_party_transaction.relay_reference_number
# MAGIC            FROM moneying_billing_party_transaction
# MAGIC           WHERE plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC           GROUP BY moneying_billing_party_transaction.relay_reference_number) one_lhc_money ON true
# MAGIC      LEFT JOIN LATERAL ( SELECT sum(moneying_billing_party_transaction.amount) FILTER (WHERE 1 = 1 AND moneying_billing_party_transaction.charge_code::string = 'linehaul'::string AND moneying_billing_party_transaction.`voided?` = false)::double / 100.00::double  AS two_lhc_money,
# MAGIC             moneying_billing_party_transaction.relay_reference_number
# MAGIC            FROM moneying_billing_party_transaction
# MAGIC           WHERE plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC           GROUP BY moneying_billing_party_transaction.relay_reference_number) two_lhc_money ON true
# MAGIC      LEFT JOIN LATERAL ( SELECT sum(moneying_billing_party_transaction.amount) FILTER (WHERE 1 = 1 AND moneying_billing_party_transaction.charge_code::string = 'fuel_surcharge'::string AND moneying_billing_party_transaction.`voided?` = false)::double  / 100.00::double  AS two_fuel_money,
# MAGIC             moneying_billing_party_transaction.relay_reference_number
# MAGIC            FROM moneying_billing_party_transaction
# MAGIC           WHERE plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC           GROUP BY moneying_billing_party_transaction.relay_reference_number) two_fuel_money ON true
# MAGIC      LEFT JOIN LATERAL ( SELECT sum(moneying_billing_party_transaction.amount) FILTER (WHERE moneying_billing_party_transaction.`voided?` = false)::double  / 100.00::double  AS two_money,
# MAGIC             moneying_billing_party_transaction.relay_reference_number
# MAGIC            FROM moneying_billing_party_transaction
# MAGIC           WHERE plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC           GROUP BY moneying_billing_party_transaction.relay_reference_number) two_money ON true
# MAGIC      LEFT JOIN LATERAL ( SELECT sum(vendor_transaction_projection.amount) FILTER (WHERE vendor_transaction_projection.`voided?` = false)::double / 100.00::double AS carrier_money,
# MAGIC             vendor_transaction_projection.relay_reference_number
# MAGIC            FROM vendor_transaction_projection
# MAGIC           WHERE canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC           GROUP BY vendor_transaction_projection.relay_reference_number) carrier_money ON true
# MAGIC      LEFT JOIN LATERAL ( SELECT sum(vendor_transaction_projection.amount) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'linehaul'::string)::double / 100.00::double AS carrier_linehaul,
# MAGIC             vendor_transaction_projection.relay_reference_number
# MAGIC            FROM vendor_transaction_projection
# MAGIC           WHERE canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC           GROUP BY vendor_transaction_projection.relay_reference_number) carrier_linehaul ON true
# MAGIC      LEFT JOIN LATERAL ( SELECT sum(vendor_transaction_projection.amount) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'fuel_surcharge'::string)::double / 100.00::double AS fuel_surcharge,
# MAGIC             vendor_transaction_projection.relay_reference_number
# MAGIC            FROM vendor_transaction_projection
# MAGIC           WHERE canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC           GROUP BY vendor_transaction_projection.relay_reference_number) fuel_surcharge ON true
# MAGIC   WHERE plan_combination_projection.is_combined = true --limit 100
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC -------------------------------------------- relay_carrier_money --------------------------------------------------
# MAGIC
# MAGIC  relay_carrier_money as (
# MAGIC  SELECT DISTINCT b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::numeric THEN NULL::numeric
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false) / 100.00::double , 0::double ) AS total_carrier_rate,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::numeric THEN NULL::numeric
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'linehaul'::string) / 100.00::double , 0::double ) AS lhc_carrier_rate,
# MAGIC     COALESCE(sum(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency::string = 'CAD'::string THEN vendor_transaction_projection.amount::double  * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::numeric THEN NULL::numeric
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)::double 
# MAGIC             ELSE vendor_transaction_projection.amount::double 
# MAGIC         END) FILTER (WHERE 1 = 1 AND vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code::string = 'fuel_surcharge'::string) / 100.00::double , 0::double ) AS fsc_carrier_rate,
# MAGIC    COALESCE(
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency = 'CAD' THEN 
# MAGIC                 vendor_transaction_projection.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN canada_conversions.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE canada_conversions.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE 
# MAGIC                 vendor_transaction_projection.amount
# MAGIC         END
# MAGIC     ) 
# MAGIC     FILTER (
# MAGIC         WHERE vendor_transaction_projection.`voided?` = false AND 
# MAGIC               vendor_transaction_projection.charge_code NOT IN ('linehaul', 'fuel_surcharge')
# MAGIC     ) / 100.0, 
# MAGIC     0.0
# MAGIC ) AS acc_carrier_rate
# MAGIC    FROM bronze.booking_projection b
# MAGIC      LEFT JOIN bronze.canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC      LEFT JOIN bronze.vendor_transaction_projection ON b.booking_id = vendor_transaction_projection.booking_id
# MAGIC      LEFT JOIN bronze.canada_conversions ON vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date
# MAGIC      JOIN average_conversions ON 1 = 1
# MAGIC   WHERE 1 = 1 AND b.status::string <> 'cancelled'::string AND (canonical_plan_projection.mode::string <> 'ltl'::string OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status::string <> 'voided'::string OR canonical_plan_projection.status IS NULL)
# MAGIC   GROUP BY b.booking_id, b.relay_reference_number, b.status
# MAGIC   
# MAGIC UNION
# MAGIC
# MAGIC  SELECT DISTINCT b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL'::string AS ltl_or_tl,
# MAGIC     b.projected_expense::numeric / 100.00 AS total_carrier_rate,
# MAGIC     b.carrier_linehaul_expense::numeric / 100.00 AS lhc_carrier_rate,
# MAGIC     b.carrier_fuel_expense::numeric / 100.00 AS fsc_carrier_rate,
# MAGIC     b.carrier_accessorial_expense::numeric / 100.00 AS acc_carrier_rate
# MAGIC    FROM big_export_projection b
# MAGIC      LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC   WHERE 1 = 1 AND b.dispatch_status::string <> 'Cancelled'::string AND (canonical_plan_projection.mode::string = 'ltl'::string OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status::string <> 'voided'::string OR canonical_plan_projection.status IS NULL)
# MAGIC ),
# MAGIC
# MAGIC -- Select * from Relay_Carrier_Rate limit 100
# MAGIC invoicing_cred as(
# MAGIC  
# MAGIC  SELECT DISTINCT invoicing_credits.relay_reference_number,
# MAGIC     sum(
# MAGIC         CASE
# MAGIC             WHEN invoicing_credits.currency::string = 'CAD'::String THEN invoicing_credits.total_amount::numeric * COALESCE(
# MAGIC             CASE
# MAGIC                 WHEN canada_conversions.us_to_cad = 0::numeric THEN NULL::numeric
# MAGIC                 ELSE canada_conversions.us_to_cad
# MAGIC             END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE invoicing_credits.total_amount::numeric
# MAGIC         END) / 100.00 AS invoicing_cred
# MAGIC    FROM invoicing_credits
# MAGIC      LEFT JOIN canada_conversions ON invoicing_credits.credited_at::date = canada_conversions.ship_date
# MAGIC      JOIN average_conversions ON 1 = 1
# MAGIC   GROUP BY invoicing_credits.relay_reference_number),
# MAGIC
# MAGIC --------------------------------------------------------------- Relay Customer Rate -----------------------------------------------------------------------
# MAGIC relay_customer_money as (
# MAGIC SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.0, 0.0) AS total_cust_rate_wo_cred,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.0, 0.0) AS customer_lhc,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.0, 0.0) AS customer_fsc,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND (m.charge_code NOT IN ('linehaul', 'fuel_surcharge'))) / 100.0, 0.0) AS customer_acc,
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0.0) AS invoicing_cred,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.0, 0.0) - COALESCE(invoicing_cred.invoicing_cred, 0.0) AS final_customer_rate,
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code = 'stop_off') AS stop_off_acc,
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code IN ('detention', 'detention_at_origin', 'detention_at_destination')) AS det_acc
# MAGIC FROM booking_projection b
# MAGIC LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
# MAGIC LEFT JOIN canada_conversions cm ON m.incurred_at::date = cm.ship_date
# MAGIC JOIN average_conversions ON 1 = 1
# MAGIC LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC WHERE b.status <> 'cancelled' AND 
# MAGIC       (canonical_plan_projection.mode <> 'ltl' OR canonical_plan_projection.mode IS NULL) AND 
# MAGIC       (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL) AND 
# MAGIC       b.booking_id <> 7370325 AND 
# MAGIC       m.`voided?` = false
# MAGIC GROUP BY b.booking_id, b.relay_reference_number, b.status, invoicing_cred.invoicing_cred
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.0, 0.0) AS total_cust_rate_wo_cred,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.0, 0.0) AS customer_lhc,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.0, 0.0) AS customer_fsc,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND (m.charge_code NOT IN ('linehaul', 'fuel_surcharge'))) / 100.0, 0.0) AS customer_acc,
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0.0) AS invoicing_cred,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.0, 0.0) - COALESCE(invoicing_cred.invoicing_cred, 0.0) AS final_customer_rate,
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code = 'stop_off') AS stop_off_acc,
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code IN ('detention', 'detention_at_origin', 'detention_at_destination')) AS det_acc
# MAGIC FROM big_export_projection b
# MAGIC LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT JOIN moneying_billing_party_transaction m ON b.load_number = m.relay_reference_number
# MAGIC LEFT JOIN canada_conversions cm ON m.incurred_at::date = cm.ship_date
# MAGIC JOIN average_conversions ON 1 = 1
# MAGIC LEFT JOIN invoicing_cred ON b.load_number = invoicing_cred.relay_reference_number
# MAGIC WHERE b.dispatch_status <> 'Cancelled' AND 
# MAGIC       (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL) AND 
# MAGIC       (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL) AND 
# MAGIC       (b.customer_id NOT IN ('hain', 'deb', 'roland')) AND 
# MAGIC       m.`voided?` = false
# MAGIC GROUP BY b.load_number, b.dispatch_status, invoicing_cred.invoicing_cred --limit 100
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     e.load_number AS booking_id,
# MAGIC     e.load_number AS relay_reference_number,
# MAGIC     e.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC     (e.carrier_accessorial_expense + CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) + CEIL(CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) *
# MAGIC         CASE e.carrier_linehaul_expense
# MAGIC             WHEN 0 THEN 0
# MAGIC             ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
# MAGIC         END)) / 100 AS total_cust_rate_wo_cred,
# MAGIC     CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) / 100 AS customer_lhc,
# MAGIC     CEIL(CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) *
# MAGIC         CASE e.carrier_linehaul_expense
# MAGIC             WHEN 0 THEN 0
# MAGIC             ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
# MAGIC         END) / 100 AS customer_fsc,
# MAGIC     e.carrier_accessorial_expense / 100 AS customer_acc,
# MAGIC     0 AS invoicing_cred,
# MAGIC     (e.carrier_accessorial_expense + CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) + CEIL(CEIL(
# MAGIC         CASE e.customer_id
# MAGIC             WHEN 'deb' THEN
# MAGIC                 CASE e.carrier_id
# MAGIC                     WHEN '300003979' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     ELSE
# MAGIC                         CASE
# MAGIC                             WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                             ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         END
# MAGIC                 END
# MAGIC             WHEN 'hain' THEN GREATEST(e.carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC             ELSE 0
# MAGIC         END + e.carrier_linehaul_expense) *
# MAGIC         CASE e.carrier_linehaul_expense
# MAGIC             WHEN 0 THEN 0
# MAGIC             ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
# MAGIC         END)) / 100 AS final_customer_rate,
# MAGIC     NULL AS stop_off_acc,
# MAGIC     NULL AS det_acc
# MAGIC FROM big_export_projection e
# MAGIC LEFT JOIN canonical_plan_projection ON e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC WHERE e.dispatch_status <> 'Cancelled' AND 
# MAGIC       (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL) AND 
# MAGIC       (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL) AND 
# MAGIC       (e.customer_id IN ('hain', 'deb', 'roland')) ---limit 100
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.0, 0.0) AS total_cust_rate_wo_cred,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.0, 0.0) AS customer_lhc,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.0, 0.0) AS customer_fsc,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND (m.charge_code NOT IN ('linehaul', 'fuel_surcharge'))) / 100.0, 0.0) AS customer_acc,
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0.0) AS invoicing_cred,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.0, 0.0) - COALESCE(invoicing_cred.invoicing_cred, 0.0) AS final_customer_rate,
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code = 'stop_off') AS stop_off_acc,
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code IN ('detention', 'detention_at_origin', 'detention_at_destination')) AS det_acc
# MAGIC FROM booking_projection b
# MAGIC JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_one
# MAGIC LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
# MAGIC LEFT JOIN canada_conversions cm ON m.incurred_at::date = cm.ship_date
# MAGIC JOIN average_conversions ON 1 = 1
# MAGIC LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC WHERE m.`voided?` = false
# MAGIC GROUP BY b.booking_id, b.relay_reference_number, b.status, invoicing_cred.invoicing_cred --limit 100
# MAGIC
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.0, 0.0) AS total_cust_rate_wo_cred,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.0, 0.0) AS customer_lhc,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.0, 0.0) AS customer_fsc,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false AND (m.charge_code NOT IN ('linehaul', 'fuel_surcharge'))) / 100.0, 0.0) AS customer_acc,
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0.0) AS invoicing_cred,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN m.currency = 'CAD' THEN 
# MAGIC                 m.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE cm.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE m.amount
# MAGIC         END) FILTER (WHERE m.`voided?` = false) / 100.0, 0.0) - COALESCE(invoicing_cred.invoicing_cred, 0.0) AS final_customer_rate,
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code = 'stop_off') AS stop_off_acc,
# MAGIC     SUM(m.amount) FILTER (WHERE m.charge_code IN ('detention', 'detention_at_origin', 'detention_at_destination')) AS det_acc
# MAGIC FROM booking_projection b
# MAGIC JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_two
# MAGIC LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
# MAGIC LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
# MAGIC JOIN average_conversions ON 1 = 1
# MAGIC LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC WHERE m.`voided?` = false
# MAGIC GROUP BY b.booking_id, b.relay_reference_number, b.status, invoicing_cred.invoicing_cred ) ,
# MAGIC
# MAGIC ------------------------------------------------------------------------ Bounced_Data -----------------------------------------------------------------
# MAGIC  Bounced_Data AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         CAST(pu_appt_date AS DATE) AS PU_Appt, 
# MAGIC         carrier_projection.carrier_name AS bounced_carrier,
# MAGIC         carrier_projection.dot_number AS Bounced_DOT_Number,
# MAGIC         booking_projection.booking_id AS Bounced_Booking_ID,
# MAGIC         booking_projection.relay_reference_number, 
# MAGIC         booked_by_name AS bounced_rep,
# MAGIC         new_office_lookup_dup.new_office_dup AS Bounced_Carrier_Office,
# MAGIC       FROM_UTC_TIMESTAMP(CAST(booking_projection.bounced_at AS TIMESTAMP), 'America/New_York') as  Bounced_At,
# MAGIC         FROM_UTC_TIMESTAMP(CAST(booking_projection.booked_at AS TIMESTAMP), 'America/New_York') as bounced_book_time,
# MAGIC         booked_total_carrier_rate_amount / 100.0 AS Bounced_Rate
# MAGIC     FROM 
# MAGIC         booking_projection
# MAGIC     JOIN 
# MAGIC         customer_profile_projection 
# MAGIC         ON booking_projection.tender_on_behalf_of_id = customer_profile_projection.customer_slug 
# MAGIC     LEFT JOIN 
# MAGIC         relay_users ON LOWER(booking_projection.booked_by_name) = LOWER(relay_users.full_name) AND relay_users.`active?` = 'true'
# MAGIC     JOIN 
# MAGIC         carrier_projection ON carrier_projection.carrier_id = booking_projection.booked_carrier_id
# MAGIC     LEFT JOIN 
# MAGIC         new_office_lookup ON new_office_lookup.old_office = customer_profile_projection.profit_center
# MAGIC     LEFT JOIN 
# MAGIC         new_office_lookup_dup ON new_office_lookup_dup.old_office_dub = relay_users.office_id
# MAGIC     LEFT JOIN 
# MAGIC         bronze.master_date ON master_date.booking_id = booking_projection.booking_id
# MAGIC     LEFT JOIN 
# MAGIC         relay_carrier_money ON relay_carrier_money.booking_id = booking_projection.booking_id
# MAGIC     LEFT JOIN 
# MAGIC         relay_customer_money ON relay_customer_money.booking_id = booking_projection.booking_id
# MAGIC     JOIN 
# MAGIC         brokerageprod.analytics.dim_date ON CAST(pu_appt_date AS DATE) = CAST(dim_date.Calendar_Date AS DATE)
# MAGIC     WHERE 
# MAGIC         tender_on_behalf_of_id NOT IN ('target')
# MAGIC         AND booked_total_carrier_rate_amount / 100.0 > 150
# MAGIC         AND booking_projection.booked_carrier_id != '1fd9f9d6-7dde-47e0-881f-0384929f7965'
# MAGIC         AND CAST(bounced_at AS DATE) = CAST(pu_appt_date AS DATE)
# MAGIC ),
# MAGIC
# MAGIC Booked_data  AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         booking_projection.relay_reference_number,
# MAGIC         booking_projection.status,
# MAGIC         carrier_projection.carrier_name AS Carrier,
# MAGIC         carrier_projection.dot_number AS DOT_Number,
# MAGIC         customer_profile_projection.lawson_id AS Lawson_ID,
# MAGIC         customer_profile_projection.bill_to_name AS Bill_To_Name,
# MAGIC         customer_profile_projection.customer_name AS Customer_Name,
# MAGIC         dim_date.period_year AS Period,
# MAGIC         new_office_lookup.new_office AS Cust_Office,
# MAGIC         booking_projection.booking_id AS Booking_ID,
# MAGIC         booked_by_name,
# MAGIC         new_office_dup AS Carrier_Office,
# MAGIC         FROM_UTC_TIMESTAMP(CAST(booking_projection.booked_at AS TIMESTAMP), 'America/New_York') AS booked_at,
# MAGIC         -- final_customer_rate,
# MAGIC         -- total_carrier_rate,
# MAGIC         booked_total_carrier_rate_amount / 100.0 AS Booked_Rate
# MAGIC     FROM 
# MAGIC         brokerageprod.bronze.booking_projection
# MAGIC     JOIN 
# MAGIC        brokerageprod.bronze.customer_profile_projection ON booking_projection.tender_on_behalf_of_id = customer_profile_projection.customer_slug 
# MAGIC     LEFT JOIN 
# MAGIC         brokerageprod.bronze.relay_users ON LOWER(booking_projection.booked_by_name) = LOWER(relay_users.full_name) AND relay_users.`active?` = 'true'
# MAGIC     JOIN 
# MAGIC         brokerageprod.bronze.carrier_projection ON carrier_projection.carrier_id = booking_projection.booked_carrier_id
# MAGIC     LEFT JOIN 
# MAGIC         brokerageprod.bronze.new_office_lookup ON new_office_lookup.old_office = customer_profile_projection.profit_center
# MAGIC     LEFT JOIN 
# MAGIC         brokerageprod.bronze.new_office_lookup_dup ON new_office_lookup_dup.old_office_dub = relay_users.office_id
# MAGIC     LEFT JOIN 
# MAGIC         brokerageprod.bronze.master_date ON master_date.booking_id = booking_projection.booking_id
# MAGIC     LEFT JOIN 
# MAGIC         relay_carrier_money ON relay_carrier_money.booking_id = booking_projection.booking_id
# MAGIC     LEFT JOIN 
# MAGIC        relay_customer_money ON relay_customer_money.booking_id = booking_projection.booking_id
# MAGIC     JOIN 
# MAGIC         brokerageprod.analytics.dim_date ON CAST(pu_appt_date AS DATE) = CAST(dim_date.Calendar_Date AS DATE)
# MAGIC     WHERE 
# MAGIC         tender_on_behalf_of_id NOT IN ('target')
# MAGIC         AND booked_total_carrier_rate_amount / 100.0 > 250
# MAGIC         AND booking_projection.booked_carrier_id != '1fd9f9d6-7dde-47e0-881f-0384929f7965'
# MAGIC         and booking_projection.status IN ('booked')
# MAGIC ),
# MAGIC Final_Code AS (
# MAGIC   SELECT
# MAGIC     bounced_data.relay_reference_number AS Load_ID,
# MAGIC     booked_data.booking_id AS Booking_ID,
# MAGIC     booked_data.Booked_At as Booked_Date,
# MAGIC     booked_data.Carrier,
# MAGIC     booked_data.Carrier_Office,
# MAGIC     booked_data.DOT_Number,
# MAGIC     booked_data.booked_by_name as Carrier_Rep,
# MAGIC     bounced_data.bounced_at AS Bounced_Date,
# MAGIC     bounced_data.bounced_carrier AS Bounced_Carrier,
# MAGIC     bounced_data.Bounced_Carrier_Office,
# MAGIC     bounced_data.Bounced_DOT_Number,
# MAGIC     bounced_data.Bounced_Rep as Bounced_Carrier_Rep,
# MAGIC     booked_data.Period,
# MAGIC     booked_data.Customer_Name,
# MAGIC     booked_data.Bill_To_Name,
# MAGIC     booked_data.Lawson_ID,
# MAGIC     booked_data.Cust_Office,
# MAGIC     booked_data.Booked_Rate,
# MAGIC     bounced_data.Bounced_Rate,
# MAGIC     Booked_Rate - Bounced_Rate as Book_Rate_Increase,
# MAGIC     PU_Appt as PickUp_Appointment_Date
# MAGIC   FROM
# MAGIC     booked_data
# MAGIC     JOIN Bounced_Data ON booked_data.relay_reference_number = Bounced_Data.relay_reference_number
# MAGIC )
# MAGIC SELECT 
# MAGIC     final_code.Load_ID,
# MAGIC     Booking_ID,
# MAGIC     final_code.Booked_Date,
# MAGIC       case when sp.Customer_Master is null then 'Unknown' else sp.customer_master end as Mapped_Shipper_Master,
# MAGIC       case when sp.Verified_AM is null then 'Unknown'else sp.Verified_AM end as Mapped_Shipper_Rep ,
# MAGIC   case when sp.Verified_Office is null then 'Unknown' else sp.Verified_Office end as  Mapped_Shipper_Office,
# MAGIC   Case when tm.Team is null then 'Unknown' 
# MAGIC   when tm.Team ='Geen' then 'Green'
# MAGIC   else  tm.Team end as Team,
# MAGIC     Carrier, 
# MAGIC     final_code.Carrier_Office, 
# MAGIC     DOT_Number,
# MAGIC     Carrier_Rep, 
# MAGIC     final_code.Bounced_Date,
# MAGIC     Bounced_Carrier, 
# MAGIC     Bounced_Carrier_Office, 
# MAGIC     Bounced_DOT_Number,
# MAGIC     Bounced_Carrier_Rep, 
# MAGIC     Period,
# MAGIC     final_code.Customer_Name, 
# MAGIC     Bill_To_Name, 
# MAGIC     final_code.Lawson_ID,
# MAGIC     Cust_Office, 
# MAGIC     Booked_Rate,
# MAGIC     Bounced_Rate,
# MAGIC     Book_Rate_Increase,
# MAGIC     final_code.PickUp_Appointment_Date,
# MAGIC     CONCAT_WS(sp.Customer_Master, sp.Verified_AM, fl.ship_date) AS SameDayBounceKey
# MAGIC   FROM Final_Code 
# MAGIC LEFT JOIN bronze.shipper_mapping sp 
# MAGIC on lower(Final_Code.customer_name) = lower(customer_master)
# MAGIC or lower(Final_Code.bill_to_name) = lower(customer_master)
# MAGIC LEFT JOIN brokerageprod.analytics.am_team_mapping tm 
# MAGIC on sp.Verified_AM = tm.`Account Manager`
# MAGIC LEFT JOIN analytics.fact_load_genai_nonsplit fl
# MAGIC on final_code.Load_ID=fl.load_id
# MAGIC where sp.is_deleted=0