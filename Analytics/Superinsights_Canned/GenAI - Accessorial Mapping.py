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
# MAGIC ##CUSTOMER ACCESSORIAL MAPPING

# COMMAND ----------

# MAGIC %sql
# MAGIC set ansi_mode = false;
# MAGIC use schema bronze;
# MAGIC create or replace table superinsight.Accessorial_Carrier as
# MAGIC
# MAGIC   with to_get_avg as (
# MAGIC     select
# MAGIC       distinct ship_date,
# MAGIC       conversion as us_to_cad,
# MAGIC       us_to_cad as cad_to_us
# MAGIC     from
# MAGIC       bronze.canada_conversions
# MAGIC     order by
# MAGIC       ship_date desc
# MAGIC     limit
# MAGIC       7
# MAGIC   ), average_conversions as (
# MAGIC     select
# MAGIC       avg(us_to_cad) as avg_us_to_cad,
# MAGIC       avg(cad_to_us) as avg_cad_to_us
# MAGIC     from
# MAGIC       to_get_avg),
# MAGIC relay_carrier_access as (SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     charge_code,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency = 'CAD' THEN 
# MAGIC                 vendor_transaction_projection.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN canada_conversions.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE canada_conversions.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE vendor_transaction_projection.amount
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false) / 100.00, 0) AS total_carrier_rate
# MAGIC         FROM 
# MAGIC     booking_projection b
# MAGIC LEFT JOIN 
# MAGIC     canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT JOIN 
# MAGIC     vendor_transaction_projection ON b.booking_id = vendor_transaction_projection.booking_id
# MAGIC LEFT JOIN 
# MAGIC     canada_conversions ON DATE(vendor_transaction_projection.incurred_at) = canada_conversions.ship_date
# MAGIC JOIN 
# MAGIC     average_conversions ON TRUE
# MAGIC WHERE 
# MAGIC     b.status not in ('cancelled','bounced') AND (canonical_plan_projection.mode <> 'ltl' OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC GROUP BY 
# MAGIC     b.booking_id, b.relay_reference_number, b.status,charge_code),
# MAGIC  all_accessorials AS (
# MAGIC   SELECT
# MAGIC     p.id,
# MAGIC     accessorial1 AS accessorial_code,
# MAGIC     Carrier_accessorial_rate1 AS accessorial_rate,
# MAGIC     'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial2, Carrier_accessorial_rate2,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial3, Carrier_accessorial_rate3,'ALJEX' as TMS 
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial4, Carrier_accessorial_rate4 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial5, Carrier_accessorial_rate5 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial6, Carrier_accessorial_rate6 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial7, Carrier_accessorial_rate7 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial8, Carrier_accessorial_rate8 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   union all
# MAGIC   select relay_reference_number,charge_code,total_carrier_rate,'RELAY' as tms from relay_carrier_access
# MAGIC
# MAGIC ),
# MAGIC filtered_accessorials AS (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     accessorial_code,
# MAGIC     TMS,
# MAGIC     CASE 
# MAGIC       WHEN accessorial_rate LIKE '%:%' THEN 0.00
# MAGIC       WHEN accessorial_rate RLIKE '^[0-9]+(\\.[0-9]+)?$' THEN CAST(accessorial_rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS rate
# MAGIC   FROM all_accessorials
# MAGIC ),
# MAGIC carrier_grouping as ( select distinct
# MAGIC   id,
# MAGIC   tms,
# MAGIC   CASE 
# MAGIC       WHEN accessorial_code IN ('ADMN', 'CUST', 'INSU', 'EXPD', 'PEAK', 'RECL', 'TRAC', 'NA', 'ADWI', 'QST','expedite_shipment', 'holiday','claim', 'quick_pay')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS admin_charge,
# MAGIC      CASE 
# MAGIC       WHEN accessorial_code IN ('CROS','ORM','out_of_route_miles','consolidation')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS Transit_charge,
# MAGIC      CASE 
# MAGIC       WHEN accessorial_code IN ('ATTE', 'DETD', 'DETN', 'AFTE', 'DELV', 'DRAS', 'DROP', 'HIGH', 'DETO', 'LATE', 'LIMI', 'NIPU', 'NOTI', 'ONDE', 'ONPU', 'PICK', 'REDI', 'RELO', 'SHRK', 'INSD', 'LAYO', 'TNLD', 'NA', 'DEMU', 'DETP', 'PERD', 'RECN', 'PCGN', 'REDE', 'REMO', 'RESI', 'RETP', 'TCF', 'TERM', 'SOC', 'TARP', 'TMDR', 'UNL', 'YARD', 'BORD','detention_at_destination', 'detention', 'detention_at_origin', 'inside_pickup_or_delivery', 'layover', 'lane_surcharge', 'per_diem', 'reconsignment_fee', 'redelivery_fee', 'remote_access', 'residential_delivery', 'return_product', 'stop_off', 'tarp_charge', 'team_charge', 'driver_assist', 'yard_pull','detention_at_destination', 'yard_storage', 'redelivery', 'inside_delivery', 'extra_stop', 'weight_fee', 'layover', 'reconsignment_fee', 'pickup_appointment', 'yard_pull', 'redelivery_fee', 'on_time_pickup', 'team_charge', 'delivery_charge', 'hawaiian_will_call', 'on_time_delivery', 'single_shipment', 'weekend', 'limited_access_delivery', 'escort_fee', 'drop_trailer', 'expedite_shipment', 'delay_charge', 'airport_pickup', 'detention', 'delivery_appointment', 'after_hours_charge', 'appointment', 'limited_access_pickup', 'border_crossing_fee', 'detention_at_origin', 'rescheduling_fee', 'chassis_truck', 'liftgate_delivery')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS Delpick_charge,
# MAGIC      CASE 
# MAGIC       WHEN accessorial_code IN ('BOB', 'CHAS', 'CRAN', 'FLIP', 'GROC', 'CHSP', 'TRAW','REEF','reefer_fuel','pallet_jack_fee', 'truck_order_not_used', 'tarp_charge', 'pallet_exchange_fee', 'liftgate', 'capacity_load_trailer', 'tanker_endorsement', 'equipment', 'reefer', 'protect_from_freeze', 'hazmat', 'per_diem')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS Equipment_charge,
# MAGIC     CASE 
# MAGIC       WHEN accessorial_code IN ('LABL', 'LIFT', 'LUMP', 'PAEX', 'PIEL', 'RAMP', 'SPO', 'DISP', 'DUNN', 'PIER', 'REWO', 'PREP', 'PROT', 'PRPU', 'REWE', 'SORT', 'CUBE', 'DEFI', 'YSTO','liftgate_delivery', 'lumper', 'pallet_exchange', 'rework','ramp_storage', 'rework', 'driver_assistance', 'forklift', 'bulkheaid', 'lumper_service', 'handling_and_labeling')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS load_charge,
# MAGIC     CASE 
# MAGIC       WHEN accessorial_code IN ('ADJU', 'CORR', 'DIS', 'ESCO', 'MISC','storage', 'air_fuel_surcharge', 'accessorial_charge', 'other', 'tracking_opt_in', 'holiday', 'unmapped', 'trailer_cleaning')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS missi_charge,
# MAGIC     CASE 
# MAGIC       WHEN accessorial_code IN ('CALI', 'HAZM', 'OVWT', 'ADDP', 'TICK', 'VCB', 'TOLL', 'HVWH', 'OVER', 'OVLG', 'QUAD', 'WEIG','california_compliance', 'hazmat', 'overweight_citation', 'citation', 'cross_border', 'rescheduling_fee', 'toll_charge', 'overdimensional','california_compliance_surcharge', 'liquor_permit', 'overweight', 'excessive_value', 'citation', 'out_of_route_miles', 'toll_charge', 'additional_insurance', 'permit', 'customs_or_in_bond_freight_charge')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS permit_charge
# MAGIC     from filtered_accessorials
# MAGIC )
# MAGIC SELECT id as Load_number,tms,sum(admin_charge)AdminFees_Carrier, sum(Transit_charge) as TransitandRouting_Carrier,sum(Delpick_charge) as DelandPickup_Carrier,sum(Equipment_charge) as EquipmentandVehicle_Carrier,sum(load_charge) as Loadandunload_carrier ,sum(missi_charge) as Miscellaneous_Carrier ,sum(permit_charge) as PermitsandCompliance_Carrier from carrier_grouping group by id,tms
# MAGIC -- select count(*),id,tms from final group by id,tms HAving count(*)>1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Carrier Accessorial Grouping

# COMMAND ----------

# MAGIC %sql
# MAGIC set ansi_mode = false;
# MAGIC use schema bronze;
# MAGIC create or replace table superinsight.Accessorial_Carrier as
# MAGIC
# MAGIC   with to_get_avg as (
# MAGIC     select
# MAGIC       distinct ship_date,
# MAGIC       conversion as us_to_cad,
# MAGIC       us_to_cad as cad_to_us
# MAGIC     from
# MAGIC       bronze.canada_conversions
# MAGIC     order by
# MAGIC       ship_date desc
# MAGIC     limit
# MAGIC       7
# MAGIC   ), average_conversions as (
# MAGIC     select
# MAGIC       avg(us_to_cad) as avg_us_to_cad,
# MAGIC       avg(cad_to_us) as avg_cad_to_us
# MAGIC     from
# MAGIC       to_get_avg),
# MAGIC relay_carrier_access as (SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     charge_code,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(
# MAGIC         CASE
# MAGIC             WHEN vendor_transaction_projection.currency = 'CAD' THEN 
# MAGIC                 vendor_transaction_projection.amount * COALESCE(
# MAGIC                     CASE
# MAGIC                         WHEN canada_conversions.us_to_cad = 0 THEN NULL
# MAGIC                         ELSE canada_conversions.us_to_cad
# MAGIC                     END, average_conversions.avg_cad_to_us)
# MAGIC             ELSE vendor_transaction_projection.amount
# MAGIC         END) FILTER (WHERE vendor_transaction_projection.`voided?` = false) / 100.00, 0) AS total_carrier_rate
# MAGIC         FROM 
# MAGIC     booking_projection b
# MAGIC LEFT JOIN 
# MAGIC     canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT JOIN 
# MAGIC     vendor_transaction_projection ON b.booking_id = vendor_transaction_projection.booking_id
# MAGIC LEFT JOIN 
# MAGIC     canada_conversions ON DATE(vendor_transaction_projection.incurred_at) = canada_conversions.ship_date
# MAGIC JOIN 
# MAGIC     average_conversions ON TRUE
# MAGIC WHERE 
# MAGIC     b.status not in ('cancelled','bounced') AND (canonical_plan_projection.mode <> 'ltl' OR canonical_plan_projection.mode IS NULL) AND (canonical_plan_projection.status <> 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC GROUP BY 
# MAGIC     b.booking_id, b.relay_reference_number, b.status,charge_code),
# MAGIC  all_accessorials AS (
# MAGIC   SELECT
# MAGIC     p.id,
# MAGIC     accessorial1 AS accessorial_code,
# MAGIC     Carrier_accessorial_rate1 AS accessorial_rate,
# MAGIC     'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial2, Carrier_accessorial_rate2,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial3, Carrier_accessorial_rate3,'ALJEX' as TMS 
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial4, Carrier_accessorial_rate4 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial5, Carrier_accessorial_rate5 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial6, Carrier_accessorial_rate6 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial7, Carrier_accessorial_rate7 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   UNION ALL
# MAGIC   SELECT p.id, accessorial8, Carrier_accessorial_rate8 ,'ALJEX' as TMS
# MAGIC   FROM bronze.projection_load_2 p 
# MAGIC   LEFT JOIN bronze.projection_load_1 
# MAGIC   ON p.id = projection_load_1.id
# MAGIC   union all
# MAGIC   select relay_reference_number,charge_code,total_carrier_rate,'RELAY' as tms from relay_carrier_access
# MAGIC
# MAGIC ),
# MAGIC filtered_accessorials AS (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     accessorial_code,
# MAGIC     TMS,
# MAGIC     CASE 
# MAGIC       WHEN accessorial_rate LIKE '%:%' THEN 0.00
# MAGIC       WHEN accessorial_rate RLIKE '^[0-9]+(\\.[0-9]+)?$' THEN CAST(accessorial_rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS rate
# MAGIC   FROM all_accessorials
# MAGIC ),
# MAGIC carrier_grouping as ( select distinct
# MAGIC   id,
# MAGIC   tms,
# MAGIC   CASE 
# MAGIC       WHEN accessorial_code IN ('ADMN', 'CUST', 'INSU', 'EXPD', 'PEAK', 'RECL', 'TRAC', 'NA', 'ADWI', 'QST','expedite_shipment', 'holiday','claim', 'quick_pay')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS admin_charge,
# MAGIC      CASE 
# MAGIC       WHEN accessorial_code IN ('CROS','ORM','out_of_route_miles','consolidation')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS Transit_charge,
# MAGIC      CASE 
# MAGIC       WHEN accessorial_code IN ('ATTE', 'DETD', 'DETN', 'AFTE', 'DELV', 'DRAS', 'DROP', 'HIGH', 'DETO', 'LATE', 'LIMI', 'NIPU', 'NOTI', 'ONDE', 'ONPU', 'PICK', 'REDI', 'RELO', 'SHRK', 'INSD', 'LAYO', 'TNLD', 'NA', 'DEMU', 'DETP', 'PERD', 'RECN', 'PCGN', 'REDE', 'REMO', 'RESI', 'RETP', 'TCF', 'TERM', 'SOC', 'TARP', 'TMDR', 'UNL', 'YARD', 'BORD','detention_at_destination', 'detention', 'detention_at_origin', 'inside_pickup_or_delivery', 'layover', 'lane_surcharge', 'per_diem', 'reconsignment_fee', 'redelivery_fee', 'remote_access', 'residential_delivery', 'return_product', 'stop_off', 'tarp_charge', 'team_charge', 'driver_assist', 'yard_pull','detention_at_destination', 'yard_storage', 'redelivery', 'inside_delivery', 'extra_stop', 'weight_fee', 'layover', 'reconsignment_fee', 'pickup_appointment', 'yard_pull', 'redelivery_fee', 'on_time_pickup', 'team_charge', 'delivery_charge', 'hawaiian_will_call', 'on_time_delivery', 'single_shipment', 'weekend', 'limited_access_delivery', 'escort_fee', 'drop_trailer', 'expedite_shipment', 'delay_charge', 'airport_pickup', 'detention', 'delivery_appointment', 'after_hours_charge', 'appointment', 'limited_access_pickup', 'border_crossing_fee', 'detention_at_origin', 'rescheduling_fee', 'chassis_truck', 'liftgate_delivery')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS Delpick_charge,
# MAGIC      CASE 
# MAGIC       WHEN accessorial_code IN ('BOB', 'CHAS', 'CRAN', 'FLIP', 'GROC', 'CHSP', 'TRAW','REEF','reefer_fuel','pallet_jack_fee', 'truck_order_not_used', 'tarp_charge', 'pallet_exchange_fee', 'liftgate', 'capacity_load_trailer', 'tanker_endorsement', 'equipment', 'reefer', 'protect_from_freeze', 'hazmat', 'per_diem')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS Equipment_charge,
# MAGIC     CASE 
# MAGIC       WHEN accessorial_code IN ('LABL', 'LIFT', 'LUMP', 'PAEX', 'PIEL', 'RAMP', 'SPO', 'DISP', 'DUNN', 'PIER', 'REWO', 'PREP', 'PROT', 'PRPU', 'REWE', 'SORT', 'CUBE', 'DEFI', 'YSTO','liftgate_delivery', 'lumper', 'pallet_exchange', 'rework','ramp_storage', 'rework', 'driver_assistance', 'forklift', 'bulkheaid', 'lumper_service', 'handling_and_labeling')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS load_charge,
# MAGIC     CASE 
# MAGIC       WHEN accessorial_code IN ('ADJU', 'CORR', 'DIS', 'ESCO', 'MISC','storage', 'air_fuel_surcharge', 'accessorial_charge', 'other', 'tracking_opt_in', 'holiday', 'unmapped', 'trailer_cleaning')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS missi_charge,
# MAGIC     CASE 
# MAGIC       WHEN accessorial_code IN ('CALI', 'HAZM', 'OVWT', 'ADDP', 'TICK', 'VCB', 'TOLL', 'HVWH', 'OVER', 'OVLG', 'QUAD', 'WEIG','california_compliance', 'hazmat', 'overweight_citation', 'citation', 'cross_border', 'rescheduling_fee', 'toll_charge', 'overdimensional','california_compliance_surcharge', 'liquor_permit', 'overweight', 'excessive_value', 'citation', 'out_of_route_miles', 'toll_charge', 'additional_insurance', 'permit', 'customs_or_in_bond_freight_charge')
# MAGIC       THEN CAST(rate AS DECIMAL(12,2))
# MAGIC       ELSE 0.00
# MAGIC     END AS permit_charge
# MAGIC     from filtered_accessorials
# MAGIC )
# MAGIC SELECT id as Load_number,tms,sum(admin_charge)AdminFees_Carrier, sum(Transit_charge) as TransitandRouting_Carrier,sum(Delpick_charge) as DelandPickup_Carrier,sum(Equipment_charge) as EquipmentandVehicle_Carrier,sum(load_charge) as Loadandunload_carrier ,sum(missi_charge) as Miscellaneous_Carrier ,sum(permit_charge) as PermitsandCompliance_Carrier from carrier_grouping group by id,tms
# MAGIC -- select count(*),id,tms from final group by id,tms HAving count(*)>1
# MAGIC