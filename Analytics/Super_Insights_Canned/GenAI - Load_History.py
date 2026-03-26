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
# MAGIC
# MAGIC USE SCHEMA bronze;
# MAGIC create or replace view bronze.Load_History_GenAI as (
# MAGIC
# MAGIC with max_schedule as
# MAGIC (SELECT DISTINCT 
# MAGIC     relay_reference_number,
# MAGIC     max(cast(scheduled_at as timestamp)) AS max_schedule
# MAGIC FROM 
# MAGIC     pickup_projection
# MAGIC GROUP BY 
# MAGIC     relay_reference_number)
# MAGIC
# MAGIC ,combined_loads_mine as (
# MAGIC SELECT DISTINCT 
# MAGIC     plan_combination_projection.relay_reference_number_one,
# MAGIC     plan_combination_projection.relay_reference_number_two,
# MAGIC     plan_combination_projection.resulting_plan_id,
# MAGIC     canonical_plan_projection.relay_reference_number AS combine_new_rrn,
# MAGIC     canonical_plan_projection.mode,
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
# MAGIC FROM plan_combination_projection
# MAGIC JOIN canonical_plan_projection 
# MAGIC     ON plan_combination_projection.resulting_plan_id = canonical_plan_projection.plan_id
# MAGIC LEFT JOIN booking_projection b 
# MAGIC     ON canonical_plan_projection.relay_reference_number = b.relay_reference_number
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT 
# MAGIC         SUM(moneying_billing_party_transaction.amount) FILTER (WHERE moneying_billing_party_transaction.`voided?` = false) / 100.0 AS one_money,
# MAGIC         moneying_billing_party_transaction.relay_reference_number
# MAGIC     FROM moneying_billing_party_transaction
# MAGIC     WHERE plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC     GROUP BY moneying_billing_party_transaction.relay_reference_number
# MAGIC ) one_money ON true
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT 
# MAGIC         SUM(moneying_billing_party_transaction.amount) FILTER (WHERE moneying_billing_party_transaction.charge_code = 'fuel_surcharge' AND moneying_billing_party_transaction.`voided?` = false) / 100.0 AS one_fuel_money,
# MAGIC         moneying_billing_party_transaction.relay_reference_number
# MAGIC     FROM moneying_billing_party_transaction
# MAGIC     WHERE plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC     GROUP BY moneying_billing_party_transaction.relay_reference_number
# MAGIC ) one_fuel_money ON true
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT 
# MAGIC         SUM(moneying_billing_party_transaction.amount) FILTER (WHERE moneying_billing_party_transaction.charge_code = 'linehaul' AND moneying_billing_party_transaction.`voided?` = false) / 100.0 AS one_lhc_money,
# MAGIC         moneying_billing_party_transaction.relay_reference_number
# MAGIC     FROM moneying_billing_party_transaction
# MAGIC     WHERE plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC     GROUP BY moneying_billing_party_transaction.relay_reference_number
# MAGIC ) one_lhc_money ON true
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT 
# MAGIC         SUM(moneying_billing_party_transaction.amount) FILTER (WHERE moneying_billing_party_transaction.charge_code = 'linehaul' AND moneying_billing_party_transaction.`voided?` = false) / 100.0 AS two_lhc_money,
# MAGIC         moneying_billing_party_transaction.relay_reference_number
# MAGIC     FROM moneying_billing_party_transaction
# MAGIC     WHERE plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC     GROUP BY moneying_billing_party_transaction.relay_reference_number
# MAGIC ) two_lhc_money ON true
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT 
# MAGIC         SUM(moneying_billing_party_transaction.amount) FILTER (WHERE moneying_billing_party_transaction.charge_code = 'fuel_surcharge' AND moneying_billing_party_transaction.`voided?` = false) / 100.0 AS two_fuel_money,
# MAGIC         moneying_billing_party_transaction.relay_reference_number
# MAGIC     FROM moneying_billing_party_transaction
# MAGIC     WHERE plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC     GROUP BY moneying_billing_party_transaction.relay_reference_number
# MAGIC ) two_fuel_money ON true
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT 
# MAGIC         SUM(moneying_billing_party_transaction.amount) FILTER (WHERE moneying_billing_party_transaction.`voided?` = false) / 100.0 AS two_money,
# MAGIC         moneying_billing_party_transaction.relay_reference_number
# MAGIC     FROM moneying_billing_party_transaction
# MAGIC     WHERE plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC     GROUP BY moneying_billing_party_transaction.relay_reference_number
# MAGIC ) two_money ON true
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT 
# MAGIC         SUM(vendor_transaction_projection.amount) FILTER (WHERE vendor_transaction_projection.`voided?` = false) / 100.0 AS carrier_money,
# MAGIC         vendor_transaction_projection.relay_reference_number
# MAGIC     FROM vendor_transaction_projection
# MAGIC     WHERE canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC     GROUP BY vendor_transaction_projection.relay_reference_number
# MAGIC ) carrier_money ON true
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT 
# MAGIC         SUM(vendor_transaction_projection.amount) FILTER (WHERE vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code = 'linehaul') / 100.0 AS carrier_linehaul,
# MAGIC         vendor_transaction_projection.relay_reference_number
# MAGIC     FROM vendor_transaction_projection
# MAGIC     WHERE canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC     GROUP BY vendor_transaction_projection.relay_reference_number
# MAGIC ) carrier_linehaul ON true
# MAGIC LEFT JOIN LATERAL (
# MAGIC     SELECT 
# MAGIC         SUM(vendor_transaction_projection.amount) FILTER (WHERE vendor_transaction_projection.`voided?` = false AND vendor_transaction_projection.charge_code = 'fuel_surcharge') / 100.0 AS fuel_surcharge,
# MAGIC         vendor_transaction_projection.relay_reference_number
# MAGIC     FROM vendor_transaction_projection
# MAGIC     WHERE canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC     GROUP BY vendor_transaction_projection.relay_reference_number
# MAGIC ) fuel_surcharge ON true
# MAGIC WHERE plan_combination_projection.is_combined = "true")
# MAGIC
# MAGIC , use_relay_loads as  (
# MAGIC select distinct 
# MAGIC canonical_plan_projection.mode as Mode_Filter,
# MAGIC case when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC else case when canonical_plan_projection.mode = 'imdl' then 'IMDL' else 'Dry Van' end end as modee,
# MAGIC 'TL' as equipment,
# MAGIC b.relay_reference_number as relay_load,
# MAGIC 'NO' as combined_load,
# MAGIC b.booking_id,
# MAGIC b.booked_by_name as Carrier_Rep,
# MAGIC carr.new_office as carr_office,
# MAGIC cust.new_office as customer_office,
# MAGIC coalesce(pss.appointment_datetime::timestamp, pss.window_end_datetime::timestamp, pss.window_start_datetime::timestamp, pp.appointment_datetime::timestamp) as pu_appt_date,
# MAGIC coalesce(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as canonical_stop,
# MAGIC coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date) as use_date,
# MAGIC case when master_customer_name is null then r.customer_name else master_customer_name end as mastername,
# MAGIC r.customer_name as customer_name,
# MAGIC z.full_name as customer_rep,
# MAGIC coalesce(tendering_planned_distance.planned_distance_amount::float, b.total_miles::float) as total_miles,
# MAGIC upper(b.status) as status,
# MAGIC initcap(b.first_shipper_city) as origin_city,
# MAGIC upper(b.first_shipper_state) as origin_state,
# MAGIC initcap(b.receiver_city) as dest_city,
# MAGIC upper(b.receiver_state) as dest_state,
# MAGIC b.first_shipper_zip as origin_zip,
# MAGIC b.receiver_zip as dest_zip,
# MAGIC o.market as market_origin,
# MAGIC d.market AS market_dest,
# MAGIC CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC --financial_calendar.financial_period_sorting,
# MAGIC --financial_calendar.financial_year,
# MAGIC carrier_projection.dot_number::string as dot_num,
# MAGIC coalesce(a.in_date_time, a.out_date_time)::date as del_date,
# MAGIC rolled_at::date as rolled_at,
# MAGIC booked_at as Booked_Date,
# MAGIC ta.accepted_at::string as Tendering_Date,
# MAGIC b.bounced_at::date as bounced_date,
# MAGIC b.bounced_at::date + 2 as cancelled_date,
# MAGIC pickup.Days_ahead as Days_Ahead,
# MAGIC pickup.prebookable as prebookable,
# MAGIC  papp.PickUp_Start,
# MAGIC  papp.PickUp_End,
# MAGIC  dapp.Drop_Start,
# MAGIC  dapp.Drop_End,
# MAGIC b.booked_carrier_name as carrier_name
# MAGIC from bronze.booking_projection b 
# MAGIC LEFT join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC JOIN bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC left join  bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC left join  bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office ----- Need to change to new_office_lookup_w_tgt later
# MAGIC LEFT JOIN  bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC LEFT JOIN  bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip 
# MAGIC LEFT join bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name) AND relay_users.`active?` = 'true'
# MAGIC LEFT join bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
# MAGIC LEFT join bronze.pickup_projection pp on (b.booking_id = pp.booking_id 
# MAGIC     AND b.first_shipper_name = pp.shipper_name 
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join bronze.planning_stop_schedule pss on (b.relay_reference_number = pss.relay_reference_number 
# MAGIC     AND b.first_shipper_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT join bronze.canonical_stop c on (pss.stop_id = c.stop_id 
# MAGIC     and c.`stale?` = 'false'
# MAGIC     and c.stop_type = 'pickup')
# MAGIC left join bronze.canonical_stop a on b.booking_id = a.booking_id
# MAGIC         and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC         and upper(b.receiver_city) = upper(a.locality)
# MAGIC         and a.stop_type = 'delivery'
# MAGIC         and a.`stale?` = 'false'
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON b.relay_reference_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON b.relay_reference_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON b.relay_reference_number = dapp.loadnum
# MAGIC     --left join bounce_data on b.relay_reference_number = bounce_data.relay_reference_number
# MAGIC --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC --JOIN bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC left join combined_loads_mine on b.relay_reference_number = combined_loads_mine.combine_new_rrn
# MAGIC left join bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC left join bronze.tendering_planned_distance on b.relay_reference_number = tendering_planned_distance.relay_reference_number
# MAGIC left join bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC where 1=1 
# MAGIC --[[and {{date_range}}]]
# MAGIC and b.status = 'booked'
# MAGIC and canonical_plan_projection.mode not like '%ltl%'
# MAGIC and canonical_plan_projection.status not in ('cancelled','voided','held')
# MAGIC and combined is null 
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC select distinct 
# MAGIC canonical_plan_projection.mode as Mode_Filter,
# MAGIC case when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC else case when canonical_plan_projection.mode = 'imdl' then 'IMDL' else 'Dry Van' end end as Modee,
# MAGIC 'TL' as Equipment,
# MAGIC combined_loads_mine.relay_reference_number_one as relay_load,
# MAGIC CAST(combined_loads_mine.combine_new_rrn As string) as combo_load,
# MAGIC b.booking_id,
# MAGIC b.booked_by_name as Carrier_Rep,
# MAGIC carr.new_office as carr_office,
# MAGIC cust.new_office as customer_office,
# MAGIC coalesce(pss.appointment_datetime::timestamp, pss.window_end_datetime::timestamp, pss.window_start_datetime::timestamp, pp.appointment_datetime::timestamp) as pu_appt_date,
# MAGIC coalesce(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as canonical_stop,
# MAGIC coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date) as use_date,
# MAGIC case when master_customer_name is null then r.customer_name else master_customer_name end as mastername,
# MAGIC r.customer_name as customer_name,
# MAGIC z.full_name as customer_rep,
# MAGIC coalesce(tendering_planned_distance.planned_distance_amount::float, b.total_miles::float) as total_miles,
# MAGIC upper(b.status) as status,
# MAGIC initcap(b.first_shipper_city) as origin_city,
# MAGIC upper(b.first_shipper_state) as origin_state,
# MAGIC initcap(b.receiver_city) as dest_city,
# MAGIC upper(b.receiver_state) as dest_state,
# MAGIC b.first_shipper_zip as origin_zip,
# MAGIC b.receiver_zip as dest_zip,
# MAGIC o.market as market_origin,
# MAGIC d.market AS market_dest,
# MAGIC CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC --dim_financial_calendar.financial_period_sorting,
# MAGIC --dim_financial_calendar.financial_year,
# MAGIC carrier_projection.dot_number::string,
# MAGIC coalesce(a.in_date_time, a.out_date_time)::date as del_date,
# MAGIC rolled_at::date as rolled_at,
# MAGIC booked_at as Booked_Date,
# MAGIC ta.accepted_at::string as Tendering_Date,
# MAGIC b.bounced_at::date as bounced_date,
# MAGIC b.bounced_at::date + 2 as cancelled_date,
# MAGIC pickup.Days_ahead as Days_Ahead,
# MAGIC pickup.prebookable as prebookable,
# MAGIC  papp.PickUp_Start,
# MAGIC  papp.PickUp_End,
# MAGIC  dapp.Drop_Start,
# MAGIC  dapp.Drop_End,
# MAGIC b.booked_carrier_name 
# MAGIC from combined_loads_mine
# MAGIC JOIN bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC LEFT join bronze.pickup_projection pp on (b.booking_id = pp.booking_id 
# MAGIC     AND b.first_shipper_name = pp.shipper_name 
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join bronze.planning_stop_schedule pss on (b.relay_reference_number = pss.relay_reference_number 
# MAGIC     AND b.first_shipper_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT join bronze.canonical_stop c on (b.booking_id = c.booking_id 
# MAGIC     AND b.first_shipper_id = c.facility_id
# MAGIC     and c.`stale?` = 'false'
# MAGIC     and c.stop_type = 'pickup')  
# MAGIC left join bronze.canonical_stop a on b.booking_id = a.booking_id
# MAGIC         and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC         and upper(b.receiver_city) = upper(a.locality)
# MAGIC         and a.stop_type = 'delivery'
# MAGIC         and a.`stale?` = 'false'
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON b.relay_reference_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON b.relay_reference_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON b.relay_reference_number = dapp.loadnum
# MAGIC --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC --LEFT JOIN analytics.dim_financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = dim_financial_calendar.date::date
# MAGIC JOIN bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC LEFT JOIN  bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC LEFT JOIN  bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip 
# MAGIC left join bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC left join  bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC left join bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC left join bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_one = tendering_planned_distance.relay_reference_number
# MAGIC left join bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC LEFT join bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name) AND relay_users.`active?` = 'true'
# MAGIC LEFT join bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC where 1=1 
# MAGIC --[[and {{date_range}}]]
# MAGIC and b.status = 'booked'
# MAGIC and canonical_plan_projection.mode not like '%ltl%'
# MAGIC and canonical_plan_projection.status not in ('cancelled','voided','held')
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC select distinct 
# MAGIC concat(canonical_plan_projection.mode,'-combo2') as Mode_Filter,
# MAGIC case when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC else case when canonical_plan_projection.mode = 'imdl' then 'IMDL' else 'Dry Van' end end as Equipment,
# MAGIC 'TL' as modee,
# MAGIC combined_loads_mine.relay_reference_number_two as relay_load,
# MAGIC CAST(combined_loads_mine.combine_new_rrn As string) as combo_load,
# MAGIC b.booking_id,
# MAGIC b.booked_by_name,
# MAGIC carr.new_office as carr_office,
# MAGIC cust.new_office as customer_office,
# MAGIC coalesce(pss.appointment_datetime::timestamp, pss.window_end_datetime::timestamp, pss.window_start_datetime::timestamp, pp.appointment_datetime::timestamp) as pu_appt_date,
# MAGIC coalesce(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as canonical_stop,
# MAGIC coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date) as use_date,
# MAGIC case when master_customer_name is null then r.customer_name else master_customer_name end as mastername,
# MAGIC r.customer_name as customer_name,
# MAGIC z.full_name as customer_rep,
# MAGIC coalesce(tendering_planned_distance.planned_distance_amount::float, b.total_miles::float) as total_miles,
# MAGIC b.status,
# MAGIC initcap(b.first_shipper_city) as origin_city,
# MAGIC upper(b.first_shipper_state) as origin_state,
# MAGIC initcap(b.receiver_city) as dest_city,
# MAGIC upper(b.receiver_state) as dest_state,
# MAGIC b.first_shipper_zip as origin_zip,
# MAGIC b.receiver_zip as dest_zip,
# MAGIC o.market as market_origin,
# MAGIC d.market AS market_dest,
# MAGIC CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC --financial_calendar.financial_period_sorting,
# MAGIC --financial_calendar.financial_year,
# MAGIC carrier_projection.dot_number::string,
# MAGIC coalesce(a.in_date_time, a.out_date_time)::date as del_date,
# MAGIC rolled_at::date as rolled_at,
# MAGIC booked_at as Booked_Date,
# MAGIC ta.accepted_at::string as Tendering_Date,
# MAGIC b.bounced_at::date as bounced_date,
# MAGIC b.bounced_at::date + 2 as cancelled_date,
# MAGIC pickup.Days_ahead as Days_Ahead,
# MAGIC pickup.prebookable as prebookable,
# MAGIC  papp.PickUp_Start,
# MAGIC  papp.PickUp_End,
# MAGIC  dapp.Drop_Start,
# MAGIC  dapp.Drop_End,
# MAGIC b.booked_carrier_name 
# MAGIC from combined_loads_mine
# MAGIC JOIN bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC LEFT join bronze.pickup_projection pp on (b.booking_id = pp.booking_id 
# MAGIC     AND b.first_shipper_name = pp.shipper_name 
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join bronze.planning_stop_schedule pss on (b.relay_reference_number = pss.relay_reference_number 
# MAGIC     AND b.first_shipper_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT join bronze.canonical_stop c on (b.booking_id = c.booking_id 
# MAGIC     AND b.first_shipper_id = c.facility_id
# MAGIC     and c.`stale?` = 'false'
# MAGIC     and c.stop_type = 'pickup')  
# MAGIC left join bronze.canonical_stop a on b.booking_id = a.booking_id
# MAGIC         and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC         and upper(b.receiver_city) = upper(a.locality)
# MAGIC         and a.stop_type = 'delivery'
# MAGIC         and a.`stale?` = 'false'
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON b.relay_reference_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON b.relay_reference_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON b.relay_reference_number = dapp.loadnum
# MAGIC --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'    
# MAGIC --JOIN bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC JOIN bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office 
# MAGIC LEFT JOIN  bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC LEFT JOIN  bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip 
# MAGIC left join bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC left join  bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC left join bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC left join bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_two = tendering_planned_distance.relay_reference_number
# MAGIC left join bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC LEFT join bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name) AND relay_users.`active?` = 'true'
# MAGIC LEFT join bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
# MAGIC where 1=1 
# MAGIC --[[and {{date_range}}]]
# MAGIC and b.status = 'booked'
# MAGIC and canonical_plan_projection.status not in ('cancelled','voided','held')
# MAGIC and canonical_plan_projection.mode not like '%ltl%'
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct 
# MAGIC canonical_plan_projection.mode as Mode_Filter,
# MAGIC case when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC else case when canonical_plan_projection.mode = 'imdl' then 'IMDL' else 'Dry Van' end end as Equipment,
# MAGIC 'LTL' as modee,
# MAGIC e.load_number,
# MAGIC 'NO' as combined_load,
# MAGIC null::float,
# MAGIC 'ltl_team' as booked_by,
# MAGIC 'LTL',
# MAGIC cust.new_office as customer_office,
# MAGIC coalesce(pss.appointment_datetime::timestamp, pss.window_end_datetime::timestamp, pss.window_start_datetime::timestamp, pp.appointment_datetime::timestamp) as planning_stop_sched,
# MAGIC coalesce(e.ship_date::timestamp, c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as canonical_stop,
# MAGIC coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date) as use_date,
# MAGIC case when master_customer_name is null then r.customer_name else master_customer_name end as mastername,
# MAGIC r.customer_name as customer_name,
# MAGIC z.full_name as customer_rep,
# MAGIC e.miles::float,
# MAGIC e.dispatch_status,
# MAGIC initcap(e.pickup_city) as origin_city,
# MAGIC upper(e.pickup_state) as origin_state,
# MAGIC initcap(e.consignee_city) as dest_city,
# MAGIC upper(e.consignee_state) as dest_state,
# MAGIC e.pickup_zip as Pickup_Zip,
# MAGIC e.consignee_zip as Consignee_Zip,
# MAGIC o.market as market_origin,
# MAGIC d.market AS market_dest,
# MAGIC CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC --financial_calendar.financial_period_sorting,
# MAGIC --financial_calendar.financial_year,
# MAGIC projection_carrier.dot_num::string,
# MAGIC cast(delivered_date as date)::date as delivery_date,
# MAGIC null::date as rolled_out,
# MAGIC null as Booked_Date,
# MAGIC null::string as Tender_Date,
# MAGIC null::date as bounced_date,
# MAGIC null::date as cancelled_date,
# MAGIC pickup.Days_ahead as Days_Ahead,
# MAGIC pickup.prebookable as prebookable,
# MAGIC  papp.PickUp_Start,
# MAGIC  papp.PickUp_End,
# MAGIC  dapp.Drop_Start,
# MAGIC  dapp.Drop_End,
# MAGIC e.carrier_name 
# MAGIC from bronze.big_export_projection e 
# MAGIC LEFT join bronze.customer_profile_projection r on e.customer_id = r.customer_slug 
# MAGIC left join bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC LEFT JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office 
# MAGIC     LEFT JOIN  bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC     LEFT JOIN  bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip 
# MAGIC left join bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC JOIN bronze.canonical_plan_projection on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT join bronze.pickup_projection pp on (e.load_number = pp.relay_reference_number 
# MAGIC     AND e.pickup_name = pp.shipper_name 
# MAGIC     AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS? 
# MAGIC     AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join bronze.planning_stop_schedule pss on (e.load_number = pss.relay_reference_number 
# MAGIC     AND e.pickup_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON e.load_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON e.load_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON e.load_number = dapp.loadnum
# MAGIC --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'    
# MAGIC LEFT join bronze.canonical_stop c on pss.stop_id = c.stop_id 
# MAGIC --JOIN bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC left join bronze.projection_carrier on e.carrier_id = projection_carrier.id 
# MAGIC where 1=1 
# MAGIC --[[and {{date_range}}]]
# MAGIC and e.dispatch_status != 'Cancelled'
# MAGIC and e.carrier_name is not null 
# MAGIC and canonical_plan_projection.status not in ('cancelled','voided','held')
# MAGIC and canonical_plan_projection.mode = 'ltl'
# MAGIC and e.customer_id NOT in ('roland','hain','deb')
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select distinct 
# MAGIC 'ltl- (hain,deb,roland)',
# MAGIC case when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC else case when canonical_plan_projection.mode = 'imdl' then 'IMDL' else 'Dry Van' end end as Equipment,
# MAGIC 'LTL' as modee,
# MAGIC e.load_number,
# MAGIC 'NO' as combined_load,
# MAGIC null::float,
# MAGIC 'ltl_team' as booked_by,
# MAGIC 'LTL',
# MAGIC cust.new_office as customer_office,
# MAGIC coalesce(pss.appointment_datetime::timestamp, pss.window_end_datetime::timestamp, pss.window_start_datetime::timestamp, pp.appointment_datetime::timestamp) as planning_stop_sched,
# MAGIC coalesce(e.ship_date::timestamp, c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as canonical_stop,
# MAGIC coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date) as use_date,
# MAGIC case when master_customer_name is null then r.customer_name else master_customer_name end as mastername,
# MAGIC r.customer_name as customer_name,
# MAGIC z.full_name as customer_rep,
# MAGIC e.miles::float,
# MAGIC e.dispatch_status,
# MAGIC initcap(e.pickup_city) as origin_city,
# MAGIC upper(e.pickup_state) as origin_state,
# MAGIC initcap(e.consignee_city) as dest_city,
# MAGIC upper(e.consignee_state) as dest_state,
# MAGIC e.pickup_zip as Pickup_Zip,
# MAGIC e.consignee_zip as Consignee_Zip,
# MAGIC o.market as market_origin,
# MAGIC d.market AS market_dest,
# MAGIC CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC --financial_calendar.financial_period_sorting,
# MAGIC --financial_calendar.financial_year,
# MAGIC projection_carrier.dot_num::string,
# MAGIC cast(delivered_date as date)::date as delivery_date,
# MAGIC null::date as rolled_out,
# MAGIC null as Booked_Date,
# MAGIC null::string as Tender_Date,
# MAGIC null::date as bounced_date,
# MAGIC null::date as cancelled_date,
# MAGIC pickup.Days_ahead as Days_Ahead,
# MAGIC pickup.prebookable as prebookable,
# MAGIC  papp.PickUp_Start,
# MAGIC  papp.PickUp_End,
# MAGIC  dapp.Drop_Start,
# MAGIC  dapp.Drop_End,
# MAGIC e.carrier_name 
# MAGIC from bronze.big_export_projection e 
# MAGIC LEFT join bronze.customer_profile_projection r on e.customer_id = r.customer_slug 
# MAGIC left join bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC LEFT JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office 
# MAGIC LEFT JOIN  bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC LEFT JOIN  bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip 
# MAGIC left join bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC JOIN bronze.canonical_plan_projection on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT join bronze.pickup_projection pp on (e.load_number = pp.relay_reference_number 
# MAGIC     AND e.pickup_name = pp.shipper_name 
# MAGIC     AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS? 
# MAGIC     AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join bronze.planning_stop_schedule pss on (e.load_number = pss.relay_reference_number 
# MAGIC     AND e.pickup_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON e.load_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON e.load_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON e.load_number = dapp.loadnum
# MAGIC --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'    
# MAGIC LEFT join bronze.canonical_stop c on pss.stop_id = c.stop_id 
# MAGIC --JOIN bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC left join bronze.projection_carrier on e.carrier_id = projection_carrier.id 
# MAGIC where 1=1 
# MAGIC --[[and {{date_range}}]]
# MAGIC and e.dispatch_status != 'Cancelled'
# MAGIC and e.carrier_name is not null 
# MAGIC and canonical_plan_projection.status not in ('cancelled','voided','held')
# MAGIC and canonical_plan_projection.mode = 'ltl'
# MAGIC and e.customer_id  in ('roland','hain','deb')
# MAGIC )
# MAGIC
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
# MAGIC
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
# MAGIC )
# MAGIC )
# MAGIC
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
# MAGIC     GROUP BY booking_id
# MAGIC ), master_date AS (
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
# MAGIC )
# MAGIC
# MAGIC
# MAGIC , lane as (
# MAGIC 	
# MAGIC SELECT  
# MAGIC     UPPER(CONCAT(first_shipper_city, ',', first_shipper_state)) AS shipper,
# MAGIC     UPPER(CONCAT(receiver_city, ',', receiver_state)) AS receiver
# MAGIC FROM 
# MAGIC     booking_projection
# MAGIC     )
# MAGIC , invoicing_cred as (
# MAGIC select distinct 
# MAGIC relay_reference_number,
# MAGIC sum(
# MAGIC case when currency = 'CAD' then total_amount::float * (coalesce(case when us_to_cad = 0 then null else canada_conversions.us_to_cad end, average_conversions.avg_cad_to_us))::float else total_amount::float end)::float / 100.00 as invoicing_cred
# MAGIC from bronze.invoicing_credits
# MAGIC JOIN use_relay_loads on relay_reference_number = use_relay_loads.relay_load 
# MAGIC left join bronze.canada_conversions on invoicing_credits.credited_at::date = canada_conversions.ship_date::date 
# MAGIC join average_conversions on 1=1 
# MAGIC where 1=1
# MAGIC group by relay_reference_number)
# MAGIC
# MAGIC ,relay_customer_money AS (
# MAGIC   -- TL Bookings
# MAGIC   SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) AS total_cust_rate_wo_cred,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.00, 0) AS customer_lhc,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.00, 0) AS customer_fsc,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code NOT IN ('linehaul', 'fuel_surcharge')) / 100.00, 0) AS customer_acc,
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate
# MAGIC   FROM booking_projection b
# MAGIC   LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC   LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
# MAGIC   LEFT JOIN canada_conversions cm ON m.incurred_at = cm.ship_date
# MAGIC   CROSS JOIN average_conversions
# MAGIC   LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC   WHERE b.status != 'cancelled'
# MAGIC     AND (canonical_plan_projection.mode != 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC     AND (canonical_plan_projection.status != 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC     AND b.booking_id != 7370325
# MAGIC   GROUP BY b.booking_id, b.relay_reference_number, b.status, invoicing_cred.invoicing_cred
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- LTL Bookings
# MAGIC   SELECT DISTINCT 
# MAGIC     b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) AS total_cust_rate_wo_cred,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.00, 0) AS customer_lhc,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.00, 0) AS customer_fsc,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code NOT IN ('linehaul', 'fuel_surcharge')) / 100.00, 0) AS customer_acc,
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate
# MAGIC   FROM big_export_projection b
# MAGIC   LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC   LEFT JOIN moneying_billing_party_transaction m ON b.load_number = m.relay_reference_number
# MAGIC   LEFT JOIN canada_conversions cm ON m.incurred_at = cm.ship_date
# MAGIC   CROSS JOIN average_conversions
# MAGIC   LEFT JOIN invoicing_cred ON b.load_number = invoicing_cred.relay_reference_number
# MAGIC   WHERE b.dispatch_status != 'Cancelled'
# MAGIC     AND (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC     AND (canonical_plan_projection.status != 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC     AND b.customer_id NOT IN ('hain', 'deb', 'roland')
# MAGIC   GROUP BY b.load_number, b.dispatch_status, invoicing_cred.invoicing_cred
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- Special LTL cases
# MAGIC   SELECT DISTINCT 
# MAGIC     e.load_number AS booking_id,
# MAGIC     e.load_number AS relay_reference_number,
# MAGIC     e.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC     (e.carrier_accessorial_expense + CEILING(
# MAGIC       CASE 
# MAGIC         WHEN e.customer_id = 'deb' THEN 
# MAGIC           CASE 
# MAGIC             WHEN e.carrier_id = '300003979' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1500)
# MAGIC             ELSE 
# MAGIC               CASE 
# MAGIC                 WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * 0.08, 1000)
# MAGIC                 ELSE GREATEST(e.carrier_linehaul_expense * 0.09, 1000)
# MAGIC               END
# MAGIC           END
# MAGIC         WHEN e.customer_id = 'hain' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1000)
# MAGIC         WHEN e.customer_id = 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC         ELSE 0
# MAGIC       END + e.carrier_linehaul_expense
# MAGIC     ) + CEILING(CEILING(
# MAGIC       CASE 
# MAGIC         WHEN e.customer_id = 'deb' THEN 
# MAGIC           CASE 
# MAGIC             WHEN e.carrier_id = '300003979' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1500)
# MAGIC             ELSE 
# MAGIC               CASE 
# MAGIC                 WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * 0.08, 1000)
# MAGIC                 ELSE GREATEST(e.carrier_linehaul_expense * 0.09, 1000)
# MAGIC               END
# MAGIC           END
# MAGIC         WHEN e.customer_id = 'hain' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1000)
# MAGIC         WHEN e.customer_id = 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC         ELSE 0
# MAGIC       END + e.carrier_linehaul_expense
# MAGIC     ) * 
# MAGIC       CASE 
# MAGIC         WHEN e.carrier_linehaul_expense = 0 THEN 0
# MAGIC         ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
# MAGIC       END
# MAGIC     )) / 100 AS total_cust_rate_wo_cred,
# MAGIC     CEILING(
# MAGIC       CASE 
# MAGIC         WHEN e.customer_id = 'deb' THEN 
# MAGIC           CASE 
# MAGIC             WHEN e.carrier_id = '300003979' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1500)
# MAGIC             ELSE 
# MAGIC               CASE 
# MAGIC                 WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * 0.08, 1000)
# MAGIC                 ELSE GREATEST(e.carrier_linehaul_expense * 0.09, 1000)
# MAGIC               END
# MAGIC           END
# MAGIC         WHEN e.customer_id = 'hain' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1000)
# MAGIC         WHEN e.customer_id = 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC         ELSE 0
# MAGIC       END + e.carrier_linehaul_expense
# MAGIC     ) / 100 AS customer_lhc,
# MAGIC     CEILING(CEILING(
# MAGIC       CASE 
# MAGIC         WHEN e.customer_id = 'deb' THEN 
# MAGIC           CASE 
# MAGIC             WHEN e.carrier_id = '300003979' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1500)
# MAGIC             ELSE 
# MAGIC               CASE 
# MAGIC                 WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * 0.08, 1000)
# MAGIC                 ELSE GREATEST(e.carrier_linehaul_expense * 0.09, 1000)
# MAGIC               END
# MAGIC           END
# MAGIC         WHEN e.customer_id = 'hain' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1000)
# MAGIC         WHEN e.customer_id = 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC         ELSE 0
# MAGIC       END + e.carrier_linehaul_expense
# MAGIC     ) * 
# MAGIC       CASE 
# MAGIC         WHEN e.carrier_linehaul_expense = 0 THEN 0
# MAGIC         ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
# MAGIC       END
# MAGIC     ) / 100 AS customer_fsc,
# MAGIC     e.carrier_accessorial_expense / 100 AS customer_acc,
# MAGIC     0 AS invoicing_cred,
# MAGIC     (e.carrier_accessorial_expense + CEILING(
# MAGIC       CASE 
# MAGIC         WHEN e.customer_id = 'deb' THEN 
# MAGIC           CASE 
# MAGIC             WHEN e.carrier_id = '300003979' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1500)
# MAGIC             ELSE 
# MAGIC               CASE 
# MAGIC                 WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * 0.08, 1000)
# MAGIC                 ELSE GREATEST(e.carrier_linehaul_expense * 0.09, 1000)
# MAGIC               END
# MAGIC           END
# MAGIC         WHEN e.customer_id = 'hain' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1000)
# MAGIC         WHEN e.customer_id = 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC         ELSE 0
# MAGIC       END + e.carrier_linehaul_expense
# MAGIC     ) + CEILING(CEILING(
# MAGIC       CASE 
# MAGIC         WHEN e.customer_id = 'deb' THEN 
# MAGIC           CASE 
# MAGIC             WHEN e.carrier_id = '300003979' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1500)
# MAGIC             ELSE 
# MAGIC               CASE 
# MAGIC                 WHEN e.ship_date < '2019-03-09' THEN GREATEST(e.carrier_linehaul_expense * 0.08, 1000)
# MAGIC                 ELSE GREATEST(e.carrier_linehaul_expense * 0.09, 1000)
# MAGIC               END
# MAGIC           END
# MAGIC         WHEN e.customer_id = 'hain' THEN GREATEST(e.carrier_linehaul_expense * 0.10, 1000)
# MAGIC         WHEN e.customer_id = 'roland' THEN e.carrier_linehaul_expense * 0.1365
# MAGIC         ELSE 0
# MAGIC       END + e.carrier_linehaul_expense
# MAGIC     ) * 
# MAGIC       CASE 
# MAGIC         WHEN e.carrier_linehaul_expense = 0 THEN 0
# MAGIC         ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
# MAGIC       END
# MAGIC     )) / 100 AS final_customer_rate
# MAGIC   FROM big_export_projection e
# MAGIC   LEFT JOIN canonical_plan_projection ON e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC   WHERE e.dispatch_status != 'Cancelled'
# MAGIC     AND (canonical_plan_projection.mode = 'ltl' OR canonical_plan_projection.mode IS NULL)
# MAGIC     AND (canonical_plan_projection.status != 'voided' OR canonical_plan_projection.status IS NULL)
# MAGIC     AND e.customer_id IN ('hain', 'deb', 'roland')
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- Combined Loads (Part 1)
# MAGIC   SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL
# MAGIC 	  ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) AS total_cust_rate_wo_cred,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.00, 0) AS customer_lhc,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.00, 0) AS customer_fsc,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code NOT IN ('linehaul', 'fuel_surcharge')) / 100.00, 0) AS customer_acc,
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate
# MAGIC   FROM booking_projection b
# MAGIC   JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_one
# MAGIC   LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
# MAGIC   LEFT JOIN canada_conversions cm ON m.incurred_at = cm.ship_date
# MAGIC   CROSS JOIN average_conversions
# MAGIC   LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC   GROUP BY b.booking_id, b.relay_reference_number, b.status, invoicing_cred.invoicing_cred
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- Combined Loads (Part 2)
# MAGIC   SELECT DISTINCT 
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) AS total_cust_rate_wo_cred,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'linehaul') / 100.00, 0) AS customer_lhc,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code = 'fuel_surcharge') / 100.00, 0) AS customer_fsc,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false AND m.charge_code NOT IN ('linehaul', 'fuel_surcharge')) / 100.00, 0) AS customer_acc,
# MAGIC     COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
# MAGIC     COALESCE(SUM(CASE
# MAGIC       WHEN m.currency = 'CAD' THEN m.amount * COALESCE(CASE WHEN cm.us_to_cad = 0 THEN NULL ELSE cm.us_to_cad END, average_conversions.avg_cad_to_us)
# MAGIC       ELSE m.amount
# MAGIC     END) FILTER (WHERE m.`voided?` = false) / 100.00, 0) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate
# MAGIC   FROM booking_projection b
# MAGIC   JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_two
# MAGIC   LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
# MAGIC   LEFT JOIN canada_conversions cm ON m.incurred_at = cm.ship_date
# MAGIC   CROSS JOIN average_conversions
# MAGIC   LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC   GROUP BY b.booking_id, b.relay_reference_number, b.status, invoicing_cred.invoicing_cred
# MAGIC )
# MAGIC ,final as (
# MAGIC SELECT DISTINCT 
# MAGIC     b.relay_reference_number AS Load,
# MAGIC     b.booking_id AS Booking_ID,
# MAGIC     CONCAT(INITCAP(b.first_shipper_city), ',', b.first_shipper_state, '>', INITCAP(b.receiver_city), ',', b.receiver_state) AS Lane,
# MAGIC     b.booked_carrier_name AS Carrier,
# MAGIC     c.dot_number AS DOT,
# MAGIC     COALESCE(combined_loads_mine.total_load_rev, relay_customer_money.final_customer_rate, 0) AS Shipper_Partner_Rate,
# MAGIC     relay_carrier_money.total_carrier_rate AS Carrier_Rate,
# MAGIC     FROM_UTC_TIMESTAMP(CAST(b.booked_at AS TIMESTAMP), 'America/New_York') AS Booked_Time,
# MAGIC     CAST(master_date.use_date AS DATE) AS Ship_Date,
# MAGIC     CAST(master_date.delivery_datetime AS DATE) AS Delivery_Date,
# MAGIC     b.booked_by_name AS Booked_By,
# MAGIC     b.rate_con_recipients AS Rate_Con_Email,
# MAGIC     b.status as Load_Status
# MAGIC FROM 
# MAGIC     booking_projection b 
# MAGIC LEFT JOIN 
# MAGIC     carrier_projection c ON b.booked_carrier_id = c.carrier_id 
# MAGIC JOIN 
# MAGIC     lane ON UPPER(CONCAT(b.first_shipper_city, ',', b.first_shipper_state)) = lane.shipper 
# MAGIC     AND UPPER(CONCAT(b.receiver_city, ',', b.receiver_state)) = lane.receiver 
# MAGIC LEFT JOIN 
# MAGIC     master_date ON b.relay_reference_number = master_date.relay_reference_number AND b.status = master_date.status 
# MAGIC LEFT JOIN 
# MAGIC     relay_customer_money ON b.relay_reference_number = relay_customer_money.relay_reference_number AND b.status = relay_customer_money.status 
# MAGIC LEFT JOIN 
# MAGIC     combined_loads_mine ON b.relay_reference_number = combined_loads_mine.combine_new_rrn 
# MAGIC LEFT JOIN 
# MAGIC     relay_carrier_money ON b.relay_reference_number = relay_carrier_money.relay_reference_number AND b.status = relay_carrier_money.status 
# MAGIC -- WHERE 
# MAGIC     -- b.status = 'booked' 
# MAGIC     order by Ship_Date desc)
# MAGIC     
# MAGIC select * from final 
# MAGIC
# MAGIC
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SET legacy_time_parser_policy = LEGACY;
# MAGIC
# MAGIC TRUNCATE TABLE analytics.Load_History_GenAI_Temp;
# MAGIC
# MAGIC INSERT INTO analytics.Load_History_GenAI_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Load_History_GenAI;

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Load_History_GenAI;
# MAGIC
# MAGIC INSERT INTO analytics.Load_History_GenAI
# MAGIC SELECT *
# MAGIC FROM analytics.Load_History_GenAI_Temp