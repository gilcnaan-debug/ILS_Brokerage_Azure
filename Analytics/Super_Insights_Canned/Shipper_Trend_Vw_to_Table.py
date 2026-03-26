# Databricks notebook source
# MAGIC %md
# MAGIC ## Canned Reprorts - Shipper Trend Old
# MAGIC * **Description:** To Replicate Metabase Canned Reports for Niffy
# MAGIC * **Created Date:** 11/25/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 11/25/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ###CREATE VIEW

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA Bronze;
# MAGIC create or replace view Genai_Shipper_Trend as( with combined_loads_mine as (
# MAGIC select distinct 
# MAGIC relay_reference_number_one,
# MAGIC relay_reference_number_two,
# MAGIC resulting_plan_id,
# MAGIC canonical_plan_projection.relay_reference_number as combine_new_rrn,
# MAGIC canonical_plan_projection.mode ,
# MAGIC b.tender_on_behalf_of_id,
# MAGIC 'COMBINED' as combined
# MAGIC from brokerageprod.bronze.plan_combination_projection
# MAGIC join brokerageprod.bronze.canonical_plan_projection on resulting_plan_id = canonical_plan_projection.plan_id
# MAGIC left join brokerageprod.bronze.booking_projection b on canonical_plan_projection.relay_reference_number = b.relay_reference_number
# MAGIC where 1=1 
# MAGIC and is_combined = 'true')
# MAGIC
# MAGIC ----- Relay Data -------------
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
# MAGIC from brokerageprod.bronze.booking_projection b 
# MAGIC LEFT join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC JOIN brokerageprod.bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC left join  brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC left join  brokerageprod.bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office ----- Need to change to new_office_lookup_w_tgt later
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip 
# MAGIC LEFT join brokerageprod.bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name) AND relay_users.`active?` = 'true'
# MAGIC LEFT join brokerageprod.bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
# MAGIC LEFT join brokerageprod.bronze.pickup_projection pp on (b.booking_id = pp.booking_id 
# MAGIC     AND b.first_shipper_name = pp.shipper_name 
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join brokerageprod.bronze.planning_stop_schedule pss on (b.relay_reference_number = pss.relay_reference_number 
# MAGIC     AND b.first_shipper_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT join brokerageprod.bronze.canonical_stop c on (pss.stop_id = c.stop_id 
# MAGIC     and c.`stale?` = 'false'
# MAGIC     and c.stop_type = 'pickup')
# MAGIC left join brokerageprod.bronze.canonical_stop a on b.booking_id = a.booking_id
# MAGIC         and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC         and upper(b.receiver_city) = upper(a.locality)
# MAGIC         and a.stop_type = 'delivery'
# MAGIC         and a.`stale?` = 'false'
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from brokerageprod.bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON b.relay_reference_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON b.relay_reference_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON b.relay_reference_number = dapp.loadnum
# MAGIC     --left join bounce_data on b.relay_reference_number = bounce_data.relay_reference_number
# MAGIC --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC --JOIN brokerageprod.bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC left join combined_loads_mine on b.relay_reference_number = combined_loads_mine.combine_new_rrn
# MAGIC left join brokerageprod.bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC left join brokerageprod.bronze.tendering_planned_distance on b.relay_reference_number = tendering_planned_distance.relay_reference_number
# MAGIC left join brokerageprod.bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
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
# MAGIC JOIN brokerageprod.bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC LEFT join brokerageprod.bronze.pickup_projection pp on (b.booking_id = pp.booking_id 
# MAGIC     AND b.first_shipper_name = pp.shipper_name 
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join brokerageprod.bronze.planning_stop_schedule pss on (b.relay_reference_number = pss.relay_reference_number 
# MAGIC     AND b.first_shipper_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT join brokerageprod.bronze.canonical_stop c on (b.booking_id = c.booking_id 
# MAGIC     AND b.first_shipper_id = c.facility_id
# MAGIC     and c.`stale?` = 'false'
# MAGIC     and c.stop_type = 'pickup')  
# MAGIC left join brokerageprod.bronze.canonical_stop a on b.booking_id = a.booking_id
# MAGIC         and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC         and upper(b.receiver_city) = upper(a.locality)
# MAGIC         and a.stop_type = 'delivery'
# MAGIC         and a.`stale?` = 'false'
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from brokerageprod.bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON b.relay_reference_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON b.relay_reference_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON b.relay_reference_number = dapp.loadnum
# MAGIC --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC --LEFT JOIN brokerageprod.analytics.dim_financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = dim_financial_calendar.date::date
# MAGIC JOIN brokerageprod.bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip 
# MAGIC left join brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC left join  brokerageprod.bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC left join brokerageprod.bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC left join brokerageprod.bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_one = tendering_planned_distance.relay_reference_number
# MAGIC left join brokerageprod.bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC LEFT join brokerageprod.bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name) AND relay_users.`active?` = 'true'
# MAGIC LEFT join brokerageprod.bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office --- Need to change the office table new_office_lookup_w_tgt
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
# MAGIC JOIN brokerageprod.bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC LEFT join brokerageprod.bronze.pickup_projection pp on (b.booking_id = pp.booking_id 
# MAGIC     AND b.first_shipper_name = pp.shipper_name 
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join brokerageprod.bronze.planning_stop_schedule pss on (b.relay_reference_number = pss.relay_reference_number 
# MAGIC     AND b.first_shipper_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT join brokerageprod.bronze.canonical_stop c on (b.booking_id = c.booking_id 
# MAGIC     AND b.first_shipper_id = c.facility_id
# MAGIC     and c.`stale?` = 'false'
# MAGIC     and c.stop_type = 'pickup')  
# MAGIC left join brokerageprod.bronze.canonical_stop a on b.booking_id = a.booking_id
# MAGIC         and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC         and upper(b.receiver_city) = upper(a.locality)
# MAGIC         and a.stop_type = 'delivery'
# MAGIC         and a.`stale?` = 'false'
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from brokerageprod.bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON b.relay_reference_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON b.relay_reference_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON b.relay_reference_number = dapp.loadnum
# MAGIC --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'    
# MAGIC --JOIN brokerageprod.bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC JOIN brokerageprod.bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office 
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip 
# MAGIC left join brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC left join  brokerageprod.bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC left join brokerageprod.bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC left join brokerageprod.bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_two = tendering_planned_distance.relay_reference_number
# MAGIC left join brokerageprod.bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC LEFT join brokerageprod.bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name) AND relay_users.`active?` = 'true'
# MAGIC LEFT join brokerageprod.bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
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
# MAGIC from brokerageprod.bronze.big_export_projection e 
# MAGIC LEFT join brokerageprod.bronze.customer_profile_projection r on e.customer_id = r.customer_slug 
# MAGIC left join brokerageprod.bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC LEFT JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office 
# MAGIC     LEFT JOIN  brokerageprod.bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC     LEFT JOIN  brokerageprod.bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip 
# MAGIC left join brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC JOIN brokerageprod.bronze.canonical_plan_projection on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT join brokerageprod.bronze.pickup_projection pp on (e.load_number = pp.relay_reference_number 
# MAGIC     AND e.pickup_name = pp.shipper_name 
# MAGIC     AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS? 
# MAGIC     AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join brokerageprod.bronze.planning_stop_schedule pss on (e.load_number = pss.relay_reference_number 
# MAGIC     AND e.pickup_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from brokerageprod.bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON e.load_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON e.load_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON e.load_number = dapp.loadnum
# MAGIC --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'    
# MAGIC LEFT join brokerageprod.bronze.canonical_stop c on pss.stop_id = c.stop_id 
# MAGIC --JOIN brokerageprod.bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC left join brokerageprod.bronze.projection_carrier on e.carrier_id = projection_carrier.id 
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
# MAGIC from brokerageprod.bronze.big_export_projection e 
# MAGIC LEFT join brokerageprod.bronze.customer_profile_projection r on e.customer_id = r.customer_slug 
# MAGIC left join brokerageprod.bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
# MAGIC LEFT JOIN brokerageprod.bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office 
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip 
# MAGIC left join brokerageprod.bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
# MAGIC JOIN brokerageprod.bronze.canonical_plan_projection on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC LEFT join brokerageprod.bronze.pickup_projection pp on (e.load_number = pp.relay_reference_number 
# MAGIC     AND e.pickup_name = pp.shipper_name 
# MAGIC     AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS? 
# MAGIC     AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC     AND pp.sequence_number = 1 )
# MAGIC LEFT join brokerageprod.bronze.planning_stop_schedule pss on (e.load_number = pss.relay_reference_number 
# MAGIC     AND e.pickup_name = pss.stop_name 
# MAGIC     AND pss.sequence_number = 1 
# MAGIC     AND pss.`removed?` = 'false')
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from brokerageprod.bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON e.load_number = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as papp ON e.load_number = papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON e.load_number = dapp.loadnum
# MAGIC --left join brokerageprod.bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'    
# MAGIC LEFT join brokerageprod.bronze.canonical_stop c on pss.stop_id = c.stop_id 
# MAGIC --JOIN brokerageprod.bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC left join brokerageprod.bronze.projection_carrier on e.carrier_id = projection_carrier.id 
# MAGIC where 1=1 
# MAGIC --[[and {{date_range}}]]
# MAGIC and e.dispatch_status != 'Cancelled'
# MAGIC and e.carrier_name is not null 
# MAGIC and canonical_plan_projection.status not in ('cancelled','voided','held')
# MAGIC and canonical_plan_projection.mode = 'ltl'
# MAGIC and e.customer_id  in ('roland','hain','deb')
# MAGIC )
# MAGIC
# MAGIC --Select * from use_relay_loads where use_date >= '2024-08-11' and use_date <= '2024-08-17'
# MAGIC
# MAGIC , to_get_avg as  (    
# MAGIC select distinct 
# MAGIC ship_date,
# MAGIC conversion as us_to_cad,
# MAGIC us_to_cad as cad_to_us
# MAGIC from brokerageprod.bronze.canada_conversions
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
# MAGIC , total_carrier_rate as  (
# MAGIC select distinct 
# MAGIC 'non_combo' as typee,
# MAGIC relay_reference_number::string,
# MAGIC sum(
# MAGIC case when currency = 'CAD' then amount::float * (coalesce(case when us_to_cad = 0 then null else canada_conversions.us_to_cad end, average_conversions.avg_cad_to_us))::float else amount::float end)::float / 100.00 as total_carrier_rate
# MAGIC from brokerageprod.bronze.vendor_transaction_projection
# MAGIC JOIN use_relay_loads on relay_reference_number = use_relay_loads.relay_load 
# MAGIC left join brokerageprod.bronze.canada_conversions on vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date::date 
# MAGIC join average_conversions on 1=1 
# MAGIC where 1=1
# MAGIC and `voided?` = 'false'
# MAGIC and combined_load = 'NO'
# MAGIC group by relay_reference_number
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct 
# MAGIC 'combo',
# MAGIC combined_load,
# MAGIC sum(
# MAGIC case when currency = 'CAD' then amount::float * (coalesce(case when us_to_cad = 0 then null else canada_conversions.us_to_cad end, average_conversions.avg_cad_to_us))::float else amount::float end)::float / 100.00 as total_carrier_rate
# MAGIC from brokerageprod.bronze.vendor_transaction_projection
# MAGIC JOIN use_relay_loads on relay_reference_number::string = use_relay_loads.combined_load::string
# MAGIC left join brokerageprod.bronze.canada_conversions on vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date::date 
# MAGIC join average_conversions on 1=1 
# MAGIC where 1=1
# MAGIC and `voided?` = 'false'
# MAGIC and combined_load != 'NO'
# MAGIC group by combined_load
# MAGIC )
# MAGIC
# MAGIC , invoicing_cred as (
# MAGIC select distinct 
# MAGIC relay_reference_number,
# MAGIC sum(
# MAGIC case when currency = 'CAD' then total_amount::float * (coalesce(case when us_to_cad = 0 then null else canada_conversions.us_to_cad end, average_conversions.avg_cad_to_us))::float else total_amount::float end)::float / 100.00 as invoicing_cred
# MAGIC from brokerageprod.bronze.invoicing_credits
# MAGIC JOIN use_relay_loads on relay_reference_number = use_relay_loads.relay_load 
# MAGIC left join brokerageprod.bronze.canada_conversions on invoicing_credits.credited_at::date = canada_conversions.ship_date::date 
# MAGIC join average_conversions on 1=1 
# MAGIC where 1=1
# MAGIC group by relay_reference_number)
# MAGIC
# MAGIC , total_cust_rate as  (
# MAGIC select distinct 
# MAGIC m.relay_reference_number::string,
# MAGIC coalesce(invoicing_cred.invoicing_cred,0) as credit_amt,
# MAGIC (sum(
# MAGIC case when m.currency = 'CAD' then amount::float * (coalesce(case when us_to_cad = 0 then null else canada_conversions.us_to_cad end, average_conversions.avg_cad_to_us))::float else amount::float end)::float / 100.00) - 
# MAGIC coalesce(invoicing_cred.invoicing_cred,0) as total_cust_rate
# MAGIC from brokerageprod.bronze.moneying_billing_party_transaction m
# MAGIC JOIN use_relay_loads on m.relay_reference_number = use_relay_loads.relay_load 
# MAGIC left join brokerageprod.bronze.canada_conversions on m.incurred_at::date = canada_conversions.ship_date::date 
# MAGIC LEFT join invoicing_cred on m.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC join average_conversions on 1=1 
# MAGIC where 1=1
# MAGIC and m.`voided?` = 'false'
# MAGIC group by m.relay_reference_number, invoicing_cred)
# MAGIC
# MAGIC , new_customer_money as (
# MAGIC select distinct 
# MAGIC moneying_billing_party_transaction.relay_reference_number as loadnum,
# MAGIC sum(amount) filter (where charge_code = 'linehaul')::float / 100 as linehaul_cust_money,
# MAGIC sum(amount) filter (where charge_code = 'fuel_surcharge')::float / 100 as fuel_cust_money,
# MAGIC sum(amount) filter (where charge_code not in ('fuel_surcharge','linehaul'))::float / 100 as acc_cust_money,
# MAGIC sum(amount)::float / 100 as total_cust_amount,
# MAGIC sum(distinct invoicing_credits.total_amount)::float / 100 as inv_cred_amt
# MAGIC from brokerageprod.bronze.moneying_billing_party_transaction
# MAGIC left join brokerageprod.bronze.invoicing_credits on moneying_billing_party_transaction.relay_reference_number::float = invoicing_credits.relay_reference_number::float 
# MAGIC where 1=1 
# MAGIC and "voided?" = 'false'
# MAGIC group by moneying_billing_party_transaction.relay_reference_number)
# MAGIC
# MAGIC
# MAGIC
# MAGIC , invoiced_amts as (
# MAGIC select distinct 
# MAGIC relay_reference_number,
# MAGIC min(invoiced_at::date) as invoiced_at,
# MAGIC case when (sum(amount)::float / 100) = 0 then 0 else (sum(amount)::float / 100) end 
# MAGIC - case when new_customer_money.inv_cred_amt is null then 0 else new_customer_money.inv_cred_amt::float end as total_recievables,
# MAGIC (sum(amount) filter (where "invoiced?" = 'true')::float / 100)::float 
# MAGIC - case when new_customer_money.inv_cred_amt is null then 0 else new_customer_money.inv_cred_amt::float end as invoiced_amt, 
# MAGIC sum(amount) filter (where "invoiced?" = 'false')::float / 100 as non_invoiced_amt
# MAGIC from brokerageprod.bronze.moneying_billing_party_transaction
# MAGIC left join new_customer_money on moneying_billing_party_transaction.relay_reference_number = new_customer_money.loadnum 
# MAGIC where 1=1 
# MAGIC and "voided?" = 'false'
# MAGIC group by relay_reference_number, new_customer_money.inv_cred_amt)
# MAGIC
# MAGIC
# MAGIC     -------------------- Adding the Linehaul, Fuelsurcharge and Other expoense CTE ----------------------------------------------
# MAGIC     , carrier_charges_one as (
# MAGIC     select distinct 
# MAGIC     relay_reference_number,
# MAGIC     incurred_at,
# MAGIC     coalesce(
# MAGIC     case when currency = 'USD' then sum(amount) filter (where charge_code = 'linehaul')::float / 100.00 
# MAGIC         else (sum(amount) filter (where charge_code = 'linehaul')::float / 100.00) * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float end, 0) as carr_lhc,
# MAGIC         
# MAGIC     coalesce(
# MAGIC     case when currency = 'USD' then sum(amount) filter (where charge_code = 'fuel_surcharge')::float / 100.00 
# MAGIC         else (sum(amount) filter (where charge_code = 'fuel_surcharge')::float / 100.00) * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float end, 0) as carr_fsc,
# MAGIC         
# MAGIC
# MAGIC     coalesce(
# MAGIC     case when currency = 'USD' then sum(amount) filter (where charge_code not in ('fuel_surcharge','linehaul'))::float / 100.00 
# MAGIC         else (sum(amount) filter (where charge_code not in ('fuel_surcharge','linehaul'))::float / 100.00) * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float end, 0) as carr_acc
# MAGIC     from brokerageprod.bronze.vendor_transaction_projection
# MAGIC     left join brokerageprod.bronze.canada_conversions on vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date::date 
# MAGIC     join average_conversions on 1=1 
# MAGIC     where 1=1 
# MAGIC     and `voided?` = 'false'
# MAGIC     group by relay_reference_number, currency , us_to_cad, avg_cad_to_us, incurred_at)
# MAGIC 	
# MAGIC ,carrier_charges as  (
# MAGIC select distinct 
# MAGIC relay_reference_number,
# MAGIC sum(carr_lhc) as carr_lhc,
# MAGIC sum(carr_fsc) as carr_fsc,
# MAGIC sum(carr_acc) as carr_acc,
# MAGIC sum(carr_acc)::float + sum(carr_fsc)::float + sum(carr_lhc)::float as total_carrier_cost
# MAGIC from carrier_charges_one
# MAGIC group by relay_reference_number)
# MAGIC --Select * from carrier_charges_one limit 100;
# MAGIC
# MAGIC , aljex_data as  (
# MAGIC select  
# MAGIC p1.id,
# MAGIC case when aljex_user_report_listing.full_name is null then key_c_user else aljex_user_report_listing.full_name end as key_c_user,
# MAGIC case when master_customer_name is null then p2.shipper else master_customer_name end as mastername,
# MAGIC p2.shipper as customer_name,
# MAGIC case when z.full_name is null then p2.srv_rep else z.full_name end as customer_rep,
# MAGIC p1.pickup_date::date as use_date,
# MAGIC CASE 
# MAGIC     WHEN carrier_line_haul LIKE '%:%' THEN 0.00 
# MAGIC     WHEN carrier_line_haul RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_line_haul AS DOUBLE)
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN carrier_accessorial_rate1 LIKE '%:%' THEN 0.00 
# MAGIC     WHEN carrier_accessorial_rate1 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate1 AS DOUBLE)
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN carrier_accessorial_rate2 LIKE '%:%' THEN 0.00 
# MAGIC     WHEN carrier_accessorial_rate2 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate2 AS DOUBLE)
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN carrier_accessorial_rate3 LIKE '%:%' THEN 0.00 
# MAGIC     WHEN carrier_accessorial_rate3 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate3 AS DOUBLE)
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN carrier_accessorial_rate4 LIKE '%:%' THEN 0.00 
# MAGIC     WHEN carrier_accessorial_rate4 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate4 AS DOUBLE)
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN carrier_accessorial_rate5 LIKE '%:%' THEN 0.00 
# MAGIC     WHEN carrier_accessorial_rate5 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate5 AS DOUBLE)
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN carrier_accessorial_rate6 LIKE '%:%' THEN 0.00 
# MAGIC     WHEN carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate6 AS DOUBLE)
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN carrier_accessorial_rate7 LIKE '%:%' THEN 0.00 
# MAGIC     WHEN carrier_accessorial_rate7 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate7 AS DOUBLE)
# MAGIC     ELSE 0.00 
# MAGIC END +
# MAGIC CASE 
# MAGIC     WHEN carrier_accessorial_rate8 LIKE '%:%' THEN 0.00 
# MAGIC     WHEN carrier_accessorial_rate8 RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_accessorial_rate8 AS DOUBLE)
# MAGIC     ELSE 0.00 
# MAGIC END AS final_carrier_rate,
# MAGIC
# MAGIC     case when carrier_line_haul like '%:%' then 0.00 
# MAGIC          when carrier_line_haul RLIKE '\\d+(\\.[0-9]+)?' then carrier_line_haul::numeric
# MAGIC          else 0.00 end as carrier_lhc,
# MAGIC
# MAGIC     case when accessorial1 like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate1 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate1::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial2 like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate2 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate2::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial3 like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate3 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate3::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial4 like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate4 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate4::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial5 like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate5 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate5::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial6 like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate6 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate6::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial7 like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate7 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate7::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial8 like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate8 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate8::numeric
# MAGIC          else 0.00 end else 0.00 end as carrier_fuel,
# MAGIC          
# MAGIC     case when accessorial1 NOT like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate1 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate1::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial2 NOT like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate2 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate2::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial3 NOT like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate3 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate3::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial4 NOT like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate4 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate4::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial5 NOT like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate5 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate5::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial6 NOT like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate6 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate6::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial7 NOT like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate7 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?' then carrier_accessorial_rate7::numeric
# MAGIC          else 0.00 end else 0.00 end + 
# MAGIC     case when accessorial8 NOT like 'FUE%' then 
# MAGIC         case when carrier_accessorial_rate8 like '%:%' then 0.00 
# MAGIC          when carrier_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'then carrier_accessorial_rate8::numeric
# MAGIC          else 0.00 end else 0.00 end as carrier_acc,
# MAGIC         
# MAGIC case when p1.office not in ('10','34','51','54','61','62','63','64','74') then 'USD' 
# MAGIC     else coalesce(cad_currency.currency_type, aljex_customer_profiles.cust_country, 'CAD') end as cust_curr,
# MAGIC     
# MAGIC case when c.name like '%CAD' then 'CAD'
# MAGIC     when c.name like 'CANADIAN R%' then 'CAD'
# MAGIC     when c.name like '%CAN' then 'CAD'
# MAGIC     when c.name like '%(CAD)%' then 'CAD'
# MAGIC     when c.name like '%-C' then 'CAD'
# MAGIC     when c.name like '%- C%' then 'CAD'
# MAGIC     when canada_carriers.canada_dot is not null then 'CAD' ELSE 'USD' end as carrier_curr,
# MAGIC case when canada_conversions.conversion is null then average_conversions.avg_us_to_cad else canada_conversions.conversion end as conversion_rate,
# MAGIC new_office_lookup_w_tgt.new_office as customer_office,
# MAGIC case when p1.key_c_user = 'IMPORT' then
# MAGIC         case when master_customer_name = 'ALTMAN PLANTS' then 'LAX' else 'DRAY' end 
# MAGIC     when p1.key_c_user = 'EDILCR' then 'LTL' 
# MAGIC     when p1.key_c_user is null then new_office_lookup_w_tgt.new_office else car.new_office end as carr_office,
# MAGIC case when p1.equipment like '%LTL%' then 'LTL' else 'TL' end as tl_or_ltl,
# MAGIC case when aljex_mode_types.equipment_mode = 'F' then 'Flatbed'
# MAGIC     when aljex_mode_types.equipment_mode is null then 'Dry Van'
# MAGIC     when aljex_mode_types.equipment_mode = 'R' then 'Reefer'
# MAGIC     when aljex_mode_types.equipment_mode = 'V' then 'Dry Van'
# MAGIC     when aljex_mode_types.equipment_mode = 'PORTD' then 'PORTD'
# MAGIC     when aljex_mode_types.equipment_mode = 'POWER ONLY' then 'POWER ONLY'
# MAGIC     when aljex_mode_types.equipment_mode = 'IMDL' then 'IMDL'  
# MAGIC else case when p1.equipment = 'RLTL' then 'Reefer' else 'Dry Van' end end as modee,
# MAGIC invoice_total,
# MAGIC p1.miles,
# MAGIC p2.status,
# MAGIC initcap(p1.origin_city) as origin_city,
# MAGIC upper(p1.origin_state) as origin_state,
# MAGIC initcap(p1.dest_city) as dest_city,
# MAGIC upper(p1.dest_state) as dest_state,
# MAGIC pickup_zip_code as origin_zip,
# MAGIC consignee_zip_code as dest_zip,
# MAGIC o.market as market_origin,
# MAGIC d.market AS market_dest,
# MAGIC CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC --financial_calendar.financial_period_sorting,
# MAGIC --financial_calendar.financial_year,
# MAGIC c.dot_num::string,
# MAGIC p1.delivery_date::date as delivery_date,
# MAGIC     null::date as bounced_date,
# MAGIC     null::date as rolled_date,
# MAGIC     p1.key_c_date as Booked_Date,
# MAGIC     p2.tag_created_by::string as Tender_Date,
# MAGIC     null::date as cancelled_date,
# MAGIC pickup.Days_ahead as Days_Ahead,
# MAGIC pickup.prebookable as prebookable,
# MAGIC     papp.PickUp_Start,
# MAGIC     papp.PickUp_End,
# MAGIC     dapp.Drop_Start,
# MAGIC     dapp.Drop_End,
# MAGIC c.name as carrier_name
# MAGIC from brokerageprod.bronze.projection_load_1 p1
# MAGIC Left Join brokerageprod.bronze.projection_load_2 p2 on p1.id = p2.id 
# MAGIC ---join brokerageprod.analytics.financial_calendar on p.pickup_date::date = financial_calendar.date::date 
# MAGIC join brokerageprod.bronze.new_office_lookup_w_tgt on p1.office = new_office_lookup_w_tgt.old_office 
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup o ON LEFT(p1.pickup_zip_code, 3) = o.pickup_zip
# MAGIC LEFT JOIN  brokerageprod.bronze.market_lookup d ON LEFT(p1.consignee_zip_code, 3) = d.pickup_zip
# MAGIC left join brokerageprod.bronze.cad_currency on p1.id::string = cad_currency.pro_number::string
# MAGIC left join brokerageprod.bronze.aljex_customer_profiles on p2.customer_id::string = aljex_customer_profiles.cust_id::string 
# MAGIC left join brokerageprod.bronze.projection_carrier c on p1.carrier_id = c.id 
# MAGIC LEFT JOIN 
# MAGIC     (
# MAGIC select system,max(load_number) as max_loadnumber, max(Pickup_date) as Pickup_date, max(prebookable) as prebookable ,Max(days_ahead) as days_ahead
# MAGIC from brokerageprod.bronze.load_pickup
# MAGIC group by system
# MAGIC
# MAGIC     ) as pickup ON p1.id = pickup.max_loadnumber
# MAGIC         LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as PickUp_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'PickUp'
# MAGIC     ) as papp ON  p1.id= papp.loadnum
# MAGIC
# MAGIC             LEFT JOIN 
# MAGIC     (
# MAGIC select max(relay_reference_number) as Loadnum , max(try_cast( pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start, max(try_cast( planning_appt_datetime as TIMESTAMP)) as Drop_End 
# MAGIC from brokerageprod.bronze.load_appointment
# MAGIC where Status = 'drop'
# MAGIC --group by relay_reference_number
# MAGIC
# MAGIC     ) as dapp ON p1.id = dapp.loadnum
# MAGIC left join brokerageprod.bronze.canada_carriers on p1.carrier_id::string = canada_carriers.canada_dot::string
# MAGIC left join brokerageprod.bronze.customer_lookup on left(shipper,32) = left(customer_lookup.aljex_customer_name,32)
# MAGIC left join brokerageprod.bronze.aljex_mode_types on p1.equipment = brokerageprod.bronze.aljex_mode_types.equipment_type 
# MAGIC join average_conversions on 1=1 
# MAGIC left join brokerageprod.bronze.canada_conversions on p1.pickup_date::date = canada_conversions.ship_date::date 
# MAGIC left join brokerageprod.bronze.aljex_user_report_listing on p1.key_c_user = aljex_user_report_listing.aljex_id 
# MAGIC left join brokerageprod.bronze.aljex_user_report_listing z on p2.srv_rep = z.aljex_id 
# MAGIC left join brokerageprod.bronze.relay_users on aljex_user_report_listing.full_name = relay_users.full_name 
# MAGIC left join brokerageprod.bronze.new_office_lookup_w_tgt car on coalesce(aljex_user_report_listing.pnl_code, relay_users.office_id) = car.old_office 
# MAGIC where 1=1
# MAGIC --[[and {{date_range}}]]
# MAGIC and p2.status not in ('OPEN','ASSIGNED')
# MAGIC and p2.status not like '%VOID%' 
# MAGIC and p1.key_c_user is not null )
# MAGIC
# MAGIC ---Select * from aljex_data where use_date >= '2024-08-11' and use_date <= '2024-08-17'
# MAGIC
# MAGIC ,system_union as  (
# MAGIC select distinct ----- Shipper
# MAGIC relay_load as Load_Number,
# MAGIC --0.5 as Load_Counter,
# MAGIC use_date as Ship_Date,
# MAGIC Equipment as Mode,
# MAGIC Case when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC when modee = 'IMDL' THEN 'Intermodal'
# MAGIC  when modee = 'PORTD' THEN 'Drayage'
# MAGIC  else modee End as Equipment,
# MAGIC --modee as Mode,
# MAGIC --mode as Equipment,
# MAGIC date_range_two.week_num as Week_Num,
# MAGIC Case when customer_office = 'TGT' then 'PHI'
# MAGIC else customer_office end as customer_office,
# MAGIC --customer_office as Customer_Office,
# MAGIC Case when carr_office = 'TGT' then 'PHI'
# MAGIC else carr_office end as Carrier_Office,
# MAGIC --carr_office as Carrier_Office,
# MAGIC --'Shipper' as Office_Type,
# MAGIC Carrier_Rep as Booked_By,
# MAGIC --carr_office as Carr_Office,
# MAGIC --financial_period_sorting as "Financial Period",
# MAGIC --financial_year as "Financial Yr",
# MAGIC mastername as customer_master,
# MAGIC customer_name,
# MAGIC customer_rep,
# MAGIC status as load_status,
# MAGIC origin_city as Origin_City,
# MAGIC origin_state as Origin_State,
# MAGIC dest_city as Dest_City,
# MAGIC dest_state as Dest_State,
# MAGIC origin_zip as Origin_Zip,
# MAGIC dest_zip as Dest_Zip,
# MAGIC  CONCAT( INITCAP(origin_city),
# MAGIC       ',',
# MAGIC       origin_state,
# MAGIC       '>',
# MAGIC       INITCAP(dest_city),
# MAGIC       ',',
# MAGIC       dest_state
# MAGIC     ) AS load_lane,
# MAGIC market_origin as Market_origin,
# MAGIC market_dest as Market_Deat,
# MAGIC market_lane as Market_Lane,
# MAGIC del_date as Delivery_Date,
# MAGIC rolled_at as rolled_date,
# MAGIC Booked_Date as Booked_Date,
# MAGIC Tendering_Date as Tendered_Date,
# MAGIC bounced_date as bounced_date,
# MAGIC cancelled_date as Cancelled_Date,
# MAGIC Days_ahead as Days_Ahead,
# MAGIC prebookable,
# MAGIC PickUp_Start as Pickup_Date,
# MAGIC PickUp_End as Pickup_EndDate,
# MAGIC Drop_Start as Drop_Start,
# MAGIC Drop_End as Drop_End,
# MAGIC invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC --load_pickup.Days_ahead as Days_Ahead,
# MAGIC CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC case when carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC dot_num as DOT_Num,
# MAGIC carrier_name as Carrier,
# MAGIC total_miles::float as Miles,
# MAGIC total_cust_rate.total_cust_rate::float as Revenue,
# MAGIC total_carrier_rate.total_carrier_rate::float  as Expense,
# MAGIC (coalesce(total_cust_rate.total_cust_rate,0)::float - coalesce(total_carrier_rate.total_carrier_rate,0)::float)  as Margin,
# MAGIC carrier_charges.carr_lhc  as Linehaul,
# MAGIC carrier_charges.carr_fsc  as Fuel_Surcharge,
# MAGIC carrier_charges.carr_acc  as Other_Fess,
# MAGIC 'RELAY' as TMS_System
# MAGIC from use_relay_loads
# MAGIC left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number 
# MAGIC left join total_carrier_rate on use_relay_loads.relay_load = total_carrier_rate.relay_reference_number 
# MAGIC left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date
# MAGIC left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC left join carrier_charges on  use_relay_loads.relay_load = carrier_charges.relay_reference_number
# MAGIC --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC where 1=1 
# MAGIC and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC and combined_load = 'NO'
# MAGIC
# MAGIC --UNION
# MAGIC
# MAGIC --select distinct ----- Carrier
# MAGIC --relay_load as Load_Number,
# MAGIC --0.5 as Load_Counter,
# MAGIC --use_date as Ship_Date,
# MAGIC --modee as Mode,
# MAGIC --mode as Equipment,
# MAGIC --date_range_two.week_num as Week_Num,
# MAGIC --carr_office as Carr_Office,
# MAGIC --customer_office as Office,
# MAGIC --'Carrier' as Office_Type,
# MAGIC --Carrier_Rep as Carrier_Rep,
# MAGIC --financial_period_sorting as "Financial Period",
# MAGIC --financial_year as "Financial Yr",
# MAGIC ---mastername as customer_master,
# MAGIC --customer_name,
# MAGIC --customer_rep,
# MAGIC --status as load_status,
# MAGIC --origin_city as Origin_City,
# MAGIC --origin_state as Origin_State,
# MAGIC --dest_city as Dest_City,
# MAGIC --dest_state as Dest_State,
# MAGIC --origin_zip as Origin_Zip,
# MAGIC --dest_zip as Dest_Zip,
# MAGIC  --CONCAT( INITCAP(origin_city),
# MAGIC       --',',
# MAGIC       --origin_state,
# MAGIC       --'>',
# MAGIC       --INITCAP(dest_city),
# MAGIC       --',',
# MAGIC       --dest_state
# MAGIC     --) AS load_lane,
# MAGIC --market_origin as Market_origin,
# MAGIC ---market_dest as Market_Deat,
# MAGIC --market_lane as Market_Lane,
# MAGIC --del_date as Delivery_Date,
# MAGIC --rolled_at as rolled_date,
# MAGIC --Booked_Date as Booked_Date,
# MAGIC --Tendering_Date as Tendered_Date,
# MAGIC ---bounced_date as bounced_date,
# MAGIC ---cancelled_date as Cancelled_Date,
# MAGIC ---Days_ahead as Days_Ahead,
# MAGIC ---prebookable,
# MAGIC ---PickUp_Start as Pickup_Date,
# MAGIC --PickUp_End as Pickup_EndDate,
# MAGIC --Drop_Start as Drop_Start,
# MAGIC --Drop_End as Drop_End,
# MAGIC --invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC --load_pickup.Days_ahead as Days_Ahead,
# MAGIC --CASE
# MAGIC   --      WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC    --     ELSE 'Same Office'
# MAGIC     --END AS Booking_type,
# MAGIC ---case when carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC --dot_num as DOT_Num,
# MAGIC --carrier_name as Carrier,
# MAGIC --total_miles::float as Miles,
# MAGIC --total_cust_rate.total_cust_rate::float / 2 as Revenue,
# MAGIC --total_carrier_rate.total_carrier_rate::float / 2  as Expense,
# MAGIC --(coalesce(total_cust_rate.total_cust_rate,0)::float - coalesce(total_carrier_rate.total_carrier_rate,0)::float) / 2 as Margin,
# MAGIC ---carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC ---carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC --carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC ---'RELAY' as TMS_System
# MAGIC ---from use_relay_loads
# MAGIC --left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number 
# MAGIC --left join total_carrier_rate on use_relay_loads.relay_load = total_carrier_rate.relay_reference_number 
# MAGIC ---left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date
# MAGIC ---left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC --left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC --where 1=1 
# MAGIC --and use_relay_loads.mode not like 'ltl%'
# MAGIC --and combined_load = 'NO'
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct ---- Shipper - Hain Roloadn and Deb
# MAGIC relay_load as relay_ref_num,
# MAGIC --0.5 as Load_Counter,
# MAGIC use_date,
# MAGIC  Equipment as Mode,
# MAGIC Case when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC when modee = 'IMDL' THEN 'Intermodal'
# MAGIC  when modee = 'PORTD' THEN 'Drayage'
# MAGIC  else modee End as Equipment,
# MAGIC date_range_two.week_num,
# MAGIC Case when customer_office = 'TGT' then 'PHI'
# MAGIC else customer_office end as customer_office,
# MAGIC --customer_office as Customer_Office,
# MAGIC Case when carr_office = 'TGT' then 'PHI'
# MAGIC else carr_office end as Carrier_Office,
# MAGIC --carr_office as Carrier_Office,
# MAGIC --'Shipper',
# MAGIC Carrier_Rep,
# MAGIC --carr_office,
# MAGIC ---financial_period_sorting,
# MAGIC --financial_year,
# MAGIC mastername,
# MAGIC customer_name,
# MAGIC customer_rep,
# MAGIC status as load_status,
# MAGIC origin_city,
# MAGIC origin_state,
# MAGIC dest_city,
# MAGIC dest_state,
# MAGIC origin_zip as Origin_Zip,
# MAGIC dest_zip as Dest_Zip,
# MAGIC  CONCAT( INITCAP(origin_city),
# MAGIC       ',',
# MAGIC       origin_state,
# MAGIC       '>',
# MAGIC       INITCAP(dest_city),
# MAGIC       ',',
# MAGIC       dest_state
# MAGIC     ) AS load_lane,
# MAGIC market_origin as Market_origin,
# MAGIC market_dest as Market_Deat,
# MAGIC market_lane as Market_Lane,
# MAGIC del_date as Delivery_Date,
# MAGIC rolled_at as rolled_date,
# MAGIC Booked_Date as Booked_Date,
# MAGIC Tendering_Date as Tendered_Date,
# MAGIC bounced_date as bounced_date,
# MAGIC cancelled_date as Cancelled_Date,
# MAGIC Days_ahead as Days_Ahead,
# MAGIC prebookable,
# MAGIC PickUp_Start as Pickup_Date,
# MAGIC PickUp_End as Pickup_EndDate,
# MAGIC Drop_Start as Drop_Start,
# MAGIC Drop_End as Drop_End,
# MAGIC invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC --load_pickup.Days_ahead as Days_Ahead,
# MAGIC CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC case when e.carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC dot_num as DOT_Number,
# MAGIC e.carrier_name as Carrier,
# MAGIC total_miles::float,
# MAGIC total_cust_rate.total_cust_rate::float  as SplitRev,
# MAGIC (projected_expense::float/100.0) as splitexp,
# MAGIC (coalesce(total_cust_rate.total_cust_rate,0)::float - coalesce(projected_expense::float/100.0,0)::float)  as splitmargin,
# MAGIC carrier_charges.carr_lhc  as Linehaul,
# MAGIC carrier_charges.carr_fsc  as Fuel_Surcharge,
# MAGIC carrier_charges.carr_acc  as Other_Fess,
# MAGIC 'RELAY' as tms
# MAGIC from use_relay_loads
# MAGIC left join brokerageprod.bronze.big_export_projection e on use_relay_loads.relay_load::float = e.load_number::float 
# MAGIC left join total_cust_rate on use_relay_loads.relay_load::float = total_cust_rate.relay_reference_number::float 
# MAGIC left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date
# MAGIC left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC left join carrier_charges on  use_relay_loads.relay_load = carrier_charges.relay_reference_number
# MAGIC --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC where 1=1 
# MAGIC and use_relay_loads.Mode_Filter = 'ltl'
# MAGIC and e.dispatch_status != 'Cancelled'
# MAGIC and e.carrier_name is not null 
# MAGIC and e.customer_id not in ('hain','roland','deb')
# MAGIC
# MAGIC ---UNION
# MAGIC
# MAGIC ----select distinct ---- Carrier Hain Roloadn and De
# MAGIC --relay_load as relay_ref_num,
# MAGIC --0.5 as Load_Counter,
# MAGIC --use_date,
# MAGIC --modee as Mode,
# MAGIC --mode as Equipment,
# MAGIC --date_range_two.week_num,
# MAGIC --carr_office as office,
# MAGIC --'Carrier',
# MAGIC --Carrier_Rep,
# MAGIC ----carr_office,
# MAGIC -----financial_period_sorting,
# MAGIC ----financial_year,
# MAGIC --mastername,
# MAGIC --customer_name,
# MAGIC --customer_rep,
# MAGIC --status as load_status,
# MAGIC --origin_city,
# MAGIC --origin_state,
# MAGIC --dest_city,
# MAGIC --dest_state,
# MAGIC --origin_zip as Origin_Zip,
# MAGIC --dest_zip as Dest_Zip,
# MAGIC -- CONCAT( INITCAP(origin_city),
# MAGIC --      ',',
# MAGIC --      origin_state,
# MAGIC --      '>',
# MAGIC --      INITCAP(dest_city),
# MAGIC --      ',',
# MAGIC --      dest_state
# MAGIC --    ) AS load_lane,
# MAGIC --market_origin as Market_origin,
# MAGIC --market_dest as Market_Deat,
# MAGIC --market_lane as Market_Lane,
# MAGIC --del_date as Delivery_Date,
# MAGIC --rolled_at as rolled_date,
# MAGIC --Booked_Date as Booked_Date,
# MAGIC --Tendering_Date as Tendered_Date,
# MAGIC --bounced_date as bounced_date,
# MAGIC --cancelled_date as Cancelled_Date,
# MAGIC --Days_ahead as Days_Ahead,
# MAGIC --prebookable,
# MAGIC --PickUp_Start as Pickup_Date,
# MAGIC --PickUp_End as Pickup_EndDate,
# MAGIC --Drop_Start as Drop_Start,
# MAGIC --Drop_End as Drop_End,
# MAGIC --invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC ----load_pickup.Days_ahead as Days_Ahead,
# MAGIC --CASE
# MAGIC --        WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC --        ELSE 'Same Office'
# MAGIC --    END AS Booking_type,
# MAGIC --case when e.carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC --dot_num as DOT_Number,
# MAGIC --e.carrier_name as Carrier,
# MAGIC --total_miles::float,
# MAGIC --total_cust_rate.total_cust_rate::float / 2 as SplitRev,
# MAGIC --(projected_expense::float/100.0) / 2 as splitexp,
# MAGIC --(coalesce(total_cust_rate.total_cust_rate,0)::float - coalesce(projected_expense::float/100.0,0)::float) / 2 as splitmargin,
# MAGIC --carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC --carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC --carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC --'RELAY' as tms
# MAGIC --from use_relay_loads
# MAGIC --left join brokerageprod.bronze.big_export_projection e on use_relay_loads.relay_load::float = e.load_number::float 
# MAGIC --left join total_cust_rate on use_relay_loads.relay_load::float = total_cust_rate.relay_reference_number::float 
# MAGIC --left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date
# MAGIC --left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC --left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC ----left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC --where 1=1 
# MAGIC --and use_relay_loads.mode = 'ltl'
# MAGIC --and e.dispatch_status != 'Cancelled'
# MAGIC --and e.carrier_name is not null 
# MAGIC --and e.customer_id not in ('hain','roland','deb')
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct -- Shipper
# MAGIC relay_load as relay_ref_num,
# MAGIC --0.5 as Load_Counter,
# MAGIC use_date,
# MAGIC Equipment as Mode,
# MAGIC Case when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC when modee = 'IMDL' THEN 'Intermodal'
# MAGIC  when modee = 'PORTD' THEN 'Drayage'
# MAGIC  else modee End as Equipment,
# MAGIC date_range_two.week_num,
# MAGIC Case when customer_office = 'TGT' then 'PHI'
# MAGIC else customer_office end as customer_office,
# MAGIC --customer_office as Customer_Office,
# MAGIC Case when carr_office = 'TGT' then 'PHI'
# MAGIC else carr_office end as Carrier_Office,
# MAGIC --carr_office as Carrier_Office,
# MAGIC --'Shipper',
# MAGIC Carrier_Rep,
# MAGIC --carr_office,
# MAGIC --financial_period_sorting,
# MAGIC --financial_year,
# MAGIC mastername,
# MAGIC customer_name,
# MAGIC customer_rep,
# MAGIC status as load_status,
# MAGIC origin_city,
# MAGIC origin_state,
# MAGIC dest_city,
# MAGIC dest_state,
# MAGIC origin_zip as Origin_Zip,
# MAGIC dest_zip as Dest_Zip,
# MAGIC  CONCAT( INITCAP(origin_city),
# MAGIC       ',',
# MAGIC       origin_state,
# MAGIC       '>',
# MAGIC       INITCAP(dest_city),
# MAGIC       ',',
# MAGIC       dest_state
# MAGIC     ) AS load_lane,
# MAGIC market_origin as Market_origin,
# MAGIC market_dest as Market_Deat,
# MAGIC market_lane as Market_Lane,
# MAGIC del_date as Delivery_Date,
# MAGIC rolled_at as rolled_date,
# MAGIC Booked_Date as Booked_Date,
# MAGIC Tendering_Date as Tendered_Date,
# MAGIC bounced_date as bounced_date,
# MAGIC cancelled_date as Cancelled_Date,
# MAGIC Days_ahead as Days_Ahead,
# MAGIC prebookable,
# MAGIC PickUp_Start as Pickup_Date,
# MAGIC PickUp_End as Pickup_EndDate,
# MAGIC Drop_Start as Drop_Start,
# MAGIC Drop_End as Drop_End,
# MAGIC invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC --load_pickup.Days_ahead as Days_Ahead,
# MAGIC CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC case when e.carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC dot_num as DOT_Number,
# MAGIC e.carrier_name as Carrier,
# MAGIC total_miles::float,
# MAGIC (    (carrier_accessorial_expense +
# MAGIC     ceiling(case customer_id     
# MAGIC         when 'deb' then 
# MAGIC             case carrier_id when '300003979' 
# MAGIC                 then greatest(carrier_linehaul_expense * (10.0/100.0), 1500)
# MAGIC                     else
# MAGIC                         case when e.ship_date::date < '2019-03-09' 
# MAGIC                             then greatest(carrier_linehaul_expense * (8.0/100.0), 1000)
# MAGIC                             else greatest(carrier_linehaul_expense * (9.0/100.0), 1000)
# MAGIC                         end 
# MAGIC                 end    
# MAGIC         when 'hain' 
# MAGIC             then greatest(carrier_linehaul_expense * (10.0/100.0), 1000)
# MAGIC         when 'roland'
# MAGIC             then carrier_linehaul_expense * 0.1365
# MAGIC         else 0 
# MAGIC     end + carrier_linehaul_expense) 
# MAGIC     + ceiling(ceiling(case customer_id     
# MAGIC         when 'deb' then 
# MAGIC             case carrier_id when '300003979' 
# MAGIC                 then greatest(carrier_linehaul_expense * (10.0/100.0), 1500)
# MAGIC                     else
# MAGIC                         case when e.ship_date::date < '2019-03-09' 
# MAGIC                             then greatest(carrier_linehaul_expense * (8.0/100.0), 1000)
# MAGIC                             else greatest(carrier_linehaul_expense * (9.0/100.0), 1000)
# MAGIC                         end 
# MAGIC                 end    
# MAGIC         when 'hain' 
# MAGIC             then greatest(carrier_linehaul_expense * (10.0/100.0), 1000)
# MAGIC         when 'roland'
# MAGIC             then carrier_linehaul_expense * 0.1365
# MAGIC         else 0 
# MAGIC     end + carrier_linehaul_expense) *     
# MAGIC     (case carrier_linehaul_expense when 0 then 0 else carrier_fuel_expense::float / carrier_linehaul_expense::float end))) / 100)::float  as split_rev,
# MAGIC
# MAGIC (projected_expense::float/100.0)  as split_exp,
# MAGIC
# MAGIC (coalesce(((carrier_accessorial_expense +
# MAGIC     ceiling(case customer_id     
# MAGIC         when 'deb' then 
# MAGIC             case carrier_id when '300003979' 
# MAGIC                 then greatest(carrier_linehaul_expense * (10.0/100.0), 1500)
# MAGIC                     else
# MAGIC                         case when e.ship_date::date < '2019-03-09' 
# MAGIC                             then greatest(carrier_linehaul_expense * (8.0/100.0), 1000)
# MAGIC                             else greatest(carrier_linehaul_expense * (9.0/100.0), 1000)
# MAGIC                         end 
# MAGIC                 end    
# MAGIC         when 'hain' 
# MAGIC             then greatest(carrier_linehaul_expense * (10.0/100.0), 1000)
# MAGIC         when 'roland'
# MAGIC             then carrier_linehaul_expense * 0.1365
# MAGIC         else 0 
# MAGIC     end + carrier_linehaul_expense) 
# MAGIC     + ceiling(ceiling(case customer_id     
# MAGIC         when 'deb' then 
# MAGIC             case carrier_id when '300003979' 
# MAGIC                 then greatest(carrier_linehaul_expense * (10.0/100.0), 1500)
# MAGIC                     else
# MAGIC                         case when e.ship_date::date < '2019-03-09' 
# MAGIC                             then greatest(carrier_linehaul_expense * (8.0/100.0), 1000)
# MAGIC                             else greatest(carrier_linehaul_expense * (9.0/100.0), 1000)
# MAGIC                         end 
# MAGIC                 end    
# MAGIC         when 'hain' 
# MAGIC             then greatest(carrier_linehaul_expense * (10.0/100.0), 1000)
# MAGIC         when 'roland'
# MAGIC             then carrier_linehaul_expense * 0.1365
# MAGIC         else 0 
# MAGIC     end + carrier_linehaul_expense) *     
# MAGIC     (case carrier_linehaul_expense when 0 then 0 else carrier_fuel_expense::float / carrier_linehaul_expense::float end))) / 100),0)::float - 
# MAGIC coalesce((projected_expense::float/100.0),0))::float  as  split_margin,
# MAGIC carrier_charges.carr_lhc as Linehaul,
# MAGIC carrier_charges.carr_fsc  as Fuel_Surcharge,
# MAGIC carrier_charges.carr_acc  as Other_Fess,
# MAGIC 'RELAY' as tms
# MAGIC from use_relay_loads
# MAGIC left join brokerageprod.bronze.big_export_projection e on use_relay_loads.relay_load::float = e.load_number::float 
# MAGIC left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date
# MAGIC left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC left join carrier_charges on  use_relay_loads.relay_load = carrier_charges.relay_reference_number
# MAGIC --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC where 1=1 
# MAGIC and use_relay_loads.Mode_Filter = 'ltl- (hain,deb,roland)'
# MAGIC and e.dispatch_status != 'Cancelled'
# MAGIC and e.carrier_name is not null 
# MAGIC and e.customer_id in ('hain','roland','deb')
# MAGIC
# MAGIC --UNION
# MAGIC
# MAGIC ---select distinct -- Carrier
# MAGIC ---relay_load as relay_ref_num,
# MAGIC ---0.5 as Load_Counter,
# MAGIC ---use_date,
# MAGIC ---modee as Mode,
# MAGIC ---mode as Equipment,
# MAGIC ---date_range_two.week_num,
# MAGIC ---carr_office as office,
# MAGIC ---'Carrier',
# MAGIC ---Carrier_Rep,
# MAGIC -----carr_office,
# MAGIC -----financial_period_sorting,
# MAGIC -----financial_year,
# MAGIC ---mastername,
# MAGIC ---customer_name,
# MAGIC ---customer_rep,
# MAGIC ---status as load_status,
# MAGIC ---origin_city,
# MAGIC ---origin_state,
# MAGIC ---dest_city,
# MAGIC ---dest_state,
# MAGIC ---origin_zip as Origin_Zip,
# MAGIC ---dest_zip as Dest_Zip,
# MAGIC --- CONCAT( INITCAP(origin_city),
# MAGIC ---      ',',
# MAGIC ---      origin_state,
# MAGIC ---      '>',
# MAGIC ---      INITCAP(dest_city),
# MAGIC ---      ',',
# MAGIC ---      dest_state
# MAGIC ---    ) AS load_lane,
# MAGIC ---market_origin as Market_origin,
# MAGIC ---market_dest as Market_Deat,
# MAGIC ---market_lane as Market_Lane,
# MAGIC ---del_date as Delivery_Date,
# MAGIC ---rolled_at as rolled_date,
# MAGIC ---Booked_Date as Booked_Date,
# MAGIC ---Tendering_Date as Tendered_Date,
# MAGIC ---bounced_date as bounced_date,
# MAGIC ---cancelled_date as Cancelled_Date,
# MAGIC ---Days_ahead as Days_Ahead,
# MAGIC ---prebookable,
# MAGIC ---PickUp_Start as Pickup_Date,
# MAGIC ---PickUp_End as Pickup_EndDate,
# MAGIC ---Drop_Start as Drop_Start,
# MAGIC ---Drop_End as Drop_End,
# MAGIC ---invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC -----load_pickup.Days_ahead as Days_Ahead,
# MAGIC ---CASE
# MAGIC ---        WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC ---        ELSE 'Same Office'
# MAGIC ---    END AS Booking_type,
# MAGIC ---case when e.carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC ---dot_num as DOT_Number,
# MAGIC ---e.carrier_name as Carrier,
# MAGIC ---total_miles::float,
# MAGIC ---(    (carrier_accessorial_expense +
# MAGIC ---    ceiling(case customer_id     
# MAGIC ---        when 'deb' then 
# MAGIC ---            case carrier_id when '300003979' 
# MAGIC ---                then greatest(carrier_linehaul_expense * (10.0/100.0), 1500)
# MAGIC ---                    else
# MAGIC ---                        case when e.ship_date::date < '2019-03-09' 
# MAGIC ---                            then greatest(carrier_linehaul_expense * (8.0/100.0), 1000)
# MAGIC ---                            else greatest(carrier_linehaul_expense * (9.0/100.0), 1000)
# MAGIC ---                        end 
# MAGIC ---                end    
# MAGIC ---        when 'hain' 
# MAGIC ---            then greatest(carrier_linehaul_expense * (10.0/100.0), 1000)
# MAGIC ---        when 'roland'
# MAGIC ---            then carrier_linehaul_expense * 0.1365
# MAGIC ---        else 0 
# MAGIC ---    end + carrier_linehaul_expense) 
# MAGIC ---    + ceiling(ceiling(case customer_id     
# MAGIC ---        when 'deb' then 
# MAGIC ---            case carrier_id when '300003979' 
# MAGIC ---                then greatest(carrier_linehaul_expense * (10.0/100.0), 1500)
# MAGIC ---                    else
# MAGIC ---                        case when e.ship_date::date < '2019-03-09' 
# MAGIC ---                            then greatest(carrier_linehaul_expense * (8.0/100.0), 1000)
# MAGIC ---                            else greatest(carrier_linehaul_expense * (9.0/100.0), 1000)
# MAGIC ---                        end 
# MAGIC ---                end    
# MAGIC ---        when 'hain' 
# MAGIC ---            then greatest(carrier_linehaul_expense * (10.0/100.0), 1000)
# MAGIC ---        when 'roland'
# MAGIC ---            then carrier_linehaul_expense * 0.1365
# MAGIC ---        else 0 
# MAGIC ---    end + carrier_linehaul_expense) *     
# MAGIC ---    (case carrier_linehaul_expense when 0 then 0 else carrier_fuel_expense::float / carrier_linehaul_expense::float end))) / 100)::float / 2 as split_rev,
# MAGIC ---
# MAGIC ---(projected_expense::float/100.0) / 2 as split_exp,
# MAGIC ---
# MAGIC ---(coalesce(((carrier_accessorial_expense +
# MAGIC ---    ceiling(case customer_id     
# MAGIC ---        when 'deb' then 
# MAGIC ---            case carrier_id when '300003979' 
# MAGIC ---                then greatest(carrier_linehaul_expense * (10.0/100.0), 1500)
# MAGIC ---                    else
# MAGIC ---                        case when e.ship_date::date < '2019-03-09' 
# MAGIC ---                            then greatest(carrier_linehaul_expense * (8.0/100.0), 1000)
# MAGIC ---                            else greatest(carrier_linehaul_expense * (9.0/100.0), 1000)
# MAGIC ---                        end 
# MAGIC ---                end    
# MAGIC ---        when 'hain' 
# MAGIC ---            then greatest(carrier_linehaul_expense * (10.0/100.0), 1000)
# MAGIC ---        when 'roland'
# MAGIC ---            then carrier_linehaul_expense * 0.1365
# MAGIC ---        else 0 
# MAGIC ---    end + carrier_linehaul_expense) 
# MAGIC ---    + ceiling(ceiling(case customer_id     
# MAGIC ---        when 'deb' then 
# MAGIC ---            case carrier_id when '300003979' 
# MAGIC ---                then greatest(carrier_linehaul_expense * (10.0/100.0), 1500)
# MAGIC ---                    else
# MAGIC ---                        case when e.ship_date::date < '2019-03-09' 
# MAGIC ---                            then greatest(carrier_linehaul_expense * (8.0/100.0), 1000)
# MAGIC ---                            else greatest(carrier_linehaul_expense * (9.0/100.0), 1000)
# MAGIC ---                        end 
# MAGIC ---                end    
# MAGIC ---        when 'hain' 
# MAGIC ---            then greatest(carrier_linehaul_expense * (10.0/100.0), 1000)
# MAGIC ---        when 'roland'
# MAGIC ---            then carrier_linehaul_expense * 0.1365
# MAGIC ---        else 0 
# MAGIC ---    end + carrier_linehaul_expense) *     
# MAGIC ---    (case carrier_linehaul_expense when 0 then 0 else carrier_fuel_expense::float / carrier_linehaul_expense::float end))) / 100),0)::float - 
# MAGIC ---coalesce((projected_expense::float/100.0),0))::float / 2 as split_margin,
# MAGIC ---carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC ---carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC ---carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC ---'RELAY' as tms
# MAGIC ---from use_relay_loads
# MAGIC ---left join brokerageprod.bronze.big_export_projection e on use_relay_loads.relay_load::float = e.load_number::float 
# MAGIC ---left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date
# MAGIC ---left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC ---left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC -----left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC ---where 1=1 
# MAGIC ---and use_relay_loads.mode = 'ltl- (hain,deb,roland)'
# MAGIC ---and e.dispatch_status != 'Cancelled'
# MAGIC ---and e.carrier_name is not null 
# MAGIC ---and e.customer_id in ('hain','roland','deb')
# MAGIC ---
# MAGIC union
# MAGIC
# MAGIC select distinct -- Shipper
# MAGIC relay_load as relay_ref_num,
# MAGIC --0.5 as Load_Counter,
# MAGIC use_date,
# MAGIC Equipment as Mode,
# MAGIC Case when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC when modee = 'IMDL' THEN 'Intermodal'
# MAGIC  when modee = 'PORTD' THEN 'Drayage'
# MAGIC  else modee End as Equipment,
# MAGIC date_range_two.week_num,
# MAGIC Case when customer_office = 'TGT' then 'PHI'
# MAGIC else customer_office end as customer_office,
# MAGIC --customer_office as Customer_Office,
# MAGIC Case when carr_office = 'TGT' then 'PHI'
# MAGIC else carr_office end as Carrier_Office,
# MAGIC --carr_office as Carrier_Office,
# MAGIC --'Shipper',
# MAGIC Carrier_Rep,
# MAGIC --carr_office,
# MAGIC --financial_period_sorting,
# MAGIC --financial_year,
# MAGIC mastername,
# MAGIC customer_name,
# MAGIC customer_rep,
# MAGIC status as load_status,
# MAGIC origin_city,
# MAGIC origin_state,
# MAGIC dest_city,
# MAGIC dest_state,
# MAGIC origin_zip as Origin_Zip,
# MAGIC dest_zip as Dest_Zip,
# MAGIC  CONCAT( INITCAP(origin_city),
# MAGIC       ',',
# MAGIC       origin_state,
# MAGIC       '>',
# MAGIC       INITCAP(dest_city),
# MAGIC       ',',
# MAGIC       dest_state
# MAGIC     ) AS load_lane,
# MAGIC market_origin as Market_origin,
# MAGIC market_dest as Market_Deat,
# MAGIC market_lane as Market_Lane,
# MAGIC del_date as Delivery_Date,
# MAGIC rolled_at as rolled_date,
# MAGIC Booked_Date as Booked_Date,
# MAGIC Tendering_Date as Tendered_Date,
# MAGIC bounced_date as bounced_date,
# MAGIC cancelled_date as Cancelled_Date,
# MAGIC Days_ahead as Days_Ahead,
# MAGIC prebookable,
# MAGIC PickUp_Start as Pickup_Date,
# MAGIC PickUp_End as Pickup_EndDate,
# MAGIC Drop_Start as Drop_Start,
# MAGIC Drop_End as Drop_End,
# MAGIC invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC --load_pickup.Days_ahead as Days_Ahead,
# MAGIC CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC case when use_relay_loads.carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC dot_num as DOT_Number,
# MAGIC use_relay_loads.carrier_name as Carrier,
# MAGIC total_miles::float,
# MAGIC total_cust_rate.total_cust_rate::float  as split_rev,
# MAGIC total_carrier_rate.total_carrier_rate::float as split_exp,
# MAGIC coalesce(total_cust_rate.total_cust_rate,0)::float - coalesce((total_carrier_rate.total_carrier_rate::float / 2),0)::float  as split_margin,
# MAGIC carrier_charges.carr_lhc  as Linehaul,
# MAGIC carrier_charges.carr_fsc  as Fuel_Surcharge,
# MAGIC carrier_charges.carr_acc  as Other_Fess,
# MAGIC 'RELAY' as tms
# MAGIC from use_relay_loads
# MAGIC left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number 
# MAGIC left join total_carrier_rate on use_relay_loads.combined_load = total_carrier_rate.relay_reference_number 
# MAGIC left join combined_loads_mine on use_relay_loads.relay_load::float = combined_loads_mine.combine_new_rrn
# MAGIC left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date
# MAGIC left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC left join carrier_charges on  use_relay_loads.relay_load = carrier_charges.relay_reference_number
# MAGIC --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC where 1=1 
# MAGIC and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC and combined_load != 'NO'
# MAGIC
# MAGIC --UNION
# MAGIC --
# MAGIC --select distinct -- Carrier
# MAGIC --relay_load as relay_ref_num,
# MAGIC --0.5 as Load_Counter,
# MAGIC --use_date,
# MAGIC --modee as Mode,
# MAGIC --use_relay_loads.mode as Equipment,
# MAGIC --date_range_two.week_num,
# MAGIC --carr_office as office,
# MAGIC --'Carrier',
# MAGIC --Carrier_Rep,
# MAGIC ----carr_office,
# MAGIC ----financial_period_sorting,
# MAGIC ----financial_year,
# MAGIC --mastername,
# MAGIC --customer_name,
# MAGIC --customer_rep,
# MAGIC --status as load_status,
# MAGIC --origin_city,
# MAGIC --origin_state,
# MAGIC --dest_city,
# MAGIC --dest_state,
# MAGIC --origin_zip as Origin_Zip,
# MAGIC --dest_zip as Dest_Zip,
# MAGIC -- CONCAT( INITCAP(origin_city),
# MAGIC --      ',',
# MAGIC --      origin_state,
# MAGIC --      '>',
# MAGIC --      INITCAP(dest_city),
# MAGIC --      ',',
# MAGIC --      dest_state
# MAGIC --    ) AS load_lane,
# MAGIC --market_origin as Market_origin,
# MAGIC --market_dest as Market_Deat,
# MAGIC --market_lane as Market_Lane,
# MAGIC --del_date as Delivery_Date,
# MAGIC --rolled_at as rolled_date,
# MAGIC --Booked_Date as Booked_Date,
# MAGIC --Tendering_Date as Tendered_Date,
# MAGIC --bounced_date as bounced_date,
# MAGIC --cancelled_date as Cancelled_Date,
# MAGIC --Days_ahead as Days_Ahead,
# MAGIC --prebookable,
# MAGIC --PickUp_Start as Pickup_Date,
# MAGIC --PickUp_End as Pickup_EndDate,
# MAGIC --Drop_Start as Drop_Start,
# MAGIC --Drop_End as Drop_End,
# MAGIC --invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC ----load_pickup.Days_ahead as Days_Ahead,
# MAGIC --CASE
# MAGIC --        WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC --        ELSE 'Same Office'
# MAGIC --    END AS Booking_type,
# MAGIC --case when carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC --dot_num as DOT_Number,
# MAGIC --use_relay_loads.carrier_name as Carrier,
# MAGIC --total_miles::float,
# MAGIC --total_cust_rate.total_cust_rate::float / 2 as split_rev,
# MAGIC --total_carrier_rate.total_carrier_rate::float / 2::float as split_exp,
# MAGIC --coalesce(total_cust_rate.total_cust_rate,0)::float - coalesce((total_carrier_rate.total_carrier_rate::float / 2),0)::float / 2 as split_margin,
# MAGIC --carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC --carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC --carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC --'RELAY' as tms
# MAGIC --from use_relay_loads
# MAGIC --left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number 
# MAGIC --left join total_carrier_rate on use_relay_loads.combined_load = total_carrier_rate.relay_reference_number 
# MAGIC --left join combined_loads_mine on use_relay_loads.relay_load::float = combined_loads_mine.combine_new_rrn
# MAGIC --left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date
# MAGIC --left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC --left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC ----left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC --where 1=1 
# MAGIC --and use_relay_loads.mode not like 'ltl%'
# MAGIC --and combined_load != 'NO'
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct -- Shipper  ----- Need to change from here.
# MAGIC id::float as relay_ref_num,
# MAGIC --0.5 as Load_Counter,
# MAGIC use_date,
# MAGIC tl_or_ltl as Mode,
# MAGIC Case when modee = 'Dry Van' then 'Dry Van'
# MAGIC  when modee = 'Reefer' then 'Reefer'
# MAGIC  when modee = 'Flatbed' then 'Flatbed'
# MAGIC  when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC  when modee = 'IMDL' THEN 'Intermodal'
# MAGIC  when modee = 'PORTD' THEN 'Drayage'
# MAGIC else null end as Equipment,
# MAGIC date_range_two.week_num,
# MAGIC Case when customer_office = 'TGT' then 'PHI'
# MAGIC else customer_office end as customer_office,
# MAGIC --customer_office as Customer_Office,
# MAGIC Case when carr_office = 'TGT' then 'PHI'
# MAGIC else carr_office end as Carrier_Office,
# MAGIC --carr_office as Carrier_Office,
# MAGIC --'Shipper',
# MAGIC key_c_user,
# MAGIC --carr_office,
# MAGIC --financial_period_sorting,
# MAGIC --financial_year,
# MAGIC mastername,
# MAGIC customer_name,
# MAGIC customer_rep,
# MAGIC status as load_status,
# MAGIC origin_city,
# MAGIC origin_state,
# MAGIC dest_city,
# MAGIC dest_state,
# MAGIC origin_zip as Origin_Zip,
# MAGIC dest_zip as Dest_Zip,
# MAGIC  CONCAT( INITCAP(origin_city),
# MAGIC       ',',
# MAGIC       origin_state,
# MAGIC       '>',
# MAGIC       INITCAP(dest_city),
# MAGIC       ',',
# MAGIC       dest_state
# MAGIC     ) AS load_lane,
# MAGIC market_origin as Market_origin,
# MAGIC market_dest as Market_Deat,
# MAGIC market_lane as Market_Lane,
# MAGIC delivery_date as Delivery_Date,
# MAGIC rolled_date as Delivery_Date,
# MAGIC Booked_Date as Booked_Date,
# MAGIC Tender_Date as Tendered_Date,
# MAGIC bounced_date as bounced_date,
# MAGIC cancelled_date as Cancelled_Date,
# MAGIC Days_ahead as Days_Ahead,
# MAGIC prebookable,
# MAGIC PickUp_Start as Pickup_Date,
# MAGIC PickUp_End as Pickup_EndDate,
# MAGIC Drop_Start as Drop_Start,
# MAGIC Drop_End as Drop_End,
# MAGIC null::date as invoiced_Date,
# MAGIC --load_pickup.Days_ahead as Days_Ahead,
# MAGIC CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC case when carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC dot_num as DOT_Number,
# MAGIC aljex_data.carrier_name as Carrier,
# MAGIC case miles Rlike '^\d+(.\d+)?$' when true then miles::float else 0 end as miles,
# MAGIC (case when cust_curr = 'USD' then invoice_total::float else invoice_total::float / aljex_data.conversion_rate::float end)  as split_rev,
# MAGIC
# MAGIC (case when carrier_curr = 'USD' then final_carrier_rate::float else final_carrier_rate::float / aljex_data.conversion_rate end)  as split_exp,
# MAGIC
# MAGIC (coalesce(case when cust_curr = 'USD' then invoice_total::float else invoice_total::float / aljex_data.conversion_rate::float end, 0) - 
# MAGIC coalesce(case when carrier_curr = 'USD' then final_carrier_rate::float else final_carrier_rate::float / aljex_data.conversion_rate end, 0))  as split_margin,
# MAGIC carrier_lhc  as Linehaul,
# MAGIC carrier_fuel  as Fuel_Surcharge,
# MAGIC carrier_acc  as Other_Fess,
# MAGIC 'ALJEX' as tms
# MAGIC from aljex_data
# MAGIC left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date)
# MAGIC --left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC
# MAGIC --UNION
# MAGIC --
# MAGIC --select distinct -- Carrier
# MAGIC --id::float as relay_ref_num,
# MAGIC --0.5 as Load_Counter,
# MAGIC --use_date,
# MAGIC --modee as Mode,
# MAGIC --mode as Equipment,
# MAGIC --date_range_two.week_num,
# MAGIC --carr_office as office,
# MAGIC --'Carrier',
# MAGIC --key_c_user,
# MAGIC ----carr_office,
# MAGIC ----financial_period_sorting,
# MAGIC ----financial_year,
# MAGIC --mastername,
# MAGIC --customer_name,
# MAGIC --customer_rep,
# MAGIC --status as load_status,
# MAGIC --origin_city,
# MAGIC --origin_state,
# MAGIC --dest_city,
# MAGIC --dest_state,
# MAGIC --origin_zip as Origin_Zip,
# MAGIC --dest_zip as Dest_Zip,
# MAGIC -- CONCAT( INITCAP(origin_city),
# MAGIC --      ',',
# MAGIC --      origin_state,
# MAGIC --      '>',
# MAGIC --      INITCAP(dest_city),
# MAGIC --      ',',
# MAGIC --      dest_state
# MAGIC --    ) AS load_lane,
# MAGIC --market_origin as Market_origin,
# MAGIC --market_dest as Market_Deat,
# MAGIC --market_lane as Market_Lane,
# MAGIC --delivery_date as Delivery_Date,
# MAGIC --rolled_date as Delivery_Date,
# MAGIC --Booked_Date as Booked_Date,
# MAGIC --Tender_Date as Tendered_Date,
# MAGIC --bounced_date as bounced_date,
# MAGIC --cancelled_date as Cancelled_Date,
# MAGIC --Days_ahead as Days_Ahead,
# MAGIC --prebookable,
# MAGIC --PickUp_Start as Pickup_Date,
# MAGIC --PickUp_End as Pickup_EndDate,
# MAGIC --Drop_Start as Drop_Start,
# MAGIC --Drop_End as Drop_End,
# MAGIC --null::date as invoiced_Date,
# MAGIC ----load_pickup.Days_ahead as Days_Ahead,
# MAGIC --CASE
# MAGIC --        WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC --        ELSE 'Same Office'
# MAGIC --    END AS Booking_type,
# MAGIC --case when carrier_name is not null then '1' else '0' end as Load_flag,
# MAGIC --dot_num as DOT_Number,
# MAGIC --aljex_data.carrier_name as Carrier,
# MAGIC --case miles Rlike '^\d+(.\d+)?$' when true then miles::float else 0 end as miles,
# MAGIC --(case when cust_curr = 'USD' then invoice_total::float else invoice_total::float / aljex_data.conversion_rate::float end) / 2 as split_rev,
# MAGIC --
# MAGIC --(case when carrier_curr = 'USD' then final_carrier_rate::float else final_carrier_rate::float / aljex_data.conversion_rate end) / 2 as split_exp,
# MAGIC --
# MAGIC --(coalesce(case when cust_curr = 'USD' then invoice_total::float else invoice_total::float / aljex_data.conversion_rate::float end, 0) - 
# MAGIC --coalesce(case when carrier_curr = 'USD' then final_carrier_rate::float else final_carrier_rate::float / aljex_data.conversion_rate end, 0)) / 2 as split_margin,
# MAGIC --carrier_lhc / 2 as Linehaul,
# MAGIC --carrier_fuel / 2 as Fuel_Surcharge,
# MAGIC --carrier_acc / 2 as Other_Fess,
# MAGIC --'ALJEX' as tms
# MAGIC --from aljex_data
# MAGIC --left join brokerageprod.bronze.date_range_two on use_date::date = date_range_two.date_date::date
# MAGIC ----left join  brokerageprod.bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC
# MAGIC -- ,OneLoad_Details as (
# MAGIC --     customer_master , customer_office Distinct(count(Load_Number))
# MAGIC -- )
# MAGIC
# MAGIC -- ,Final_Code as (
# MAGIC -- Select 
# MAGIC -- Load_Number,
# MAGIC -- Ship_Date,
# MAGIC -- Mode,
# MAGIC -- Equipment,
# MAGIC -- Week_Num,
# MAGIC -- customer_office,
# MAGIC -- Carrier_Office,
# MAGIC -- Booked_By,
# MAGIC -- --customer_master,
# MAGIC -- Case when customer_master in ('TARGET','TARGET', 'TARGET' , 'TARGET' , 'TARGET NON-RETAIL', 'Target Retail',
# MAGIC -- 'TARGET SHIPPING') then 'TARGET LEGACY'
# MAGIC -- when customer_master = "ACCESS BUSINESS GROUP (AMWAY)" Then "AMWAY"
# MAGIC -- when customer_master = "Ekaterra Tea Manufacturing USA, LLC" Then "Ekaterra Tea "
# MAGIC -- when customer_master = "IKEA PURCHASING SERVICES INTER" Then "IKEA "
# MAGIC -- when customer_master = "IKEA US RETAIL LLC" Then "IKEA "
# MAGIC -- when customer_master = "INTERNATIONAL DAIRY & FOOD DISTRIBUTORS LLC" Then "INTERNATIONAL DAIRY & FOOD"
# MAGIC -- when customer_master = "LG Electronics" Then "LG Electronics - Legacy"
# MAGIC -- when customer_master = "Master Builders Solutions Admixtures US LLC" Then "Master Builders Solutions "
# MAGIC -- when customer_master = "Pierce Manufacturing / Oshkosh Corp. c/o Cass Information Systems" Then "Pierce Manufacturing "
# MAGIC -- when customer_master = "WCD LOGISTICS LLC" Then "WCD LOGISTICS "
# MAGIC -- when customer_master = "World Class Distribution" Then "WCD LOGISTICS "
# MAGIC -- else customer_master end as customer_master,
# MAGIC -- customer_name,
# MAGIC -- customer_rep,
# MAGIC -- load_status,
# MAGIC -- Origin_City,
# MAGIC -- Origin_State,
# MAGIC -- Dest_City,
# MAGIC -- Dest_State,
# MAGIC -- Origin_Zip,
# MAGIC -- Dest_Zip,
# MAGIC -- load_lane,
# MAGIC -- Market_origin,
# MAGIC -- Market_Deat,
# MAGIC -- Market_Lane,
# MAGIC -- Delivery_Date,
# MAGIC -- rolled_date,
# MAGIC -- Booked_Date,
# MAGIC -- Tendered_Date,
# MAGIC -- bounced_date,
# MAGIC -- Cancelled_Date,
# MAGIC -- Days_Ahead,
# MAGIC -- prebookable,
# MAGIC -- Pickup_Date,
# MAGIC -- Pickup_EndDate,
# MAGIC -- Drop_Start,
# MAGIC -- Drop_End,
# MAGIC -- invoiced_Date,
# MAGIC -- Load_flag,
# MAGIC -- DOT_Num,
# MAGIC -- Carrier,
# MAGIC -- Miles,
# MAGIC -- Revenue,
# MAGIC -- Expense,
# MAGIC -- Margin,
# MAGIC -- Linehaul,
# MAGIC -- Fuel_Surcharge,
# MAGIC -- Other_Fess,
# MAGIC -- TMS_System
# MAGIC -- From system_union
# MAGIC -- )
# MAGIC
# MAGIC -- ,Final_Shipper_Mapping_Code as(
# MAGIC -- Select *, 
# MAGIC -- Case when FC.customer_office = SOM.Office then SOM.Office else "Different Office" End as Customer_Office_2
# MAGIC --  from Final_Code FC
# MAGIC --   join brokerageprod.analytics.shipper_office_mapping_New SOM on UPPER(FC.customer_master) = SOM.`Shipper _Partner`
# MAGIC -- and  FC.customer_Office = SOM.Office
# MAGIC
# MAGIC -- )
# MAGIC ,final_code as (
# MAGIC SELECT  
# MAGIC Load_id,
# MAGIC Ship_Date,
# MAGIC Mode,
# MAGIC Equipment,
# MAGIC Week_Num,
# MAGIC customer_office,
# MAGIC Carrier_Office,
# MAGIC Booked_By,
# MAGIC Case when customer_master in ('TARGET','TARGET', 'TARGET' , 'TARGET' , 'TARGET NON-RETAIL', 'Target Retail',
# MAGIC 'TARGET SHIPPING') then 'Target'
# MAGIC when lower(customer_master) in ('bek foods llc','cash-wa distributing','cheney brothers','gordon food service','nicholas & company','performance food group',
# MAGIC 'quality custom distribution','raising canes (lineage)','shamrock foods','telos logistics','upper lakes foods, inc.') then 'Raising Canes Chicken'
# MAGIC when customer_master = "ACCESS BUSINESS GROUP (AMWAY)" Then "AMWAY"
# MAGIC when customer_master = "Ekaterra Tea Manufacturing USA, LLC" Then "Ekaterra Tea "
# MAGIC when customer_master = "IKEA PURCHASING SERVICES INTER" Then "IKEA "
# MAGIC when customer_master = "IKEA US RETAIL LLC" Then "IKEA "
# MAGIC when customer_master = "INTERNATIONAL DAIRY & FOOD DISTRIBUTORS LLC" Then "INTERNATIONAL DAIRY & FOOD"
# MAGIC when customer_master = "LG Electronics" Then "LG Electronics - Legacy"
# MAGIC when customer_master = "Master Builders Solutions Admixtures US LLC" Then "Master Builders Solutions "
# MAGIC when customer_master = "Pierce Manufacturing / Oshkosh Corp. c/o Cass Information Systems" Then "Pierce Manufacturing "
# MAGIC when customer_master = "WCD LOGISTICS LLC" Then "WCD LOGISTICS "
# MAGIC when customer_master = "World Class Distribution" Then "WCD LOGISTICS "
# MAGIC when customer_master = 'GE APPLIANCES' then 'GE Appliances'
# MAGIC else customer_master end as customer_master,
# MAGIC customer_name,
# MAGIC Employee,
# MAGIC load_status,
# MAGIC Origin_City,
# MAGIC Origin_State,
# MAGIC Dest_City,
# MAGIC Dest_State,
# MAGIC Origin_Zip,
# MAGIC Dest_Zip,
# MAGIC load_lane,
# MAGIC Market_origin,
# MAGIC Market_Dest,
# MAGIC Market_Lane,
# MAGIC Delivery_Date,
# MAGIC rolled_date,
# MAGIC Booked_Date,
# MAGIC Tendered_Date,
# MAGIC bounced_date,
# MAGIC Cancelled_Date,
# MAGIC Days_Ahead,
# MAGIC prebookable,
# MAGIC Pickup_start,
# MAGIC Pickup_End,
# MAGIC Drop_Start,
# MAGIC Drop_End,
# MAGIC invoiced_Date,
# MAGIC Load_flag,
# MAGIC DOT_Num,
# MAGIC Carrier_name,
# MAGIC Miles,
# MAGIC Revenue,
# MAGIC Expense,
# MAGIC Margin,
# MAGIC Linehaul,
# MAGIC Fuel_Surcharge,
# MAGIC Other_Fess,
# MAGIC SourceSystem_Name
# MAGIC -- From system_union cm
# MAGIC FROM analytics.Fact_Load_GenAI_NonSplit cm)
# MAGIC , Final_mapping as(
# MAGIC select * from final_code cm
# MAGIC JOIN analytics.shipper_office_mapping_New sp
# MAGIC on cm.customer_master = sp.`Shippe _Partner`
# MAGIC where UPPER(cm.customer_master) = UPPER(sp.`Shippe _Partner`) and cm.Customer_Office=sp.Office)
# MAGIC
# MAGIC
# MAGIC select *
# MAGIC from Final_mapping  
# MAGIC -- where Ship_Date >= '2024-10-27' and Ship_Date <= '2024-11-09' 
# MAGIC -- group by customer_master,customer_office
# MAGIC
# MAGIC -- Select customer_master , customer_Office_2 , count(distinct Load_Number) as Load_Count , sum(Margin) as Margin, sum(Revenue) as from Final_Shipper_Mapping_Code
# MAGIC -- where Ship_Date >= '2024-08-25' and Ship_Date <= '2024-09-28' and customer_office_2 <> 'Different Office' group by customer_master , customer_Office_2
# MAGIC
# MAGIC -- --   Select distinct(Mode) from brokerageprod.analytics.fact_load_genai_nonsplit
# MAGIC -- --   where Ship_Date >= '2024-08-25' and Ship_Date <= '2024-09-28'
# MAGIC -- Select * from brokerageprod.analytics.shipper_office_mapping
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Genai_Shipper_Trend_Temp;
# MAGIC INSERT INTO analytics.Genai_Shipper_Trend_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Genai_Shipper_Trend
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.Genai_Shipper_Trend;
# MAGIC INSERT INTO analytics.Genai_Shipper_Trend
# MAGIC SELECT *
# MAGIC FROM analytics.Genai_Shipper_Trend_Temp