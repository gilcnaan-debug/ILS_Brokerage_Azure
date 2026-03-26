# Databricks notebook source
# MAGIC %md
# MAGIC ## Reporting Views
# MAGIC * **Description:** To bulid a code that extracts req data from source for Bandwidth Report 
# MAGIC * **Created Date:** 06/25/2025
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 07/15/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating View

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view bronze.reporting_split_fact_load_New as (
# MAGIC   with combined_loads_mine as (
# MAGIC     select
# MAGIC       distinct relay_reference_number_one,
# MAGIC       relay_reference_number_two,
# MAGIC       resulting_plan_id,
# MAGIC       canonical_plan_projection.relay_reference_number as combine_new_rrn,
# MAGIC       canonical_plan_projection.mode,
# MAGIC       b.tender_on_behalf_of_id,
# MAGIC       'COMBINED' as combined
# MAGIC     from
# MAGIC       bronze.plan_combination_projection
# MAGIC       join bronze.canonical_plan_projection on resulting_plan_id = canonical_plan_projection.plan_id
# MAGIC       left join bronze.booking_projection b on canonical_plan_projection.relay_reference_number = b.relay_reference_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and is_combined = 'true'
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
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as pu_appt_date,
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
# MAGIC         when master_customer_name is null then r.customer_name
# MAGIC         else master_customer_name
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
# MAGIC       booked_at as Booked_Date,
# MAGIC       ta.accepted_at :: string as Tendering_Date,
# MAGIC       b.bounced_at :: date as bounced_date,
# MAGIC       b.bounced_at :: date + 2 as cancelled_date,
# MAGIC       pickup.Days_ahead as Days_Ahead,
# MAGIC       pickup.prebookable as prebookable,
# MAGIC       papp.PickUp_Start,
# MAGIC       papp.PickUp_End,
# MAGIC       dapp.Drop_Start,
# MAGIC       dapp.Drop_End,
# MAGIC       b.booked_carrier_name as carrier_name
# MAGIC     from
# MAGIC       bronze.booking_projection b
# MAGIC       LEFT join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC       JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office ----- Need to change to new_office_lookup_w_tgt later
# MAGIC       LEFT JOIN bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip
# MAGIC       LEFT join bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC       AND relay_users.`active?` = 'true'
# MAGIC       LEFT join bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
# MAGIC       LEFT join bronze.pickup_projection pp on (
# MAGIC         b.booking_id = pp.booking_id
# MAGIC         AND b.first_shipper_name = pp.shipper_name
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join bronze.planning_stop_schedule pss on (
# MAGIC         b.relay_reference_number = pss.relay_reference_number
# MAGIC         AND b.first_shipper_name = pss.stop_name
# MAGIC         AND pss.sequence_number = 1
# MAGIC         AND pss.`removed?` = 'false'
# MAGIC       )
# MAGIC       LEFT join bronze.canonical_stop c on (
# MAGIC         pss.stop_id = c.stop_id
# MAGIC         and c.`stale?` = 'false'
# MAGIC         and c.stop_type = 'pickup'
# MAGIC       )
# MAGIC       left join bronze.canonical_stop a on b.booking_id = a.booking_id
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
# MAGIC           bronze.load_pickup
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
# MAGIC           bronze.load_appointment
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
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON b.relay_reference_number = dapp.loadnum --left join bounce_data on b.relay_reference_number = bounce_data.relay_reference_number
# MAGIC       --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       --JOIN bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC       left join combined_loads_mine on b.relay_reference_number = combined_loads_mine.combine_new_rrn
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join bronze.tendering_planned_distance on b.relay_reference_number = tendering_planned_distance.relay_reference_number
# MAGIC       left join bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC     where
# MAGIC       1 = 1 --[[and {{date_range}}]]
# MAGIC       and b.status = 'booked'
# MAGIC       and canonical_plan_projection.mode not like '%ltl%'
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and combined is null
# MAGIC     UNION
# MAGIC     select
# MAGIC       distinct canonical_plan_projection.mode as Mode_Filter,
# MAGIC       case
# MAGIC         when canonical_plan_projection.mode = 'prtl' then 'PORTD'
# MAGIC         else case
# MAGIC           when canonical_plan_projection.mode = 'imdl' then 'IMDL'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC       end as Equipment,
# MAGIC       'TL' as modee,
# MAGIC       combined_loads_mine.relay_reference_number_one as relay_load,
# MAGIC       CAST(combined_loads_mine.combine_new_rrn As string) as combo_load,
# MAGIC       b.booking_id,
# MAGIC       b.booked_by_name as Carrier_Rep,
# MAGIC       carr.new_office as carr_office,
# MAGIC       cust.new_office as customer_office,
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as pu_appt_date,
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
# MAGIC         when master_customer_name is null then r.customer_name
# MAGIC         else master_customer_name
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
# MAGIC       pickup.Days_ahead as Days_Ahead,
# MAGIC       pickup.prebookable as prebookable,
# MAGIC       papp.PickUp_Start,
# MAGIC       papp.PickUp_End,
# MAGIC       dapp.Drop_Start,
# MAGIC       dapp.Drop_End,
# MAGIC       b.booked_carrier_name
# MAGIC     from
# MAGIC       combined_loads_mine
# MAGIC       JOIN bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC       LEFT join bronze.pickup_projection pp on (
# MAGIC         b.booking_id = pp.booking_id
# MAGIC         AND b.first_shipper_name = pp.shipper_name
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join bronze.planning_stop_schedule pss on (
# MAGIC         b.relay_reference_number = pss.relay_reference_number
# MAGIC         AND b.first_shipper_name = pss.stop_name
# MAGIC         AND pss.sequence_number = 1
# MAGIC         AND pss.`removed?` = 'false'
# MAGIC       )
# MAGIC       LEFT join bronze.canonical_stop c on (
# MAGIC         b.booking_id = c.booking_id
# MAGIC         AND b.first_shipper_id = c.facility_id
# MAGIC         and c.`stale?` = 'false'
# MAGIC         and c.stop_type = 'pickup'
# MAGIC       )
# MAGIC       left join bronze.canonical_stop a on b.booking_id = a.booking_id
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
# MAGIC           bronze.load_pickup
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
# MAGIC           bronze.load_appointment
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
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON b.relay_reference_number = dapp.loadnum --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       --LEFT JOIN analytics.dim_financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = dim_financial_calendar.date::date
# MAGIC       JOIN bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC       LEFT JOIN bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_one = tendering_planned_distance.relay_reference_number
# MAGIC       left join bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC       LEFT join bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC       AND relay_users.`active?` = 'true'
# MAGIC       LEFT join bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC     where
# MAGIC       1 = 1 --[[and {{date_range}}]]
# MAGIC       and b.status = 'booked'
# MAGIC       and canonical_plan_projection.mode not like '%ltl%'
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC     UNION
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
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as pu_appt_date,
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
# MAGIC         when master_customer_name is null then r.customer_name
# MAGIC         else master_customer_name
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
# MAGIC       pickup.Days_ahead as Days_Ahead,
# MAGIC       pickup.prebookable as prebookable,
# MAGIC       papp.PickUp_Start,
# MAGIC       papp.PickUp_End,
# MAGIC       dapp.Drop_Start,
# MAGIC       dapp.Drop_End,
# MAGIC       b.booked_carrier_name
# MAGIC     from
# MAGIC       combined_loads_mine
# MAGIC       JOIN bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC       LEFT join bronze.pickup_projection pp on (
# MAGIC         b.booking_id = pp.booking_id
# MAGIC         AND b.first_shipper_name = pp.shipper_name
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join bronze.planning_stop_schedule pss on (
# MAGIC         b.relay_reference_number = pss.relay_reference_number
# MAGIC         AND b.first_shipper_name = pss.stop_name
# MAGIC         AND pss.sequence_number = 1
# MAGIC         AND pss.`removed?` = 'false'
# MAGIC       )
# MAGIC       LEFT join bronze.canonical_stop c on (
# MAGIC         b.booking_id = c.booking_id
# MAGIC         AND b.first_shipper_id = c.facility_id
# MAGIC         and c.`stale?` = 'false'
# MAGIC         and c.stop_type = 'pickup'
# MAGIC       )
# MAGIC       left join bronze.canonical_stop a on b.booking_id = a.booking_id
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
# MAGIC           bronze.load_pickup
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
# MAGIC           bronze.load_appointment
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
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON b.relay_reference_number = dapp.loadnum --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       --JOIN bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC       JOIN bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_two = tendering_planned_distance.relay_reference_number
# MAGIC       left join bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC       LEFT join bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC       AND relay_users.`active?` = 'true'
# MAGIC       LEFT join bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
# MAGIC     where
# MAGIC       1 = 1 --[[and {{date_range}}]]
# MAGIC       and b.status = 'booked'
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and canonical_plan_projection.mode not like '%ltl%'
# MAGIC     UNION
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
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as planning_stop_sched,
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
# MAGIC         when master_customer_name is null then r.customer_name
# MAGIC         else master_customer_name
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
# MAGIC       pickup.Days_ahead as Days_Ahead,
# MAGIC       pickup.prebookable as prebookable,
# MAGIC       papp.PickUp_Start,
# MAGIC       papp.PickUp_End,
# MAGIC       dapp.Drop_Start,
# MAGIC       dapp.Drop_End,
# MAGIC       e.carrier_name
# MAGIC     from
# MAGIC       bronze.big_export_projection e
# MAGIC       LEFT join bronze.customer_profile_projection r on e.customer_id = r.customer_slug
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       LEFT JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       JOIN bronze.canonical_plan_projection on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT join bronze.pickup_projection pp on (
# MAGIC         e.load_number = pp.relay_reference_number
# MAGIC         AND e.pickup_name = pp.shipper_name
# MAGIC         AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS?
# MAGIC         AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join bronze.planning_stop_schedule pss on (
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
# MAGIC           bronze.load_pickup
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
# MAGIC           bronze.load_appointment
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
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON e.load_number = dapp.loadnum --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       LEFT join bronze.canonical_stop c on pss.stop_id = c.stop_id --JOIN bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC       left join bronze.projection_carrier on e.carrier_id = projection_carrier.id
# MAGIC     where
# MAGIC       1 = 1 --[[and {{date_range}}]]
# MAGIC       and e.dispatch_status != 'Cancelled'
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
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as planning_stop_sched,
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
# MAGIC         when master_customer_name is null then r.customer_name
# MAGIC         else master_customer_name
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
# MAGIC       pickup.Days_ahead as Days_Ahead,
# MAGIC       pickup.prebookable as prebookable,
# MAGIC       papp.PickUp_Start,
# MAGIC       papp.PickUp_End,
# MAGIC       dapp.Drop_Start,
# MAGIC       dapp.Drop_End,
# MAGIC       e.carrier_name
# MAGIC     from
# MAGIC       bronze.big_export_projection e
# MAGIC       LEFT join bronze.customer_profile_projection r on e.customer_id = r.customer_slug
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       LEFT JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       JOIN bronze.canonical_plan_projection on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT join bronze.pickup_projection pp on (
# MAGIC         e.load_number = pp.relay_reference_number
# MAGIC         AND e.pickup_name = pp.shipper_name
# MAGIC         AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS?
# MAGIC         AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC         AND pp.sequence_number = 1
# MAGIC       )
# MAGIC       LEFT join bronze.planning_stop_schedule pss on (
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
# MAGIC           bronze.load_pickup
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
# MAGIC           bronze.load_appointment
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
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON e.load_number = dapp.loadnum --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       LEFT join bronze.canonical_stop c on pss.stop_id = c.stop_id --JOIN bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC       left join bronze.projection_carrier on e.carrier_id = projection_carrier.id
# MAGIC     where
# MAGIC       1 = 1 --[[and {{date_range}}]]
# MAGIC       and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and canonical_plan_projection.mode = 'ltl'
# MAGIC       and e.customer_id in ('roland', 'hain', 'deb')
# MAGIC   ),
# MAGIC   to_get_avg as (
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
# MAGIC       to_get_avg
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
# MAGIC       bronze.vendor_transaction_projection
# MAGIC       JOIN use_relay_loads on relay_reference_number = use_relay_loads.relay_load
# MAGIC       left join bronze.canada_conversions on vendor_transaction_projection.incurred_at :: date = canada_conversions.ship_date :: date
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
# MAGIC       bronze.vendor_transaction_projection
# MAGIC       JOIN use_relay_loads on relay_reference_number :: string = use_relay_loads.combined_load :: string
# MAGIC       left join bronze.canada_conversions on vendor_transaction_projection.incurred_at :: date = canada_conversions.ship_date :: date
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
# MAGIC       bronze.invoicing_credits
# MAGIC       JOIN use_relay_loads on relay_reference_number = use_relay_loads.relay_load
# MAGIC       left join bronze.canada_conversions on invoicing_credits.credited_at :: date = canada_conversions.ship_date :: date
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
# MAGIC       bronze.moneying_billing_party_transaction m
# MAGIC       JOIN use_relay_loads on m.relay_reference_number = use_relay_loads.relay_load
# MAGIC       left join bronze.canada_conversions on m.incurred_at :: date = canada_conversions.ship_date :: date
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
# MAGIC       bronze.moneying_billing_party_transaction
# MAGIC       left join bronze.invoicing_credits on moneying_billing_party_transaction.relay_reference_number :: float = invoicing_credits.relay_reference_number :: float
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
# MAGIC       bronze.moneying_billing_party_transaction
# MAGIC       left join new_customer_money on moneying_billing_party_transaction.relay_reference_number = new_customer_money.loadnum
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and "voided?" = 'false'
# MAGIC     group by
# MAGIC       relay_reference_number,
# MAGIC       new_customer_money.inv_cred_amt
# MAGIC   ) -------------------- Adding the Linehaul, Fuelsurcharge and Other expoense CTE ----------------------------------------------
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
# MAGIC       bronze.vendor_transaction_projection
# MAGIC       left join bronze.canada_conversions on vendor_transaction_projection.incurred_at :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and "voided?" = 'false'
# MAGIC     group by
# MAGIC       relay_reference_number,
# MAGIC       currency,
# MAGIC       us_to_cad,
# MAGIC       avg_cad_to_us,
# MAGIC       incurred_at
# MAGIC   ) ----------------- Customer Linehaul, FuelSurcharge, Other Expense CTE ------------------------
# MAGIC ,
# MAGIC   new_customer_money_two as (
# MAGIC     select
# MAGIC       distinct incurred_at,
# MAGIC       `voided?`,
# MAGIC       moneying_billing_party_transaction.currency as cust_currency_type,
# MAGIC       moneying_billing_party_transaction.relay_reference_number as loadnum,
# MAGIC       charge_code,
# MAGIC       amount :: float / 100 as amount
# MAGIC     from
# MAGIC       bronze.moneying_billing_party_transaction
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
# MAGIC       bronze.invoicing_credits
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
# MAGIC   ) --------------------------- Invoice Bucket Data Relay ----------------------------------------------
# MAGIC ,
# MAGIC   new_pu_appt as (
# MAGIC     SELECT
# MAGIC       DISTINCT b.relay_reference_number,
# MAGIC       max(
# MAGIC         COALESCE(
# MAGIC           s.appointment_datetime :: TIMESTAMP,
# MAGIC           s.window_start_datetime :: TIMESTAMP,
# MAGIC           s.window_end_datetime :: TIMESTAMP,
# MAGIC           p.appointment_datetime :: TIMESTAMP,
# MAGIC           b.ready_date :: date
# MAGIC         )
# MAGIC       ) AS new_pu_appt
# MAGIC     FROM
# MAGIC       bronze.booking_projection b
# MAGIC       left join bronze.customer_profile_projection on b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC       LEFT JOIN bronze.pickup_projection p ON b.relay_reference_number = p.relay_reference_number
# MAGIC       AND b.first_shipper_name = p.shipper_name
# MAGIC       AND p.sequence_number = 1
# MAGIC       LEFT JOIN bronze.planning_stop_schedule s ON b.relay_reference_number = s.relay_reference_number
# MAGIC       AND s.stop_type = 'pickup'
# MAGIC       AND s.sequence_number = 1
# MAGIC       AND `removed?` = 'false'
# MAGIC     WHERE
# MAGIC       1 = 1
# MAGIC       AND b.status NOT IN ('cancelled', 'bounced') ---[[and {{customer}}]]
# MAGIC     GROUP BY
# MAGIC       b.relay_reference_number
# MAGIC   ),
# MAGIC   lawson_transactions as (
# MAGIC     select
# MAGIC       distinct invoice,
# MAGIC       max(deposit_date) as last_deposit,
# MAGIC       sum(transaction_amount) as total_transactions
# MAGIC     from
# MAGIC       bronze.ar_lawson_arpmt
# MAGIC     group by
# MAGIC       invoice
# MAGIC   ),
# MAGIC   tl_invoice_start as (
# MAGIC     -- need to sum all this together
# MAGIC     select
# MAGIC       distinct invoice_number,
# MAGIC       tl.relay_reference_number,
# MAGIC       invoiced_at :: date,
# MAGIC       case
# MAGIC         when currency = 'CAD' then (
# MAGIC           (
# MAGIC             accessorial_amount :: float + fuel_surcharge_amount :: float + linehaul_amount :: float
# MAGIC           ) / 100.00
# MAGIC         ) * (
# MAGIC           case
# MAGIC             when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC             else canada_conversions.us_to_cad
# MAGIC           end
# MAGIC         ) :: float
# MAGIC         ELSE (
# MAGIC           (
# MAGIC             accessorial_amount :: float + fuel_surcharge_amount :: float + linehaul_amount :: float
# MAGIC           ) / 100.00
# MAGIC         )
# MAGIC       end as invoice_amt,
# MAGIC       currency,
# MAGIC       customer_profile_projection.customer_slug,
# MAGIC       lawson_transactions.total_transactions as already_paid_amt,
# MAGIC       lawson_transactions.last_deposit
# MAGIC     from
# MAGIC       bronze.tl_invoice_projection tl
# MAGIC       left join bronze.tender_reference_numbers_projection on tl.relay_reference_number = tender_reference_numbers_projection.relay_reference_number
# MAGIC       left join bronze.customer_profile_projection on tender_reference_numbers_projection.tender_on_behalf_of = customer_profile_projection.customer_slug
# MAGIC       left join new_pu_appt on tl.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC       left join bronze.canada_conversions on new_pu_appt :: date = canada_conversions.ship_date :: date
# MAGIC       left join lawson_transactions on invoice_number = lawson_transactions.invoice
# MAGIC       left join average_conversions on 1 = 1
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and do_not_invoice != 'true'
# MAGIC   ) ---[[and {{customer}}]])
# MAGIC ,
# MAGIC   ltl_invoice_start as (
# MAGIC     select
# MAGIC       distinct ltl_invoice_projection.relay_reference_number,
# MAGIC       ltl_invoice_projection.invoice_number,
# MAGIC       total_transactions,
# MAGIC       last_deposit,
# MAGIC       tender_reference_numbers_projection.tender_on_behalf_of,
# MAGIC       invoiced_at,
# MAGIC       replace(replace(invoiced_amount, ',', ''), '.', '') :: float / 100 as invoiced_amount,
# MAGIC       rank() over (
# MAGIC         partition by ltl_invoice_projection.relay_reference_number
# MAGIC         order by
# MAGIC           invoiced_at :: date desc
# MAGIC       ) as ranker
# MAGIC     from
# MAGIC       bronze.ltl_invoice_projection
# MAGIC       left join bronze.tender_reference_numbers_projection on ltl_invoice_projection.relay_reference_number = tender_reference_numbers_projection.relay_reference_number
# MAGIC       left join bronze.customer_profile_projection on case
# MAGIC         when tender_on_behalf_of = 'hain' then 'hain_celestial'
# MAGIC         else tender_reference_numbers_projection.tender_on_behalf_of
# MAGIC       end = customer_profile_projection.customer_slug
# MAGIC       left join lawson_transactions on ltl_invoice_projection.invoice_number = lawson_transactions.invoice
# MAGIC     where
# MAGIC       1 = 1 --[[and {{customer}}]]
# MAGIC     order by
# MAGIC       ltl_invoice_projection.relay_reference_number
# MAGIC   ),
# MAGIC   invoiced_at_and_amt_final as (
# MAGIC     select
# MAGIC       distinct relay_reference_number as loadnum,
# MAGIC       invoiced_at :: date,
# MAGIC       invoiced_amount,
# MAGIC       tender_on_behalf_of,
# MAGIC       total_transactions :: float,
# MAGIC       last_deposit
# MAGIC     from
# MAGIC       ltl_invoice_start
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and ranker = 1
# MAGIC     union
# MAGIC     select
# MAGIC       distinct relay_reference_number,
# MAGIC       min(invoiced_at :: date),
# MAGIC       sum(invoice_amt),
# MAGIC       customer_slug,
# MAGIC       sum(already_paid_amt),
# MAGIC       max(last_deposit)
# MAGIC     from
# MAGIC       tl_invoice_start
# MAGIC     group by
# MAGIC       relay_reference_number,
# MAGIC       customer_slug
# MAGIC   ) ----------------------- Aljex Data -----------------------------------------------
# MAGIC ,
# MAGIC   aljex_data as (
# MAGIC     select
# MAGIC       p1.id,
# MAGIC       case
# MAGIC         when aljex_user_report_listing.full_name is null then key_c_user
# MAGIC         else aljex_user_report_listing.full_name
# MAGIC       end as key_c_user,
# MAGIC       case
# MAGIC         when master_customer_name is null then p2.shipper
# MAGIC         else master_customer_name
# MAGIC       end as mastername,
# MAGIC       p2.shipper as customer_name,
# MAGIC       case
# MAGIC         when z.full_name is null then p2.srv_rep
# MAGIC         else z.full_name
# MAGIC       end as customer_rep,
# MAGIC       p1.pickup_date :: date as use_date,
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
# MAGIC         when p1.office not in ('10', '34', '51', '54', '61', '62', '63', '64', '74') then 'USD'
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
# MAGIC           when master_customer_name = 'ALTMAN PLANTS' then 'LAX'
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
# MAGIC       papp.PickUp_Start,
# MAGIC       papp.PickUp_End,
# MAGIC       dapp.Drop_Start,
# MAGIC       dapp.Drop_End,
# MAGIC       c.name as carrier_name
# MAGIC     from
# MAGIC       bronze.projection_load_1 p1
# MAGIC       Left Join bronze.projection_load_2 p2 on p1.id = p2.id ---join analytics.financial_calendar on p.pickup_date::date = financial_calendar.date::date
# MAGIC       join bronze.new_office_lookup_w_tgt on p1.office = new_office_lookup_w_tgt.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o ON LEFT(p1.pickup_zip_code, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(p1.consignee_zip_code, 3) = d.pickup_zip
# MAGIC       left join bronze.cad_currency on p1.id :: string = cad_currency.pro_number :: string
# MAGIC       left join bronze.aljex_customer_profiles on p2.customer_id :: string = aljex_customer_profiles.cust_id :: string
# MAGIC       left join bronze.projection_carrier c on p1.carrier_id = c.id
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           system,
# MAGIC           max(load_number) as max_loadnumber,
# MAGIC           max(Pickup_date) as Pickup_date,
# MAGIC           max(prebookable) as prebookable,
# MAGIC           Max(days_ahead) as days_ahead
# MAGIC         from
# MAGIC           bronze.load_pickup
# MAGIC         group by
# MAGIC           system
# MAGIC       ) as pickup ON p1.id = pickup.max_loadnumber
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp'
# MAGIC       ) as papp ON p1.id = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(
# MAGIC             try_cast(pickup_proj_appt_datetime as TIMESTAMP)
# MAGIC           ) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp ON p1.id = dapp.loadnum
# MAGIC       left join bronze.canada_carriers on p1.carrier_id :: string = canada_carriers.canada_dot :: string
# MAGIC       left join bronze.customer_lookup on left(shipper, 32) = left(customer_lookup.aljex_customer_name, 32)
# MAGIC       left join bronze.aljex_mode_types on p1.equipment = bronze.aljex_mode_types.equipment_type
# MAGIC       join average_conversions on 1 = 1
# MAGIC       left join bronze.canada_conversions on p1.pickup_date :: date = canada_conversions.ship_date :: date
# MAGIC       left join bronze.aljex_user_report_listing on p1.key_c_user = aljex_user_report_listing.aljex_id
# MAGIC       left join bronze.aljex_user_report_listing z on p2.srv_rep = z.aljex_id
# MAGIC       left join bronze.relay_users on aljex_user_report_listing.full_name = relay_users.full_name
# MAGIC       left join bronze.new_office_lookup_w_tgt car on coalesce(
# MAGIC         aljex_user_report_listing.pnl_code,
# MAGIC         relay_users.office_id
# MAGIC       ) = car.old_office
# MAGIC     where
# MAGIC       1 = 1 --[[and {{date_range}}]]
# MAGIC       and p2.status not in ('OPEN', 'ASSIGNED')
# MAGIC       and p2.status not like '%VOID%'
# MAGIC       and p1.key_c_user is not null
# MAGIC   ) --------------------------------- Final CTE -------------------------------
# MAGIC ,
# MAGIC   system_union as (
# MAGIC     select
# MAGIC       distinct ----- Shipper
# MAGIC       relay_load as Load_Number,
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date as Ship_Date,
# MAGIC       Case
# MAGIC         when modee in ('POWER ONLY', 'IMDL', 'PORTD') then null
# MAGIC         else modee
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else Equipment
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num as Week_Num,
# MAGIC       customer_office as Office,
# MAGIC       'Shipper' as Office_Type,
# MAGIC       Carrier_Rep as Booked_By,
# MAGIC       --carr_office as Carr_Office,
# MAGIC       --financial_period_sorting as "Financial Period",
# MAGIC       --financial_year as "Financial Yr",
# MAGIC       mastername as customer_master,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
# MAGIC       origin_city as Origin_City,
# MAGIC       origin_state as Origin_State,
# MAGIC       dest_city as Dest_City,
# MAGIC       dest_state as Dest_State,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       del_date as Delivery_Date,
# MAGIC       rolled_at as rolled_date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tendering_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       --financial_period_sorting as period,
# MAGIC       invoiced_at_and_amt_final.invoiced_at,
# MAGIC       invoiced_at_and_amt_final.invoiced_amount,
# MAGIC       invoiced_at_and_amt_final.total_transactions,
# MAGIC       invoiced_at_and_amt_final.last_deposit,
# MAGIC       case
# MAGIC         when invoiced_at_and_amt_final.invoiced_amount is null then 0
# MAGIC         else invoiced_at_and_amt_final.invoiced_amount :: float
# MAGIC       end - case
# MAGIC         when invoiced_at_and_amt_final.total_transactions is null then 0
# MAGIC         else total_transactions :: float
# MAGIC       end as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Num,
# MAGIC       carrier_name as Carrier,
# MAGIC       total_miles :: float as Miles,
# MAGIC       total_cust_rate.total_cust_rate :: float / 2 as Revenue,
# MAGIC       total_carrier_rate.total_carrier_rate :: float / 2 as Expense,
# MAGIC       (
# MAGIC         coalesce(total_cust_rate.total_cust_rate, 0) :: float - coalesce(total_carrier_rate.total_carrier_rate, 0) :: float
# MAGIC       ) / 2 as Margin,
# MAGIC       --carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC       --carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC       --carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_lhc,
# MAGIC       'RELAY' as TMS_System
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number
# MAGIC       left join total_carrier_rate on use_relay_loads.relay_load = total_carrier_rate.relay_reference_number
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_at_and_amt_final on use_relay_loads.relay_load :: float = invoiced_at_and_amt_final.loadnum :: float
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1 --left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC       --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC       and combined_load = 'NO' --Select * from system_union limit 100
# MAGIC     Union
# MAGIC     select
# MAGIC       distinct ----- Carrier
# MAGIC       relay_load as Load_Number,
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date as Ship_Date,
# MAGIC       Case
# MAGIC         when modee in ('POWER ONLY', 'IMDL', 'PORTD') then null
# MAGIC         else modee
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else Equipment
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num as Week_Num,
# MAGIC       carr_office as Office,
# MAGIC       'Carrier' as Office_Type,
# MAGIC       Carrier_Rep as Booked_By,
# MAGIC       --carr_office as Carr_Office,
# MAGIC       --financial_period_sorting as "Financial Period",
# MAGIC       --financial_year as "Financial Yr",
# MAGIC       mastername as customer_master,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
# MAGIC       origin_city as Origin_City,
# MAGIC       origin_state as Origin_State,
# MAGIC       dest_city as Dest_City,
# MAGIC       dest_state as Dest_State,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       del_date as Delivery_Date,
# MAGIC       rolled_at as rolled_date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tendering_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       invoiced_at_and_amt_final.invoiced_at,
# MAGIC       invoiced_at_and_amt_final.invoiced_amount,
# MAGIC       invoiced_at_and_amt_final.total_transactions,
# MAGIC       invoiced_at_and_amt_final.last_deposit,
# MAGIC       case
# MAGIC         when invoiced_at_and_amt_final.invoiced_amount is null then 0
# MAGIC         else invoiced_at_and_amt_final.invoiced_amount :: float
# MAGIC       end - case
# MAGIC         when invoiced_at_and_amt_final.total_transactions is null then 0
# MAGIC         else total_transactions :: float
# MAGIC       end as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Num,
# MAGIC       carrier_name as Carrier,
# MAGIC       total_miles :: float as Miles,
# MAGIC       total_cust_rate.total_cust_rate :: float / 2 as Revenue,
# MAGIC       total_carrier_rate.total_carrier_rate :: float / 2 as Expense,
# MAGIC       (
# MAGIC         coalesce(total_cust_rate.total_cust_rate, 0) :: float - coalesce(total_carrier_rate.total_carrier_rate, 0) :: float
# MAGIC       ) / 2 as Margin,
# MAGIC       --carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC       --carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC       --carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_lhc,
# MAGIC       'RELAY' as TMS_System
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number
# MAGIC       left join total_carrier_rate on use_relay_loads.relay_load = total_carrier_rate.relay_reference_number
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_at_and_amt_final on use_relay_loads.relay_load :: float = invoiced_at_and_amt_final.loadnum :: float
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1 --left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC       --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC       and combined_load = 'NO'
# MAGIC     Union
# MAGIC     select
# MAGIC       distinct ---- Shipper - Hain Roloadn and Deb
# MAGIC       relay_load as relay_ref_num,
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       Case
# MAGIC         when modee in ('POWER ONLY', 'IMDL', 'PORTD') then null
# MAGIC         else modee
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else Equipment
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num,
# MAGIC       customer_office as office,
# MAGIC       'Shipper',
# MAGIC       Carrier_Rep,
# MAGIC       --carr_office,
# MAGIC       ---financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       mastername,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       del_date as Delivery_Date,
# MAGIC       rolled_at as rolled_date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tendering_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       invoiced_at_and_amt_final.invoiced_at,
# MAGIC       invoiced_at_and_amt_final.invoiced_amount,
# MAGIC       invoiced_at_and_amt_final.total_transactions,
# MAGIC       invoiced_at_and_amt_final.last_deposit,
# MAGIC       case
# MAGIC         when invoiced_at_and_amt_final.invoiced_amount is null then 0
# MAGIC         else invoiced_at_and_amt_final.invoiced_amount :: float
# MAGIC       end - case
# MAGIC         when invoiced_at_and_amt_final.total_transactions is null then 0
# MAGIC         else total_transactions :: float
# MAGIC       end as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when e.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Number,
# MAGIC       e.carrier_name as Carrier,
# MAGIC       total_miles :: float,
# MAGIC       total_cust_rate.total_cust_rate :: float / 2 as SplitRev,
# MAGIC       (projected_expense :: float / 100.0) / 2 as splitexp,
# MAGIC       (
# MAGIC         coalesce(total_cust_rate.total_cust_rate, 0) :: float - coalesce(projected_expense :: float / 100.0, 0) :: float
# MAGIC       ) / 2 as splitmargin,
# MAGIC       ---carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC       ---carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC       ----carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_lhc,
# MAGIC       'RELAY' as tms
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join bronze.big_export_projection e on use_relay_loads.relay_load :: float = e.load_number :: float
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load :: float = total_cust_rate.relay_reference_number :: float
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_at_and_amt_final on use_relay_loads.relay_load :: float = invoiced_at_and_amt_final.loadnum :: float
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1 ---left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC       --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter = 'ltl'
# MAGIC       and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and e.customer_id not in ('hain', 'roland', 'deb')
# MAGIC     union
# MAGIC     select
# MAGIC       distinct ---- Carrier - Hain Roloadn and Deb
# MAGIC       relay_load as relay_ref_num,
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       Case
# MAGIC         when modee in ('POWER ONLY', 'IMDL', 'PORTD') then null
# MAGIC         else modee
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else Equipment
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num,
# MAGIC       carr_office as office,
# MAGIC       'Carrier',
# MAGIC       Carrier_Rep,
# MAGIC       --carr_office,
# MAGIC       ---financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       mastername,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       del_date as Delivery_Date,
# MAGIC       rolled_at as rolled_date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tendering_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       invoiced_at_and_amt_final.invoiced_at,
# MAGIC       invoiced_at_and_amt_final.invoiced_amount,
# MAGIC       invoiced_at_and_amt_final.total_transactions,
# MAGIC       invoiced_at_and_amt_final.last_deposit,
# MAGIC       case
# MAGIC         when invoiced_at_and_amt_final.invoiced_amount is null then 0
# MAGIC         else invoiced_at_and_amt_final.invoiced_amount :: float
# MAGIC       end - case
# MAGIC         when invoiced_at_and_amt_final.total_transactions is null then 0
# MAGIC         else total_transactions :: float
# MAGIC       end as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,invoiced_at_and_amt_final.invoiced_at,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when e.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Number,
# MAGIC       e.carrier_name as Carrier,
# MAGIC       total_miles :: float,
# MAGIC       total_cust_rate.total_cust_rate :: float / 2 as SplitRev,
# MAGIC       (projected_expense :: float / 100.0) / 2 as splitexp,
# MAGIC       (
# MAGIC         coalesce(total_cust_rate.total_cust_rate, 0) :: float - coalesce(projected_expense :: float / 100.0, 0) :: float
# MAGIC       ) / 2 as splitmargin,
# MAGIC       ---carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC       ---carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC       ----carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_lhc,
# MAGIC       'RELAY' as tms
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join bronze.big_export_projection e on use_relay_loads.relay_load :: float = e.load_number :: float
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load :: float = total_cust_rate.relay_reference_number :: float
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_at_and_amt_final on use_relay_loads.relay_load :: float = invoiced_at_and_amt_final.loadnum :: float
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1 ---left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC       --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
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
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       Case
# MAGIC         when modee in ('POWER ONLY', 'IMDL', 'PORTD') then null
# MAGIC         else modee
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else Equipment
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num,
# MAGIC       customer_office as office,
# MAGIC       'Shipper',
# MAGIC       Carrier_Rep,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       mastername,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       del_date as Delivery_Date,
# MAGIC       rolled_at as rolled_date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tendering_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       invoiced_at_and_amt_final.invoiced_at,
# MAGIC       invoiced_at_and_amt_final.invoiced_amount,
# MAGIC       invoiced_at_and_amt_final.total_transactions,
# MAGIC       invoiced_at_and_amt_final.last_deposit,
# MAGIC       case
# MAGIC         when invoiced_at_and_amt_final.invoiced_amount is null then 0
# MAGIC         else invoiced_at_and_amt_final.invoiced_amount :: float
# MAGIC       end - case
# MAGIC         when invoiced_at_and_amt_final.total_transactions is null then 0
# MAGIC         else total_transactions :: float
# MAGIC       end as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when e.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Number,
# MAGIC       e.carrier_name as Carrier,
# MAGIC       total_miles :: float,
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
# MAGIC       ) :: float / 2 as split_rev,
# MAGIC       (projected_expense :: float / 100.0) / 2 as split_exp,
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
# MAGIC       ) :: float / 2 as split_margin,
# MAGIC       --carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC       --carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC       --carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_lhc,
# MAGIC       'RELAY' as tms
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join bronze.big_export_projection e on use_relay_loads.relay_load :: float = e.load_number :: float
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date --left join invoiced_at_and_amt_final on big_export_projection.load_number::float = invoiced_at_and_amt_final.loadnum::float
# MAGIC       left join invoiced_at_and_amt_final on use_relay_loads.relay_load :: float = invoiced_at_and_amt_final.loadnum :: float
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1 --left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC       --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter = 'ltl- (hain,deb,roland)'
# MAGIC       and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and e.customer_id in ('hain', 'roland', 'deb')
# MAGIC     Union
# MAGIC     select
# MAGIC       distinct -- Carrier
# MAGIC       relay_load as relay_ref_num,
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       Case
# MAGIC         when modee in ('POWER ONLY', 'IMDL', 'PORTD') then null
# MAGIC         else modee
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else Equipment
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num,
# MAGIC       carr_office as office,
# MAGIC       'Carrier',
# MAGIC       Carrier_Rep,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       mastername,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       del_date as Delivery_Date,
# MAGIC       rolled_at as rolled_date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tendering_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       invoiced_at_and_amt_final.invoiced_at,
# MAGIC       invoiced_at_and_amt_final.invoiced_amount,
# MAGIC       invoiced_at_and_amt_final.total_transactions,
# MAGIC       invoiced_at_and_amt_final.last_deposit,
# MAGIC       case
# MAGIC         when invoiced_at_and_amt_final.invoiced_amount is null then 0
# MAGIC         else invoiced_at_and_amt_final.invoiced_amount :: float
# MAGIC       end - case
# MAGIC         when invoiced_at_and_amt_final.total_transactions is null then 0
# MAGIC         else total_transactions :: float
# MAGIC       end as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when e.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Number,
# MAGIC       e.carrier_name as Carrier,
# MAGIC       total_miles :: float,
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
# MAGIC       ) :: float / 2 as split_rev,
# MAGIC       (projected_expense :: float / 100.0) / 2 as split_exp,
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
# MAGIC       ) :: float / 2 as split_margin,
# MAGIC       --carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC       --carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC       --carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_lhc,
# MAGIC       'RELAY' as tms
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join bronze.big_export_projection e on use_relay_loads.relay_load :: float = e.load_number :: float
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_at_and_amt_final on use_relay_loads.relay_load :: float = invoiced_at_and_amt_final.loadnum :: float
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1 --left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC       --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter = 'ltl- (hain,deb,roland)'
# MAGIC       and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and e.customer_id in ('hain', 'roland', 'deb')
# MAGIC     union
# MAGIC     select
# MAGIC       distinct -- Shipper
# MAGIC       relay_load as relay_ref_num,
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       Case
# MAGIC         when modee in ('POWER ONLY', 'IMDL', 'PORTD') then null
# MAGIC         else modee
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else Equipment
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num,
# MAGIC       customer_office as office,
# MAGIC       'Shipper',
# MAGIC       Carrier_Rep,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       mastername,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       del_date as Delivery_Date,
# MAGIC       rolled_at as rolled_date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tendering_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       invoiced_at_and_amt_final.invoiced_at,
# MAGIC       invoiced_at_and_amt_final.invoiced_amount,
# MAGIC       invoiced_at_and_amt_final.total_transactions,
# MAGIC       invoiced_at_and_amt_final.last_deposit,
# MAGIC       case
# MAGIC         when invoiced_at_and_amt_final.invoiced_amount is null then 0
# MAGIC         else invoiced_at_and_amt_final.invoiced_amount :: float
# MAGIC       end - case
# MAGIC         when invoiced_at_and_amt_final.total_transactions is null then 0
# MAGIC         else total_transactions :: float
# MAGIC       end as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when use_relay_loads.carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Number,
# MAGIC       use_relay_loads.carrier_name as Carrier,
# MAGIC       total_miles :: float,
# MAGIC       total_cust_rate.total_cust_rate :: float / 2 as split_rev,
# MAGIC       total_carrier_rate.total_carrier_rate :: float / 2 :: float as split_exp,
# MAGIC       coalesce(total_cust_rate.total_cust_rate, 0) :: float - coalesce(
# MAGIC         (total_carrier_rate.total_carrier_rate :: float / 2),
# MAGIC         0
# MAGIC       ) :: float / 2 as split_margin,
# MAGIC       --carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC       --carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC       --carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC       (
# MAGIC         customer_money_final.usd_fsc :: float + (
# MAGIC           customer_money_final.cad_fsc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc :: float + (
# MAGIC           customer_money_final.cad_acc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc :: float + (
# MAGIC           customer_money_final.cad_lhc * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           ) :: float
# MAGIC         )
# MAGIC       ) / 2 as customer_lhc,
# MAGIC       'RELAY' as tms
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number
# MAGIC       left join total_carrier_rate on use_relay_loads.combined_load = total_carrier_rate.relay_reference_number
# MAGIC       left join combined_loads_mine on use_relay_loads.relay_load :: float = combined_loads_mine.combine_new_rrn
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_at_and_amt_final on use_relay_loads.relay_load :: float = invoiced_at_and_amt_final.loadnum :: float
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join customer_money_final on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC       left join bronze.canada_conversions on use_date :: date = canada_conversions.ship_date :: date
# MAGIC       join average_conversions on 1 = 1 --left join carrier_charges_one on  use_relay_loads.relay_load = carrier_charges_one.relay_reference_number
# MAGIC       --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC       and combined_load != 'NO'
# MAGIC     Union
# MAGIC     select
# MAGIC       distinct -- Carrier
# MAGIC       relay_load as relay_ref_num,
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       Case
# MAGIC         when modee in ('POWER ONLY', 'IMDL', 'PORTD') then null
# MAGIC         else modee
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else Equipment
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num,
# MAGIC       carr_office as office,
# MAGIC       'Carrier',
# MAGIC       Carrier_Rep,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       mastername,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       del_date as Delivery_Date,
# MAGIC       rolled_at as rolled_date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tendering_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       invoiced_amts.invoiced_at :: date as invoiced_Date,
# MAGIC       invoiced_at_and_amt_final.invoiced_at,
# MAGIC       invoiced_at_and_amt_final.invoiced_amount,
# MAGIC       invoiced_at_and_amt_final.total_transactions,
# MAGIC       invoiced_at_and_amt_final.last_deposit,
# MAGIC       case
# MAGIC         when invoiced_at_and_amt_final.invoiced_amount is null then 0
# MAGIC         else invoiced_at_and_amt_final.invoiced_amount :: float
# MAGIC       end - case
# MAGIC         when invoiced_at_and_amt_final.total_transactions is null then 0
# MAGIC         else total_transactions :: float
# MAGIC       end as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Number,
# MAGIC       use_relay_loads.carrier_name as Carrier,
# MAGIC       total_miles :: float,
# MAGIC       total_cust_rate.total_cust_rate :: float / 2 as split_rev,
# MAGIC       total_carrier_rate.total_carrier_rate :: float / 2 :: float as split_exp,
# MAGIC       coalesce(total_cust_rate.total_cust_rate, 0) :: float - coalesce(
# MAGIC         (total_carrier_rate.total_carrier_rate :: float / 2),
# MAGIC         0
# MAGIC       ) :: float / 2 as split_margin,
# MAGIC       carrier_charges_one.carr_lhc / 2 as Linehaul,
# MAGIC       carrier_charges_one.carr_fsc / 2 as Fuel_Surcharge,
# MAGIC       carrier_charges_one.carr_acc / 2 as Other_Fess,
# MAGIC       'RELAY' as tms
# MAGIC     from
# MAGIC       use_relay_loads
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load = total_cust_rate.relay_reference_number
# MAGIC       left join total_carrier_rate on use_relay_loads.combined_load = total_carrier_rate.relay_reference_number
# MAGIC       left join combined_loads_mine on use_relay_loads.relay_load :: float = combined_loads_mine.combine_new_rrn
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_at_and_amt_final on use_relay_loads.relay_load :: float = invoiced_at_and_amt_final.loadnum :: float
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join carrier_charges_one on use_relay_loads.relay_load = carrier_charges_one.relay_reference_number --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC       and combined_load != 'NO'
# MAGIC     union
# MAGIC     select
# MAGIC       distinct -- Shipper  ----- Need to change from here.
# MAGIC       id :: float as relay_ref_num,
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       Case
# MAGIC         when modee = 'Dry Van' then 'Dry Van'
# MAGIC         when modee = 'Reefer' then 'Reefer'
# MAGIC         when modee = 'Flatbed' then 'Flatbed'
# MAGIC         else null
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else tl_or_ltl
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num,
# MAGIC       customer_office as office,
# MAGIC       'Shipper',
# MAGIC       key_c_user,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       mastername,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       delivery_date as Delivery_Date,
# MAGIC       rolled_date as Delivery_Date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tender_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       null :: date as invoiced_Date,
# MAGIC       null as invoiced_at,
# MAGIC       null as invoiced_amount,
# MAGIC       null as total_transactions,
# MAGIC       null as last_deposit,
# MAGIC       null as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Number,
# MAGIC       aljex_data.carrier_name as Carrier,
# MAGIC       case
# MAGIC         miles Rlike '^\d+(.\d+)?$'
# MAGIC         when true then miles :: float
# MAGIC         else 0
# MAGIC       end as miles,
# MAGIC       (
# MAGIC         case
# MAGIC           when cust_curr = 'USD' then invoice_total :: float
# MAGIC           else invoice_total :: float / aljex_data.conversion_rate :: float
# MAGIC         end
# MAGIC       ) / 2 as split_rev,
# MAGIC       (
# MAGIC         case
# MAGIC           when carrier_curr = 'USD' then final_carrier_rate :: float
# MAGIC           else final_carrier_rate :: float / aljex_data.conversion_rate
# MAGIC         end
# MAGIC       ) / 2 as split_exp,
# MAGIC       (
# MAGIC         coalesce(
# MAGIC           case
# MAGIC             when cust_curr = 'USD' then invoice_total :: float
# MAGIC             else invoice_total :: float / aljex_data.conversion_rate :: float
# MAGIC           end,
# MAGIC           0
# MAGIC         ) - coalesce(
# MAGIC           case
# MAGIC             when carrier_curr = 'USD' then final_carrier_rate :: float
# MAGIC             else final_carrier_rate :: float / aljex_data.conversion_rate
# MAGIC           end,
# MAGIC           0
# MAGIC         )
# MAGIC       ) / 2 as split_margin,
# MAGIC       --carrier_lhc / 2 as Linehaul,
# MAGIC       --carrier_fuel / 2 as Fuel_Surcharge,
# MAGIC       --carrier_acc / 2 as Other_Fess,
# MAGIC       (aljex_data.customer_fuel) / 2 as customer_fuel,
# MAGIC       (aljex_data.customer_acc) / 2 as customer_acc,
# MAGIC       (
# MAGIC         aljex_data.invoice_total :: float - aljex_data.customer_fuel :: float - aljex_data.customer_acc :: float
# MAGIC       ) / 2 as customer_lhc,
# MAGIC       'ALJEX' as tms
# MAGIC     from
# MAGIC       aljex_data
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     Union
# MAGIC     select
# MAGIC       distinct -- Shipper  ----- Need to change from here.
# MAGIC       id :: float as relay_ref_num,
# MAGIC       0.5 as Load_Counter,
# MAGIC       use_date,
# MAGIC       Case
# MAGIC         when modee = 'Dry Van' then 'Dry Van'
# MAGIC         when modee = 'Reefer' then 'Reefer'
# MAGIC         when modee = 'Flatbed' then 'Flatbed'
# MAGIC         else null
# MAGIC       end as Equipment,
# MAGIC       Case
# MAGIC         when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC         when modee = 'IMDL' THEN 'Intermodal'
# MAGIC         when modee = 'PORTD' THEN 'Drayage'
# MAGIC         else tl_or_ltl
# MAGIC       End as Mode,
# MAGIC       date_range_two.week_num,
# MAGIC       carr_office as office,
# MAGIC       'carrier',
# MAGIC       key_c_user,
# MAGIC       --carr_office,
# MAGIC       --financial_period_sorting,
# MAGIC       --financial_year,
# MAGIC       mastername,
# MAGIC       customer_name,
# MAGIC       customer_rep,
# MAGIC       status as load_status,
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
# MAGIC       market_origin as Market_origin,
# MAGIC       market_dest as Market_Deat,
# MAGIC       market_lane as Market_Lane,
# MAGIC       delivery_date as Delivery_Date,
# MAGIC       rolled_date as Delivery_Date,
# MAGIC       Booked_Date as Booked_Date,
# MAGIC       Tender_Date as Tendered_Date,
# MAGIC       bounced_date as bounced_date,
# MAGIC       cancelled_date as Cancelled_Date,
# MAGIC       Days_ahead as Days_Ahead,
# MAGIC       prebookable,
# MAGIC       PickUp_Start as Pickup_Date,
# MAGIC       PickUp_End as Pickup_EndDate,
# MAGIC       Drop_Start as Drop_Start,
# MAGIC       Drop_End as Drop_End,
# MAGIC       null :: date as invoiced_Date,
# MAGIC       null as invoiced_at,
# MAGIC       null as invoiced_amount,
# MAGIC       null as total_transactions,
# MAGIC       null as last_deposit,
# MAGIC       null as outstanding_bal,
# MAGIC       --load_pickup.Days_ahead as Days_Ahead,
# MAGIC       CASE
# MAGIC         WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC         ELSE 'Same Office'
# MAGIC       END AS Booking_type,
# MAGIC       case
# MAGIC         when carrier_name is not null then '1'
# MAGIC         else '0'
# MAGIC       end as Load_flag,
# MAGIC       dot_num as DOT_Number,
# MAGIC       aljex_data.carrier_name as Carrier,
# MAGIC       case
# MAGIC         miles Rlike '^\d+(.\d+)?$'
# MAGIC         when true then miles :: float
# MAGIC         else 0
# MAGIC       end as miles,
# MAGIC       (
# MAGIC         case
# MAGIC           when cust_curr = 'USD' then invoice_total :: float
# MAGIC           else invoice_total :: float / aljex_data.conversion_rate :: float
# MAGIC         end
# MAGIC       ) / 2 as split_rev,
# MAGIC       (
# MAGIC         case
# MAGIC           when carrier_curr = 'USD' then final_carrier_rate :: float
# MAGIC           else final_carrier_rate :: float / aljex_data.conversion_rate
# MAGIC         end
# MAGIC       ) / 2 as split_exp,
# MAGIC       (
# MAGIC         coalesce(
# MAGIC           case
# MAGIC             when cust_curr = 'USD' then invoice_total :: float
# MAGIC             else invoice_total :: float / aljex_data.conversion_rate :: float
# MAGIC           end,
# MAGIC           0
# MAGIC         ) - coalesce(
# MAGIC           case
# MAGIC             when carrier_curr = 'USD' then final_carrier_rate :: float
# MAGIC             else final_carrier_rate :: float / aljex_data.conversion_rate
# MAGIC           end,
# MAGIC           0
# MAGIC         )
# MAGIC       ) / 2 as split_margin,
# MAGIC       --carrier_lhc / 2 as Linehaul,
# MAGIC       --carrier_fuel / 2 as Fuel_Surcharge,
# MAGIC       --carrier_acc / 2 as Other_Fess,
# MAGIC       (aljex_data.customer_fuel) / 2 as customer_fuel,
# MAGIC       (aljex_data.customer_acc) / 2 as customer_acc,
# MAGIC       (
# MAGIC         aljex_data.invoice_total :: float - aljex_data.customer_fuel :: float - aljex_data.customer_acc :: float
# MAGIC       ) / 2 as customer_lhc,
# MAGIC       'ALJEX' as tms
# MAGIC     from
# MAGIC       aljex_data
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC   )
# MAGIC   Select
# MAGIC     *
# MAGIC   from
# MAGIC     system_union --limit 100
# MAGIC )
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC TRUNCATE table analytics.reporting_split_fact_load_New_Temp;
# MAGIC INSERT INTO analytics.reporting_split_fact_load_New_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.reporting_split_fact_load_New

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.reporting_split_fact_load_New;
# MAGIC INSERT INTO analytics.reporting_split_fact_load_New
# MAGIC SELECT *
# MAGIC FROM analytics.reporting_split_fact_load_New_Temp