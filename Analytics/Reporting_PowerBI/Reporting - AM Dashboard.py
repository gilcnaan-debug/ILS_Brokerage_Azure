# Databricks notebook source
# MAGIC %md
# MAGIC ## Reporting Views
# MAGIC * **Description:** To bulid a code that extracts req data from source for AM Dashboard
# MAGIC * **Created Date:** 06/25/2025
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 07/15/2025
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC  SET ansi_mode = false; 
# MAGIC create or replace view bronze.am_spot as (
# MAGIC WITH new_customer_money_sp AS (
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
# MAGIC       bronze.moneying_billing_party_transaction AS m
# MAGIC       LEFT JOIN bronze.invoicing_credits AS ic ON CAST(m.relay_reference_number AS FLOAT) = CAST(ic.relay_reference_number AS FLOAT)
# MAGIC     WHERE
# MAGIC       m.`voided?` = 'false'
# MAGIC     GROUP BY
# MAGIC       m.relay_reference_number
# MAGIC   ),
# MAGIC
# MAGIC -- Select * from new_customer_money_sp
# MAGIC  raw_data_one AS (
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
# MAGIC       bronze.spot_quotes
# MAGIC       JOIN analytics.dim_date ON CAST(spot_quotes.created_at AS DATE) = dim_date.Calendar_Date
# MAGIC       LEFT JOIN bronze.relay_users ON spot_quotes.created_by = relay_users.full_name
# MAGIC       LEFT JOIN bronze.aljex_user_report_listing ON spot_quotes.created_by = aljex_user_report_listing.full_name
# MAGIC       LEFT JOIN bronze.customer_lookup ON LEFT(customer_name, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC     WHERE
# MAGIC       1 = 1
# MAGIC     ORDER BY
# MAGIC       created_at DESC
# MAGIC   ),
# MAGIC -- Select * from raw_data_one
# MAGIC raw_data AS (
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
# MAGIC       
# MAGIC     ORDER BY
# MAGIC       created_at DESC
# MAGIC   ),
# MAGIC -- Select * from raw_data
# MAGIC relay_spot_number AS (
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
# MAGIC       bronze.booking_projection AS b
# MAGIC       LEFT JOIN bronze.customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       LEFT JOIN bronze.customer_lookup AS cl ON LEFT(r.customer_name, 32) = LEFT(cl.aljex_customer_name, 32)
# MAGIC       LEFT JOIN bronze.tendering_acceptance AS t ON b.relay_reference_number = t.relay_reference_number
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
# MAGIC aljex_spot_loads as (
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
# MAGIC       bronze.projection_load_1 p
# MAGIC       left join bronze.projection_load_2 p1 on p.id = p1.id
# MAGIC       LEFT JOIN bronze.customer_lookup cl ON LEFT(p1.shipper, 32) = LEFT(cl.aljex_customer_name, 32)
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
# MAGIC
# MAGIC -- Select * from aljex_spot_loads
# MAGIC combined_loads_mine as (
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
# MAGIC
# MAGIC   -- Select * from combined_loads_mine
# MAGIC max_schedule_del as (
# MAGIC     SELECT
# MAGIC       DISTINCT delivery_projection.relay_reference_number,
# MAGIC       max(delivery_projection.scheduled_at :: timestamp) AS max_schedule_del
# MAGIC     FROM
# MAGIC       bronze.delivery_projection
# MAGIC     GROUP BY
# MAGIC       delivery_projection.relay_reference_number
# MAGIC   ),
# MAGIC
# MAGIC last_delivery as (
# MAGIC     SELECT
# MAGIC       DISTINCT planning_stop_schedule.relay_reference_number,
# MAGIC       max(planning_stop_schedule.sequence_number) AS last_delivery
# MAGIC     FROM
# MAGIC       bronze.planning_stop_schedule
# MAGIC     WHERE
# MAGIC       planning_stop_schedule.`removed?` = false
# MAGIC     GROUP BY
# MAGIC       planning_stop_schedule.relay_reference_number
# MAGIC   ) ,
# MAGIC
# MAGIC    pricing_nfi_load_predictions as (
# MAGIC     select
# MAGIC       external_load_id as MR_id,
# MAGIC       max(target_buy_rate_gsn) * max(miles_predicted) as Market_Buy_Rate,
# MAGIC       1 as Markey_buy_rate_flag
# MAGIC     from
# MAGIC       `bronze`.`pricing_nfi_load_predictions`
# MAGIC     group by
# MAGIC       external_load_id
# MAGIC   ) ,
# MAGIC
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
# MAGIC   ),
# MAGIC     average_conversions as (select
# MAGIC       avg(us_to_cad) as avg_us_to_cad,
# MAGIC       avg(cad_to_us) as avg_cad_to_us
# MAGIC     from
# MAGIC       to_get_avg
# MAGIC   ),
# MAGIC use_relay_loads as (
# MAGIC        select
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
# MAGIC 	   coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as pu_appt_date,
# MAGIC 	  --b.booked_at as Booked_Date,
# MAGIC 	--    coalesce(
# MAGIC     --     (coalesce(s.appointment_datetime, s.window_start_datetime, s.window_end_datetime, p.appointment_datetime, b.ready_date::timestamp)::timestamp at time zone shippers.time_zone at time zone 'america/chicago')::timestamp,
# MAGIC     --     (b.ready_date::timestamp at time zone shippers.time_zone at time zone 'america/chicago')::timestamp
# MAGIC     -- --   ) as new_pu_appt,
# MAGIC     --    coalesce(
# MAGIC     --     COALESCE(s.appointment_datetime, s.window_start_datetime, s.window_end_datetime, p.appointment_datetime, b.ready_date::timestamp) at time zone shippers.time_zone at time zone 'america/chicago',
# MAGIC     --     b.ready_date::timestamp at time zone shippers.time_zone at time zone 'america/chicago'  -- making sure ready_date fallback also has timezone conversion
# MAGIC     --   ) as new_pu_appt,
# MAGIC  COALESCE(
# MAGIC       CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       CAST(pp.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(b.ready_date AS TIMESTAMP)
# MAGIC     ) AS new_pu_appt,
# MAGIC     c.in_date_time :: TIMESTAMP As pickup_in,
# MAGIC         c.out_date_time :: TIMESTAMP As pickup_out,
# MAGIC      coalesce(
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
# MAGIC 	  
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
# MAGIC        cd.out_date_time AS delivery_out,
# MAGIC         COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC         
# MAGIC     CASE
# MAGIC     WHEN date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC     ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval)
# MAGIC END AS del_appt_date,
# MAGIC       --pickup.Days_ahead as Days_Ahead,
# MAGIC       --pickup.prebookable as prebookable,
# MAGIC       --papp.PickUp_Start,
# MAGIC       --papp.PickUp_End,
# MAGIC       --dapp.Drop_Start,
# MAGIC       --dapp.Drop_End,
# MAGIC       b.booked_carrier_name as carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC 	  b.cargo_value_amount/100.00 as Cargo_Value,
# MAGIC       b.total_weight as Weight,
# MAGIC 	  mx.max_buy::float / 100.00 as max_buy,
# MAGIC 	  mr.Market_Buy_Rate,
# MAGIC      sp.quoted_price as Spot_Revenue,
# MAGIC      sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       bronze.booking_projection b
# MAGIC 	  left join relay_spot_number sp on b.relay_reference_number = sp.relay_reference_number
# MAGIC       left join bronze.sourcing_max_buy_v2 mx on b.relay_reference_number = mx.relay_reference_number 
# MAGIC 	  left join pricing_nfi_load_predictions mr on b.relay_reference_number = mr.MR_id   ------ Market rate 
# MAGIC     --   Left Join bronze.spot_quotes spot on b.relay_reference_number::string = spot.load_number::string and spot.status_str = 'WON'
# MAGIC       left join bronze.truckload_projection t on b.relay_reference_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced'
# MAGIC       and t.trailer_number is not null
# MAGIC        LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC       LEFT join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true'
# MAGIC       left join bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC       JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office ----- Need to change to new_office_lookup_w_tgt later
# MAGIC       LEFT JOIN bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(b.receiver_zip, 3) = d.pickup_zip
# MAGIC       LEFT join bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC       AND relay_users.`active?` = 'true'
# MAGIC       LEFT join bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
# MAGIC
# MAGIC       Left Join  last_delivery on b.relay_reference_number = last_delivery.relay_reference_number
# MAGIC ---- Delivery date JOIN
# MAGIC LEFT JOIN bronze.delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number AND b.receiver_name::string = dp.receiver_name::string 
# MAGIC AND max_schedule_del.max_schedule_del = dp.scheduled_at --::timestamp --without time zone
# MAGIC
# MAGIC
# MAGIC       LEFT JOIN bronze.planning_stop_schedule dpss ON
# MAGIC 	 b.relay_reference_number = dpss.relay_reference_number 
# MAGIC 	 AND b.receiver_name::string = dpss.stop_name::string 
# MAGIC 	 AND b.receiver_city::string = dpss.locality::string 
# MAGIC 	 AND last_delivery.last_delivery = dpss.sequence_number 
# MAGIC 	 AND dpss.stop_type::string = 'delivery'::string AND dpss.`removed?` = false
# MAGIC 	 
# MAGIC 	  LEFT JOIN bronze.canonical_stop cd ON dpss.stop_id::string = cd.stop_id::string AND b.receiver_city::string = cd.locality::string AND cd.`stale?` = false AND cd.stop_type::string = 'delivery'::string
# MAGIC ---- Delivery Detail End
# MAGIC
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
# MAGIC 	  
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join bronze.tendering_planned_distance on b.relay_reference_number = tendering_planned_distance.relay_reference_number
# MAGIC       left join bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC     where
# MAGIC       1 = 1 -- [[and {{date_range}}]]
# MAGIC       -- and t.trailer_number is not null
# MAGIC       and b.status = 'booked'
# MAGIC       and canonical_plan_projection.mode not like '%ltl%'
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and combined is null
# MAGIC
# MAGIC -- Select * from use_relay_loads  limit 100
# MAGIC
# MAGIC     Union
# MAGIC 	
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
# MAGIC 	   COALESCE(
# MAGIC       CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       CAST(pp.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(b.ready_date AS TIMESTAMP)
# MAGIC     ) AS new_pu_appt,  
# MAGIC  c.in_date_time :: TIMESTAMP As pickup_in,
# MAGIC         c.out_date_time :: TIMESTAMP As pickup_out,
# MAGIC      coalesce(
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
# MAGIC 	     cd.in_date_time AS delivery_in,
# MAGIC        cd.out_date_time AS delivery_out,
# MAGIC         COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC         CASE
# MAGIC     WHEN date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC     ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval)
# MAGIC END AS del_appt_date,
# MAGIC     --   pickup.Days_ahead as Days_Ahead,
# MAGIC     --   pickup.prebookable as prebookable,
# MAGIC     --   papp.PickUp_Start,
# MAGIC     --   papp.PickUp_End,
# MAGIC     --   dapp.Drop_Start,
# MAGIC     --   dapp.Drop_End,
# MAGIC       b.booked_carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC 	  b.cargo_value_amount/100.00 as Cargo_Value,
# MAGIC       b.total_weight as Weight,
# MAGIC 	   mx.max_buy::float / 100.00 as max_buy,
# MAGIC 	   mr.Market_Buy_Rate,
# MAGIC      sp.quoted_price as Spot_Revenue,
# MAGIC     sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       combined_loads_mine
# MAGIC       JOIN bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC 	  left join relay_spot_number sp on b.relay_reference_number = sp.relay_reference_number
# MAGIC       left join bronze.sourcing_max_buy_v2 mx on b.relay_reference_number = mx.relay_reference_number 
# MAGIC 	  left join pricing_nfi_load_predictions mr on b.relay_reference_number = mr.MR_id   ----- market_Rate
# MAGIC 	   LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC     --   Left Join bronze.spot_quotes spot on b.relay_reference_number = spot.load_number and spot.status_str = 'WON'
# MAGIC       left join bronze.truckload_projection t on b.relay_reference_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced'
# MAGIC 	Left Join  last_delivery on b.relay_reference_number = last_delivery.relay_reference_number  
# MAGIC 	  ---- Delivery date JOIN
# MAGIC LEFT JOIN bronze.delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number AND b.receiver_name::string = dp.receiver_name::string 
# MAGIC AND max_schedule_del.max_schedule_del = dp.scheduled_at --::timestamp --without time zone
# MAGIC
# MAGIC
# MAGIC       LEFT JOIN bronze.planning_stop_schedule dpss ON
# MAGIC 	 b.relay_reference_number = dpss.relay_reference_number 
# MAGIC 	 AND b.receiver_name::string = dpss.stop_name::string 
# MAGIC 	 AND b.receiver_city::string = dpss.locality::string 
# MAGIC 	 AND last_delivery.last_delivery = dpss.sequence_number 
# MAGIC 	 AND dpss.stop_type::string = 'delivery'::string AND dpss.`removed?` = false
# MAGIC 	 
# MAGIC 	  LEFT JOIN bronze.canonical_stop cd ON dpss.stop_id::string = cd.stop_id::string AND b.receiver_city::string = cd.locality::string AND cd.`stale?` = false AND cd.stop_type::string = 'delivery'::string
# MAGIC ---- Delivery Detail End
# MAGIC
# MAGIC
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
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(b.receiver_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true'
# MAGIC       left join bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_one = tendering_planned_distance.relay_reference_number
# MAGIC       left join bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC       LEFT join bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC       AND relay_users.`active?` = 'true'
# MAGIC       LEFT join bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC     where
# MAGIC       1 = 1 --[[and {{date_range}}]]
# MAGIC       --and t.trailer_number is not null
# MAGIC       and b.status = 'booked'
# MAGIC       and canonical_plan_projection.mode not like '%ltl%'
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC 	 
# MAGIC  --Select * from use_relay_loads limit 100	 
# MAGIC  
# MAGIC  --- 3rd update-- Delivery
# MAGIC     Union
# MAGIC 	
# MAGIC 	
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
# MAGIC       CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       CAST(pp.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(b.ready_date AS TIMESTAMP)
# MAGIC     ) AS new_pu_appt, 
# MAGIC 	 c.in_date_time :: TIMESTAMP As pickup_in,
# MAGIC         c.out_date_time :: TIMESTAMP As pickup_out,
# MAGIC      coalesce(
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
# MAGIC 	     cd.in_date_time AS delivery_in,
# MAGIC        cd.out_date_time AS delivery_out,
# MAGIC         COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC            CASE
# MAGIC     WHEN date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC     ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval)
# MAGIC END AS del_appt_date,
# MAGIC     --   pickup.Days_ahead as Days_Ahead,
# MAGIC     --   pickup.prebookable as prebookable,
# MAGIC     --   papp.PickUp_Start,
# MAGIC     --   papp.PickUp_End,
# MAGIC     --   dapp.Drop_Start,
# MAGIC     --   dapp.Drop_End,
# MAGIC       b.booked_carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC 	  b.cargo_value_amount/100.00 as Cargo_Value,
# MAGIC     b.total_weight as Weight,
# MAGIC     mx.max_buy::float / 100.00 as max_buy,
# MAGIC 	mr.Market_Buy_Rate,
# MAGIC      
# MAGIC     sp.quoted_price as Spot_Revenue,
# MAGIC     sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       combined_loads_mine
# MAGIC       JOIN bronze.booking_projection b on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC 	   left join relay_spot_number sp on b.relay_reference_number = sp.relay_reference_number
# MAGIC        left join bronze.sourcing_max_buy_v2 mx on b.relay_reference_number = mx.relay_reference_number 
# MAGIC 	    left join pricing_nfi_load_predictions mr on b.relay_reference_number = mr.MR_id --- Market rate 
# MAGIC 	   LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC     --   Left Join bronze.spot_quotes spot on b.relay_reference_number = spot.load_number and spot.status_str = 'WON'
# MAGIC       left join bronze.truckload_projection t on b.relay_reference_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced'
# MAGIC       and t.trailer_number is not null
# MAGIC 	  	Left Join  last_delivery on b.relay_reference_number = last_delivery.relay_reference_number  
# MAGIC 		
# MAGIC 	  ---- Delivery date JOIN
# MAGIC LEFT JOIN bronze.delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number AND b.receiver_name::string = dp.receiver_name::string 
# MAGIC AND max_schedule_del.max_schedule_del = dp.scheduled_at --::timestamp --without time zone
# MAGIC
# MAGIC
# MAGIC       LEFT JOIN bronze.planning_stop_schedule dpss ON
# MAGIC 	 b.relay_reference_number = dpss.relay_reference_number 
# MAGIC 	 AND b.receiver_name::string = dpss.stop_name::string 
# MAGIC 	 AND b.receiver_city::string = dpss.locality::string 
# MAGIC 	 AND last_delivery.last_delivery = dpss.sequence_number 
# MAGIC 	 AND dpss.stop_type::string = 'delivery'::string AND dpss.`removed?` = false
# MAGIC 	 
# MAGIC 	  LEFT JOIN bronze.canonical_stop cd ON dpss.stop_id::string = cd.stop_id::string AND b.receiver_city::string = cd.locality::string AND cd.`stale?` = false AND cd.stop_type::string = 'delivery'::string
# MAGIC ---- Delivery Detail End 
# MAGIC
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
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(b.receiver_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true'
# MAGIC       left join bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join bronze.tendering_planned_distance on combined_loads_mine.relay_reference_number_two = tendering_planned_distance.relay_reference_number
# MAGIC       left join bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC       LEFT join bronze.relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC       AND relay_users.`active?` = 'true'
# MAGIC       LEFT join bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office
# MAGIC     where
# MAGIC       1 = 1 --[[and {{date_range}}]]
# MAGIC       --and and t.trailer_number is not null
# MAGIC       and b.status = 'booked'
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and canonical_plan_projection.mode not like '%ltl%'
# MAGIC
# MAGIC       union 
# MAGIC
# MAGIC       select
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
# MAGIC       null::date as ready_date,
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as planning_stop_sched,
# MAGIC 	   null::date as new_pu_appt,
# MAGIC 	  coalesce(
# MAGIC         e.ship_date :: timestamp,
# MAGIC         c.in_date_time :: TIMESTAMP) as pickup_in,
# MAGIC 		c.out_date_time :: TIMESTAMP as pickup_out,
# MAGIC 		 coalesce(
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
# MAGIC 	  COALESCE(e.delivered_date::timestamp, cd.in_date_time::timestamp) AS delivery_in,
# MAGIC     cd.out_date_time::timestamp AS delivery_out,
# MAGIC     COALESCE(e.delivered_date::timestamp , cd.in_date_time::timestamp , cd.out_date_time::timestamp) AS delivery_datetime,
# MAGIC
# MAGIC    CASE
# MAGIC     WHEN date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC     ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval)
# MAGIC END AS del_appt_date,
# MAGIC     --   pickup.Days_ahead as Days_Ahead,
# MAGIC     --   pickup.prebookable as prebookable,
# MAGIC     --   papp.PickUp_Start,
# MAGIC     --   papp.PickUp_End,
# MAGIC     --   dapp.Drop_Start,
# MAGIC     --   dapp.Drop_End,
# MAGIC     e.carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC 	  Null AS Cargo_value,
# MAGIC       e.weight,
# MAGIC       mx.max_buy::float / 100.00 as max_buy,
# MAGIC 	 mr.Market_Buy_Rate,
# MAGIC     sp.quoted_price as Spot_Revenue,
# MAGIC     sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       bronze.big_export_projection e
# MAGIC 	   left join relay_spot_number sp on e.load_number = sp.relay_reference_number
# MAGIC     --   Left Join bronze.spot_quotes spot on e.load_number = spot.load_number and spot.status_str = 'WON'
# MAGIC       left join  bronze.truckload_projection t on e.load_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced'
# MAGIC       --and t.trailer_number is not null
# MAGIC 	     LEFT JOIN max_schedule_del ON e.load_number = max_schedule_del.relay_reference_number
# MAGIC 		  left join pricing_nfi_load_predictions mr on e.load_number = mr.MR_id   
# MAGIC 	 left join bronze.sourcing_max_buy_v2 mx on e.load_number = mx.relay_reference_number
# MAGIC       LEFT join bronze.customer_profile_projection r on e.customer_id = r.customer_slug
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       LEFT JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join  bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true'
# MAGIC       JOIN bronze.canonical_plan_projection on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC 	  
# MAGIC 	  ---- Delivery Date Start---
# MAGIC     LEFT JOIN bronze.delivery_projection dp 
# MAGIC ON e.load_number = dp.relay_reference_number 
# MAGIC AND e.consignee_name = dp.receiver_name 
# MAGIC AND max_schedule_del.max_schedule_del = cast(dp.scheduled_at as timestamp)
# MAGIC 	  LEFT JOIN last_delivery ON e.load_number = last_delivery.relay_reference_number
# MAGIC      LEFT JOIN bronze.planning_stop_schedule dpss ON e.load_number = dpss.relay_reference_number AND e.consignee_name::string = dpss.stop_name::string AND e.consignee_city::string = dpss.locality::string 
# MAGIC 	 AND last_delivery.last_delivery = dpss.sequence_number AND dpss.stop_type::string = 'delivery'::string AND dpss.`removed?` = false
# MAGIC      LEFT JOIN bronze.canonical_stop cd ON dpss.stop_id::string = cd.stop_id::string
# MAGIC 	 --- Delivery Date end-----
# MAGIC 	  
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
# MAGIC       --and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC       and canonical_plan_projection.mode = 'ltl'
# MAGIC       and e.customer_id NOT in ('roland', 'hain', 'deb')
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC    select
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
# MAGIC       null::date as ready_date,
# MAGIC 	--   null::date as new_pu_appt,
# MAGIC       coalesce(
# MAGIC         pss.appointment_datetime :: timestamp,
# MAGIC         pss.window_end_datetime :: timestamp,
# MAGIC         pss.window_start_datetime :: timestamp,
# MAGIC         pp.appointment_datetime :: timestamp
# MAGIC       ) as planning_stop_sched,
# MAGIC       Null:Date as new_pu_appt,
# MAGIC 	  coalesce(
# MAGIC         e.ship_date :: timestamp,
# MAGIC         c.in_date_time :: TIMESTAMP) as pickup_in,
# MAGIC 		c.out_date_time :: TIMESTAMP as pickup_out,
# MAGIC 		 coalesce(
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
# MAGIC       
# MAGIC       null :: date as rolled_out,
# MAGIC       null as Booked_Date,
# MAGIC       null :: string as Tender_Date,
# MAGIC       null :: date as bounced_date,
# MAGIC       null :: date as cancelled_date,
# MAGIC 	   COALESCE(e.delivered_date::timestamp, cd.in_date_time::timestamp) AS delivery_in,
# MAGIC     cd.out_date_time::timestamp AS delivery_out,
# MAGIC     COALESCE(e.delivered_date::timestamp , cd.in_date_time::timestamp , cd.out_date_time::timestamp) AS delivery_datetime,
# MAGIC        CASE
# MAGIC     WHEN date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL THEN cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC     ELSE cast(nullif(dp.appointment_date, '') as timestamp) + cast(date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval)
# MAGIC END AS del_appt_date,
# MAGIC     --   pickup.Days_ahead as Days_Ahead,
# MAGIC     --   pickup.prebookable as prebookable,
# MAGIC     --   papp.PickUp_Start,
# MAGIC     --   papp.PickUp_End,
# MAGIC     --   dapp.Drop_Start,
# MAGIC     --   dapp.Drop_End,
# MAGIC       e.carrier_name,
# MAGIC       sales.full_name as salesrep,
# MAGIC       t.trailer_number as Trailer_Num,
# MAGIC 	  Null as Cargo_Value,
# MAGIC       e.weight as weight,
# MAGIC       mx.max_buy::float / 100.00 as max_buy,
# MAGIC 	  mr.Market_Buy_Rate,
# MAGIC      sp.quoted_price as Spot_Revenue,
# MAGIC      sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       bronze.big_export_projection e
# MAGIC 	  left join relay_spot_number sp on e.load_number = sp.relay_reference_number
# MAGIC 	  left join bronze.sourcing_max_buy_v2 mx on e.load_number = mx.relay_reference_number
# MAGIC     --    Left Join bronze.spot_quotes spot on e.load_number = spot.load_number and spot.status_str = 'WON'
# MAGIC       left join bronze.truckload_projection t on e.load_number = t.relay_reference_number
# MAGIC       and t.status != 'CancelledOrBounced'
# MAGIC       --and t.trailer_number is not null
# MAGIC 	  LEFT JOIN max_schedule_del ON e.load_number = max_schedule_del.relay_reference_number
# MAGIC 	   left join pricing_nfi_load_predictions mr on e.load_number = mr.MR_id  
# MAGIC       LEFT join bronze.customer_profile_projection r on e.customer_id = r.customer_slug
# MAGIC       left join bronze.customer_lookup on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       LEFT JOIN bronze.new_office_lookup_w_tgt cust on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d ON LEFT(e.consignee_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z on r.primary_relay_user_id = z.user_id
# MAGIC       and z.`active?` = 'true'
# MAGIC       left join bronze.relay_users sales on r.sales_relay_user_id = sales.user_id
# MAGIC       and sales.`active?` = 'true'
# MAGIC 	  
# MAGIC 	   ---- Delivery Date Start---
# MAGIC      LEFT JOIN bronze.delivery_projection dp 
# MAGIC 	ON e.load_number = dp.relay_reference_number 
# MAGIC 	AND e.consignee_name = dp.receiver_name 
# MAGIC 	AND max_schedule_del.max_schedule_del = cast(dp.scheduled_at as timestamp)
# MAGIC 	  LEFT JOIN last_delivery ON e.load_number = last_delivery.relay_reference_number
# MAGIC      LEFT JOIN bronze.planning_stop_schedule dpss ON e.load_number = dpss.relay_reference_number AND e.consignee_name::string = dpss.stop_name::string AND e.consignee_city::string = dpss.locality::string 
# MAGIC 	 AND last_delivery.last_delivery = dpss.sequence_number AND dpss.stop_type::string = 'delivery'::string AND dpss.`removed?` = false
# MAGIC      LEFT JOIN bronze.canonical_stop cd ON dpss.stop_id::string = cd.stop_id::string
# MAGIC 	 --- Delivery Date end-----
# MAGIC 	 
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
# MAGIC   )  
# MAGIC ,
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
# MAGIC total_cust_rate as (
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
# MAGIC  new_customer_money as (
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
# MAGIC   ) ,
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
# MAGIC       bronze.moneying_billing_party_transaction
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
# MAGIC       bronze.vendor_transaction_projection
# MAGIC       left join bronze.canada_conversions on vendor_transaction_projection.incurred_at :: date = canada_conversions.ship_date :: date
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
# MAGIC   
# MAGIC  ------------------------------ Customer Linehual , Fuel Surcharge and Accessorial Rate CTE --------------------------------
# MAGIC  
# MAGIC  new_customer_money_two as (
# MAGIC     select distinct
# MAGIC       incurred_at,
# MAGIC       `voided?`,
# MAGIC       moneying_billing_party_transaction.currency as cust_currency_type,
# MAGIC       moneying_billing_party_transaction.relay_reference_number as loadnum,
# MAGIC       charge_code,
# MAGIC       amount::float / 100 as amount
# MAGIC     from
# MAGIC       bronze.moneying_billing_party_transaction
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and `voided?` = 'false'
# MAGIC     order by
# MAGIC       loadnum
# MAGIC   ),
# MAGIC   invoicing_credits as (
# MAGIC     select distinct
# MAGIC       relay_reference_number,
# MAGIC       sum(total_amount)
# MAGIC         filter ( where currency = 'USD' )::float
# MAGIC       / 100.00 as usd_cred,
# MAGIC       sum(total_amount)
# MAGIC         filter ( where currency = 'CAD' )::float
# MAGIC       / 100.00 as cad_cred
# MAGIC     from
# MAGIC       bronze.invoicing_credits
# MAGIC     group by
# MAGIC       relay_reference_number
# MAGIC   ),
# MAGIC   customer_money_final as (
# MAGIC     select distinct
# MAGIC       loadnum,
# MAGIC       case
# MAGIC         when
# MAGIC           sum(amount)
# MAGIC             filter ( where cust_currency_type = 'USD' ) is null
# MAGIC         then
# MAGIC           0
# MAGIC         else
# MAGIC           sum(amount)
# MAGIC             filter ( where cust_currency_type = 'USD' )
# MAGIC       end as total_usd,
# MAGIC       case
# MAGIC         when
# MAGIC           sum(amount)
# MAGIC             filter ( where cust_currency_type = 'CAD' ) is null
# MAGIC         then
# MAGIC           0
# MAGIC         else
# MAGIC           sum(amount)
# MAGIC             filter ( where cust_currency_type = 'CAD' )
# MAGIC       end as total_cad,
# MAGIC       case
# MAGIC         when
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'USD'
# MAGIC               and charge_code = 'linehaul'
# MAGIC
# MAGIC             ) is null
# MAGIC         then
# MAGIC           0
# MAGIC         else
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'USD'
# MAGIC               and charge_code = 'linehaul'
# MAGIC
# MAGIC             )
# MAGIC       end as usd_lhc,
# MAGIC       case
# MAGIC         when
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'USD'
# MAGIC               and charge_code = 'fuel_surcharge'
# MAGIC
# MAGIC             ) is null
# MAGIC         then
# MAGIC           0
# MAGIC         else
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'USD'
# MAGIC               and charge_code = 'fuel_surcharge'
# MAGIC
# MAGIC             )
# MAGIC       end as usd_fsc,
# MAGIC       case
# MAGIC         when
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'USD'
# MAGIC               and charge_code not in ('linehaul', 'fuel_surcharge')
# MAGIC
# MAGIC             ) is null
# MAGIC         then
# MAGIC           0
# MAGIC         else
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'USD'
# MAGIC               and charge_code not in ('linehaul', 'fuel_surcharge')
# MAGIC
# MAGIC             )
# MAGIC       end as usd_acc,
# MAGIC       case
# MAGIC         when
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'CAD'
# MAGIC               and charge_code = 'linehaul'
# MAGIC
# MAGIC             ) is null
# MAGIC         then
# MAGIC           0
# MAGIC         else
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'CAD'
# MAGIC               and charge_code = 'linehaul'
# MAGIC
# MAGIC             )
# MAGIC       end as cad_lhc,
# MAGIC       case
# MAGIC         when
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'CAD'
# MAGIC               and charge_code = 'fuel_surcharge'
# MAGIC
# MAGIC             ) is null
# MAGIC         then
# MAGIC           0
# MAGIC         else
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'CAD'
# MAGIC               and charge_code = 'fuel_surcharge'
# MAGIC
# MAGIC             )
# MAGIC       end as cad_fsc,
# MAGIC       case
# MAGIC         when
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'CAD'
# MAGIC               and charge_code not in ('linehaul', 'fuel_surcharge')
# MAGIC
# MAGIC             ) is null
# MAGIC         then
# MAGIC           0
# MAGIC         else
# MAGIC           sum(amount)
# MAGIC             filter (
# MAGIC
# MAGIC               where 1 = 1
# MAGIC               and cust_currency_type = 'CAD'
# MAGIC               and charge_code not in ('linehaul', 'fuel_surcharge')
# MAGIC
# MAGIC             )
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
# MAGIC         left join invoicing_credits
# MAGIC           on loadnum = invoicing_credits.relay_reference_number
# MAGIC     group by
# MAGIC       loadnum,
# MAGIC       usd_cred,
# MAGIC       cad_cred
# MAGIC   ), 
# MAGIC   
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
# MAGIC       p1.pickup_date :: date as use_date,
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
# MAGIC 	  case
# MAGIC         when
# MAGIC           accessorial1 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate1 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate1::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial2 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate2 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate2::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial3 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate3 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate3::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial4 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate4 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate4::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial5 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate5 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate5::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial6 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate6 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate6::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial7 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate7 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate7::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial8 like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate8 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate8::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end as customer_fuel,
# MAGIC       case
# MAGIC         when
# MAGIC           accessorial1 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate1 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate1::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial2 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate2 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate2::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial3 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate3 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate3::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial4 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate4 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate4::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial5 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate5 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate5::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial6 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate6 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate6::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial7 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate7 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate7::numeric
# MAGIC             else 0.00
# MAGIC           end
# MAGIC         else 0.00
# MAGIC       end
# MAGIC       + case
# MAGIC         when
# MAGIC           accessorial8 not like 'FUE%'
# MAGIC         then
# MAGIC           case
# MAGIC             when customer_accessorial_rate8 like '%:%' then 0.00
# MAGIC             when
# MAGIC               customer_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC             then
# MAGIC               customer_accessorial_rate8::numeric
# MAGIC             else 0.00
# MAGIC           end
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
# MAGIC 	  CASE
# MAGIC        WHEN p2.value REGEXP '^-?[0-9]+\\.?[0-9]*$' THEN CAST(p2.value AS DECIMAL(10, 2))
# MAGIC         ELSE CAST(0.00 AS DECIMAL(10, 2))
# MAGIC        END AS cargo_value,
# MAGIC       p2.trailer_number as Trailer_Num,
# MAGIC       mr.Market_Buy_Rate,
# MAGIC       sp.quoted_price as Spot_Revenue,
# MAGIC       sp.margin as Spot_Margin
# MAGIC     from
# MAGIC       bronze.projection_load_1 p1
# MAGIC       Left Join bronze.projection_load_2 p2 on p1.id = p2.id
# MAGIC       left join aljex_spot_loads sp on p1.id = sp.id ---join analytics.financial_calendar on p.pickup_date::date = financial_calendar.date::date
# MAGIC       left join pricing_nfi_load_predictions mr on p1.id = mr.MR_id
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
# MAGIC       left join bronze.canada_carriers on p1.carrier_id :: string = canada_carriers.canada_dot :: string
# MAGIC       left join bronze.customer_lookup on left(p2.shipper, 32) = left(customer_lookup.aljex_customer_name, 32)
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
# MAGIC   ),
# MAGIC   
# MAGIC  ----------------------------------------------------------------- System Union Code ----------------------------------------------------------
# MAGIC  
# MAGIC   System_Union as (
# MAGIC     Select   -------- Relay ----------------------------------------------------------Relay----------------------- Changing this now ---------------------
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
# MAGIC 	  (
# MAGIC         customer_money_final.usd_fsc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_fsc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC        as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_acc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC         as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_lhc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC        as customer_lhc,
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
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC 	  left join customer_money_final
# MAGIC           on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC 		  left join bronze.canada_conversions
# MAGIC           on use_date::date = canada_conversions.ship_date::date
# MAGIC 		      join average_conversions
# MAGIC           on 1 = 1 
# MAGIC       left join invoice_details on use_relay_loads.relay_load = invoice_details.relay_reference_number
# MAGIC       left join analytics.Dim_date dim_date on dim_date.calendar_date = use_relay_loads.use_date
# MAGIC       left join gold.fact_awards_summary d on use_relay_loads.relay_load = d.Load_ID  and d.is_deleted = '0'
# MAGIC       left join carrier_charges on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC       and combined_load = 'NO' --UNION
# MAGIC 	  
# MAGIC 	
# MAGIC     Union
# MAGIC 	
# MAGIC
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
# MAGIC 	  (
# MAGIC         customer_money_final.usd_fsc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_fsc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC        as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_acc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC       as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_lhc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC        as customer_lhc,
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
# MAGIC       left join bronze.big_export_projection e on use_relay_loads.relay_load :: float = e.load_number :: float
# MAGIC       left join total_cust_rate on use_relay_loads.relay_load :: float = total_cust_rate.relay_reference_number :: float
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC 	  left join customer_money_final
# MAGIC           on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC 		  left join bronze.canada_conversions
# MAGIC           on use_date::date = canada_conversions.ship_date::date
# MAGIC 		      join average_conversions
# MAGIC           on 1 = 1 
# MAGIC       left join invoice_details on use_relay_loads.relay_load = invoice_details.relay_reference_number
# MAGIC       left join analytics.Dim_date dim_date on dim_date.calendar_date = use_relay_loads.use_date
# MAGIC       left join gold.fact_awards_summary d on use_relay_loads.relay_load = d.Load_ID and d.is_deleted = '0'
# MAGIC       left join carrier_charges on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter = 'ltl'
# MAGIC       and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and e.customer_id not in ('hain', 'roland', 'deb')
# MAGIC 	  
# MAGIC 	  
# MAGIC     Union
# MAGIC 	
# MAGIC 	
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
# MAGIC 	   (
# MAGIC         customer_money_final.usd_fsc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_fsc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC       as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_acc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC       as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_lhc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC       as customer_lhc,
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
# MAGIC       left join bronze.big_export_projection e on use_relay_loads.relay_load :: float = e.load_number :: float
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC 	   left join customer_money_final
# MAGIC           on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC         left join bronze.canada_conversions
# MAGIC           on use_date::date = canada_conversions.ship_date::date
# MAGIC         join average_conversions
# MAGIC           on 1 = 1 
# MAGIC       left join invoice_details on use_relay_loads.relay_load = invoice_details.relay_reference_number
# MAGIC       left join analytics.Dim_date dim_date on dim_date.calendar_date = use_relay_loads.use_date
# MAGIC       left join gold.fact_awards_summary d on use_relay_loads.relay_load = d.Load_ID  and d.is_deleted = '0'
# MAGIC       left join carrier_charges on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and use_relay_loads.Mode_Filter = 'ltl- (hain,deb,roland)'
# MAGIC       and e.dispatch_status != 'Cancelled'
# MAGIC       and e.carrier_name is not null
# MAGIC       and e.customer_id in ('hain', 'roland', 'deb') --UNION
# MAGIC 	  
# MAGIC 	  
# MAGIC     Union                              ------------------------- Need to take from here ------------------------
# MAGIC 	
# MAGIC 	
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
# MAGIC 	  (
# MAGIC         customer_money_final.usd_fsc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_fsc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC       as customer_fuel,
# MAGIC       (
# MAGIC         customer_money_final.usd_acc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_acc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC       as customer_acc,
# MAGIC       (
# MAGIC         customer_money_final.usd_lhc::float
# MAGIC         + (
# MAGIC           customer_money_final.cad_lhc
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC         )
# MAGIC       )
# MAGIC        as customer_lhc,
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
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date
# MAGIC       left join invoiced_amts on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC 	  left join customer_money_final
# MAGIC           on use_relay_loads.relay_load = customer_money_final.loadnum
# MAGIC         left join bronze.canada_conversions
# MAGIC           on use_date::date = canada_conversions.ship_date::date
# MAGIC         join average_conversions
# MAGIC           on 1 = 1 
# MAGIC       left join invoice_details on use_relay_loads.relay_load = invoice_details.relay_reference_number
# MAGIC       left join analytics.Dim_date dim_date on dim_date.calendar_date = use_relay_loads.use_date
# MAGIC       left join gold.fact_awards_summary d on use_relay_loads.relay_load = d.Load_ID  and d.is_deleted = '0'
# MAGIC       left join carrier_charges on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
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
# MAGIC           Pickup_Appointment_Date DESC
# MAGIC       ) AS rank
# MAGIC     from
# MAGIC       system_union
# MAGIC   ),
# MAGIC
# MAGIC
# MAGIC -- Select * from Ordered 
# MAGIC
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
# MAGIC       pickup_datetime,
# MAGIC       ot_pu,
# MAGIC       Delivery_In,
# MAGIC       Delivery_out,
# MAGIC       Delivery_Datetime,
# MAGIC       ot_del,
# MAGIC       invoiced_Date,
# MAGIC       Booking_type,
# MAGIC       Load_flag,
# MAGIC       DOT_Num,
# MAGIC       Carrier,
# MAGIC       Miles,
# MAGIC       Revenue,
# MAGIC       Expense,
# MAGIC       Margin,
# MAGIC       Lead_Time,
# MAGIC       invoiceable_amt,
# MAGIC       amt_already_invoiced,
# MAGIC       outstanindg_amt,
# MAGIC       Linehaul,
# MAGIC       Fuel_Surcharge,
# MAGIC       Other_Fess,
# MAGIC 	  customer_fuel,
# MAGIC 	  customer_acc,
# MAGIC 	  customer_lhc,
# MAGIC       Trailer_Num,
# MAGIC       Cargo_Value,
# MAGIC 	  case when 
# MAGIC 	  Cargo_Value :: float / 100 > 100000 then 'YES' else 'NO' end as High_Value,
# MAGIC       Weight,
# MAGIC       max_buy,
# MAGIC       market_buy_rate,
# MAGIC       Spot_Revenue,
# MAGIC       Spot_Margin,
# MAGIC       Is_Awarded,
# MAGIC       TMS_System
# MAGIC     from
# MAGIC       Ordered
# MAGIC     where
# MAGIC       rank = 1
# MAGIC   	  
# MAGIC     union
# MAGIC 	
# MAGIC 	
# MAGIC     select
# MAGIC       distinct -- Shipper  ----- Need to change from here.
# MAGIC       id  as relay_ref_num,
# MAGIC       --0.5 as Load_Counter,
# MAGIC       use_date,
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
# MAGIC       coalesce(
# MAGIC         pickup_in :: TIMESTAMP,
# MAGIC         Pickup_Out :: TIMESTAMP
# MAGIC       ) as pickup_datetime,
# MAGIC       case
# MAGIC         when pickup_datetime :: date > COALESCE(Pickup_Appointment_Date :: date, use_date :: date) then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_pu,
# MAGIC       Drop_Start as Delivery_In,
# MAGIC       Drop_End as Delivery_Out,
# MAGIC       coalesce(
# MAGIC         Delivery_In :: TIMESTAMP,
# MAGIC         Delivery_Out :: TIMESTAMP
# MAGIC       ) as Delivery_Datetime,
# MAGIC       case
# MAGIC         when delivery_datetime::date > aljex_data.delivery_date :: date then 'late'
# MAGIC         else 'ontime'
# MAGIC       end as ot_del,
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
# MAGIC 	  (aljex_data.customer_fuel)
# MAGIC       as customer_fuel,
# MAGIC       (aljex_data.customer_acc) as customer_acc,
# MAGIC       (
# MAGIC         aljex_data.invoice_total::float - aljex_data.customer_fuel::float
# MAGIC         - aljex_data.customer_acc::float
# MAGIC       )
# MAGIC       as customer_lhc,
# MAGIC       aljex_data.Trailer_Num,
# MAGIC       Cargo_Value,
# MAGIC 	  case when Cargo_Value > 100000 Then 'YES' else 'NO' end as High_Value,
# MAGIC       aljex_data.Weight,
# MAGIC       null as max_buy,
# MAGIC       aljex_data.Market_Buy_Rate,
# MAGIC       aljex_data.Spot_Revenue,
# MAGIC       aljex_data.Spot_Margin,
# MAGIC       d.Is_Awarded,
# MAGIC       'ALJEX' as tms
# MAGIC     from
# MAGIC       aljex_data
# MAGIC       left join bronze.date_range_two on use_date :: date = date_range_two.date_date :: date --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC       left join gold.fact_awards_summary d on aljex_data.id = d.Load_ID  and d.is_deleted = '0'
# MAGIC       left join analytics.Dim_date dim_date on dim_date.calendar_date = aljex_data.use_date
# MAGIC       left join invoice_details on aljex_data.id = invoice_details.relay_reference_number
# MAGIC     
# MAGIC
# MAGIC     union
# MAGIC
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
# MAGIC 	  transfix_data.cust_lhc,
# MAGIC 	  transfix_data.cust_fuel,
# MAGIC 	  transfix_data.cust_acc,
# MAGIC       Null as Trailer_Num,
# MAGIC       null as Cargo_Value,
# MAGIC 	  null as High_Value,
# MAGIC       transfix_data.Weight,
# MAGIC       Null as max_buy,
# MAGIC       null as Market_Buy_Rate,
# MAGIC       null as Spot_Revenue,
# MAGIC       null as Spot_Margin,
# MAGIC       null as Is_Awarded,
# MAGIC       'Transfix' as tms
# MAGIC     from
# MAGIC       bronze.transfix_data
# MAGIC       join analytics.dim_date on coalesce(
# MAGIC         arrive_pu_date :: date,
# MAGIC         depart_pu_date :: date,
# MAGIC         pu_appt :: date
# MAGIC       ) :: date = analytics.dim_date.calendar_date :: date
# MAGIC       left join bronze.date_range_two on analytics.dim_date.calendar_date :: date = date_range_two.date_date :: date
# MAGIC   ),
# MAGIC
# MAGIC         -- Select * from final
# MAGIC
# MAGIC   tender as (
# MAGIC     SELECT
# MAGIC       tender_reference_numbers_projection.relay_reference_number as id,
# MAGIC       tenderer,
# MAGIC       tendering_acceptance.accepted_at,
# MAGIC       integration_tender_mapped_projection_v2.tendered_at
# MAGIC     FROM
# MAGIC       bronze.tender_reference_numbers_projection
# MAGIC       LEFT JOIN bronze.tendering_acceptance ON tender_reference_numbers_projection.tender_id = tendering_acceptance.tender_id
# MAGIC       LEFT JOIN bronze.integration_tender_mapped_projection_v2 ON tendering_acceptance.shipment_id = integration_tender_mapped_projection_v2.shipment_id
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
# MAGIC   )
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     final
# MAGIC     left join tender_final on final.load_number = tender_final.id)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table  analytics.Am_spot_Temp;
# MAGIC INSERT INTO analytics.Am_spot_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Am_spot

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Am_spot;
# MAGIC
# MAGIC INSERT INTO analytics.Am_spot
# MAGIC SELECT *
# MAGIC FROM analytics.Am_spot_Temp
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Touches Table for AM Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table analytics.Am_Dashboard_Touches
# MAGIC as
# MAGIC with Temp as 
# MAGIC (
# MAGIC     select 
# MAGIC
# MAGIC touch_id as Touches_ID,
# MAGIC  Touch_type as Touches_Type,
# MAGIC   full_name as Employee_Name,
# MAGIC   EMP.Email  as Email,
# MAGIC   
# MAGIC     w.Weight * 1 as Touches_Score,
# MAGIC      date as Touch_Timestamp,
# MAGIC
# MAGIC  --case when touch_type like 'Aljex%' then 'Aljex' else 'Relay' end as Source_System, 
# MAGIC  case when touch_type like 'Aljex%' then 1 else 2 end as Sourcesystem_id
# MAGIC
# MAGIC from Bronze.Load_Touch as LT
# MAGIC
# MAGIC Left Join
# MAGIC
# MAGIC analytics.fact_employee_am as EMP
# MAGIC
# MAGIC on LT.full_name = EMP.Employee_Name     --- all the TMS name are matched with the Full name in load_Touch
# MAGIC
# MAGIC left join 
# MAGIC
# MAGIC analytics.tms_events_weightage as W
# MAGIC
# MAGIC on LT.Touch_type  = W.Events
# MAGIC )
# MAGIC
# MAGIC ,
# MAGIC temp2 as 
# MAGIC (
# MAGIC select touches_ID,Touches_Type, Employee_Name, Email, Touches_score, Touch_Timestamp,sourcesystem_id from gold.fact_touches where sourcesystem_id not in (1,2,6)
# MAGIC )
# MAGIC ,
# MAGIC
# MAGIC  selected_data_Public_Private AS (
# MAGIC SELECT 
# MAGIC         REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC         c.Channel_Name,
# MAGIC         h.Conversation_Type AS Channel_type,
# MAGIC         h.Channel_ID,
# MAGIC         h.Actual_Text,
# MAGIC         TRY_CAST(h.DW_Timestamp AS Date) AS DT1,
# MAGIC         CASE
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN MINUTE(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC     FROM silver.silver_slack_conversations_history_new AS h
# MAGIC     LEFT JOIN silver.silver_slack_channels_new AS c ON h.Channel_ID = c.Channel_ID
# MAGIC     LEFT JOIN silver.silver_slack_users_new AS u ON h.User_ID = u.User_ID
# MAGIC )
# MAGIC
# MAGIC ,Selected_Data_DM As (
# MAGIC SELECT 
# MAGIC     REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC     c.Channel_Name,
# MAGIC     DM.Conversation_Type AS Channel_type,
# MAGIC     DM.Channel_ID,
# MAGIC     DM.Actual_Text AS DMText,
# MAGIC     TRY_CAST(DM.DW_Timestamp AS Date) AS DT1,
# MAGIC     CASE
# MAGIC         WHEN MINUTE(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC         WHEN MINUTE(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC         WHEN MINUTE(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC         WHEN MINUTE(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC     END AS FifteenMinGroup
# MAGIC FROM 
# MAGIC     silver.silver_slack_conversations_history_dm DM
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_channels_new AS c ON DM.Channel_ID = c.Channel_ID
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_users_new AS u ON DM.User_ID = u.User_ID
# MAGIC )
# MAGIC
# MAGIC ,Selected_Data_Group As 
# MAGIC (
# MAGIC SELECT 
# MAGIC     REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC     c.Channel_Name,
# MAGIC     GRP.Conversation_Type AS Channel_type,
# MAGIC     GRP.Channel_ID,
# MAGIC     GRP.Actual_Text AS GrpText,
# MAGIC     TRY_CAST(GRP.DW_Timestamp AS Date) AS DT1,
# MAGIC     CASE
# MAGIC         WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC         WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC         WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC         WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC     END AS FifteenMinGroup
# MAGIC FROM 
# MAGIC     silver.silver_slack_conversations_history_groups GRP
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_channels_new AS c ON GRP.Channel_ID = c.Channel_ID
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_users_new AS u ON GRP.User_ID = u.User_ID
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC     -- Aggregate the dataframe based on User_ID, Channel_name, Channel_type, and datetime (every 15 mins)
# MAGIC     -- Calculate the count of action ID
# MAGIC     ,aggregated_data AS (
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         Actual_Text
# MAGIC     FROM selected_data_Public_Private
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, Actual_Text, DT1, FifteenMinGroup
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         DMText AS Actual_Text
# MAGIC     FROM Selected_Data_DM
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, DMText, DT1, FifteenMinGroup
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT 
# MAGIC         User_ID,
# MAGIC         Channel_name,
# MAGIC         Channel_ID,
# MAGIC         Channel_type,
# MAGIC         DT1,
# MAGIC         FifteenMinGroup,
# MAGIC         CAST(1 AS INT) AS Touches_Score,
# MAGIC         GrpText AS Actual_Text
# MAGIC     FROM Selected_Data_Group
# MAGIC     GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, GrpText, DT1, FifteenMinGroup
# MAGIC ),
# MAGIC 	
# MAGIC slack_meassages as (
# MAGIC     -- Store the result in the DataFrame DF_Touches_Slack
# MAGIC     SELECT
# MAGIC         
# MAGIC         CONCAT_WS(User_ID, Channel_name, Channel_type, DT1,FifteenMinGroup) AS Touches_id,
# MAGIC         'Slack - Activities' AS Touch_Type,
# MAGIC         Null as full_name,
# MAGIC         User_ID as Email,
# MAGIC         Touches_Score,
# MAGIC         DT1 AS Date,
# MAGIC         CAST(6 AS INT) AS Source_system_id,
# MAGIC 		CASE WHEN Channel_type = 'Public' THEN Channel_ID END AS Publicchannel,
# MAGIC 		CASE WHEN Channel_type = 'Private' THEN Channel_ID END AS Privatechannel,
# MAGIC 		CASE WHEN Channel_type = 'Direct Message' THEN Channel_ID END AS DirectMessages,
# MAGIC 		CASE WHEN Channel_type = 'Group Message' THEN Channel_ID END AS GroupMessages,
# MAGIC 		CASE WHEN Channel_type = 'Public' THEN Actual_Text END AS Publicchannelconversation,
# MAGIC 		CASE WHEN Channel_type = 'Private' THEN Actual_Text END AS Privatechannelconversation,
# MAGIC 		CASE WHEN Channel_type = 'Direct Message' THEN Actual_Text END AS DMConversations,
# MAGIC 		CASE WHEN Channel_type = 'Group Message' THEN Actual_Text END AS GroupConverstation
# MAGIC 		
# MAGIC     FROM aggregated_data)
# MAGIC
# MAGIC ----Select * from slack_meassages  where Date >= '2024-07-28' and Date <= '2024-08-24'
# MAGIC
# MAGIC
# MAGIC
# MAGIC --- Reactions
# MAGIC
# MAGIC ,
# MAGIC  reactions_selected_data AS (select REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,c.Channel_Name,
# MAGIC h.Conversation_Type as Channel_type,	
# MAGIC h.Channel_ID,
# MAGIC h.Reaction_Name,
# MAGIC --DM.Reactions as DMReaction,
# MAGIC
# MAGIC             try_cast(h.DW_Timestamp AS Date) as DT1,
# MAGIC         CASE
# MAGIC             WHEN minute(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN minute(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN minute(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN minute(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC from  silver.silver_slack_reactions_new as h
# MAGIC left join  silver.silver_slack_channels_new as c
# MAGIC on h.Channel_ID = c.Channel_ID ---and c.Is_Private = false
# MAGIC left join silver.silver_slack_users_new as u
# MAGIC on h.User_ID = u.User_ID
# MAGIC ---where h.reply_count is null
# MAGIC
# MAGIC     )
# MAGIC
# MAGIC , reactions_selected_data_DM AS (
# MAGIC select REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC c.Channel_Name,
# MAGIC DM.Conversation_Type as Channel_type,	
# MAGIC DM.Channel_ID,
# MAGIC --h.Reaction_Name,
# MAGIC DM.Reactions as Reaction_Name,
# MAGIC
# MAGIC             try_cast(DM.DW_Timestamp AS Date) as DT1,
# MAGIC         CASE
# MAGIC             WHEN minute(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN minute(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN minute(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN minute(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC
# MAGIC FROM 
# MAGIC     silver.silver_slack_conversations_history_dm DM
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_channels_new AS c ON DM.Channel_ID = c.Channel_ID
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_users_new AS u ON DM.User_ID = u.User_ID
# MAGIC where Is_Reactions = '1'
# MAGIC ---where h.reply_count is null
# MAGIC
# MAGIC     )
# MAGIC
# MAGIC ,reactions_selected_data_Group as (
# MAGIC select REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
# MAGIC c.Channel_Name,
# MAGIC GRP.Conversation_Type as Channel_type,	
# MAGIC GRP.Channel_ID,
# MAGIC --h.Reaction_Name,
# MAGIC GRP.Reactions as Reaction_Name,
# MAGIC
# MAGIC             try_cast(GRP.DW_Timestamp AS Date) as DT1,
# MAGIC         CASE
# MAGIC             WHEN minute(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
# MAGIC             WHEN minute(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
# MAGIC             WHEN minute(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
# MAGIC             WHEN minute(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
# MAGIC         END AS FifteenMinGroup
# MAGIC
# MAGIC FROM 
# MAGIC     silver.silver_slack_conversations_history_groups GRP
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_channels_new AS c ON GRP.Channel_ID = c.Channel_ID
# MAGIC LEFT JOIN 
# MAGIC     silver.silver_slack_users_new AS u ON GRP.User_ID = u.User_ID
# MAGIC where Is_Reactions = '1'
# MAGIC )
# MAGIC
# MAGIC     -- Aggregate the dataframe based on User_ID, Channel_name, Channel_type, and datetime (every 15 mins)
# MAGIC     -- Calculate the count of action ID
# MAGIC     , reaction_aggregated_data AS (
# MAGIC         SELECT
# MAGIC             User_ID,
# MAGIC 			Channel_ID,
# MAGIC             Channel_name,
# MAGIC             Channel_type,
# MAGIC             DT1,
# MAGIC             FifteenMinGroup,
# MAGIC             CAST(1 AS INT) AS Touches_Score,
# MAGIC 			Reaction_Name
# MAGIC             --DMReaction
# MAGIC         FROM reactions_selected_data
# MAGIC         GROUP BY User_ID,Channel_ID, Channel_name, Channel_type,Reaction_Name, DT1,FifteenMinGroup
# MAGIC 		
# MAGIC 		Union 
# MAGIC 		
# MAGIC 		 SELECT
# MAGIC             User_ID,
# MAGIC 			Channel_ID,
# MAGIC             Channel_name,
# MAGIC             Channel_type,
# MAGIC             DT1,
# MAGIC             FifteenMinGroup,
# MAGIC             CAST(1 AS INT) AS Touches_Score,
# MAGIC 			Reaction_Name  
# MAGIC         FROM reactions_selected_data_DM
# MAGIC         GROUP BY User_ID,Channel_ID, Channel_name, Channel_type,Reaction_Name, DT1,FifteenMinGroup
# MAGIC 		
# MAGIC 		UNION
# MAGIC 		SELECT
# MAGIC             User_ID,
# MAGIC 			Channel_ID,
# MAGIC             Channel_name,
# MAGIC             Channel_type,
# MAGIC             DT1,
# MAGIC             FifteenMinGroup,
# MAGIC             CAST(1 AS INT) AS Touches_Score,
# MAGIC 			Reaction_Name           
# MAGIC         FROM reactions_selected_data_Group
# MAGIC         GROUP BY User_ID,Channel_ID, Channel_name, Channel_type,Reaction_Name, DT1,FifteenMinGroup
# MAGIC 			
# MAGIC     )
# MAGIC 	
# MAGIC --Select * from reaction_aggregated_data limit 100
# MAGIC ,
# MAGIC Slack_reactions as (
# MAGIC     -- Store the result in the DataFrame DF_Touches_Slack
# MAGIC     SELECT
# MAGIC         
# MAGIC         CONCAT_WS(User_ID, Channel_name, Channel_type, DT1,FifteenMinGroup) AS Touches_id,
# MAGIC         'Slack - Reactions' AS Touch_Type,
# MAGIC         Null as full_name,
# MAGIC         User_ID as Email,
# MAGIC         Touches_Score,
# MAGIC         DT1 AS Date,
# MAGIC         CAST(6 AS INT) AS Source_system_id,
# MAGIC 		CASE WHEN Channel_type = 'Public' THEN Channel_ID END AS Publicchannel,
# MAGIC 		CASE WHEN Channel_type = 'Private' THEN Channel_ID END AS Privatechannel,
# MAGIC 		CASE WHEN Channel_type = 'Direct Message' THEN Channel_ID END AS DirectMessages,
# MAGIC 		CASE WHEN Channel_type = 'Group Message' THEN Channel_ID END AS GroupMessages,
# MAGIC 		CASE WHEN Channel_type = 'Public' THEN Reaction_Name END AS Publicchannelconversation,
# MAGIC 		CASE WHEN Channel_type = 'Private' THEN Reaction_Name END AS Privatechannelconversation,
# MAGIC         CASE WHEN Channel_type = 'Direct Message' THEN Reaction_Name END AS DMConversations,
# MAGIC 		CASE WHEN Channel_type = 'Group Message' THEN Reaction_Name END AS GroupConverstation		
# MAGIC     FROM reaction_aggregated_data)
# MAGIC
# MAGIC ,
# MAGIC
# MAGIC slack_consolidated as (
# MAGIC
# MAGIC     select * from Slack_reactions
# MAGIC
# MAGIC     union 
# MAGIC
# MAGIC select * from slack_meassages
# MAGIC )
# MAGIC ,
# MAGIC hubspot_calls_df( select 
# MAGIC hs_activity_type,
# MAGIC hs_object_id,
# MAGIC hubspot_owner_id,
# MAGIC hs_createdate,
# MAGIC from_utc_timestamp(hs_createdate, 'CST') AS hs_createdate_cst,
# MAGIC SourcesystemID
# MAGIC from bronze.hubspot_calls where sourcesystemid = '5' and hs_activity_type = 'Introduce NFI'),
# MAGIC
# MAGIC hubspot_owners_df as (
# MAGIC select id,email from bronze.hubspot_owners ),
# MAGIC
# MAGIC Hubspot_call as (select distinct
# MAGIC 'Hubspot-Introcalls' as Touches_Type,
# MAGIC CONCAT_WS(hs_object_id,email,'Hubspot-Introcalls',hs_createdate_cst) AS Touches_id,
# MAGIC email AS email,
# MAGIC '' as Employee_Name,
# MAGIC 1 as Touches_score,
# MAGIC hs_createdate_cst,
# MAGIC CAST(hs_createdate_cst AS TIMESTAMP) AS Touch_Timestamp,
# MAGIC SourcesystemID AS sourcesystem_id
# MAGIC FROM hubspot_calls_df n
# MAGIC JOIN Hubspot_Owners_DF o ON n.hubspot_owner_id = o.id),
# MAGIC
# MAGIC  employee as (
# MAGIC select 
# MAGIC Role_Leader_Ops,
# MAGIC Employee_Name,
# MAGIC Title,
# MAGIC UltiPro_Title,
# MAGIC Email,
# MAGIC Remote_Office,
# MAGIC Assigned_Office,
# MAGIC LSS_Y_N,
# MAGIC Customer_Office
# MAGIC Role
# MAGIC  from analytics.fact_employee_am ),
# MAGIC
# MAGIC hubspot_calls_df1( select 
# MAGIC hs_activity_type,
# MAGIC hs_object_id,
# MAGIC hubspot_owner_id,
# MAGIC hs_createdate,
# MAGIC from_utc_timestamp(hs_createdate, 'CST') AS hs_createdate_cst,
# MAGIC SourcesystemID
# MAGIC from bronze.hubspot_calls where sourcesystemid = '4' and hs_activity_type = 'External Business Review Meeting'),
# MAGIC
# MAGIC hubspot_owners_df1 as (
# MAGIC select id,email from bronze.hubspot_owners ),
# MAGIC
# MAGIC Hubspot_business_call as (select distinct
# MAGIC 'External-Business-Review-Meeting' as Touches_Type,
# MAGIC CONCAT_WS(hs_object_id,email,'External-Business-Review-Meeting',hs_createdate_cst) AS Touches_id,
# MAGIC email AS email,
# MAGIC '' as Employee_Name,
# MAGIC 1 as Touches_score,
# MAGIC hs_createdate_cst,
# MAGIC CAST(hs_createdate_cst AS TIMESTAMP) AS Touch_Timestamp,
# MAGIC SourcesystemID AS sourcesystem_id
# MAGIC FROM hubspot_calls_df1 n
# MAGIC JOIN Hubspot_Owners_DF1 o ON n.hubspot_owner_id = o.id),
# MAGIC
# MAGIC final_hubspot_call as ( select touches_ID,Touches_Type, 
# MAGIC employee.Employee_Name, Hubspot_business_call.Email, Touches_score, Touch_Timestamp,sourcesystem_id 
# MAGIC from Hubspot_business_call join employee on lower(Hubspot_business_call.email) = lower(employee.Email) ),
# MAGIC
# MAGIC -- Select * from final_hubspot_call where Touch_Timestamp between '2025-03-30' and '2025-04-26'
# MAGIC
# MAGIC consolidate as (
# MAGIC select touches_ID,Touches_Type, Employee_Name, Email, Touches_score, cast(Touch_Timestamp as date) as Touch_Timestamp ,sourcesystem_id,Null as Publicchannel,Null as Privatechannel, Null as DirectMessages, 
# MAGIC Null as GroupMessages , Null as Publicchannelconversation,Null as Privatechannelconversation,
# MAGIC Null as DMConversations,
# MAGIC Null as GroupConverstation
# MAGIC from gold.fact_touches
# MAGIC
# MAGIC union
# MAGIC     SELECT 
# MAGIC         touches_ID,
# MAGIC         Touches_Type,
# MAGIC         Employee_Name,
# MAGIC         Email,
# MAGIC         Touches_score,
# MAGIC         CAST(Touch_Timestamp AS DATE) AS Touch_Timestamp,
# MAGIC         sourcesystem_id,
# MAGIC         NULL AS Publicchannel,
# MAGIC         NULL AS Privatechannel,
# MAGIC         NULL AS DirectMessages,
# MAGIC         NULL AS GroupMessages,
# MAGIC         NULL AS Publicchannelconversation,
# MAGIC         NULL AS Privatechannelconversation,
# MAGIC         NULL AS DMConversations,
# MAGIC         NULL AS GroupConverstation
# MAGIC     FROM 
# MAGIC         temp
# MAGIC
# MAGIC union 
# MAGIC select touches_ID,Touches_Type, Employee_Name, Email, Touches_score, cast(Touch_Timestamp as date) as Touch_Timestamp,sourcesystem_id,Null as Publicchannel,Null as Privatechannel, Null as DirectMessages, 
# MAGIC Null as GroupMessages , Null as Publicchannelconversation,Null as Privatechannelconversation,
# MAGIC Null as DMConversations,
# MAGIC Null as GroupConverstation
# MAGIC from Hubspot_call
# MAGIC union 
# MAGIC select touches_ID,Touches_Type,Employee_Name,Email, Touches_score, cast(Touch_Timestamp as date) as Touch_Timestamp,sourcesystem_id ,Null as Publicchannel,Null as Privatechannel, Null as DirectMessages, 
# MAGIC Null as GroupMessages , Null as Publicchannelconversation,Null as Privatechannelconversation,
# MAGIC Null as DMConversations,
# MAGIC Null as GroupConverstation
# MAGIC from final_hubspot_call
# MAGIC union
# MAGIC  
# MAGIC select * from slack_consolidated )
# MAGIC
# MAGIC -- Select * from consolidate where Touches_Type = 'External-Business-Review-Meeting' 
# MAGIC -- and Touch_Timestamp between '2025-03-30' and '2025-04-26'
# MAGIC
# MAGIC --Select * from slack_consolidated where DMConversations is not null;
# MAGIC ,
# MAGIC
# MAGIC  EmployeeQuartiles AS (
# MAGIC     SELECT
# MAGIC         lower(m.email) AS Email,
# MAGIC         m.role AS Role,
# MAGIC         period_year,
# MAGIC         financial_period_number,
# MAGIC         Calendar_Year,
# MAGIC         sum(Touches_Score) AS Touch_Score,
# MAGIC 		COUNT (distinct Publicchannel) as Publicchannel,
# MAGIC 		COUNT (distinct Privatechannel) as Privatechannel,
# MAGIC 		COUNT (distinct DirectMessages) as DirectMessages,
# MAGIC 		COUNT (distinct GroupMessages) as GroupMessages,
# MAGIC 		COUNT (Publicchannelconversation) as Publicchannel_conversation,
# MAGIC 		COUNT (Privatechannelconversation) as Privatechannel_conversation,
# MAGIC 		COUNT (DMConversations) as DMConversations,
# MAGIC 		COUNT (GroupConverstation) as GroupConverstation,
# MAGIC 		(COUNT(Publicchannelconversation) + COUNT(Privatechannelconversation) + COUNT (DMConversations) + COUNT (GroupConverstation) ) AS Total_conversation,
# MAGIC         NTILE(4) OVER (PARTITION BY m.role, period_year, financial_period_number, Calendar_Year ORDER BY sum(Touches_Score) DESC) AS Quartile_Number
# MAGIC     FROM 
# MAGIC         consolidate AS T
# MAGIC         LEFT JOIN `gold`.`lookup_sourcesystem` AS S ON T.Sourcesystem_ID = S.DW_Sourcesystem_ID
# MAGIC         INNER JOIN analytics.fact_employee_am AS m ON lower(T.email) = lower(m.Email)
# MAGIC         LEFT JOIN (
# MAGIC           SELECT calendar_date, period_year, financial_period_number, Calendar_Year 
# MAGIC           FROM `analytics`.`dim_date`
# MAGIC         ) AS Cal ON Touch_Timestamp  = Cal.calendar_date
# MAGIC     
# MAGIC        WHERE calendar_year BETWEEN EXTRACT(YEAR FROM CURRENT_DATE) - 2 AND EXTRACT(YEAR FROM CURRENT_DATE)
# MAGIC     GROUP BY 
# MAGIC         lower(m.email), period_year, financial_period_number, Calendar_Year, m.role,calendar_date,Touch_Timestamp
# MAGIC )
# MAGIC
# MAGIC ,
# MAGIC  TouchesSummary AS (
# MAGIC     -- The provided CTE to summarize touch data
# MAGIC     -- You can keep this CTE as is
# MAGIC     SELECT 
# MAGIC         lower(email) AS Email,
# MAGIC         Touches_Type,
# MAGIC         COALESCE(S.Sourcesystem_Name, 'Slack') AS Sourcesystem_Name,
# MAGIC         Sourcesystem_ID,
# MAGIC         CAST(Touch_Timestamp AS Date) AS Touch_Date,
# MAGIC         SUM(Touches_Score) AS Touch_Score,
# MAGIC         CASE 
# MAGIC             WHEN Touches_Type IN ('Aljex - Load Booked', 'Aljex - Marked Delivered', 'Aljex - Marked Driver_Dispatched', 'Aljex - Marked Loaded') THEN 'TMS - Aljex'
# MAGIC             WHEN Touches_Type IN ('Hubspot-Deals', 'Hubspot-Email', 'Hubspot-Meeting', 'Hubspot-Notes','Hubspot-Introcalls','External-Business-Review-Meeting') THEN 'Hubspot'
# MAGIC             WHEN Touches_Type IN ('Relay - Appt Scheduled', 'Relay - Assigned load', 'Relay - Bounced a Load', 'Relay - Detention Reported', 'Relay - Driver Dispatched', 'Relay - Driver Info Captured', 'Relay - Equipment Assigned', 'Relay - In Transit Update Captured', 'Relay - Load Booked', 'Relay - Load Cancelled', 'Relay - Manual Tender Created', 'Relay - Marked Arrived At Stop', 'Relay - Marked Delivered', 'Relay - Marked Loaded', 'Relay - Put Offer on a Load', 'Relay - Reserved a Load', 'Relay - Rolled a Load', 'Relay - Set a Max Buy', 'Relay - Stop Marked Delivered', 'Relay - Stop Recorded', 'Relay - Tender Accepted', 'Relay - Tracking Contact Info Captured', 'Relay - Tracking Note Captured', 'Relay - Tracking Stopped') THEN 'TMS - Relay'
# MAGIC             WHEN Touches_Type in ('Slack - Activities','Slack - Reactions')  THEN 'Slack'
# MAGIC             ELSE 'Other'
# MAGIC         END AS Event_Group,
# MAGIC         CASE 
# MAGIC             WHEN Touches_Type IN ('Aljex - Load Booked', 'Aljex - Marked Delivered', 'Aljex - Marked Driver_Dispatched', 'Aljex - Marked Loaded') THEN 'Operational'
# MAGIC             WHEN Touches_Type IN ('Hubspot-Deals', 'Hubspot-Email', 'Hubspot-Meeting', 'Hubspot-Notes','Hubspot-Introcalls','External-Business-Review-Meeting') THEN 'Hubspot'
# MAGIC             WHEN Touches_Type IN ('Relay - Appt Scheduled', 'Relay - Assigned load', 'Relay - Bounced a Load', 'Relay - Detention Reported', 'Relay - Driver Dispatched', 'Relay - Driver Info Captured', 'Relay - Equipment Assigned', 'Relay - In Transit Update Captured', 'Relay - Load Booked', 'Relay - Load Cancelled', 'Relay - Manual Tender Created', 'Relay - Marked Arrived At Stop', 'Relay - Marked Delivered', 'Relay - Marked Loaded', 'Relay - Put Offer on a Load', 'Relay - Reserved a Load', 'Relay - Rolled a Load', 'Relay - Set a Max Buy', 'Relay - Stop Marked Delivered', 'Relay - Stop Recorded', 'Relay - Tender Accepted', 'Relay - Tracking Contact Info Captured', 'Relay - Tracking Note Captured', 'Relay - Tracking Stopped') THEN 'Operational'
# MAGIC             WHEN Touches_Type in ('Slack - Activities','Slack - Reactions') THEN 'Slack'
# MAGIC             ELSE 'Other'
# MAGIC         END AS Event_Type
# MAGIC     FROM 
# MAGIC         consolidate AS T
# MAGIC         LEFT JOIN  `gold`.`lookup_sourcesystem` AS S ON T.Sourcesystem_ID = S.DW_Sourcesystem_ID
# MAGIC LEFT JOIN (
# MAGIC           SELECT calendar_date, period_year, financial_period_number, Calendar_Year 
# MAGIC           FROM `analytics`.`dim_date`
# MAGIC         ) AS Cal ON Touch_Timestamp  = Cal.calendar_date
# MAGIC     
# MAGIC        WHERE calendar_year BETWEEN EXTRACT(YEAR FROM CURRENT_DATE) - 2 AND EXTRACT(YEAR FROM CURRENT_DATE)
# MAGIC     GROUP BY 
# MAGIC         lower(email), Touches_Type, CAST(Touch_Timestamp AS Date), S.Sourcesystem_Name, Sourcesystem_ID
# MAGIC )
# MAGIC
# MAGIC -- Select * from TouchesSummary where Touch_Date between '2025-03-30' and '2025-04-26' and  
# MAGIC -- Touches_Type = 'External-Business-Review-Meeting'
# MAGIC
# MAGIC ,
# MAGIC TouchesAggregated AS (
# MAGIC     -- CTE to aggregate touch data according to specifications
# MAGIC     SELECT 
# MAGIC         D.Calendar_Year AS Calendar_Year,
# MAGIC         D.financial_period_number AS Period,
# MAGIC         CONCAT(D.financial_period_number, '-', D.Calendar_Year) AS Period_Year,
# MAGIC         SUM(Touch_Score) AS Total_Touch_Score,
# MAGIC 		Touch_Date,
# MAGIC         SUM(CASE WHEN event_type = 'Operational' THEN Touch_Score ELSE 0 END) AS Operational_Touches,
# MAGIC         SUM(CASE WHEN Event_Group = 'TMS - Relay' THEN Touch_Score ELSE 0 END) AS Relay_Touches,
# MAGIC         SUM(CASE WHEN Event_Group = 'TMS - Aljex' THEN Touch_Score ELSE 0 END) AS Aljex_Touches,
# MAGIC         SUM(CASE WHEN Event_Group = 'Hubspot' THEN Touch_Score ELSE 0 END) AS Hubspot_Touches,
# MAGIC         SUM(CASE WHEN Event_Group = 'Slack' THEN Touch_Score ELSE 0 END) AS Slack_Touches,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Hubspot-Email' THEN Touch_Score ELSE 0 END) AS Hubspot_Email_Touches,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Hubspot-Deals' THEN Touch_Score ELSE 0 END) AS Hubspot_Deals,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Hubspot-Notes' THEN Touch_Score ELSE 0 END) AS Hubspot_Notes,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Hubspot-Meeting' THEN Touch_Score ELSE 0 END) AS Hubspot_Meetings,
# MAGIC 		SUM(CASE WHEN Touches_Type = 'Hubspot-Introcalls' THEN Touch_Score ELSE 0 END) AS Hubspot_Introcalls,
# MAGIC 		SUM(CASE WHEN Touches_Type = 'External-Business-Review-Meeting' THEN Touch_Score ELSE 0 END) AS Hubspot_External_Business_Review_Meeting,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Slack - Activities' THEN Touch_Score ELSE 0 END) AS Slack_Message_Touches,
# MAGIC         SUM(CASE WHEN Touches_Type = 'Slack - Reactions' THEN Touch_Score ELSE 0 END) AS Slack_Reaction_Touches,
# MAGIC         Email AS Email_Address
# MAGIC
# MAGIC     FROM 
# MAGIC         TouchesSummary TS
# MAGIC         JOIN `analytics`.`dim_date` D ON TS.Touch_Date = D.calendar_date
# MAGIC     GROUP BY 
# MAGIC         D.Calendar_Year, D.financial_period_number, D.Calendar_Year, Email,Touch_Date
# MAGIC )
# MAGIC
# MAGIC -- Select *
# MAGIC -- from TouchesAggregated where Touch_Date between '2025-03-30' and '2025-04-26' 
# MAGIC -- and Hubspot_External_Business_Review_Meeting > '0'
# MAGIC
# MAGIC ,
# MAGIC RevenueSummary AS (
# MAGIC Select period_year, financial_period_number, Calendar_Year,emp.Email,(Sum(Revenue)/2) as Revenue,count(distinct loadnum) as Volume,((sum(Revenue)/2) - (sum(expense)/2) ) as Margin  from 
# MAGIC (
# MAGIC select 'Carrier' as Revenue_Type ,
# MAGIC CASE 
# MAGIC         WHEN Full_name IS NULL THEN booked_by 
# MAGIC         ELSE Full_name 
# MAGIC     END AS Full_name, l.Ship_date as shipdate , l.Revenue,l.load_id as loadnum, l.expense from analytics.Fact_Load_GenAI_NonSplit as l 
# MAGIC left join `bronze`.`aljex_user_report_listing` as u1 
# MAGIC on l.booked_by = u1.full_name
# MAGIC where dot_num is not null
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC select 'Shipper' as Shipper ,
# MAGIC CASE 
# MAGIC         WHEN Full_name IS NULL THEN l.Employee 
# MAGIC         ELSE Full_name 
# MAGIC     END AS Full_name, l.Ship_date as shipdate, l.Revenue,l.load_id as loadnum, l.expense from analytics.Fact_Load_GenAI_NonSplit as l 
# MAGIC left join `bronze`.`aljex_user_report_listing` as u1 
# MAGIC on l.Employee = u1.Full_name
# MAGIC where dot_num is not null and full_name is not null
# MAGIC )
# MAGIC as S
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC             SELECT calendar_date, period_year, financial_period_number, Calendar_Year 
# MAGIC             FROM `analytics`.`dim_date`
# MAGIC         ) AS Cal ON cast(S.Shipdate AS Date) = Cal.calendar_date
# MAGIC Left JOIN
# MAGIC analytics.fact_employee_am as Emp
# MAGIC on s.Full_name = emp.Employee_Name
# MAGIC where s.Shipdate BETWEEN DATEADD(year, -2, CURRENT_DATE()) AND CURRENT_DATE()
# MAGIC
# MAGIC group by
# MAGIC period_year, financial_period_number, Calendar_Year,emp.Email
# MAGIC
# MAGIC )
# MAGIC
# MAGIC ,
# MAGIC
# MAGIC templast as (
# MAGIC SELECT concat("Quartile - ",Quartile_Number) as Quartile_Name,Quartile_Number, A2.*,A3.Revenue,A3.margin, A3.Volume,emp2.*,Concat(emp2.Employee_Name,',',A2.period_year) as TMS_period,
# MAGIC Concat(emp2.Employee_Name,',',A2.period_year) as Employee_period,A1.Publicchannel,A1.Privatechannel,A1.DirectMessages,A1.GroupMessages,
# MAGIC A1.Publicchannel_conversation,A1.Privatechannel_conversation,A1.DMConversations, A1.GroupConverstation,
# MAGIC A1.Total_conversation
# MAGIC FROM 
# MAGIC     EmployeeQuartiles as A1
# MAGIC     
# MAGIC     left join 
# MAGIC     
# MAGIC     TouchesAggregated as A2
# MAGIC     
# MAGIC     on A1.Email  = a2.Email_Address and a1.period_year = a2.Period_Year
# MAGIC     
# MAGIC     left join 
# MAGIC
# MAGIC     RevenueSummary  as A3
# MAGIC     
# MAGIC     on A1.Email  = a3.Email and a1.period_year = a3.Period_Year
# MAGIC     
# MAGIC     left join analytics.fact_employee_am as Emp2
# MAGIC on A1.Email = lower(Emp2.Email)
# MAGIC
# MAGIC )
# MAGIC
# MAGIC Select *, concat_ws(Employee_Name,Touch_Date,Email) As AM_Touch_Key  from templast 
# MAGIC -- select * from EmployeeQuartiles
# MAGIC
# MAGIC

# COMMAND ----------

