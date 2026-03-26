# Databricks notebook source
# MAGIC %md
# MAGIC ## ViewToTable
# MAGIC * **Description:** To Convert a View into table for Super_Insights
# MAGIC * **Created Date:** 15/10/2024
# MAGIC * **Created By:** Uday
# MAGIC * **Modified Date:** 15/10/2024
# MAGIC * **Modified By:** Uday
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating View

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE SCHEMA bronze;
# MAGIC SET ansi_mode = false; 
# MAGIC create or replace view bronze.Fact_Load_GenAI_NonSplit as 
# MAGIC WITH new_customer_money_sp AS (
# MAGIC   SELECT DISTINCT
# MAGIC     m.relay_reference_number AS loadnum,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN m.charge_code = 'linehaul' THEN m.amount
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )
# MAGIC     / 100.0 AS linehaul_cust_money,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN m.charge_code = 'fuel_surcharge' THEN m.amount
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )
# MAGIC     / 100.0 AS fuel_cust_money,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN m.charge_code NOT IN ('fuel_surcharge', 'linehaul') THEN m.amount
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )
# MAGIC     / 100.0 AS acc_cust_money,
# MAGIC     SUM(m.amount) / 100.0 AS total_cust_amount,
# MAGIC     SUM(DISTINCT COALESCE(ic.total_amount, 0)) / 100.0 AS inv_cred_amt
# MAGIC   FROM
# MAGIC     bronze.moneying_billing_party_transaction AS m
# MAGIC       LEFT JOIN bronze.invoicing_credits AS ic
# MAGIC         ON CAST(m.relay_reference_number AS FLOAT) = CAST(ic.relay_reference_number AS FLOAT)
# MAGIC   WHERE
# MAGIC     m.`voided?` = 'false'
# MAGIC   GROUP BY
# MAGIC     m.relay_reference_number
# MAGIC ),
# MAGIC -- Select * from new_customer_money_sp
# MAGIC raw_data_one AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN customer_name = 'TJX Companies c/o Blueyonder' THEN 'IL'
# MAGIC       ELSE COALESCE(relay_users.office_id, aljex_user_report_listing.pnl_code)
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     COALESCE(master_customer_name, customer_name) AS mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at
# MAGIC   FROM
# MAGIC     bronze.spot_quotes
# MAGIC       JOIN analytics.dim_date
# MAGIC         ON CAST(spot_quotes.created_at AS DATE) = dim_date.Calendar_Date
# MAGIC       LEFT JOIN bronze.relay_users
# MAGIC         ON spot_quotes.created_by = relay_users.full_name
# MAGIC       LEFT JOIN bronze.aljex_user_report_listing
# MAGIC         ON spot_quotes.created_by = aljex_user_report_listing.full_name
# MAGIC       LEFT JOIN bronze.customer_lookup
# MAGIC         ON LEFT(customer_name, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC -- Select * from raw_data_one
# MAGIC raw_data AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC       WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC       ELSE rep_office
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at,
# MAGIC     rank() OVER (PARTITION BY Load_Number, status_str ORDER BY last_updated_at DESC) AS rank
# MAGIC   FROM
# MAGIC     raw_data_one
# MAGIC       LEFT JOIN bronze.days_to_pay_offices
# MAGIC         ON CASE
# MAGIC           WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC           WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC           ELSE rep_office
# MAGIC         END = days_to_pay_offices.office
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC -- Select * from raw_data
# MAGIC relay_spot_number AS (
# MAGIC   SELECT DISTINCT
# MAGIC     b.relay_reference_number,
# MAGIC     t.shipment_id,
# MAGIC     r.customer_name,
# MAGIC     master_customer_name,
# MAGIC     COALESCE(ncm.total_cust_amount, 0.0) - COALESCE(ncm.inv_cred_amt, 0.0) AS revenue,
# MAGIC     NULL AS column_null,
# MAGIC     rd.created_at,
# MAGIC     rd.quote_id,
# MAGIC     1 AS counter,
# MAGIC     rd.quoted_price,
# MAGIC     rd.margin,
# MAGIC     rd.created_by,
# MAGIC     rd.rep_office,
# MAGIC     rd.load_number,
# MAGIC     rd.shipper_ref_number,
# MAGIC     rd.status_str,
# MAGIC     rd.last_updated_at
# MAGIC   FROM
# MAGIC     bronze.booking_projection AS b
# MAGIC       LEFT JOIN bronze.customer_profile_projection r
# MAGIC         ON b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       LEFT JOIN bronze.customer_lookup AS cl
# MAGIC         ON LEFT(r.customer_name, 32) = LEFT(cl.aljex_customer_name, 32)
# MAGIC       LEFT JOIN bronze.tendering_acceptance AS t
# MAGIC         ON b.relay_reference_number = t.relay_reference_number
# MAGIC       JOIN raw_data AS rd
# MAGIC         ON CAST(b.relay_reference_number AS STRING) = rd.load_number
# MAGIC         AND master_customer_name = rd.mastername
# MAGIC         AND b.first_shipper_state = rd.shipper_state
# MAGIC         AND b.receiver_state = rd.receiver_state
# MAGIC       LEFT JOIN new_customer_money_sp AS ncm
# MAGIC         ON b.relay_reference_number = ncm.loadnum
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     and rd.status_str = 'WON'
# MAGIC     and rd.rank = 1
# MAGIC ),
# MAGIC -- Select * from relay_spot_number
# MAGIC aljex_spot_loads as (
# MAGIC   SELECT DISTINCT
# MAGIC     CAST(p.id AS FLOAT) AS id,
# MAGIC     p.pickup_reference_number,
# MAGIC     p1.shipper,
# MAGIC     COALESCE(cl.master_customer_name, p1.shipper) AS master_customer_name,
# MAGIC     CAST(p.invoice_total AS FLOAT) AS revenue,
# MAGIC     NULL AS column_null,
# MAGIC     rd.created_at,
# MAGIC     rd.quote_id,
# MAGIC     1 AS counter,
# MAGIC     rd.quoted_price,
# MAGIC     rd.margin,
# MAGIC     rd.created_by,
# MAGIC     rd.rep_office,
# MAGIC     rd.load_number,
# MAGIC     rd.shipper_ref_number,
# MAGIC     rd.status_str,
# MAGIC     rd.last_updated_at
# MAGIC   FROM
# MAGIC     bronze.projection_load_1 p
# MAGIC       left join bronze.projection_load_2 p1
# MAGIC         on p.id = p1.id
# MAGIC       LEFT JOIN bronze.customer_lookup cl
# MAGIC         ON LEFT(p1.shipper, 32) = LEFT(cl.aljex_customer_name, 32)
# MAGIC       JOIN raw_data rd
# MAGIC         ON CAST(p.id AS STRING) = rd.load_number
# MAGIC         AND COALESCE(cl.master_customer_name, p1.shipper) = rd.mastername
# MAGIC         AND p.origin_state = rd.shipper_state
# MAGIC         AND p.dest_state = rd.receiver_state
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     AND CAST(p.pickup_date AS DATE) >= '2021-01-01'
# MAGIC     and rd.status_str = 'WON'
# MAGIC     and rd.rank = 1
# MAGIC ),
# MAGIC combined_loads_mine as (
# MAGIC   select distinct
# MAGIC     relay_reference_number_one,
# MAGIC     relay_reference_number_two,
# MAGIC     resulting_plan_id,
# MAGIC     canonical_plan_projection.relay_reference_number as combine_new_rrn,
# MAGIC     canonical_plan_projection.mode,
# MAGIC     b.tender_on_behalf_of_id,
# MAGIC     'COMBINED' as combined
# MAGIC   from
# MAGIC     bronze.plan_combination_projection
# MAGIC       join bronze.canonical_plan_projection
# MAGIC         on resulting_plan_id = canonical_plan_projection.plan_id
# MAGIC       left join bronze.booking_projection b
# MAGIC         on canonical_plan_projection.relay_reference_number = b.relay_reference_number
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and is_combined = 'true'
# MAGIC ),
# MAGIC last_delivery as (
# MAGIC   SELECT DISTINCT
# MAGIC     planning_stop_schedule.relay_reference_number,
# MAGIC     max(planning_stop_schedule.sequence_number) AS last_delivery
# MAGIC   FROM
# MAGIC     bronze.planning_stop_schedule
# MAGIC   WHERE
# MAGIC     planning_stop_schedule.`removed?` = false
# MAGIC   GROUP BY
# MAGIC     planning_stop_schedule.relay_reference_number
# MAGIC ), 
# MAGIC  pricing_nfi_load_predictions as (
# MAGIC     select
# MAGIC       external_load_id as MR_id,
# MAGIC       max(target_buy_rate_gsn) * max(miles_predicted) as Market_Buy_Rate,
# MAGIC       1 as Markey_buy_rate_flag
# MAGIC     from
# MAGIC       `bronze`.`pricing_nfi_load_predictions`
# MAGIC     group by
# MAGIC       external_load_id),
# MAGIC 	  ----- Relay Data -------------
# MAGIC max_schedule_del as (
# MAGIC   SELECT DISTINCT
# MAGIC     delivery_projection.relay_reference_number,
# MAGIC     max(delivery_projection.scheduled_at::timestamp) AS max_schedule_del
# MAGIC   FROM
# MAGIC     bronze.delivery_projection
# MAGIC   GROUP BY
# MAGIC     delivery_projection.relay_reference_number
# MAGIC ),
# MAGIC cell1 as (
# MAGIC   select
# MAGIC     b.relay_reference_number,
# MAGIC     tn.is_split,
# MAGIC     tn.order_numbers,
# MAGIC     tn.updated_at
# MAGIC   from
# MAGIC     booking_projection b
# MAGIC       left join bronze.tender_reference_numbers_projection tn
# MAGIC         on b.relay_reference_number = tn.relay_reference_number
# MAGIC         AND tn.is_split = 'false'
# MAGIC ),
# MAGIC rank as (
# MAGIC   select
# MAGIC     cell1.*,
# MAGIC     row_number() over (
# MAGIC         partition by cell1.relay_reference_number
# MAGIC         order by cell1.updated_at desc
# MAGIC       ) as rank
# MAGIC   from
# MAGIC     cell1
# MAGIC ),
# MAGIC tender_reference_numbers_projection as (
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     rank
# MAGIC   where
# MAGIC     rank = 1 
# MAGIC ),
# MAGIC use_relay_loads as (
# MAGIC   select distinct
# MAGIC     canonical_plan_projection.mode as Mode_Filter,
# MAGIC 	CASE
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'flatbed' THEN 'Flatbed'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'refrigerated' THEN 'Reefer'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'Dry Van'
# MAGIC 	  ELSE tendering_service_line.service_line_type
# MAGIC 	END AS equipment,
# MAGIC     'TL' as Mode,
# MAGIC     b.relay_reference_number as relay_load,
# MAGIC     'NO' as combined_load,
# MAGIC     b.booking_id,
# MAGIC     b.booked_by_name as Carrier_Rep,
# MAGIC     carr.new_office as carr_office,
# MAGIC     cust.new_office as customer_office,
# MAGIC     coalesce(
# MAGIC       pss.appointment_datetime::timestamp,
# MAGIC       pss.window_end_datetime::timestamp,
# MAGIC       pss.window_start_datetime::timestamp,
# MAGIC       pp.appointment_datetime::timestamp
# MAGIC     ) as pu_appt_date,
# MAGIC     coalesce(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as canonical_stop,
# MAGIC     coalesce(
# MAGIC       c.in_date_time::date,
# MAGIC       c.out_date_time::date,
# MAGIC       pss.appointment_datetime::date,
# MAGIC       pss.window_end_datetime::date,
# MAGIC       pss.window_start_datetime::date,
# MAGIC       pp.appointment_datetime::date,
# MAGIC       b.ready_date::date
# MAGIC     ) as use_date,
# MAGIC     pss.scheduled_at as scheduled_Date,
# MAGIC     case
# MAGIC       when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC       else customer_lookup.master_customer_name
# MAGIC     end as mastername,
# MAGIC     r.customer_name as customer_name,
# MAGIC     z.full_name as customer_rep,
# MAGIC     coalesce(
# MAGIC       tendering_planned_distance.planned_distance_amount::float, b.total_miles::float
# MAGIC     ) as total_miles,
# MAGIC     upper(b.status) as status,
# MAGIC     initcap(b.first_shipper_city) as origin_city,
# MAGIC     upper(b.first_shipper_state) as origin_state,
# MAGIC     initcap(b.receiver_city) as dest_city,
# MAGIC     upper(b.receiver_state) as dest_state,
# MAGIC     b.first_shipper_zip as origin_zip,
# MAGIC     b.receiver_zip as dest_zip,
# MAGIC     o.market as market_origin,
# MAGIC     d.market AS market_dest,
# MAGIC     CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC     --financial_calendar.financial_period_sorting,
# MAGIC     --financial_calendar.financial_year,
# MAGIC     carrier_projection.dot_number::string as dot_num,
# MAGIC     coalesce(a.in_date_time, a.out_date_time)::date as del_date,
# MAGIC     rolled_at::date as rolled_at,
# MAGIC     booked_at as Booked_Date,
# MAGIC     ta.accepted_at::string as Tendering_Date,
# MAGIC     b.bounced_at::date as bounced_date,
# MAGIC     b.bounced_at::date + 2 as cancelled_date,
# MAGIC     --pickup.Days_ahead as Days_Ahead,
# MAGIC     --pickup.prebookable as prebookable,
# MAGIC     c.in_date_time As PickUp_Start,
# MAGIC     c.out_date_time As PickUp_End,
# MAGIC     coalesce(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as pickup_datetime,
# MAGIC     cd.in_date_time AS Drop_Start,
# MAGIC     cd.out_date_time AS Drop_End,
# MAGIC     COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL
# MAGIC       THEN
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC       ELSE
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         + cast(
# MAGIC           date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval
# MAGIC         )
# MAGIC     END AS del_appt_date,
# MAGIC     b.booked_carrier_name as carrier_name,
# MAGIC     sales.full_name as salesrep,
# MAGIC     t.trailer_number as Trailer_Num,
# MAGIC     tn.order_numbers as Po_Number,
# MAGIC     b.receiver_name as consignee_name,
# MAGIC     --re.address1 as Delivery_consignee_Address,
# MAGIC     CONCAT(
# MAGIC       re.address1, ' ,', re.city, ' ,', re.state_code, ' ,', re.zip_code
# MAGIC     ) AS Delivery_Consignee_Address,
# MAGIC     CONCAT(
# MAGIC       'Pickuplocation1: ',
# MAGIC       s1.address1,
# MAGIC       ' ,',
# MAGIC       s1.city,
# MAGIC       ' ,',
# MAGIC       s1.state_code,
# MAGIC       ' ,',
# MAGIC       s1.zip_code,
# MAGIC       '\n' 'Pickuplocation2: ',
# MAGIC       s2.address1,
# MAGIC       ' ,',
# MAGIC       s2.city,
# MAGIC       ' ,',
# MAGIC       s2.state_code,
# MAGIC       ' ,',
# MAGIC       s2.zip_code
# MAGIC     ) AS pickup_Consignee_Address,
# MAGIC     carrier_projection.mc_number as Mc_Number,
# MAGIC     sp.quoted_price as Spot_Revenue,
# MAGIC     sp.margin as Spot_Margin
# MAGIC   from
# MAGIC     bronze.booking_projection b
# MAGIC       left join relay_spot_number sp
# MAGIC         on b.relay_reference_number = sp.relay_reference_number
# MAGIC 	Left join tendering_service_line 
# MAGIC 	  on b.relay_reference_number = tendering_service_line.relay_reference_number
# MAGIC       left join receivers re
# MAGIC         on b.receiver_id = re.uuid
# MAGIC       left join bronze.shippers s1
# MAGIC         on b.first_shipper_id = s1.uuid
# MAGIC       left join bronze.shippers s2
# MAGIC         on b.second_shipper_id = s2.uuid
# MAGIC       left join tender_reference_numbers_projection tn
# MAGIC         on b.relay_reference_number = tn.relay_reference_number
# MAGIC       LEFT JOIN max_schedule_del
# MAGIC         ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC       left join truckload_projection t
# MAGIC         on b.relay_reference_number = t.relay_reference_number
# MAGIC         and t.status != 'CancelledOrBounced'
# MAGIC         and t.trailer_number is not null
# MAGIC       LEFT join bronze.customer_profile_projection r
# MAGIC         on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN bronze.canonical_plan_projection
# MAGIC         on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       left join bronze.relay_users z
# MAGIC         on r.primary_relay_user_id = z.user_id
# MAGIC         and z.`active?` = 'true'
# MAGIC       left join relay_users sales
# MAGIC         on r.sales_relay_user_id = sales.user_id
# MAGIC         and sales.`active?` = 'true'
# MAGIC       left join bronze.tendering_acceptance ta
# MAGIC         on b.relay_reference_number = ta.relay_reference_number
# MAGIC       JOIN bronze.new_office_lookup_w_tgt cust
# MAGIC         on r.profit_center = cust.old_office ----- Need to change to new_office_lookup_w_tgt later
# MAGIC       LEFT JOIN bronze.market_lookup o
# MAGIC         ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d
# MAGIC         ON LEFT(receiver_zip, 3) = d.pickup_zip
# MAGIC       LEFT join bronze.relay_users
# MAGIC         on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC         AND relay_users.`active?` = 'true'
# MAGIC       LEFT join bronze.new_office_lookup_w_tgt carr
# MAGIC         on relay_users.office_id = carr.old_office
# MAGIC       Left Join last_delivery
# MAGIC         on b.relay_reference_number = last_delivery.relay_reference_number
# MAGIC       LEFT JOIN bronze.delivery_projection dp
# MAGIC         ON b.relay_reference_number = dp.relay_reference_number
# MAGIC         AND b.receiver_name::string = dp.receiver_name::string
# MAGIC         AND max_schedule_del.max_schedule_del = dp.scheduled_at
# MAGIC       LEFT JOIN bronze.planning_stop_schedule dpss
# MAGIC         ON b.relay_reference_number = dpss.relay_reference_number
# MAGIC         AND b.receiver_name::string = dpss.stop_name::string
# MAGIC         AND b.receiver_city::string = dpss.locality::string
# MAGIC         AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC         AND dpss.stop_type::string = 'delivery'::string
# MAGIC         AND dpss.`removed?` = false
# MAGIC       LEFT JOIN bronze.canonical_stop cd
# MAGIC         ON dpss.stop_id::string = cd.stop_id::string
# MAGIC         AND b.receiver_city::string = cd.locality::string
# MAGIC         AND cd.`stale?` = false
# MAGIC         AND cast(cd.stop_type as string) = 'delivery'
# MAGIC       LEFT join bronze.pickup_projection pp
# MAGIC         on (
# MAGIC           b.booking_id = pp.booking_id
# MAGIC           AND b.first_shipper_name = pp.shipper_name
# MAGIC           AND pp.sequence_number = 1
# MAGIC         )
# MAGIC       LEFT join bronze.planning_stop_schedule pss
# MAGIC         on (
# MAGIC           b.relay_reference_number = pss.relay_reference_number
# MAGIC           AND b.first_shipper_name = pss.stop_name
# MAGIC           AND pss.sequence_number = 1
# MAGIC           AND pss.`removed?` = 'false'
# MAGIC         )
# MAGIC       LEFT join bronze.canonical_stop c
# MAGIC         on (
# MAGIC           pss.stop_id = c.stop_id
# MAGIC           and c.`stale?` = 'false'
# MAGIC           and c.stop_type = 'pickup'
# MAGIC         )
# MAGIC       left join bronze.canonical_stop a
# MAGIC         on b.booking_id = a.booking_id
# MAGIC         and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC         and upper(b.receiver_city) = upper(a.locality)
# MAGIC         and a.stop_type = 'delivery'
# MAGIC         and a.`stale?` = 'false'
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp
# MAGIC         ON b.relay_reference_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp
# MAGIC         ON b.relay_reference_number = dapp.loadnum --left join bounce_data on b.relay_reference_number = bounce_data.relay_reference_number
# MAGIC       --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       --JOIN bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC       left join combined_loads_mine
# MAGIC         on b.relay_reference_number = combined_loads_mine.combine_new_rrn
# MAGIC       left join bronze.customer_lookup
# MAGIC         on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join bronze.tendering_planned_distance
# MAGIC         on b.relay_reference_number = tendering_planned_distance.relay_reference_number
# MAGIC       left join bronze.carrier_projection
# MAGIC         on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     -- [[and {{date_range}}]]
# MAGIC     -- and t.trailer_number is not null
# MAGIC     and b.status = 'booked'
# MAGIC     and canonical_plan_projection.mode not like '%ltl%'
# MAGIC     and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC     and combined is null
# MAGIC   Union
# MAGIC   select distinct
# MAGIC     canonical_plan_projection.mode as Mode_Filter,
# MAGIC 	CASE
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'flatbed' THEN 'Flatbed'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'refrigerated' THEN 'Reefer'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'Dry Van'
# MAGIC 	  ELSE tendering_service_line.service_line_type
# MAGIC 	END AS equipment,
# MAGIC     'TL' as Mode,
# MAGIC     combined_loads_mine.relay_reference_number_one as relay_load,
# MAGIC     CAST(combined_loads_mine.combine_new_rrn As string) as combo_load,
# MAGIC     b.booking_id,
# MAGIC     b.booked_by_name as Carrier_Rep,
# MAGIC     carr.new_office as carr_office,
# MAGIC     cust.new_office as customer_office,
# MAGIC     coalesce(
# MAGIC       pss.appointment_datetime::timestamp,
# MAGIC       pss.window_end_datetime::timestamp,
# MAGIC       pss.window_start_datetime::timestamp,
# MAGIC       pp.appointment_datetime::timestamp
# MAGIC     ) as pu_appt_date,
# MAGIC     coalesce(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as canonical_stop,
# MAGIC     coalesce(
# MAGIC       c.in_date_time::date,
# MAGIC       c.out_date_time::date,
# MAGIC       pss.appointment_datetime::date,
# MAGIC       pss.window_end_datetime::date,
# MAGIC       pss.window_start_datetime::date,
# MAGIC       pp.appointment_datetime::date,
# MAGIC       b.ready_date::date
# MAGIC     ) as use_date,
# MAGIC     pss.scheduled_at as scheduled_Date,
# MAGIC     case
# MAGIC       when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC       else customer_lookup.master_customer_name
# MAGIC     end as mastername,
# MAGIC     r.customer_name as customer_name,
# MAGIC     z.full_name as customer_rep,
# MAGIC     coalesce(
# MAGIC       tendering_planned_distance.planned_distance_amount::float, b.total_miles::float
# MAGIC     ) as total_miles,
# MAGIC     upper(b.status) as status,
# MAGIC     initcap(b.first_shipper_city) as origin_city,
# MAGIC     upper(b.first_shipper_state) as origin_state,
# MAGIC     initcap(b.receiver_city) as dest_city,
# MAGIC     upper(b.receiver_state) as dest_state,
# MAGIC     b.first_shipper_zip as origin_zip,
# MAGIC     b.receiver_zip as dest_zip,
# MAGIC     o.market as market_origin,
# MAGIC     d.market AS market_dest,
# MAGIC     CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC     --dim_financial_calendar.financial_period_sorting,
# MAGIC     --dim_financial_calendar.financial_year,
# MAGIC     carrier_projection.dot_number::string,
# MAGIC     coalesce(a.in_date_time, a.out_date_time)::date as del_date,
# MAGIC     rolled_at::date as rolled_at,
# MAGIC     booked_at as Booked_Date,
# MAGIC     ta.accepted_at::string as Tendering_Date,
# MAGIC     b.bounced_at::date as bounced_date,
# MAGIC     b.bounced_at::date + 2 as cancelled_date,
# MAGIC     --pickup.Days_ahead as Days_Ahead,
# MAGIC     --pickup.prebookable as prebookable,
# MAGIC     c.in_date_time As PickUp_Start,
# MAGIC     c.out_date_time As PickUp_End,
# MAGIC     coalesce(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as pickup_datetime,
# MAGIC     cd.in_date_time AS Drop_Start,
# MAGIC     cd.out_date_time AS Drop_End,
# MAGIC     COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL
# MAGIC       THEN
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC       ELSE
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         + cast(
# MAGIC           date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval
# MAGIC         )
# MAGIC     END AS del_appt_date,
# MAGIC     b.booked_carrier_name,
# MAGIC     sales.full_name as salesrep,
# MAGIC     t.trailer_number as Trailer_Num,
# MAGIC     tn.order_numbers as Po_Number,
# MAGIC     b.receiver_name as consignee_name,
# MAGIC     CONCAT(
# MAGIC       re.address1, ' ,', re.city, ' ,', re.state_code, ' ,', re.zip_code
# MAGIC     ) AS Delivery_Consignee_Address,
# MAGIC     CONCAT(
# MAGIC       'Pickuplocation1: ',
# MAGIC       s1.address1,
# MAGIC       ' ,',
# MAGIC       s1.city,
# MAGIC       ' ,',
# MAGIC       s1.state_code,
# MAGIC       ' ,',
# MAGIC       s1.zip_code,
# MAGIC       '\n' 'Pickuplocation2: ',
# MAGIC       s2.address1,
# MAGIC       ' ,',
# MAGIC       s2.city,
# MAGIC       ' ,',
# MAGIC       s2.state_code,
# MAGIC       ' ,',
# MAGIC       s2.zip_code
# MAGIC     ) AS pickup_Consignee_Address,
# MAGIC     carrier_projection.mc_number as Mc_Number,
# MAGIC     sp.quoted_price as Spot_Revenue,
# MAGIC     sp.margin as Spot_Margin
# MAGIC   from
# MAGIC     combined_loads_mine
# MAGIC       JOIN bronze.booking_projection b
# MAGIC         on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC       left join relay_spot_number sp
# MAGIC         on b.relay_reference_number = sp.relay_reference_number
# MAGIC 	Left join tendering_service_line 
# MAGIC 	  on b.relay_reference_number = tendering_service_line.relay_reference_number
# MAGIC       left join receivers re
# MAGIC         on b.receiver_id = re.uuid
# MAGIC       left join bronze.shippers s1
# MAGIC         on b.first_shipper_id = s1.uuid
# MAGIC       left join bronze.shippers s2
# MAGIC         on b.second_shipper_id = s2.uuid
# MAGIC       LEFT JOIN max_schedule_del
# MAGIC         ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC       left join tender_reference_numbers_projection tn
# MAGIC         on b.relay_reference_number = tn.relay_reference_number
# MAGIC       left join truckload_projection t
# MAGIC         on b.relay_reference_number = t.relay_reference_number
# MAGIC         and t.status != 'CancelledOrBounced'
# MAGIC       Left Join last_delivery
# MAGIC         on b.relay_reference_number = last_delivery.relay_reference_number
# MAGIC       LEFT JOIN bronze.delivery_projection dp
# MAGIC         ON b.relay_reference_number = dp.relay_reference_number
# MAGIC         AND b.receiver_name::string = dp.receiver_name::string
# MAGIC         AND max_schedule_del.max_schedule_del = dp.scheduled_at --::timestamp --without time zone
# MAGIC       LEFT JOIN bronze.planning_stop_schedule dpss
# MAGIC         ON b.relay_reference_number = dpss.relay_reference_number
# MAGIC         AND b.receiver_name::string = dpss.stop_name::string
# MAGIC         AND b.receiver_city::string = dpss.locality::string
# MAGIC         AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC         AND dpss.stop_type::string = 'delivery'::string
# MAGIC         AND dpss.`removed?` = false
# MAGIC       LEFT JOIN bronze.canonical_stop cd
# MAGIC         ON dpss.stop_id::string = cd.stop_id::string
# MAGIC         AND b.receiver_city::string = cd.locality::string
# MAGIC         AND cd.`stale?` = false
# MAGIC         AND cast(cd.stop_type as string) = 'delivery'
# MAGIC       LEFT join bronze.pickup_projection pp
# MAGIC         on (
# MAGIC           b.booking_id = pp.booking_id
# MAGIC           AND b.first_shipper_name = pp.shipper_name
# MAGIC           AND pp.sequence_number = 1
# MAGIC         )
# MAGIC       LEFT join bronze.planning_stop_schedule pss
# MAGIC         on (
# MAGIC           b.relay_reference_number = pss.relay_reference_number
# MAGIC           AND b.first_shipper_name = pss.stop_name
# MAGIC           AND pss.sequence_number = 1
# MAGIC           AND pss.`removed?` = 'false'
# MAGIC         )
# MAGIC       LEFT join bronze.canonical_stop c
# MAGIC         on (
# MAGIC           b.booking_id = c.booking_id
# MAGIC           AND b.first_shipper_id = c.facility_id
# MAGIC           and c.`stale?` = 'false'
# MAGIC           and c.stop_type = 'pickup'
# MAGIC         )
# MAGIC       left join bronze.canonical_stop a
# MAGIC         on b.booking_id = a.booking_id
# MAGIC         and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC         and upper(b.receiver_city) = upper(a.locality)
# MAGIC         and a.stop_type = 'delivery'
# MAGIC         and a.`stale?` = 'false'
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp
# MAGIC         ON b.relay_reference_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp
# MAGIC         ON b.relay_reference_number = dapp.loadnum --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       --LEFT JOIN analytics.dim_financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = dim_financial_calendar.date::date
# MAGIC       JOIN bronze.canonical_plan_projection
# MAGIC         on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT join bronze.customer_profile_projection r
# MAGIC         on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN bronze.new_office_lookup_w_tgt cust
# MAGIC         on r.profit_center = cust.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC       LEFT JOIN bronze.market_lookup o
# MAGIC         ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d
# MAGIC         ON LEFT(receiver_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z
# MAGIC         on r.primary_relay_user_id = z.user_id
# MAGIC         and z.`active?` = 'true'
# MAGIC       left join relay_users sales
# MAGIC         on r.sales_relay_user_id = sales.user_id
# MAGIC         and sales.`active?` = 'true'
# MAGIC       left join bronze.tendering_acceptance ta
# MAGIC         on b.relay_reference_number = ta.relay_reference_number
# MAGIC       left join bronze.customer_lookup
# MAGIC         on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join bronze.tendering_planned_distance
# MAGIC         on combined_loads_mine.relay_reference_number_one
# MAGIC         = tendering_planned_distance.relay_reference_number
# MAGIC       left join bronze.carrier_projection
# MAGIC         on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC       LEFT join bronze.relay_users
# MAGIC         on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC         AND relay_users.`active?` = 'true'
# MAGIC       LEFT join bronze.new_office_lookup_w_tgt carr
# MAGIC         on relay_users.office_id = carr.old_office --- Need to change the office table new_office_lookup_w_tgt
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     --[[and {{date_range}}]]
# MAGIC     --and t.trailer_number is not null
# MAGIC     and b.status = 'booked'
# MAGIC     and canonical_plan_projection.mode not like '%ltl%'
# MAGIC     and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC   Union
# MAGIC   select distinct
# MAGIC     concat(canonical_plan_projection.mode, '-combo2') as Mode_Filter,
# MAGIC 	CASE
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'flatbed' THEN 'Flatbed'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'refrigerated' THEN 'Reefer'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'Dry Van'
# MAGIC 	  ELSE tendering_service_line.service_line_type
# MAGIC 	END AS equipment,
# MAGIC 	'TL' as mode,
# MAGIC     combined_loads_mine.relay_reference_number_two as relay_load,
# MAGIC     CAST(combined_loads_mine.combine_new_rrn As string) as combo_load,
# MAGIC     b.booking_id,
# MAGIC     b.booked_by_name,
# MAGIC     carr.new_office as carr_office,
# MAGIC     cust.new_office as customer_office,
# MAGIC     coalesce(
# MAGIC       pss.appointment_datetime::timestamp,
# MAGIC       pss.window_end_datetime::timestamp,
# MAGIC       pss.window_start_datetime::timestamp,
# MAGIC       pp.appointment_datetime::timestamp
# MAGIC     ) as pu_appt_date,
# MAGIC     coalesce(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as canonical_stop,
# MAGIC     coalesce(
# MAGIC       c.in_date_time::date,
# MAGIC       c.out_date_time::date,
# MAGIC       pss.appointment_datetime::date,
# MAGIC       pss.window_end_datetime::date,
# MAGIC       pss.window_start_datetime::date,
# MAGIC       pp.appointment_datetime::date,
# MAGIC       b.ready_date::date
# MAGIC     ) as use_date,
# MAGIC     pss.scheduled_at as scheduled_Date,
# MAGIC     case
# MAGIC       when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC       else customer_lookup.master_customer_name
# MAGIC     end as mastername, -- Specify the source of master_customer_name      sp.master_customer_name as master_customer_name,
# MAGIC     r.customer_name as customer_name,
# MAGIC     z.full_name as customer_rep,
# MAGIC     coalesce(
# MAGIC       tendering_planned_distance.planned_distance_amount::float, b.total_miles::float
# MAGIC     ) as total_miles,
# MAGIC     b.status,
# MAGIC     initcap(b.first_shipper_city) as origin_city,
# MAGIC     upper(b.first_shipper_state) as origin_state,
# MAGIC     initcap(b.receiver_city) as dest_city,
# MAGIC     upper(b.receiver_state) as dest_state,
# MAGIC     b.first_shipper_zip as origin_zip,
# MAGIC     b.receiver_zip as dest_zip,
# MAGIC     o.market as market_origin,
# MAGIC     d.market AS market_dest,
# MAGIC     CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC     --financial_calendar.financial_period_sorting,
# MAGIC     --financial_calendar.financial_year,
# MAGIC     carrier_projection.dot_number::string,
# MAGIC     coalesce(a.in_date_time, a.out_date_time)::date as del_date,
# MAGIC     rolled_at::date as rolled_at,
# MAGIC     booked_at as Booked_Date,
# MAGIC     ta.accepted_at::string as Tendering_Date,
# MAGIC     b.bounced_at::date as bounced_date,
# MAGIC     b.bounced_at::date + 2 as cancelled_date,
# MAGIC     -- pickup.Days_ahead as Days_Ahead,
# MAGIC     -- pickup.prebookable as prebookable,
# MAGIC     c.in_date_time As PickUp_Start,
# MAGIC     c.out_date_time As PickUp_End,
# MAGIC     coalesce(c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP) as pickup_datetime,
# MAGIC     cd.in_date_time AS Drop_Start,
# MAGIC     cd.out_date_time AS Drop_End,
# MAGIC     COALESCE(cd.in_date_time, cd.out_date_time) AS delivery_datetime,
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL
# MAGIC       THEN
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC       ELSE
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         + cast(
# MAGIC           date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval
# MAGIC         )
# MAGIC     END AS del_appt_date,
# MAGIC     b.booked_carrier_name,
# MAGIC     sales.full_name as salesrep,
# MAGIC     t.trailer_number as Trailer_Num,
# MAGIC     tn.order_numbers as Po_Number,
# MAGIC     b.receiver_name as consignee_name,
# MAGIC     CONCAT(
# MAGIC       re.address1, ' ,', re.city, ' ,', re.state_code, ' ,', re.zip_code
# MAGIC     ) AS Delivery_Consignee_Address,
# MAGIC     CONCAT(
# MAGIC       'Pickuplocation1: ',
# MAGIC       s1.address1,
# MAGIC       ' ,',
# MAGIC       s1.city,
# MAGIC       ' ,',
# MAGIC       s1.state_code,
# MAGIC       ' ,',
# MAGIC       s1.zip_code,
# MAGIC       '\n' 'Pickuplocation2: ',
# MAGIC       s2.address1,
# MAGIC       ' ,',
# MAGIC       s2.city,
# MAGIC       ' ,',
# MAGIC       s2.state_code,
# MAGIC       ' ,',
# MAGIC       s2.zip_code
# MAGIC     ) AS pickup_Consignee_Address,
# MAGIC     carrier_projection.mc_number as Mc_Number,
# MAGIC     sp.quoted_price as Spot_Revenue,
# MAGIC     sp.margin as Spot_Margin
# MAGIC   from
# MAGIC     combined_loads_mine
# MAGIC       JOIN bronze.booking_projection b
# MAGIC         on combined_loads_mine.combine_new_rrn = b.relay_reference_number
# MAGIC 	  Left join tendering_service_line 
# MAGIC 	  on b.relay_reference_number = tendering_service_line.relay_reference_number
# MAGIC       left join relay_spot_number sp
# MAGIC         on b.relay_reference_number = sp.relay_reference_number
# MAGIC       left join receivers re
# MAGIC         on b.receiver_id = re.uuid
# MAGIC       left join bronze.shippers s1
# MAGIC         on b.first_shipper_id = s1.uuid
# MAGIC       left join bronze.shippers s2
# MAGIC         on b.second_shipper_id = s2.uuid
# MAGIC       left join tender_reference_numbers_projection tn
# MAGIC         on b.relay_reference_number = tn.relay_reference_number
# MAGIC       LEFT JOIN max_schedule_del
# MAGIC         ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC       left join truckload_projection t
# MAGIC         on b.relay_reference_number = t.relay_reference_number
# MAGIC         and t.status != 'CancelledOrBounced'
# MAGIC         and t.trailer_number is not null
# MAGIC       Left Join last_delivery
# MAGIC         on b.relay_reference_number = last_delivery.relay_reference_number
# MAGIC       LEFT JOIN bronze.planning_stop_schedule dpss
# MAGIC         ON b.relay_reference_number = dpss.relay_reference_number
# MAGIC         AND b.receiver_name::string = dpss.stop_name::string
# MAGIC         AND b.receiver_city::string = dpss.locality::string
# MAGIC         AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC         AND dpss.stop_type::string = 'delivery'::string
# MAGIC         AND dpss.`removed?` = false
# MAGIC       LEFT JOIN bronze.canonical_stop cd
# MAGIC         ON dpss.stop_id::string = cd.stop_id::string
# MAGIC         AND b.receiver_city::string = cd.locality::string
# MAGIC         AND cd.`stale?` = false
# MAGIC         AND cast(cd.stop_type as string) = 'delivery'
# MAGIC       LEFT join bronze.pickup_projection pp
# MAGIC         on (
# MAGIC           b.booking_id = pp.booking_id
# MAGIC           AND b.first_shipper_name = pp.shipper_name
# MAGIC           AND pp.sequence_number = 1
# MAGIC         )
# MAGIC       LEFT JOIN bronze.delivery_projection dp
# MAGIC         ON b.relay_reference_number = dp.relay_reference_number
# MAGIC         AND b.receiver_name::string = dp.receiver_name::string
# MAGIC         AND max_schedule_del.max_schedule_del = dp.scheduled_at
# MAGIC       LEFT join bronze.planning_stop_schedule pss
# MAGIC         on (
# MAGIC           b.relay_reference_number = pss.relay_reference_number
# MAGIC           AND b.first_shipper_name = pss.stop_name
# MAGIC           AND pss.sequence_number = 1
# MAGIC           AND pss.`removed?` = 'false'
# MAGIC         )
# MAGIC       LEFT join bronze.canonical_stop c
# MAGIC         on (
# MAGIC           b.booking_id = c.booking_id
# MAGIC           AND b.first_shipper_id = c.facility_id
# MAGIC           and c.`stale?` = 'false'
# MAGIC           and c.stop_type = 'pickup'
# MAGIC         )
# MAGIC       left join bronze.canonical_stop a
# MAGIC         on b.booking_id = a.booking_id
# MAGIC         and upper(b.receiver_name) = upper(a.facility_name)
# MAGIC         and upper(b.receiver_city) = upper(a.locality)
# MAGIC         and a.stop_type = 'delivery'
# MAGIC         and a.`stale?` = 'false'
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp
# MAGIC         ON b.relay_reference_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp
# MAGIC         ON b.relay_reference_number = dapp.loadnum --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       --JOIN bronze.financial_calendar on coalesce(c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date, b.ready_date::date)::date = financial_calendar.date::date
# MAGIC       JOIN bronze.canonical_plan_projection
# MAGIC         on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT join bronze.customer_profile_projection r
# MAGIC         on b.tender_on_behalf_of_id = r.customer_slug
# MAGIC       JOIN bronze.new_office_lookup_w_tgt cust
# MAGIC         on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o
# MAGIC         ON LEFT(first_shipper_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d
# MAGIC         ON LEFT(receiver_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z
# MAGIC         on r.primary_relay_user_id = z.user_id
# MAGIC         and z.`active?` = 'true'
# MAGIC       left join relay_users sales
# MAGIC         on r.sales_relay_user_id = sales.user_id
# MAGIC         and sales.`active?` = 'true'
# MAGIC       left join bronze.tendering_acceptance ta
# MAGIC         on b.relay_reference_number = ta.relay_reference_number
# MAGIC       left join bronze.customer_lookup
# MAGIC         on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       left join bronze.tendering_planned_distance
# MAGIC         on combined_loads_mine.relay_reference_number_two
# MAGIC         = tendering_planned_distance.relay_reference_number
# MAGIC       left join bronze.carrier_projection
# MAGIC         on b.booked_carrier_id = carrier_projection.carrier_id
# MAGIC       LEFT join bronze.relay_users
# MAGIC         on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC         AND relay_users.`active?` = 'true'
# MAGIC       LEFT join bronze.new_office_lookup_w_tgt carr
# MAGIC         on relay_users.office_id = carr.old_office
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     --[[and {{date_range}}]]
# MAGIC     --and and t.trailer_number is not null
# MAGIC     and b.status = 'booked'
# MAGIC     and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC     and canonical_plan_projection.mode not like '%ltl%'
# MAGIC   union
# MAGIC   select distinct
# MAGIC     canonical_plan_projection.mode as Mode_Filter,
# MAGIC 	CASE
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'flatbed' THEN 'Flatbed'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'refrigerated' THEN 'Reefer'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'Dry Van'
# MAGIC 	  ELSE tendering_service_line.service_line_type
# MAGIC 	END AS equipment,
# MAGIC     'TL' as Mode,
# MAGIC     e.load_number,
# MAGIC     'NO' as combined_load,
# MAGIC     null::float,
# MAGIC     'ltl_team' as booked_by,
# MAGIC     'LTL',
# MAGIC     cust.new_office as customer_office,
# MAGIC     coalesce(
# MAGIC       pss.appointment_datetime::timestamp,
# MAGIC       pss.window_end_datetime::timestamp,
# MAGIC       pss.window_start_datetime::timestamp,
# MAGIC       pp.appointment_datetime::timestamp
# MAGIC     ) as planning_stop_sched,
# MAGIC     coalesce(
# MAGIC       e.ship_date::timestamp, c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP
# MAGIC     ) as canonical_stop,
# MAGIC     coalesce(
# MAGIC       e.ship_date::date,
# MAGIC       c.in_date_time::date,
# MAGIC       c.out_date_time::date,
# MAGIC       pss.appointment_datetime::date,
# MAGIC       pss.window_end_datetime::date,
# MAGIC       pss.window_start_datetime::date,
# MAGIC       pp.appointment_datetime::date
# MAGIC     ) as use_date,
# MAGIC     pss.scheduled_at as scheduled_Date,
# MAGIC     case
# MAGIC       when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC       else customer_lookup.master_customer_name
# MAGIC     end as mastername,
# MAGIC     r.customer_name as customer_name,
# MAGIC     z.full_name as customer_rep,
# MAGIC     e.miles::float,
# MAGIC     e.dispatch_status,
# MAGIC     initcap(e.pickup_city) as origin_city,
# MAGIC     upper(e.pickup_state) as origin_state,
# MAGIC     initcap(e.consignee_city) as dest_city,
# MAGIC     upper(e.consignee_state) as dest_state,
# MAGIC     e.pickup_zip as Pickup_Zip,
# MAGIC     e.consignee_zip as Consignee_Zip,
# MAGIC     o.market as market_origin,
# MAGIC     d.market AS market_dest,
# MAGIC     CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC     --financial_calendar.financial_period_sorting,
# MAGIC     --financial_calendar.financial_year,
# MAGIC     projection_carrier.dot_num::string,
# MAGIC     cast(delivered_date as date)::date as delivery_date,
# MAGIC     null::date as rolled_out,
# MAGIC     null as Booked_Date,
# MAGIC     null::string as Tender_Date,
# MAGIC     null::date as bounced_date,
# MAGIC     null::date as cancelled_date,
# MAGIC     --pickup.Days_ahead as Days_Ahead,
# MAGIC     --pickup.prebookable as prebookable,
# MAGIC     coalesce(e.ship_date::timestamp, c.in_date_time::TIMESTAMP) as PickUp_Start,
# MAGIC     c.out_date_time::TIMESTAMP as PickUp_End,
# MAGIC     coalesce(
# MAGIC       e.ship_date::timestamp, c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP
# MAGIC     ) as pickup_datetime,
# MAGIC     COALESCE(e.delivered_date::timestamp, cd.in_date_time::timestamp) AS Drop_Start,
# MAGIC     cd.out_date_time::timestamp AS Drop_End,
# MAGIC     COALESCE(
# MAGIC       e.delivered_date::timestamp, cd.in_date_time::timestamp, cd.out_date_time::timestamp
# MAGIC     ) AS delivery_datetime,
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL
# MAGIC       THEN
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC       ELSE
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         + cast(
# MAGIC           date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval
# MAGIC         )
# MAGIC     END AS del_appt_date,
# MAGIC     e.carrier_name,
# MAGIC     sales.full_name as salesrep,
# MAGIC     t.trailer_number as Trailer_Num,
# MAGIC     tn.order_numbers as Po_Number,
# MAGIC     e.consignee_name,
# MAGIC     CONCAT(
# MAGIC       dpss.address_1,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           dpss.address_2 IS NOT NULL
# MAGIC           AND dpss.address_2 <> ''
# MAGIC         THEN
# MAGIC           CONCAT(' ,', dpss.address_2)
# MAGIC         ELSE ''
# MAGIC       END,
# MAGIC       ' ,',
# MAGIC       dpss.locality,
# MAGIC       ' ,',
# MAGIC       dpss.administrative_region,
# MAGIC       ' ,',
# MAGIC       dpss.postal_code
# MAGIC     ) AS Delivery_Consignee_Address,
# MAGIC     --dpss.address_1 as consignee_Address,
# MAGIC     CONCAT(
# MAGIC       pss.address_1,
# MAGIC       ' ,',
# MAGIC       pss.address_2,
# MAGIC       ' ,',
# MAGIC       pss.locality,
# MAGIC       ' ,',
# MAGIC       pss.administrative_region,
# MAGIC       ' ,',
# MAGIC       pss.postal_code
# MAGIC     ) AS pickup_Consignee_Address,
# MAGIC     --pss.Address_1 as pickup_Consignee_Address,
# MAGIC     projection_carrier.mc_num as Mc_Number,
# MAGIC     sp.quoted_price as Spot_Revenue,
# MAGIC     sp.margin as Spot_Margin
# MAGIC   from
# MAGIC     bronze.big_export_projection e
# MAGIC       left join relay_spot_number sp
# MAGIC         on e.load_number = sp.relay_reference_number
# MAGIC 	  Left join tendering_service_line 
# MAGIC 	    on e.load_number = tendering_service_line.relay_reference_number
# MAGIC       left join tender_reference_numbers_projection tn
# MAGIC         on e.load_number = tn.relay_reference_number
# MAGIC       left join truckload_projection t
# MAGIC         on e.load_number = t.relay_reference_number
# MAGIC         and t.status != 'CancelledOrBounced'
# MAGIC         and t.trailer_number is not null
# MAGIC       LEFT join bronze.customer_profile_projection r
# MAGIC         on e.customer_id = r.customer_slug
# MAGIC       LEFT JOIN max_schedule_del
# MAGIC         ON e.load_number = max_schedule_del.relay_reference_number
# MAGIC       left join bronze.customer_lookup
# MAGIC         on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       LEFT JOIN bronze.new_office_lookup_w_tgt cust
# MAGIC         on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o
# MAGIC         ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d
# MAGIC         ON LEFT(e.consignee_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z
# MAGIC         on r.primary_relay_user_id = z.user_id
# MAGIC         and z.`active?` = 'true'
# MAGIC       left join relay_users sales
# MAGIC         on r.sales_relay_user_id = sales.user_id
# MAGIC         and sales.`active?` = 'true'
# MAGIC       LEFT JOIN bronze.delivery_projection dp
# MAGIC         ON e.load_number = dp.relay_reference_number
# MAGIC         AND e.consignee_name = dp.receiver_name
# MAGIC         AND max_schedule_del.max_schedule_del = cast(dp.scheduled_at as timestamp)
# MAGIC       JOIN bronze.canonical_plan_projection
# MAGIC         on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT JOIN last_delivery
# MAGIC         ON e.load_number = last_delivery.relay_reference_number
# MAGIC       LEFT JOIN bronze.planning_stop_schedule dpss
# MAGIC         ON e.load_number = dpss.relay_reference_number
# MAGIC         AND e.consignee_name::string = dpss.stop_name::string
# MAGIC         AND e.consignee_city::string = dpss.locality::string
# MAGIC         AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC         AND dpss.stop_type::string = 'delivery'::string
# MAGIC         AND dpss.`removed?` = false
# MAGIC       LEFT JOIN bronze.canonical_stop cd
# MAGIC         ON dpss.stop_id::string = cd.stop_id::string
# MAGIC       LEFT join bronze.pickup_projection pp
# MAGIC         on (
# MAGIC           e.load_number = pp.relay_reference_number
# MAGIC           AND e.pickup_name = pp.shipper_name
# MAGIC           AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS?
# MAGIC           AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC           AND pp.sequence_number = 1
# MAGIC         )
# MAGIC       LEFT join bronze.planning_stop_schedule pss
# MAGIC         on (
# MAGIC           e.load_number = pss.relay_reference_number
# MAGIC           AND e.pickup_name = pss.stop_name
# MAGIC           AND pss.sequence_number = 1
# MAGIC           AND pss.`removed?` = 'false'
# MAGIC         )
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp
# MAGIC         ON e.load_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp
# MAGIC         ON e.load_number = dapp.loadnum --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       LEFT join bronze.canonical_stop c
# MAGIC         on pss.stop_id = c.stop_id --JOIN bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC       left join bronze.projection_carrier
# MAGIC         on e.carrier_id = projection_carrier.id
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     --[[and {{date_range}}]]
# MAGIC     --and e.dispatch_status != 'Cancelled'
# MAGIC     and e.carrier_name is not null
# MAGIC     and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC     and canonical_plan_projection.mode = 'ltl'
# MAGIC     and e.customer_id NOT in ('roland', 'hain', 'deb')
# MAGIC   UNION
# MAGIC   select distinct
# MAGIC     'ltl- (hain,deb,roland)',
# MAGIC 	CASE
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'flatbed' THEN 'Flatbed'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'refrigerated' THEN 'Reefer'
# MAGIC 	  WHEN tendering_service_line.service_line_type = 'dry_van' THEN 'Dry Van'
# MAGIC 	  ELSE tendering_service_line.service_line_type
# MAGIC 	END AS equipment,
# MAGIC     'TL' as Mode,
# MAGIC     e.load_number,
# MAGIC     'NO' as combined_load,
# MAGIC     null::float,
# MAGIC     'ltl_team' as booked_by,
# MAGIC     'LTL',
# MAGIC     cust.new_office as customer_office,
# MAGIC     coalesce(
# MAGIC       pss.appointment_datetime::timestamp,
# MAGIC       pss.window_end_datetime::timestamp,
# MAGIC       pss.window_start_datetime::timestamp,
# MAGIC       pp.appointment_datetime::timestamp
# MAGIC     ) as planning_stop_sched,
# MAGIC     coalesce(
# MAGIC       e.ship_date::timestamp, c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP
# MAGIC     ) as canonical_stop,
# MAGIC     coalesce(
# MAGIC       e.ship_date::date,
# MAGIC       c.in_date_time::date,
# MAGIC       c.out_date_time::date,
# MAGIC       pss.appointment_datetime::date,
# MAGIC       pss.window_end_datetime::date,
# MAGIC       pss.window_start_datetime::date,
# MAGIC       pp.appointment_datetime::date
# MAGIC     ) as use_date,
# MAGIC     pss.scheduled_at as scheduled_Date,
# MAGIC     case
# MAGIC       when customer_lookup.master_customer_name is null then r.customer_name
# MAGIC       else customer_lookup.master_customer_name
# MAGIC     end as mastername,
# MAGIC     r.customer_name as customer_name,
# MAGIC     z.full_name as customer_rep,
# MAGIC     e.miles::float,
# MAGIC     e.dispatch_status,
# MAGIC     initcap(e.pickup_city) as origin_city,
# MAGIC     upper(e.pickup_state) as origin_state,
# MAGIC     initcap(e.consignee_city) as dest_city,
# MAGIC     upper(e.consignee_state) as dest_state,
# MAGIC     e.pickup_zip as Pickup_Zip,
# MAGIC     e.consignee_zip as Consignee_Zip,
# MAGIC     o.market as market_origin,
# MAGIC     d.market AS market_dest,
# MAGIC     CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC     --financial_calendar.financial_period_sorting,
# MAGIC     --financial_calendar.financial_year,
# MAGIC     projection_carrier.dot_num::string,
# MAGIC     cast(delivered_date as date)::date as delivery_date,
# MAGIC     null::date as rolled_out,
# MAGIC     null as Booked_Date,
# MAGIC     null::string as Tender_Date,
# MAGIC     null::date as bounced_date,
# MAGIC     null::date as cancelled_date,
# MAGIC     --pickup.Days_ahead as Days_Ahead,
# MAGIC     --pickup.prebookable as prebookable,
# MAGIC     coalesce(e.ship_date::timestamp, c.in_date_time::TIMESTAMP) as PickUp_Start,
# MAGIC     c.out_date_time::TIMESTAMP as PickUp_End,
# MAGIC     coalesce(
# MAGIC       e.ship_date::timestamp, c.in_date_time::TIMESTAMP, c.out_date_time::TIMESTAMP
# MAGIC     ) as pickup_datetime,
# MAGIC     COALESCE(e.delivered_date::timestamp, cd.in_date_time::timestamp) AS Drop_Start,
# MAGIC     cd.out_date_time::timestamp AS Drop_End,
# MAGIC     COALESCE(
# MAGIC       e.delivered_date::timestamp, cd.in_date_time::timestamp, cd.out_date_time::timestamp
# MAGIC     ) AS delivery_datetime,
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') IS NULL
# MAGIC       THEN
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC       ELSE
# MAGIC         cast(nullif(dp.appointment_date, '') as timestamp)
# MAGIC         + cast(
# MAGIC           date_format(cast(nullif(dp.appointment_time_local, '') as timestamp), 'HH:mm') as interval
# MAGIC         )
# MAGIC     END AS del_appt_date,
# MAGIC     e.carrier_name,
# MAGIC     sales.full_name as salesrep,
# MAGIC     t.trailer_number as Trailer_Num,
# MAGIC     -- t.trailer_number as Trailer_Num,
# MAGIC     tn.order_numbers as Po_Number,
# MAGIC     e.consignee_name,
# MAGIC     CONCAT(
# MAGIC       dpss.address_1,
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           dpss.address_2 IS NOT NULL
# MAGIC           AND dpss.address_2 <> ''
# MAGIC         THEN
# MAGIC           CONCAT(' ,', dpss.address_2)
# MAGIC         ELSE ''
# MAGIC       END,
# MAGIC       ' ,',
# MAGIC       dpss.locality,
# MAGIC       ' ,',
# MAGIC       dpss.administrative_region,
# MAGIC       ' ,',
# MAGIC       dpss.postal_code
# MAGIC     ) AS Delivery_Consignee_Address,
# MAGIC     --dpss.address_1 as consignee_Address,
# MAGIC     CONCAT(
# MAGIC       pss.address_1,
# MAGIC       ' ,',
# MAGIC       pss.address_2,
# MAGIC       ' ,',
# MAGIC       pss.locality,
# MAGIC       ' ,',
# MAGIC       pss.administrative_region,
# MAGIC       ' ,',
# MAGIC       pss.postal_code
# MAGIC     ) AS pickup_Consignee_Address,
# MAGIC     --pss.Address_1 as pickup_Consignee_Address,
# MAGIC     projection_carrier.mc_num as Mc_Number,
# MAGIC     sp.quoted_price as Spot_Revenue,
# MAGIC     sp.margin as Spot_Margin
# MAGIC   from
# MAGIC     bronze.big_export_projection e
# MAGIC       left join relay_spot_number sp
# MAGIC         on e.load_number = sp.relay_reference_number
# MAGIC 	  Left join tendering_service_line 
# MAGIC 	  on e.load_number = tendering_service_line.relay_reference_number
# MAGIC       left join tender_reference_numbers_projection tn
# MAGIC         on e.load_number = tn.relay_reference_number
# MAGIC       left join truckload_projection t
# MAGIC         on e.load_number = t.relay_reference_number
# MAGIC         and t.status != 'CancelledOrBounced'
# MAGIC         and t.trailer_number is not null
# MAGIC       LEFT join bronze.customer_profile_projection r
# MAGIC         on e.customer_id = r.customer_slug
# MAGIC       LEFT JOIN max_schedule_del
# MAGIC         ON e.load_number = max_schedule_del.relay_reference_number
# MAGIC       left join bronze.customer_lookup
# MAGIC         on left(r.customer_name, 32) = left(aljex_customer_name, 32)
# MAGIC       LEFT JOIN bronze.new_office_lookup_w_tgt cust
# MAGIC         on r.profit_center = cust.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o
# MAGIC         ON LEFT(e.pickup_zip, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d
# MAGIC         ON LEFT(e.consignee_zip, 3) = d.pickup_zip
# MAGIC       left join bronze.relay_users z
# MAGIC         on r.primary_relay_user_id = z.user_id
# MAGIC         and z.`active?` = 'true'
# MAGIC       JOIN bronze.canonical_plan_projection
# MAGIC         on e.load_number = canonical_plan_projection.relay_reference_number
# MAGIC       LEFT JOIN last_delivery
# MAGIC         ON e.load_number = last_delivery.relay_reference_number
# MAGIC       LEFT JOIN bronze.planning_stop_schedule dpss
# MAGIC         ON e.load_number = dpss.relay_reference_number
# MAGIC         AND e.consignee_name::string = dpss.stop_name::string
# MAGIC         AND e.consignee_city::string = dpss.locality::string
# MAGIC         AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC         AND dpss.stop_type::string = 'delivery'::string
# MAGIC         AND dpss.`removed?` = false
# MAGIC       LEFT JOIN bronze.canonical_stop cd
# MAGIC         ON dpss.stop_id::string = cd.stop_id::string
# MAGIC       LEFT JOIN bronze.delivery_projection dp
# MAGIC         ON e.load_number = dp.relay_reference_number
# MAGIC         AND e.consignee_name = dp.receiver_name
# MAGIC         AND max_schedule_del.max_schedule_del = cast(dp.scheduled_at as timestamp)
# MAGIC       left join relay_users sales
# MAGIC         on r.sales_relay_user_id = sales.user_id
# MAGIC         and sales.`active?` = 'true'
# MAGIC       LEFT join bronze.pickup_projection pp
# MAGIC         on (
# MAGIC           e.load_number = pp.relay_reference_number
# MAGIC           AND e.pickup_name = pp.shipper_name
# MAGIC           AND e.weight = pp.weight_to_pickup_amount -- JUST ADDED THIS LINE, MIGHT NEED TO REMOVE IF IM MISSING LOADS?
# MAGIC           AND e.piece_count = pp.pieces_to_pickup_count
# MAGIC           AND pp.sequence_number = 1
# MAGIC         )
# MAGIC       LEFT join bronze.planning_stop_schedule pss
# MAGIC         on (
# MAGIC           e.load_number = pss.relay_reference_number
# MAGIC           AND e.pickup_name = pss.stop_name
# MAGIC           AND pss.sequence_number = 1
# MAGIC           AND pss.`removed?` = 'false'
# MAGIC         )
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp' --group by relay_reference_number
# MAGIC       ) as papp
# MAGIC         ON e.load_number = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp
# MAGIC         ON e.load_number = dapp.loadnum --left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
# MAGIC       LEFT join bronze.canonical_stop c
# MAGIC         on pss.stop_id = c.stop_id --JOIN bronze.canonical_plan_projection.financial_calendar on coalesce(e.ship_date::date, c.in_date_time::date, c.out_date_time::date, pss.appointment_datetime::date, pss.window_end_datetime::date, pss.window_start_datetime::date, pp.appointment_datetime::date)::date = financial_calendar.date::date
# MAGIC       left join bronze.projection_carrier
# MAGIC         on e.carrier_id = projection_carrier.id
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     --[[and {{date_range}}]]
# MAGIC     --and t.trailer_number is not null
# MAGIC     and e.dispatch_status != 'Cancelled'
# MAGIC     and e.carrier_name is not null
# MAGIC     and canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC     and canonical_plan_projection.mode = 'ltl'
# MAGIC     and e.customer_id in ('roland', 'hain', 'deb')
# MAGIC ),
# MAGIC to_get_avg as (
# MAGIC   select distinct
# MAGIC     ship_date,
# MAGIC     conversion as us_to_cad,
# MAGIC     us_to_cad as cad_to_us
# MAGIC   from
# MAGIC     bronze.canada_conversions
# MAGIC   order by
# MAGIC     ship_date desc
# MAGIC   limit 7
# MAGIC ),
# MAGIC average_conversions as (
# MAGIC   select
# MAGIC     avg(us_to_cad) as avg_us_to_cad,
# MAGIC     avg(cad_to_us) as avg_cad_to_us
# MAGIC   from
# MAGIC     to_get_avg
# MAGIC ),
# MAGIC total_carrier_rate as (
# MAGIC   select distinct
# MAGIC     'non_combo' as typee,
# MAGIC     relay_reference_number::string,
# MAGIC     sum(
# MAGIC       case
# MAGIC         when
# MAGIC           currency = 'CAD'
# MAGIC         then
# MAGIC           amount::float
# MAGIC           * (
# MAGIC             coalesce(
# MAGIC               case
# MAGIC                 when us_to_cad = 0 then null
# MAGIC                 else canada_conversions.us_to_cad
# MAGIC               end,
# MAGIC               average_conversions.avg_cad_to_us
# MAGIC             )
# MAGIC           )::float
# MAGIC         else amount::float
# MAGIC       end
# MAGIC     )::float
# MAGIC     / 100.00 as total_carrier_rate
# MAGIC   from
# MAGIC     bronze.vendor_transaction_projection
# MAGIC       JOIN use_relay_loads
# MAGIC         on relay_reference_number = use_relay_loads.relay_load
# MAGIC       left join bronze.canada_conversions
# MAGIC         on vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date::date
# MAGIC       join average_conversions
# MAGIC         on 1 = 1
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and `voided?` = 'false'
# MAGIC     and combined_load = 'NO'
# MAGIC   group by
# MAGIC     relay_reference_number
# MAGIC   union
# MAGIC   select distinct
# MAGIC     'combo',
# MAGIC     combined_load,
# MAGIC     sum(
# MAGIC       case
# MAGIC         when
# MAGIC           currency = 'CAD'
# MAGIC         then
# MAGIC           amount::float
# MAGIC           * (
# MAGIC             coalesce(
# MAGIC               case
# MAGIC                 when us_to_cad = 0 then null
# MAGIC                 else canada_conversions.us_to_cad
# MAGIC               end,
# MAGIC               average_conversions.avg_cad_to_us
# MAGIC             )
# MAGIC           )::float
# MAGIC         else amount::float
# MAGIC       end
# MAGIC     )::float
# MAGIC     / 100.00 as total_carrier_rate
# MAGIC   from
# MAGIC     bronze.vendor_transaction_projection
# MAGIC       JOIN use_relay_loads
# MAGIC         on relay_reference_number::string = use_relay_loads.combined_load::string
# MAGIC       left join bronze.canada_conversions
# MAGIC         on vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date::date
# MAGIC       join average_conversions
# MAGIC         on 1 = 1
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and `voided?` = 'false'
# MAGIC     and combined_load != 'NO'
# MAGIC   group by
# MAGIC     combined_load
# MAGIC ),
# MAGIC invoicing_cred as (
# MAGIC   select distinct
# MAGIC     relay_reference_number,
# MAGIC     sum(
# MAGIC       case
# MAGIC         when
# MAGIC           currency = 'CAD'
# MAGIC         then
# MAGIC           total_amount::float
# MAGIC           * (
# MAGIC             coalesce(
# MAGIC               case
# MAGIC                 when us_to_cad = 0 then null
# MAGIC                 else canada_conversions.us_to_cad
# MAGIC               end,
# MAGIC               average_conversions.avg_cad_to_us
# MAGIC             )
# MAGIC           )::float
# MAGIC         else total_amount::float
# MAGIC       end
# MAGIC     )::float
# MAGIC     / 100.00 as invoicing_cred
# MAGIC   from
# MAGIC     bronze.invoicing_credits
# MAGIC       JOIN use_relay_loads
# MAGIC         on relay_reference_number = use_relay_loads.relay_load
# MAGIC       left join bronze.canada_conversions
# MAGIC         on invoicing_credits.credited_at::date = canada_conversions.ship_date::date
# MAGIC       join average_conversions
# MAGIC         on 1 = 1
# MAGIC   where
# MAGIC     1 = 1
# MAGIC   group by
# MAGIC     relay_reference_number
# MAGIC ),
# MAGIC total_cust_rate as (
# MAGIC   select distinct
# MAGIC     m.relay_reference_number::string,
# MAGIC     coalesce(invoicing_cred.invoicing_cred, 0) as credit_amt,
# MAGIC     (
# MAGIC       sum(
# MAGIC         case
# MAGIC           when
# MAGIC             m.currency = 'CAD'
# MAGIC           then
# MAGIC             amount::float
# MAGIC             * (
# MAGIC               coalesce(
# MAGIC                 case
# MAGIC                   when us_to_cad = 0 then null
# MAGIC                   else canada_conversions.us_to_cad
# MAGIC                 end,
# MAGIC                 average_conversions.avg_cad_to_us
# MAGIC               )
# MAGIC             )::float
# MAGIC           else amount::float
# MAGIC         end
# MAGIC       )::float
# MAGIC       / 100.00
# MAGIC     )
# MAGIC     - coalesce(invoicing_cred.invoicing_cred, 0) as total_cust_rate
# MAGIC   from
# MAGIC     bronze.moneying_billing_party_transaction m
# MAGIC       JOIN use_relay_loads
# MAGIC         on m.relay_reference_number = use_relay_loads.relay_load
# MAGIC       left join bronze.canada_conversions
# MAGIC         on m.incurred_at::date = canada_conversions.ship_date::date
# MAGIC       LEFT join invoicing_cred
# MAGIC         on m.relay_reference_number = invoicing_cred.relay_reference_number
# MAGIC       join average_conversions
# MAGIC         on 1 = 1
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and m.`voided?` = 'false'
# MAGIC   group by
# MAGIC     m.relay_reference_number,
# MAGIC     invoicing_cred
# MAGIC ),
# MAGIC new_customer_money as (
# MAGIC   select distinct
# MAGIC     moneying_billing_party_transaction.relay_reference_number as loadnum,
# MAGIC     sum(amount)
# MAGIC       filter ( where charge_code = 'linehaul' )::float
# MAGIC     / 100 as linehaul_cust_money,
# MAGIC     sum(amount)
# MAGIC       filter ( where charge_code = 'fuel_surcharge' )::float
# MAGIC     / 100 as fuel_cust_money,
# MAGIC     sum(amount)
# MAGIC       filter ( where charge_code not in ('fuel_surcharge', 'linehaul') )::float
# MAGIC     / 100 as acc_cust_money,
# MAGIC     sum(amount)::float / 100 as total_cust_amount,
# MAGIC     sum(distinct invoicing_credits.total_amount)::float / 100 as inv_cred_amt
# MAGIC   from
# MAGIC     bronze.moneying_billing_party_transaction
# MAGIC       left join bronze.invoicing_credits
# MAGIC         on moneying_billing_party_transaction.relay_reference_number::float
# MAGIC         = invoicing_credits.relay_reference_number::float
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and "voided?" = 'false'
# MAGIC   group by
# MAGIC     moneying_billing_party_transaction.relay_reference_number
# MAGIC ),
# MAGIC invoiced_amts as (
# MAGIC   select distinct
# MAGIC     relay_reference_number,
# MAGIC     min(invoiced_at::date) as invoiced_at,
# MAGIC     case
# MAGIC       when (sum(amount)::float / 100) = 0 then 0
# MAGIC       else (sum(amount)::float / 100)
# MAGIC     end
# MAGIC     - case
# MAGIC       when new_customer_money.inv_cred_amt is null then 0
# MAGIC       else new_customer_money.inv_cred_amt::float
# MAGIC     end as total_recievables,
# MAGIC     (
# MAGIC       sum(amount)
# MAGIC         filter ( where "invoiced?" = 'true' )::float
# MAGIC       / 100
# MAGIC     )::float
# MAGIC     - case
# MAGIC       when new_customer_money.inv_cred_amt is null then 0
# MAGIC       else new_customer_money.inv_cred_amt::float
# MAGIC     end as invoiced_amt,
# MAGIC     sum(amount)
# MAGIC       filter ( where "invoiced?" = 'false' )::float
# MAGIC     / 100 as non_invoiced_amt
# MAGIC   from
# MAGIC     bronze.moneying_billing_party_transaction
# MAGIC       left join new_customer_money
# MAGIC         on moneying_billing_party_transaction.relay_reference_number = new_customer_money.loadnum
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and "voided?" = 'false'
# MAGIC   group by
# MAGIC     relay_reference_number,
# MAGIC     new_customer_money.inv_cred_amt
# MAGIC ), -------------------- Adding the Linehaul, Fuelsurcharge and Other expoense CTE ----------------------------------------------
# MAGIC carrier_charges_one as (
# MAGIC   select distinct
# MAGIC     relay_reference_number,
# MAGIC     incurred_at,
# MAGIC     coalesce(
# MAGIC       case
# MAGIC         when
# MAGIC           currency = 'USD'
# MAGIC         then
# MAGIC           sum(amount)
# MAGIC             filter ( where charge_code = 'linehaul' )::float
# MAGIC           / 100.00
# MAGIC         else
# MAGIC           (
# MAGIC             sum(amount)
# MAGIC               filter ( where charge_code = 'linehaul' )::float
# MAGIC             / 100.00
# MAGIC           )
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC       end,
# MAGIC       0
# MAGIC     ) as carr_lhc,
# MAGIC     coalesce(
# MAGIC       case
# MAGIC         when
# MAGIC           currency = 'USD'
# MAGIC         then
# MAGIC           sum(amount)
# MAGIC             filter ( where charge_code = 'fuel_surcharge' )::float
# MAGIC           / 100.00
# MAGIC         else
# MAGIC           (
# MAGIC             sum(amount)
# MAGIC               filter ( where charge_code = 'fuel_surcharge' )::float
# MAGIC             / 100.00
# MAGIC           )
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC       end,
# MAGIC       0
# MAGIC     ) as carr_fsc,
# MAGIC     coalesce(
# MAGIC       case
# MAGIC         when
# MAGIC           currency = 'USD'
# MAGIC         then
# MAGIC           sum(amount)
# MAGIC             filter ( where charge_code not in ('fuel_surcharge', 'linehaul') )::float
# MAGIC           / 100.00
# MAGIC         else
# MAGIC           (
# MAGIC             sum(amount)
# MAGIC               filter ( where charge_code not in ('fuel_surcharge', 'linehaul') )::float
# MAGIC             / 100.00
# MAGIC           )
# MAGIC           * (
# MAGIC             case
# MAGIC               when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us
# MAGIC               else canada_conversions.us_to_cad
# MAGIC             end
# MAGIC           )::float
# MAGIC       end,
# MAGIC       0
# MAGIC     ) as carr_acc
# MAGIC   from
# MAGIC     bronze.vendor_transaction_projection
# MAGIC       left join bronze.canada_conversions
# MAGIC         on vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date::date
# MAGIC       join average_conversions
# MAGIC         on 1 = 1
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and `voided?` = 'false'
# MAGIC   group by
# MAGIC     relay_reference_number,
# MAGIC     currency,
# MAGIC     us_to_cad,
# MAGIC     avg_cad_to_us,
# MAGIC     incurred_at
# MAGIC ),
# MAGIC carrier_charges as (
# MAGIC   select distinct
# MAGIC     relay_reference_number,
# MAGIC     sum(carr_lhc) as carr_lhc,
# MAGIC     sum(carr_fsc) as carr_fsc,
# MAGIC     sum(carr_acc) as carr_acc,
# MAGIC     sum(carr_acc)::float + sum(carr_fsc)::float + sum(carr_lhc)::float as total_carrier_cost
# MAGIC   from
# MAGIC     carrier_charges_one
# MAGIC   group by
# MAGIC     relay_reference_number
# MAGIC ), --Select * from carrier_charges_one limit 100;
# MAGIC aljex_data as (
# MAGIC   select
# MAGIC     p1.id,
# MAGIC     case
# MAGIC       when aljex_user_report_listing.full_name is null then key_c_user
# MAGIC       else aljex_user_report_listing.full_name
# MAGIC     end as key_c_user,
# MAGIC     case
# MAGIC       when customer_lookup.master_customer_name is null then p2.shipper
# MAGIC       else customer_lookup.master_customer_name
# MAGIC     end as mastername,
# MAGIC     p2.shipper as customer_name,
# MAGIC     case
# MAGIC       when z.full_name is null then p2.srv_rep
# MAGIC       else z.full_name
# MAGIC     end as customer_rep,
# MAGIC     coalesce(p1.arrive_pickup_date, p1.loaded_date, p1.pickup_date) as use_date,
# MAGIC     CASE
# MAGIC       WHEN carrier_line_haul LIKE '%:%' THEN 0.00
# MAGIC       WHEN carrier_line_haul RLIKE '-?[0-9]+(\\.[0-9]+)?' THEN CAST(carrier_line_haul AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END
# MAGIC     + CASE
# MAGIC       WHEN carrier_accessorial_rate1 LIKE '%:%' THEN 0.00
# MAGIC       WHEN
# MAGIC         carrier_accessorial_rate1 RLIKE '-?[0-9]+(\\.[0-9]+)?'
# MAGIC       THEN
# MAGIC         CAST(carrier_accessorial_rate1 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END
# MAGIC     + CASE
# MAGIC       WHEN carrier_accessorial_rate2 LIKE '%:%' THEN 0.00
# MAGIC       WHEN
# MAGIC         carrier_accessorial_rate2 RLIKE '-?[0-9]+(\\.[0-9]+)?'
# MAGIC       THEN
# MAGIC         CAST(carrier_accessorial_rate2 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END
# MAGIC     + CASE
# MAGIC       WHEN carrier_accessorial_rate3 LIKE '%:%' THEN 0.00
# MAGIC       WHEN
# MAGIC         carrier_accessorial_rate3 RLIKE '-?[0-9]+(\\.[0-9]+)?'
# MAGIC       THEN
# MAGIC         CAST(carrier_accessorial_rate3 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END
# MAGIC     + CASE
# MAGIC       WHEN carrier_accessorial_rate4 LIKE '%:%' THEN 0.00
# MAGIC       WHEN
# MAGIC         carrier_accessorial_rate4 RLIKE '-?[0-9]+(\\.[0-9]+)?'
# MAGIC       THEN
# MAGIC         CAST(carrier_accessorial_rate4 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END
# MAGIC     + CASE
# MAGIC       WHEN carrier_accessorial_rate5 LIKE '%:%' THEN 0.00
# MAGIC       WHEN
# MAGIC         carrier_accessorial_rate5 RLIKE '-?[0-9]+(\\.[0-9]+)?'
# MAGIC       THEN
# MAGIC         CAST(carrier_accessorial_rate5 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END
# MAGIC     + CASE
# MAGIC       WHEN carrier_accessorial_rate6 LIKE '%:%' THEN 0.00
# MAGIC       WHEN
# MAGIC         carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC       THEN
# MAGIC         CAST(carrier_accessorial_rate6 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END
# MAGIC     + CASE
# MAGIC       WHEN carrier_accessorial_rate7 LIKE '%:%' THEN 0.00
# MAGIC       WHEN
# MAGIC         carrier_accessorial_rate7 RLIKE '-?[0-9]+(\\.[0-9]+)?'
# MAGIC       THEN
# MAGIC         CAST(carrier_accessorial_rate7 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END
# MAGIC     + CASE
# MAGIC       WHEN carrier_accessorial_rate8 LIKE '%:%' THEN 0.00
# MAGIC       WHEN
# MAGIC         carrier_accessorial_rate8 RLIKE '-?[0-9]+(\\.[0-9]+)?'
# MAGIC       THEN
# MAGIC         CAST(carrier_accessorial_rate8 AS DOUBLE)
# MAGIC       ELSE 0.00
# MAGIC     END AS final_carrier_rate,
# MAGIC     case
# MAGIC       when carrier_line_haul like '%:%' then 0.00
# MAGIC       when carrier_line_haul RLIKE '\\d+(\\.[0-9]+)?' then carrier_line_haul::numeric
# MAGIC       else 0.00
# MAGIC     end as carrier_lhc,
# MAGIC     case
# MAGIC       when
# MAGIC         accessorial1 like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate1 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate1::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial2 like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate2 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate2::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial3 like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate3 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate3::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial4 like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate4 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate4::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial5 like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate5 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate5::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial6 like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate6 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate6::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial7 like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate7 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate7::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial8 like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate8 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate8::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end as carrier_fuel,
# MAGIC     case
# MAGIC       when
# MAGIC         accessorial1 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate1 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate1 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate1::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial2 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate2 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate2 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate2::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial3 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate3 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate3 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate3::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial4 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate4 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate4 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate4::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial5 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate5 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate5 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate5::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial6 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate6 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate6 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate6::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial7 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate7 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate7 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate7::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end
# MAGIC     + case
# MAGIC       when
# MAGIC         accessorial8 NOT like 'FUE%'
# MAGIC       then
# MAGIC         case
# MAGIC           when carrier_accessorial_rate8 like '%:%' then 0.00
# MAGIC           when
# MAGIC             carrier_accessorial_rate8 RLIKE '\\d+(\\.[0-9]+)?'
# MAGIC           then
# MAGIC             carrier_accessorial_rate8::numeric
# MAGIC           else 0.00
# MAGIC         end
# MAGIC       else 0.00
# MAGIC     end as carrier_acc,
# MAGIC     case
# MAGIC       when p1.office not in ('10', '34', '51', '54', '61', '62', '63', '64', '74') then 'USD'
# MAGIC       else coalesce(cad_currency.currency_type, aljex_customer_profiles.cust_country, 'CAD')
# MAGIC     end as cust_curr,
# MAGIC     case
# MAGIC       when c.name like '%CAD' then 'CAD'
# MAGIC       when c.name like 'CANADIAN R%' then 'CAD'
# MAGIC       when c.name like '%CAN' then 'CAD'
# MAGIC       when c.name like '%(CAD)%' then 'CAD'
# MAGIC       when c.name like '%-C' then 'CAD'
# MAGIC       when c.name like '%- C%' then 'CAD'
# MAGIC       when canada_carriers.canada_dot is not null then 'CAD'
# MAGIC       ELSE 'USD'
# MAGIC     end as carrier_curr,
# MAGIC     case
# MAGIC       when canada_conversions.conversion is null then average_conversions.avg_us_to_cad
# MAGIC       else canada_conversions.conversion
# MAGIC     end as conversion_rate,
# MAGIC     new_office_lookup_w_tgt.new_office as customer_office,
# MAGIC     case
# MAGIC       when
# MAGIC         p1.key_c_user = 'IMPORT'
# MAGIC       then
# MAGIC         case
# MAGIC           when customer_lookup.master_customer_name = 'ALTMAN PLANTS' then 'LAX'
# MAGIC           else 'DRAY'
# MAGIC         end
# MAGIC       when p1.key_c_user = 'EDILCR' then 'LTL'
# MAGIC       when p1.key_c_user is null then new_office_lookup_w_tgt.new_office
# MAGIC       else car.new_office
# MAGIC     end as carr_office,
# MAGIC     case
# MAGIC       when p1.equipment like '%LTL%' then 'LTL'
# MAGIC       else 'TL'
# MAGIC     end as tl_or_ltl,
# MAGIC     case
# MAGIC       when aljex_mode_types.equipment_mode = 'F' then 'Flatbed'
# MAGIC       when aljex_mode_types.equipment_mode is null then 'Dry Van'
# MAGIC       when aljex_mode_types.equipment_mode = 'R' then 'Reefer'
# MAGIC       when aljex_mode_types.equipment_mode = 'V' then 'Dry Van'
# MAGIC       when aljex_mode_types.equipment_mode = 'PORTD' then 'PORTD'
# MAGIC       when aljex_mode_types.equipment_mode = 'POWER ONLY' then 'POWER ONLY'
# MAGIC       when aljex_mode_types.equipment_mode = 'IMDL' then 'IMDL'
# MAGIC       else
# MAGIC         case
# MAGIC           when p1.equipment = 'RLTL' then 'Reefer'
# MAGIC           else 'Dry Van'
# MAGIC         end
# MAGIC     end as modee,
# MAGIC     invoice_total,
# MAGIC     p1.miles,
# MAGIC     p2.status,
# MAGIC     initcap(p1.origin_city) as origin_city,
# MAGIC     upper(p1.origin_state) as origin_state,
# MAGIC     initcap(p1.dest_city) as dest_city,
# MAGIC     upper(p1.dest_state) as dest_state,
# MAGIC     pickup_zip_code as origin_zip,
# MAGIC     consignee_zip_code as dest_zip,
# MAGIC     o.market as market_origin,
# MAGIC     d.market AS market_dest,
# MAGIC     CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC     --financial_calendar.financial_period_sorting,
# MAGIC     --financial_calendar.financial_year,
# MAGIC     c.dot_num::string,
# MAGIC     p1.delivery_date::date as delivery_date,
# MAGIC     null::date as bounced_date,
# MAGIC     null::date as rolled_date,
# MAGIC     p1.key_c_date as Booked_Date,
# MAGIC     p2.tag_creation_date::string as Tender_Date,
# MAGIC     null::date as cancelled_date,
# MAGIC     --pickup.Days_ahead as Days_Ahead,
# MAGIC     --pickup.prebookable as prebookable,
# MAGIC     p1.Pickup_Date as PickUp_Start,
# MAGIC     p1.Key_R_Date as PickUp_End,
# MAGIC     P1.pickup_Appt_Date as Pickup_Appointment_Date,
# MAGIC     P1.pickup_Appt_Date as scheduled_Date,
# MAGIC     P1.pickup_Appt_Date as New_Pickup_Appointment_Date,
# MAGIC     coalesce(Pickup_Date, Key_R_Date) as pickup_datetime,
# MAGIC     p1.Delivery_Date as Drop_Start,
# MAGIC     p1.Key_H_Date as Drop_End,
# MAGIC     coalesce(Delivery_Date, Key_H_Date) as Delivery_Datetime,
# MAGIC     c.name as carrier_name,
# MAGIC     case
# MAGIC       when sales.full_name is null then p2.sales_rep
# MAGIC       else sales.full_name
# MAGIC     end as salesrep,
# MAGIC     p2.trailer_number as Trailer_Num,
# MAGIC     p2.ref_num as Po_Number,
# MAGIC     p1.consignee as consignee_name,
# MAGIC     p1.consignee_address as consignee_Address,
# MAGIC     p1.consignee_address_line2,
# MAGIC     p1.pickup_address,
# MAGIC     p1.pickup_address_line2,
# MAGIC     c.mc_num as Mc_Number,
# MAGIC     sp.quoted_price as Spot_Revenue,
# MAGIC     sp.margin as Spot_Margin
# MAGIC   from
# MAGIC     bronze.projection_load_1 p1
# MAGIC       Left Join bronze.projection_load_2 p2
# MAGIC         on p1.id = p2.id ---join analytics.financial_calendar on p.pickup_date::date = financial_calendar.date::date
# MAGIC       left join aljex_spot_loads sp
# MAGIC         on p1.id = sp.id
# MAGIC       join bronze.new_office_lookup_w_tgt
# MAGIC         on p1.office = new_office_lookup_w_tgt.old_office
# MAGIC       LEFT JOIN bronze.market_lookup o
# MAGIC         ON LEFT(p1.pickup_zip_code, 3) = o.pickup_zip
# MAGIC       LEFT JOIN bronze.market_lookup d
# MAGIC         ON LEFT(p1.consignee_zip_code, 3) = d.pickup_zip
# MAGIC       left join bronze.cad_currency
# MAGIC         on p1.id::string = cad_currency.pro_number::string
# MAGIC       left join bronze.aljex_customer_profiles
# MAGIC         on p2.customer_id::string = aljex_customer_profiles.cust_id::string
# MAGIC       left join bronze.projection_carrier c
# MAGIC         on p1.carrier_id = c.id
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as PickUp_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as PickUp_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'PickUp'
# MAGIC       ) as papp
# MAGIC         ON p1.id = papp.loadnum
# MAGIC       LEFT JOIN (
# MAGIC         select
# MAGIC           max(relay_reference_number) as Loadnum,
# MAGIC           max(try_cast(pickup_proj_appt_datetime as TIMESTAMP)) as Drop_Start,
# MAGIC           max(try_cast(planning_appt_datetime as TIMESTAMP)) as Drop_End
# MAGIC         from
# MAGIC           bronze.load_appointment
# MAGIC         where
# MAGIC           Status = 'drop' --group by relay_reference_number
# MAGIC       ) as dapp
# MAGIC         ON p1.id = dapp.loadnum
# MAGIC       left join bronze.canada_carriers
# MAGIC         on p1.carrier_id::string = canada_carriers.canada_dot::string
# MAGIC       left join bronze.customer_lookup
# MAGIC         on left(p2.shipper, 32) = left(customer_lookup.aljex_customer_name, 32)
# MAGIC       left join bronze.aljex_mode_types
# MAGIC         on p1.equipment = bronze.aljex_mode_types.equipment_type
# MAGIC       join average_conversions
# MAGIC         on 1 = 1
# MAGIC       left join bronze.canada_conversions
# MAGIC         on p1.pickup_date::date = canada_conversions.ship_date::date
# MAGIC       left join bronze.aljex_user_report_listing
# MAGIC         on p1.key_c_user = aljex_user_report_listing.aljex_id
# MAGIC       left join bronze.aljex_user_report_listing sales
# MAGIC         on p2.sales_rep = sales.sales_rep
# MAGIC       left join bronze.aljex_user_report_listing z
# MAGIC         on p2.srv_rep = z.aljex_id
# MAGIC       left join bronze.relay_users
# MAGIC         on aljex_user_report_listing.full_name = relay_users.full_name
# MAGIC       left join bronze.new_office_lookup_w_tgt car
# MAGIC         on coalesce(aljex_user_report_listing.pnl_code, relay_users.office_id) = car.old_office
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     --[[and {{date_range}}]]
# MAGIC     and p2.status not in ('OPEN', 'ASSIGNED')
# MAGIC     and p2.status not like '%VOID%'
# MAGIC     and p1.key_c_user is not null
# MAGIC ), ---Select * from aljex_dat---Select * from aljex_data where use_date >= '2024-08-11' and use_date <= '2024-08-17'
# MAGIC system_union as (
# MAGIC   select distinct ----- Shipper
# MAGIC     relay_load as Load_Number,
# MAGIC     --0.5 as Load_Counter,
# MAGIC     use_date as Ship_Date,
# MAGIC     Mode,
# MAGIC    Equipment,
# MAGIC     --modee as Mode,
# MAGIC     --mode as Equipment,
# MAGIC     date_range_two.week_num as Week_Num,
# MAGIC     Case
# MAGIC       when customer_office = 'TGT' then 'PHI'
# MAGIC       else customer_office
# MAGIC     end as customer_office,
# MAGIC     --customer_office as Customer_Office,
# MAGIC     Case
# MAGIC       when carr_office = 'TGT' then 'PHI'
# MAGIC       else carr_office
# MAGIC     end as Carrier_Office,
# MAGIC     --carr_office as Carrier_Office,
# MAGIC     --'Shipper' as Office_Type,
# MAGIC     Carrier_Rep as Booked_By,
# MAGIC     --carr_office as Carr_Office,
# MAGIC     --financial_period_sorting as "Financial Period",
# MAGIC     --financial_year as "Financial Yr",
# MAGIC     mastername as customer_master,
# MAGIC     customer_name,
# MAGIC     customer_rep,
# MAGIC     salesrep,
# MAGIC     status as load_status,
# MAGIC     origin_city as Origin_City,
# MAGIC     origin_state as Origin_State,
# MAGIC     dest_city as Dest_City,
# MAGIC     dest_state as Dest_State,
# MAGIC     origin_zip as Origin_Zip,
# MAGIC     dest_zip as Dest_Zip,
# MAGIC     CONCAT(
# MAGIC       INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC     ) AS load_lane,
# MAGIC     market_origin as Market_origin,
# MAGIC     market_dest as Market_Deat,
# MAGIC     market_lane as Market_Lane,
# MAGIC     del_date as Delivery_Date,
# MAGIC     rolled_at as rolled_date,
# MAGIC     Booked_Date as Booked_Date,
# MAGIC     Tendering_Date as Tendered_Date,
# MAGIC     bounced_date as bounced_date,
# MAGIC     cancelled_date as Cancelled_Date,
# MAGIC     pu_appt_date as Pickup_Appointment_Date,
# MAGIC     scheduled_Date,
# MAGIC     --ays_ahead as Days_Ahead,
# MAGIC     --rebookable,
# MAGIC     PickUp_Start as Pickup_StartDate,
# MAGIC     PickUp_End as Pickup_EndDate,
# MAGIC     pickup_datetime as pickup_datetime,
# MAGIC     case
# MAGIC       when pickup_datetime::date > COALESCE(pu_appt_date::date, use_date::date) then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_pu,
# MAGIC     Drop_Start as Drop_Start,
# MAGIC     Drop_End as Drop_End,
# MAGIC     delivery_datetime as Delivery_Datetime,
# MAGIC     case
# MAGIC       when delivery_datetime::date > del_appt_date::date then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_del,
# MAGIC     invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC     --load_pickup.Days_ahead as Days_Ahead,
# MAGIC     CASE
# MAGIC       WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC       ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC     case
# MAGIC       when carrier_name is not null then '1'
# MAGIC       else '0'
# MAGIC     end as Load_flag,
# MAGIC     dot_num as DOT_Num,
# MAGIC     carrier_name as Carrier,
# MAGIC     total_miles::float as Miles,
# MAGIC     total_cust_rate.total_cust_rate::float as Revenue,
# MAGIC     total_carrier_rate.total_carrier_rate::float as Expense,
# MAGIC     (
# MAGIC       coalesce(total_cust_rate.total_cust_rate, 0)::float
# MAGIC       - coalesce(total_carrier_rate.total_carrier_rate, 0)::float
# MAGIC     ) as Margin,
# MAGIC     carrier_charges.carr_lhc as Linehaul,
# MAGIC     carrier_charges.carr_fsc as Fuel_Surcharge,
# MAGIC     carrier_charges.carr_acc as Other_Fess,
# MAGIC     Trailer_Num,
# MAGIC     use_relay_loads.Po_Number,
# MAGIC     use_relay_loads.consignee_name,
# MAGIC     Delivery_consignee_Address,
# MAGIC     pickup_Consignee_Address,
# MAGIC     use_relay_loads.Mc_Number,
# MAGIC     use_relay_loads.Spot_Revenue,
# MAGIC     use_relay_loads.Spot_Margin,
# MAGIC     'RELAY' as TMS_System
# MAGIC   from
# MAGIC     use_relay_loads
# MAGIC       left join total_cust_rate
# MAGIC         on use_relay_loads.relay_load = total_cust_rate.relay_reference_number
# MAGIC       left join total_carrier_rate
# MAGIC         on use_relay_loads.relay_load = total_carrier_rate.relay_reference_number
# MAGIC       left join bronze.date_range_two
# MAGIC         on use_date::date = date_range_two.date_date::date
# MAGIC       left join invoiced_amts
# MAGIC         on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join carrier_charges
# MAGIC         on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC     and combined_load = 'NO' --UNION
# MAGIC   union
# MAGIC   select distinct ---- Shipper - Hain Roloadn and Deb
# MAGIC     relay_load as relay_ref_num,
# MAGIC     --0.5 as Load_Counter,
# MAGIC     use_date,
# MAGIC     Mode,
# MAGIC 	Equipment,
# MAGIC     date_range_two.week_num,
# MAGIC     Case
# MAGIC       when customer_office = 'TGT' then 'PHI'
# MAGIC       else customer_office
# MAGIC     end as customer_office,
# MAGIC     --customer_office as Customer_Office,
# MAGIC     Case
# MAGIC       when carr_office = 'TGT' then 'PHI'
# MAGIC       else carr_office
# MAGIC     end as Carrier_Office,
# MAGIC     --carr_office as Carrier_Office,
# MAGIC     --'Shipper',
# MAGIC     Carrier_Rep,
# MAGIC     --carr_office,
# MAGIC     ---financial_period_sorting,
# MAGIC     --financial_year,
# MAGIC     mastername,
# MAGIC     customer_name,
# MAGIC     customer_rep,
# MAGIC     salesrep,
# MAGIC     status as load_status,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     origin_zip as Origin_Zip,
# MAGIC     dest_zip as Dest_Zip,
# MAGIC     CONCAT(
# MAGIC       INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC     ) AS load_lane,
# MAGIC     market_origin as Market_origin,
# MAGIC     market_dest as Market_Deat,
# MAGIC     market_lane as Market_Lane,
# MAGIC     del_date as Delivery_Date,
# MAGIC     rolled_at as rolled_date,
# MAGIC     Booked_Date as Booked_Date,
# MAGIC     Tendering_Date as Tendered_Date,
# MAGIC     bounced_date as bounced_date,
# MAGIC     cancelled_date as Cancelled_Date,
# MAGIC     pu_appt_date as Pickup_Appointment_Date,
# MAGIC     scheduled_Date,
# MAGIC     --Days_ahead as Days_Ahead,
# MAGIC     -- prebookable,
# MAGIC     PickUp_Start as Pickup_Date,
# MAGIC     PickUp_End as Pickup_EndDate,
# MAGIC     pickup_datetime as pickup_datetime,
# MAGIC     case
# MAGIC       when pickup_datetime::date > COALESCE(pu_appt_date::date, use_date::date) then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_pu,
# MAGIC     Drop_Start as Drop_Start,
# MAGIC     Drop_End as Drop_End,
# MAGIC     delivery_datetime as Delivery_Datetime,
# MAGIC     case
# MAGIC       when delivery_datetime::date > del_appt_date::date then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_del,
# MAGIC     invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC     --load_pickup.Days_ahead as Days_Ahead,
# MAGIC     CASE
# MAGIC       WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC       ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC     case
# MAGIC       when e.carrier_name is not null then '1'
# MAGIC       else '0'
# MAGIC     end as Load_flag,
# MAGIC     dot_num as DOT_Number,
# MAGIC     e.carrier_name as Carrier,
# MAGIC     total_miles::float,
# MAGIC     total_cust_rate.total_cust_rate::float as SplitRev,
# MAGIC     (projected_expense::float / 100.0) as splitexp,
# MAGIC     (
# MAGIC       coalesce(total_cust_rate.total_cust_rate, 0)::float
# MAGIC       - coalesce(projected_expense::float / 100.0, 0)::float
# MAGIC     ) as splitmargin,
# MAGIC     carrier_charges.carr_lhc as Linehaul,
# MAGIC     carrier_charges.carr_fsc as Fuel_Surcharge,
# MAGIC     carrier_charges.carr_acc as Other_Fess,
# MAGIC     Trailer_Num,
# MAGIC     use_relay_loads.Po_Number,
# MAGIC     use_relay_loads.consignee_name,
# MAGIC     Delivery_consignee_Address,
# MAGIC     pickup_Consignee_Address,
# MAGIC     use_relay_loads.Mc_Number,
# MAGIC     use_relay_loads.Spot_Revenue,
# MAGIC     use_relay_loads.Spot_Margin,
# MAGIC     'RELAY' as tms
# MAGIC   from
# MAGIC     use_relay_loads
# MAGIC       left join bronze.big_export_projection e
# MAGIC         on use_relay_loads.relay_load::float = e.load_number::float
# MAGIC       left join total_cust_rate
# MAGIC         on use_relay_loads.relay_load::float = total_cust_rate.relay_reference_number::float
# MAGIC       left join bronze.date_range_two
# MAGIC         on use_date::date = date_range_two.date_date::date
# MAGIC       left join invoiced_amts
# MAGIC         on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join carrier_charges
# MAGIC         on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and use_relay_loads.Mode_Filter = 'ltl'
# MAGIC     and e.dispatch_status != 'Cancelled'
# MAGIC     and e.carrier_name is not null
# MAGIC     and e.customer_id not in ('hain', 'roland', 'deb') ---UNION
# MAGIC   union
# MAGIC   select distinct -- Shipper
# MAGIC     relay_load as relay_ref_num,
# MAGIC     --0.5 as Load_Counter,
# MAGIC     use_date,
# MAGIC     Mode,
# MAGIC    Equipment,
# MAGIC     date_range_two.week_num,
# MAGIC     Case
# MAGIC       when customer_office = 'TGT' then 'PHI'
# MAGIC       else customer_office
# MAGIC     end as customer_office,
# MAGIC     --customer_office as Customer_Office,
# MAGIC     Case
# MAGIC       when carr_office = 'TGT' then 'PHI'
# MAGIC       else carr_office
# MAGIC     end as Carrier_Office,
# MAGIC     --carr_office as Carrier_Office,
# MAGIC     --'Shipper',
# MAGIC     Carrier_Rep,
# MAGIC     --carr_office,
# MAGIC     --financial_period_sorting,
# MAGIC     --financial_year,
# MAGIC     mastername,
# MAGIC     customer_name,
# MAGIC     customer_rep,
# MAGIC     salesrep,
# MAGIC     status as load_status,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     origin_zip as Origin_Zip,
# MAGIC     dest_zip as Dest_Zip,
# MAGIC     CONCAT(
# MAGIC       INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC     ) AS load_lane,
# MAGIC     market_origin as Market_origin,
# MAGIC     market_dest as Market_Deat,
# MAGIC     market_lane as Market_Lane,
# MAGIC     del_date as Delivery_Date,
# MAGIC     rolled_at as rolled_date,
# MAGIC     Booked_Date as Booked_Date,
# MAGIC     Tendering_Date as Tendered_Date,
# MAGIC     bounced_date as bounced_date,
# MAGIC     cancelled_date as Cancelled_Date,
# MAGIC     pu_appt_date as Pickup_Appointment_Date,
# MAGIC     scheduled_Date,
# MAGIC     --Days_ahead as Days_Ahead,
# MAGIC     --prebookable,
# MAGIC     PickUp_Start as Pickup_Date,
# MAGIC     PickUp_End as Pickup_EndDate,
# MAGIC     pickup_datetime as pickup_datetime,
# MAGIC     case
# MAGIC       when pickup_datetime::date > COALESCE(pu_appt_date::date, use_date::date) then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_pu,
# MAGIC     Drop_Start as Drop_Start,
# MAGIC     Drop_End as Drop_End,
# MAGIC     delivery_datetime as Delivery_Datetime,
# MAGIC     case
# MAGIC       when delivery_datetime::date > del_appt_date::date then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_del,
# MAGIC     invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC     --load_pickup.Days_ahead as Days_Ahead,
# MAGIC     CASE
# MAGIC       WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC       ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC     case
# MAGIC       when e.carrier_name is not null then '1'
# MAGIC       else '0'
# MAGIC     end as Load_flag,
# MAGIC     dot_num as DOT_Number,
# MAGIC     e.carrier_name as Carrier,
# MAGIC     total_miles::float,
# MAGIC     (
# MAGIC       (
# MAGIC         carrier_accessorial_expense
# MAGIC         + ceiling(
# MAGIC           case customer_id
# MAGIC             when
# MAGIC               'deb'
# MAGIC             then
# MAGIC               case carrier_id
# MAGIC                 when '300003979' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                 else
# MAGIC                   case
# MAGIC                     when
# MAGIC                       e.ship_date::date < '2019-03-09'
# MAGIC                     then
# MAGIC                       greatest(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                     else greatest(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                   end
# MAGIC               end
# MAGIC             when 'hain' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC             when 'roland' then carrier_linehaul_expense * 0.1365
# MAGIC             else 0
# MAGIC           end
# MAGIC           + carrier_linehaul_expense
# MAGIC         )
# MAGIC         + ceiling(
# MAGIC           ceiling(
# MAGIC             case customer_id
# MAGIC               when
# MAGIC                 'deb'
# MAGIC               then
# MAGIC                 case carrier_id
# MAGIC                   when '300003979' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                   else
# MAGIC                     case
# MAGIC                       when
# MAGIC                         e.ship_date::date < '2019-03-09'
# MAGIC                       then
# MAGIC                         greatest(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                       else greatest(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                     end
# MAGIC                 end
# MAGIC               when 'hain' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC               when 'roland' then carrier_linehaul_expense * 0.1365
# MAGIC               else 0
# MAGIC             end
# MAGIC             + carrier_linehaul_expense
# MAGIC           )
# MAGIC           * (
# MAGIC             case carrier_linehaul_expense
# MAGIC               when 0 then 0
# MAGIC               else carrier_fuel_expense::float / carrier_linehaul_expense::float
# MAGIC             end
# MAGIC           )
# MAGIC         )
# MAGIC       )
# MAGIC       / 100
# MAGIC     )::float as split_rev,
# MAGIC     (projected_expense::float / 100.0) as split_exp,
# MAGIC     (
# MAGIC       coalesce(
# MAGIC         (
# MAGIC           (
# MAGIC             carrier_accessorial_expense
# MAGIC             + ceiling(
# MAGIC               case customer_id
# MAGIC                 when
# MAGIC                   'deb'
# MAGIC                 then
# MAGIC                   case carrier_id
# MAGIC                     when '300003979' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                     else
# MAGIC                       case
# MAGIC                         when
# MAGIC                           e.ship_date::date < '2019-03-09'
# MAGIC                         then
# MAGIC                           greatest(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                         else greatest(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                       end
# MAGIC                   end
# MAGIC                 when 'hain' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC                 when 'roland' then carrier_linehaul_expense * 0.1365
# MAGIC                 else 0
# MAGIC               end
# MAGIC               + carrier_linehaul_expense
# MAGIC             )
# MAGIC             + ceiling(
# MAGIC               ceiling(
# MAGIC                 case customer_id
# MAGIC                   when
# MAGIC                     'deb'
# MAGIC                   then
# MAGIC                     case carrier_id
# MAGIC                       when
# MAGIC                         '300003979'
# MAGIC                       then
# MAGIC                         greatest(carrier_linehaul_expense * (10.0 / 100.0), 1500)
# MAGIC                       else
# MAGIC                         case
# MAGIC                           when
# MAGIC                             e.ship_date::date < '2019-03-09'
# MAGIC                           then
# MAGIC                             greatest(carrier_linehaul_expense * (8.0 / 100.0), 1000)
# MAGIC                           else greatest(carrier_linehaul_expense * (9.0 / 100.0), 1000)
# MAGIC                         end
# MAGIC                     end
# MAGIC                   when 'hain' then greatest(carrier_linehaul_expense * (10.0 / 100.0), 1000)
# MAGIC                   when 'roland' then carrier_linehaul_expense * 0.1365
# MAGIC                   else 0
# MAGIC                 end
# MAGIC                 + carrier_linehaul_expense
# MAGIC               )
# MAGIC               * (
# MAGIC                 case carrier_linehaul_expense
# MAGIC                   when 0 then 0
# MAGIC                   else carrier_fuel_expense::float / carrier_linehaul_expense::float
# MAGIC                 end
# MAGIC               )
# MAGIC             )
# MAGIC           )
# MAGIC           / 100
# MAGIC         ),
# MAGIC         0
# MAGIC       )::float
# MAGIC       - coalesce((projected_expense::float / 100.0), 0)
# MAGIC     )::float as split_margin,
# MAGIC     carrier_charges.carr_lhc as Linehaul,
# MAGIC     carrier_charges.carr_fsc as Fuel_Surcharge,
# MAGIC     carrier_charges.carr_acc as Other_Fess,
# MAGIC     Trailer_Num,
# MAGIC     use_relay_loads.Po_Number,
# MAGIC     use_relay_loads.consignee_name,
# MAGIC     Delivery_consignee_Address,
# MAGIC     pickup_Consignee_Address,
# MAGIC     use_relay_loads.Mc_Number,
# MAGIC     use_relay_loads.Spot_Revenue,
# MAGIC     use_relay_loads.Spot_Margin,
# MAGIC     'RELAY' as tms
# MAGIC   from
# MAGIC     use_relay_loads
# MAGIC       left join bronze.big_export_projection e
# MAGIC         on use_relay_loads.relay_load::float = e.load_number::float
# MAGIC       left join bronze.date_range_two
# MAGIC         on use_date::date = date_range_two.date_date::date
# MAGIC       left join invoiced_amts
# MAGIC         on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join carrier_charges
# MAGIC         on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and use_relay_loads.Mode_Filter = 'ltl- (hain,deb,roland)'
# MAGIC     and e.dispatch_status != 'Cancelled'
# MAGIC     and e.carrier_name is not null
# MAGIC     and e.customer_id in ('hain', 'roland', 'deb') --UNION
# MAGIC   union
# MAGIC   select distinct -- Shipper
# MAGIC     relay_load as relay_ref_num,
# MAGIC     --0.5 as Load_Counter,
# MAGIC     use_date,
# MAGIC     use_relay_loads.Mode,
# MAGIC     use_relay_loads.Equipment,
# MAGIC     date_range_two.week_num,
# MAGIC     Case
# MAGIC       when customer_office = 'TGT' then 'PHI'
# MAGIC       else customer_office
# MAGIC     end as customer_office,
# MAGIC     --customer_office as Customer_Office,
# MAGIC     Case
# MAGIC       when carr_office = 'TGT' then 'PHI'
# MAGIC       else carr_office
# MAGIC     end as Carrier_Office,
# MAGIC     --carr_office as Carrier_Office,
# MAGIC     --'Shipper',
# MAGIC     Carrier_Rep,
# MAGIC     --carr_office,
# MAGIC     --financial_period_sorting,
# MAGIC     --financial_year,
# MAGIC     mastername,
# MAGIC     customer_name,
# MAGIC     customer_rep,
# MAGIC     salesrep,
# MAGIC     status as load_status,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     origin_zip as Origin_Zip,
# MAGIC     dest_zip as Dest_Zip,
# MAGIC     CONCAT(
# MAGIC       INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC     ) AS load_lane,
# MAGIC     market_origin as Market_origin,
# MAGIC     market_dest as Market_Deat,
# MAGIC     market_lane as Market_Lane,
# MAGIC     del_date as Delivery_Date,
# MAGIC     rolled_at as rolled_date,
# MAGIC     Booked_Date as Booked_Date,
# MAGIC     Tendering_Date as Tendered_Date,
# MAGIC     bounced_date as bounced_date,
# MAGIC     cancelled_date as Cancelled_Date,
# MAGIC     pu_appt_date as Pickup_Appointment_Date,
# MAGIC     scheduled_Date,
# MAGIC     -- Days_ahead as Days_Ahead,
# MAGIC     --prebookable,
# MAGIC     PickUp_Start as Pickup_Date,
# MAGIC     PickUp_End as Pickup_EndDate,
# MAGIC     pickup_datetime as pickup_datetime,
# MAGIC     case
# MAGIC       when pickup_datetime::date > COALESCE(pu_appt_date::date, use_date::date) then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_pu,
# MAGIC     Drop_Start as Drop_Start,
# MAGIC     Drop_End as Drop_End,
# MAGIC     delivery_datetime as Delivery_Datetime,
# MAGIC     case
# MAGIC       when delivery_datetime::date > del_appt_date::date then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_del,
# MAGIC     invoiced_amts.invoiced_at::date as invoiced_Date,
# MAGIC     --load_pickup.Days_ahead as Days_Ahead,
# MAGIC     CASE
# MAGIC       WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC       ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC     case
# MAGIC       when use_relay_loads.carrier_name is not null then '1'
# MAGIC       else '0'
# MAGIC     end as Load_flag,
# MAGIC     dot_num as DOT_Number,
# MAGIC     use_relay_loads.carrier_name as Carrier,
# MAGIC     total_miles::float,
# MAGIC     total_cust_rate.total_cust_rate::float as split_rev,
# MAGIC     total_carrier_rate.total_carrier_rate::float as split_exp,
# MAGIC     coalesce(total_cust_rate.total_cust_rate, 0)::float
# MAGIC     - coalesce((total_carrier_rate.total_carrier_rate::float / 2), 0)::float as split_margin,
# MAGIC     carrier_charges.carr_lhc as Linehaul,
# MAGIC     carrier_charges.carr_fsc as Fuel_Surcharge,
# MAGIC     carrier_charges.carr_acc as Other_Fess,
# MAGIC     Trailer_Num,
# MAGIC     Po_Number,
# MAGIC     consignee_name,
# MAGIC     Delivery_consignee_Address,
# MAGIC     pickup_Consignee_Address,
# MAGIC     Mc_Number,
# MAGIC     use_relay_loads.Spot_Revenue,
# MAGIC     use_relay_loads.Spot_Margin,
# MAGIC     'RELAY' as tms
# MAGIC   from
# MAGIC     use_relay_loads
# MAGIC       left join total_cust_rate
# MAGIC         on use_relay_loads.relay_load = total_cust_rate.relay_reference_number
# MAGIC       left join total_carrier_rate
# MAGIC         on use_relay_loads.combined_load = total_carrier_rate.relay_reference_number
# MAGIC       left join combined_loads_mine
# MAGIC         on use_relay_loads.relay_load::float = combined_loads_mine.combine_new_rrn
# MAGIC       left join bronze.date_range_two
# MAGIC         on use_date::date = date_range_two.date_date::date
# MAGIC       left join invoiced_amts
# MAGIC         on use_relay_loads.relay_load = invoiced_amts.relay_reference_number
# MAGIC       left join carrier_charges
# MAGIC         on use_relay_loads.relay_load = carrier_charges.relay_reference_number --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and use_relay_loads.Mode_Filter not like 'ltl%'
# MAGIC     and combined_load != 'NO'
# MAGIC ),
# MAGIC Aljex_updated as (
# MAGIC   select distinct -- Shipper  ----- Need to change from here.
# MAGIC     id::float as relay_ref_num,
# MAGIC     --0.5 as Load_Counter,
# MAGIC     use_date,
# MAGIC     tl_or_ltl as Mode,
# MAGIC     Case
# MAGIC       when modee = 'Dry Van' then 'Dry Van'
# MAGIC       when modee = 'Reefer' then 'Reefer'
# MAGIC       when modee = 'Flatbed' then 'Flatbed'
# MAGIC       when modee = 'POWER ONLY' THEN 'Power Only'
# MAGIC       when modee = 'IMDL' THEN 'Intermodal'
# MAGIC       when modee = 'PORTD' THEN 'Drayage'
# MAGIC       else null
# MAGIC     end as Equipment,
# MAGIC     date_range_two.week_num,
# MAGIC     Case
# MAGIC       when customer_office = 'TGT' then 'PHI'
# MAGIC       else customer_office
# MAGIC     end as customer_office,
# MAGIC     --customer_office as Customer_Office,
# MAGIC     Case
# MAGIC       when carr_office = 'TGT' then 'PHI'
# MAGIC       else carr_office
# MAGIC     end as Carrier_Office,
# MAGIC     --carr_office as Carrier_Office,
# MAGIC     --'Shipper',
# MAGIC     key_c_user,
# MAGIC     --carr_office,
# MAGIC     --financial_period_sorting,
# MAGIC     --financial_year,
# MAGIC     mastername,
# MAGIC     customer_name,
# MAGIC     customer_rep,
# MAGIC     salesrep,
# MAGIC     status as load_status,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     origin_zip as Origin_Zip,
# MAGIC     dest_zip as Dest_Zip,
# MAGIC     CONCAT(
# MAGIC       INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC     ) AS load_lane,
# MAGIC     market_origin as Market_origin,
# MAGIC     market_dest as Market_Deat,
# MAGIC     market_lane as Market_Lane,
# MAGIC     delivery_date as Delivery_Date,
# MAGIC     rolled_date as Rolled_Date,
# MAGIC     Booked_Date as Booked_Date,
# MAGIC     Tender_Date as Tendered_Date,
# MAGIC     bounced_date as bounced_date,
# MAGIC     cancelled_date as Cancelled_Date,
# MAGIC     aljex_data.Pickup_Appointment_Date,
# MAGIC     scheduled_Date,
# MAGIC     --Days_ahead as Days_Ahead,
# MAGIC     --prebookable,
# MAGIC     PickUp_Start as Pickup_Date,
# MAGIC     PickUp_End as Pickup_EndDate,
# MAGIC     pickup_datetime as pickup_datetime,
# MAGIC     case
# MAGIC       when
# MAGIC         pickup_datetime::date > COALESCE(Pickup_Appointment_Date::date, use_date::date)
# MAGIC       then
# MAGIC         'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_pu,
# MAGIC     Drop_Start as Drop_Start,
# MAGIC     Drop_End as Drop_End,
# MAGIC     Delivery_Datetime as Delivery_Datetime,
# MAGIC     case
# MAGIC       when delivery_datetime::date > aljex_data.delivery_date::date then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_del,
# MAGIC     null::date as invoiced_Date,
# MAGIC     --load_pickup.Days_ahead as Days_Ahead,
# MAGIC     CASE
# MAGIC       WHEN customer_office <> carr_office THEN 'Crossbooked'
# MAGIC       ELSE 'Same Office'
# MAGIC     END AS Booking_type,
# MAGIC     case
# MAGIC       when carrier_name is not null then '1'
# MAGIC       else '0'
# MAGIC     end as Load_flag,
# MAGIC     dot_num as DOT_Number,
# MAGIC     aljex_data.carrier_name as Carrier,
# MAGIC     case miles Rlike '^\d+(.\d+)?$'
# MAGIC       when true then miles::float
# MAGIC       else 0
# MAGIC     end as miles,
# MAGIC     (
# MAGIC       case
# MAGIC         when cust_curr = 'USD' then invoice_total::float
# MAGIC         else invoice_total::float / aljex_data.conversion_rate::float
# MAGIC       end
# MAGIC     ) as split_rev,
# MAGIC     (
# MAGIC       case
# MAGIC         when carrier_curr = 'USD' then final_carrier_rate::float
# MAGIC         else final_carrier_rate::float / aljex_data.conversion_rate
# MAGIC       end
# MAGIC     ) as split_exp,
# MAGIC     (
# MAGIC       coalesce(
# MAGIC         case
# MAGIC           when cust_curr = 'USD' then invoice_total::float
# MAGIC           else invoice_total::float / aljex_data.conversion_rate::float
# MAGIC         end,
# MAGIC         0
# MAGIC       )
# MAGIC       - coalesce(
# MAGIC         case
# MAGIC           when carrier_curr = 'USD' then final_carrier_rate::float
# MAGIC           else final_carrier_rate::float / aljex_data.conversion_rate
# MAGIC         end,
# MAGIC         0
# MAGIC       )
# MAGIC     ) as split_margin,
# MAGIC     carrier_lhc as Linehaul,
# MAGIC     carrier_fuel as Fuel_Surcharge,
# MAGIC     carrier_acc as Other_Fess,
# MAGIC     Trailer_Num,
# MAGIC     Po_Number,
# MAGIC     consignee_name,
# MAGIC     CONCAT(
# MAGIC       INITCAP(consignee_Address),
# MAGIC       ', ',
# MAGIC       INITCAP(consignee_address_line2),
# MAGIC       ', ',
# MAGIC       INITCAP(dest_city),
# MAGIC       ', ',
# MAGIC       dest_state,
# MAGIC       ', ',
# MAGIC       Dest_Zip
# MAGIC     ) AS Delivery_consignee_Address,
# MAGIC     CONCAT(
# MAGIC       INITCAP(pickup_address), ', ', INITCAP(origin_city), ', ', origin_state, ', ', origin_Zip
# MAGIC     ) AS Pickup_consignee_Address,
# MAGIC     Mc_Number,
# MAGIC     aljex_data.Spot_Revenue,
# MAGIC     aljex_data.Spot_Margin,
# MAGIC     'ALJEX' as tms
# MAGIC   from
# MAGIC     aljex_data
# MAGIC       left join bronze.date_range_two
# MAGIC         on use_date::date = date_range_two.date_date::date
# MAGIC ),
# MAGIC ordered as (
# MAGIC   select
# MAGIC     *,
# MAGIC     rank() OVER (
# MAGIC         PARTITION BY Load_Number
# MAGIC         ORDER BY Pickup_Appointment_Date, Delivery_Date, Tendered_Date DESC
# MAGIC       ) AS rank
# MAGIC   from
# MAGIC     system_union
# MAGIC ),
# MAGIC final as (
# MAGIC   select
# MAGIC     Load_Number,
# MAGIC     Ship_Date,
# MAGIC     Mode,
# MAGIC     Equipment,
# MAGIC     Week_Num,
# MAGIC     customer_office,
# MAGIC     Carrier_Office,
# MAGIC     Booked_By,
# MAGIC     customer_master,
# MAGIC     customer_name,
# MAGIC     customer_rep,
# MAGIC     salesrep,
# MAGIC     load_status,
# MAGIC     Origin_City,
# MAGIC     Origin_State,
# MAGIC     Dest_City,
# MAGIC     Dest_State,
# MAGIC     Origin_Zip,
# MAGIC     Dest_Zip,
# MAGIC     load_lane,
# MAGIC     Market_origin,
# MAGIC     Market_Deat,
# MAGIC     Market_Lane,
# MAGIC     Delivery_Date,
# MAGIC     rolled_date,
# MAGIC     Booked_Date,
# MAGIC     Tendered_Date,
# MAGIC     bounced_date,
# MAGIC     Cancelled_Date,
# MAGIC     Pickup_Appointment_Date,
# MAGIC     scheduled_Date,
# MAGIC     Pickup_startdate,
# MAGIC     Pickup_enddate,
# MAGIC     pickup_datetime,
# MAGIC     ot_pu,
# MAGIC     drop_start,
# MAGIC     drop_end,
# MAGIC     Delivery_Datetime,
# MAGIC     ot_del,
# MAGIC     invoiced_Date,
# MAGIC     Booking_type,
# MAGIC     Load_flag,
# MAGIC     DOT_Num,
# MAGIC     Carrier,
# MAGIC     Miles,
# MAGIC     Revenue,
# MAGIC     Expense,
# MAGIC     Margin,
# MAGIC     Linehaul,
# MAGIC     Fuel_Surcharge,
# MAGIC     Other_Fess,
# MAGIC     Trailer_Num,
# MAGIC     Po_Number,
# MAGIC     consignee_name,
# MAGIC     Delivery_consignee_Address,
# MAGIC     Pickup_consignee_Address,
# MAGIC     Mc_Number,
# MAGIC     Spot_Revenue,
# MAGIC     Spot_Margin,
# MAGIC    mx.max_buy::float / 100.00 as Max_buy_rate,
# MAGIC    mr.Market_Buy_Rate,
# MAGIC     TMS_System
# MAGIC   from
# MAGIC     ordered
# MAGIC     left join bronze.sourcing_max_buy_v2 mx on ordered.Load_Number = mx.relay_reference_number 
# MAGIC   left join pricing_nfi_load_predictions mr on ordered.Load_Number = mr.MR_id
# MAGIC   where
# MAGIC     rank = 1
# MAGIC   union
# MAGIC   SELECT
# MAGIC     Aljex_updated.relay_ref_num,
# MAGIC     Aljex_updated.use_date,
# MAGIC     Aljex_updated.Mode,
# MAGIC     Aljex_updated.Equipment,
# MAGIC     Aljex_updated.week_num,
# MAGIC     Aljex_updated.customer_office,
# MAGIC     Aljex_updated.Carrier_Office,
# MAGIC     Aljex_updated.key_c_user,
# MAGIC     Aljex_updated.mastername,
# MAGIC     Aljex_updated.customer_name,
# MAGIC     Aljex_updated.customer_rep,
# MAGIC     Aljex_updated.salesrep,
# MAGIC     Aljex_updated.load_status,
# MAGIC     Aljex_updated.origin_city,
# MAGIC     Aljex_updated.origin_state,
# MAGIC     Aljex_updated.dest_city,
# MAGIC     Aljex_updated.dest_state,
# MAGIC     Aljex_updated.Origin_Zip,
# MAGIC     Aljex_updated.Dest_Zip,
# MAGIC     Aljex_updated.load_lane,
# MAGIC     Aljex_updated.Market_origin,
# MAGIC     Aljex_updated.Market_Deat,
# MAGIC     Aljex_updated.Market_Lane,
# MAGIC     Aljex_updated.Delivery_Date,
# MAGIC     Aljex_updated.Rolled_Date,
# MAGIC     Aljex_updated.Booked_Date,
# MAGIC     Aljex_updated.Tendered_Date,
# MAGIC     Aljex_updated.bounced_date,
# MAGIC     Aljex_updated.Cancelled_Date,
# MAGIC     Aljex_updated.Pickup_Appointment_Date,
# MAGIC     Aljex_updated.scheduled_Date,
# MAGIC     Aljex_updated.Pickup_Date,
# MAGIC     Aljex_updated.Pickup_EndDate,
# MAGIC     Aljex_updated.pickup_datetime,
# MAGIC     Aljex_updated.ot_pu,
# MAGIC     Aljex_updated.Drop_Start,
# MAGIC     Aljex_updated.Drop_End,
# MAGIC     Aljex_updated.Delivery_Datetime,
# MAGIC     Aljex_updated.ot_del,
# MAGIC     Aljex_updated.invoiced_Date,
# MAGIC     Aljex_updated.Booking_type,
# MAGIC     Aljex_updated.Load_flag,
# MAGIC     Aljex_updated.DOT_Number,
# MAGIC     Aljex_updated.Carrier,
# MAGIC     Aljex_updated.miles,
# MAGIC     Aljex_updated.split_rev,
# MAGIC     Aljex_updated.split_exp,
# MAGIC     Aljex_updated.split_margin,
# MAGIC     Aljex_updated.Linehaul,
# MAGIC     Aljex_updated.Fuel_Surcharge,
# MAGIC     Aljex_updated.Other_Fess,
# MAGIC     Aljex_updated.Trailer_Num,
# MAGIC     Aljex_updated.Po_Number,
# MAGIC     Aljex_updated.consignee_name,
# MAGIC     Aljex_updated.Delivery_consignee_Address,
# MAGIC     Aljex_updated.Pickup_consignee_Address,
# MAGIC     Aljex_updated.Mc_Number,
# MAGIC     Aljex_updated.Spot_Revenue,
# MAGIC     Aljex_updated.Spot_Margin,
# MAGIC     null as Max_buy_rate,
# MAGIC     mr.Market_Buy_Rate,
# MAGIC     tms
# MAGIC FROM Aljex_updated
# MAGIC left join pricing_nfi_load_predictions mr on relay_ref_num = mr.MR_id
# MAGIC   --left join  bronze.load_pickup on use_relay_loads.relay_load  = load_pickup.Load_number
# MAGIC   UNION
# MAGIC   select distinct -- Shipper  ----- Need to change from here.
# MAGIC     Loadnum::float as relay_ref_num,
# MAGIC     --0.5 as Load_Counter,
# MAGIC     coalesce(arrive_pu_date::date, depart_pu_date::date, pu_appt::date) as use_date,
# MAGIC     load_mode as Mode,
# MAGIC     service_line as Equipment,
# MAGIC     date_range_two.week_num as week_num,
# MAGIC     'DBG' as customer_office,
# MAGIC     --customer_office as Customer_Office,
# MAGIC     'DBG' as Carrier_Office,
# MAGIC     --carr_office as Carrier_Office,
# MAGIC     --'Shipper',
# MAGIC     null as key_c_user,
# MAGIC     --carr_office,
# MAGIC     --financial_period_sorting,
# MAGIC     --financial_year,
# MAGIC     Shipper as mastername,
# MAGIC     shipper as customer_name,
# MAGIC     Null as customer_rep,
# MAGIC     Null as salesrep,
# MAGIC     Load_status as load_status,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     origin_zip as Origin_Zip,
# MAGIC     dest_zip as Dest_Zip,
# MAGIC     CONCAT(
# MAGIC       INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC     ) AS load_lane,
# MAGIC     Null as Market_origin,
# MAGIC     Null as Market_Deat,
# MAGIC     Null as Market_Lane,
# MAGIC     del_appt as Delivery_Date,
# MAGIC     rolled_date as rolled_date,
# MAGIC     Booked_Date as Booked_Date,
# MAGIC     Tendered_Date as Tendered_Date,
# MAGIC     bounced_date as bounced_date,
# MAGIC     Cancelled_Date as Cancelled_Date,
# MAGIC     pu_appt as Pickup_Appointment_Date,
# MAGIC     pu_appt as scheduled_Date,
# MAGIC     --Null as Days_Ahead,
# MAGIC     -- Null as prebookable,
# MAGIC     arrive_pu_date as Pickup_Date,
# MAGIC     depart_pu_date as Pickup_EndDate,
# MAGIC     CAST(Pickup_Appointment_Date AS TIMESTAMP) as pickup_datetime,
# MAGIC     case
# MAGIC       when pickup_datetime::date > COALESCE(arrive_pu_date::date, pu_appt::date) then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_pu,
# MAGIC     arrive_del_date as Delivery_In,
# MAGIC     depart_del_date as Delivery_out,
# MAGIC     cast(depart_del_date as TIMESTAMP) as Delivery_Datetime,
# MAGIC     case
# MAGIC       when delivery_datetime::date > del_appt::date then 'late'
# MAGIC       else 'ontime'
# MAGIC     end as ot_del,
# MAGIC     first_invoice_sent_date::date as invoiced_Date,
# MAGIC     --load_pickup.Days_ahead as Days_Ahead,
# MAGIC     Null as Booking_type,
# MAGIC     case
# MAGIC       when carrier is not null then '1'
# MAGIC       else '0'
# MAGIC     end as Load_flag,
# MAGIC     dot as DOT_Number,
# MAGIC     transfix_data.carrier as Carrier,
# MAGIC     calc_miles::float,
# MAGIC     transfix_data.total_rev::float as split_rev,
# MAGIC     transfix_data.total_exp::float::float as split_exp,
# MAGIC     coalesce(transfix_data.total_rev, 0)::float
# MAGIC     - coalesce((transfix_data.total_exp::float), 0)::float as split_margin,
# MAGIC     transfix_data.carr_lhc as Linehaul,
# MAGIC     transfix_data.carr_fuel as Fuel_Surcharge,
# MAGIC     transfix_data.carr_acc as Other_Fess,
# MAGIC     Null as Trailer_Num,
# MAGIC     null as Po_Number,
# MAGIC     null as consignee_name,
# MAGIC     Null as Delivery_consignee_Address,
# MAGIC     Null as pickup_consignee_Address,
# MAGIC     null as Mc_Number,
# MAGIC     Null as Spot_Revenue,
# MAGIC     Null as Spot_Margin,
# MAGIC     Null as Max_buy_rate,
# MAGIC     Null as Market_Buy_Rate,
# MAGIC     'Transfix' as tms
# MAGIC   from
# MAGIC     transfix_data
# MAGIC       join analytics.dim_financial_calendar
# MAGIC         on coalesce(arrive_pu_date::date, depart_pu_date::date, pu_appt::date)::date
# MAGIC         = analytics.dim_financial_calendar.date::date
# MAGIC       left join date_range_two
# MAGIC         on analytics.dim_financial_calendar.date::date
# MAGIC         = date_range_two.date_date::date
# MAGIC ),
# MAGIC maxbuy as (
# MAGIC   select
# MAGIC     final.*,
# MAGIC     CASE
# MAGIC       WHEN TRY_CAST(Booked_date AS DATE) < TRY_CAST(ship_date AS DATE) THEN 'PreBooked'
# MAGIC       ELSE 'SameDay'
# MAGIC     END AS PreBookStatus,
# MAGIC     datediff(try_cast(Ship_Date as date), try_cast(Tendered_Date as date)) as Days_ahead,
# MAGIC     cs.AdminFees_Cust,
# MAGIC     cs.TransitandRouting_Cust,
# MAGIC     cs.DelandPickup_Cust,
# MAGIC     cs.EquipmentandVehicle_Cust,
# MAGIC     cs.Loadandunload_Cust,
# MAGIC     cs.Miscellaneous_Cust,
# MAGIC     cs.PermitsandCompliance_cust,
# MAGIC     ca.AdminFees_Carrier,
# MAGIC     ca.TransitandRouting_Carrier,
# MAGIC     ca.DelandPickup_Carrier,
# MAGIC     ca.EquipmentandVehicle_Carrier,
# MAGIC     ca.Loadandunload_Carrier,
# MAGIC     ca.Miscellaneous_Carrier,
# MAGIC     ca.PermitsandCompliance_carrier
# MAGIC   from
# MAGIC     final
# MAGIC       left join superinsight.accessorial_customer cs
# MAGIC         on final.load_number = cs.load_number
# MAGIC         and final.TMS_System = cs.tms
# MAGIC       left join superinsight.accessorial_carrier ca
# MAGIC         on final.load_number = ca.load_number
# MAGIC         and final.TMS_System = ca.tms
# MAGIC )
# MAGIC
# MAGIC   ,
# MAGIC tender as (
# MAGIC   SELECT
# MAGIC     tender_reference_numbers_projection.relay_reference_number as id,
# MAGIC     tenderer,
# MAGIC     tendering_acceptance.accepted_at,
# MAGIC     integration_tender_mapped_projection_v2.tendered_at
# MAGIC   FROM
# MAGIC     bronze.tender_reference_numbers_projection
# MAGIC       LEFT JOIN bronze.tendering_acceptance
# MAGIC         ON tender_reference_numbers_projection.tender_id = tendering_acceptance.tender_id
# MAGIC       LEFT JOIN bronze.integration_tender_mapped_projection_v2
# MAGIC         ON tendering_acceptance.shipment_id = integration_tender_mapped_projection_v2.shipment_id
# MAGIC         AND event_type = 'tender_mapped'
# MAGIC       JOIN analytics.dim_date
# MAGIC         ON cast(accepted_At as DATE) = analytics.dim_date.Calendar_Date
# MAGIC   WHERE
# MAGIC     `accepted?` = 'true'
# MAGIC     AND is_split = 'false'
# MAGIC ),
# MAGIC max as (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     rank() OVER (PARTITION BY id ORDER BY tendered_at, accepted_at DESC) AS rank
# MAGIC   from
# MAGIC     tender
# MAGIC ),
# MAGIC tender_final AS (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     tenderer,
# MAGIC     CASE
# MAGIC       WHEN tenderer IN ('orderful', '3gtms') THEN 'EDI'
# MAGIC       WHEN tenderer = 'vooma-orderful' THEN 'Vooma'
# MAGIC       WHEN tenderer IS NULL THEN 'Manual'
# MAGIC       ELSE tenderer
# MAGIC     END AS Tender_Source_Type
# MAGIC   FROM
# MAGIC     max
# MAGIC   WHERE
# MAGIC     rank = 1
# MAGIC )
# MAGIC select
# MAGIC   maxbuy.*,tender_final.Tender_Source_Type
# MAGIC from
# MAGIC   maxbuy
# MAGIC     left join tender_final
# MAGIC       on maxbuy.load_number = tender_final.id

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table analytics.Fact_Load_GenAI_NonSplit_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Fact_Load_GenAI_NonSplit

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Fact_Load_GenAI_NonSplit;
# MAGIC
# MAGIC INSERT INTO analytics.Fact_Load_GenAI_NonSplit
# MAGIC SELECT *
# MAGIC FROM analytics.Fact_Load_GenAI_NonSplit_Temp
# MAGIC

# COMMAND ----------

