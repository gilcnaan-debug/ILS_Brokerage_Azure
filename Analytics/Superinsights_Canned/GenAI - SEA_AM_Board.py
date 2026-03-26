# Databricks notebook source
# MAGIC %md
# MAGIC ## ViewToTable
# MAGIC * **Description:** To Convert a View into table for Super_Insights
# MAGIC * **Created Date:** 15/10/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 15/10/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating View

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC create or replace view bronze.SEA_AM_Board_GenAI as ( WITH new_pu_appt AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         b.relay_reference_number,
# MAGIC         CAST(s.appointment_datetime AS TIMESTAMP) AS planning_appt_datetime,
# MAGIC         CAST(s.window_start_datetime AS TIMESTAMP),
# MAGIC         CAST(s.window_end_datetime AS TIMESTAMP),
# MAGIC         s.schedule_reference,
# MAGIC         s.address_1,
# MAGIC         COALESCE(
# MAGIC             CAST(s.appointment_datetime AS TIMESTAMP),
# MAGIC             CAST(s.window_start_datetime AS TIMESTAMP),
# MAGIC             CAST(s.window_end_datetime AS TIMESTAMP)
# MAGIC         ) AS new_pu_appt
# MAGIC     FROM
# MAGIC         booking_projection b
# MAGIC     LEFT JOIN 
# MAGIC         planning_stop_schedule s 
# MAGIC         ON b.relay_reference_number = s.relay_reference_number
# MAGIC         AND s.stop_type = 'pickup'
# MAGIC         AND s.sequence_number = 1
# MAGIC         AND s.`removed?` = 'false'
# MAGIC     LEFT JOIN 
# MAGIC         customer_profile_projection r 
# MAGIC         ON b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC     WHERE
# MAGIC         --r.profit_center IN ('SCR', 'SEA')
# MAGIC          b.status NOT IN ('cancelled', 'bounced')
# MAGIC )
# MAGIC
# MAGIC , to_get_avg as  (	
# MAGIC select distinct 
# MAGIC ship_date,
# MAGIC conversion as us_to_cad,
# MAGIC us_to_cad as cad_to_us
# MAGIC from canada_conversions
# MAGIC order by ship_date desc 
# MAGIC limit 7 )
# MAGIC
# MAGIC , average_conversions as  (
# MAGIC select  
# MAGIC avg(us_to_cad) as avg_us_to_cad,
# MAGIC avg(cad_to_us) as avg_cad_to_us
# MAGIC from to_get_avg)
# MAGIC
# MAGIC
# MAGIC , new_customer_money_two as  (
# MAGIC select distinct 
# MAGIC incurred_at,
# MAGIC `voided?`,
# MAGIC moneying_billing_party_transaction.currency as cust_currency_type,
# MAGIC moneying_billing_party_transaction.relay_reference_number as loadnum,
# MAGIC charge_code,
# MAGIC amount::float / 100 as amount
# MAGIC from moneying_billing_party_transaction
# MAGIC left join customer_profile_projection r on moneying_billing_party_transaction.billing_party_id = r.customer_slug 
# MAGIC where 1=1 
# MAGIC --and r.profit_center in ('SCR','SEA')
# MAGIC and `voided?` = 'false'
# MAGIC order by loadnum)
# MAGIC
# MAGIC , invoicing_credits as  (
# MAGIC select  
# MAGIC relay_reference_number,
# MAGIC sum(total_amount) filter (where currency = 'USD')::float / 100.00 as usd_cred,
# MAGIC sum(total_amount) filter (where currency = 'CAD')::float / 100.00 as cad_cred
# MAGIC from invoicing_credits
# MAGIC group by relay_reference_number)
# MAGIC
# MAGIC
# MAGIC , customer_money_final as  (
# MAGIC select distinct 
# MAGIC loadnum,
# MAGIC case when sum(amount) filter (where cust_currency_type = 'USD') is null then 0 
# MAGIC     else sum(amount) filter (where cust_currency_type = 'USD') end as total_usd,
# MAGIC case when sum(amount) filter (where cust_currency_type = 'CAD') is null then 0 else 
# MAGIC     sum(amount) filter (where cust_currency_type = 'CAD') end as total_cad,
# MAGIC case when sum(amount) filter (where 1=1 and cust_currency_type = 'USD' and charge_code = 'linehaul') is null then 0 else 
# MAGIC     sum(amount) filter (where 1=1 and cust_currency_type = 'USD' and charge_code = 'linehaul') end as usd_lhc,
# MAGIC case when sum(amount) filter (where 1=1 and cust_currency_type = 'USD' and charge_code = 'fuel_surcharge')  is null then 0 else 
# MAGIC     sum(amount) filter (where 1=1 and cust_currency_type = 'USD' and charge_code = 'fuel_surcharge')end as usd_fsc,
# MAGIC case when sum(amount) filter (where 1=1 and cust_currency_type = 'USD' and charge_code not in ('linehaul','fuel_surcharge')) is null then 0 
# MAGIC     else sum(amount) filter (where 1=1 and cust_currency_type = 'USD' and charge_code not in ('linehaul','fuel_surcharge')) end as usd_acc,
# MAGIC case when sum(amount) filter (where 1=1 and cust_currency_type = 'CAD' and charge_code = 'linehaul') is null then 0 
# MAGIC     else sum(amount) filter (where 1=1 and cust_currency_type = 'CAD' and charge_code = 'linehaul') end as cad_lhc,
# MAGIC case when sum(amount) filter (where 1=1 and cust_currency_type = 'CAD' and charge_code = 'fuel_surcharge') is null then 0 
# MAGIC     else sum(amount) filter (where 1=1 and cust_currency_type = 'CAD' and charge_code = 'fuel_surcharge') end as cad_fsc,
# MAGIC case when sum(amount) filter (where 1=1 and cust_currency_type = 'CAD' and charge_code not in ('linehaul','fuel_surcharge')) is null then 0 
# MAGIC     else sum(amount) filter (where 1=1 and cust_currency_type = 'CAD' and charge_code not in ('linehaul','fuel_surcharge')) end as cad_acc,
# MAGIC case when invoicing_credits.usd_cred is null then 0 else invoicing_credits.usd_cred end as usd_cred,
# MAGIC case when invoicing_credits.cad_cred  is null then 0 else invoicing_credits.cad_cred end as cad_cred
# MAGIC from new_customer_money_two
# MAGIC left join invoicing_credits on loadnum = invoicing_credits.relay_reference_number
# MAGIC group by loadnum, usd_cred, cad_cred)
# MAGIC 	
# MAGIC
# MAGIC
# MAGIC , carrier_charges_one as  (
# MAGIC select distinct 
# MAGIC relay_reference_number,
# MAGIC coalesce(
# MAGIC case when currency = 'USD' then sum(amount) filter (where charge_code = 'linehaul')::float / 100.00 
# MAGIC     else (sum(amount) filter (where charge_code = 'linehaul')::float / 100.00) * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float end, 0) as carr_lhc,
# MAGIC     
# MAGIC coalesce(
# MAGIC case when currency = 'USD' then sum(amount) filter (where charge_code = 'fuel_surcharge')::float / 100.00 
# MAGIC     else (sum(amount) filter (where charge_code = 'fuel_surcharge')::float / 100.00) * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float end, 0) as carr_fsc,
# MAGIC     
# MAGIC
# MAGIC coalesce(
# MAGIC case when currency = 'USD' then sum(amount) filter (where charge_code not in ('fuel_surcharge','linehaul'))::float / 100.00 
# MAGIC     else (sum(amount) filter (where charge_code not in ('fuel_surcharge','linehaul'))::float / 100.00) * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float end, 0) as carr_acc
# MAGIC from vendor_transaction_projection
# MAGIC left join canada_conversions on vendor_transaction_projection.incurred_at::date = canada_conversions.ship_date::date 
# MAGIC join average_conversions on 1=1 
# MAGIC where 1=1 
# MAGIC and `voided?` = 'false'
# MAGIC group by relay_reference_number, currency , us_to_cad, avg_cad_to_us)
# MAGIC
# MAGIC , carrier_charges as  (
# MAGIC select distinct 
# MAGIC relay_reference_number,
# MAGIC sum(carr_lhc) as carr_lhc,
# MAGIC sum(carr_fsc) as carr_fsc,
# MAGIC sum(carr_acc) as carr_acc,
# MAGIC sum(carr_acc)::float + sum(carr_fsc)::float + sum(carr_lhc)::float as total_carrier_cost
# MAGIC from carrier_charges_one
# MAGIC group by relay_reference_number)
# MAGIC
# MAGIC , pu_late_reason as  (
# MAGIC select distinct 
# MAGIC planning_stop_schedule.relay_reference_number,
# MAGIC concat_ws(late_reason,'-') as pu_reasons
# MAGIC from planning_stop_schedule
# MAGIC where 1=1
# MAGIC and stop_type = 'pickup'
# MAGIC and late_reason is not null 
# MAGIC group by planning_stop_schedule.relay_reference_number,late_reason)
# MAGIC
# MAGIC
# MAGIC , del_late_reason as  (
# MAGIC select distinct 
# MAGIC planning_stop_schedule.relay_reference_number,
# MAGIC concat_ws(late_reason,'-') as del_reasons
# MAGIC from planning_stop_schedule
# MAGIC where 1=1 
# MAGIC and stop_type = 'delivery'
# MAGIC and late_reason is not null 
# MAGIC group by planning_stop_schedule.relay_reference_number,late_reason)
# MAGIC
# MAGIC
# MAGIC , final_del_seq as  (
# MAGIC select distinct 
# MAGIC planning_stop_schedule.relay_reference_number,
# MAGIC max(sequence_number) as final_del_seq
# MAGIC from planning_stop_schedule
# MAGIC left join booking_projection b on planning_stop_schedule.relay_reference_number = b.relay_reference_number
# MAGIC left join customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC where 1=1 
# MAGIC --and r.profit_center in ('SCR','SEA')
# MAGIC and planning_stop_schedule.stop_type = 'delivery'
# MAGIC and planning_stop_schedule.`removed?` = 'false'
# MAGIC group by planning_stop_schedule.relay_reference_number)
# MAGIC
# MAGIC , final_del_stop_id as  (
# MAGIC select distinct 
# MAGIC final_del_seq.relay_reference_number,
# MAGIC final_del_seq,
# MAGIC planning_stop_schedule.stop_id as final_del_stop_id
# MAGIC from final_del_seq
# MAGIC left join planning_stop_schedule on final_del_seq.relay_reference_number = planning_stop_schedule.relay_reference_number
# MAGIC    and final_del_seq.final_del_seq = planning_stop_schedule.sequence_number
# MAGIC    and `removed?` = 'false')
# MAGIC    
# MAGIC , actual_del_date as  (
# MAGIC select distinct 
# MAGIC b.booking_id,
# MAGIC b.ready_date,
# MAGIC c.stop_id,
# MAGIC c.in_date_time::timestamp as in_del_time,
# MAGIC c.out_date_time::timestamp as out_del_time,
# MAGIC coalesce(c.in_date_time::date, c.out_date_time::date) as actual_del_date
# MAGIC from booking_projection b 
# MAGIC left join canonical_stop c on b.booking_id = c.booking_id 
# MAGIC     and b.receiver_id = c.facility_id
# MAGIC     and stop_type = 'delivery'
# MAGIC     and `stale?` = 'false'
# MAGIC join final_del_stop_id on c.stop_id = final_del_stop_id.final_del_stop_id
# MAGIC left join customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug 
# MAGIC where 1=1 
# MAGIC --and r.profit_center in ('SCR','SEA')
# MAGIC and b.status = 'booked')
# MAGIC
# MAGIC
# MAGIC
# MAGIC , invoice_start as  (
# MAGIC select distinct 
# MAGIC relay_reference_number,
# MAGIC max(invoiced_at::date) as invoiced_at_ltl
# MAGIC from ltl_invoice_projection 
# MAGIC group by relay_reference_number
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct 
# MAGIC relay_reference_number,
# MAGIC min(invoiced_at::date) as invoiced_tl
# MAGIC from tl_invoice_projection
# MAGIC group by relay_reference_number)
# MAGIC
# MAGIC , first_invoiced as  (
# MAGIC select distinct 
# MAGIC relay_reference_number,
# MAGIC min(invoiced_at_ltl) as first_invoiced
# MAGIC from invoice_start
# MAGIC group by relay_reference_number)
# MAGIC
# MAGIC
# MAGIC , use_shipper as  (
# MAGIC select distinct
# MAGIC case 
# MAGIC when upper(first_shipper_name) = 'FARA CAFE ROASTING' then 'FARA CAFE'
# MAGIC when upper(first_shipper_name) = 'AMES FARM LTD' then 'AMES FARM'
# MAGIC else upper(first_shipper_name)
# MAGIC end as use_shipper
# MAGIC from booking_projection 
# MAGIC left join customer_profile_projection on booking_projection.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC --where 1=1 
# MAGIC --and customer_profile_projection.profit_center in ('SCR','SEA')
# MAGIC )
# MAGIC
# MAGIC  ,system_union as (
# MAGIC select distinct
# MAGIC coalesce(t.status, case when b.status = 'eligible' then 'Open' else b.status end) as Status,
# MAGIC b.relay_reference_number::float as Load,
# MAGIC relay_users.full_name as Account_Mgr,
# MAGIC concat(upper(canonical_plan_projection.mode),'-',tendering_service_line.service_line_type) as Mode,
# MAGIC tender_reference_numbers_projection.nfi_pro_number as Shipment_ID,
# MAGIC tender_reference_numbers_projection.order_numbers as PO,
# MAGIC planning_note_captured.planning_note as Planning_Note,
# MAGIC planning_note_captured.internal_note as Internal_Note,
# MAGIC b.booked_by_name as Booked_By,
# MAGIC b.booked_at::timestamp as Booked_At,
# MAGIC hain_tracking_accuracy_projection.delivery_by_date_at_tender::date as Delivery_By_Date,
# MAGIC coalesce(distance_projection.total_distance::float, b.total_miles::float, 0) as Miles,
# MAGIC b.booked_carrier_name as Carrier,
# MAGIC b.first_shipper_name as Pickup_location,
# MAGIC new_pu_appt.address_1 as Shipper_Address,
# MAGIC concat(b.first_shipper_city,',',b.first_shipper_state) as Shipper_Location,
# MAGIC new_pu_appt.new_pu_appt::timestamp as Pickup_Schedule,
# MAGIC new_pu_appt::date as PU_Schedule_Date,
# MAGIC date_format(new_pu_appt, 'HH:mm') as PU_Schedule_Time,
# MAGIC new_pu_appt.schedule_reference as PU_Ref,
# MAGIC pu_late_reason.pu_reasons as PU_Late_Reason,
# MAGIC b.receiver_name as Receiver_Name,
# MAGIC d.address_1 as Receiver_Address,
# MAGIC concat(b.receiver_city,',',b.receiver_state) as Receiver_Location,
# MAGIC COALESCE(d.appointment_datetime::TIMESTAMP, d.window_start_datetime::TIMESTAMP, d.window_end_datetime::TIMESTAMP) as Delivery_Schedule,
# MAGIC COALESCE(d.appointment_datetime::TIMESTAMP, d.window_start_datetime::TIMESTAMP, d.window_end_datetime::TIMESTAMP)::date as Del_Schedule_Date,
# MAGIC date_format(COALESCE(d.appointment_datetime, d.window_start_datetime, d.window_end_datetime), 'HH:mm') as Del_Schedule_Time,
# MAGIC
# MAGIC actual_del_date::date as Actual_Del_Date,
# MAGIC d.schedule_reference as Del_Ref,
# MAGIC del_late_reason.del_reasons as Del_Late_Reason,
# MAGIC b.total_pallets as Pallets,
# MAGIC b.total_pieces as Pieces,
# MAGIC b.total_weight as Weight,
# MAGIC carrier_charges.total_carrier_cost as Total_Carrier_Rate,
# MAGIC carrier_charges.carr_lhc as Carr_LHC,
# MAGIC carrier_charges.carr_fsc as Carr_FSC,
# MAGIC carrier_charges.carr_acc as Carr_ACC,
# MAGIC (customer_money_final.usd_lhc::float +
# MAGIC (customer_money_final.cad_lhc * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) as Cust_LHC,
# MAGIC (customer_money_final.usd_fsc::float +
# MAGIC (customer_money_final.cad_fsc * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) as Cust_FSC,
# MAGIC (customer_money_final.usd_acc::float +
# MAGIC (customer_money_final.cad_acc * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) as Cust_ACC,
# MAGIC
# MAGIC (customer_money_final.total_usd::float +
# MAGIC (customer_money_final.total_cad * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) - 
# MAGIC (customer_money_final.usd_cred::float +
# MAGIC (customer_money_final.cad_cred * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) AS Total_Cust_Rate,
# MAGIC customer_profile_projection.customer_name as Customer_Name
# MAGIC from booking_projection b
# MAGIC join use_shipper on upper(b.first_shipper_name) = use_shipper.use_shipper
# MAGIC left join new_pu_appt on b.relay_reference_number = new_pu_appt.relay_reference_number
# MAGIC left join financial_calendar on new_pu_appt.new_pu_appt::date = financial_calendar.date 
# MAGIC left join customer_profile_projection on b.tender_on_behalf_of_id = customer_profile_projection.customer_slug and customer_profile_projection.status = 'published'
# MAGIC left join carrier_charges on b.relay_reference_number = carrier_charges.relay_reference_number
# MAGIC left join carrier_projection c on b.booked_carrier_id = c.carrier_id 
# MAGIC left join truckload_projection t on b.relay_reference_number = t.relay_reference_number and t.status != 'CancelledOrBounced'
# MAGIC left join customer_money_final on b.relay_reference_number = customer_money_final.loadnum
# MAGIC join average_conversions on 1=1 
# MAGIC left join canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC left join actual_del_date on b.booking_id = actual_del_date.booking_id
# MAGIC left join planning_note_captured on b.relay_reference_number::float = planning_note_captured.relay_reference_number::float 
# MAGIC left join canada_conversions on new_pu_appt::date = canada_conversions.ship_date::date 
# MAGIC left join first_invoiced on b.relay_reference_number = first_invoiced.relay_reference_number
# MAGIC left join tender_reference_numbers_projection on b.relay_reference_number = tender_reference_numbers_projection.relay_reference_number
# MAGIC left join tendering_service_line on b.relay_reference_number = tendering_service_line.relay_reference_number
# MAGIC left join hain_tracking_accuracy_projection on b.relay_reference_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC left join distance_projection on b.relay_reference_number = distance_projection.relay_reference_number
# MAGIC left join pu_late_reason on b.relay_reference_number = pu_late_reason.relay_reference_number
# MAGIC left join del_late_reason on b.relay_reference_number = del_late_reason.relay_reference_number
# MAGIC left join relay_users on customer_profile_projection.primary_relay_user_id = relay_users.user_id and relay_users.`active?` = 'true'
# MAGIC left join planning_stop_schedule d on b.relay_reference_number::float = d.relay_reference_number::float 
# MAGIC     and b.receiver_name = d.stop_name 
# MAGIC     and d.stop_type = 'delivery'
# MAGIC     and d.`removed?` = 'false'
# MAGIC where 1=1
# MAGIC --and customer_profile_projection.profit_center in ('SCR','SEA')
# MAGIC and b.status not in ('cancelled','bounced')
# MAGIC and first_invoiced.first_invoiced is null 
# MAGIC --and actual_del_date.actual_del_date is null 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC   dispatch_status,
# MAGIC   CAST(load_number AS FLOAT) AS load_number,
# MAGIC   s.full_name as Account_Mgr,
# MAGIC   CONCAT('LTL-', tendering_service_line.service_line_type) AS mode_service_line,
# MAGIC   tender_reference_numbers_projection.nfi_pro_number AS shipment_id,
# MAGIC   tender_reference_numbers_projection.order_numbers AS po_number,
# MAGIC   planning_note_captured.planning_note,
# MAGIC   planning_note_captured.internal_note,
# MAGIC   'ltl_team' AS team,
# MAGIC   NULL AS timestamp,
# MAGIC   CAST(hain_tracking_accuracy_projection.delivery_by_date_at_tender AS DATE) AS delivery_by_date,
# MAGIC   CAST(big_export_projection.miles AS FLOAT) AS miles,
# MAGIC   carrier_name,
# MAGIC   pickup_name,
# MAGIC   o.address_1,
# MAGIC   CONCAT(big_export_projection.pickup_city, ',', big_export_projection.pickup_state) AS shipper_location,
# MAGIC   COALESCE(o.appointment_datetime, o.window_start_datetime, o.window_end_datetime) AS pickup_datetime,
# MAGIC   CAST(COALESCE(o.appointment_datetime, o.window_start_datetime, o.window_end_datetime) AS DATE) AS PU_Schedule_Date,
# MAGIC   DATE_FORMAT(COALESCE(o.appointment_datetime, o.window_start_datetime, o.window_end_datetime), 'HH:mm') AS PU_Schedule_Time,
# MAGIC   o.schedule_reference,
# MAGIC   o.late_reason,
# MAGIC   big_export_projection.consignee_name,
# MAGIC   d.address_1,
# MAGIC   CONCAT(big_export_projection.consignee_city, ',', big_export_projection.consignee_state) AS receiver_location,
# MAGIC   COALESCE(d.appointment_datetime, d.window_start_datetime, d.window_end_datetime) AS del_appt,
# MAGIC   CAST(COALESCE(d.appointment_datetime, d.window_start_datetime, d.window_end_datetime) AS DATE) AS Del_Schedule_Date,
# MAGIC   DATE_FORMAT(COALESCE(d.appointment_datetime, d.window_start_datetime, d.window_end_datetime), 'HH:mm') AS Del_Schedule_Time,
# MAGIC   CAST(delivered_date AS DATE) AS delivered_date,
# MAGIC d.schedule_reference,
# MAGIC d.late_reason,
# MAGIC big_export_projection.pallet_count,
# MAGIC big_export_projection.piece_count,
# MAGIC big_export_projection.weight,
# MAGIC projected_expense::float/100.0 as carrier_rate,
# MAGIC big_export_projection.carrier_linehaul_expense::float / 100.0 as carrier_lhc,
# MAGIC big_export_projection.carrier_fuel_expense::float / 100.0 as carrier_fsc,
# MAGIC big_export_projection.carrier_accessorial_expense::float / 100.0 as carrier_acc,
# MAGIC
# MAGIC (customer_money_final.usd_lhc::float +
# MAGIC (customer_money_final.cad_lhc * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) as customer_lhc,
# MAGIC (customer_money_final.usd_fsc::float +
# MAGIC (customer_money_final.cad_fsc * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) as customer_fsc,
# MAGIC (customer_money_final.usd_acc::float +
# MAGIC (customer_money_final.cad_acc * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) as customer_acc,
# MAGIC
# MAGIC (customer_money_final.total_usd::float +
# MAGIC (customer_money_final.total_cad * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) - 
# MAGIC (customer_money_final.usd_cred::float +
# MAGIC (customer_money_final.cad_cred * (case when canada_conversions.us_to_cad is null then average_conversions.avg_cad_to_us else canada_conversions.us_to_cad end)::float)) AS revenue,
# MAGIC customer_profile_projection.customer_name as customer
# MAGIC from big_export_projection 
# MAGIC join use_shipper on upper(big_export_projection.pickup_name) = use_shipper.use_shipper
# MAGIC left join customer_profile_projection on customer_id = customer_profile_projection.customer_slug and customer_profile_projection.status = 'published'
# MAGIC left join customer_money_final on big_export_projection.load_number = customer_money_final.loadnum
# MAGIC left join relay_users on customer_profile_projection.primary_relay_user_id = relay_users.user_id and relay_users.`active?` = 'true'
# MAGIC left join relay_users s on sales_relay_user_id = s.user_id and s.`active?` = 'true'
# MAGIC left join first_invoiced on big_export_projection.load_number::float = first_invoiced.relay_reference_number::float 
# MAGIC left join projection_carrier c on big_export_projection.carrier_id = c.id 
# MAGIC join average_conversions on 1=1 
# MAGIC left join tender_reference_numbers_projection on big_export_projection.load_number::float = tender_reference_numbers_projection.relay_reference_number::float 
# MAGIC left join canada_conversions on big_export_projection.ship_date::date = canada_conversions.ship_date::date 
# MAGIC left join canonical_plan_projection on big_export_projection.load_number::float = canonical_plan_projection.relay_reference_number
# MAGIC left join tendering_service_line on big_export_projection.load_number::float = tendering_service_line.relay_reference_number::float
# MAGIC left join planning_note_captured on big_export_projection.load_number::float = planning_note_captured.relay_reference_number::float 
# MAGIC left join hain_tracking_accuracy_projection on big_export_projection.load_number::float = hain_tracking_accuracy_projection.relay_reference_number::float
# MAGIC left join planning_stop_schedule o on big_export_projection.load_number::float = o.relay_reference_number::float 
# MAGIC     and big_export_projection.pickup_name = o.stop_name 
# MAGIC     and o.stop_type = 'pickup'
# MAGIC     and o.`removed?` = 'false'
# MAGIC left join financial_calendar on COALESCE(o.appointment_datetime::TIMESTAMP, o.window_start_datetime::TIMESTAMP, o.window_end_datetime::TIMESTAMP)::date = financial_calendar.date::date 
# MAGIC left join planning_stop_schedule d on big_export_projection.load_number::float = d.relay_reference_number::float 
# MAGIC     and big_export_projection.consignee_name = d.stop_name 
# MAGIC     and d.stop_type = 'delivery'
# MAGIC     and d.`removed?` = 'false'
# MAGIC where 
# MAGIC 	1 = 1 
# MAGIC 	and canonical_plan_projection.mode = 'ltl'
# MAGIC 	--and customer_profile_projection.profit_center in ('SCR','SEA')
# MAGIC 	and dispatch_status <> 'Cancelled'
# MAGIC 	and first_invoiced.first_invoiced is null 
# MAGIC     --and delivered_date is null
# MAGIC  )
# MAGIC  select * from system_union
# MAGIC   
# MAGIC   
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.SEA_AM_Board_GenAI_Temp;
# MAGIC INSERT INTO analytics.SEA_AM_Board_GenAI_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.SEA_AM_Board_GenAI

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table analytics.SEA_AM_Board_GenAI;
# MAGIC INSERT INTO analytics.SEA_AM_Board_GenAI
# MAGIC SELECT *
# MAGIC FROM analytics.SEA_AM_Board_GenAI_Temp