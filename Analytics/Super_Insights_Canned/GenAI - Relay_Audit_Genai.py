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
# MAGIC create or replace view bronze.Relay_Audit_Genai as(
# MAGIC WITH use_load AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         relay_reference_number AS use_load,
# MAGIC         nfi_pro_number AS shipment_id
# MAGIC     FROM tender_reference_numbers_projection
# MAGIC     UNION
# MAGIC     SELECT DISTINCT 
# MAGIC         relay_reference_number AS use_load,
# MAGIC         nfi_pro_number AS shipment_id
# MAGIC     FROM tender_reference_numbers_projection
# MAGIC )
# MAGIC ,final_code(SELECT DISTINCT
# MAGIC     b.relay_reference_number AS RRN,
# MAGIC     b.booking_id AS booking_id,
# MAGIC     use_load.shipment_id AS shipment_id,
# MAGIC     'Load Booked' AS Type,
# MAGIC     date_format(from_utc_timestamp(b.booked_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Carrier ', booked_carrier_name, ' was booked for $', booked_total_carrier_rate_amount / 100) AS Details,
# MAGIC     b.booked_by_name AS by_user
# MAGIC FROM booking_projection b 
# MAGIC JOIN use_load ON b.relay_reference_number = use_load.use_load
# MAGIC WHERE booked_at IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     b.relay_reference_number AS RRN,
# MAGIC     b.booking_id AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Load Reserved' AS Type,
# MAGIC     date_format(from_utc_timestamp(b.reserved_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT(reserved_carrier_name, ' was reserved') AS Details,
# MAGIC     CONCAT(u.first_name, ' ', u.last_name) AS by_user
# MAGIC FROM booking_projection b 
# MAGIC JOIN use_load ON b.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users u ON b.reserved_by = u.user_id
# MAGIC WHERE reserved_at IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     b.relay_reference_number AS RRN,
# MAGIC     b.booking_id AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Load Bounced' AS Type,
# MAGIC     date_format(from_utc_timestamp(b.bounced_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT(booked_carrier_name, ' was bounced') AS Details,
# MAGIC     CONCAT(u.first_name, ' ', u.last_name) AS by_user
# MAGIC FROM booking_projection b 
# MAGIC JOIN use_load ON b.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users u ON b.bounced_by = u.user_id
# MAGIC WHERE bounced_at IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     b.relay_reference_number AS RRN,
# MAGIC     b.booking_id AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Load Rolled' AS Type,
# MAGIC     date_format(from_utc_timestamp(b.rolled_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Ready Date Rolled to ', DATE_FORMAT(b.ready_date, 'MM/dd/yyyy')) AS Details,
# MAGIC     CONCAT(u.first_name, ' ', u.last_name) AS by_user
# MAGIC FROM booking_projection b 
# MAGIC JOIN use_load ON b.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users u ON b.rolled_by = u.user_id
# MAGIC WHERE rolled_at IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     tendering_acceptance.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     tendering_acceptance.shipment_id,
# MAGIC     'Tender Accepted' AS Type,
# MAGIC     date_format(from_utc_timestamp(accepted_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Accepted ', tendering_acceptance.tender_on_behalf_of_id, ' tender from ', DATE_FORMAT(tendered_at, 'MM/dd/yyyy')) AS details,
# MAGIC     CASE WHEN accepted_by IS NULL THEN 'Auto Accept' ELSE relay_users.full_name END AS accepted_by_name
# MAGIC FROM tendering_acceptance
# MAGIC JOIN use_load ON tendering_acceptance.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users ON accepted_by = relay_users.user_id
# MAGIC WHERE 1=1 AND `accepted?` = 'true'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     tendering_tender_draft_id.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Manual Tender Created' AS Type,
# MAGIC     date_format(from_utc_timestamp(completed_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     NULL AS `Update`,
# MAGIC     relay_users.full_name AS action_by
# MAGIC FROM tendering_tender_draft_id
# MAGIC JOIN use_load ON tendering_tender_draft_id.relay_reference_number = use_load.use_load
# MAGIC JOIN tendering_manual_tender_entry_usage ON tendering_tender_draft_id.tender_draft_id = tendering_manual_tender_entry_usage.tender_draft_id
# MAGIC LEFT JOIN relay_users ON tendering_manual_tender_entry_usage.draft_by_user_id = relay_users.user_id
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     planning_assignment_log.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     assignment_action AS Type,
# MAGIC     date_format(from_utc_timestamp(action_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CASE WHEN assignment_action IN ('Plan Unassigned', 'Assigned to Open Board') THEN assignment_action ELSE CONCAT('Assigned To: ', b.full_name) END AS details,
# MAGIC     CASE WHEN assignment_action = 'Assigned to Open Board' THEN NULL ELSE a.full_name END AS action_by
# MAGIC FROM planning_assignment_log
# MAGIC JOIN use_load ON planning_assignment_log.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users a ON action_by = a.user_id
# MAGIC LEFT JOIN relay_users b ON action_involving = b.user_id
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     sourcing_max_buy_v2.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Set Max Buy' AS Type,
# MAGIC     date_format(from_utc_timestamp(set_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Set Max Buy: $', max_buy / 100) AS details,
# MAGIC     set_by_name
# MAGIC FROM sourcing_max_buy_v2
# MAGIC JOIN use_load ON sourcing_max_buy_v2.relay_reference_number = use_load.use_load
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     tender_reference_numbers_projection.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     tender_reference_numbers_projection.nfi_pro_number,
# MAGIC     'Load Cancelled' AS Type,
# MAGIC     date_format(from_utc_timestamp(cancelled_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     NULL AS Details,
# MAGIC     CASE WHEN cancelled_by IS NULL THEN 'EDI' ELSE relay_users.full_name END AS action_by
# MAGIC FROM tender_reference_numbers_projection
# MAGIC JOIN use_load ON tender_reference_numbers_projection.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users ON cancelled_by = relay_users.user_id 
# MAGIC WHERE cancelled_at IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     offer_negotiation_reflected.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Load Offer' AS Type,
# MAGIC     date_format(from_utc_timestamp(offered_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Offered $', total_rate_in_pennies / 100, ' to carrier ', offer_negotiation_reflected.carrier_name) AS details,
# MAGIC     offer_negotiation_reflected.offered_by_name AS action_by
# MAGIC FROM offer_negotiation_reflected
# MAGIC JOIN use_load ON offer_negotiation_reflected.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN offer_negotiation_invalidated ON offer_negotiation_reflected.relay_reference_number = offer_negotiation_invalidated.relay_reference_number
# MAGIC WHERE offer_negotiation_invalidated.relay_reference_number IS NULL
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     b.relay_reference_number,
# MAGIC     b.booking_id,
# MAGIC     use_load.shipment_id AS shipment_id,
# MAGIC     'Rate Con Sent' AS Type,
# MAGIC     CASE 
# MAGIC         WHEN TRY_TO_TIMESTAMP(last_rate_con_sent_time, 'MM-dd-yyyy HHmm') IS NOT NULL THEN 
# MAGIC             date_format(from_utc_timestamp(TRY_TO_TIMESTAMP(last_rate_con_sent_time, 'MM-dd-yyyy HHmm'), 'America/Chicago'), 'yyyy-MM-dd HH.mm')
# MAGIC         WHEN TRY_TO_TIMESTAMP(last_rate_con_sent_time, 'MM-dd-yyyy HH:mm') IS NOT NULL THEN 
# MAGIC             date_format(from_utc_timestamp(TRY_TO_TIMESTAMP(last_rate_con_sent_time, 'MM-dd-yyyy HH:mm'), 'America/Chicago'), 'yyyy-MM-dd HH.mm')
# MAGIC         WHEN TRY_TO_TIMESTAMP(last_rate_con_sent_time, 'MM-d-yyyy HHmm') IS NOT NULL THEN 
# MAGIC             date_format(from_utc_timestamp(TRY_TO_TIMESTAMP(last_rate_con_sent_time, 'MM-d-yyyy HHmm'), 'America/Chicago'), 'yyyy-MM-dd HH.mm')
# MAGIC         ELSE NULL
# MAGIC     END AS Timestamp_Central_Time,
# MAGIC     CONCAT('Sent to ', b.booked_carrier_name) AS details,
# MAGIC     u.full_name AS action_by
# MAGIC FROM booking_projection b
# MAGIC JOIN use_load ON b.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users u ON b.rate_con_sent_by = u.user_id
# MAGIC WHERE last_rate_con_sent_time IS NOT NULL
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     planning_schedule_activity.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     CASE WHEN event_type = 'Planning.Schedule.StopScheduled' THEN 
# MAGIC         CONCAT('Scheduled ', planning_stop_schedule.stop_type, ' appt @ ', planning_stop_schedule.stop_name) ELSE  
# MAGIC         CONCAT('RE-Scheduled ', planning_stop_schedule.stop_type, ' appt @ ', planning_stop_schedule.stop_name) 
# MAGIC     END AS Type,
# MAGIC     date_format(from_utc_timestamp(planning_schedule_activity.scheduled_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CASE WHEN planning_schedule_activity.window_start_datetime IS NOT NULL THEN 
# MAGIC         CONCAT('Window appt from ', DATE_FORMAT(planning_schedule_activity.window_start_datetime, 'MM/dd/yy HH:mm'), ' thru ', DATE_FORMAT(planning_schedule_activity.window_end_datetime, 'MM/dd/yy HH:mm'))
# MAGIC     ELSE 
# MAGIC         CONCAT('Appt: ', DATE_FORMAT(planning_schedule_activity.appointment_datetime, 'MM/dd/yy HH:mm')) 
# MAGIC     END AS details,
# MAGIC     relay_users.full_name AS action_by
# MAGIC FROM planning_schedule_activity
# MAGIC JOIN use_load ON planning_schedule_activity.relay_reference_number = use_load.use_load
# MAGIC JOIN planning_stop_schedule ON planning_schedule_activity.stop_id = planning_stop_schedule.stop_id 
# MAGIC LEFT JOIN relay_users ON planning_schedule_activity.scheduled_by = relay_users.user_id
# MAGIC WHERE event_type IN ('Planning.Schedule.StopRescheduled', 'Planning.Schedule.StopScheduled')
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     planning_schedule_activity.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     CONCAT('Cleared ', planning_stop_schedule.stop_type, ' appt @ ', planning_stop_schedule.stop_name) AS Type,
# MAGIC     date_format(from_utc_timestamp(planning_schedule_activity.cleared_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     'Appointment Removed' AS Details,
# MAGIC     relay_users.full_name AS action_by
# MAGIC FROM planning_schedule_activity
# MAGIC JOIN use_load ON planning_schedule_activity.relay_reference_number = use_load.use_load
# MAGIC JOIN planning_stop_schedule ON planning_schedule_activity.stop_id = planning_stop_schedule.stop_id 
# MAGIC LEFT JOIN relay_users ON planning_schedule_activity.cleared_by = relay_users.user_id
# MAGIC WHERE event_type = 'Planning.Schedule.StopScheduleCleared'
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     moneying_billing_party_transaction.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Set Cust. Money' AS Type,
# MAGIC     date_format(from_utc_timestamp(incurred_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Set ', charge_code, ' $', amount / 100) AS details,
# MAGIC     relay_users.full_name AS incurred_by
# MAGIC FROM moneying_billing_party_transaction
# MAGIC JOIN use_load ON moneying_billing_party_transaction.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users ON incurred_by = relay_users.user_id
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     moneying_billing_party_transaction.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Voided Cust. Money' AS Type,
# MAGIC     date_format(from_utc_timestamp(voided_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Voided ', charge_code, ' $', amount / 100) AS details,
# MAGIC     relay_users.full_name AS incurred_by
# MAGIC FROM moneying_billing_party_transaction
# MAGIC JOIN use_load ON moneying_billing_party_transaction.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users ON voided_by = relay_users.user_id
# MAGIC WHERE `voided?` = 'true'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     booking_projection.relay_reference_number,
# MAGIC     booking_projection.booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     CONCAT('Actual ', INITCAP(canonical_stop.stop_type), ' In Time') AS Type,
# MAGIC     date_format((canonical_stop.in_date_time ), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Marked IN @ ', DATE_FORMAT(from_utc_timestamp(canonical_stop.in_date_time, 'America/Chicago'), 'MM/dd/yy HH:mm')) AS Details,
# MAGIC     'Relay TMS' AS action_by
# MAGIC FROM booking_projection
# MAGIC JOIN use_load ON booking_projection.relay_reference_number = use_load.use_load
# MAGIC JOIN canonical_stop ON booking_projection.booking_id = canonical_stop.booking_id
# MAGIC WHERE booking_projection.status = 'booked' AND canonical_stop.`stale?` = 'false' AND canonical_stop.in_date_time IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     booking_projection.relay_reference_number,
# MAGIC     booking_projection.booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     CONCAT('Actual ', INITCAP(canonical_stop.stop_type), ' Out Time') AS Type,
# MAGIC     date_format((canonical_stop.out_date_time), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Marked OUT @ ', DATE_FORMAT(from_utc_timestamp(canonical_stop.out_date_time, 'America/Chicago'), 'MM/dd/yy HH:mm')) AS Details,
# MAGIC     'Relay TMS' AS action_by
# MAGIC FROM booking_projection
# MAGIC JOIN use_load ON booking_projection.relay_reference_number = use_load.use_load
# MAGIC JOIN canonical_stop ON booking_projection.booking_id = canonical_stop.booking_id
# MAGIC WHERE booking_projection.status = 'booked' AND canonical_stop.`stale?` = 'false' AND canonical_stop.out_date_time IS NOT NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     tl_invoice_projection.relay_reference_number,
# MAGIC     NULL AS booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Invoiced Customer' AS Type,
# MAGIC     date_format(from_utc_timestamp(invoiced_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Invoiced Inv# ', invoice_number, ' for $ ', (accessorial_amount + fuel_surcharge_amount + linehaul_amount) / 100) AS Details,
# MAGIC     COALESCE(relay_users.full_name, 'Automated') AS action_by
# MAGIC FROM tl_invoice_projection
# MAGIC JOIN use_load ON tl_invoice_projection.relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN relay_users ON tl_invoice_projection.prepared_by = relay_users.user_id
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     relay_reference_number,
# MAGIC     booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     CONCAT('Tracking Update: ', REPLACE(tracking_activity_v2.type, 'Tracking.TruckLoadThing.', '')) AS Type,
# MAGIC     date_format(from_utc_timestamp(tracking_activity_v2.at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     LTRIM(tracking_activity_v2.type, 'Tracking.TruckLoadThing.') AS Details,
# MAGIC     CASE WHEN by_type = 'system' THEN by_system_id ELSE relay_users.full_name END AS action_by
# MAGIC FROM truckload_projection
# MAGIC JOIN use_load ON relay_reference_number = use_load.use_load
# MAGIC LEFT JOIN tracking_activity_v2 ON truckload_projection.truck_load_thing_id = tracking_activity_v2.truck_load_thing_id
# MAGIC LEFT JOIN relay_users ON by_user_id = relay_users.user_id
# MAGIC WHERE tracking_activity_v2.type NOT IN ('Tracking.TruckLoadThing.TrackingStarted', 'Tracking.TruckLoadThing.TruckLoadThingChanged', 'Tracking.TruckLoadThing.StatusUpdateIntegrationIdentified', 'Tracking.TruckLoadThing.TrackingStopped')
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     vendor_transaction_projection.relay_reference_number,
# MAGIC     vendor_transaction_projection.booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Set Carr. Money' AS Type,
# MAGIC     date_format(from_utc_timestamp(vendor_transaction_projection.incurred_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Set ', vendor_transaction_projection.charge_code, ' $', amount / 100, ' on ', booking_projection.booked_carrier_name) AS Details,
# MAGIC     NULL AS action_by
# MAGIC FROM vendor_transaction_projection
# MAGIC JOIN use_load ON vendor_transaction_projection.relay_reference_number = use_load.use_load
# MAGIC JOIN booking_projection ON vendor_transaction_projection.booking_id = booking_projection.booking_id
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     vendor_transaction_projection.relay_reference_number,
# MAGIC     vendor_transaction_projection.booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Voided Carr. Money' AS Type,
# MAGIC     date_format(from_utc_timestamp(vendor_transaction_projection.voided_at, 'America/Chicago'), 'yyyy-MM-dd HH.mm') AS Timestamp_Central_Time,
# MAGIC     CONCAT('Voided ', vendor_transaction_projection.charge_code, ' $', amount / 100, ' on ', booking_projection.booked_carrier_name) AS Details,
# MAGIC     NULL AS action_by
# MAGIC FROM vendor_transaction_projection
# MAGIC JOIN use_load ON vendor_transaction_projection.relay_reference_number = use_load.use_load
# MAGIC JOIN booking_projection ON vendor_transaction_projection.booking_id = booking_projection.booking_id 
# MAGIC WHERE vendor_transaction_projection.`voided?` = 'true'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     b.relay_reference_number,
# MAGIC     integration_hubtran_vendor_invoice_approved.booking_id,
# MAGIC     use_load.shipment_id,
# MAGIC     'Invoiced Carrier' AS Type,
# MAGIC     integration_hubtran_vendor_invoice_approved.invoice_date  AS invoice_date,
# MAGIC     CONCAT('Invoiced Inv# ', invoice_number, ' for $', total_approved_amount_to_pay / 100) AS Details,
# MAGIC     approved_by AS action_by
# MAGIC FROM booking_projection b 
# MAGIC JOIN use_load ON b.relay_reference_number = use_load.use_load
# MAGIC JOIN integration_hubtran_vendor_invoice_approved ON b.booking_id = integration_hubtran_vendor_invoice_approved.booking_id 
# MAGIC WHERE b.status = 'booked'
# MAGIC
# MAGIC ORDER BY Timestamp_Central_Time)
# MAGIC  select * from final_code
# MAGIC   
# MAGIC   
# MAGIC     
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Into Table

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Relay_Audit_Genai_Temp;
# MAGIC
# MAGIC INSERT INTO analytics.Relay_Audit_Genai_Temp
# MAGIC SELECT *
# MAGIC FROM bronze.Relay_Audit_Genai

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table analytics.Relay_Audit_Genai;
# MAGIC
# MAGIC INSERT INTO analytics.Relay_Audit_Genai
# MAGIC SELECT *
# MAGIC FROM analytics.Relay_Audit_Genai_Temp