# Databricks notebook source
# MAGIC %md
# MAGIC Incremental Load

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Need to create the Hash Key and also the Dw_touch_Id------------------------------
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Relay_Touches AS
# MAGIC with use_dates as (
# MAGIC select distinct 
# MAGIC Calendar_Date as use_dates,
# MAGIC period_year as Period_Year,
# MAGIC week_end_date as Week_end_date
# MAGIC from analytics.dim_date
# MAGIC where 1=1 
# MAGIC )
# MAGIC
# MAGIC , all_activity as (
# MAGIC SELECT DISTINCT 
# MAGIC     'Relay - Booked a Load' AS Activity_Type,
# MAGIC     relay_users.full_name AS employee_Name,
# MAGIC     new_office AS Office,
# MAGIC 	period_year,
# MAGIC 	week_end_date,
# MAGIC     b.relay_reference_number::String as Activity_Id,
# MAGIC     CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', booked_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', booked_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', booked_at)
# MAGIC         ELSE 
# MAGIC             booked_at  -- Fallback if no conditions match
# MAGIC     END AS activity_Date,
# MAGIC 	1 as Touch_Type,
# MAGIC 	'Relay' as Source_System_Type
# MAGIC FROM 
# MAGIC     booking_projection b 
# MAGIC JOIN 
# MAGIC     use_dates ON b.booked_at::date = use_dates.use_dates::date 
# MAGIC JOIN 
# MAGIC     relay_users ON UPPER(b.booked_by_name) = UPPER(relay_users.full_name) AND `active?`= 'true'
# MAGIC LEFT JOIN 
# MAGIC     new_office_lookup ON relay_users.office_id = new_office_lookup.old_office 
# MAGIC WHERE 
# MAGIC     booked_by_name IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Scheduled Appt',
# MAGIC relay_users.full_name as scheduled_name,
# MAGIC master_am_list.office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC --master_am_list.job_title,
# MAGIC --master_am_list.lss_y_or_no,
# MAGIC relay_reference_number::String,
# MAGIC scheduled_at,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC --'no' as "contains_hubspot"
# MAGIC from planning_schedule_activity
# MAGIC left join use_dates on scheduled_at::date = use_dates.use_dates::date 
# MAGIC left join relay_users on scheduled_by = relay_users.user_id 
# MAGIC LEFT JOIN new_office_lookup ON relay_users.office_id = new_office_lookup.old_office 
# MAGIC join master_am_list on relay_users.user_id::string = master_am_list.am_relay_id 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC select distinct 
# MAGIC 'Spot Quote' as Activity_Date,
# MAGIC created_by as created_by,
# MAGIC master_am_list.office as Office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC quote_id::string,
# MAGIC created_at,
# MAGIC --master_am_list.job_title,
# MAGIC --master_am_list.lss_y_or_no,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC ---'no' as "contains_hubspot"
# MAGIC from spot_quotes
# MAGIC left join relay_users on created_by = relay_users.full_name 
# MAGIC left join use_dates on created_at::date = use_dates.use_dates::date 
# MAGIC --left join financial_calendar on created_at::date = financial_calendar.date::date
# MAGIC join master_am_list on relay_users.user_id::String = master_am_list.am_relay_id 
# MAGIC where 1=1 
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC select distinct 
# MAGIC 'Set Assignment' as category,
# MAGIC relay_users.full_name as action_by_name,
# MAGIC master_am_list.office as Office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC relay_reference_number::String,
# MAGIC action_at,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC --'no' as "contains_hubspot"
# MAGIC from planning_assignment_log
# MAGIC --left join financial_calendar on action_at::date = financial_calendar.date::date 
# MAGIC left join use_dates on action_at::date = use_dates.use_dates::date 
# MAGIC left join relay_users on action_by = relay_users.user_id 
# MAGIC join master_am_list on relay_users.user_id::String = master_am_list.am_relay_id 
# MAGIC where 1=1
# MAGIC
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC select distinct 
# MAGIC 'Relay - Rolled a Load' As Activity_Type,
# MAGIC relay_users.full_name As Employee_Name,
# MAGIC new_office as Office ,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC b.relay_reference_number::string as Activity_Id ,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', rolled_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', rolled_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', rolled_at)
# MAGIC         ELSE 
# MAGIC             rolled_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from booking_projection b 
# MAGIC join relay_users on b.rolled_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on rolled_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and rolled_by is not null 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct 
# MAGIC 'Relay - Bounced a Load',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC b.relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', bounced_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', bounced_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', bounced_at)
# MAGIC         ELSE 
# MAGIC             bounced_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from booking_projection b 
# MAGIC join relay_users on b.bounced_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on bounced_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and bounced_by is not null 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct 
# MAGIC 'Relay - Reserved a Load',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC b.relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', reserved_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', reserved_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', reserved_at)
# MAGIC         ELSE 
# MAGIC             reserved_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from booking_projection b 
# MAGIC join relay_users on b.reserved_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on reserved_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC and reserved_by is not null 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Tender Accepted',
# MAGIC case when accepted_by is null then 'Auto Accept' else relay_users.full_name end as accepted_by_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC tendering_acceptance.relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', accepted_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', accepted_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', accepted_at)
# MAGIC         ELSE 
# MAGIC             accepted_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from tendering_acceptance
# MAGIC join use_dates on accepted_at::date = use_dates.use_dates::date 
# MAGIC left join relay_users on accepted_by = relay_users.user_id
# MAGIC join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC where 1=1
# MAGIC and "accepted?" = 'true'
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Manual Tender',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC tendering_tender_draft_id.relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', completed_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', completed_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', completed_at)
# MAGIC         ELSE 
# MAGIC             completed_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from tendering_tender_draft_id
# MAGIC join tendering_manual_tender_entry_usage on tendering_tender_draft_id.tender_draft_id::string = tendering_manual_tender_entry_usage.tender_draft_id::string
# MAGIC join use_dates on completed_at::date = use_dates.use_dates::date 
# MAGIC join relay_users on tendering_manual_tender_entry_usage.draft_by_user_id = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC where 1=1
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC case when event_type = 'Planning.Schedule.StopScheduled' then 'Relay - Stop Scheduled' else 'Relay - Stop Rescheduled' end as activity_type,
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC planning_schedule_activity.event_id::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', scheduled_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', scheduled_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', scheduled_at)
# MAGIC         ELSE 
# MAGIC             scheduled_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from planning_schedule_activity
# MAGIC join relay_users on planning_schedule_activity.scheduled_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on scheduled_at::date = use_dates.use_dates::date 
# MAGIC where 1=1
# MAGIC and event_type in ('Planning.Schedule.StopRescheduled','Planning.Schedule.StopScheduled')
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Cleared Appointment',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC planning_schedule_activity.event_id::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', cleared_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', cleared_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', cleared_at)
# MAGIC         ELSE 
# MAGIC             cleared_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from planning_schedule_activity
# MAGIC join relay_users on planning_schedule_activity.cleared_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on cleared_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and event_type = 'Planning.Schedule.StopScheduleCleared'
# MAGIC
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct 
# MAGIC case when assignment_action = 'Assigned to Carrier Rep' then 'Relay - Assigned to Rep'
# MAGIC     when assignment_action = 'Assigned to Open Board' then 'Relay - Assigned to Open Board'
# MAGIC     when assignment_action = 'Assignment Given Back' then 'Relay - Assignment Given Back'
# MAGIC     when assignment_action = 'Plan Unassigned' then 'plan_unassigned' else assignment_action end,
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC event_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', action_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', action_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', action_at)
# MAGIC         ELSE 
# MAGIC             action_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from planning_assignment_log
# MAGIC join relay_users on planning_assignment_log.action_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on action_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Set Max Buy',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', set_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', set_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', set_at)
# MAGIC         ELSE 
# MAGIC             set_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from sourcing_max_buy_v2
# MAGIC join relay_users on sourcing_max_buy_v2.set_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on set_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Cancelled a Load',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', cancelled_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', cancelled_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', cancelled_at)
# MAGIC         ELSE 
# MAGIC             cancelled_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from tender_reference_numbers_projection
# MAGIC join relay_users on cancelled_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on cancelled_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC and cancelled_at is not null
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Put Offer on a Load',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC offer_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', offered_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', offered_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', offered_at)
# MAGIC         ELSE 
# MAGIC             offered_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from offer_negotiation_reflected
# MAGIC join relay_users on offered_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on offered_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Set Customer Money',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC charge_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', incurred_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', incurred_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', incurred_at)
# MAGIC         ELSE 
# MAGIC             incurred_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from moneying_billing_party_transaction
# MAGIC join relay_users on incurred_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on incurred_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Voided Customer Money',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC charge_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', voided_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', voided_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', voided_at)
# MAGIC         ELSE 
# MAGIC             voided_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from moneying_billing_party_transaction
# MAGIC join relay_users on voided_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on voided_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and `voided?` = 'true'
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC 'Relay - Prepare Invoice',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC event_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', invoiced_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', invoiced_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', invoiced_at)
# MAGIC         ELSE 
# MAGIC             invoiced_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC FROM tl_invoice_projection
# MAGIC join relay_users on tl_invoice_projection.prepared_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on invoiced_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC case 
# MAGIC     when type = 'Tracking.TruckLoadThing.AppointmentCaptured' then 'Relay - Appointment Captured'
# MAGIC     when type = 'Tracking.TruckLoadThing.DetentionReported' then 'Relay - Detention Reported'
# MAGIC     when type = 'Tracking.TruckLoadThing.DriverDispatched' then 'Relay - Driver Dispatched'
# MAGIC     when type = 'Tracking.TruckLoadThing.DriverInformationCaptured' then 'Relay - Driver Info Captured'
# MAGIC     when type = 'Tracking.TruckLoadThing.EquipmentAssigned' then 'Relay - Equipment Assigned'
# MAGIC     when type = 'Tracking.TruckLoadThing.InTransitUpdateCaptured' then 'Relay - In Transit Update Captured'
# MAGIC     when type = 'Tracking.TruckLoadThing.MarkedArrivedAtStop' then 'Relay - Marked Arrived At Stop'
# MAGIC     when type = 'Tracking.TruckLoadThing.MarkedDelivered' then 'Relay - Marked Delivered'
# MAGIC     when type = 'Tracking.TruckLoadThing.MarkedDepartedFromStop' then 'Relay - Marked Departed From Stop'
# MAGIC     when type = 'Tracking.TruckLoadThing.MarkedLoaded' then 'Relay - Marked Loaded'
# MAGIC     when type = 'Tracking.TruckLoadThing.StopMarkedDelivered' then 'Relay - Stop Marked Delivered'
# MAGIC     when type = 'Tracking.TruckLoadThing.StopsReordered' then 'Relay - Stops Reordered'
# MAGIC     when type = 'Tracking.TruckLoadThing.TrackingContactInformationCaptured' then 'Relay - Tracking Contact Info Captured'
# MAGIC     when type = 'Tracking.TruckLoadThing.TrackingNoteCaptured' then 'Relay - Tracking Note Captured' end as typee,
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC event_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', at)
# MAGIC         ELSE 
# MAGIC             at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC FROM tracking_activity_v2
# MAGIC join relay_users on tracking_activity_v2.by_user_id = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and type not in ('Tracking.TruckLoadThing.TrackingStarted','Tracking.TruckLoadThing.TruckLoadThingChanged','Tracking.TruckLoadThing.StatusUpdateIntegrationIdentified','Tracking.TruckLoadThing.TrackingStopped')
# MAGIC ),
# MAGIC
# MAGIC Final_Code as (
# MAGIC Select  
# MAGIC Concat_ws(Activity_Type,Activity_Date,Activity_Id) as DW_Touch_Id,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date::date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touch_Type,
# MAGIC period_year,
# MAGIC Week_End_Date,
# MAGIC Source_System_Type 
# MAGIC  from all_activity )
# MAGIC
# MAGIC  Select *,
# MAGIC SHA1(
# MAGIC   Concat(
# MAGIC     COALESCE(Activity_Id::string,''),
# MAGIC     COALESCE(Activity_Type::String,''),
# MAGIC     COALESCE(Activity_Date::String,''),
# MAGIC     COALESCE(Office::String,''),
# MAGIC     COALESCE(Employee_Name::String,''),
# MAGIC     COALESCE(Touch_Type::String,''),
# MAGIC     COALESCE(period_year::String,''),
# MAGIC     COALESCE(Week_End_Date::String,''),
# MAGIC     COALESCE(Source_System_Type,'') 
# MAGIC   )
# MAGIC  )As HashKey from Final_Code

# COMMAND ----------

# MAGIC %md
# MAGIC Full Load

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Need to create the Hash Key and also the Dw_touch_Id------------------------------
# MAGIC USE SCHEMA Bronze;
# MAGIC CREATE OR REPLACE VIEW Silver.Silver_Relay_Touches_Full_Load AS
# MAGIC with use_dates as (
# MAGIC select distinct 
# MAGIC Calendar_Date as use_dates,
# MAGIC period_year as Period_Year,
# MAGIC week_end_date as Week_end_date
# MAGIC from analytics.dim_date
# MAGIC where 1=1 
# MAGIC )
# MAGIC
# MAGIC , all_activity as (
# MAGIC SELECT DISTINCT 
# MAGIC     'Relay - Booked a Load' AS Activity_Type,
# MAGIC     relay_users.full_name AS employee_Name,
# MAGIC     new_office AS Office,
# MAGIC 	period_year,
# MAGIC 	week_end_date,
# MAGIC     b.relay_reference_number::String as Activity_Id,
# MAGIC     CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', booked_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', booked_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', booked_at)
# MAGIC         ELSE 
# MAGIC             booked_at  -- Fallback if no conditions match
# MAGIC     END AS activity_Date,
# MAGIC 	1 as Touch_Type,
# MAGIC 	'Relay' as Source_System_Type
# MAGIC FROM 
# MAGIC     booking_projection b 
# MAGIC JOIN 
# MAGIC     use_dates ON b.booked_at::date = use_dates.use_dates::date 
# MAGIC JOIN 
# MAGIC     relay_users ON UPPER(b.booked_by_name) = UPPER(relay_users.full_name) AND `active?`= 'true'
# MAGIC LEFT JOIN 
# MAGIC     new_office_lookup ON relay_users.office_id = new_office_lookup.old_office 
# MAGIC WHERE 
# MAGIC     booked_by_name IS NOT NULL
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Scheduled Appt',
# MAGIC relay_users.full_name as scheduled_name,
# MAGIC master_am_list.office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC --master_am_list.job_title,
# MAGIC --master_am_list.lss_y_or_no,
# MAGIC relay_reference_number::String,
# MAGIC scheduled_at,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC --'no' as "contains_hubspot"
# MAGIC from planning_schedule_activity
# MAGIC left join use_dates on scheduled_at::date = use_dates.use_dates::date 
# MAGIC left join relay_users on scheduled_by = relay_users.user_id 
# MAGIC LEFT JOIN new_office_lookup ON relay_users.office_id = new_office_lookup.old_office 
# MAGIC join master_am_list on relay_users.user_id::string = master_am_list.am_relay_id 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC Union 
# MAGIC
# MAGIC select distinct 
# MAGIC 'Spot Quote' as Activity_Date,
# MAGIC created_by as created_by,
# MAGIC master_am_list.office as Office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC quote_id::string,
# MAGIC created_at,
# MAGIC --master_am_list.job_title,
# MAGIC --master_am_list.lss_y_or_no,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC ---'no' as "contains_hubspot"
# MAGIC from spot_quotes
# MAGIC left join relay_users on created_by = relay_users.full_name 
# MAGIC left join use_dates on created_at::date = use_dates.use_dates::date 
# MAGIC --left join financial_calendar on created_at::date = financial_calendar.date::date
# MAGIC join master_am_list on relay_users.user_id::String = master_am_list.am_relay_id 
# MAGIC where 1=1 
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC select distinct 
# MAGIC 'Set Assignment' as category,
# MAGIC relay_users.full_name as action_by_name,
# MAGIC master_am_list.office as Office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC relay_reference_number::String,
# MAGIC action_at,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC --'no' as "contains_hubspot"
# MAGIC from planning_assignment_log
# MAGIC --left join financial_calendar on action_at::date = financial_calendar.date::date 
# MAGIC left join use_dates on action_at::date = use_dates.use_dates::date 
# MAGIC left join relay_users on action_by = relay_users.user_id 
# MAGIC join master_am_list on relay_users.user_id::String = master_am_list.am_relay_id 
# MAGIC where 1=1
# MAGIC
# MAGIC
# MAGIC Union
# MAGIC
# MAGIC select distinct 
# MAGIC 'Relay - Rolled a Load' As Activity_Type,
# MAGIC relay_users.full_name As Employee_Name,
# MAGIC new_office as Office ,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC b.relay_reference_number::string as Activity_Id ,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', rolled_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', rolled_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', rolled_at)
# MAGIC         ELSE 
# MAGIC             rolled_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from booking_projection b 
# MAGIC join relay_users on b.rolled_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on rolled_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and rolled_by is not null 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct 
# MAGIC 'Relay - Bounced a Load',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC b.relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', bounced_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', bounced_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', bounced_at)
# MAGIC         ELSE 
# MAGIC             bounced_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from booking_projection b 
# MAGIC join relay_users on b.bounced_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on bounced_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and bounced_by is not null 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct 
# MAGIC 'Relay - Reserved a Load',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC b.relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', reserved_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', reserved_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', reserved_at)
# MAGIC         ELSE 
# MAGIC             reserved_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from booking_projection b 
# MAGIC join relay_users on b.reserved_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on reserved_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC and reserved_by is not null 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Tender Accepted',
# MAGIC case when accepted_by is null then 'Auto Accept' else relay_users.full_name end as accepted_by_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC tendering_acceptance.relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', accepted_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', accepted_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', accepted_at)
# MAGIC         ELSE 
# MAGIC             accepted_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from tendering_acceptance
# MAGIC join use_dates on accepted_at::date = use_dates.use_dates::date 
# MAGIC left join relay_users on accepted_by = relay_users.user_id
# MAGIC join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC where 1=1
# MAGIC and "accepted?" = 'true'
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Manual Tender',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC tendering_tender_draft_id.relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', completed_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', completed_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', completed_at)
# MAGIC         ELSE 
# MAGIC             completed_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from tendering_tender_draft_id
# MAGIC join tendering_manual_tender_entry_usage on tendering_tender_draft_id.tender_draft_id::string = tendering_manual_tender_entry_usage.tender_draft_id::string
# MAGIC join use_dates on completed_at::date = use_dates.use_dates::date 
# MAGIC join relay_users on tendering_manual_tender_entry_usage.draft_by_user_id = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC where 1=1
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC case when event_type = 'Planning.Schedule.StopScheduled' then 'Relay - Stop Scheduled' else 'Relay - Stop Rescheduled' end as activity_type,
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC planning_schedule_activity.event_id::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', scheduled_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', scheduled_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', scheduled_at)
# MAGIC         ELSE 
# MAGIC             scheduled_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from planning_schedule_activity
# MAGIC join relay_users on planning_schedule_activity.scheduled_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on scheduled_at::date = use_dates.use_dates::date 
# MAGIC where 1=1
# MAGIC and event_type in ('Planning.Schedule.StopRescheduled','Planning.Schedule.StopScheduled')
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Cleared Appointment',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC planning_schedule_activity.event_id::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', cleared_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', cleared_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', cleared_at)
# MAGIC         ELSE 
# MAGIC             cleared_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from planning_schedule_activity
# MAGIC join relay_users on planning_schedule_activity.cleared_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on cleared_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and event_type = 'Planning.Schedule.StopScheduleCleared'
# MAGIC
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select distinct 
# MAGIC case when assignment_action = 'Assigned to Carrier Rep' then 'Relay - Assigned to Rep'
# MAGIC     when assignment_action = 'Assigned to Open Board' then 'Relay - Assigned to Open Board'
# MAGIC     when assignment_action = 'Assignment Given Back' then 'Relay - Assignment Given Back'
# MAGIC     when assignment_action = 'Plan Unassigned' then 'plan_unassigned' else assignment_action end,
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC event_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', action_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', action_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', action_at)
# MAGIC         ELSE 
# MAGIC             action_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from planning_assignment_log
# MAGIC join relay_users on planning_assignment_log.action_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on action_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Set Max Buy',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', set_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', set_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', set_at)
# MAGIC         ELSE 
# MAGIC             set_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from sourcing_max_buy_v2
# MAGIC join relay_users on sourcing_max_buy_v2.set_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on set_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Cancelled a Load',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC relay_reference_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', cancelled_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', cancelled_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', cancelled_at)
# MAGIC         ELSE 
# MAGIC             cancelled_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from tender_reference_numbers_projection
# MAGIC join relay_users on cancelled_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on cancelled_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC and cancelled_at is not null
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Put Offer on a Load',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC offer_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', offered_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', offered_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', offered_at)
# MAGIC         ELSE 
# MAGIC             offered_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from offer_negotiation_reflected
# MAGIC join relay_users on offered_by = relay_users.user_id 
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on offered_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Set Customer Money',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC charge_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', incurred_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', incurred_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', incurred_at)
# MAGIC         ELSE 
# MAGIC             incurred_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from moneying_billing_party_transaction
# MAGIC join relay_users on incurred_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on incurred_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC select distinct
# MAGIC 'Relay - Voided Customer Money',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC charge_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', voided_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', voided_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', voided_at)
# MAGIC         ELSE 
# MAGIC             voided_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC from moneying_billing_party_transaction
# MAGIC join relay_users on voided_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on voided_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and `voided?` = 'true'
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC 'Relay - Prepare Invoice',
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC event_number::string,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', invoiced_at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', invoiced_at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', invoiced_at)
# MAGIC         ELSE 
# MAGIC             invoiced_at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC FROM tl_invoice_projection
# MAGIC join relay_users on tl_invoice_projection.prepared_by = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on invoiced_at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC
# MAGIC
# MAGIC union 
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC case 
# MAGIC     when type = 'Tracking.TruckLoadThing.AppointmentCaptured' then 'Relay - Appointment Captured'
# MAGIC     when type = 'Tracking.TruckLoadThing.DetentionReported' then 'Relay - Detention Reported'
# MAGIC     when type = 'Tracking.TruckLoadThing.DriverDispatched' then 'Relay - Driver Dispatched'
# MAGIC     when type = 'Tracking.TruckLoadThing.DriverInformationCaptured' then 'Relay - Driver Info Captured'
# MAGIC     when type = 'Tracking.TruckLoadThing.EquipmentAssigned' then 'Relay - Equipment Assigned'
# MAGIC     when type = 'Tracking.TruckLoadThing.InTransitUpdateCaptured' then 'Relay - In Transit Update Captured'
# MAGIC     when type = 'Tracking.TruckLoadThing.MarkedArrivedAtStop' then 'Relay - Marked Arrived At Stop'
# MAGIC     when type = 'Tracking.TruckLoadThing.MarkedDelivered' then 'Relay - Marked Delivered'
# MAGIC     when type = 'Tracking.TruckLoadThing.MarkedDepartedFromStop' then 'Relay - Marked Departed From Stop'
# MAGIC     when type = 'Tracking.TruckLoadThing.MarkedLoaded' then 'Relay - Marked Loaded'
# MAGIC     when type = 'Tracking.TruckLoadThing.StopMarkedDelivered' then 'Relay - Stop Marked Delivered'
# MAGIC     when type = 'Tracking.TruckLoadThing.StopsReordered' then 'Relay - Stops Reordered'
# MAGIC     when type = 'Tracking.TruckLoadThing.TrackingContactInformationCaptured' then 'Relay - Tracking Contact Info Captured'
# MAGIC     when type = 'Tracking.TruckLoadThing.TrackingNoteCaptured' then 'Relay - Tracking Note Captured' end as typee,
# MAGIC relay_users.full_name,
# MAGIC new_office,
# MAGIC period_year,
# MAGIC week_end_date,
# MAGIC event_id,
# MAGIC CASE 
# MAGIC         WHEN new_office IN ('CLT', 'HAR', 'PHI', 'PROD', 'FLAT') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', at)
# MAGIC         WHEN new_office IN ('BNC', 'CHI', 'DAL', 'DRAY') THEN 
# MAGIC             convert_timezone( 'UTC', 'America/Chicago', at)
# MAGIC         WHEN new_office IN ('LAX', 'POR', 'SEA') THEN 
# MAGIC             convert_timezone('UTC', 'America/Los_Angeles', at)
# MAGIC         ELSE 
# MAGIC             at 
# MAGIC 			End As Activity_Date,
# MAGIC 1 as counter,
# MAGIC 'Relay' as Source_System_Type
# MAGIC FROM tracking_activity_v2
# MAGIC join relay_users on tracking_activity_v2.by_user_id = relay_users.user_id
# MAGIC left join new_office_lookup on relay_users.office_id = new_office_lookup.old_office 
# MAGIC join use_dates on at::date = use_dates.use_dates::date 
# MAGIC where 1=1 
# MAGIC and type not in ('Tracking.TruckLoadThing.TrackingStarted','Tracking.TruckLoadThing.TruckLoadThingChanged','Tracking.TruckLoadThing.StatusUpdateIntegrationIdentified','Tracking.TruckLoadThing.TrackingStopped')
# MAGIC ),
# MAGIC
# MAGIC Final_Code as (
# MAGIC Select  
# MAGIC Concat_ws(Activity_Type,Activity_Date,Activity_Id) as DW_Touch_Id,
# MAGIC Activity_Id,
# MAGIC Activity_Type,
# MAGIC Activity_Date::date,
# MAGIC Office,
# MAGIC Employee_Name,
# MAGIC Touch_Type,
# MAGIC period_year,
# MAGIC Week_End_Date,
# MAGIC Source_System_Type 
# MAGIC  from all_activity )
# MAGIC
# MAGIC  Select *,
# MAGIC SHA1(
# MAGIC   Concat(
# MAGIC     COALESCE(Activity_Id::string,''),
# MAGIC     COALESCE(Activity_Type::String,''),
# MAGIC     COALESCE(Activity_Date::String,''),
# MAGIC     COALESCE(Office::String,''),
# MAGIC     COALESCE(Employee_Name::String,''),
# MAGIC     COALESCE(Touch_Type::String,''),
# MAGIC     COALESCE(period_year::String,''),
# MAGIC     COALESCE(Week_End_Date::String,''),
# MAGIC     COALESCE(Source_System_Type,'') 
# MAGIC   )
# MAGIC  )As HashKey from Final_Code

# COMMAND ----------

# MAGIC %md
# MAGIC DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Silver.Sliver_Touch_Relay (
# MAGIC     DW_Touches_ID VARCHAR(255)NOT NULL PRIMARY KEY,
# MAGIC     Activity_Id VARCHAR (255),
# MAGIC     Activity_Type VARCHAR (255),
# MAGIC     Activity_Date DATE,
# MAGIC     Office VARCHAR (255),
# MAGIC     Employee_Name VARCHAR (255),
# MAGIC     Touches_Type INT,
# MAGIC     Period_Year VARCHAR (255),
# MAGIC     Week_end_date DATE,
# MAGIC     Source_System_Type VARCHAR(255),
# MAGIC     Hash_key VARCHAR (255),
# MAGIC     Created_Date TIMESTAMP_NTZ,
# MAGIC     Created_By VARCHAR(50) NOT NULL,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ,
# MAGIC     Last_Modified_by VARCHAR(50) NOT NULL,
# MAGIC     Is_Deleted SMALLINT
# MAGIC );