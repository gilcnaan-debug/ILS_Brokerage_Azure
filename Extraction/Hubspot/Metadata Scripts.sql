-- Databricks notebook source
-- INSERT INTO metadata.mastermetadata (
--     SourceSystem,
--     SourceSecretName,
--     TableID,
--     SubjectArea,
--     SourceDBName,
--     SourceSchema,
--     SourceTableName,
--     LoadType,
--     IsActive,
--     Frequency,
--     StagePath,
--     RawPath,
--     CuratedPath,
--     DWHSchemaName,
--     DWHTableName,
--     ErrorLogPath,
--     LastLoadDateColumn,
--     MergeKey,
--     DependencyTableIDs,
--     LastLoadDateValue,
--     PipelineEndTime,
--     PipelineStartTime,
--     PipelineRunStatus,
--     Zone,
--     MergeKeyColumn,
--     SourceSelectQuery,
--     UnixTime,
--     Job_ID,
--     NB_ID

-- ) VALUES 
-- (
--     'HubSpot',
--     null,
--     'H18',
--     'HubSpot',
--     'HubSpot_API-Shipper',
--     'api_response.results',
--     'HubSpot_Communications',
--     'incremental load',
--     1,
--     'daily',
--     null,
--     null,
--     null,
--     null,
--     null,
--     null,
--     'hs_lastmodifieddate',
--     'hs_object_id',
--     null,
--     '2018-03-27T14:34:17.326+00:00',
--     '2018-03-27T11:16:12.370+00:00',
--      null,
--     'Succeeded',
--     'Bronze',
--     null,
--     null,
--     null,
--     "JOB_H3",
--     "NB50"
-- ),
-- (
--     'HubSpot',
--     null,
--     'H19',
--     'HubSpot',
--     'HubSpot_API-Carrier',
--     'api_response.results',
--     'HubSpot_Communications',
--     'incremental load',
--     1,
--     'daily',
--     null,
--     null,
--     null,
--     null,
--     null,
--     null,
--     'hs_lastmodifieddate',
--     'hs_object_id',
--     null,
--     '2018-03-27T15:45:03.709+00:00',
--     '2018-03-27T07:18:29.967+00:00',
--      null,
--     'Succeeded',
--     'Bronze',
--     null,
--     null,
--     null,
--     "JOB_H3",
--     "NB50"
-- ),
-- (
--     'HubSpot',
--     null,
--     'H20',
--     'HubSpot',
--     'HubSpot_API-Shipper',
--     'api_response.results',
--     'HubSpot_Postal_Mail',
--     'incremental load',
--     1,
--     'daily',
--     null,
--     null,
--     null,
--     null,
--     null,
--     null,
--     'hs_lastmodifieddate',
--     'hs_object_id',
--     null,
--     '2018-03-27T13:03:19.069+00:00',
--     '2018-03-27T11:15:59.721+00:00',
--      null,
--     'Succeeded',
--     'Bronze',
--     null,
--     null,
--     null,
--     "JOB_H12",
--     "NB52"
-- ),
-- (
--     'HubSpot',
--     null,
--     'H21',
--     'HubSpot',
--     'HubSpot_API',
--     'api_response.results',
--     'HubSpot_Postal_Mail',
--     'incremental load',
--     1,
--     'daily',
--     null,
--     null,
--     null,
--     null,
--     null,
--     null,
--     'hs_lastmodifieddate',
--     'hs_object_id',
--     null,
--     '2018-03-27T13:03:19.069+00:00',
--     '2018-03-27T11:15:59.721+00:00',
--      null,
--     'Succeeded',
--     'Bronze',
--     null,
--     null,
--     null,
--     "JOB_H12",
--     "NB52"
-- ),
-- (
--     'HubSpot',
--     null,
--     'H22',
--     'HubSpot',
--     'HubSpot_API-Shipper',
--     'api_response.results',
--     'HubSpot_Tasks',
--     'incremental load',
--     1,
--     'daily',
--     null,
--     null,
--     null,
--     null,
--     null,
--     null,
--     'hs_lastmodifieddate',
--     'hs_object_id',
--     null,
--     '2018-03-27T13:03:19.069+00:00',
--     '2018-03-27T11:15:59.721+00:00',
--      null,
--     'Succeeded',
--     'Bronze',
--     null,
--     null,
--     null,
--     "JOB_H13",
--     "NB53"
-- ),
-- (
--     'HubSpot',
--     null,
--     'H23',
--     'HubSpot',
--     'HubSpot_API-Carrier',
--     'api_response.results',
--     'HubSpot_Tasks',
--     'incremental load',
--     1,
--     'daily',
--     null,
--     null,
--     null,
--     null,
--     null,
--     null,
--     'hs_lastmodifieddate',
--     'hs_object_id',
--     null,
--     '2018-03-27T13:03:19.069+00:00',
--     '2018-03-27T11:15:59.721+00:00',
--      null,
--     'Succeeded',
--     'Bronze',
--     null,
--     null,
--     null,
--     "JOB_H13",
--     "NB53"
-- );

-- COMMAND ----------

UPDATE METADATA.MASTERMETADATA SET SourceSelectQuery = '''hs_object_id,about_us,accessorial_fuel_schedule,account_manager,account_manager__cloned_,account_status,account_status_detail,action_item___overview,additional_notes___overview,address,address2,aljex_id,aljex_sales_rep,annualrevenue,archived,archived_at,assignment_helper,automated_tracking_provider,automated_tracking_status,caam_cdr_dashboard,cacl_2,cad_owner,cargo_value,carrier_am,carrier_am__2_,cdr,cdr__2_,city,click_here_for_the_caam_cdr_dashboard,closedate,contact_email,contact_phone_number,continuous_improvement_items,contract_document,country,createdate,ct_office_rep,days_to_close,deal_name,description,documents,domain,dot_number,dray_owner,due_date___overview,eld_provider,engagements_last_meeting_booked,engagements_last_meeting_booked_campaign,engagements_last_meeting_booked_medium,engagements_last_meeting_booked_source,escalation_poc,external_business_review_meeting,external_operational_review_meeting,external_project_status_update_meeting,external_strategic_planning_meeting,facebook_company_page,facebookfans,first_contact_createdate,first_contact_createdate_timestamp_earliest_value_78b50eea,first_conversion_date,first_conversion_event_name,first_deal_created_date,founded_year,fourkites_connected_to_nfi_,googleplus_page,hs_additional_domains,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_owner_ids,hs_all_team_ids,hs_analytics_first_timestamp,hs_analytics_first_touch_converting_campaign,hs_analytics_first_visit_timestamp,hs_analytics_last_timestamp,hs_analytics_last_timestamp_timestamp_latest_value_4e16365a,hs_analytics_last_touch_converting_campaign,hs_analytics_last_touch_converting_campaign_timestamp_latest_value_81a64e30,hs_analytics_last_visit_timestamp,hs_analytics_last_visit_timestamp_timestamp_latest_value_999a0fce,hs_analytics_latest_source,hs_analytics_latest_source_data_1,hs_analytics_latest_source_data_2,hs_analytics_latest_source_timestamp,hs_analytics_num_page_views,hs_analytics_num_page_views_cardinality_sum_e46e85b0,hs_analytics_num_visits,hs_analytics_num_visits_cardinality_sum_53d952a6,hs_analytics_source,hs_analytics_source_data_1,hs_analytics_source_data_2,hs_annual_revenue_currency_code,hs_avatar_filemanager_key,hs_country_code,hs_created_by_user_id,hs_createdate,hs_csm_sentiment,hs_customer_success_ticket_sentiment,hs_date_entered_customer,hs_date_entered_evangelist,hs_date_entered_lead,hs_date_entered_marketingqualifiedlead,hs_date_entered_opportunity,hs_date_entered_other,hs_date_entered_salesqualifiedlead,hs_date_entered_subscriber,hs_date_exited_customer,hs_date_exited_evangelist,hs_date_exited_lead,hs_date_exited_marketingqualifiedlead,hs_date_exited_opportunity,hs_date_exited_other,hs_date_exited_salesqualifiedlead,hs_date_exited_subscriber,hs_employee_range,hs_ideal_customer_profile,hs_industry_group,hs_is_enriched,hs_is_target_account,hs_keywords,hs_last_booked_meeting_date,hs_last_logged_call_date,hs_last_logged_outgoing_email_date,hs_last_metered_enrichment_timestamp,hs_last_open_task_date,hs_last_sales_activity_date,hs_last_sales_activity_timestamp,hs_last_sales_activity_type,hs_lastmodifieddate,hs_latest_createdate_of_active_subscriptions,hs_latest_meeting_activity,hs_lead_status,hs_linkedin_handle,hs_logo_url,hs_merged_object_ids,hs_notes_last_activity,hs_notes_next_activity,hs_notes_next_activity_type,hs_num_blockers,hs_num_child_companies,hs_num_contacts_with_buying_roles,hs_num_decision_makers,hs_num_open_deals,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_parent_company_id,hs_pinned_engagement_id,hs_pipeline,hs_predictivecontactscore_v2,hs_predictivecontactscore_v2_next_max_max_d4e58c1e,hs_quick_context,hs_read_only,hs_revenue_range,hs_sales_email_last_replied,hs_shared_team_ids,hs_shared_user_ids,hs_source_object_id,hs_target_account,hs_target_account_probability,hs_target_account_recommendation_snooze_time,hs_target_account_recommendation_state,hs_task_label,hs_time_in_customer,hs_time_in_evangelist,hs_time_in_lead,hs_time_in_marketingqualifiedlead,hs_time_in_opportunity,hs_time_in_other,hs_time_in_salesqualifiedlead,hs_time_in_subscriber,hs_total_deal_value,hs_unique_creation_key,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_was_imported,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,hubspotscore,important_dates,industry,intake_form_submission_date,internal_business_review_meeting,internal_operational_review_meeting,internal_project_status_update_meeting,internal_strategic_planning_meeting,is_public,lane_preference,lane_preference__cloned_,lane_preference_1__last_update_date_,lane_preference_10,lane_preference_3,lane_preference_4,lane_preference_5,lane_preference_6,lane_preference_7,lane_preference_8,lane_preference_9,lawson_id,lifecyclestage,linkedin_company_page,linkedinbio,mc_number,metabase_summary,metric_1,metric_2,metric_3,modes,modes___available_but_not_managed_by_nfi,n1__current_year,n1__previous_year,n1__variance,n2__current_year,n2__previous_year,n2__variance,n3__current_year,n3__previous_year,n3__variance,n3x3_email,n3x3_name,n3x3_phone_number,n3x3_role,n4__current_year,n4__previous_year,n4__variance,n4pl_name,name,nfi_branch,nfi_employee_submitting_form,nfi_onboarding_date,note_body,notes,notes_last_contacted,notes_last_updated,notes_next_activity_date,num_associated_contacts,num_associated_deals,num_contacted_notes,num_conversion_events,num_conversion_events_cardinality_sum_d095f14b,num_notes,numberofemployees,of_company_drivers,of_flatbeds,of_owner_operators,of_reefers,of_tractors,of_trailers,of_vans,on_time_delivery,on_time_delivery_percentage,on_time_pickup,on_time_pickup_percentage,onboarding_date,online_portal_usage,opportunity,owner___overview,owneremail,ownername,payment_terms,peak_utilization,phone,pipeline,post_lane,priority_level___overview,recent_conversion_date,recent_conversion_date_timestamp_latest_value_72856da1,recent_conversion_event_name,recent_conversion_event_name_timestamp_latest_value_66c820bf,recent_deal_amount,recent_deal_close_date,relay_carrier_id,responsible_party___overview,rfp_document,rfp_strategy_form_completed,sales_owner,scope,send_large_opportunities_to_,shipper_assessorial,shipper_expectations_of_nfi,shipper_fuel_chart_schedule,shipper_fuel_schedule_document,shipper_industry,shipper_performance_expectations__kpis_,slack_channel,sop,state,status___overview,strength,system,target_caam,target_cdr,target_rep,tender_source,tender_type,threat,timezone,tms_name,total_money_raised,total_revenue,transportation_spend,twitterbio,twitterfollowers,twitterhandle,type,type_of_business,variance_calc,vertical,weakness,web_technologies,website,zip,SourcesystemID'''

WHERE TableID IN ('H8', 'H2')

-- COMMAND ----------

-- UPDATE Metadata.mastermetadata SET DWHTableName = SourceTableName WHERE TableID IN ("H1",
-- "H2",
-- "H3",
-- "H4",
-- "H5",
-- "H6",
-- "H7",
-- "H52",
-- "H8",
-- "H14",
-- "H12",
-- "H9",
-- "H13",
-- "H11",
-- "H10",
-- "H15",
-- "H18" ,
-- "H19", 
-- "H20",
-- "H21",
-- "H22",
-- "H23",


-- )

-- COMMAND ----------

UPDATE METADATA.MASTERMETADATA SET SourceSelectQuery = "hs_object_id,hs_activity_type,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_owner_ids,hs_all_team_ids,hs_at_mentioned_owner_ids,hs_attachment_ids,hs_body_preview,hs_body_preview_html,hs_body_preview_is_truncated,hs_call_app_id,hs_call_authed_url_provider,hs_call_body,hs_call_callee_object_id,hs_call_callee_object_type,hs_call_deal_stage_during_call,hs_call_direction,hs_call_disposition,hs_call_duration,hs_call_external_account_id,hs_call_external_id,hs_call_from_number,hs_call_from_number_nickname,hs_call_has_transcript,hs_call_has_voicemail,hs_call_location,hs_call_meeting_id,hs_call_ms_teams_payload,hs_call_phone_number_source,hs_call_primary_deal,hs_call_recording_duration,hs_call_recording_url,hs_call_routing_strategy_type,hs_call_source,hs_call_status,hs_call_suggested_next_actions,hs_call_summary,hs_call_summary_execution_id,hs_call_title,hs_call_to_number,hs_call_to_number_nickname,hs_call_transcript_audio_num_channels,hs_call_transcript_tracked_terms,hs_call_transcription_id,hs_call_unified_phone_number_id,hs_call_unique_external_id,hs_call_video_meeting_type,hs_call_video_recording_url,hs_call_zoom_meeting_uuid,hs_calls_service_call_id,hs_campaign_guid,hs_connected_count,hs_created_by,hs_created_by_user_id,hs_createdate,hs_engagement_source,hs_engagement_source_id,hs_follow_up_action,hs_gdpr_deleted,hs_hosted_video_conference_url,hs_lastmodifieddate,hs_merged_object_ids,hs_modified_by,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_product_name,hs_queue_membership_ids,hs_read_only,hs_shared_team_ids,hs_shared_user_ids,hs_timestamp,hs_unique_creation_key,hs_unique_id,hs_unknown_visitor_conversation,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_voicemail_count,hs_was_imported,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,archived,archived_at,SourcesystemID"
WHERE TableID IN ('H10', 'H15')

-- COMMAND ----------

UPDATE METADATA.mastermetadata SET SourceSelectQuery = "hs_object_id,additional_lost_revenue__for_reporting_,amount,amount_in_home_currency,archived,archived_at,award_date,awarded_amount,awarded_number_of_lanes,awarded_volume,business_review_date,business_review_deck_is_approved,business_review_meeting_complete_,business_review_meeting_complete_date,business_review_scheduled,closed_lost_reason,closed_won_reason,closedate,consignee_city,consignee_state,consignee_zip,createdate,days_to_close,deal_currency_code,dealname,dealstage,dealtype,deck_sent_to_shipper_partner,description,engagements_last_meeting_booked,engagements_last_meeting_booked_campaign,engagements_last_meeting_booked_medium,engagements_last_meeting_booked_source,hs_acv,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_collaborator_owner_ids,hs_all_deal_split_owner_ids,hs_all_owner_ids,hs_all_team_ids,hs_analytics_latest_source,hs_analytics_latest_source_company,hs_analytics_latest_source_contact,hs_analytics_latest_source_data_1,hs_analytics_latest_source_data_1_company,hs_analytics_latest_source_data_1_contact,hs_analytics_latest_source_data_2,hs_analytics_latest_source_data_2_company,hs_analytics_latest_source_data_2_contact,hs_analytics_latest_source_timestamp,hs_analytics_latest_source_timestamp_company,hs_analytics_latest_source_timestamp_contact,hs_analytics_source,hs_analytics_source_data_1,hs_analytics_source_data_2,hs_arr,hs_associated_deal_registration_deal_type,hs_associated_deal_registration_product_interests,hs_attributed_team_ids,hs_campaign,hs_closed_amount,hs_closed_amount_in_home_currency,hs_closed_deal_close_date,hs_closed_deal_create_date,hs_closed_won_count,hs_closed_won_date,hs_created_by_user_id,hs_createdate,hs_date_entered_13305417,hs_date_entered_13305418,hs_date_entered_13305422,hs_date_entered_13305423,hs_date_entered_13897383,hs_date_entered_13897384,hs_date_entered_13897385,hs_date_entered_13897386,hs_date_entered_13897387,hs_date_entered_13897388,hs_date_entered_13897389,hs_date_entered_13897390,hs_date_entered_13916564,hs_date_entered_appointmentscheduled,hs_date_entered_closedlost,hs_date_entered_closedwon,hs_date_entered_contractsent,hs_date_entered_decisionmakerboughtin,hs_date_entered_presentationscheduled,hs_date_entered_qualifiedtobuy,hs_date_exited_13305417,hs_date_exited_13305418,hs_date_exited_13305422,hs_date_exited_13305423,hs_date_exited_13897383,hs_date_exited_13897384,hs_date_exited_13897385,hs_date_exited_13897386,hs_date_exited_13897387,hs_date_exited_13897388,hs_date_exited_13897389,hs_date_exited_13897390,hs_date_exited_13916564,hs_date_exited_appointmentscheduled,hs_date_exited_closedlost,hs_date_exited_closedwon,hs_date_exited_contractsent,hs_date_exited_decisionmakerboughtin,hs_date_exited_presentationscheduled,hs_date_exited_qualifiedtobuy,hs_days_to_close_raw,hs_deal_amount_calculation_preference,hs_deal_registration_mrr,hs_deal_registration_mrr_currency_code,hs_deal_score,hs_deal_stage_probability,hs_deal_stage_probability_shadow,hs_exchange_rate,hs_forecast_amount,hs_forecast_probability,hs_has_empty_conditional_stage_properties,hs_is_active_shared_deal,hs_is_closed,hs_is_closed_count,hs_is_closed_lost,hs_is_closed_won,hs_is_deal_split,hs_is_in_first_deal_stage,hs_is_open_count,hs_lastmodifieddate,hs_latest_approval_status,hs_latest_approval_status_approval_id,hs_latest_meeting_activity,hs_likelihood_to_close,hs_line_item_global_term_hs_discount_percentage,hs_line_item_global_term_hs_discount_percentage_enabled,hs_line_item_global_term_hs_recurring_billing_period,hs_line_item_global_term_hs_recurring_billing_period_enabled,hs_line_item_global_term_hs_recurring_billing_start_date,hs_line_item_global_term_hs_recurring_billing_start_date_enabled,hs_line_item_global_term_recurringbillingfrequency,hs_line_item_global_term_recurringbillingfrequency_enabled,hs_manual_forecast_category,hs_merged_object_ids,hs_mrr,hs_net_pipeline_impact,hs_next_step,hs_next_step_updated_at,hs_notes_last_activity,hs_notes_next_activity,hs_notes_next_activity_type,hs_num_associated_active_deal_registrations,hs_num_associated_deal_registrations,hs_num_associated_deal_splits,hs_num_of_associated_line_items,hs_num_target_accounts,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_open_deal_create_date,hs_pinned_engagement_id,hs_predicted_amount,hs_predicted_amount_in_home_currency,hs_priority,hs_projected_amount,hs_projected_amount_in_home_currency,hs_read_only,hs_sales_email_last_replied,hs_shared_team_ids,hs_shared_user_ids,hs_source_object_id,hs_synced_deal_owner_name_and_email,hs_tag_ids,hs_tcv,hs_time_in_13305417,hs_time_in_13305418,hs_time_in_13305422,hs_time_in_13305423,hs_time_in_13897383,hs_time_in_13897384,hs_time_in_13897385,hs_time_in_13897386,hs_time_in_13897387,hs_time_in_13897388,hs_time_in_13897389,hs_time_in_13897390,hs_time_in_13916564,hs_time_in_appointmentscheduled,hs_time_in_closedlost,hs_time_in_closedwon,hs_time_in_contractsent,hs_time_in_decisionmakerboughtin,hs_time_in_presentationscheduled,hs_time_in_qualifiedtobuy,hs_unique_creation_key,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_v2_cumulative_time_in_13305417,hs_v2_cumulative_time_in_13305418,hs_v2_cumulative_time_in_13305422,hs_v2_cumulative_time_in_13305423,hs_v2_cumulative_time_in_13897383,hs_v2_cumulative_time_in_13897384,hs_v2_cumulative_time_in_13897385,hs_v2_cumulative_time_in_13897386,hs_v2_cumulative_time_in_13897387,hs_v2_cumulative_time_in_13897388,hs_v2_cumulative_time_in_13897389,hs_v2_cumulative_time_in_13897390,hs_v2_cumulative_time_in_13916564,hs_v2_cumulative_time_in_appointmentscheduled,hs_v2_cumulative_time_in_closedlost,hs_v2_cumulative_time_in_closedwon,hs_v2_cumulative_time_in_contractsent,hs_v2_cumulative_time_in_decisionmakerboughtin,hs_v2_cumulative_time_in_presentationscheduled,hs_v2_cumulative_time_in_qualifiedtobuy,hs_v2_date_entered_13305417,hs_v2_date_entered_13305418,hs_v2_date_entered_13305422,hs_v2_date_entered_13305423,hs_v2_date_entered_13897383,hs_v2_date_entered_13897384,hs_v2_date_entered_13897385,hs_v2_date_entered_13897386,hs_v2_date_entered_13897387,hs_v2_date_entered_13897388,hs_v2_date_entered_13897389,hs_v2_date_entered_13897390,hs_v2_date_entered_13916564,hs_v2_date_entered_appointmentscheduled,hs_v2_date_entered_closedlost,hs_v2_date_entered_closedwon,hs_v2_date_entered_contractsent,hs_v2_date_entered_current_stage,hs_v2_date_entered_decisionmakerboughtin,hs_v2_date_entered_presentationscheduled,hs_v2_date_entered_qualifiedtobuy,hs_v2_date_exited_13305417,hs_v2_date_exited_13305418,hs_v2_date_exited_13305422,hs_v2_date_exited_13305423,hs_v2_date_exited_13897383,hs_v2_date_exited_13897384,hs_v2_date_exited_13897385,hs_v2_date_exited_13897386,hs_v2_date_exited_13897387,hs_v2_date_exited_13897388,hs_v2_date_exited_13897389,hs_v2_date_exited_13897390,hs_v2_date_exited_13916564,hs_v2_date_exited_appointmentscheduled,hs_v2_date_exited_closedlost,hs_v2_date_exited_closedwon,hs_v2_date_exited_contractsent,hs_v2_date_exited_decisionmakerboughtin,hs_v2_date_exited_presentationscheduled,hs_v2_date_exited_qualifiedtobuy,hs_v2_latest_time_in_13305417,hs_v2_latest_time_in_13305418,hs_v2_latest_time_in_13305422,hs_v2_latest_time_in_13305423,hs_v2_latest_time_in_13897383,hs_v2_latest_time_in_13897384,hs_v2_latest_time_in_13897385,hs_v2_latest_time_in_13897386,hs_v2_latest_time_in_13897387,hs_v2_latest_time_in_13897388,hs_v2_latest_time_in_13897389,hs_v2_latest_time_in_13897390,hs_v2_latest_time_in_13916564,hs_v2_latest_time_in_appointmentscheduled,hs_v2_latest_time_in_closedlost,hs_v2_latest_time_in_closedwon,hs_v2_latest_time_in_contractsent,hs_v2_latest_time_in_decisionmakerboughtin,hs_v2_latest_time_in_presentationscheduled,hs_v2_latest_time_in_qualifiedtobuy,hs_v2_time_in_current_stage,hs_was_imported,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,lost_won_rev,missed_number_of_lanes,missed_opportunity_amount,missed_opportunity_volume,mode,next_step_with_customer,notes_last_contacted,notes_last_updated,notes_next_activity_date,num_associated_contacts,num_contacted_notes,num_notes,number_of_lanes,pipeline,quoted_lanes,quoted_revenue,quoted_revenue__lost_amount_,quoted_revenue__won_amount_,quoted_volume,shipper_city,shipper_state,shipper_zip,summarized_deal_information,volume,SourcesystemID"
WHERE TableID IN ('H1','H9')

-- COMMAND ----------

UPDATE metadata.mastermetadata SET SourceSelectQuery = "hs_object_id,address,annualrevenue,archived,archived_at,associatedcompanyid,associatedcompanylastupdated,cdr,cdr__2_,city,closedate,company,company_size,contact_level,contact_type,country,createdate,currentlyinworkflow,date_of_birth,days_to_close,dba_name,degree,dot_number,email,engagements_last_meeting_booked,engagements_last_meeting_booked_campaign,engagements_last_meeting_booked_medium,engagements_last_meeting_booked_source,fax,field_of_study,first_conversion_date,first_conversion_event_name,first_deal_created_date,firstname,flatbed_count,fleet_size,followercount,gender,graduation_date,heymarket_webchat,hm_primary_contact,hs_additional_emails,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_contact_vids,hs_all_owner_ids,hs_all_team_ids,hs_analytics_average_page_views,hs_analytics_first_referrer,hs_analytics_first_timestamp,hs_analytics_first_touch_converting_campaign,hs_analytics_first_url,hs_analytics_first_visit_timestamp,hs_analytics_last_referrer,hs_analytics_last_timestamp,hs_analytics_last_touch_converting_campaign,hs_analytics_last_url,hs_analytics_last_visit_timestamp,hs_analytics_num_event_completions,hs_analytics_num_page_views,hs_analytics_num_visits,hs_analytics_revenue,hs_analytics_source,hs_analytics_source_data_1,hs_analytics_source_data_2,hs_associated_target_accounts,hs_avatar_filemanager_key,hs_buying_role,hs_calculated_form_submissions,hs_calculated_merged_vids,hs_calculated_mobile_number,hs_calculated_phone_number,hs_calculated_phone_number_area_code,hs_calculated_phone_number_country_code,hs_calculated_phone_number_region_code,hs_clicked_linkedin_ad,hs_contact_enrichment_opt_out,hs_contact_enrichment_opt_out_timestamp,hs_content_membership_email,hs_content_membership_email_confirmed,hs_content_membership_follow_up_enqueued_at,hs_content_membership_notes,hs_content_membership_registered_at,hs_content_membership_registration_domain_sent_to,hs_content_membership_registration_email_sent_at,hs_content_membership_status,hs_conversations_visitor_email,hs_count_is_unworked,hs_count_is_worked,hs_country_region_code,hs_created_by_conversations,hs_created_by_user_id,hs_createdate,hs_currently_enrolled_in_prospecting_agent,hs_data_privacy_ads_consent,hs_date_entered_941581212,hs_date_entered_customer,hs_date_entered_evangelist,hs_date_entered_lead,hs_date_entered_marketingqualifiedlead,hs_date_entered_opportunity,hs_date_entered_other,hs_date_entered_salesqualifiedlead,hs_date_entered_subscriber,hs_date_exited_941581212,hs_date_exited_customer,hs_date_exited_evangelist,hs_date_exited_lead,hs_date_exited_marketingqualifiedlead,hs_date_exited_opportunity,hs_date_exited_other,hs_date_exited_salesqualifiedlead,hs_date_exited_subscriber,hs_document_last_revisited,hs_email_bad_address,hs_email_bounce,hs_email_click,hs_email_customer_quarantined_reason,hs_email_delivered,hs_email_domain,hs_email_first_click_date,hs_email_first_open_date,hs_email_first_reply_date,hs_email_first_send_date,hs_email_hard_bounce_reason,hs_email_hard_bounce_reason_enum,hs_email_is_ineligible,hs_email_last_click_date,hs_email_last_email_name,hs_email_last_open_date,hs_email_last_reply_date,hs_email_last_send_date,hs_email_open,hs_email_optimal_send_day_of_week,hs_email_optimal_send_time_of_day,hs_email_optout,hs_email_optout_10807423,hs_email_optout_10884087,hs_email_optout_110272716,hs_email_optout_110659195,hs_email_optout_28585437,hs_email_optout_29079123,hs_email_quarantined,hs_email_quarantined_reason,hs_email_recipient_fatigue_recovery_time,hs_email_replied,hs_email_sends_since_last_engagement,hs_emailconfirmationstatus,hs_employment_change_detected_date,hs_enriched_email_bounce_detected,hs_facebook_ad_clicked,hs_facebook_click_id,hs_facebookid,hs_feedback_last_nps_follow_up,hs_feedback_last_nps_rating,hs_feedback_last_survey_date,hs_feedback_show_nps_web_survey,hs_first_closed_order_id,hs_first_engagement_object_id,hs_first_order_closed_date,hs_first_outreach_date,hs_first_subscription_create_date,hs_full_name_or_email,hs_google_click_id,hs_googleplusid,hs_has_active_subscription,hs_ip_timezone,hs_is_contact,hs_is_enriched,hs_is_unworked,hs_job_change_detected_date,hs_journey_stage,hs_language,hs_last_metered_enrichment_timestamp,hs_last_sales_activity_date,hs_last_sales_activity_timestamp,hs_last_sales_activity_type,hs_lastmodifieddate,hs_latest_disqualified_lead_date,hs_latest_meeting_activity,hs_latest_open_lead_date,hs_latest_qualified_lead_date,hs_latest_sequence_ended_date,hs_latest_sequence_enrolled,hs_latest_sequence_enrolled_date,hs_latest_sequence_finished_date,hs_latest_sequence_unenrolled_date,hs_latest_source,hs_latest_source_data_1,hs_latest_source_data_2,hs_latest_source_timestamp,hs_latest_subscription_create_date,hs_lead_status,hs_legal_basis,hs_lifecyclestage_customer_date,hs_lifecyclestage_evangelist_date,hs_lifecyclestage_lead_date,hs_lifecyclestage_marketingqualifiedlead_date,hs_lifecyclestage_opportunity_date,hs_lifecyclestage_other_date,hs_lifecyclestage_salesqualifiedlead_date,hs_lifecyclestage_subscriber_date,hs_linkedin_ad_clicked,hs_linkedin_url,hs_linkedinid,hs_live_enrichment_deadline,hs_marketable_reason_id,hs_marketable_reason_type,hs_marketable_status,hs_marketable_until_renewal,hs_membership_has_accessed_private_content,hs_membership_last_private_content_access_date,hs_merged_object_ids,hs_mobile_sdk_push_tokens,hs_notes_last_activity,hs_notes_next_activity,hs_notes_next_activity_type,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_persona,hs_pinned_engagement_id,hs_pipeline,hs_predictivecontactscore,hs_predictivecontactscore_v2,hs_predictivecontactscorebucket,hs_predictivescoringtier,hs_prospecting_agent_actively_enrolled_count,hs_quarantined_emails,hs_read_only,hs_recent_closed_order_date,hs_registered_member,hs_registration_method,hs_returning_to_office_detected_date,hs_role,hs_sa_first_engagement_date,hs_sa_first_engagement_descr,hs_sa_first_engagement_object_type,hs_sales_email_last_clicked,hs_sales_email_last_opened,hs_sales_email_last_replied,hs_searchable_calculated_international_mobile_number,hs_searchable_calculated_international_phone_number,hs_searchable_calculated_mobile_number,hs_searchable_calculated_phone_number,hs_seniority,hs_sequences_actively_enrolled_count,hs_sequences_enrolled_count,hs_sequences_is_enrolled,hs_shared_team_ids,hs_shared_user_ids,hs_social_facebook_clicks,hs_social_google_plus_clicks,hs_social_last_engagement,hs_social_linkedin_clicks,hs_social_num_broadcast_clicks,hs_social_twitter_clicks,hs_source_object_id,hs_source_portal_id,hs_state_code,hs_sub_role,hs_testpurge,hs_testrollback,hs_time_between_contact_creation_and_deal_close,hs_time_between_contact_creation_and_deal_creation,hs_time_in_941581212,hs_time_in_customer,hs_time_in_evangelist,hs_time_in_lead,hs_time_in_marketingqualifiedlead,hs_time_in_opportunity,hs_time_in_other,hs_time_in_salesqualifiedlead,hs_time_in_subscriber,hs_time_to_first_engagement,hs_time_to_move_from_lead_to_customer,hs_time_to_move_from_marketingqualifiedlead_to_customer,hs_time_to_move_from_opportunity_to_customer,hs_time_to_move_from_salesqualifiedlead_to_customer,hs_time_to_move_from_subscriber_to_customer,hs_timezone,hs_twitterid,hs_unique_creation_key,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_v2_cumulative_time_in_customer,hs_v2_cumulative_time_in_evangelist,hs_v2_cumulative_time_in_lead,hs_v2_cumulative_time_in_marketingqualifiedlead,hs_v2_cumulative_time_in_opportunity,hs_v2_cumulative_time_in_other,hs_v2_cumulative_time_in_salesqualifiedlead,hs_v2_cumulative_time_in_subscriber,hs_v2_date_entered_customer,hs_v2_date_entered_evangelist,hs_v2_date_entered_lead,hs_v2_date_entered_marketingqualifiedlead,hs_v2_date_entered_opportunity,hs_v2_date_entered_other,hs_v2_date_entered_salesqualifiedlead,hs_v2_date_entered_subscriber,hs_v2_date_exited_customer,hs_v2_date_exited_evangelist,hs_v2_date_exited_lead,hs_v2_date_exited_marketingqualifiedlead,hs_v2_date_exited_opportunity,hs_v2_date_exited_other,hs_v2_date_exited_salesqualifiedlead,hs_v2_date_exited_subscriber,hs_v2_latest_time_in_customer,hs_v2_latest_time_in_evangelist,hs_v2_latest_time_in_lead,hs_v2_latest_time_in_marketingqualifiedlead,hs_v2_latest_time_in_opportunity,hs_v2_latest_time_in_other,hs_v2_latest_time_in_salesqualifiedlead,hs_v2_latest_time_in_subscriber,hs_was_imported,hs_whatsapp_phone_number,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,hubspotscore,industry,ip_city,ip_country,ip_country_code,ip_latlon,ip_state,ip_state_code,ip_zipcode,job_function,jobtitle,kloutscoregeneral,lastmodifieddate,lastname,lifecyclestage,linkedinbio,linkedinconnections,marital_status,mc_number,message,messagebird_contactid,military_status,mobilephone,nfi_relationship_management,notes,notes_last_contacted,notes_last_updated,notes_next_activity_date,num_associated_deals,num_contacted_notes,num_conversion_events,num_notes,num_unique_conversion_events,numemployees,owneremail,ownername,phone,photo,recent_conversion_date,recent_conversion_event_name,recent_deal_amount,recent_deal_close_date,reefer_count,relationship_status,role,sakari_message_received_at,sakari_message_received_destination,sakari_message_received_error,sakari_message_received_id,sakari_message_received_message,sakari_message_received_price,sakari_message_received_segments,sakari_message_received_status,sakari_message_received_type,sakari_message_sent_at,sakari_message_sent_by,sakari_message_sent_error,sakari_message_sent_id,sakari_message_sent_message,sakari_message_sent_price,sakari_message_sent_segments,sakari_message_sent_source,sakari_message_sent_status,sakari_message_sent_type,sakari_opt_in,sakari_opt_out,sakari_send_sms,sakari_sms_received_qty,sakari_sms_sent_qty,salutation,school,seniority,start_date,state,surveymonkeyeventlastupdated,title,total_revenue,tractor_count,trailer_count,twitterbio,twitterhandle,twitterprofilephoto,van_count,webinareventlastupdated,website,work_email,zip,SourcesystemID"
WHERE TABLEID  IN ('H3', 'H11')

-- COMMAND ----------

UPDATE metadata.mastermetadata SET SourceSelectQuery = 'hs_object_id,archived,archived_at,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_owner_ids,hs_all_team_ids,hs_at_mentioned_owner_ids,hs_attachment_ids,hs_body_preview,hs_body_preview_html,hs_body_preview_is_truncated,hs_communication_body,hs_communication_channel_type,hs_communication_conversation_associations_synced_at,hs_communication_conversation_object_id,hs_communication_conversations_channel_ids,hs_communication_conversations_channel_instance_ids,hs_communication_conversations_first_message_at,hs_communication_conversations_thread_id,hs_communication_logged_from,hs_created_by,hs_created_by_user_id,hs_createdate,hs_engagement_source,hs_engagement_source_id,hs_gdpr_deleted,hs_lastmodifieddate,hs_merged_object_ids,hs_modified_by,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_read_only,hs_shared_team_ids,hs_shared_user_ids,hs_timestamp,hs_unique_creation_key,hs_unique_id,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_was_imported,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,SourcesystemID'
WHERE TableID IN ('H18', 'H19')

-- COMMAND ----------

UPDATE Metadata.mastermetadata SET SourceSelectQuery = 'hs_object_id,archived,archived_at,hs_activity_type,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_owner_ids,hs_all_team_ids,hs_at_mentioned_owner_ids,hs_attachment_ids,hs_attendee_owner_ids,hs_body_preview,hs_body_preview_html,hs_body_preview_is_truncated,hs_booking_instance_id,hs_campaign_guid,hs_contact_first_outreach_date,hs_created_by,hs_created_by_scheduling_page,hs_created_by_user_id,hs_createdate,hs_engagement_source,hs_engagement_source_id,hs_external_calendar_id,hs_follow_up_action,hs_follow_up_context,hs_follow_up_tasks_remaining,hs_gdpr_deleted,hs_guest_emails,hs_i_cal_uid,hs_include_description_in_reminder,hs_internal_meeting_notes,hs_lastmodifieddate,hs_meeting_body,hs_meeting_calendar_event_hash,hs_meeting_change_id,hs_meeting_created_from_link_id,hs_meeting_end_time,hs_meeting_external_url,hs_meeting_location,hs_meeting_location_type,hs_meeting_ms_teams_payload,hs_meeting_notetaker_id,hs_meeting_outcome,hs_meeting_payments_session_id,hs_meeting_pre_meeting_prospect_reminders,hs_meeting_source,hs_meeting_source_id,hs_meeting_start_time,hs_meeting_title,hs_meeting_web_conference_meeting_id,hs_merged_object_ids,hs_modified_by,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_outcome_canceled_count,hs_outcome_completed_count,hs_outcome_no_show_count,hs_outcome_rescheduled_count,hs_outcome_scheduled_count,hs_product_name,hs_queue_membership_ids,hs_read_only,hs_roster_object_coordinates,hs_scheduled_tasks,hs_shared_team_ids,hs_shared_user_ids,hs_time_to_book_meeting_from_first_contact,hs_timestamp,hs_timezone,hs_unique_creation_key,hs_unique_id,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_video_conference_url,hs_was_imported,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,SourcesystemID'
WHERE TableID IN ('H6','H13')

-- COMMAND ----------

UPDATE Metadata.mastermetadata SET SOurceSelectQuery = 'hs_object_id,archived,archived_at,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_owner_ids,hs_all_team_ids,hs_at_mentioned_owner_ids,hs_attachment_ids,hs_body_preview,hs_body_preview_html,hs_body_preview_is_truncated,hs_created_by,hs_created_by_user_id,hs_createdate,hs_engagement_source,hs_engagement_source_id,hs_follow_up_action,hs_gdpr_deleted,hs_hd_ticket_ids,hs_lastmodifieddate,hs_merged_object_ids,hs_modified_by,hs_note_body,hs_note_ms_teams_payload,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_product_name,hs_queue_membership_ids,hs_read_only,hs_shared_team_ids,hs_shared_user_ids,hs_timestamp,hs_unique_creation_key,hs_unique_id,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_was_imported,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,SourcesystemID'
WHERE TABLEID IN ('H7', 'H14')

-- COMMAND ----------

UPDATE Metadata.mastermetadata SET SourceSelectQuery = "hs_object_id,archived,archived_at,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_owner_ids,hs_all_team_ids,hs_at_mentioned_owner_ids,hs_attachment_ids,hs_body_preview,hs_body_preview_html,hs_body_preview_is_truncated,hs_created_by,hs_created_by_user_id,hs_createdate,hs_engagement_source,hs_engagement_source_id,hs_gdpr_deleted,hs_lastmodifieddate,hs_merged_object_ids,hs_modified_by,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_postal_mail_body,hs_postal_mail_type,hs_queue_membership_ids,hs_read_only,hs_shared_team_ids,hs_shared_user_ids,hs_timestamp,hs_unique_creation_key,hs_unique_id,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_was_imported,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,SourcesystemID"
WHERE TABLEID IN ('H20', 'H21')

-- COMMAND ----------

UPDATE metadata.mastermetadata SET SourceSelectQuery = 'hs_object_id,archived,archived_at,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_owner_ids,hs_all_team_ids,hs_associated_company_labels,hs_associated_contact_labels,hs_at_mentioned_owner_ids,hs_attachment_ids,hs_body_preview,hs_body_preview_html,hs_body_preview_is_truncated,hs_calendar_event_id,hs_created_by,hs_created_by_user_id,hs_createdate,hs_date_entered_60b5c368_04c4_4d32_9b4a_457e159f49b7_13292096,hs_date_entered_61bafb31_e7fa_46ed_aaa9_1322438d6e67_1866552342,hs_date_entered_af0e6a5c_2ea3_4c72_b69f_7c6cb3fdb591_1652950531,hs_date_entered_dd5826e4_c976_4654_a527_b59ada542e52_2144133616,hs_date_entered_fc8148fb_3a2d_4b59_834e_69b7859347cb_1813133675,hs_date_exited_60b5c368_04c4_4d32_9b4a_457e159f49b7_13292096,hs_date_exited_61bafb31_e7fa_46ed_aaa9_1322438d6e67_1866552342,hs_date_exited_af0e6a5c_2ea3_4c72_b69f_7c6cb3fdb591_1652950531,hs_date_exited_dd5826e4_c976_4654_a527_b59ada542e52_2144133616,hs_date_exited_fc8148fb_3a2d_4b59_834e_69b7859347cb_1813133675,hs_engagement_source,hs_engagement_source_id,hs_follow_up_action,hs_gdpr_deleted,hs_lastmodifieddate,hs_marketing_task_category,hs_marketing_task_category_id,hs_merged_object_ids,hs_modified_by,hs_msteams_message_id,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_pipeline,hs_pipeline_stage,hs_product_name,hs_queue_membership_ids,hs_read_only,hs_repeat_status,hs_scheduled_tasks,hs_shared_team_ids,hs_shared_user_ids,hs_task_body,hs_task_campaign_guid,hs_task_completion_count,hs_task_completion_date,hs_task_contact_timezone,hs_task_family,hs_task_for_object_type,hs_task_is_all_day,hs_task_is_completed,hs_task_is_completed_call,hs_task_is_completed_email,hs_task_is_completed_linked_in,hs_task_is_completed_sequence,hs_task_is_overdue,hs_task_is_past_due_date,hs_task_last_contact_outreach,hs_task_last_sales_activity_timestamp,hs_task_missed_due_date,hs_task_missed_due_date_count,hs_task_ms_teams_payload,hs_task_priority,hs_task_probability_to_complete,hs_task_relative_reminders,hs_task_reminders,hs_task_repeat_interval,hs_task_send_default_reminder,hs_task_sequence_enrollment_active,hs_task_sequence_step_enrollment_contact_id,hs_task_sequence_step_enrollment_id,hs_task_sequence_step_order,hs_task_status,hs_task_subject,hs_task_template_id,hs_task_type,hs_time_in_60b5c368_04c4_4d32_9b4a_457e159f49b7_13292096,hs_time_in_61bafb31_e7fa_46ed_aaa9_1322438d6e67_1866552342,hs_time_in_af0e6a5c_2ea3_4c72_b69f_7c6cb3fdb591_1652950531,hs_time_in_dd5826e4_c976_4654_a527_b59ada542e52_2144133616,hs_time_in_fc8148fb_3a2d_4b59_834e_69b7859347cb_1813133675,hs_timestamp,hs_unique_creation_key,hs_unique_id,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_was_imported,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,SourcesystemID'
WHERE TableID IN ('H22', 'H23')

-- COMMAND ----------

UPDATE metadata.mastermetadata SET SourceSelectQuery = 'hs_object_id,archived,archived_at,hs_all_accessible_team_ids,hs_all_assigned_business_unit_ids,hs_all_owner_ids,hs_all_team_ids,hs_app_id,hs_at_mentioned_owner_ids,hs_attachment_ids,hs_body_preview,hs_body_preview_html,hs_body_preview_is_truncated,hs_campaign_guid,hs_created_by,hs_created_by_user_id,hs_createdate,hs_direction_and_unique_id,hs_email_associated_contact_id,hs_email_attached_video_id,hs_email_attached_video_name,hs_email_attached_video_opened,hs_email_attached_video_watched,hs_email_bcc_email,hs_email_bcc_firstname,hs_email_bcc_lastname,hs_email_bcc_raw,hs_email_bounce_error_detail_message,hs_email_bounce_error_detail_status_code,hs_email_cc_email,hs_email_cc_firstname,hs_email_cc_lastname,hs_email_cc_raw,hs_email_click_count,hs_email_click_rate,hs_email_direction,hs_email_encoded_email_associations_request,hs_email_error_message,hs_email_facsimile_send_id,hs_email_from_email,hs_email_from_firstname,hs_email_from_lastname,hs_email_from_raw,hs_email_has_inline_images_stripped,hs_email_headers,hs_email_html,hs_email_logged_from,hs_email_media_processing_status,hs_email_member_of_forwarded_subthread,hs_email_message_id,hs_email_migrated_via_portal_data_migration,hs_email_ms_teams_payload,hs_email_open_count,hs_email_open_rate,hs_email_pending_inline_image_ids,hs_email_post_send_status,hs_email_recipient_drop_reasons,hs_email_reply_count,hs_email_reply_rate,hs_email_send_event_id,hs_email_send_event_id_created,hs_email_sender_email,hs_email_sender_firstname,hs_email_sender_lastname,hs_email_sender_raw,hs_email_sent_count,hs_email_sent_via,hs_email_status,hs_email_stripped_attachment_count,hs_email_subject,hs_email_text,hs_email_thread_id,hs_email_thread_summary,hs_email_to_email,hs_email_to_firstname,hs_email_to_lastname,hs_email_to_raw,hs_email_tracker_key,hs_email_validation_skipped,hs_engagement_source,hs_engagement_source_id,hs_follow_up_action,hs_gdpr_deleted,hs_incoming_email_is_out_of_office,hs_lastmodifieddate,hs_merged_object_ids,hs_modified_by,hs_not_tracking_opens_or_clicks,hs_object_source,hs_object_source_detail_1,hs_object_source_detail_2,hs_object_source_detail_3,hs_object_source_id,hs_object_source_label,hs_object_source_user_id,hs_owner_ids_bcc,hs_owner_ids_cc,hs_owner_ids_from,hs_owner_ids_to,hs_product_name,hs_queue_membership_ids,hs_read_only,hs_scs_association_status,hs_scs_audit_id,hs_sequence_id,hs_shared_team_ids,hs_shared_user_ids,hs_template_id,hs_ticket_create_date,hs_timestamp,hs_unique_creation_key,hs_unique_id,hs_updated_by_user_id,hs_user_ids_of_all_notification_followers,hs_user_ids_of_all_notification_unfollowers,hs_user_ids_of_all_owners,hs_was_imported,hubspot_owner_assigneddate,hubspot_owner_id,hubspot_team_id,SourcesystemID'
WHERE TableID IN ('H5', 'H12')