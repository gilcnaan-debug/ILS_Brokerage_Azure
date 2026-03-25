-- Databricks notebook source
-- MAGIC %python
-- MAGIC ## Mention the catalog used, getting the catalog name from key vault
-- MAGIC CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
-- MAGIC spark.sql(f"USE CATALOG {CatalogName}")

-- COMMAND ----------

-- MAGIC %md ##equip_mode_table

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.equip_mode_table (
  equip_mode_type STRING)


-- COMMAND ----------

-- MAGIC %md ##new_office_lookup

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.new_office_lookup (
  old_office VARCHAR(255),
  new_office VARCHAR(255))

-- COMMAND ----------

-- MAGIC %md ##canada_conversions

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.canada_conversions (
  ship_date DATE,
  conversion DECIMAL(10,5),
  us_to_cad DECIMAL(10,5))

-- COMMAND ----------

-- MAGIC %md ##office_to_cost_center_lookup

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.office_to_cost_center_lookup (
  cost_center VARCHAR(255),
  location VARCHAR(255),
  office_id VARCHAR(255))


-- COMMAND ----------

-- MAGIC %md ##aljex_user_report_listing

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.aljex_user_report_listing (
  aljex_id VARCHAR(255),
  company INT,
  pnl_code VARCHAR(255),
  sales_rep VARCHAR(255),
  full_name VARCHAR(255),
  scr_id VARCHAR(255),
  ultipro_name VARCHAR(255))

-- COMMAND ----------

-- MAGIC %md ##HubSpot_Notes

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.HubSpot_Notes (
  createdate TIMESTAMP,
  hs_lastmodifieddate TIMESTAMP,
  hs_note_body STRING,
  hs_object_id BIGINT NOT NULL,
  hs_timestamp TIMESTAMP,
  hubspot_owner_id DECIMAL(10,0),
  SourcesystemID INT,
  CONSTRAINT `notes_pkey` PRIMARY KEY (hs_object_id))

-- COMMAND ----------

-- MAGIC %md ##HubSpot_Meetings

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.HubSpot_Meetings (
  createdate TIMESTAMP,
  hs_internal_meeting_notes STRING,
  hs_lastmodifieddate TIMESTAMP,
  hs_meeting_body STRING,
  hs_meeting_end_time TIMESTAMP,
  hs_meeting_external_url STRING,
  hs_meeting_location STRING,
  hs_meeting_outcome STRING,
  hs_meeting_start_time TIMESTAMP,
  hs_meeting_title STRING,
  hs_object_id BIGINT NOT NULL,
  hs_timestamp TIMESTAMP,
  hubspot_owner_id DECIMAL(10,0),
  SourcesystemID INT,
  CONSTRAINT `meetings_pkey` PRIMARY KEY (hs_object_id))

-- COMMAND ----------

-- MAGIC %md ##HubSpot_Emails

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.HubSpot_Emails (
  hs_createdate TIMESTAMP,
  hs_email_direction STRING,
  hs_email_sender_email STRING,
  hs_email_sender_firstname STRING,
  hs_email_sender_lastname STRING,
  hs_email_status STRING,
  hs_email_subject STRING,
  hs_email_text STRING,
  hs_email_to_email STRING,
  hs_email_to_firstname STRING,
  hs_email_to_lastname STRING,
  hs_lastmodifieddate TIMESTAMP,
  hs_object_id BIGINT NOT NULL,
  hs_timestamp TIMESTAMP,
  hubspot_owner_id DECIMAL(10,0),
  SourcesystemID INT,
  CONSTRAINT `emails_pkey` PRIMARY KEY (hs_object_id))

-- COMMAND ----------

-- MAGIC %md ##HubSpot_Owners

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.HubSpot_Owners (
  id BIGINT NOT NULL,
  email STRING,
  first_name STRING,
  last_name STRING,
  updated_at TIMESTAMP,
  created_at TIMESTAMP,
  user_id BIGINT,
  SourcesystemID INT,
  CONSTRAINT `owners_pkey` PRIMARY KEY (id))


-- COMMAND ----------

-- MAGIC %md ##HubSpot_Contacts

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.HubSpot_Contacts (
  company STRING,
  createdate TIMESTAMP,
  email STRING,
  firstname STRING,
  hs_object_id BIGINT NOT NULL,
  industry STRING,
  lastmodifieddate TIMESTAMP,
  lastname STRING,
  phone STRING,
  state STRING,
  website STRING,
  SourcesystemID INT,
  CONSTRAINT `contacts_pkey` PRIMARY KEY (hs_object_id))

-- COMMAND ----------

-- MAGIC %md ##HubSpot_Companies

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.HubSpot_Companies (
  city STRING,
  createdate TIMESTAMP,
  domain STRING,
  hs_lastmodifieddate TIMESTAMP,
  hs_object_id BIGINT NOT NULL,
  industry STRING,
  name STRING,
  phone STRING,
  state STRING,
  SourcesystemID INT,
  CONSTRAINT `companies_pkey` PRIMARY KEY (hs_object_id))

-- COMMAND ----------

-- MAGIC %md ##HubSpot_Deals

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.HubSpot_Deals (
  amount STRING,
  closedate TIMESTAMP,
  createdate TIMESTAMP,
  dealname STRING,
  dealstage STRING,
  hs_lastmodifieddate TIMESTAMP,
  hs_object_id BIGINT NOT NULL,
  hubspot_owner_id BIGINT,
  pipeline STRING,
  SourcesystemID INT,
  CONSTRAINT `deals_pkey` PRIMARY KEY (hs_object_id))

-- COMMAND ----------

-- MAGIC %md ##aljex_invoice

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.aljex_invoice (
  invoice_date DATE,
  pro_number DECIMAL(10,0) NOT NULL,
  shipdate DATE,
  CONSTRAINT `aljex_invoice_exp_pkey` PRIMARY KEY (pro_number))

-- COMMAND ----------

-- MAGIC %md ##invoicing_invoice_charges

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.invoicing_invoice_charges (
  charge_id VARCHAR(255) NOT NULL,
  invoice_id VARCHAR(255),
  relay_reference_number INT,
  charge_code VARCHAR(255),
  charge_by INT,
  prepared_at TIMESTAMP,
  `voided?` BOOLEAN,
  `estimated?` BOOLEAN,
  amount INT,
  amount_float FLOAT,
  currency VARCHAR(255),
  CONSTRAINT `invoicing_invoice_charges_pkey` PRIMARY KEY (charge_id))

-- COMMAND ----------

-- MAGIC %md ##cai_data

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.cai_data (
  ship_date DATE,
  load_num INT NOT NULL,
  ops_office VARCHAR(255),
  customer VARCHAR(255),
  cust_city VARCHAR(255),
  cust_state VARCHAR(255),
  cust_zip VARCHAR(255),
  cust_ref_num VARCHAR(255),
  delivery_date DATE,
  total_picks INT,
  total_delvs INT,
  status VARCHAR(255),
  mode VARCHAR(255),
  shipper VARCHAR(255),
  shipper_city VARCHAR(255),
  shipper_state VARCHAR(255),
  shipper_zip VARCHAR(255),
  booked_by VARCHAR(255),
  salesperson VARCHAR(255),
  acct_mgr VARCHAR(255),
  miles DECIMAL(10,0),
  carrier VARCHAR(255),
  carr_city VARCHAR(255),
  carr_state VARCHAR(255),
  carr_zip VARCHAR(255),
  carr_mc VARCHAR(255),
  consignee VARCHAR(255),
  consignee_city VARCHAR(255),
  consignee_state VARCHAR(255),
  consignee_zip VARCHAR(255),
  total_expense DECIMAL(10,0),
  total_revenue DECIMAL(10,2),
  total_margin DECIMAL(10,2),
  created_date DATE,
  booked_date DATE,
  invoice_date DATE,
  pickup_appointment TIMESTAMP,
  delivery_appointment TIMESTAMP,
  CONSTRAINT `cai_data_pkey` PRIMARY KEY (load_num))

-- COMMAND ----------

-- MAGIC %md ##tendering_origin_destination

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tendering_origin_destination (
  relay_reference_number INT NOT NULL,
  tender_id VARCHAR(255),
  origin_address_1 VARCHAR(255),
  origin_address_2 VARCHAR(255),
  origin_administrative_region VARCHAR(255),
  origin_city VARCHAR(255),
  origin_country_code VARCHAR(255),
  shipper_id VARCHAR(255),
  shipper_name VARCHAR(255),
  destination_address_1 VARCHAR(255),
  destination_address_2 VARCHAR(255),
  destination_city VARCHAR(255),
  destination_administrative_region VARCHAR(255),
  destination_country_code VARCHAR(255),
  receiver_id VARCHAR(255),
  receiver_name VARCHAR(255),
  `cancelled?` BOOLEAN,
  CONSTRAINT `tendering_origin_destination_pkey` PRIMARY KEY (relay_reference_number))

-- COMMAND ----------

-- MAGIC %md ##invoicing_invoice_eligibility

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.invoicing_invoice_eligibility (
  invoice_id VARCHAR(255) NOT NULL,
  auto_invoicing_delay INT,
  `auto_invoicing_enabled?` BOOLEAN,
  `eligible?` BOOLEAN,
  CONSTRAINT `invoicing_invoice_eligibility_pkey` PRIMARY KEY (invoice_id))

-- COMMAND ----------

-- MAGIC %md ##invoicing_credits

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.invoicing_credits (
  event_id VARCHAR(255) NOT NULL,
  invoice_number VARCHAR(255),
  invoice_id VARCHAR(255),
  relay_reference_number INT,
  total_amount INT,
  currency VARCHAR(255),
  credited_at TIMESTAMP,
  reason STRING,
  issued_by_user_id INT,
  issued_by_name VARCHAR(255),
  taxes_amount INT,
  CONSTRAINT `invoicing_credits_pkey` PRIMARY KEY (event_id))

-- COMMAND ----------

-- MAGIC %md ##booking_carrier_planned

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.booking_carrier_planned (
  relay_reference_number INT NOT NULL,
  carrier_id VARCHAR(255),
  managed_load_id VARCHAR(255),
  styimed BOOLEAN,
  CONSTRAINT `booking_carrier_planned_pkey` PRIMARY KEY (relay_reference_number))


-- COMMAND ----------

-- MAGIC %md ##integration_hubtran_vendor_invoice_approved

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.integration_hubtran_vendor_invoice_approved (
  event_id VARCHAR(255) NOT NULL,
  booking_id INT,
  approved_by VARCHAR(255),
  event_at TIMESTAMP,
  invoice_date VARCHAR(255),
  invoice_number VARCHAR(255),
  proof_of_delivery_seen BOOLEAN,
  proof_of_delivery_url VARCHAR(255),
  total_approved_amount_to_pay INT,
  vendor_id VARCHAR(255),
  date_to_pay VARCHAR(255),
  notes STRING,
  currency VARCHAR(255),
  CONSTRAINT `integration_hubtran_vendor_invoice_approved_pkey` PRIMARY KEY (event_id))


-- COMMAND ----------

-- MAGIC %md ##tendering_manual_tender_entry_usage

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tendering_manual_tender_entry_usage (
  tender_draft_id VARCHAR(255) NOT NULL,
  draft_by_user_id INT,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  total_time_ms INT,
  scratch_pad_notes_used BOOLEAN,
  origin_type VARCHAR(255),
  CONSTRAINT `tendering_manual_tender_entry_usage_pkey` PRIMARY KEY (tender_draft_id))

-- COMMAND ----------

-- MAGIC %md ##planning_team_driver_marked

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.planning_team_driver_marked (
  relay_reference_number INT NOT NULL,
  plan_id VARCHAR(255),
  `team_driver?` BOOLEAN,
  CONSTRAINT `planning_team_driver_marked_pkey` PRIMARY KEY (relay_reference_number))

-- COMMAND ----------

-- MAGIC %md ##tracking_in_transit_reason_code

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tracking_in_transit_reason_code (
  relay_reference_number INT NOT NULL,
  reason_code VARCHAR(255),
  CONSTRAINT `tracking_in_transit_reason_code_pkey` PRIMARY KEY (`relay_reference_number`))

-- COMMAND ----------

-- MAGIC %md ##offer_negotiation_invalidated

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.offer_negotiation_invalidated (
  offer_id VARCHAR(255) NOT NULL,
  relay_reference_number INT,
  invalidated_by INT,
  at TIMESTAMP,
  reason VARCHAR(255),
  master_carrier_id VARCHAR(255),
  customer_slug VARCHAR(255),
  carrier_profile_id VARCHAR(255),
  CONSTRAINT `offer_negotiation_invalidated_pkey` PRIMARY KEY (offer_id))

-- COMMAND ----------

-- MAGIC %md ##ultipro_list

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.ultipro_list (
  employee_number VARCHAR(255),
  employee_name VARCHAR(255) NOT NULL,
  employment_status VARCHAR(255),
  job_code VARCHAR(255),
  job_title VARCHAR(255),
  company_name VARCHAR(255),
  business_name VARCHAR(255),
  division_name VARCHAR(255),
  region_name VARCHAR(255),
  dept_code VARCHAR(255),
  department_name VARCHAR(255),
  location_code VARCHAR(255),
  location_name VARCHAR(255),
  location_site VARCHAR(255),
  employee_type VARCHAR(255),
  full_or_part_time VARCHAR(255),
  last_hire_date DATE,
  original_hire_date DATE,
  benefits_senority_date DATE,
  senority_date DATE,
  senority_years DECIMAL(10,2),
  date_in_job DATE,
  email VARCHAR(255),
  CONSTRAINT `ultipro_list_pkey` PRIMARY KEY (`employee_name`))

-- COMMAND ----------

-- MAGIC %md ##ultipro_terms

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.ultipro_terms (
  date_processed DATE,
  employee_num VARCHAR(255),
  employee_name VARCHAR(255),
  email_address VARCHAR(255),
  last_hire_date DATE,
  term_date DATE,
  job_code VARCHAR(255),
  job_title VARCHAR(255),
  division VARCHAR(255),
  dept_code VARCHAR(255),
  department VARCHAR(255),
  work_location_code VARCHAR(255),
  work_location VARCHAR(255),
  employee_type VARCHAR(255),
  full_or_part_time VARCHAR(255),
  supervisor VARCHAR(255))

-- COMMAND ----------

-- MAGIC %md ##cai_equipment

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.cai_equipment (
  equip_type_id INT,
  mode VARCHAR(255),
  sub_mode VARCHAR(255),
  abbreviation VARCHAR(255),
  description VARCHAR(255))

-- COMMAND ----------

-- MAGIC %md ##edge_mode

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.edge_mode (
  mode VARCHAR(255),
  sub_mode VARCHAR(255),
  abbrev VARCHAR(255))

-- COMMAND ----------

-- MAGIC %md ##cai_salesperson_lookup

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.cai_salesperson_lookup (
  full_name VARCHAR(255),
  salesperson_id DECIMAL(10,0))

-- COMMAND ----------

-- MAGIC %md ##cai_mode_lookup

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.cai_mode_lookup (
  original_mode VARCHAR(255),
  new_mode VARCHAR(255))

-- COMMAND ----------

-- MAGIC %md ##target_audit_lookup

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.target_audit_lookup (
  relay_reference_number VARCHAR(255),
  reconciled_date DATE)

-- COMMAND ----------

-- MAGIC %md ##distance_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.distance_projection (
  tender_id VARCHAR(255) NOT NULL,
  distance_1_to_2 DOUBLE,
  distance_2_to_3 DOUBLE,
  relay_reference_number INT,
  total_distance DOUBLE,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `distance_projection_pkey` PRIMARY KEY (tender_id))

-- COMMAND ----------

-- MAGIC %md ##slack_actions_2

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.slack_actions_2 (
  action_id STRING NOT NULL,
  action_type VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  channel_name VARCHAR(255) NOT NULL,
  channel_type VARCHAR(255) NOT NULL,
  datetime TIMESTAMP_NTZ NOT NULL,
  UpdatedDateTime DATE,
  CONSTRAINT `slack_actions_2_pkey` PRIMARY KEY (action_id))

-- COMMAND ----------

-- MAGIC %md ##projection_invoicing_audit

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.projection_invoicing_audit (
  event_number INT NOT NULL,
  description16 VARCHAR(255),
  office VARCHAR(255),
  web_sync_action VARCHAR(255),
  pro_num VARCHAR(255),
  cont_trailer_num VARCHAR(255),
  amount10 VARCHAR(255),
  amount3 VARCHAR(255),
  total VARCHAR(255),
  amount16 VARCHAR(255),
  amount4 VARCHAR(255),
  ship_date VARCHAR(255),
  amount13 VARCHAR(255),
  act_expense VARCHAR(255),
  description14 VARCHAR(255),
  type VARCHAR(255),
  billto_name VARCHAR(255),
  description15 VARCHAR(255),
  amount9 VARCHAR(255),
  amount7 VARCHAR(255),
  description3 VARCHAR(255),
  web_sync_table_name VARCHAR(255),
  description9 VARCHAR(255),
  description20 VARCHAR(255),
  description5 VARCHAR(255),
  amount20 VARCHAR(255),
  description4 VARCHAR(255),
  amount15 VARCHAR(255),
  description6 VARCHAR(255),
  credits_amount VARCHAR(255),
  description8 VARCHAR(255),
  amount8 VARCHAR(255),
  description18 VARCHAR(255),
  hold VARCHAR(255),
  description11 VARCHAR(255),
  act_profit VARCHAR(255),
  amount18 VARCHAR(255),
  amount21 VARCHAR(255),
  customer_name VARCHAR(255),
  description21 VARCHAR(255),
  proj_rev VARCHAR(255),
  description12 VARCHAR(255),
  act_ratio VARCHAR(255),
  description10 VARCHAR(255),
  description2 VARCHAR(255),
  description19 VARCHAR(255),
  weight VARCHAR(255),
  advances_amount VARCHAR(255),
  inv_date VARCHAR(255),
  del_date VARCHAR(255),
  proj_ratio VARCHAR(255),
  description17 VARCHAR(255),
  amount17 VARCHAR(255),
  posted_date VARCHAR(255),
  description1 VARCHAR(255),
  proj_profit VARCHAR(255),
  proj_expense VARCHAR(255),
  description13 VARCHAR(255),
  bal_due_rev VARCHAR(255),
  description7 VARCHAR(255),
  debits_amount VARCHAR(255),
  amount14 VARCHAR(255),
  act_rev VARCHAR(255),
  bal_due_exp VARCHAR(255),
  amount6 VARCHAR(255),
  amount1 VARCHAR(255),
  amount12 VARCHAR(255),
  amount11 VARCHAR(255),
  amount5 VARCHAR(255),
  amount19 VARCHAR(255),
  ref_num VARCHAR(255),
  amount2 VARCHAR(255),
  salesrep VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `projection_invoicing_audit_pkey` PRIMARY KEY (event_number))

-- COMMAND ----------

-- MAGIC %md ##projection_invoicing

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.projection_invoicing (
  pro_num VARCHAR(255) NOT NULL,
  description4 VARCHAR(255),
  amount10 VARCHAR(255),
  debits_amount VARCHAR(255),
  total VARCHAR(255),
  description19 VARCHAR(255),
  amount3 VARCHAR(255),
  description18 VARCHAR(255),
  amount14 VARCHAR(255),
  hold VARCHAR(255),
  amount16 VARCHAR(255),
  amount1 VARCHAR(255),
  type VARCHAR(255),
  amount7 VARCHAR(255),
  description10 VARCHAR(255),
  weight VARCHAR(255),
  description16 VARCHAR(255),
  amount19 VARCHAR(255),
  description12 VARCHAR(255),
  office VARCHAR(255),
  web_sync_action VARCHAR(255),
  description21 VARCHAR(255),
  amount21 VARCHAR(255),
  credits_amount VARCHAR(255),
  customer_name VARCHAR(255),
  description7 VARCHAR(255),
  act_profit VARCHAR(255),
  posted_date VARCHAR(255),
  web_sync_table_name VARCHAR(255),
  amount20 VARCHAR(255),
  proj_expense VARCHAR(255),
  amount12 VARCHAR(255),
  cont_trailer_num VARCHAR(255),
  proj_ratio VARCHAR(255),
  amount2 VARCHAR(255),
  bal_due_rev VARCHAR(255),
  description2 VARCHAR(255),
  amount11 VARCHAR(255),
  bal_due_exp VARCHAR(255),
  description17 VARCHAR(255),
  amount8 VARCHAR(255),
  amount9 VARCHAR(255),
  ship_date VARCHAR(255),
  description6 VARCHAR(255),
  amount4 VARCHAR(255),
  description5 VARCHAR(255),
  description9 VARCHAR(255),
  amount15 VARCHAR(255),
  inv_date VARCHAR(255),
  amount5 VARCHAR(255),
  advances_amount VARCHAR(255),
  del_date VARCHAR(255),
  amount17 VARCHAR(255),
  act_ratio VARCHAR(255),
  description11 VARCHAR(255),
  act_expense VARCHAR(255),
  description20 VARCHAR(255),
  description13 VARCHAR(255),
  billto_name VARCHAR(255),
  amount18 VARCHAR(255),
  description3 VARCHAR(255),
  amount13 VARCHAR(255),
  description15 VARCHAR(255),
  proj_rev VARCHAR(255),
  amount6 VARCHAR(255),
  ref_num VARCHAR(255),
  description1 VARCHAR(255),
  description14 VARCHAR(255),
  salesrep VARCHAR(255),
  proj_profit VARCHAR(255),
  description8 VARCHAR(255),
  act_rev VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `projection_invoicing_pkey` PRIMARY KEY (pro_num))

-- COMMAND ----------

-- MAGIC %md ##carrier_money_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.carrier_money_projection (
  booking_id INT NOT NULL,
  is_tonu BOOLEAN,
  relay_reference_number INT,
  status VARCHAR(255),
  total_carrier_rate_amount INT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `carrier_money_projection_pkey` PRIMARY KEY (booking_id))

-- COMMAND ----------

-- MAGIC %md ##customer_money

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.customer_money (
  relay_reference_number INT NOT NULL,
  fuel_surcharge_amount INT,
  linehaul_amount INT,
  accessorial_amount INT,
  customer VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `customer_money_pkey` PRIMARY KEY (relay_reference_number))

-- COMMAND ----------

-- MAGIC %md ##carrier_nomination_and_onboarding_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.carrier_nomination_and_onboarding_projection (
  dot_number VARCHAR(255) NOT NULL,
  approval STRING,
  `approved?` BOOLEAN,
  current_status VARCHAR(255),
  denial VARCHAR(2147483647),
  `denied?` BOOLEAN,
  `duplicated?` BOOLEAN,
  initial_nomination VARCHAR(2147483647),
  master_carrier_id VARCHAR(255),
  name VARCHAR(255),
  nomination_count INT,
  `onboard_prohibited?` BOOLEAN,
  onboard_prohibition VARCHAR(2147483647),
  `onboarded?` BOOLEAN,
  onboarding VARCHAR(2147483647),
  carrier_profile_id VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `carrier_nomination_and_onboarding_projection_pkey` PRIMARY KEY (dot_number))

-- COMMAND ----------

-- MAGIC %md ##tracking_activity_v2

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tracking_activity_v2 (
  event_id VARCHAR(255) NOT NULL,
  at TIMESTAMP_NTZ,
  by_system_id VARCHAR(255),
  by_type VARCHAR(255),
  by_user_id INT,
  truck_load_thing_id VARCHAR(255),
  type VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `tracking_activity_v2_pkey` PRIMARY KEY (event_id))

-- COMMAND ----------

-- MAGIC %md ##moneying_billing_party_transaction

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.moneying_billing_party_transaction (
  charge_id VARCHAR(255) NOT NULL,
  amount INT,
  billing_party_id VARCHAR(255),
  billing_party_transaction_id VARCHAR(255),
  charge_code VARCHAR(255),
  currency VARCHAR(255),
  `estimated?` BOOLEAN,
  incurred_at TIMESTAMP_NTZ,
  incurred_by INT,
  `invoiceable?` BOOLEAN,
  `invoiced?` BOOLEAN,
  invoiced_at TIMESTAMP_NTZ,
  invoiced_by INT,
  relay_reference_number INT,
  `voided?` BOOLEAN,
  voided_at TIMESTAMP_NTZ,
  voided_by INT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `moneying_billing_party_transaction_pkey` PRIMARY KEY (charge_id))

-- COMMAND ----------

-- MAGIC %md ##shippers

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.shippers (
  uuid VARCHAR(255) NOT NULL,
  address1 VARCHAR(255),
  address2 VARCHAR(255),
  city VARCHAR(255),
  latitude DOUBLE,
  longitude DOUBLE,
  name VARCHAR(255),
  phone_number VARCHAR(255),
  state_code VARCHAR(255),
  time_zone VARCHAR(255),
  utc_offset INT,
  `validated?` BOOLEAN,
  zip_code VARCHAR(255),
  created_at TIMESTAMP_NTZ,
  inserted_at TIMESTAMP_NTZ,
  updated_at TIMESTAMP_NTZ,
  CONSTRAINT `shippers_pkey` PRIMARY KEY (`uuid`))

-- COMMAND ----------

-- MAGIC %md ##aljex_mode_types

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.aljex_mode_types (
  equipment_type VARCHAR(2147483647),
  equipment_mode VARCHAR(2147483647))

-- COMMAND ----------

-- MAGIC %md ##customers

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.customers (
  id STRING,
  zip STRING,
  web_sync_table_name STRING,
  web_sync_action STRING,
  type STRING,
  total_exposure STRING,
  status STRING,
  state STRING,
  sales_rep STRING,
  revenue_type STRING,
  phone3 STRING,
  phone2 STRING,
  phone1 STRING,
  passwd STRING,
  old_system_id STRING,
  name STRING,
  fax STRING,
  email4 STRING,
  email3 STRING,
  email2 STRING,
  email1 STRING,
  credit_limit STRING,
  contact4 STRING,
  contact3 STRING,
  contact2 STRING,
  contact1 STRING,
  city STRING,
  address2 STRING,
  address1 STRING)

-- COMMAND ----------

-- MAGIC %md ##aljex_customer_profiles

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.aljex_customer_profiles (
  cust_id VARCHAR(2147483647),
  cust_name VARCHAR(2147483647),
  sales_rep VARCHAR(2147483647),
  state VARCHAR(2147483647),
  status VARCHAR(2147483647),
  cust_country VARCHAR(2147483647),
  address_one VARCHAR(2147483647),
  address_two VARCHAR(2147483647),
  address_city VARCHAR(2147483647),
  address_zip VARCHAR(2147483647))

-- COMMAND ----------

-- MAGIC %md ##aljex_dot_lawson_ref

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.aljex_dot_lawson_ref (
  aljex_carrier_id BIGINT,
  dot_number BIGINT,
  lawson_id VARCHAR(2147483647))

-- COMMAND ----------

-- MAGIC %md ##aljex_cred_debt

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.aljex_cred_debt (
  office VARCHAR(2147483647),
  pro_num BIGINT,
  customer VARCHAR(2147483647),
  type_of_ship VARCHAR(2147483647),
  ship_date DATE,
  revenue DECIMAL(10,2),
  expense DECIMAL(10,2))

-- COMMAND ----------

-- MAGIC %md ##event_name_status

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.event_name_status (
  truckload_p_status VARCHAR(255),
  shortcut_status VARCHAR(255))

-- COMMAND ----------

-- MAGIC %md ##customer_profile_external_ids

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.customer_profile_external_ids (
  customer_slug VARCHAR(255) NOT NULL,
  aljex_id VARCHAR(255),
  lawson_id VARCHAR(255),
  been_discarded BOOLEAN,
  external_ids VARCHAR(300),
  has_unmapped_external_ids BOOLEAN,
  CONSTRAINT `customer_profile_external_ids_pkey` PRIMARY KEY (`customer_slug`))

-- COMMAND ----------

-- MAGIC %md ##customer_profile_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.customer_profile_projection (
  customer_slug VARCHAR(255) NOT NULL,
  customer_name VARCHAR(255),
  bill_to_name VARCHAR(255),
  bill_to_email VARCHAR(255),
  billing_address_1 VARCHAR(255),
  billing_address_2 VARCHAR(255),
  billing_address_city VARCHAR(255),
  billing_address_state VARCHAR(255),
  billing_address_zip VARCHAR(255),
  contact_name VARCHAR(255),
  contact_email VARCHAR(255),
  contact_phone VARCHAR(255),
  credit_amount_cents BIGINT,
  credit_amount_currency VARCHAR(255),
  detention_hour_interval_to_start INT,
  detention_cost_per_hour_amount_cents INT,
  detention_cost_per_hour_currency VARCHAR(255),
  invoicing_method VARCHAR(255),
  `is_a_4pl?` BOOLEAN,
  postal_address_1 VARCHAR(255),
  postal_address_2 VARCHAR(255),
  postal_address_city VARCHAR(255),
  postal_address_state VARCHAR(255),
  postal_address_zip VARCHAR(255),
  profit_center VARCHAR(255),
  published_at TIMESTAMP,
  published_by INT,
  sales_relay_user_id INT,
  sales_relay_user_email VARCHAR(255),
  primary_relay_user_id INT,
  primary_relay_user_email VARCHAR(255),
  first_published_at TIMESTAMP,
  most_recent_published_at TIMESTAMP,
  status VARCHAR(255),
  rate_con_text STRING,
  notes STRING,
  lawson_company_id VARCHAR(255),
  aljex_id VARCHAR(255),
  lawson_id VARCHAR(255),
  external_ids VARCHAR(255),
  has_unmapped_external_ids BOOLEAN,
  invoicing_customer_profile_id VARCHAR(255),
  current_payment_terms INT,
  first_published_with_relay_type_lawson_id TIMESTAMP,
  CONSTRAINT `customer_profile_projection_pkey` PRIMARY KEY (`customer_slug`))

-- COMMAND ----------

-- MAGIC %md ##relay_users

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.relay_users (
  user_id INT NOT NULL,
  `active?` BOOLEAN,
  email_address VARCHAR(255),
  first_name VARCHAR(255),
  full_name VARCHAR(255),
  last_name VARCHAR(255),
  office_id VARCHAR(255),
  CONSTRAINT `relay_users_pkey` PRIMARY KEY (`user_id`))

-- COMMAND ----------

-- MAGIC %md ##invoicing_customer_profile

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.invoicing_customer_profile (
  customer_profile_id VARCHAR(255) NOT NULL,
  at TIMESTAMP,
  auto_invoice_delay INT,
  `auto_invoice_enabled?` BOOLEAN,
  by INT,
  invoicing_method VARCHAR(255),
  `show_intermediary_stops?` BOOLEAN,
  `stop_tracking_requirement?` BOOLEAN,
  document_requirements ARRAY<VARCHAR(255)>,
  data_requirements ARRAY<VARCHAR(255)>,
  CONSTRAINT `invoicing_customer_profile_pkey` PRIMARY KEY (`customer_profile_id`))

-- COMMAND ----------

-- MAGIC %md ##plan_combination_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.plan_combination_projection (
  plan_combination_id VARCHAR(255) NOT NULL,
  combined_plan_one_id VARCHAR(255),
  combined_plan_two_id VARCHAR(255),
  is_combined BOOLEAN,
  relay_reference_number_one INT,
  relay_reference_number_two INT,
  resulting_plan_id VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `plan_combination_projection_pkey` PRIMARY KEY (`plan_combination_id`))

-- COMMAND ----------

-- MAGIC %md ##receivers

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.receivers (
  uuid VARCHAR(255) NOT NULL,
  address1 VARCHAR(255),
  address2 VARCHAR(255),
  city VARCHAR(255),
  latitude DOUBLE,
  longitude DOUBLE,
  name VARCHAR(255),
  phone_number VARCHAR(255),
  state_code VARCHAR(255),
  time_zone VARCHAR(255),
  utc_offset INT,
  `validated?` BOOLEAN,
  zip_code VARCHAR(255),
  created_at TIMESTAMP_NTZ,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `receivers_pkey` PRIMARY KEY (`uuid`))

-- COMMAND ----------

-- MAGIC %md ##ltl_invoice_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.ltl_invoice_projection (
  event_number INT NOT NULL,
  invoice_number VARCHAR(255),
  invoiced_amount VARCHAR(255),
  invoiced_at VARCHAR(255),
  relay_reference_number INT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `ltl_invoice_projection_pkey` PRIMARY KEY (`event_number`))

-- COMMAND ----------

-- MAGIC %md ##projection_carrier

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.projection_carrier (
  id VARCHAR(255) NOT NULL,
  liab_ins_exp VARCHAR(255),
  comments_1 VARCHAR(255),
  pay_to_city VARCHAR(255),
  legal_name VARCHAR(255),
  carrier_yn VARCHAR(255),
  comments_2 VARCHAR(255),
  pay_to_phone VARCHAR(255),
  pay_to_contact VARCHAR(255),
  dot_num VARCHAR(255),
  power_units VARCHAR(255),
  liab_insurer VARCHAR(255),
  cargo_insurer VARCHAR(255),
  smartway VARCHAR(255),
  cargo_ins_exp VARCHAR(255),
  work_comp_amount VARCHAR(255),
  zip VARCHAR(255),
  email2 VARCHAR(255),
  cargo_ins_policy VARCHAR(255),
  num_vans VARCHAR(255),
  num_reefs VARCHAR(255),
  passwd VARCHAR(255),
  cargo_ins_amount VARCHAR(255),
  city VARCHAR(255),
  liab_ins_policy VARCHAR(255),
  created_date VARCHAR(255),
  pay_to_zip VARCHAR(255),
  pay_to_zip_4 VARCHAR(255),
  liab_ins_amount VARCHAR(255),
  work_comp_insurer VARCHAR(255),
  gen_liab_insurer VARCHAR(255),
  address1 VARCHAR(255),
  address2 VARCHAR(255),
  name VARCHAR(255),
  state VARCHAR(255),
  num_flats VARCHAR(255),
  email1 VARCHAR(255),
  status VARCHAR(255),
  old_system_id VARCHAR(255),
  gen_liab_policy VARCHAR(255),
  gen_liab_exp VARCHAR(255),
  pay_to_name VARCHAR(255),
  pay_to_state VARCHAR(255),
  pay_to_address_1 VARCHAR(255),
  pay_to_address_2 VARCHAR(255),
  scac VARCHAR(255),
  carrier_type VARCHAR(255),
  pay_to_account VARCHAR(255),
  assigned_dispatcher VARCHAR(255),
  work_comp_deduct VARCHAR(255),
  cargo_ins_deduct VARCHAR(255),
  bulk_email VARCHAR(255),
  mc_num VARCHAR(255),
  work_comp_exp VARCHAR(255),
  gen_liab_amount VARCHAR(255),
  work_comp_policy VARCHAR(255),
  created_by VARCHAR(255),
  fax VARCHAR(255),
  phone VARCHAR(255),
  username VARCHAR(255),
  gen_liab_deduct VARCHAR(255),
  liab_ins_deduct VARCHAR(255),
  web_sync_action VARCHAR(255),
  web_sync_table_name VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `projection_carrier_pkey` PRIMARY KEY (`id`))

-- COMMAND ----------

-- MAGIC %md ##offer_negotiation_reflected

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.offer_negotiation_reflected (
  offer_id VARCHAR(255) NOT NULL,
  authority VARCHAR(255),
  authority_id VARCHAR(255),
  carrier_name VARCHAR(255),
  customer VARCHAR(255),
  master_carrier_id VARCHAR(255),
  notes VARCHAR(2147483647),
  offered_by INT,
  offered_by_name VARCHAR(255),
  offered_system VARCHAR(255),
  offered_at TIMESTAMP,
  relay_reference_number INT,
  total_rate_currency VARCHAR(255),
  total_rate_in_pennies INT,
  carrier_profile_id VARCHAR(255),
  inserted_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT `offer_negotiation_reflected_pkey` PRIMARY KEY (`offer_id`))

-- COMMAND ----------

-- MAGIC %md ##carrier_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.carrier_projection (
  carrier_id VARCHAR(255) NOT NULL,
  approved_at VARCHAR(255),
  approved_by INT,
  carrier_name VARCHAR(255),
  current_cargo_insurance_amount INT,
  denied_at VARCHAR(255),
  denied_by INT,
  dot_number VARCHAR(255),
  duplicated_reason VARCHAR(255),
  is_overridden BOOLEAN,
  master_carrier_id VARCHAR(255),
  nominated_at VARCHAR(255),
  nominated_by INT,
  nominated_email_address VARCHAR(255),
  onboarded_at VARCHAR(255),
  overridden_cargo_insurance_amount INT,
  overridden_cargo_insurance_currency VARCHAR(255),
  prohibited_at VARCHAR(255),
  prohibited_by INT,
  prohibited_reason VARCHAR(255),
  reported_cargo_insurance_amount INT,
  reported_cargo_insurance_currency VARCHAR(255),
  status VARCHAR(255),
  cargo_expiration_date VARCHAR(255),
  default_rate_con_recipients VARCHAR(255),
  carrier_profile_id VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `carrier_projection_pkey` PRIMARY KEY (`carrier_id`))

-- COMMAND ----------

-- MAGIC %md ##planning_note_captured

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.planning_note_captured (
  plan_id VARCHAR(255) NOT NULL,
  internal_note STRING,
  note_id VARCHAR(255),
  planning_note VARCHAR(2147483647),
  relay_reference_number INT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `planning_note_captured_pkey` PRIMARY KEY (`plan_id`))

-- COMMAND ----------

-- MAGIC %md ##planning_current_assignment

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.sourcing_max_buy_v2 (
  relay_reference_number INT NOT NULL,
  currency VARCHAR(255),
  max_buy INT,
  notes VARCHAR(2147483647),
  pricing_id VARCHAR(255),
  set_at TIMESTAMP_NTZ,
  set_by INT,
  set_by_name VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `sourcing_max_buy_v2_pkey` PRIMARY KEY (`relay_reference_number`))

-- COMMAND ----------

-- MAGIC %md ##planning_current_assignment

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.planning_current_assignment (
  assignment_id VARCHAR(255) NOT NULL,
  assigned_at TIMESTAMP_NTZ,
  assigned_by INT,
  assigned_to INT,
  is_open BOOLEAN,
  plan_id VARCHAR(255),
  relay_reference_number INT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `planning_current_assignment_pkey` PRIMARY KEY (`assignment_id`))

-- COMMAND ----------

-- MAGIC %md ##carrier_load_money_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.carrier_load_money_projection (
  charge_id VARCHAR(255) NOT NULL,
  carrier_vendor_id VARCHAR(255),
  charge_code INT,
  initiated_by INT,
  initiated_from VARCHAR(255),
  is_locked BOOLEAN,
  is_voided BOOLEAN,
  load_number INT,
  locked_by INT,
  locked_from VARCHAR(255),
  total_amount_amount INT,
  total_amount_currency VARCHAR(255),
  voided_by INT,
  voided_from VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `carrier_load_money_projection_pkey` PRIMARY KEY (`charge_id`))

-- COMMAND ----------

-- MAGIC %md ##delivery_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.delivery_projection (
  delivery_id VARCHAR(255) NOT NULL,
  scheduled_from VARCHAR(255),
  location_state VARCHAR(255),
  weight_to_deliver_amount DOUBLE,
  pieces_to_deliver_packaging_type VARCHAR(255),
  receiver_name VARCHAR(255),
  location_name VARCHAR(255),
  is_drop_trailer BOOLEAN,
  plan_id VARCHAR(255),
  location_phone_number VARCHAR(255),
  location_city VARCHAR(255),
  weight_to_deliver_unit VARCHAR(255),
  receiver_id VARCHAR(255),
  appointment_time_local VARCHAR(255),
  location_country_code VARCHAR(255),
  pieces_to_deliver_count DOUBLE,
  shipping_units_to_deliver_count INT,
  volume_to_deliver_unit VARCHAR(255),
  delivery_numbers STRING,
  relay_reference_number INT,
  appointment_reference VARCHAR(2147483647),
  location_address_2 VARCHAR(255),
  scheduled_at VARCHAR(255),
  volume_to_deliver_amount DOUBLE,
  sequence_number INT,
  location_address_1 VARCHAR(255),
  appointment_date VARCHAR(255),
  shipping_units_to_deliver_type VARCHAR(255),
  is_appointment_scheduled BOOLEAN,
  location_type VARCHAR(255),
  location_postal_code VARCHAR(255),
  location_external_id VARCHAR(255),
  scheduled_by INT,
  scheduled_booking_id INT,
  action_needed_booking_id INT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `delivery_projection_pkey` PRIMARY KEY (`delivery_id`))

-- COMMAND ----------

-- MAGIC %md ##pickup_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.pickup_projection (
  schedule_id VARCHAR(255) NOT NULL,
  location_country_code VARCHAR(255),
  requested_appointment_time VARCHAR(255),
  scheduled_by INT,
  ready_date VARCHAR(255),
  shipper_name VARCHAR(255),
  appointment_datetime VARCHAR(255),
  weight_to_pickup_amount DOUBLE,
  appointment_reference VARCHAR(2147483647),
  volume_to_pickup_unit VARCHAR(255),
  location_state VARCHAR(255),
  location_postal_code VARCHAR(255),
  location_type VARCHAR(255),
  sequence_number INT,
  volume_to_pickup_amount DOUBLE,
  weight_to_pickup_unit VARCHAR(255),
  shipment_id VARCHAR(255),
  shipping_units_to_pickup_count INT,
  ready_time VARCHAR(255),
  shipping_units_to_pickup_type VARCHAR(255),
  pieces_to_pickup_count DOUBLE,
  shipper_id VARCHAR(255),
  pickup_numbers STRING,
  scheduled_from VARCHAR(255),
  location_phone_number VARCHAR(255),
  pieces_to_pickup_packaging_type VARCHAR(255),
  location_city VARCHAR(255),
  plan_id VARCHAR(255),
  location_address_2 VARCHAR(255),
  relay_reference_number INT,
  location_address_1 VARCHAR(255),
  scheduled_at VARCHAR(255),
  location_external_id VARCHAR(255),
  is_appointment_scheduled BOOLEAN,
  location_name VARCHAR(255),
  booking_id INT,
  scheduling_action_booking_id INT,
  order_numbers STRING,
  inserted_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT `pickup_projection_pkey` PRIMARY KEY (`schedule_id`))

-- COMMAND ----------

-- MAGIC %md ##tracking_last_reason_code

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tracking_last_reason_code (
  relay_reference_number INT NOT NULL,
  reason_code VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `tracking_last_reason_code_pkey` PRIMARY KEY (`relay_reference_number`))

-- COMMAND ----------

-- MAGIC %md ##tl_invoice_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tl_invoice_projection (
  event_number INT NOT NULL,
  accessorial_amount INT,
  customer VARCHAR(255),
  fuel_surcharge_amount INT,
  invoice_number VARCHAR(255),
  invoiced_at VARCHAR(255),
  linehaul_amount INT,
  relay_reference_number INT,
  do_not_invoice BOOLEAN,
  prepared_by INT,
  taxes_amount INT,
  currency VARCHAR(255),
  approval_number VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `tl_invoice_projection_pkey` PRIMARY KEY (`event_number`))

-- COMMAND ----------

-- MAGIC %md ##tendering_tender_draft_id

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tendering_tender_draft_id (
  tender_draft_id VARCHAR(255) NOT NULL,
  relay_reference_number INT,
  CONSTRAINT `tendering_tender_draft_id_pkey` PRIMARY KEY (`tender_draft_id`))

-- COMMAND ----------

-- MAGIC %md ##ap_lawson_appt

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.ap_lawson_appt (
  company INT,
  due_date DATE,
  invoice_number VARCHAR(255),
  lawson_id VARCHAR(255),
  payment_amount DECIMAL(10,2),
  payment_date DATE,
  payment_num INT,
  po_number VARCHAR(255),
  record_status INT)

-- COMMAND ----------

-- MAGIC %md ##tendering_service_line

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tendering_service_line (
  relay_reference_number INT NOT NULL,
  tender_id VARCHAR(255),
  service_line VARCHAR(300),
  service_line_type VARCHAR(255),
  equipment_type VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `tendering_service_line_pkey` PRIMARY KEY (`relay_reference_number`))

-- COMMAND ----------

-- MAGIC %md ##tendering_planned_distance

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tendering_planned_distance (
  tender_id VARCHAR(255) NOT NULL,
  planned_distance_amount DOUBLE,
  planned_distance_unit VARCHAR(255),
  relay_reference_number INT,
  shipment_id VARCHAR(2147483647),
  tender_on_behalf_of_id VARCHAR(255),
  tender_on_behalf_of_type VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `tendering_planned_distance_pkey` PRIMARY KEY (`tender_id`))

-- COMMAND ----------

-- MAGIC %md ##truckload_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.truckload_projection (
  truck_load_thing_id VARCHAR(255) NOT NULL,
  booking_id INT,
  last_update_by INT,
  last_update_date_time VARCHAR(255),
  last_update_event_name VARCHAR(255),
  order_numbers VARCHAR(2147483647),
  relay_reference_number INT,
  shipment_ids STRING,
  status VARCHAR(255),
  truck_number VARCHAR(255),
  trailer_number VARCHAR(255),
  eta_to_shipper VARCHAR(255),
  tender_on_behalf_of_id VARCHAR(255),
  tender_on_behalf_of_type VARCHAR(255),
  driver_phone_number VARCHAR(255),
  tracking_contact_name VARCHAR(255),
  tracking_contact_phone_number VARCHAR(255),
  tracking_contact_email VARCHAR(255),
  last_update_location VARCHAR(255),
  last_update_location_time VARCHAR(255),
  last_update_note STRING,
  driver_name VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `truckload_projection_pkey` PRIMARY KEY (`truck_load_thing_id`))

-- COMMAND ----------

-- MAGIC %md ##canonical_plan_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.canonical_plan_projection (
  plan_id VARCHAR(255) NOT NULL,
  relay_reference_number INT,
  status VARCHAR(255),
  tender_id VARCHAR(255),
  mode VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `canonical_plan_projection_pkey` PRIMARY KEY (`plan_id`))

-- COMMAND ----------

-- MAGIC %md ##rolled_loads

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.rolled_loads (
  plan_id VARCHAR(255) NOT NULL,
  new_pickup_date DATE,
  reason_code VARCHAR(255),
  relay_reference_number INT,
  rolled_by INT,
  shipment_id VARCHAR(255),
  tender_pickup_date DATE,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `rolled_loads_pkey` PRIMARY KEY (`plan_id`))

-- COMMAND ----------

-- MAGIC %md ##hain_tracking_accuracy_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.hain_tracking_accuracy_projection (
  relay_reference_number INT NOT NULL,
  customer VARCHAR(255),
  expected_delivery_date VARCHAR(255),
  expected_ship_date VARCHAR(255),
  rolled_count INT,
  delivery_appointment VARCHAR(255),
  delivery_appointment_when_delivered VARCHAR(255),
  delivery_by_date_at_tender VARCHAR(255),
  ready_date_at_tender VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `hain_tracking_accuracy_projection_pkey` PRIMARY KEY (`relay_reference_number`))

-- COMMAND ----------

-- MAGIC %md ##booking_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.booking_projection (
  booking_id INT NOT NULL,
  booked_carrier_name VARCHAR(255),
  total_pallets INT,
  truck_number VARCHAR(255),
  tonu_customer_money_currency VARCHAR(255),
  reserved_carrier_id VARCHAR(255),
  booked_carrier_id VARCHAR(255),
  cargo_value_amount INT,
  empty_city_state VARCHAR(255),
  is_committed BOOLEAN,
  low_value_captured_at VARCHAR(255),
  status VARCHAR(255),
  empty_date_time VARCHAR(255),
  total_weight DOUBLE,
  reserved_carrier_name VARCHAR(255),
  driver_phone_number VARCHAR(255),
  ready_date DATE,
  total_pieces INT,
  rate_con_sent_by INT,
  tonu_carrier_money_currency VARCHAR(255),
  ready_time VARCHAR(255),
  tonu_rate_con_recipients VARCHAR(255),
  trailer_number VARCHAR(255),
  rolled_by INT,
  relay_reference_number INT,
  booked_by_name VARCHAR(255),
  driver_info_captured_by INT,
  receiver_city VARCHAR(255),
  total_miles DOUBLE,
  tonu_carrier_money_amount INT,
  booked_at VARCHAR(255),
  booked_total_carrier_rate_currency VARCHAR(255),
  tonu_customer_money_amount INT,
  receiver_state VARCHAR(255),
  reservation_cancelled_by INT,
  is_tonu_issued BOOLEAN,
  reserved_by INT,
  first_shipper_city VARCHAR(255),
  rate_cons_sent_count INT,
  receiver_name VARCHAR(255),
  reserved_at VARCHAR(255),
  tonu_issued_by INT,
  driver_name VARCHAR(255),
  first_shipper_state VARCHAR(255),
  second_shipper_state VARCHAR(255),
  rate_con_recipients STRING,
  low_value_captured_by INT,
  rolled_at VARCHAR(255),
  first_shipper_name VARCHAR(255),
  is_must_check_high_value BOOLEAN,
  second_shipper_city VARCHAR(255),
  booked_total_carrier_rate_amount INT,
  stop_count INT,
  cargo_value_currency VARCHAR(255),
  second_shipper_name VARCHAR(255),
  last_rate_con_sent_time VARCHAR(255),
  bounced_at VARCHAR(255),
  bounced_by INT,
  loading_type VARCHAR(255),
  first_shipper_zip VARCHAR(255),
  first_shipper_id VARCHAR(255),
  second_shipper_zip VARCHAR(255),
  second_shipper_id VARCHAR(255),
  receiver_id VARCHAR(255),
  receiver_zip VARCHAR(255),
  first_intermediary_receiver_id VARCHAR(255),
  first_intermediary_receiver_name VARCHAR(255),
  first_intermediary_receiver_city VARCHAR(255),
  first_intermediary_receiver_state VARCHAR(255),
  first_intermediary_receiver_zip VARCHAR(255),
  second_intermediary_receiver_id VARCHAR(255),
  second_intermediary_receiver_name VARCHAR(255),
  second_intermediary_receiver_city VARCHAR(255),
  second_intermediary_receiver_state VARCHAR(255),
  second_intermediary_receiver_zip VARCHAR(255),
  pickup_count INT,
  delivery_count INT,
  tender_on_behalf_of_id VARCHAR(255),
  tender_on_behalf_of_type VARCHAR(255),
  booked_by_user_id INT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `booking_projection_pkey` PRIMARY KEY (`booking_id`))

-- COMMAND ----------

-- MAGIC %md ##customer_distance_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.customer_distance_projection (
  tender_id VARCHAR(255) NOT NULL,
  command_builder_url VARCHAR(255),
  customer VARCHAR(255),
  customer_distance_in_miles INT,
  is_cancelled BOOLEAN,
  relay_reference_number INT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `customer_distance_projection_pkey` PRIMARY KEY (`tender_id`))

-- COMMAND ----------

-- MAGIC %md ##tendering_orders_product_descriptions

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tendering_orders_product_descriptions (
  tender_id VARCHAR(255) NOT NULL,
  orders_product_descriptions STRING,
  relay_reference_number INT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `tendering_orders_product_descriptions_pkey` PRIMARY KEY (`tender_id`))

-- COMMAND ----------

-- MAGIC %md ##tender_reference_numbers_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tender_reference_numbers_projection (
  tender_id VARCHAR(255) NOT NULL,
  cancelled_by INT,
  is_cancelled BOOLEAN,
  is_split BOOLEAN,
  nfi_pro_number STRING,
  original_shipment_id STRING,
  order_numbers STRING,
  po_numbers STRING,
  relay_reference_number INT,
  target_shipment_id STRING,
  tender_on_behalf_of VARCHAR(255),
  cancelled_at TIMESTAMP_NTZ,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `tender_reference_numbers_projection_pkey1` PRIMARY KEY (`tender_id`))

-- COMMAND ----------

-- MAGIC %md ##big_export_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.big_export_projection (
  load_number INT NOT NULL,
  carrier_accessorial_expense INT,
  carrier_fuel_expense INT,
  carrier_id VARCHAR(255),
  carrier_linehaul_expense INT,
  carrier_name VARCHAR(255),
  carrier_pro_number VARCHAR(255),
  charges STRING,
  consignee_city VARCHAR(255),
  consignee_name VARCHAR(255),
  consignee_state VARCHAR(255),
  consignee_zip VARCHAR(255),
  customer_id VARCHAR(255),
  delivered_date VARCHAR(255),
  delivery_number STRING,
  dispatch_status VARCHAR(255),
  invoice_date VARCHAR(255),
  miles DOUBLE,
  pallet_count INT,
  pickup_city VARCHAR(255),
  pickup_name VARCHAR(255),
  pickup_number STRING,
  pickup_state VARCHAR(255),
  pickup_zip VARCHAR(255),
  piece_count INT,
  po_number VARCHAR(255),
  projected_expense INT,
  ship_date VARCHAR(255),
  weight DOUBLE,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `big_export_projection_pkey` PRIMARY KEY (`load_number`))

-- COMMAND ----------

-- MAGIC %md ##tendering_acceptance

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.tendering_acceptance (
  tender_id VARCHAR(255) NOT NULL,
  `accepted?` BOOLEAN,
  accepted_at TIMESTAMP_NTZ,
  accepted_by INT,
  must_respond_by TIMESTAMP_NTZ,
  relay_reference_number INT,
  shipment_id STRING,
  tender_on_behalf_of_id VARCHAR(255),
  tender_on_behalf_of_type VARCHAR(255),
  tendered_at TIMESTAMP_NTZ,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `tendering_acceptance_pkey` PRIMARY KEY (`tender_id`))

-- COMMAND ----------

-- MAGIC %md ##vendor_transaction_projection

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.vendor_transaction_projection (
  charge_id VARCHAR(255) NOT NULL,
  amount INT,
  booking_id INT,
  charge_code VARCHAR(255),
  currency VARCHAR(255),
  `finalized?` BOOLEAN,
  finalized_at TIMESTAMP_NTZ,
  incurred_at TIMESTAMP_NTZ,
  relay_reference_number INT,
  vendor_id VARCHAR(255),
  vendor_transaction_id VARCHAR(255),
  `voided?` BOOLEAN,
  voided_at TIMESTAMP_NTZ,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `vendor_transaction_projection_pkey` PRIMARY KEY (`charge_id`))

-- COMMAND ----------

-- MAGIC %md ##planning_stop_schedule

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.planning_stop_schedule (
  stop_id VARCHAR(255) NOT NULL,
  appointment_datetime TIMESTAMP_NTZ,
  stop_type VARCHAR(255),
  volume_unit VARCHAR(255),
  address_1 VARCHAR(255),
  administrative_region VARCHAR(255),
  window_end_datetime TIMESTAMP_NTZ,
  ready_date DATE,
  relay_reference_number INT,
  plan_id VARCHAR(255),
  schedule_id VARCHAR(255),
  window_start_datetime TIMESTAMP_NTZ,
  volume_amount FLOAT,
  address_2 VARCHAR(255),
  `removed?` BOOLEAN,
  weight_amount FLOAT,
  postal_code VARCHAR(255),
  phone_number VARCHAR(255),
  scheduled_at TIMESTAMP_NTZ,
  order_numbers STRING,
  shipment_id STRING,
  sequence_number INT,
  pieces_count FLOAT,
  cleared_at TIMESTAMP_NTZ,
  scheduled_by INT,
  weight_unit VARCHAR(255),
  pieces_packaging_type VARCHAR(255),
  late_reason VARCHAR(255),
  cleared_by INT,
  country_code VARCHAR(255),
  stop_name VARCHAR(255),
  reference_numbers STRING,
  schedule_reference STRING,
  locality VARCHAR(255),
  shipping_units_type VARCHAR(255),
  schedule_type VARCHAR(255),
  shipping_units_count FLOAT,
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `planning_stop_schedule_pkey` PRIMARY KEY (`stop_id`))

-- COMMAND ----------

-- MAGIC %md ##canonical_stop

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.canonical_stop (
  stop_id VARCHAR(255) NOT NULL,
  address_1 VARCHAR(255),
  address_2 VARCHAR(255),
  administrative_region VARCHAR(255),
  booking_id INT,
  country_code VARCHAR(255),
  facility_id VARCHAR(255),
  facility_name VARCHAR(255),
  in_date_time VARCHAR(255),
  locality VARCHAR(255),
  out_date_time VARCHAR(255),
  postal_code VARCHAR(255),
  stop_reference_numbers STRING,
  stop_type VARCHAR(255),
  `stale?` BOOLEAN,
  reason_code VARCHAR(255),
  inserted_at TIMESTAMP_NTZ NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT `canonical_stop_pkey` PRIMARY KEY (`stop_id`))

-- COMMAND ----------

-- MAGIC %md ##Projection_load_1

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.projection_load_1 (
  id VARCHAR(255) NOT NULL,
  accessorial1 VARCHAR(255),
  accessorial2 VARCHAR(255),
  accessorial3 VARCHAR(255),
  accessorial4 VARCHAR(255),
  accessorial5 VARCHAR(255),
  accessorial6 VARCHAR(255),
  accessorial7 VARCHAR(255),
  accessorial8 VARCHAR(255),
  act_disp VARCHAR(255),
  act_min VARCHAR(255),
  act_team VARCHAR(255),
  arrive_cons_code VARCHAR(255),
  arrive_consignee_date VARCHAR(255),
  arrive_consignee_time VARCHAR(255),
  arrive_pickup_code VARCHAR(255),
  arrive_pickup_date VARCHAR(255),
  arrive_pickup_time VARCHAR(255),
  billed_date VARCHAR(255),
  blank199 VARCHAR(255),
  blind VARCHAR(255),
  carrier_accessorial_id_1 VARCHAR(255),
  carrier_accessorial_id_2 VARCHAR(255),
  carrier_accessorial_id_3 VARCHAR(255),
  carrier_accessorial_id_4 VARCHAR(255),
  carrier_accessorial_id_5 VARCHAR(255),
  carrier_accessorial_id_6 VARCHAR(255),
  carrier_accessorial_id_7 VARCHAR(255),
  carrier_accessorial_id_8 VARCHAR(255),
  carrier_accessorial1 VARCHAR(255),
  carrier_accessorial2 VARCHAR(255),
  carrier_accessorial3 VARCHAR(255),
  carrier_accessorial4 VARCHAR(255),
  carrier_accessorial5 VARCHAR(255),
  carrier_accessorial6 VARCHAR(255),
  carrier_accessorial7 VARCHAR(255),
  carrier_accessorial8 VARCHAR(255),
  carrier_fax_num VARCHAR(255),
  carrier_id VARCHAR(255),
  carrier_line_haul VARCHAR(255),
  carrier_phone_num VARCHAR(255),
  carrier_ref_num VARCHAR(255),
  carrier_total_rate VARCHAR(255),
  check_call_date VARCHAR(255),
  check_call_location VARCHAR(255),
  check_call_notes VARCHAR(255),
  check_call_state VARCHAR(255),
  check_call_status VARCHAR(255),
  check_call_time VARCHAR(255),
  check_call_user VARCHAR(255),
  class VARCHAR(255),
  consignee VARCHAR(255),
  consignee_address VARCHAR(255),
  consignee_address_line2 VARCHAR(255),
  consignee_appointment_date VARCHAR(255),
  consignee_appointment_note VARCHAR(255),
  consignee_appointment_time VARCHAR(255),
  consignee_contact VARCHAR(255),
  consignee_hours VARCHAR(255),
  consignee_phone VARCHAR(255),
  consignee_reference_number VARCHAR(255),
  consignee_zip_code VARCHAR(255),
  covered_date VARCHAR(255),
  date VARCHAR(255),
  del_eta_date VARCHAR(255),
  del_eta_time VARCHAR(255),
  delivery_date VARCHAR(255),
  delivery_time VARCHAR(255),
  depart_cons_code VARCHAR(255),
  depart_pickup_code VARCHAR(255),
  description VARCHAR(255),
  dest_city VARCHAR(255),
  dest_state VARCHAR(255),
  dispatched_city VARCHAR(255),
  dispatched_date VARCHAR(255),
  dispatched_state VARCHAR(255),
  dispatched_time VARCHAR(255),
  division VARCHAR(255),
  driver_cell_num VARCHAR(255),
  equipment VARCHAR(255),
  extra_ref_1 VARCHAR(255),
  extra_ref_2 VARCHAR(255),
  extra_ref_3 VARCHAR(255),
  extra_ref_4 VARCHAR(255),
  extra_ref_5 VARCHAR(255),
  extra_ref_6 VARCHAR(255),
  hazmat VARCHAR(255),
  invoice_total VARCHAR(255),
  key_c_date VARCHAR(255),
  key_c_time VARCHAR(255),
  key_c_user VARCHAR(255),
  key_d_date VARCHAR(255),
  key_d_time VARCHAR(255),
  key_d_user VARCHAR(255),
  key_h_date VARCHAR(255),
  key_h_time VARCHAR(255),
  key_h_user VARCHAR(255),
  key_l_date VARCHAR(255),
  key_l_time VARCHAR(255),
  key_l_user VARCHAR(255),
  key_p_date VARCHAR(255),
  key_p_time VARCHAR(255),
  key_p_user VARCHAR(255),
  key_r_date VARCHAR(255),
  key_r_time VARCHAR(255),
  key_r_user VARCHAR(255),
  key_w_date VARCHAR(255),
  key_w_time VARCHAR(255),
  key_w_user VARCHAR(255),
  load_ref VARCHAR(255),
  loaded_date VARCHAR(255),
  loaded_time VARCHAR(255),
  manifest_num VARCHAR(255),
  miles VARCHAR(255),
  mode VARCHAR(255),
  must_del_date VARCHAR(255),
  must_ship_date VARCHAR(255),
  office VARCHAR(255),
  origin_city VARCHAR(255),
  origin_state VARCHAR(255),
  Osd VARCHAR(255),
  oversized VARCHAR(255),
  pallet_ex VARCHAR(255),
  pallets_in VARCHAR(255),
  pallets_out VARCHAR(255),
  permits VARCHAR(255),
  pickup_address VARCHAR(255),
  pickup_address_line2 VARCHAR(255),
  pickup_appointment_note VARCHAR(255),
  pickup_appt_date VARCHAR(255),
  pickup_appt_time VARCHAR(255),
  pickup_contact VARCHAR(255),
  pickup_date VARCHAR(255),
  pickup_hours VARCHAR(255),
  pickup_name VARCHAR(255),
  pickup_phone VARCHAR(255),
  pickup_reference_number VARCHAR(255),
  pickup_time VARCHAR(255),
  pickup_zip_code VARCHAR(255),
  pieces VARCHAR(255),
  pod VARCHAR(255),
  pod_signature_1 VARCHAR(255),
  pod_signature_2 VARCHAR(255),
  pod_signature_3 VARCHAR(255),
  pod_signature_4 VARCHAR(255),
  pod_signature_5 VARCHAR(255),
  pod_signature_6 VARCHAR(255),
  pod_signature_14 VARCHAR(255),
  pod_signature_17 VARCHAR(255),
  pod_signature_19 VARCHAR(255),
  pod_signature_20 VARCHAR(255),
  ps_acct_1 VARCHAR(255),
  ps_acct_2 VARCHAR(255),
  ps_acct_3 VARCHAR(255),
  ps_acct_4 VARCHAR(255),
  ps_acct_5 VARCHAR(255),
  ps_acct_6 VARCHAR(255),
  ps_acct_7 VARCHAR(255),
  ps_acct_8 VARCHAR(255),
  ps_acct_9 VARCHAR(255),
  ps_acct_10 VARCHAR(255),
  ps_acct_11 VARCHAR(255),
  ps_acct_12 VARCHAR(255),
  ps_acct_13 VARCHAR(255),
  ps_acct_14 VARCHAR(255),
  ps_acct_15 VARCHAR(255),
  ps_acct_16 VARCHAR(255),
  ps_acct_17 VARCHAR(255),
  ps_acct_18 VARCHAR(255),
  ps_acct_19 VARCHAR(255),
  ps_acct_20 VARCHAR(255),
  ps_address_1 VARCHAR(255),
  ps_address_2 VARCHAR(255),
  ps_address_3 VARCHAR(255),
  ps_address_4 VARCHAR(255),
  ps_address_5 VARCHAR(255),
  ps_address_6 VARCHAR(255),
  ps_address_7 VARCHAR(255),
  ps_address_8 VARCHAR(255),
  ps_address_9 VARCHAR(255),
  ps_address_10 VARCHAR(255),
  ps_address_11 VARCHAR(255),
  ps_address_12 VARCHAR(255),
  ps_address_13 VARCHAR(255),
  ps_address_14 VARCHAR(255),
  ps_address_15 VARCHAR(255),
  ps_address_16 VARCHAR(255),
  ps_address_17 VARCHAR(255),
  ps_address_18 VARCHAR(255),
  ps_address_19 VARCHAR(255),
  ps_address_20 VARCHAR(255),
  ps_appt_date_1 VARCHAR(255),
  ps_appt_date_2 VARCHAR(255),
  ps_appt_date_3 VARCHAR(255),
  ps_appt_date_4 VARCHAR(255),
  ps_appt_date_5 VARCHAR(255),
  ps_appt_date_6 VARCHAR(255),
  ps_appt_date_7 VARCHAR(255),
  ps_appt_date_8 VARCHAR(255),
  ps_appt_date_9 VARCHAR(255),
  ps_appt_date_10 VARCHAR(255),
  ps_appt_date_11 VARCHAR(255),
  ps_appt_date_12 VARCHAR(255),
  ps_appt_date_13 VARCHAR(255),
  ps_appt_date_14 VARCHAR(255),
  ps_appt_date_15 VARCHAR(255),
  ps_appt_date_16 VARCHAR(255),
  ps_appt_date_17 VARCHAR(255),
  ps_appt_date_18 VARCHAR(255),
  ps_appt_date_19 VARCHAR(255),
  ps_appt_time_1 VARCHAR(255),
  ps_appt_time_2 VARCHAR(255),
  ps_appt_time_3 VARCHAR(255),
  ps_appt_time_4 VARCHAR(255),
  ps_appt_time_5 VARCHAR(255),
  ps_appt_time_6 VARCHAR(255),
  ps_appt_time_7 VARCHAR(255),
  ps_appt_time_8 VARCHAR(255),
  ps_appt_time_9 VARCHAR(255),
  ps_appt_time_10 VARCHAR(255),
  ps_appt_time_11 VARCHAR(255),
  ps_appt_time_12 VARCHAR(255),
  ps_appt_time_13 VARCHAR(255),
  ps_appt_time_14 VARCHAR(255),
  ps_appt_time_15 VARCHAR(255),
  ps_appt_time_16 VARCHAR(255),
  ps_appt_time_17 VARCHAR(255),
  ps_appt_time_18 VARCHAR(255),
  ps_appt_time_19 VARCHAR(255),
  inserted_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT `projection_load_pkey_1` PRIMARY KEY (`id`))

-- COMMAND ----------

-- MAGIC %md ##Projection_load_2

-- COMMAND ----------

CREATE TABLE brokeragedev.bronze.projection_load_2 (
  id VARCHAR(255) NOT NULL,
  ps_arrive_code_1 VARCHAR(255),
  ps_arrive_code_2 VARCHAR(255),
  ps_arrive_code_3 VARCHAR(255),
  ps_arrive_code_4 VARCHAR(255),
  ps_arrive_code_5 VARCHAR(255),
  ps_arrive_code_6 VARCHAR(255),
  ps_arrive_code_7 VARCHAR(255),
  ps_arrive_code_8 VARCHAR(255),
  ps_arrive_code_9 VARCHAR(255),
  ps_arrive_code_10 VARCHAR(255),
  ps_arrive_code_11 VARCHAR(255),
  ps_arrive_code_12 VARCHAR(255),
  ps_arrive_code_16 VARCHAR(255),
  ps_arrive_date_1 VARCHAR(255),
  ps_arrive_date_2 VARCHAR(255),
  ps_arrive_date_3 VARCHAR(255),
  ps_arrive_date_4 VARCHAR(255),
  ps_arrive_date_5 VARCHAR(255),
  ps_arrive_date_6 VARCHAR(255),
  ps_arrive_date_7 VARCHAR(255),
  ps_arrive_date_8 VARCHAR(255),
  ps_arrive_date_9 VARCHAR(255),
  ps_arrive_date_10 VARCHAR(255),
  ps_arrive_date_11 VARCHAR(255),
  ps_arrive_date_12 VARCHAR(255),
  ps_arrive_time_1 VARCHAR(255),
  ps_arrive_time_2 VARCHAR(255),
  ps_arrive_time_3 VARCHAR(255),
  ps_arrive_time_4 VARCHAR(255),
  ps_arrive_time_5 VARCHAR(255),
  ps_arrive_time_6 VARCHAR(255),
  ps_arrive_time_7 VARCHAR(255),
  ps_arrive_time_8 VARCHAR(255),
  ps_arrive_time_9 VARCHAR(255),
  ps_arrive_time_10 VARCHAR(255),
  ps_arrive_time_11 VARCHAR(255),
  ps_arrive_time_12 VARCHAR(255),
  ps_arrive_time_20 VARCHAR(255),
  ps_city_1 VARCHAR(255),
  ps_city_2 VARCHAR(255),
  ps_city_3 VARCHAR(255),
  ps_city_4 VARCHAR(255),
  ps_city_5 VARCHAR(255),
  ps_city_6 VARCHAR(255),
  ps_city_7 VARCHAR(255),
  ps_city_8 VARCHAR(255),
  ps_city_9 VARCHAR(255),
  ps_city_10 VARCHAR(255),
  ps_city_11 VARCHAR(255),
  ps_city_12 VARCHAR(255),
  ps_city_13 VARCHAR(255),
  ps_city_14 VARCHAR(255),
  ps_city_15 VARCHAR(255),
  ps_city_16 VARCHAR(255),
  ps_city_17 VARCHAR(255),
  ps_city_18 VARCHAR(255),
  ps_city_19 VARCHAR(255),
  ps_company_1 VARCHAR(255),
  ps_company_2 VARCHAR(255),
  ps_company_3 VARCHAR(255),
  ps_company_4 VARCHAR(255),
  ps_company_5 VARCHAR(255),
  ps_company_6 VARCHAR(255),
  ps_company_7 VARCHAR(255),
  ps_company_8 VARCHAR(255),
  ps_company_9 VARCHAR(255),
  ps_company_10 VARCHAR(255),
  ps_company_11 VARCHAR(255),
  ps_company_12 VARCHAR(255),
  ps_company_13 VARCHAR(255),
  ps_company_14 VARCHAR(255),
  ps_company_15 VARCHAR(255),
  ps_company_16 VARCHAR(255),
  ps_company_17 VARCHAR(255),
  ps_company_18 VARCHAR(255),
  ps_company_19 VARCHAR(255),
  ps_depart_code_1 VARCHAR(255),
  ps_depart_code_2 VARCHAR(255),
  ps_depart_code_3 VARCHAR(255),
  ps_depart_code_4 VARCHAR(255),
  ps_depart_code_5 VARCHAR(255),
  ps_depart_code_6 VARCHAR(255),
  ps_depart_code_7 VARCHAR(255),
  ps_depart_code_8 VARCHAR(255),
  ps_depart_code_9 VARCHAR(255),
  ps_depart_code_10 VARCHAR(255),
  ps_depart_code_11 VARCHAR(255),
  ps_depart_code_12 VARCHAR(255),
  ps_depart_code_14 VARCHAR(255),
  ps_depart_date_1 VARCHAR(255),
  ps_depart_date_2 VARCHAR(255),
  ps_depart_date_3 VARCHAR(255),
  ps_depart_date_4 VARCHAR(255),
  ps_depart_date_5 VARCHAR(255),
  ps_depart_date_6 VARCHAR(255),
  ps_depart_date_7 VARCHAR(255),
  ps_depart_date_8 VARCHAR(255),
  ps_depart_date_9 VARCHAR(255),
  ps_depart_date_10 VARCHAR(255),
  ps_depart_date_11 VARCHAR(255),
  ps_depart_date_12 VARCHAR(255),
  ps_depart_time_1 VARCHAR(255),
  ps_depart_time_2 VARCHAR(255),
  ps_depart_time_3 VARCHAR(255),
  ps_depart_time_4 VARCHAR(255),
  ps_depart_time_5 VARCHAR(255),
  ps_depart_time_6 VARCHAR(255),
  ps_depart_time_7 VARCHAR(255),
  ps_depart_time_8 VARCHAR(255),
  ps_depart_time_9 VARCHAR(255),
  ps_depart_time_10 VARCHAR(255),
  ps_depart_time_11 VARCHAR(255),
  ps_depart_time_12 VARCHAR(255),
  ps_ref_1 VARCHAR(255),
  ps_ref_2 VARCHAR(255),
  ps_ref_3 VARCHAR(255),
  ps_ref_4 VARCHAR(255),
  ps_ref_5 VARCHAR(255),
  ps_ref_6 VARCHAR(255),
  ps_ref_7 VARCHAR(255),
  ps_ref_8 VARCHAR(255),
  ps_ref_9 VARCHAR(255),
  ps_ref_10 VARCHAR(255),
  ps_ref_11 VARCHAR(255),
  ps_ref_12 VARCHAR(255),
  ps_ref_13 VARCHAR(255),
  ps_ref_14 VARCHAR(255),
  ps_ref_15 VARCHAR(255),
  ps_ref_16 VARCHAR(255),
  ps_ref_17 VARCHAR(255),
  ps_ref_18 VARCHAR(255),
  ps_ref_19 VARCHAR(255),
  ps_state_1 VARCHAR(255),
  ps_state_2 VARCHAR(255),
  ps_state_3 VARCHAR(255),
  ps_state_4 VARCHAR(255),
  ps_state_5 VARCHAR(255),
  ps_state_6 VARCHAR(255),
  ps_state_7 VARCHAR(255),
  ps_state_8 VARCHAR(255),
  ps_state_9 VARCHAR(255),
  ps_state_10 VARCHAR(255),
  ps_state_11 VARCHAR(255),
  ps_state_12 VARCHAR(255),
  ps_state_13 VARCHAR(255),
  ps_state_14 VARCHAR(255),
  ps_state_15 VARCHAR(255),
  ps_state_16 VARCHAR(255),
  ps_state_17 VARCHAR(255),
  ps_state_18 VARCHAR(255),
  ps_state_19 VARCHAR(255),
  ps_zip_1 VARCHAR(255),
  ps_zip_2 VARCHAR(255),
  ps_zip_3 VARCHAR(255),
  ps_zip_4 VARCHAR(255),
  ps_zip_5 VARCHAR(255),
  ps_zip_6 VARCHAR(255),
  ps_zip_7 VARCHAR(255),
  ps_zip_8 VARCHAR(255),
  ps_zip_9 VARCHAR(255),
  ps_zip_10 VARCHAR(255),
  ps_zip_11 VARCHAR(255),
  ps_zip_12 VARCHAR(255),
  ps_zip_13 VARCHAR(255),
  ps_zip_14 VARCHAR(255),
  ps_zip_15 VARCHAR(255),
  ps_zip_16 VARCHAR(255),
  ps_zip_17 VARCHAR(255),
  ps_zip_18 VARCHAR(255),
  ps_zip_19 VARCHAR(255),
  Ps1 VARCHAR(255),
  Ps2 VARCHAR(255),
  Ps3 VARCHAR(255),
  Ps4 VARCHAR(255),
  Ps5 VARCHAR(255),
  Ps6 VARCHAR(255),
  Ps7 VARCHAR(255),
  Ps8 VARCHAR(255),
  Ps9 VARCHAR(255),
  Ps10 VARCHAR(255),
  Ps11 VARCHAR(255),
  Ps12 VARCHAR(255),
  Ps13 VARCHAR(255),
  Ps14 VARCHAR(255),
  Ps15 VARCHAR(255),
  Ps16 VARCHAR(255),
  Ps17 VARCHAR(255),
  Ps18 VARCHAR(255),
  Ps19 VARCHAR(255),
  quote_number VARCHAR(255),
  ref_num VARCHAR(255),
  release_date VARCHAR(255),
  sales_rep VARCHAR(255),
  sales_team VARCHAR(255),
  seal VARCHAR(255),
  shipper VARCHAR(255),
  signed_by VARCHAR(255),
  sls_pct VARCHAR(255),
  srv_rep VARCHAR(255),
  status VARCHAR(255),
  straps_chains VARCHAR(255),
  tag_created_by VARCHAR(255),
  tag_creation_date VARCHAR(255),
  tag_creation_time VARCHAR(255),
  tariff_num VARCHAR(255),
  tarp_required VARCHAR(255),
  tarp_size VARCHAR(255),
  temp VARCHAR(255),
  temp_ctrl VARCHAR(255),
  time VARCHAR(255),
  Tord VARCHAR(255),
  trailer_number VARCHAR(255),
  truck_num VARCHAR(255),
  value VARCHAR(255),
  web_post VARCHAR(255),
  web_sync_action VARCHAR(255),
  web_sync_table_name VARCHAR(255),
  weight VARCHAR(255),
  will_pickup_date VARCHAR(255),
  will_pickup_time VARCHAR(255),
  asg_disp VARCHAR(255),
  bl_ref VARCHAR(255),
  carrier_accessorial_rate1 VARCHAR(255),
  carrier_accessorial_rate2 VARCHAR(255),
  carrier_accessorial_rate3 VARCHAR(255),
  carrier_accessorial_rate4 VARCHAR(255),
  carrier_accessorial_rate5 VARCHAR(255),
  carrier_accessorial_rate6 VARCHAR(255),
  carrier_accessorial_rate7 VARCHAR(255),
  carrier_accessorial_rate8 VARCHAR(255),
  customer_accessorial_rate1 VARCHAR(255),
  customer_accessorial_rate2 VARCHAR(255),
  customer_accessorial_rate3 VARCHAR(255),
  customer_accessorial_rate4 VARCHAR(255),
  customer_accessorial_rate5 VARCHAR(255),
  customer_accessorial_rate6 VARCHAR(255),
  customer_accessorial_rate7 VARCHAR(255),
  customer_accessorial_rate8 VARCHAR(255),
  customer_id VARCHAR(255),
  nmfc_num VARCHAR(255),
  inserted_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT `projection_load_pkey_2` PRIMARY KEY (`id`))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Market_Lookup

-- COMMAND ----------

CREATE TABLE bronze.market_lookup
(
    pickup_zip STRING,
    market VARCHAR(255),
    market_state VARCHAR(255)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Planning_Assignment_Log

-- COMMAND ----------

CREATE TABLE bronze.planning_assignment_log
(
    event_id VARCHAR(255) NOT NULL,
    action_at TIMESTAMP_NTZ,
    action_by integer,
    action_involving integer,
    assignment_action VARCHAR(255),
    assignment_id VARCHAR(255),
    plan_id VARCHAR(255),
    relay_reference_number integer,
    CONSTRAINT planning_assignment_log_pkey PRIMARY KEY (event_id)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Customer_Lookup

-- COMMAND ----------

CREATE OR REPLACE Table bronze.customer_lookup
(
    aljex_customer_name STRING,
    master_customer_name STRING,
    customer_ref_number STRING,
    aljex_customer_id STRING
)

-- COMMAND ----------

-- MAGIC %md ##cad_currency

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.cad_currency
(
    office varchar(255),
    pro_number BIGINT,
    currency_type varchar(255)
)

-- COMMAND ----------

-- MAGIC %md ##canada_carriers

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.canada_carriers
(
    canada_dot varchar(255)
)

-- COMMAND ----------

-- MAGIC %md ##new_office_lookup_dup

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.new_office_lookup_dup
(
    old_office_dup varchar(255),
    new_office_dup varchar(255)
)

-- COMMAND ----------

