-- Databricks notebook source
-- MAGIC %python
-- MAGIC ## Mention the catalog used, getting the catalog name from key vault
-- MAGIC CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
-- MAGIC spark.sql(f"USE CATALOG {CatalogName}")

-- COMMAND ----------

-- MAGIC %md ##Truncate and load

-- COMMAND ----------

INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
SourceSelectQuery
)
VALUES ("Lookup"	,
"L10"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"caam_list"	,
"truncate and load"	,
"1"	,
"SELECT dot_number, mc_number, carrier_name, caam_full_name, caam_id, cdr, hubspot_team, period, financial_qtr, start_date, period_start, today_date, big_concat_for_update FROM public.caam_list")

-- COMMAND ----------

INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
SourceSelectQuery
)
VALUES ("Lookup"	,
"L11"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"new_carrier_ownership"	,
"truncate and load"	,
"1"	,
"SELECT cdr_id, cdr_office, owned_carrier_name, mc_number, dot_number, period, financial_qtr, concat_primary_key FROM public.new_carrier_ownership");

-- COMMAND ----------

--new_brokerage_budget
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
Frequency ,
SourceSelectQuery
)
VALUES ("Relay"	,
"R55"	,
"Finance"	,
"ad_hoc_analysis"	,
"Public"	,
"new_brokerage_budget"	,
"truncate and load"	,
"1"	,
"15 Minutes"  ,
"SELECT financial_period, office_name, rev_budget, mar_budget, mar_perc_budget, volume_budget FROM public.new_brokerage_budget")

-- COMMAND ----------

--am_sales_lookup
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
Frequency ,
SourceSelectQuery
)
VALUES ("Relay"	,
"R56"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"am_sales_lookup"	,
"truncate and load"	,
"1"	,
"15 Minutes"  ,
"SELECT acct_mgr, customer_name, lawson_id, office, sales_rep, extra_bill_to FROM public.am_sales_lookup");

-- COMMAND ----------

--integration_p44_create_shipment_succeeded
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
Frequency  ,
SourceSelectQuery
)
VALUES ("Relay"	,
"R57"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"integration_p44_create_shipment_succeeded"	,
"truncate and load"	,
"1"	,
"15 Minutes"  ,
"SELECT relay_reference_number, pickup_confirmation_number
FROM public.integration_p44_create_shipment_succeeded");

-- COMMAND ----------

--invoicing_invoices
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
Frequency  ,
SourceSelectQuery
)
VALUES ("Relay"	,
"R58"	,
"Invoice&Finance"	,
"ad_hoc_analysis"	,
"Public"	,
"invoicing_invoices"	,
"truncate and load"	,
"1"	,
"15 Minutes"  ,
"SELECT invoice_number, invoice_id, relay_reference_number, total_taxes_float, total_credits_float, invoice_total_float, total_taxes_amount, 
total_credits_amount, invoice_total_amount, billing_party_id, responsible_party_id, charge_ids, currency FROM public.invoicing_invoices");

-- COMMAND ----------

--tendering_tendered_assigned_carrier
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
Frequency  ,
SourceSelectQuery
)
VALUES ("Relay"	,
"R59"	,
"Carrier"	,
"ad_hoc_analysis"	,
"Public"	,
"tendering_tendered_assigned_carrier"	,
"truncate and load"	,
"1"	,
"15 Minutes"  ,
"SELECT tender_id, assigned_carrier_name, assigned_carrier_scac, relay_reference_number, shipment_id, tender_on_behalf_of_id, tender_on_behalf_of_type FROM public.tendering_tendered_assigned_carrier")

-- COMMAND ----------

--integration_standardized_distance
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
Frequency,
SourceSelectQuery
)
VALUES ("Relay"	,
"R65"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"integration_standardized_distance"	,
"truncate and load"	,
"1"	,
"15 Minutes"  ,
'SELECT id, relay_reference_number, standardized_distance, standardized_distance_units, distance_source	FROM public.integration_standardized_distance');

-- COMMAND ----------

INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
Frequency	,
SourceSelectQuery
)
VALUES ("Aljex"	,
"A12"	,
"Customer"	,
"ad_hoc_analysis"	,
"Public"	,
"aljex_am_by_acct"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
"SELECT customer_name, account_num, salesrep, office, am FROM public.aljex_am_by_acct")

-- COMMAND ----------

-- MAGIC %md ##Incremental Load

-- COMMAND ----------

--integration_tracking_notification_triggered
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R61"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"integration_tracking_notification_triggered"	,
"incremental load"	,
"1"	,
'SELECT event_id, booking_id, relay_reference_number, shipment_id, status, status_datetime, stop_id, tender_on_behalf_of_id, tender_on_behalf_of_type, triggered_at, status_reason_code, status_type, inserted_at, updated_at, latitude, longitude FROM public.integration_tracking_notification_triggered where "{0}" >=  ',
"updated_at",
"2020-01-01T19:34:43.775403",
"event_id");

-- COMMAND ----------

--tender_split_projection
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R63"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"tender_split_projection"	,
"incremental load"	,
"1"	,
'SELECT tender_id, relay_reference_number, remaining_orders, split_orders, inserted_at, updated_at FROM public.tender_split_projection where "{0}" >= ',
"updated_at",
"2020-01-01T19:34:43.775403",
"tender_id");

-- COMMAND ----------

--tender_shipper_address_projection
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R62"	,
"shipper"	,
"ad_hoc_analysis"	,
"Public"	,
"tender_shipper_address_projection"	,
"incremental load"	,
"1"	,
'SELECT tender_id, enriched_pickup_1_address_1, enriched_pickup_1_address_2, enriched_pickup_1_city, enriched_pickup_1_name, enriched_pickup_1_postal_code, enriched_pickup_1_state, enriched_pickup_2_address_1, enriched_pickup_2_address_2, enriched_pickup_2_city, enriched_pickup_2_name, enriched_pickup_2_postal_code, enriched_pickup_2_state, tender_on_behalf_of, validated_pickup_1_address_1, validated_pickup_1_address_2, validated_pickup_1_city, validated_pickup_1_name, validated_pickup_1_postal_code, validated_pickup_1_state, validated_pickup_2_address_1, validated_pickup_2_address_2, validated_pickup_2_city, validated_pickup_2_name, validated_pickup_2_postal_code, validated_pickup_2_state, inserted_at, updated_at	FROM public.tender_shipper_address_projection  where "{0}" >=  ',
"updated_at",
"2020-01-01T19:34:43.775403",
"tender_id");

-- COMMAND ----------

--max_buy_projection
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R64"	,
"Finance"	,
"ad_hoc_analysis"	,
"Public"	,
"max_buy_projection"	,
"incremental load"	,
"1"	,
'SELECT relay_reference_number, currency, max_buy, notes, pricing_id, set_at, set_by, inserted_at, updated_at	FROM public.max_buy_projection where "{0}" >=  ',
"updated_at",
"2020-01-01T19:34:43.775403",
"relay_reference_number");

-- COMMAND ----------

--integration_tender_mapped_projection_v2
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R60"	,
"tender&shipments"	,
"ad_hoc_analysis"	,
"Public"	,
"integration_tender_mapped_projection_v2"	,
"incremental load"	,
"1"	,
'SELECT event_number, customer, shipment_id, tendered_at, event_type, tenderer FROM public.integration_tender_mapped_projection_v2 where "{0}" >=  ',
"tendered_at",
"2018-01-01T19:34:43.775403",
"event_number");

-- COMMAND ----------

--shipper_in_out_projection
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
Frequency	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R66"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"shipper_in_out_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
'SELECT truck_load_thing_id, booking_id, pickup_one_in_date_time, pickup_one_out_date_time, pickup_one_stop_id, pickup_two_in_date_time, pickup_two_out_date_time,pickup_two_stop_id, relay_reference_number, inserted_at, updated_at FROM public.shipper_in_out_projection  where "{0}" >=  ',
"updated_at",
"2020-01-01T01:42:56.465Z",
"truck_load_thing_id");

-- COMMAND ----------

--target_customer_money
INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R67"	,
"Finance"	,
"ad_hoc_analysis"	,
"Public"	,
"target_customer_money"	,
"incremental load"	,
"1"	,
'SELECT tender_id, fuel_surcharge_amount, linehaul_amount, relay_reference_number, customer, accessorial_amount, cargo_info_corrected, inserted_at, updated_at FROM public.target_customer_money  where "{0}" >=  ',
"updated_at",
"2020-01-01T01:42:56.465Z",
"tender_id");

-- COMMAND ----------

INSERT INTO metadata.MasterMetadata (SourceSystem	,
TableID	,
SubjectArea	,
SourceDBName	,
SourceSchema	,
SourceTableName	,
LoadType	,
IsActive	,
SourceSelectQuery
)
VALUES ("Lookup"	,
"L12"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"financial_calendar"	,
"truncate and load"	,
"1"	,
"SELECT date,month,year,year_month,financial_period_number,financial_quarter_number,financial_year,financial_period,financial_period_sorting,financial_period_description,financial_quarter,financial_quarter_description FROM public.financial_calendar")

-- COMMAND ----------

