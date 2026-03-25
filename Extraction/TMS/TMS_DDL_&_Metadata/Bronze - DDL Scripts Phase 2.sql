-- Databricks notebook source
-- MAGIC %python
-- MAGIC ## Mention the catalog used, getting the catalog name from key vault
-- MAGIC CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
-- MAGIC spark.sql(f"USE CATALOG {CatalogName}")

-- COMMAND ----------

-- MAGIC %md ##am_sales_lookup

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.am_sales_lookup
(
    acct_mgr  VARCHAR(255),
    customer_name  VARCHAR(255),
    lawson_id  VARCHAR(255),
    office  VARCHAR(255),
    sales_rep  VARCHAR(255),
    extra_bill_to VARCHAR(255) 
)

-- COMMAND ----------

-- MAGIC %md ##integration_p44_create_shipment_succeeded

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.integration_p44_create_shipment_succeeded
(
    relay_reference_number integer NOT NULL,
    pickup_confirmation_number   VARCHAR(255), 
    CONSTRAINT integration_p44_create_shipment_succeeded_pkey PRIMARY KEY (relay_reference_number)
)

-- COMMAND ----------

-- MAGIC %md ##integration_standardized_distance

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.integration_standardized_distance
(
    id bigint NOT NULL,
    relay_reference_number integer NOT NULL,
    standardized_distance double,
    standardized_distance_units   VARCHAR(255), 
    distance_source   VARCHAR(255), 
    CONSTRAINT integration_standardized_distance_pkey PRIMARY KEY (id, relay_reference_number)
)

-- COMMAND ----------

-- MAGIC %md ##integration_tender_mapped_projection_v2

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.integration_tender_mapped_projection_v2
(
    event_number bigint NOT NULL,
    customer   VARCHAR(255), 
    shipment_id STRING,
    tendered_at  TIMESTAMP_NTZ  ,
    event_type   VARCHAR(255), 
    tenderer   VARCHAR(255), 
    CONSTRAINT integration_tender_mapped_projection_v2_pkey PRIMARY KEY (event_number)
)

-- COMMAND ----------

-- MAGIC %md ##integration_tracking_notification_triggered

-- COMMAND ----------

CREATE OR REPLACE TABLE  bronze.integration_tracking_notification_triggered
(
    event_id   VARCHAR(255)  NOT NULL,
    booking_id integer,
    relay_reference_number integer,
    shipment_id STRING,
    status   VARCHAR(255), 
    status_datetime TIMESTAMP_NTZ,
    stop_id   VARCHAR(255), 
    tender_on_behalf_of_id   VARCHAR(255), 
    tender_on_behalf_of_type   VARCHAR(255), 
    triggered_at  TIMESTAMP_NTZ  ,
    status_reason_code   VARCHAR(255), 
    status_type   VARCHAR(255), 
    inserted_at  TIMESTAMP_NTZ   NOT NULL,
    updated_at  TIMESTAMP_NTZ   NOT NULL,
    latitude double,
    longitude double,
    CONSTRAINT integration_tracking_notification_triggered_pkey PRIMARY KEY (event_id)
)

-- COMMAND ----------

-- MAGIC %md ##invoicing_invoices

-- COMMAND ----------

CREATE OR REPLACE TABLE  bronze.invoicing_invoices
(
    invoice_number  VARCHAR(255)  NOT NULL,
    invoice_id   VARCHAR(255), 
    relay_reference_number integer,
    total_taxes_float double,
    total_credits_float double,
    invoice_total_float double,
    total_taxes_amount integer,
    total_credits_amount integer,
    invoice_total_amount integer,
    billing_party_id   VARCHAR(255), 
    responsible_party_id   VARCHAR(255), 
    charge_ids string,
    currency   VARCHAR(255), 
    CONSTRAINT invoicing_invoices_pkey PRIMARY KEY (invoice_number)
)

-- COMMAND ----------

-- MAGIC %md ##max_buy_projection

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.max_buy_projection
(
    relay_reference_number integer NOT NULL,
    currency   VARCHAR(255), 
    max_buy integer,
    notes STRING,
    pricing_id   VARCHAR(255), 
    set_at   VARCHAR(255), 
    set_by   VARCHAR(255), 
    inserted_at  TIMESTAMP_NTZ   NOT NULL,
    updated_at  TIMESTAMP_NTZ   NOT NULL,
    CONSTRAINT max_buy_projection_pkey PRIMARY KEY (relay_reference_number)
)

-- COMMAND ----------

-- MAGIC %md ##shipper_in_out_projection

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.shipper_in_out_projection
(
    truck_load_thing_id  VARCHAR(255) NOT NULL,
    booking_id integer,
    pickup_one_in_date_time   VARCHAR(255), 
    pickup_one_out_date_time   VARCHAR(255), 
    pickup_one_stop_id   VARCHAR(255), 
    pickup_two_in_date_time   VARCHAR(255), 
    pickup_two_out_date_time   VARCHAR(255), 
    pickup_two_stop_id   VARCHAR(255), 
    relay_reference_number integer,
    inserted_at  TIMESTAMP_NTZ   NOT NULL,
    updated_at  TIMESTAMP_NTZ   NOT NULL,
    CONSTRAINT shipper_in_out_projection_pkey PRIMARY KEY (truck_load_thing_id)
)

-- COMMAND ----------

-- MAGIC %md ##target_customer_money

-- COMMAND ----------

CREATE OR REPLACE TABLE  bronze.target_customer_money
(
    tender_id VARCHAR(255) NOT NULL,
    fuel_surcharge_amount integer,
    linehaul_amount integer,
    relay_reference_number integer,
    customer   VARCHAR(255), 
    accessorial_amount integer,
    cargo_info_corrected boolean,
    inserted_at  TIMESTAMP_NTZ   NOT NULL,
    updated_at  TIMESTAMP_NTZ   NOT NULL,
    CONSTRAINT target_customer_money_pkey PRIMARY KEY (tender_id)
)

-- COMMAND ----------

-- MAGIC %md ##tender_shipper_address_projection

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.tender_shipper_address_projection
(
    tender_id VARCHAR(255)   NOT NULL,
    enriched_pickup_1_address_1   VARCHAR(255), 
    enriched_pickup_1_address_2   VARCHAR(255), 
    enriched_pickup_1_city   VARCHAR(255), 
    enriched_pickup_1_name   VARCHAR(255), 
    enriched_pickup_1_postal_code   VARCHAR(255), 
    enriched_pickup_1_state   VARCHAR(255), 
    enriched_pickup_2_address_1   VARCHAR(255), 
    enriched_pickup_2_address_2   VARCHAR(255), 
    enriched_pickup_2_city   VARCHAR(255), 
    enriched_pickup_2_name   VARCHAR(255), 
    enriched_pickup_2_postal_code   VARCHAR(255), 
    enriched_pickup_2_state   VARCHAR(255), 
    tender_on_behalf_of   VARCHAR(255), 
    validated_pickup_1_address_1   VARCHAR(255), 
    validated_pickup_1_address_2   VARCHAR(255), 
    validated_pickup_1_city   VARCHAR(255), 
    validated_pickup_1_name   VARCHAR(255), 
    validated_pickup_1_postal_code   VARCHAR(255), 
    validated_pickup_1_state   VARCHAR(255), 
    validated_pickup_2_address_1   VARCHAR(255), 
    validated_pickup_2_address_2   VARCHAR(255), 
    validated_pickup_2_city   VARCHAR(255), 
    validated_pickup_2_name   VARCHAR(255), 
    validated_pickup_2_postal_code   VARCHAR(255), 
    validated_pickup_2_state   VARCHAR(255), 
    inserted_at  TIMESTAMP_NTZ   NOT NULL,
    updated_at  TIMESTAMP_NTZ   NOT NULL,
    CONSTRAINT tender_shipper_address_projection_pkey PRIMARY KEY (tender_id)
)

-- COMMAND ----------

-- MAGIC %md ##tender_split_projection

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.tender_split_projection
(
    tender_id VARCHAR(255) NOT NULL,
    relay_reference_number integer,
    remaining_orders   VARCHAR(255), 
    split_orders   VARCHAR(255), 
    inserted_at  TIMESTAMP_NTZ   NOT NULL,
    updated_at  TIMESTAMP_NTZ   NOT NULL,
    CONSTRAINT tender_split_projection_pkey PRIMARY KEY (tender_id)
)

-- COMMAND ----------

-- MAGIC %md ##tendering_tendered_assigned_carrier

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.tendering_tendered_assigned_carrier
(
    tender_id  VARCHAR(255) NOT NULL,
    assigned_carrier_name   VARCHAR(255), 
    assigned_carrier_scac   VARCHAR(255), 
    relay_reference_number integer,
    shipment_id  STRING,
    tender_on_behalf_of_id   VARCHAR(255), 
    tender_on_behalf_of_type   VARCHAR(255), 
    CONSTRAINT tendering_tendered_assigned_carrier_pkey PRIMARY KEY (tender_id)
)

-- COMMAND ----------

-- MAGIC %md ##caam_list

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.caam_list
(
    dot_number integer,
    mc_number VARCHAR(255),
    carrier_name VARCHAR(255),
    caam_full_name VARCHAR(255),
    caam_id VARCHAR(255),
    cdr VARCHAR(255),
    hubspot_team VARCHAR(255),
    period VARCHAR(255),
    financial_qtr VARCHAR(255),
    start_date Timestamp_ntz,
    period_start Timestamp_ntz,
    today_date Timestamp_ntz,
    big_concat_for_update VARCHAR(255) NOT NULL,
    CONSTRAINT caam_list_dup_pkey PRIMARY KEY (big_concat_for_update)
)

-- COMMAND ----------

-- MAGIC %md ##new_carrier_ownership

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.new_carrier_ownership
(
    cdr_id VARCHAR(255),
    cdr_office VARCHAR(255),
    owned_carrier_name VARCHAR(255),
    mc_number VARCHAR(255),
    dot_number numeric,
    period VARCHAR(255),
    financial_qtr VARCHAR(255),
    concat_primary_key  VARCHAR(255) NOT NULL,
    CONSTRAINT cdr_ownership_test_pkey PRIMARY KEY (concat_primary_key)
)

-- COMMAND ----------

-- MAGIC %md ##new_brokerage_budget

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.new_brokerage_budget
(
    financial_period VARCHAR(255),
    office_name VARCHAR(255),
    rev_budget Decimal(10,2),
    mar_budget Decimal(10,2),
    mar_perc_budget Decimal(10,2),
    volume_budget Decimal(10,2)
)

-- COMMAND ----------

-- MAGIC %md ##aljex_am_by_acct

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.aljex_am_by_acct
(
    customer_name VARCHAR(255),
    account_num numeric,
    salesrep VARCHAR(255),
    office VARCHAR(255),
    am VARCHAR(255)
)

-- COMMAND ----------

-- MAGIC %md ##Financial_Calendar

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.financial_calendar
(
date	date	NOT	NULL,
month	integer	NOT	NULL,
year	integer	NOT	NULL,
year_month	varchar(8)	NOT	NULL,
financial_period_number	integer	NOT	NULL,
financial_quarter_number	integer	NOT	NULL,
financial_year	integer	NOT	NULL,
financial_period	varchar(8)	NOT	NULL,
financial_period_sorting	varchar(8)	NOT	NULL,
financial_period_description	varchar(8)	NOT	NULL,
financial_quarter	varchar(8)	NOT	NULL,
financial_quarter_description	varchar(8)	NOT	NULL
)

-- COMMAND ----------

