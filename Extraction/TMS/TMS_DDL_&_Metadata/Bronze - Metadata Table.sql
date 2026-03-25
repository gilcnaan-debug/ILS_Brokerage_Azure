-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Create Metadata Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Mention the catalog used, getting the catalog name from key vault
-- MAGIC CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
-- MAGIC spark.sql(f"USE CATALOG {CatalogName}")

-- COMMAND ----------

-- MAGIC %md ##Metadata Table Creation

-- COMMAND ----------

CREATE TABLE metadata.MasterMetadata( SourceSystem	STRING,
SourceSecretName	STRING,
TableID	STRING,
SubjectArea	STRING,
SourceDBName	STRING,
SourceSchema	STRING,
SourceTableName	STRING,
LoadType	STRING,
IsActive	STRING,
Frequency	STRING,
StagePath	STRING,
RawPath	STRING,
CuratedPath	STRING,
DWHSchemaName	STRING,
DWHTableName	STRING,
ErrorLogPath	STRING,
LastLoadDateColumn	STRING,
MergeKey	STRING,
DependencyTableIDs	STRING,
LastLoadDateValue	TIMESTAMP,
PipelineEndTime	TIMESTAMP,
PipelineStartTime	TIMESTAMP,
PipelineRunStatus	STRING,
Zone	STRING,
MergeKeyColumn	STRING,
SourceSelectQuery	STRING)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Truncate and load-Aljex tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###equip_mode_table

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
"A1"	,
"Carrier"	,
"ad_hoc_analysis"	,
"Public"	,
"equip_mode_table"	,
"truncate and load"	,
"1"	,
"monthly once"	,
"select equip_mode_type from Public.equip_mode_table")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###aljex_mode_types

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
"A2"	,
"Look-up"	,
"ad_hoc_analysis"	,
"Public"	,
"aljex_mode_types"	,
"truncate and load"	,
"1"	,
"monthly once"	,
"select equipment_type,equipment_mode from Public.aljex_mode_types");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###customers

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
"A3"	,
"Customer"	,
"ad_hoc_analysis"	,
"Public"	,
"customers"	,
"truncate and load"	,
"1"	,
"24hrs"	,
"Select id, zip, web_sync_table_name, web_sync_action, type, total_exposure, status, state, sales_rep, revenue_type, phone3, phone2, phone1, passwd, old_system_id, name, fax, email4, email3, email2, email1, credit_limit, contact4, contact3, contact2, contact1, city, address2, address1 from public.customers");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###aljex_customer_profiles

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
"A4"	,
"Customer"	,
"ad_hoc_analysis"	,
"Public"	,
"aljex_customer_profiles"	,
"truncate and load"	,
"1"	,
"24hrs"	,
"select cust_id,cust_name,sales_rep,state,status,cust_country,address_one,address_two,address_city,address_zip from Public.aljex_customer_profiles");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###aljex_dot_lawson_ref

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
"A5"	,
"Carrier"	,
"ad_hoc_analysis"	,
"Public"	,
"aljex_dot_lawson_ref"	,
"truncate and load"	,
"1"	,
"24hrs"	,
"select aljex_carrier_id,dot_number,lawson_id from Public.aljex_dot_lawson_ref");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###aljex_cred_debt

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
"A6"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"aljex_cred_debt"	,
"truncate and load"	,
"1"	,
"manually upload 1 a week"	,
"select office, pro_num, customer, type_of_ship, ship_date, revenue, expense from Public.aljex_cred_debt");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###aljex_invoice

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
"A8"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"aljex_invoice"	,
"truncate and load"	,
"1"	,
"24hrs"	,
"select invoice_date, pro_number, shipdate from Public.aljex_invoice");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Incremental load-Aljex tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###projection_carrier

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Aljex"	,
"A7"	,
"Carrier"	,
"ad_hoc_analysis"	,
"Public"	,
"projection_carrier"	,
"incremental load"	,
"1"	,
"15 Minutes",
"Select 
id               ,	 
liab_ins_exp     ,	 
comments_1       ,	 
comments_2       ,	 
pay_to_city      ,	 
legal_name       ,	 
carrier_yn       ,	 
pay_to_phone     ,	 
pay_to_contact   ,	 
dot_num          ,	 
power_units      ,	 
liab_insurer     ,	 
cargo_insurer    ,	 
smartway         ,	 
cargo_ins_exp    ,	 
work_comp_amount ,	 
zip              ,	 
email2           ,	 
cargo_ins_policy ,	 
num_vans         ,	 
num_reefs        ,	 
passwd           ,	 
cargo_ins_amount ,	 
city             ,	 
liab_ins_policy  ,	 
created_date     ,	 
pay_to_zip       ,	 
pay_to_zip_4     ,	 
liab_ins_amount  ,	 
work_comp_insurer,	 
gen_liab_insurer ,	 
address1         ,	 
address2         ,	 
name ,	 
state	,
num_flats	,
email1	,
status	,
old_system_id	,
gen_liab_policy	,
gen_liab_exp	,
pay_to_name	,
pay_to_state	,
pay_to_address_1 ,	 
pay_to_address_2 ,	 
scac  ,	 
carrier_type     ,	 
pay_to_account	,
assigned_dispatcher,
work_comp_deduct	,
cargo_ins_deduct	,
bulk_email	,
mc_num	,
work_comp_exp	,
gen_liab_amount	,
work_comp_policy	,
fax	,
phone	,
username	,
gen_liab_deduct	,
liab_ins_deduct	,
web_sync_action	,
web_sync_table_name	,
inserted_at	,
updated_at	,
created_by	
  FROM
    public.projection_carrier
  WHERE
    '{0}' >=  ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"id");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###projection_load_1

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Aljex"	,
"A10"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"projection_load_1"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
" select
        id                        ,
        accessorial1              ,
        accessorial2              ,
        accessorial3              ,
        accessorial4              ,
        accessorial5              ,
        accessorial6              ,
        accessorial7              ,
        accessorial8              ,
        act_disp                  ,
        act_min                   ,
        act_team                  ,
        arrive_cons_code          ,
        arrive_consignee_date     ,
        arrive_consignee_time     ,
        arrive_pickup_code        ,
        arrive_pickup_date        ,
        arrive_pickup_time        ,
        billed_date               ,
        blank199                  ,
        blind                     ,
        carrier_accessorial_id_1  ,
        carrier_accessorial_id_2  ,
        carrier_accessorial_id_3  ,
        carrier_accessorial_id_4  ,
        carrier_accessorial_id_5  ,
        carrier_accessorial_id_6  ,
        carrier_accessorial_id_7  ,
        carrier_accessorial_id_8  ,
        carrier_accessorial1      ,
        carrier_accessorial2      ,
        carrier_accessorial3      ,
        carrier_accessorial4      ,
        carrier_accessorial5      ,
        carrier_accessorial6      ,
        carrier_accessorial7      ,
        carrier_accessorial8      ,
        carrier_fax_num           ,
        carrier_id                ,
        carrier_line_haul         ,
        carrier_phone_num         ,
        carrier_ref_num           ,
        carrier_total_rate        ,
        check_call_date           ,
        check_call_location       ,
        check_call_notes          ,
        check_call_state          ,
        check_call_status         ,
        check_call_time           ,
        check_call_user           ,
        class                     ,
        consignee                 ,
        consignee_address         ,
        consignee_address_line2   ,
        consignee_appointment_date,
        consignee_appointment_note,
        consignee_appointment_time,
        consignee_contact         ,
        consignee_hours           ,
        consignee_phone           ,
        consignee_reference_number,
        consignee_zip_code        ,
        covered_date              ,
        date                      ,
        del_eta_date              ,
        del_eta_time              ,
        delivery_date             ,
        delivery_time             ,
        depart_cons_code          ,
        depart_pickup_code        ,
        description               ,
        dest_city                 ,
        dest_state                ,
        dispatched_city           ,
        dispatched_date           ,
        dispatched_state          ,
        dispatched_time           ,
        division                  ,
        driver_cell_num           ,
        equipment                 ,
        extra_ref_1               ,
        extra_ref_2               ,
        extra_ref_3               ,
        extra_ref_4               ,
        extra_ref_5               ,
        extra_ref_6               ,
        hazmat                    ,
        invoice_total             ,
        key_c_date                ,
        key_c_time                ,
        key_c_user                ,
        key_d_date                ,
        key_d_time                ,
        key_d_user                ,
        key_h_date                ,
        key_h_time                ,
        key_h_user                ,
        key_l_date                ,
        key_l_time                ,
        key_l_user                ,
        key_p_date                ,
        key_p_time                ,
        key_p_user                ,
        key_r_date                ,
        key_r_time                ,
        key_r_user                ,
        key_w_date                ,
        key_w_time                ,
        key_w_user                ,
        load_ref                  ,
        loaded_date               ,
        loaded_time               ,
        manifest_num              ,
        miles                     ,
        mode                      ,
        must_del_date             ,
        must_ship_date            ,
        office                    ,
        origin_city               ,
        origin_state              ,
        Osd                       ,
        oversized                 ,
        pallet_ex                 ,
        pallets_in                ,
        pallets_out               ,
        permits                   ,
        pickup_address            ,
        pickup_address_line2      ,
        pickup_appointment_note   ,
        pickup_appt_date          ,
        pickup_appt_time          ,
        pickup_contact            ,
        pickup_date               ,
        pickup_hours              ,
        pickup_name               ,
        pickup_phone              ,
        pickup_reference_number   ,
        pickup_time               ,
        pickup_zip_code           ,
        pieces                    ,
        pod                       ,
        pod_signature_1           ,
        pod_signature_2           ,
        pod_signature_3           ,
        pod_signature_4           ,
        pod_signature_5           ,
        pod_signature_6           ,
        pod_signature_14          ,
        pod_signature_17          ,
        pod_signature_19          ,
        pod_signature_20          ,
        ps_acct_1                 ,
        ps_acct_2                 ,
        ps_acct_3                 ,
        ps_acct_4                 ,
        ps_acct_5                 ,
        ps_acct_6                 ,
        ps_acct_7                 ,
        ps_acct_8                 ,
        ps_acct_9                 ,
        ps_acct_10                ,
        ps_acct_11                ,
        ps_acct_12                ,
        ps_acct_13                ,
        ps_acct_14                ,
        ps_acct_15                ,
        ps_acct_16                ,
        ps_acct_17                ,
        ps_acct_18                ,
        ps_acct_19                ,
        ps_acct_20                ,
        ps_address_1              ,
        ps_address_2              ,
        ps_address_3              ,
        ps_address_4              ,
        ps_address_5              ,
        ps_address_6              ,
        ps_address_7              ,
        ps_address_8              ,
        ps_address_9              ,
        ps_address_10             ,
        ps_address_11             ,
        ps_address_12             ,
        ps_address_13             ,
        ps_address_14             ,
        ps_address_15             ,
        ps_address_16             ,
        ps_address_17             ,
        ps_address_18             ,
        ps_address_19             ,
        ps_address_20             ,
        ps_appt_date_1            ,
        ps_appt_date_2            ,
        ps_appt_date_3            ,
        ps_appt_date_4            ,
        ps_appt_date_5            ,
        ps_appt_date_6            ,
        ps_appt_date_7            ,
        ps_appt_date_8            ,
        ps_appt_date_9            ,
        ps_appt_date_10           ,
        ps_appt_date_11           ,
        ps_appt_date_12           ,
        ps_appt_date_13           ,
        ps_appt_date_14           ,
        ps_appt_date_15           ,
        ps_appt_date_16           ,
        ps_appt_date_17           ,
        ps_appt_date_18           ,
        ps_appt_date_19           ,
        ps_appt_time_1            ,
        ps_appt_time_2            ,
        ps_appt_time_3            ,
        ps_appt_time_4            ,
        ps_appt_time_5            ,
        ps_appt_time_6            ,
        ps_appt_time_7            ,
        ps_appt_time_8            ,
        ps_appt_time_9            ,
        ps_appt_time_10           ,
        ps_appt_time_11           ,
        ps_appt_time_12           ,
        ps_appt_time_13           ,
        ps_appt_time_14           ,
        ps_appt_time_15           ,
        ps_appt_time_16           ,
        ps_appt_time_17           ,
        ps_appt_time_18           ,
        ps_appt_time_19           ,
        inserted_at               ,
        updated_at
from
        public.projection_load
where
        '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"Id");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###projection_invoicing

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Aljex"	,
"A9"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"projection_invoicing"	,
"incremental load"	,
"1"	,
"24hrs"	,
"Select pro_num, description4, amount10, debits_amount, total, description19, amount3, description18, amount14, hold, amount16, amount1, type, amount7, description10, weight, description16, amount19, description12, office, web_sync_action, description21, amount21, credits_amount, customer_name, description7, act_profit, posted_date, web_sync_table_name, amount20, proj_expense, amount12, cont_trailer_num, proj_ratio, amount2, bal_due_rev, description2, amount11, bal_due_exp, description17, amount8, amount9, ship_date, description6, amount4, description5, description9, amount15, inv_date, amount5, advances_amount, del_date, amount17, act_ratio, description11, act_expense, description20, description13, billto_name, amount18, description3, amount13, description15, proj_rev, amount6, ref_num, description1, description14, salesrep, proj_profit, description8, act_rev, inserted_at, updated_at from public.projection_invoicing where '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"pro_num");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###projection_invoicing_audit

-- COMMAND ----------

INSERT INTO
  metadata.MasterMetadata (
    SourceSystem,
    TableID,
    SubjectArea,
    SourceDBName,
    SourceSchema,
    SourceTableName,
    LoadType,
    IsActive,
    Frequency,
    SourceSelectQuery,
    LastLoadDateColumn,
    LastLoadDateValue,
    MergeKey
  )
VALUES
  (
    "Aljex",
    "A11",
    "Finance & Invoices",
    "ad_hoc_analysis",
    "Public",
    "projection_invoicing_audit",
    "incremental load",
    "1",
    "24hrs",
    "Select event_number, description16, office, web_sync_action, pro_num, cont_trailer_num, amount10, amount3, total, amount16, amount4, ship_date, amount13, act_expense, description14, type, billto_name, description15, amount9, amount7, description3, web_sync_table_name, description9, description20, description5, amount20, description4, amount15, description6, credits_amount, description8, amount8, description18, hold, description11, act_profit, amount18, amount21, customer_name, description21, proj_rev, description12, act_ratio, description10, description2, description19, weight, advances_amount, inv_date, del_date, proj_ratio, description17, amount17, posted_date, description1, proj_profit, proj_expense, description13, bal_due_rev, description7, debits_amount, amount14, act_rev, bal_due_exp, amount6, amount1, amount12, amount11, amount5, amount19, ref_num, amount2, salesrep, inserted_at, updated_at from public.projection_invoicing_audit where '{0}' >= ",
    "updated_at",
    "2020-01-01T01:42:56.465Z",
    "event_number"
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###projection_load_2

-- COMMAND ----------

INSERT INTO
  metadata.MasterMetadata (
    SourceSystem,
    TableID,
    SubjectArea,
    SourceDBName,
    SourceSchema,
    SourceTableName,
    LoadType,
    IsActive,
    Frequency,
    SourceSelectQuery,
    LastLoadDateColumn,
    LastLoadDateValue,
    MergeKey
  )
VALUES
  (
    "Aljex",
    "A10-1",
    "Load",
    "ad_hoc_analysis",
    "Public",
    "projection_load_2",
    "incremental load",
    "1",
    "15 Minutes",
    "select
        id                        ,
        ps_arrive_code_1          ,
        ps_arrive_code_2          ,
        ps_arrive_code_3          ,
        ps_arrive_code_4          ,
        ps_arrive_code_5          ,
        ps_arrive_code_6          ,
        ps_arrive_code_7          ,
        ps_arrive_code_8          ,
        ps_arrive_code_9          ,
        ps_arrive_code_10         ,
        ps_arrive_code_11         ,
        ps_arrive_code_12         ,
        ps_arrive_code_16         ,
        ps_arrive_date_1          ,
        ps_arrive_date_2          ,
        ps_arrive_date_3          ,
        ps_arrive_date_4          ,
        ps_arrive_date_5          ,
        ps_arrive_date_6          ,
        ps_arrive_date_7          ,
        ps_arrive_date_8          ,
        ps_arrive_date_9          ,
        ps_arrive_date_10         ,
        ps_arrive_date_11         ,
        ps_arrive_date_12         ,
        ps_arrive_time_1          ,
        ps_arrive_time_2          ,
        ps_arrive_time_3          ,
        ps_arrive_time_4          ,
        ps_arrive_time_5          ,
        ps_arrive_time_6          ,
        ps_arrive_time_7          ,
        ps_arrive_time_8          ,
        ps_arrive_time_9          ,
        ps_arrive_time_10         ,
        ps_arrive_time_11         ,
        ps_arrive_time_12         ,
        ps_arrive_time_20         ,
        ps_city_1                 ,
        ps_city_2                 ,
        ps_city_3                 ,
        ps_city_4                 ,
        ps_city_5                 ,
        ps_city_6                 ,
        ps_city_7                 ,
        ps_city_8                 ,
        ps_city_9                 ,
        ps_city_10                ,
        ps_city_11                ,
        ps_city_12                ,
        ps_city_13                ,
        ps_city_14                ,
        ps_city_15                ,
        ps_city_16                ,
        ps_city_17                ,
        ps_city_18                ,
        ps_city_19                ,
        ps_company_1              ,
        ps_company_2              ,
        ps_company_3              ,
        ps_company_4              ,
        ps_company_5              ,
        ps_company_6              ,
        ps_company_7              ,
        ps_company_8              ,
        ps_company_9              ,
        ps_company_10             ,
        ps_company_11             ,
        ps_company_12             ,
        ps_company_13             ,
        ps_company_14             ,
        ps_company_15             ,
        ps_company_16             ,
        ps_company_17             ,
        ps_company_18             ,
        ps_company_19             ,
        ps_depart_code_1          ,
        ps_depart_code_2          ,
        ps_depart_code_3          ,
        ps_depart_code_4          ,
        ps_depart_code_5          ,
        ps_depart_code_6          ,
        ps_depart_code_7          ,
        ps_depart_code_8          ,
        ps_depart_code_9          ,
        ps_depart_code_10         ,
        ps_depart_code_11         ,
        ps_depart_code_12         ,
        ps_depart_code_14         ,
        ps_depart_date_1          ,
        ps_depart_date_2          ,
        ps_depart_date_3          ,
        ps_depart_date_4          ,
        ps_depart_date_5          ,
        ps_depart_date_6          ,
        ps_depart_date_7          ,
        ps_depart_date_8          ,
        ps_depart_date_9          ,
        ps_depart_date_10         ,
        ps_depart_date_11         ,
        ps_depart_date_12         ,
        ps_depart_time_1          ,
        ps_depart_time_2          ,
        ps_depart_time_3          ,
        ps_depart_time_4          ,
        ps_depart_time_5          ,
        ps_depart_time_6          ,
        ps_depart_time_7          ,
        ps_depart_time_8          ,
        ps_depart_time_9          ,
        ps_depart_time_10         ,
        ps_depart_time_11         ,
        ps_depart_time_12         ,
        ps_ref_1                  ,
        ps_ref_2                  ,
        ps_ref_3                  ,
        ps_ref_4                  ,
        ps_ref_5                  ,
        ps_ref_6                  ,
        ps_ref_7                  ,
        ps_ref_8                  ,
        ps_ref_9                  ,
        ps_ref_10                 ,
        ps_ref_11                 ,
        ps_ref_12                 ,
        ps_ref_13                 ,
        ps_ref_14                 ,
        ps_ref_15                 ,
        ps_ref_16                 ,
        ps_ref_17                 ,
        ps_ref_18                 ,
        ps_ref_19                 ,
        ps_state_1                ,
        ps_state_2                ,
        ps_state_3                ,
        ps_state_4                ,
        ps_state_5                ,
        ps_state_6                ,
        ps_state_7                ,
        ps_state_8                ,
        ps_state_9                ,
        ps_state_10               ,
        ps_state_11               ,
        ps_state_12               ,
        ps_state_13               ,
        ps_state_14               ,
        ps_state_15               ,
        ps_state_16               ,
        ps_state_17               ,
        ps_state_18               ,
        ps_state_19               ,
        ps_zip_1                  ,
        ps_zip_2                  ,
        ps_zip_3                  ,
        ps_zip_4                  ,
        ps_zip_5                  ,
        ps_zip_6                  ,
        ps_zip_7                  ,
        ps_zip_8                  ,
        ps_zip_9                  ,
        ps_zip_10                 ,
        ps_zip_11                 ,
        ps_zip_12                 ,
        ps_zip_13                 ,
        ps_zip_14                 ,
        ps_zip_15                 ,
        ps_zip_16                 ,
        ps_zip_17                 ,
        ps_zip_18                 ,
        ps_zip_19                 ,
        Ps1                       ,
        Ps2                       ,
        Ps3                       ,
        Ps4                       ,
        Ps5                       ,
        Ps6                       ,
        Ps7                       ,
        Ps8                       ,
        Ps9                       ,
        Ps10                      ,
        Ps11                      ,
        Ps12                      ,
        Ps13                      ,
        Ps14                      ,
        Ps15                      ,
        Ps16                      ,
        Ps17                      ,
        Ps18                      ,
        Ps19                      ,
        quote_number              ,
        ref_num                   ,
        release_date              ,
        sales_rep                 ,
        sales_team                ,
        seal                      ,
        shipper                   ,
        signed_by                 ,
        sls_pct                   ,
        srv_rep                   ,
        status                    ,
        straps_chains             ,
        tag_created_by            ,
        tag_creation_date         ,
        tag_creation_time         ,
        tariff_num                ,
        tarp_required             ,
        tarp_size                 ,
        temp                      ,
        temp_ctrl                 ,
        time                      ,
        Tord                      ,
        trailer_number            ,
        truck_num                 ,
        value                     ,
        web_post                  ,
        web_sync_action           ,
        web_sync_table_name       ,
        weight                    ,
        will_pickup_date          ,
        will_pickup_time          ,
        asg_disp                  ,
        bl_ref                    ,
        carrier_accessorial_rate1 ,
        carrier_accessorial_rate2 ,
        carrier_accessorial_rate3 ,
        carrier_accessorial_rate4 ,
        carrier_accessorial_rate5 ,
        carrier_accessorial_rate6 ,
        carrier_accessorial_rate7 ,
        carrier_accessorial_rate8 ,
        customer_accessorial_rate1,
        customer_accessorial_rate2,
        customer_accessorial_rate3,
        customer_accessorial_rate4,
        customer_accessorial_rate5,
        customer_accessorial_rate6,
        customer_accessorial_rate7,
        customer_accessorial_rate8,
        customer_id               ,
        nmfc_num                  ,
        inserted_at               ,
        updated_at
from
        public.projection_load
where
        '{0}' >= ",
    "updated_at",
    "2020-01-01T01:42:56.465Z",
    "Id"
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Truncate and load-Edge Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###cai_mode_lookup

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
VALUES ("Edge"	,
"E1"	,
"Look-up"	,
"ad_hoc_analysis"	,
"Public"	,
"cai_mode_lookup"	,
"truncate and load"	,
"1"	,
"monthly once",
"select original_mode, new_mode  from Public.cai_mode_lookup");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###cai_salesperson_lookup

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
VALUES ("Edge"	,
"E2"	,
"Look-up"	,
"ad_hoc_analysis"	,
"Public"	,
"cai_salesperson_lookup"	,
"truncate and load"	,
"1"	,
"monthly once",
"select full_name, salesperson_id  from Public.cai_salesperson_lookup");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###edge_mode

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
VALUES ("Edge"	,
"E3"	,
"Look-up"	,
"ad_hoc_analysis"	,
"Public"	,
"edge_mode"	,
"truncate and load"	,
"1"	,
"monthly once",
"select mode,sub_mode,abbrev from Public.edge_mode");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###cai_equipment

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
VALUES ("Edge"	,
"E4"	,
"Look-up"	,
"ad_hoc_analysis"	,
"Public"	,
"cai_equipment"	,
"truncate and load"	,
"1"	,
"monthly once",
"select equip_type_id,mode,sub_mode,abbreviation,description from Public.cai_equipment");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###ultipro_terms

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
VALUES ("Edge"	,
"E5"	,
"HR"	,
"ad_hoc_analysis"	,
"Public"	,
"ultipro_terms"	,
"truncate and load"	,
"1"	,
"24hrs",
"select date_processed, employee_num, employee_name, email_address, last_hire_date, term_date, job_code, job_title, division, dept_code, department, work_location_code, work_location, employee_type, full_or_part_time, supervisor from Public.ultipro_terms");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###ultipro_list

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
VALUES ("Edge"	,
"E6"	,
"HR"	,
"ad_hoc_analysis"	,
"Public"	,
"ultipro_list"	,
"truncate and load"	,
"1"	,
"24hrs",
"select employee_number,employee_name,employment_status,job_code,job_title,company_name,business_name,division_name,region_name,dept_code,department_name,location_code,location_name,location_site,employee_type,full_or_part_time,last_hire_date,original_hire_date,benefits_senority_date,senority_date,senority_years,date_in_job,email from public.ultipro_list");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###cai_data

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
VALUES ("Edge"	,
"E7"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"cai_data"	,
"truncate and load"	,
"1"	,
"24hrs"	,
"Select ship_date, load_num, ops_office, customer, cust_city, cust_state, cust_zip, cust_ref_num, delivery_date, total_picks, total_delvs, status, mode, shipper, shipper_city, shipper_state, shipper_zip, booked_by, salesperson, acct_mgr, miles, carrier, carr_city, carr_state, carr_zip, carr_mc, consignee, consignee_city, consignee_state, consignee_zip, total_expense, total_revenue, total_margin, created_date, booked_date, invoice_date, pickup_appointment, delivery_appointment from public.cai_data");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Truncate and Load -Relay Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##event_name_status

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
VALUES ("Relay"	,
"R1"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"event_name_status"	,
"truncate and load"	,
"1"	,
"24hrs"	,
"select truckload_p_status, shortcut_status from Public.event_name_status");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##customer_profile_external_ids

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
VALUES ("Relay"	,
"R2"	,
"Customer"	,
"ad_hoc_analysis"	,
"Public"	,
"customer_profile_external_ids"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
"select customer_slug  ,aljex_id , lawson_id  ,been_discarded ,external_ids ,has_unmapped_external_ids from Public.customer_profile_external_ids");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##customer_profile_projection

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
VALUES ("Relay"	,
"R3"	,
"Customer"	,
"ad_hoc_analysis"	,
"Public"	,
"customer_profile_projection"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
'Select customer_slug ,customer_name , bill_to_name , bill_to_email , billing_address_1 , billing_address_2 , billing_address_city , billing_address_state , billing_address_zip , contact_name , contact_email , contact_phone , credit_amount_cents , credit_amount_currency , detention_hour_interval_to_start , detention_cost_per_hour_amount_cents , detention_cost_per_hour_currency , invoicing_method , "is_a_4pl?", postal_address_1 , postal_address_2 , postal_address_city , postal_address_state , postal_address_zip , profit_center , published_at , published_by , sales_relay_user_id , sales_relay_user_email , primary_relay_user_id , primary_relay_user_email , first_published_at , most_recent_published_at , status , rate_con_text , notes , lawson_company_id , aljex_id , lawson_id , external_ids , has_unmapped_external_ids , invoicing_customer_profile_id , current_payment_terms, first_published_with_relay_type_lawson_id from public.customer_profile_projection ');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##relay_users

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
VALUES ("Relay"	,
"R4"	,
"Customer"	,
"ad_hoc_analysis"	,
"Public"	,
"relay_users"	,
"truncate and load"	,
"1"	,
"15 Minutes",
'select user_id, "active?", email_address, first_name, full_name, last_name, office_id from Public.relay_users');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##invoicing_customer_profile

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
VALUES ("Relay"	,
"R5"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"invoicing_customer_profile"	,
"truncate and load"	,
"1"	,
"15 Minutes",
'select customer_profile_id, at, auto_invoice_delay, "auto_invoice_enabled?", by, invoicing_method,"show_intermediary_stops?", "stop_tracking_requirement?", document_requirements,data_requirements from Public.invoicing_customer_profile');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##offer_negotiation_invalidated

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
VALUES ("Relay"	,
"R8"	,
"Bidding"	,
"ad_hoc_analysis"	,
"Public"	,
"offer_negotiation_invalidated"	,
"truncate and load"	,
"1"	,
"15 Minutes",
"select offer_id, relay_reference_number, invalidated_by, at, reason, master_carrier_id, customer_slug, carrier_profile_id  from Public.offer_negotiation_invalidated");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##tl_invoice_projection

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
VALUES ("Relay"	,
"R23"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"tl_invoice_projection"	,
"truncate and load"	,
"1"	,
"15 Minutes",
"select event_number, accessorial_amount, customer, fuel_surcharge_amount, invoice_number, invoiced_at, linehaul_amount, relay_reference_number, do_not_invoice, prepared_by, taxes_amount, currency, approval_number, inserted_at, updated_at from Public.tl_invoice_projection")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##tendering_tender_draft_id

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
VALUES ("Relay"	,
"R20"	,
"Look-up"	,
"ad_hoc_analysis"	,
"Public"	,
"tendering_tender_draft_id"	,
"truncate and load"	,
"1"	,
"15 Minutes",
"select tender_draft_id ,relay_reference_number  from Public.tendering_tender_draft_id");

-- COMMAND ----------

-- MAGIC %md ##ap_lawson_appt

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
VALUES ("Relay"	,
"R16"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"ap_lawson_appt"	,
"truncate and load"	,
"1"	,
"15 Minutes",
"select company, due_date, invoice_number, lawson_id, payment_amount, payment_date, payment_num, po_number, record_status  from Public.ap_lawson_appt");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##distance_projection

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
VALUES ("Relay"	,
"R43"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"distance_projection"	,
"truncate and load"	,
"1"	,
"15 Minutes",
"select tender_id, distance_1_to_2, distance_2_to_3, relay_reference_number, total_distance, inserted_at, updated_at  from Public.distance_projection");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##target_audit_lookup

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
VALUES ("Relay"	,
"R24"	,
"Look-up"	,
"ad_hoc_analysis"	,
"Public"	,
"target_audit_lookup"	,
"truncate and load"	,
"1"	,
"24hrs",
"select relay_reference_number ,reconciled_date  from Public.target_audit_lookup");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##tendering_manual_tender_entry_usage

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
VALUES ("Relay"	,
"R21"	,
"Bidding"	,
"ad_hoc_analysis"	,
"Public"	,
"tendering_manual_tender_entry_usage"	,
"truncate and load"	,
"1"	,
"15 Minutes",
'select tender_draft_id, draft_by_user_id, started_at, completed_at, total_time_ms, scratch_pad_notes_used, origin_type  from Public.tendering_manual_tender_entry_usage');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##invoicing_invoice_eligibility

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
VALUES ("Relay"	,
"R29"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"invoicing_invoice_eligibility"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
'select invoice_id,auto_invoicing_delay,"auto_invoicing_enabled?","eligible?" from Public.invoicing_invoice_eligibility');

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##integration_hubtran_vendor_invoice_approved

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
VALUES ("Relay"	,
"R33"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"integration_hubtran_vendor_invoice_approved"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
"select event_id, booking_id, approved_by, event_at, invoice_date, invoice_number, proof_of_delivery_seen, proof_of_delivery_url, total_approved_amount_to_pay, vendor_id, date_to_pay, notes, currency  from Public.integration_hubtran_vendor_invoice_approved ");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##planning_team_driver_marked

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
VALUES ("Relay"	,
"R12"	,
""	,
"ad_hoc_analysis"	,
"Public"	,
"planning_team_driver_marked"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
'select relay_reference_number, plan_id, "team_driver?" from Public.planning_team_driver_marked');

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R14"	,
"Carrier"	,
"ad_hoc_analysis"	,
"Public"	,
"carrier_nomination_and_onboarding_projection"	,
"incremental load"	,
"1"	,
"24hrs"	,
'Select dot_number, approval, "approved?", current_status, denial, "denied?", "duplicated?", initial_nomination, master_carrier_id, name, nomination_count, "onboard_prohibited?", onboard_prohibition, "onboarded?", onboarding,
carrier_profile_id, inserted_at, updated_at from public. carrier_nomination_and_onboarding_projection where "{0}" >= ',
"updated_at",
"2020-01-01T01:42:56.465Z",
"dot_number");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##invoicing_credits

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
VALUES ("Relay"	,
"R7"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"invoicing_credits"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
"select event_id  , invoice_number , invoice_id  , relay_reference_number, total_amount  , currency, credited_at  , reason, issued_by_user_id  , issued_by_name  , taxes_amount from Public.invoicing_credits");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###tracking_in_transit_reason_code

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
VALUES ("Relay"	,
"R9"	,
""	,
"ad_hoc_analysis"	,
"Public"	,
"tracking_in_transit_reason_code"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
"select relay_reference_number,reason_code from Public.tracking_in_transit_reason_code ");

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##tendering_origin_destination

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
VALUES ("Relay"	,
"R38"	,
"Shipments"	,
"ad_hoc_analysis"	,
"Public"	,
"tendering_origin_destination"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
'Select relay_reference_number, tender_id, origin_address_1, origin_address_2, origin_administrative_region, origin_city, origin_country_code, shipper_id, shipper_name, destination_address_1, destination_address_2, destination_city, destination_administrative_region, destination_country_code, receiver_id, receiver_name, "cancelled?" From public.tendering_origin_destination');

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##invoicing_invoice_charges

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
VALUES ("Relay"	,
"R51"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"invoicing_invoice_charges"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
'select charge_id, invoice_id, relay_reference_number, charge_code, charge_by, prepared_at,"voided?","estimated?", amount, amount_float, currency from Public.invoicing_invoice_charges');

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ##planning_assignment_log

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
VALUES ("Relay"	,
"R54"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"planning_assignment_log"	,
"truncate and load"	,
"1",
"15 Minutes"	,
"select event_id, action_at,action_by,action_involving,assignment_action,assignment_id,plan_id,relay_reference_number from Public.planning_assignment_log");

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##booking_carrier_planned

-- COMMAND ----------

INSERT INTO
  metadata.MasterMetadata (
    SourceSystem,
    TableID,
    SubjectArea,
    SourceDBName,
    SourceSchema,
    SourceTableName,
    LoadType,
    IsActive,
    Frequency,
    SourceSelectQuery
  )
VALUES
  (
    "Relay",
    "R17",
    "Look-up",
    "ad_hoc_analysis",
    "Public",
    "booking_carrier_planned",
    "truncate and load",
    "1",
    "15 Minutes",
    "select relay_reference_number, carrier_id, managed_load_id, styimed  from Public.booking_carrier_planned "
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Incremental Load - Relay Tables

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ##shippers

-- COMMAND ----------

INSERT INTO
  metadata.MasterMetadata (
    SourceSystem,
    TableID,
    SubjectArea,
    SourceDBName,
    SourceSchema,
    SourceTableName,
    LoadType,
    IsActive,
    Frequency,
    SourceSelectQuery,
    LastLoadDateColumn,
    LastLoadDateValue,
    MergeKey
  )
VALUES
  (
    "Relay",
    "R10",
    "Customer",
    "ad_hoc_analysis",
    "Public",
    "shippers",
    "incremental load",
    "1",
    "15 Minutes",
    'select uuid, address1, address2, city, latitude, longitude, name, phone_number, state_code, time_zone, utc_offset, "validated?", zip_code, created_at, inserted_at, updated_at  from Public.shippers where "{0}" >=  ',
    "updated_at",
    "2020-01-01T01:42:56.465Z",
    "uuid"
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##plan_combination_projection

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R6"	,
""	,
"ad_hoc_analysis"	,
"Public"	,
"plan_combination_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select plan_combination_id  ,combined_plan_one_id  ,combined_plan_two_id ,is_combined,relay_reference_number_one,relay_reference_number_two,resulting_plan_id ,inserted_at ,updated_at from Public.plan_combination_projection where '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"plan_combination_id");

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##receivers

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R11"	,
"Customer"	,
"ad_hoc_analysis"	,
"Public"	,
"receivers"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
'select uuid, address1, address2, city, latitude, longitude, name, phone_number, state_code, time_zone, utc_offset, "validated?", zip_code, created_at, inserted_at, updated_at  from Public.receivers where "{0}" >= ',
"updated_at",
"2020-01-01T01:42:56.465Z",
"uuid");

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##ltl_invoice_projection

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R13"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"ltl_invoice_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select event_number, invoice_number, invoiced_amount, invoiced_at, relay_reference_number, inserted_at, updated_at  from Public.ltl_invoice_projection where '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"event_number");

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##offer_negotiation_reflected

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R18"	,
"Bidding"	,
"ad_hoc_analysis"	,
"Public"	,
"offer_negotiation_reflected"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"Select offer_id, authority, authority_id, carrier_name, customer, master_carrier_id, notes, offered_by, offered_by_name, offered_system, offered_at, relay_reference_number, total_rate_currency, total_rate_in_pennies, 
carrier_profile_id, inserted_at, updated_at from public.offer_negotiation_reflected where '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"offer_id");

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##carrier_projection

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R15"	,
"Carrier"	,
"ad_hoc_analysis"	,
"Public"	,
"carrier_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"Select carrier_id, approved_at, approved_by, carrier_name, 
current_cargo_insurance_amount, denied_at, denied_by, dot_number, duplicated_reason, is_overridden, master_carrier_id, 
nominated_at, nominated_by, nominated_email_address, onboarded_at, overridden_cargo_insurance_amount, overridden_cargo_insurance_currency,
prohibited_at, prohibited_by, prohibited_reason, reported_cargo_insurance_amount, reported_cargo_insurance_currency, status, cargo_expiration_date,
default_rate_con_recipients, carrier_profile_id, inserted_at, updated_at from public.carrier_projection where '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"carrier_id");

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ##planning_note_captured

-- COMMAND ----------

INSERT INTO
  metadata.MasterMetadata (
    SourceSystem,
    TableID,
    SubjectArea,
    SourceDBName,
    SourceSchema,
    SourceTableName,
    LoadType,
    IsActive,
    Frequency,
    SourceSelectQuery,
    LastLoadDateColumn,
    LastLoadDateValue,
    MergeKey
  )
VALUES
  (
    "Relay",
    "R19",
    "Load",
    "ad_hoc_analysis",
    "Public",
    "planning_note_captured",
    "incremental load",
    "1",
    "15 Minutes",
    "select plan_id, internal_note, note_id, planning_note, relay_reference_number, inserted_at, updated_at 
from Public.planning_note_captured where '{0}' >= ",
    "updated_at",
    "2020-01-01T01:42:56.465Z",
    "plan_id"
  );

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ##sourcing_max_buy_v2

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R22"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"sourcing_max_buy_v2"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select relay_reference_number, currency, max_buy, notes, pricing_id, set_at, set_by, set_by_name, inserted_at, updated_at from Public.sourcing_max_buy_v2 where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"relay_reference_number");

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##planning_current_assignment

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R25"	,
""	,
"ad_hoc_analysis"	,
"Public"	,
"planning_current_assignment"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select assignment_id, assigned_at, assigned_by, assigned_to,
 is_open, plan_id, relay_reference_number, inserted_at, updated_at from Public.planning_current_assignment where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"assignment_id");

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ##carrier_load_money_projection

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R26"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"carrier_load_money_projection"	,
"incremental load"	,
"1"	,
"24hrs"	,
"select charge_id, carrier_vendor_id, charge_code, initiated_by, initiated_from, is_locked, is_voided, load_number, locked_by, locked_from, 
total_amount_amount, total_amount_currency, voided_by, voided_from, inserted_at, updated_at  from Public.carrier_load_money_projection where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"charge_id");

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ##delivery_projection

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R27"	,
"Shipments",
"delivery_projection"	,
"Public"	,
"delivery_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select delivery_id, scheduled_from, location_state, weight_to_deliver_amount, pieces_to_deliver_packaging_type, receiver_name, location_name, is_drop_trailer, plan_id, location_phone_number, location_city, weight_to_deliver_unit, receiver_id, appointment_time_local, location_country_code, pieces_to_deliver_count, shipping_units_to_deliver_count, volume_to_deliver_unit, delivery_numbers, relay_reference_number, appointment_reference, location_address_2, scheduled_at, volume_to_deliver_amount, sequence_number, location_address_1, appointment_date, shipping_units_to_deliver_type, is_appointment_scheduled, location_type, location_postal_code, 
location_external_id, scheduled_by, scheduled_booking_id, action_needed_booking_id, inserted_at,
 updated_at from public.delivery_projection where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"delivery_id");

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ##pickup_projection

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R28"	,
"Shipments",
"delivery_projection"	,
"Public"	,
"pickup_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select schedule_id, location_country_code, requested_appointment_time, scheduled_by, ready_date, shipper_name, appointment_datetime, weight_to_pickup_amount, appointment_reference, volume_to_pickup_unit, location_state, location_postal_code, location_type, sequence_number, volume_to_pickup_amount, weight_to_pickup_unit, shipment_id, shipping_units_to_pickup_count, ready_time, shipping_units_to_pickup_type, pieces_to_pickup_count, shipper_id, pickup_numbers, scheduled_from, location_phone_number, pieces_to_pickup_packaging_type, location_city, plan_id, location_address_2, relay_reference_number, location_address_1, scheduled_at, location_external_id, is_appointment_scheduled, 
location_name, booking_id, scheduling_action_booking_id, order_numbers, inserted_at,
 updated_at from public.pickup_projection where '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"schedule_id");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R30"	,
""	,
"ad_hoc_analysis"	,
"Public"	,
"tracking_last_reason_code"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select relay_reference_number, reason_code, inserted_at, updated_at  from Public.tracking_last_reason_code where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"relay_reference_number");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R31"	,
"Bidding"	,
"ad_hoc_analysis"	,
"Public"	,
"tendering_service_line"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select relay_reference_number, tender_id, service_line, service_line_type, equipment_type, inserted_at, updated_at from Public.tendering_service_line where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"relay_reference_number");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R32"	,
"Bidding"	,
"ad_hoc_analysis"	,
"Public"	,
"tendering_planned_distance"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select tender_id, planned_distance_amount, planned_distance_unit, relay_reference_number, shipment_id, tender_on_behalf_of_id, tender_on_behalf_of_type, inserted_at, updated_at from Public.tendering_planned_distance where '{0}' >=",
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
Frequency	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R35"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"truckload_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"Select truck_load_thing_id, booking_id, last_update_by, last_update_date_time, last_update_event_name, order_numbers, relay_reference_number, shipment_ids, status, truck_number, trailer_number, eta_to_shipper, tender_on_behalf_of_id, tender_on_behalf_of_type, driver_phone_number, tracking_contact_name, tracking_contact_phone_number, tracking_contact_email, last_update_location, last_update_location_time, last_update_note, driver_name, inserted_at, updated_at from public.truckload_projection where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"truck_load_thing_id");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R36"	,
"Shipments"	,
"ad_hoc_analysis"	,
"Public"	,
"canonical_plan_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select plan_id, relay_reference_number, status, tender_id, mode, inserted_at, updated_at from Public.canonical_plan_projection where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"plan_id");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R37"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"rolled_loads"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select plan_id, new_pickup_date, reason_code, relay_reference_number, rolled_by, shipment_id, tender_pickup_date, inserted_at, updated_at  from Public.rolled_loads where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"plan_id");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R39"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"hain_tracking_accuracy_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select relay_reference_number, customer, expected_delivery_date, expected_ship_date, rolled_count, delivery_appointment, delivery_appointment_when_delivered, delivery_by_date_at_tender, ready_date_at_tender, inserted_at, updated_at from Public.hain_tracking_accuracy_projection where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"relay_reference_number");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R40"	,
"Carrier"	,
"ad_hoc_analysis"	,
"Public"	,
"booking_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"Select booking_id, booked_carrier_name, total_pallets, truck_number, tonu_customer_money_currency, reserved_carrier_id, booked_carrier_id, cargo_value_amount, empty_city_state, is_committed, low_value_captured_at, status, empty_date_time, total_weight, reserved_carrier_name, driver_phone_number, ready_date, total_pieces, rate_con_sent_by, tonu_carrier_money_currency, ready_time, tonu_rate_con_recipients, trailer_number, rolled_by, relay_reference_number, booked_by_name, driver_info_captured_by, receiver_city, total_miles, tonu_carrier_money_amount, booked_at, booked_total_carrier_rate_currency, tonu_customer_money_amount, receiver_state, reservation_cancelled_by, is_tonu_issued, reserved_by, first_shipper_city, rate_cons_sent_count, receiver_name, reserved_at, tonu_issued_by, driver_name, first_shipper_state, second_shipper_state, rate_con_recipients, low_value_captured_by, rolled_at, first_shipper_name, is_must_check_high_value, second_shipper_city, booked_total_carrier_rate_amount, stop_count, cargo_value_currency, second_shipper_name, last_rate_con_sent_time, bounced_at, bounced_by, loading_type, first_shipper_zip, first_shipper_id, second_shipper_zip, second_shipper_id, receiver_id, receiver_zip, first_intermediary_receiver_id, first_intermediary_receiver_name, first_intermediary_receiver_city, first_intermediary_receiver_state, first_intermediary_receiver_zip, second_intermediary_receiver_id, second_intermediary_receiver_name, second_intermediary_receiver_city, second_intermediary_receiver_state, second_intermediary_receiver_zip, pickup_count, delivery_count, tender_on_behalf_of_id, tender_on_behalf_of_type, booked_by_user_id, inserted_at, updated_at from public.booking_projection where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"booking_id");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R42"	,
"Customer"	,
"ad_hoc_analysis"	,
"Public"	,
"customer_distance_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select tender_id, command_builder_url, customer, customer_distance_in_miles, is_cancelled, relay_reference_number, inserted_at, updated_at from Public.customer_distance_projection where '{0}' >=",
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
Frequency	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R44"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"tendering_orders_product_descriptions"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select tender_id, orders_product_descriptions, relay_reference_number, inserted_at, updated_at from Public.tendering_orders_product_descriptions where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"tender_id");

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R45"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"tender_reference_numbers_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select tender_id, cancelled_by, is_cancelled, is_split, nfi_pro_number, original_shipment_id, order_numbers, po_numbers, relay_reference_number, target_shipment_id, tender_on_behalf_of, cancelled_at, inserted_at, updated_at from Public.tender_reference_numbers_projection where '{0}' >=",
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
Frequency	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R46"	,
"Carrier"	,
"ad_hoc_analysis"	,
"Public"	,
"big_export_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"Select load_number, carrier_accessorial_expense, carrier_fuel_expense, carrier_id, carrier_linehaul_expense, carrier_name, carrier_pro_number, charges, consignee_city, consignee_name, consignee_state, consignee_zip, customer_id, delivered_date, delivery_number, dispatch_status, invoice_date, miles, pallet_count, pickup_city, pickup_name, pickup_number, pickup_state, pickup_zip, piece_count, po_number, projected_expense, ship_date, weight, inserted_at, updated_at from public. big_export_projection where '{0}' >=",
"updated_at",
"2020-01-01T01:42:56.465Z",
"load_number");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R47"	,
"Shipments"	,
"ad_hoc_analysis"	,
"Public"	,
"tendering_acceptance"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
'select tender_id, "accepted?", accepted_at, accepted_by, must_respond_by, relay_reference_number, shipment_id, tender_on_behalf_of_id, tender_on_behalf_of_type, tendered_at, inserted_at, updated_at from Public.tendering_acceptance where "{0}">=',
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
Frequency	,
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R48"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"vendor_transaction_projection"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
'select charge_id, amount, booking_id, charge_code, currency, "finalized?", finalized_at, incurred_at, relay_reference_number, vendor_id, vendor_transaction_id, "voided?", voided_at, inserted_at, updated_at  from Public.vendor_transaction_projection where "{0}" >=',
"updated_at",
"2020-01-01T01:42:56.465Z",
"charge_id");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R49"	,
"Shipments"	,
"ad_hoc_analysis"	,
"Public"	,
"planning_stop_schedule"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
'Select stop_id, appointment_datetime, stop_type, volume_unit, address_1, administrative_region, window_end_datetime, ready_date, relay_reference_number, plan_id, schedule_id, window_start_datetime, volume_amount, address_2, "removed?", weight_amount, postal_code, phone_number, scheduled_at, order_numbers, shipment_id, sequence_number, pieces_count, cleared_at, scheduled_by, weight_unit, pieces_packaging_type, late_reason, cleared_by, country_code, stop_name, reference_numbers, schedule_reference, locality, shipping_units_type, schedule_type, shipping_units_count, inserted_at, updated_at from public.planning_stop_schedule where "{0}" >=',
"updated_at",
"2020-01-01T01:42:56.465Z",
"stop_id");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R50"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"canonical_stop"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
'select stop_id, address_1, address_2, administrative_region, booking_id, country_code, facility_id, facility_name, in_date_time, locality, out_date_time, postal_code, stop_reference_numbers, stop_type, "stale?", reason_code, inserted_at, updated_at from Public.canonical_stop where "{0}" >=',
"updated_at",
"2020-01-01T01:42:56.465Z",
"stop_id");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R52"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"moneying_billing_party_transaction"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
'Select charge_id, amount, billing_party_id, billing_party_transaction_id, charge_code, currency,"estimated?", incurred_at, incurred_by,"invoiceable?","invoiced?", invoiced_at, invoiced_by, relay_reference_number, "voided?", voided_at, voided_by, inserted_at, updated_at from public.moneying_billing_party_transaction  where "{0}" >=',
"updated_at",
"2020-01-01T01:42:56.465Z",
"charge_id");

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R53"	,
""	,
"ad_hoc_analysis"	,
"Public"	,
"tracking_activity_v2"	,
"incremental load"	,
"1"	,
"15 Minutes"	,
"select event_id, at, by_system_id, by_type, by_user_id, truck_load_thing_id, type, inserted_at, updated_at from Public.tracking_activity_v2 where '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"event_id");

-- COMMAND ----------

-- MAGIC %md ##customer_money

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R34"	,
"Finance & Invoices"	,
"ad_hoc_analysis"	,
"Public"	,
"customer_money"	,
"incremental load"	,
"1"	,
"24hrs"	,
"select relay_reference_number, fuel_surcharge_amount, linehaul_amount, accessorial_amount, customer, inserted_at, updated_at  from Public.customer_money where '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"relay_reference_number");

-- COMMAND ----------

-- MAGIC %md ##carrier_money_projection

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Relay"	,
"R41"	,
"Load"	,
"ad_hoc_analysis"	,
"Public"	,
"carrier_money_projection"	,
"incremental load"	,
"1"	,
"24hrs"	,
"select booking_id, is_tonu, relay_reference_number, status, total_carrier_rate_amount, inserted_at, updated_at from Public.carrier_money_projection where '{0}' >= ",
"updated_at",
"2020-01-01T01:42:56.465Z",
"booking_id");

-- COMMAND ----------

-- MAGIC %md ##Truncate and load - Lookup Tables

-- COMMAND ----------

-- MAGIC %md ## new_office_lookup

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
VALUES ("Lookup"	,
"L1"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"new_office_lookup"	,
"truncate and load"	,
"1",
"15 Minutes"	,
"select old_office, new_office from Public.new_office_lookup");

-- COMMAND ----------

-- MAGIC %md ##aljex_user_report_listing

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
VALUES ("Lookup"	,
"L2"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"aljex_user_report_listing"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
"select aljex_id, company, pnl_code,sales_rep, full_name, scr_id, ultipro_name from Public.aljex_user_report_listing");

-- COMMAND ----------

-- MAGIC %md ##office_to_cost_center_lookup

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
VALUES ("Lookup"	,
"L3"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"office_to_cost_center_lookup"	,
"truncate and load"	,
"1"	,
"15 Minutes"	,
"select cost_center, location, office_id from Public.office_to_cost_center_lookup");

-- COMMAND ----------

-- MAGIC %md ##canada_conversions

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
VALUES ("Lookup"	,
"L4"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"canada_conversions"	,
"truncate and load"	,
"1",
"15 Minutes"	,
"select ship_date,conversion,us_to_cad from Public.canada_conversions");

-- COMMAND ----------

-- MAGIC %md ##market_lookup

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
VALUES ("Lookup"	,
"L5"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"market_lookup"	,
"truncate and load"	,
"1",
"15 Minutes"	,
"select pickup_zip, market,market_state from Public.market_lookup");

-- COMMAND ----------

-- MAGIC %md ##customer_lookup

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
VALUES ("Lookup"	,
"L6"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"customer_lookup"	,
"truncate and load"	,
"1",
"15 Minutes"	,
"select aljex_customer_name, master_customer_name, customer_ref_number,aljex_customer_id from Public.customer_lookup");

-- COMMAND ----------

-- MAGIC %md ##cad_currency

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
VALUES ("Lookup"	,
"L7"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"cad_currency"	,
"truncate and load"	,
"1",
"15 Minutes"	,
"select office, pro_number, currency_type from Public.cad_currency");

-- COMMAND ----------

-- MAGIC %md ##canada_carriers

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
VALUES ("Lookup"	,
"L8"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"canada_carriers"	,
"truncate and load"	,
"1",
"15 Minutes"	,
"select canada_dot from Public.canada_carriers");

-- COMMAND ----------

-- MAGIC %md ##new_office_lookup_dup

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
VALUES ("Lookup"	,
"L9"	,
"Lookup"	,
"ad_hoc_analysis"	,
"Public"	,
"new_office_lookup_dup"	,
"truncate and load"	,
"1",
"15 Minutes"	,
"select old_office_dup, new_office_dup from Public.new_office_lookup_dup");

-- COMMAND ----------

-- MAGIC %md ##Incremental Load- Hubspot Tables

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
LastLoadDateColumn,
MergeKey,
LastLoadDateValue,
SourceSelectQuery
)
VALUES ("HubSpot"	,
"H1"	,
"HubSpot"	,
"HubSpot_API"	,
"api_response.results"	,
"HubSpot_Deals"	,
"incremental load"	,
"1"	,
"daily"	,
"hs_lastmodifieddate",
"hs_object_id",
"2020-01-01T01:42:56.465Z",
"'hubspot_owner_id','amount','closedate','createdate','dealname','dealstage','hs_lastmodifieddate','pipeline'");

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
LastLoadDateColumn,
MergeKey,
LastLoadDateValue,
SourceSelectQuery
)
VALUES ("HubSpot"	,
"H2"	,
"HubSpot"	,
"HubSpot_API"	,
"api_response.results"	,
"HubSpot_Companies"	,
"incremental load"	,
"1"	,
"daily"	,
"hs_lastmodifieddate",
"hs_object_id",
"2020-01-01T01:42:56.465Z",
"hubspot_owner_id','city','country','createdate','domain','hs_lastmodifieddate','industry','phone'");

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
LastLoadDateColumn,
MergeKey,
LastLoadDateValue,
SourceSelectQuery
)
VALUES ("HubSpot"	,
"H3"	,
"HubSpot"	,
"HubSpot_API"	,
"api_response.results"	,
"HubSpot_Contacts"	,
"incremental load"	,
"1"	,
"daily"	,
"lastmodifieddate",
"hs_object_id",
"2020-01-01T01:42:56.465Z",
"'hubspot_object_id','company','email','createdate','firstname','industry','lastmodifieddate','lastname','phone','state','website'");

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
LastLoadDateColumn,
MergeKey,
LastLoadDateValue,
SourceSelectQuery
)
VALUES ("HubSpot"	,
"H4"	,
"HubSpot"	,
"HubSpot_API"	,
"api_response.results"	,
"HubSpot_Owners"	,
"incremental load"	,
"1"	,
"daily"	,
"updated_at",
"id",
"2020-01-01T01:42:56.465Z",
"'id','firstname','email','lastname','userID','createdAt','updatedAt','archived','teamsId','teamsName','teamsPrimary'");

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
LastLoadDateColumn,
MergeKey,
LastLoadDateValue,
SourceSelectQuery
)
VALUES ("HubSpot"	,
"H5"	,
"HubSpot"	,
"HubSpot_API"	,
"api_response.results"	,
"HubSpot_Emails"	,
"incremental load"	,
"1"	,
"daily"	,
"hs_lastmodifieddate",
"hs_object_id",
"2020-01-01T01:42:56.465Z",
"'hs_object_id','hs_email_sender_email','hs_email_direction','hs_email_sender_firstname','hs_email_sender_lastname','hs_email_status','hs_email_subject','hs_email_text','hs_email_to_email','hs_email_to_firstname','hs_email_to_lastname','hs_lastmodifieddate','hs_timestamp','hubspot_owner_id'");

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
LastLoadDateColumn,
MergeKey,
LastLoadDateValue,
SourceSelectQuery
)
VALUES ("HubSpot"	,
"H52"	,
"HubSpot"	,
"HubSpot_API"	,
"api_response.results"	,
"HubSpot_Emails"	,
"incremental load"	,
"1"	,
"daily"	,
"hs_lastmodifieddate",
"hs_object_id",
"2023-12-25T07:31:36.163Z",
"'hs_object_id','hs_email_sender_email','hs_email_direction','hs_email_sender_firstname','hs_email_sender_lastname','hs_email_status','hs_email_subject','hs_email_text','hs_email_to_email','hs_email_to_firstname','hs_email_to_lastname','hs_lastmodifieddate','hs_timestamp','hubspot_owner_id'");

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
LastLoadDateColumn,
MergeKey,
LastLoadDateValue,
SourceSelectQuery
)
VALUES ("HubSpot"	,
"H6"	,
"HubSpot"	,
"HubSpot_API"	,
"api_response.results"	,
"HubSpot_Meetings"	,
"incremental load"	,
"1"	,
"daily"	,
"hs_lastmodifieddate",
"hs_object_id",
"2020-01-01T01:42:56.465Z",
"'hs_internal_meeting_notes','hs_meeting_body','hs_meeting_end_time','hs_meeting_external_url','hs_meeting_location','hs_meeting_outcome','hs_meeting_start_time','hs_meeting_title','hs_timestamp','hubspot_owner_id'");

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
LastLoadDateColumn,
MergeKey,
LastLoadDateValue,
SourceSelectQuery
)
VALUES ("HubSpot"	,
"H7"	,
"HubSpot"	,
"HubSpot_API"	,
"api_response.results"	,
"HubSpot_Notes"	,
"incremental load"	,
"1"	,
"daily"	,
"hs_lastmodifieddate",
"hs_object_id",
"2020-01-01T01:42:56.465Z",
"'hs_createdate','hs_lastmodifieddate','hs_note_body','hs_object_id','hs_timestamp','hubspot_owner_id'");

-- COMMAND ----------

-- MAGIC %md ##slack_actions_2

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
SourceSelectQuery,
LastLoadDateColumn,
LastLoadDateValue,
MergeKey
)
VALUES ("Slack"	,
"S1"	,
"Slack"	,
"ad_hoc_analysis"	,
"Public"	,
"slack_actions_2"	,
"incremental load"	,
"1"	,
"24hrs"	,
'select action_id, action_type, user_id, channel_name, channel_type, datetime,
"UpdatedDateTime" from Public.slack_actions_2 where "{0}" >= ',
"datetime",
"2016-01-01T23:55:39Z",
"action_id");

-- COMMAND ----------

