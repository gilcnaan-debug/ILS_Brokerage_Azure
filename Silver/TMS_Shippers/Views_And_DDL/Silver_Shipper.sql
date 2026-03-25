-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## View

-- COMMAND ----------

CREATE OR REPLACE VIEW Silver.VW_Silver_Shipper AS
  WITH aljex_loads AS (
  SELECT 
    Customer_Master,
    Customer_Name,
    Lawson_ID,
    Load_Number AS load_id,
    Customer_New_Office AS office,
    Customer_ID AS aljex_customer_id,
    NULL AS relay_customer_id,
    Customer_Rep AS tms_rep_name,
    Customer_Rep_ID AS aljex_rep_id,
    NULL AS relay_rep_id,
    'aljex' AS source,
    Ship_Date AS load_date,
    tendered_date,
    booked_date,
    Actual_Delivered_Date
  FROM silver.silver_aljex
  WHERE Is_Deleted = 0 
    AND load_status NOT LIKE '%VOID%' 
    AND load_status <> 'HOLD'
),
relay_loads AS (
  SELECT
    Customer_Master,
    Customer_Name,
    Lawson_ID,
    Load_ID AS load_id,
    Customer_New_Office AS office,
    NULL AS aljex_customer_id,
    Customer_ID AS relay_customer_id,
    Customer_Rep AS tms_rep_name,
    NULL AS aljex_rep_id,
    Customer_Rep_ID AS relay_rep_id,
    'relay' AS source,
    Ship_Date AS load_date,
    tendered_date,
    booked_date,
    Actual_Delivered_Date
  FROM silver.silver_relay
  WHERE Is_Deleted = 0 
    AND load_status NOT LIKE '%VOID%' 
    AND load_status <> 'HOLD'
),
aljex_customer AS (
  SELECT
    lawson_id,
    aljex_customer_id
  FROM (
    SELECT
      lawson_id,
      aljex_customer_id,
      COUNT(*) AS total_loads,
      MAX(load_date) AS last_load_date,
      MAX(tendered_date) AS last_tendered_date,
      MAX(booked_date) AS last_booked_date,
      MAX(actual_delivered_date) AS last_delivered_date,
      DENSE_RANK() OVER (
        PARTITION BY lawson_id
        ORDER BY COUNT(*) DESC, MAX(load_date) DESC, MAX(tendered_date) DESC, MAX(booked_date) DESC, MAX(actual_delivered_date) DESC
      ) AS rn
    FROM aljex_loads
    WHERE lawson_id IS NOT NULL
    GROUP BY lawson_id, aljex_customer_id
  ) t
  WHERE rn = 1
),
relay_customer AS (
  SELECT
    lawson_id,
    relay_customer_id
  FROM (
    SELECT
      lawson_id,
      relay_customer_id,
      COUNT(*) AS total_loads,
      MAX(load_date) AS last_load_date,
      MAX(tendered_date) AS last_tendered_date,
      MAX(booked_date) AS last_booked_date,
      MAX(actual_delivered_date) AS last_delivered_date,
      DENSE_RANK() OVER (
        PARTITION BY lawson_id
        ORDER BY COUNT(*) DESC, MAX(load_date) DESC, MAX(tendered_date) DESC, MAX(booked_date) DESC, MAX(actual_delivered_date) DESC
      ) AS rn
    FROM relay_loads
    WHERE lawson_id IS NOT NULL
    GROUP BY lawson_id, relay_customer_id
  ) t
  WHERE rn = 1
),
unified_loads AS (
  SELECT * FROM aljex_loads
  UNION ALL
  SELECT * FROM relay_loads
),
customer_load_date_details AS (
SELECT
  customer_master,
  MAX(load_date) AS last_load_date
FROM unified_loads
WHERE lawson_id IS NULL
GROUP BY customer_master

UNION

SELECT
  customer_name AS customer_master,
  MAX(load_date) AS last_load_date
FROM unified_loads
WHERE lawson_id IS NULL
GROUP BY customer_name
),
load_date_details AS (
  SELECT
    lawson_id,
    MAX(load_date) AS last_load_date
  FROM unified_loads
  where lawson_id is not null
  GROUP BY lawson_id
),
aljex_rep_id AS (
  SELECT
    ul.tms_rep_name,
    CASE 
      WHEN UPPER(ul.tms_rep_name) = 'MICHELLE DENTON' THEN 'MDEN1'
      ELSE ar.aljex_id
    END AS rep_aljex_id
  FROM (
    SELECT DISTINCT tms_rep_name FROM unified_loads WHERE tms_rep_name IS NOT NULL
  ) ul
  LEFT JOIN bronze.aljex_user_report_listing ar
    ON UPPER(ul.tms_rep_name) = UPPER(ar.full_name) AND ar.aljex_id <> 'repeat'
),
relay_rep_id AS (
  SELECT
    ul.tms_rep_name,
    rr.user_id AS rep_relay_id
  FROM (
    SELECT DISTINCT tms_rep_name FROM unified_loads WHERE tms_rep_name IS NOT NULL
  ) ul 
  LEFT JOIN bronze.relay_users rr
    ON UPPER(ul.tms_rep_name) = UPPER(rr.full_name) AND rr.`active?` = TRUE
),
rep_ids AS (
  SELECT distinct
    a.tms_rep_name,
    nullif(a.rep_aljex_id, "") as rep_aljex_id,
    r.rep_relay_id
  FROM aljex_rep_id a
  LEFT JOIN relay_rep_id r
    ON UPPER(a.tms_rep_name) = UPPER(r.tms_rep_name)
),
customer_lawson AS (
  select distinct * from
  (SELECT 
    MAX_BY(UPPER(Customer_Master), load_date) AS customer_master, 
    MAX_BY(UPPER(Customer_Name), load_date) AS customer_name,
    lawson_id
  FROM unified_loads
  WHERE lawson_id IS NOT NULL
  GROUP BY lawson_id

  UNION ALL

  SELECT
    UPPER(CorporateCustomerName) AS customer_master,
    UPPER(CorporateCustomerName) AS customer_name,
    CustomerCode AS lawson_id
  FROM bronze.finance_corporate_customer
  WHERE CustomerCode IS NOT NULL AND Is_Deleted = 0)
),
final AS (
  SELECT DISTINCT
    far.CorporateCustomerId AS shipper_id,
    coalesce(far.CustomerCode, lm.lawson_id) AS lawson_id,
    sm.Customer_Master AS customer_master, 
    coalesce(far.CustomerCodeName,lm.Customer_Name) AS customer_name,
    try_CAST(ac.aljex_customer_id AS VARCHAR(50)) AS customer_aljex_id,
    try_CAST(rc.relay_customer_id AS VARCHAR(50)) AS customer_relay_id,
    NULLIF(NULLIF(TRIM(sm.Shipper_Rep), ''), 'null') AS shipper_rep,
    NULLIF(NULLIF(NULLIF(TRIM(sm.Sales_Rep), ''), 'null'), 'NA') AS sales_rep,
    NULLIF(NULLIF(TRIM(sm.Vertical), ''), 'null') AS vertical,
    NULLIF(NULLIF(TRIM(sm.Office), ''), 'null') AS shipper_office,
    NULLIF(NULLIF(TRIM(sm.Email_Address), ''), 'null') AS rep_email_address,
    NULLIF(NULLIF(TRIM(sm.Slack_Channel_Name), ''), 'null') AS shipper_slack_channel,
    NULLIF(NULLIF(TRIM(sm.Hubspot_Company_Name), ''), 'null') AS hubspot_account,
    NULLIF(NULLIF(TRIM(sm.Hubspot_Account_Manager), ''), 'null') AS hubspot_rep_name,
    try_CAST(rep_ids.rep_aljex_id AS VARCHAR(50)) AS rep_aljex_id,
    try_CAST(rep_ids.rep_relay_id AS VARCHAR(50)) AS rep_relay_id,
    try_CAST(ldd.last_load_date AS timestamp_ntz) AS last_load_date,
    try_CAST(
      CASE 
        WHEN ldd.last_load_date >= date_add(current_date(), -365) THEN 1
        ELSE 0
      END AS INT
    ) AS is_active
  FROM silver.silver_shipper_mapping sm
  LEFT JOIN customer_lawson lm
    ON upper(sm.customer_master) = upper(lm.customer_master)
    or upper(sm.Customer_Master) = upper(lm.customer_name)
  LEFT JOIN bronze.finance_corporate_customer far
    ON lm.lawson_id = far.CustomerCode 
  LEFT JOIN rep_ids
    ON upper(NULLIF(NULLIF(TRIM(sm.Shipper_Rep), ''), 'null')) = upper(rep_ids.tms_rep_name)
  LEFT JOIN load_date_details ldd
    ON lm.lawson_id = ldd.lawson_id
  LEFT JOIN aljex_customer ac
    ON lm.lawson_id = ac.lawson_id
  LEFT JOIN relay_customer rc
    ON lm.lawson_id = rc.lawson_id
  WHERE 
  upper(sm.customer_master) not like "NFI%" and 
  upper(sm.customer_master) not like "NFIL%" and 
  upper(sm.customer_master) not like "TARGET%"
  and coalesce(far.customercode,lm.lawson_id) is not null
),
final_null_lawson AS (
  SELECT DISTINCT
    try_CAST(far.CorporateCustomerId AS BIGINT) AS shipper_id,
    coalesce(far.customercode,lm.lawson_id) as Lawson_ID,
    final.Customer_Master AS customer_master, 
    coalesce(far.CustomerCodeName, cldd.customer_master) AS customer_name,
    try_CAST(rep_ids.rep_aljex_id AS VARCHAR(50)) AS rep_aljex_id,
    try_CAST(rep_ids.rep_relay_id AS VARCHAR(50)) AS rep_relay_id,
    NULLIF(NULLIF(TRIM(final.Shipper_Rep), ''), 'null') AS shipper_rep,
    NULLIF(NULLIF(NULLIF(TRIM(final.Sales_Rep), ''), 'null'), 'NA') AS sales_rep,
    NULLIF(NULLIF(TRIM(final.Vertical), ''), 'null') AS vertical,
    NULLIF(NULLIF(TRIM(final.Office), ''), 'null') AS shipper_office,
    NULLIF(NULLIF(TRIM(final.Email_Address), ''), 'null') AS rep_email_address,
    NULLIF(NULLIF(TRIM(final.Slack_Channel_name), ''), 'null') AS shipper_slack_channel,
    NULLIF(NULLIF(TRIM(final.Hubspot_Company_Name), ''), 'null') AS hubspot_account,
    NULLIF(NULLIF(TRIM(final.Hubspot_Account_Manager), ''), 'null') AS hubspot_rep_name,
    try_CAST(rep_ids.rep_aljex_id AS VARCHAR(50)) AS rep_aljex_id,
    try_CAST(rep_ids.rep_relay_id AS VARCHAR(50)) AS rep_relay_id,
    try_CAST(cldd.last_load_date AS timestamp_ntz) AS last_load_date,
    try_CAST(
      CASE 
        WHEN cldd.last_load_date >= date_add(current_date(), -365) THEN 1
        ELSE 0
      END AS INT
    ) AS is_active
  FROM silver.silver_shipper_mapping final
  LEFT JOIN customer_lawson lm
    ON upper(final.customer_master) = upper(lm.customer_master)
    or upper(final.Customer_Master) = upper(lm.customer_name)
  LEFT JOIN customer_load_date_details cldd
    ON upper(final.customer_master) = upper(cldd.customer_master)
  LEFT JOIN bronze.finance_corporate_customer far
    ON upper(final.customer_master) = upper(far.CorporateCustomerName)
    OR upper(final.customer_master) = upper(far.CustomerCodeName)
  LEFT JOIN rep_ids
    ON upper(final.Shipper_Rep) = upper(rep_ids.tms_rep_name)
  WHERE 
  upper(final.customer_master) not like "NFI%" and 
  upper(final.customer_master) not like "NFIL%" and 
  upper(final.customer_master) not like "TARGET%" and 
  coalesce(far.customercode,lm.lawson_id) is null
),
final_code as (
select distinct
final_union.*, 
sha2(concat_ws('||',
  coalesce(final_union.lawson_id,""),coalesce(final_union.customer_master,"")), 256) as MergeKey,
  sha2(concat_ws('||',
      coalesce(cast(final_union.shipper_id as string), '|'),
      coalesce(final_union.customer_name, '|'),
      coalesce(final_union.customer_aljex_id, '|'),
      coalesce(final_union.customer_relay_id, '|'),
      coalesce(final_union.shipper_rep, '|'),
      coalesce(final_union.sales_rep, '|'),
      coalesce(final_union.vertical, '|'),
      coalesce(final_union.shipper_office, '|'),
      coalesce(final_union.rep_email_address, '|'),
      coalesce(final_union.shipper_slack_channel, '|'),
      coalesce(final_union.hubspot_account, '|'),
      coalesce(final_union.hubspot_rep_name, '|'),
      coalesce(final_union.rep_aljex_id, '|'),
      coalesce(final_union.rep_relay_id, '|'),
      coalesce(cast(final_union.last_load_date as string), '|'),
      coalesce(cast(final_union.is_active as string), '|')
    ),256) as HashKey
 from (select * from final UNION select * from final_null_lawson) final_union)
select * from final_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DDL

-- COMMAND ----------

CREATE OR REPLACE TABLE Silver.Silver_Shipper (
    DW_Shipper_ID BIGINT GENERATED ALWAYS AS IDENTITY,
    Lawson_ID VARCHAR(50),
    Customer_Master VARCHAR(255), 
    Shipper_ID VARCHAR(50),
    Customer_Name VARCHAR(255),
    Customer_Aljex_ID VARCHAR(50),
    Customer_Relay_ID VARCHAR(50),
    Shipper_Rep VARCHAR(255),
    Sales_Rep VARCHAR(255),
    Vertical VARCHAR(255),
    Shipper_Office VARCHAR(10),
    Rep_Email_Address VARCHAR(50),
    Shipper_Slack_Channel VARCHAR(50),
    HubSpot_Account VARCHAR(50),
    HubSpot_Rep_Name VARCHAR(255),
    Rep_Aljex_ID VARCHAR(50),
    Rep_Relay_ID VARCHAR(50),
    Last_Load_Date TIMESTAMP_NTZ,
    Is_Active INT, 
    Mergekey STRING,
    Hashkey STRING,
    Created_Date TIMESTAMP_NTZ,
    Created_By VARCHAR(255),
    Last_Modified_Date TIMESTAMP_NTZ,
    Last_Modified_By VARCHAR(255),
    Is_Deleted INT
);