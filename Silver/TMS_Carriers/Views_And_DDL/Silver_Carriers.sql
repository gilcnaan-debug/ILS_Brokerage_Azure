-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver Carriers
-- MAGIC * **Description:** Code for View and DDL for Loading Table in Silver Zone
-- MAGIC * **Created Date:** 03/07/2025
-- MAGIC * **Created By:** Uday
-- MAGIC * **Modified Date:** 06/10/2025
-- MAGIC * **Modified By:** Uday
-- MAGIC * **Changes Made:** Logic Update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View

-- COMMAND ----------

-- DBTITLE 1,View: Silver_Carriers
CREATE OR REPLACE VIEW Silver.VW_Silver_Carrier AS 
WITH Aljex_Loads AS (
  SELECT
    CASE 
      WHEN try_cast(sa.DOT_Number as bigint) IS NULL OR try_cast(sa.DOT_Number as bigint) = 0 THEN try_cast(pc.DOT_Num as bigint) 
      ELSE try_cast(sa.DOT_Number as bigint)
    END AS Carrier_DOT,
    NULLIF(concat("MC",(TRIM(REPLACE(mc_number, 'MC', '')))),"MC") as MC_Number,
    Carrier_Name,
    Load_Number as load_id,
    Carrier_New_Office as Office,
    Carrier_ID as Aljex_Carrier_ID,
    null as Relay_Carrier_ID,
    Carrier_Rep as TMS_Rep_Name,
    Carrier_Rep_ID as Aljex_Rep_ID,
    null as Relay_Rep_ID,
    'aljex' as Source,
    Ship_Date as Load_Date,
    tendered_date,
    booked_date,
    Actual_Delivered_Date
  FROM silver.silver_aljex sa
   left join bronze.projection_carrier pc on upper(sa.carrier_name)= upper(pc.legal_name)
where sa.Is_Deleted=0 and load_status NOT LIKE '%VOID%' AND load_status <> 'HOLD' 
),
Relay_Loads AS (
  SELECT
    CASE 
      WHEN try_cast(sr.DOT_Number as bigint) IS NULL OR try_cast(sr.DOT_Number as bigint) = 0 THEN try_cast(pc.DOT_Num as bigint) 
      ELSE try_cast(sr.DOT_Number as bigint)
    END AS Carrier_DOT,
    NULLIF(concat("MC",(TRIM(REPLACE(mc_number, 'MC', '')))),"MC") as MC_Number,
    Carrier_Name,
    Load_ID as load_id,
    Carrier_New_Office as Office,
    null as Aljex_Carrier_ID,
    Carrier_ID as Relay_Carrier_ID,
    Carrier_Rep as TMS_Rep_Name,
    null as Aljex_Rep_ID,
    Carrier_Rep_ID as Relay_Rep_ID,
    'relay' as Source,
    Ship_Date as Load_Date,
    tendered_date,
    booked_date,
    Actual_Delivered_Date
  FROM silver.silver_relay sr
   left join bronze.projection_carrier pc on upper(sr.carrier_name)= upper(pc.legal_name)
where sr.Is_Deleted=0 and load_status NOT LIKE '%VOID%' AND load_status <> 'HOLD' 
),
unified_loads AS (
  SELECT * FROM Aljex_Loads where carrier_dot is not null
  UNION ALL
  SELECT * FROM Relay_Loads where carrier_dot is not null
),
HubSpot_Carriers AS (
   SELECT
    CASE 
      WHEN try_cast(s.Carrier_DOT as bigint) IS NULL OR try_cast(s.Carrier_DOT as bigint) = 0 THEN try_cast(r.Carrier_DOT as bigint)
      ELSE try_cast(s.Carrier_DOT as bigint)
    END as Carrier_DOT_final,
    coalesce(nullif(s.Carrier_MC,""), NULLIF(concat("MC",(TRIM(REPLACE(r.mc_number, 'MC', '')))),"MC")) as Carrier_MC_final,
    s.*
  FROM Silver.silver_hubspot_carriers s
  LEFT JOIN (
    SELECT DISTINCT Carrier_DOT, Carrier_Name, MC_Number
    FROM unified_loads
  ) r
    ON upper(s.Carrier_Name) = upper(r.Carrier_Name)
    OR nullif(s.Carrier_MC,"") = NULLIF(concat("MC",(TRIM(REPLACE(r.mc_number, 'MC', '')))),"MC")
),
hubspot_account_cte AS (
  SELECT
    Carrier_DOT_final as Carrier_DOT,
    HubSpot_Company_ID,
    Carrier_MC_final as Carrier_MC,
    row_number() OVER (
      PARTITION BY Carrier_DOT_final
      ORDER BY 
        coalesce(Total_Calls, 0) DESC,
        CASE WHEN Last_Call_Date IS NOT NULL THEN 1 ELSE 0 END DESC,
        Last_Call_Date DESC,
        coalesce(Total_Emails, 0) DESC,
        CASE WHEN Last_Email_Date IS NOT NULL THEN 1 ELSE 0 END DESC,
        Last_Email_Date DESC,
        Carrier_DOT desc,
        Carrier_MC desc
    ) as rn
  FROM HubSpot_Carriers
  WHERE Is_Deleted = 0
),

-- CTE for non-null Carrier_DOT
Carrier_ID_CTE_notnull AS (
  SELECT DISTINCT
    c.Carrier_DOT, 
    a.Aljex_Carrier_ID,
    NULLIF(r.Relay_Carrier_ID, 'Unknown Carrier SCAC') AS Relay_Carrier_ID,
    h.HubSpot_Company_ID
  FROM (
    SELECT Carrier_DOT FROM (
      SELECT try_cast(DOT_Number as bigint) AS Carrier_DOT FROM silver.silver_aljex
      UNION
      SELECT try_cast(DOT_Number as bigint) AS Carrier_DOT FROM silver.silver_relay
      UNION
      SELECT try_cast(Carrier_DOT as bigint) AS Carrier_DOT FROM Silver.Silver_HubSpot_Carriers
    ) AS combined
    WHERE Carrier_DOT IS NOT NULL AND Carrier_DOT <> 0
  ) c
  LEFT JOIN (
    SELECT DISTINCT Carrier_DOT, Aljex_Carrier_ID
    FROM Aljex_Loads
  ) a ON c.Carrier_DOT = a.Carrier_DOT
  LEFT JOIN (
    SELECT DISTINCT Carrier_DOT, Relay_Carrier_ID
    FROM Relay_Loads
  ) r ON c.Carrier_DOT = r.Carrier_DOT
  LEFT JOIN (
    SELECT DISTINCT 
      hubspot_account_cte.Carrier_DOT, 
      HubSpot_Company_ID, 
      Carrier_MC,
      rn
    FROM hubspot_account_cte
  ) h 
    ON c.Carrier_DOT = h.Carrier_DOT
    AND h.rn = 1
),

Rep_Details_general AS (
  SELECT
    Carrier_DOT,
    COUNT(load_id) AS load_count,
    TMS_Rep_Name,
    max(load_date) as last_load_date,
    max(tendered_date) as last_tendered_date,
    max(booked_date) as last_booked_date,
    max(actual_delivered_date) as last_delivered_date, 
    dense_rank() OVER (
      PARTITION BY Carrier_DOT
      ORDER BY COUNT(load_id) DESC
    ) AS rep_total_rank,
    dense_rank() OVER (
      PARTITION BY Carrier_DOT
      ORDER BY max(load_date) DESC
    ) AS rep_recent_rank
  FROM unified_loads
  WHERE  carrier_dot <> 0
  GROUP BY Carrier_DOT, tms_rep_name
),
final_rep AS (
  SELECT
    Carrier_DOT,
    load_count,
    TMS_Rep_Name,
    last_load_date,
    last_tendered_date,
    last_booked_date,
    last_delivered_date,
    rep_total_rank,
    rep_recent_rank
  FROM Rep_Details_general
  WHERE rep_total_rank = 1
),
rep_details AS (
  SELECT
    Carrier_DOT,
    load_count as rep_load_count,
    last_load_date as rep_last_load_date,
    last_tendered_date as rep_last_tendered_date,
    last_booked_date as rep_last_booked_date,
    nullif(tms_rep_name, "") as tms_rep_name
  FROM (
    SELECT *,
      dense_rank() OVER (PARTITION BY Carrier_DOT ORDER BY rep_recent_rank asc, last_load_date desc, last_tendered_date desc, last_booked_date desc ) as rn
    FROM final_rep
  ) t
  WHERE rn = 1
),
recent_details AS (
  SELECT
    Carrier_DOT,
    max_by(Office, Load_Date) as recent_office,
    max_by(Carrier_Name, Load_Date) as recent_carrier_name,
    max(Load_Date) as last_load_date,
    min(load_date) as first_load_Date,
    count(load_id) as load_count
  FROM unified_loads
  WHERE Carrier_DOT <> 0
  GROUP BY Carrier_DOT
),
regular_details AS (
  SELECT
    Carrier_DOT,
    Office as regular_office,
    cnt as office_load_count
  FROM (
    SELECT
      Carrier_DOT,
      Office,
      count(distinct load_id) as cnt,
      dense_Rank() OVER (PARTITION BY Carrier_DOT ORDER BY count(distinct load_id) desc, max(load_date) desc, max(tendered_date) desc, max(booked_date) desc) as rn
    FROM unified_loads
    WHERE Carrier_DOT <> 0
    GROUP BY Carrier_DOT, Office
  ) t
  WHERE rn = 1
),
recent_regular_details AS (
  SELECT
    r.Carrier_DOT,
    r.recent_office as Carrier_Recent_Office,
    r.recent_carrier_name as Carrier_Name,
    r.Last_Load_Date,
    r.first_load_Date as Onboarded_Date,
    r.Load_Count as Total_Loads,
    reg.regular_office as Carrier_Regular_Office,
    coalesce(rd.tms_rep_name) as TMS_Rep_Name
  FROM recent_details r
  LEFT JOIN regular_details reg
    ON r.Carrier_DOT = reg.Carrier_DOT
  LEFT JOIN rep_details rd
    ON r.Carrier_DOT = rd.Carrier_DOT
),
final_notnull AS (
  SELECT
  sha2(concat_ws('||',
    coalesce(CAST(hf.HubSpot_Company_ID AS STRING), ''),
    coalesce(CAST(hf.Carrier_DOT AS STRING), ''), 
    coalesce(hs.Carrier_MC, ''),
    coalesce(hf.Aljex_Carrier_ID,''),
    coalesce(hf.Relay_Carrier_ID,'')
  ), 256) as Merge_Key,
    hs.DW_HubSpot_Carrier_ID,
    hf.Carrier_DOT,
    hf.HubSpot_Company_ID as HubSpot_Carrier_ID,
    hf.Aljex_Carrier_ID,
    hf.Relay_Carrier_ID,
    hs.Carrier_MC,
    COALESCE(rrd.Carrier_Name,hs.Carrier_Name) as Carrier_Name,
    hs.Carrier_Vertical_Industry,
    hs.Address_Line_1,
    hs.Address_Line_2,
    hs.City,
    hs.State,
    hs.Country,
    hs.Zip_Code,
    hs.Carrier_Email_Address,
    hs.Carrier_Phone_Number,
    hs.Tracking_Platform,
    hs.Tracking_Platform_Status,
    hs.Flatbed_Count,
    hs.Reefer_Count,
    hs.Tractor_Count,
    hs.Trailer_Count,
    hs.Van_Count,
    hs.Current_Lifecycle_Stage,
    hs.Is_Managed_Carrier,
    hs.Managed_Carrier_Converted_Date,
    hs.Carrier_Name as HubSpot_Account,
    rrd.Carrier_Recent_Office,
    rrd.Carrier_Regular_Office,
    rrd.Last_Load_Date,
    rrd.Onboarded_Date as Onboarded_Date,
    rrd.Total_Loads,
    rrd.TMS_Rep_Name,
    ru.user_id as TMS_Rep_Relay_ID,
    au.aljex_id as TMS_Rep_Aljex_ID,
    hs.CAM_Name as HubSpot_Rep_Name,
    ru1.user_id as HubSpot_Rep_Relay_ID,
    au1.aljex_id as HubSpot_Rep_Aljex_ID,
    ru2.user_id as CDR_Rep_Relay_ID,
    au2.aljex_id as CDR_Rep_Aljex_ID,
    hs.CDR_Name as CDR_Rep_Name,
    sha2(concat_ws('||', 
    coalesce(string(hs.HubSpot_Company_ID),''),
      coalesce(hs.Carrier_MC, ''), 
      coalesce(hf.Aljex_Carrier_ID, ''),
      coalesce(hf.Relay_Carrier_ID, ''), 
      coalesce(hs.Carrier_Vertical_Industry, ''),
      coalesce(hs.Address_Line_1, ''),
      coalesce(hs.Address_Line_2, ''),
      coalesce(hs.City, ''),
      coalesce(hs.State, ''),
      coalesce(hs.Country, ''),
      coalesce(hs.Zip_Code, ''),
      coalesce(hs.Carrier_Email_Address, ''),
      coalesce(hs.Carrier_Phone_Number, ''),
      coalesce(hs.Tracking_Platform, ''),
      coalesce(hs.Tracking_Platform_Status, ''),
      coalesce(hs.Flatbed_Count, 0),
      coalesce(hs.Reefer_Count, 0),
      coalesce(hs.Tractor_Count, 0),
      coalesce(hs.Trailer_Count, 0),
      coalesce(hs.Van_Count, 0),
      coalesce(hs.Current_Lifecycle_Stage, ''),
      coalesce(STRING(hs.Is_Managed_Carrier), ''),
      coalesce(STRING(hs.Managed_Carrier_Converted_Date), ''),
      coalesce(hs.Carrier_Name, ''),
      coalesce(rrd.Carrier_Recent_Office, ''),
      coalesce(rrd.Carrier_Regular_Office, ''),
      coalesce(rrd.Carrier_Name, ''),
      coalesce(STRING(rrd.Last_Load_Date), ''),
      coalesce(STRING(rrd.Onboarded_Date), ''),
      coalesce(STRING(rrd.Total_Loads), ''),
      coalesce(rrd.TMS_Rep_Name, ''),
      coalesce(STRING(ru.user_id), ''),
      coalesce(au.aljex_id, ''),
      coalesce(hs.CAM_Name, ''),
      coalesce(STRING(ru1.user_id), ''),
      coalesce(au1.aljex_id, ''),
      coalesce(STRING(ru2.user_id), ''),
      coalesce(au2.aljex_id, ''),
      coalesce(hs.CDR_Name, '')
    ), 256) as Hashkey,
    CASE WHEN rrd.last_load_date >= date_add(MONTH, -6, current_date) THEN 1 ELSE 0 END as is_active
  FROM Carrier_ID_CTE_notnull hf
  LEFT JOIN recent_regular_details rrd
    ON hf.Carrier_DOT = rrd.Carrier_DOT
  LEFT JOIN silver.silver_hubspot_carriers hs
    ON hf.hubspot_company_id = hs.hubspot_company_id
  LEFT JOIN bronze.relay_users ru
    ON upper(rrd.tms_rep_name) = upper(ru.full_name) and ru.`active?` = true
  LEFT JOIN bronze.aljex_user_report_listing au
    ON upper(rrd.tms_rep_name) = upper(au.ultipro_name) and au.aljex_id <> 'repeat' 
  LEFT JOIN bronze.relay_users ru1
    ON (upper(hs.CAM_Name) = upper(ru1.full_name) OR hs.cam_email = ru1.email_address) and ru1.`active?` = true
  LEFT JOIN bronze.aljex_user_report_listing au1
    ON upper(hs.CAM_Name) = upper(au1.ultipro_name) and au1.aljex_id <> 'repeat' 
  LEFT JOIN bronze.relay_users ru2
    ON (upper(hs.CDR_Name) = upper(ru2.full_name) OR hs.cdr_email = ru2.email_address) and ru2.`active?` = true
  LEFT JOIN bronze.aljex_user_report_listing au2
    ON upper(hs.CDR_Name) = upper(au2.ultipro_name) and au2.aljex_id <> 'repeat' 
),
final_null AS (
  SELECT
   sha2(concat_ws('||',
    coalesce(CAST(hs.HubSpot_Company_ID AS STRING), ''),
    coalesce(CAST(Carrier_DOT_final AS STRING), ''), 
    coalesce(Carrier_MC, '')
  ), 256) as Merge_Key,
    hs.DW_HubSpot_Carrier_ID,
    Carrier_DOT_final AS Carrier_DOT,
    hs.HubSpot_Company_ID as HubSpot_Carrier_ID,
    NULL AS Aljex_Carrier_ID,
    NULL AS Relay_Carrier_ID,
    hs.Carrier_MC,
    hs.Carrier_Name,
    hs.Carrier_Vertical_Industry,
    hs.Address_Line_1,
    hs.Address_Line_2,
    hs.City,
    hs.State,
    hs.Country,
    hs.Zip_Code,
    hs.Carrier_Email_Address,
    hs.Carrier_Phone_Number,
    hs.Tracking_Platform,
    hs.Tracking_Platform_Status,
    hs.Flatbed_Count,
    hs.Reefer_Count,
    hs.Tractor_Count,
    hs.Trailer_Count,
    hs.Van_Count,
    hs.Current_Lifecycle_Stage,
    hs.Is_Managed_Carrier,
    hs.Managed_Carrier_Converted_Date,
    hs.Carrier_Name as HubSpot_Account,
    NULL as Carrier_Recent_Office,
    NULL as Carrier_Regular_Office,
    NULL as Last_Load_Date,
    NULL as Onboarded_Date,
    NULL as Total_Loads,
    NULL as TMS_Rep_Name,
    NULL as TMS_Rep_Relay_ID,
    NULL as TMS_Rep_Aljex_ID,
    hs.CAM_Name as HubSpot_Rep_Name,
    ru1.user_id as HubSpot_Rep_Relay_ID,
    au1.aljex_id as HubSpot_Rep_Aljex_ID,
    ru2.user_id as CDR_Rep_Relay_ID,
    au2.aljex_id as CDR_Rep_Aljex_ID,
    hs.CDR_Name as CDR_Rep_Name,
    sha2(concat_ws('||',
      '',
    coalesce(string(hs.HubSpot_Company_ID),''),
      coalesce(hs.Carrier_MC, ''), 
      '',
      '', 
      coalesce(hs.Carrier_Vertical_Industry, ''),
      coalesce(hs.Address_Line_1, ''),
      coalesce(hs.Address_Line_2, ''),
      coalesce(hs.City, ''),
      coalesce(hs.State, ''),
      coalesce(hs.Country, ''),
      coalesce(hs.Zip_Code, ''),
      coalesce(hs.Carrier_Email_Address, ''),
      coalesce(hs.Carrier_Phone_Number, ''),
      coalesce(hs.Tracking_Platform, ''),
      coalesce(hs.Tracking_Platform_Status, ''),
      coalesce(hs.Flatbed_Count, 0),
      coalesce(hs.Reefer_Count, 0),
      coalesce(hs.Tractor_Count, 0),
      coalesce(hs.Trailer_Count, 0),
      coalesce(hs.Van_Count, 0),
      coalesce(hs.Current_Lifecycle_Stage, ''),
      coalesce(STRING(hs.Is_Managed_Carrier), ''),
      coalesce(STRING(hs.Managed_Carrier_Converted_Date), ''),
      coalesce(hs.Carrier_Name, ''),
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      coalesce(hs.CAM_Name, ''),
      coalesce(STRING(ru1.user_id), ''),
      au1.aljex_id,
      coalesce(STRING(ru2.user_id), ''),
      au2.aljex_id,
      coalesce(hs.CDR_Name, '')
    ), 256) as Hashkey,
    0 as is_active
  FROM HubSpot_Carriers hs
  LEFT JOIN bronze.relay_users ru1
    ON (upper(hs.CAM_Name) = upper(ru1.full_name) OR hs.cam_email = ru1.email_address) and ru1.`active?` = true
  LEFT JOIN bronze.aljex_user_report_listing au1
    ON upper(hs.CAM_Name) = upper(au1.ultipro_name) and au1.aljex_id <> 'repeat' 
  LEFT JOIN bronze.relay_users ru2
    ON (upper(hs.CDR_Name) = upper(ru2.full_name) OR hs.cdr_email = ru2.email_address) and ru2.`active?` = true
  LEFT JOIN bronze.aljex_user_report_listing au2
    ON upper(hs.CDR_Name) = upper(au2.ultipro_name) and au2.aljex_id <> 'repeat' 
  WHERE (Carrier_DOT_final = '0')
    AND Is_Deleted = 0
)
select * from final_notnull union all
select * from final_null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DDL

-- COMMAND ----------

-- DBTITLE 1,DDL: Silver_Carriers
CREATE OR REPLACE TABLE Silver.Silver_Carriers (
    DW_Carrier_ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    DW_HubSpot_Carrier_ID BIGINT,
    Carrier_DOT BIGINT,
    Carrier_MC VARCHAR(50),
    HubSpot_Carrier_ID BIGINT,
    Aljex_Carrier_ID VARCHAR(50),
    Relay_Carrier_ID VARCHAR(50),
    Carrier_Name VARCHAR(255),
    HubSpot_Account VARCHAR(255),
    Carrier_Vertical_Industry VARCHAR(255),
    Address_Line_1 VARCHAR(255),
    Address_Line_2 VARCHAR(255),
    City VARCHAR(255),
    State VARCHAR(255),
    Country VARCHAR(255),
    Zip_Code VARCHAR(50),
    TMS_Rep_Aljex_ID VARCHAR(50),
    TMS_Rep_Relay_ID VARCHAR(50),
    TMS_Rep_Name VARCHAR(255),
    HubSpot_Rep_Aljex_ID VARCHAR(50),
    HubSpot_Rep_Relay_ID VARCHAR(50),
    HubSpot_Rep_Name VARCHAR(255),
    CDR_Rep_Aljex_ID VARCHAR(50),
    CDR_Rep_Relay_ID VARCHAR(50),
    CDR_Rep_Name VARCHAR(255),
    Carrier_Email_Address VARCHAR(255),
    Carrier_Phone_Number VARCHAR(255),
    Tracking_Platform VARCHAR(255),
    Tracking_Platform_Status VARCHAR(255),
    Flatbed_Count INT,
    Reefer_Count INT,
    Tractor_Count INT,
    Trailer_Count INT,
    Van_Count INT,
    Is_Active INT,
    Onboarded_Date DATE,
    Last_Load_Date DATE,
    Carrier_Regular_Office	VARCHAR(20),
    Carrier_Recent_Office	VARCHAR(20),
    Current_Lifecycle_Stage VARCHAR(255),
    Is_Managed_Carrier INT,
    Managed_Carrier_Converted_Date TIMESTAMP_NTZ,
    Hashkey STRING,
    Merge_Key STRING,
    Created_Date TIMESTAMP_NTZ,
    Created_By VARCHAR(255),
    Last_Modified_Date TIMESTAMP_NTZ,
    Last_Modified_By VARCHAR(255),
    Is_Deleted INT
);