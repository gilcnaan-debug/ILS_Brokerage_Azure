-- Databricks notebook source
truncate table silver.silver_shipper

-- COMMAND ----------

select * from silver.silver_shipper where mergekey in (select mergekey from silver.silver_shipper group by mergekey having count(*)>1)

-- COMMAND ----------

select * from silver.silver_shipper where Customer_Master like "%NEO%"

-- COMMAND ----------

select * from silver.silver_shipper_mapping where Customer_Master like "%NEO%"

-- COMMAND ----------

-- DBTITLE 1,Last Load Date - Metadata
SELECT LastLoadDateValue FROM Metadata.mastermetadata from where TableID="SL8"

-- COMMAND ----------

-- DBTITLE 1,Last Load Date - Silver_HubSpot_Companies
select max(Last_Modified_Date) from silver.silver_carriers

-- COMMAND ----------

-- DBTITLE 1,Sample Data: Silver_HubSpot_Carriers
select * from silver.silver_hubspot_carriers order by HubSpot_Company_ID limit 10

-- COMMAND ----------

-- DBTITLE 1,Duplicates: Silver.Silver_Carriers
select DW_Carrier_ID from Silver.silver_carriers group by DW_Carrier_ID having count(*) > 1

-- COMMAND ----------

-- DBTITLE 1,Rows: Silver.Silver_Carriers
select count(*) from Silver.silver_carriers

-- COMMAND ----------

-- DBTITLE 1,Rows: Silver Carriers CTE
WITH Aljex_Loads AS (
  SELECT
    try_cast(DOT_Number as bigint) AS Carrier_DOT,
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
    Ship_Date as Load_Date
  FROM silver.silver_aljex
),
Relay_Loads AS (
  SELECT
    try_cast(DOT_Number as bigint) AS Carrier_DOT,
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
    Ship_Date as Load_Date
  FROM silver.silver_relay
),
unified_loads AS (
  SELECT * FROM Aljex_Loads
  UNION ALL
  SELECT * FROM Relay_Loads
),
HubSpot_Carriers AS (
  SELECT
    coalesce(nullif(try_cast(r.Carrier_DOT as bigint),NULL), try_cast(s.Carrier_DOT as bigint)) as Carrier_DOT_final,
    coalesce(s.Carrier_MC, NULLIF(concat("MC",(TRIM(REPLACE(r.mc_number, 'MC', '')))),"MC")) as Carrier_MC_final,
    s.*
  FROM Silver.silver_hubspot_carriers s
  LEFT JOIN (
    SELECT DISTINCT Carrier_DOT, Carrier_Name, MC_Number
    FROM unified_loads
  ) r
    ON upper(s.Carrier_Name) = upper(r.Carrier_Name)
    OR s.Carrier_MC = NULLIF(concat("MC",(TRIM(REPLACE(r.mc_number, 'MC', '')))),"MC")
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
        Last_Email_Date DESC
    ) as rn
  FROM HubSpot_Carriers
  WHERE Is_Deleted = 0
),
Carrier_ID_CTE AS (
SELECT DISTINCT
    c.Carrier_DOT, 
    a.Aljex_Carrier_ID,
    NULLIF(r.Relay_Carrier_ID, 'Unknown Carrier SCAC') AS Relay_Carrier_ID,
    h.HubSpot_Company_ID
  FROM (
    SELECT  
    Carrier_DOT FROM (
      SELECT try_cast(DOT_Number as bigint) AS Carrier_DOT FROM brokeragedev.silver.silver_aljex
      UNION
      SELECT try_cast(DOT_Number as bigint) AS Carrier_DOT FROM brokeragedev.silver.silver_relay
      UNION
      SELECT try_cast(Carrier_DOT as bigint) AS Carrier_DOT FROM brokeragedev.Silver.Silver_HubSpot_Carriers
    ) AS combined
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
    SELECT DISTINCT hubspot_account_cte.Carrier_DOT, HubSpot_Company_ID, Carrier_MC
    FROM hubspot_account_cte 
    where rn=1
  ) h ON c.Carrier_DOT = h.Carrier_DOT)
,
Rep_Details_general AS (
  SELECT
    Carrier_DOT,
    COUNT(load_id) AS load_count,
    TMS_Rep_Name,
    max(load_date) as last_load_date,
    dense_rank() OVER (
      PARTITION BY Carrier_DOT
      ORDER BY COUNT(load_id) DESC
    ) AS rep_total_rank,
    dense_rank() OVER (
      PARTITION BY Carrier_DOT
      ORDER BY max(load_date) DESC
    ) AS rep_recent_rank
  FROM unified_loads
  WHERE carrier_dot IS NOT NULL AND carrier_dot <> 0
  GROUP BY Carrier_DOT, tms_rep_name
),
final_rep AS (
  SELECT
    Carrier_DOT,
    load_count,
    TMS_Rep_Name,
    last_load_date,
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
    nullif(tms_rep_name, "") as tms_rep_name
  FROM (
    SELECT *,
      dense_rank() OVER (PARTITION BY Carrier_DOT ORDER BY rep_recent_rank) as rn
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
  WHERE Office IS NOT NULL
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
      row_number() OVER (PARTITION BY Carrier_DOT ORDER BY count(distinct load_id) DESC) as rn
    FROM unified_loads
    WHERE Office IS NOT NULL
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
    rd.tms_rep_name as TMS_Rep_Name
  FROM recent_details r
  LEFT JOIN regular_details reg
    ON r.Carrier_DOT = reg.Carrier_DOT
  LEFT JOIN rep_details rd
    ON r.Carrier_DOT = rd.Carrier_DOT
),
final AS (
  SELECT
    hs.DW_HubSpot_Carrier_ID,
    hf.Carrier_DOT,
    hf.Aljex_Carrier_ID,
    hf.Relay_Carrier_ID,
    hs.Carrier_MC,
    rrd.Carrier_Name,
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
      coalesce(STRING(hf.Carrier_DOT), ''),
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
  FROM Carrier_ID_CTE hf
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
)
select count(*) from final

-- COMMAND ----------

-- DBTITLE 1,Nulls: Silver_Carriers
SELECT
  COUNT(*) - COUNT(DW_Carrier_ID) AS DW_Carrier_ID,
  COUNT(*) - COUNT(Carrier_DOT) AS Carrier_DOT,
  COUNT(*) - COUNT(Carrier_MC) AS Carrier_MC,
  COUNT(*) - COUNT(Carrier_Name) AS Carrier_Name,
  COUNT(*) - COUNT(Address_Line_1) AS Address_Line_1,
  COUNT(*) - COUNT(Address_Line_2) AS Address_Line_2,
  COUNT(*) - COUNT(City) AS City,
  COUNT(*) - COUNT(State) AS State,
  COUNT(*) - COUNT(Country) AS Country,
  COUNT(*) - COUNT(Zip_Code) AS Zip_Code,
  COUNT(*) - COUNT(Carrier_Email_Address) AS Carrier_Email_Address,
  COUNT(*) - COUNT(Carrier_Phone_Number) AS Carrier_Phone_Number,
  COUNT(*) - COUNT(Tracking_Platform) AS Tracking_Platform,
  COUNT(*) - COUNT(Tracking_Platform_Status) AS Tracking_Platform_Status,
  COUNT(*) - COUNT(Flatbed_Count) AS Flatbed_Count,
  COUNT(*) - COUNT(Reefer_Count) AS Reefer_Count,
  COUNT(*) - COUNT(Tractor_Count) AS Tractor_Count,
  COUNT(*) - COUNT(Trailer_Count) AS Trailer_Count,
  COUNT(*) - COUNT(Van_Count) AS Van_Count,
  COUNT(*) - COUNT(Current_Lifecycle_Stage) AS Current_Lifecycle_Stage,
  COUNT(*) - COUNT(Is_Managed_Carrier) AS Is_Managed_Carrier,
  COUNT(*) - COUNT(Managed_Carrier_Converted_Date) AS Managed_Carrier_Converted_Date,
  COUNT(*) - COUNT(Hashkey) AS Hashkey,
  COUNT(*) - COUNT(Created_Date) AS Created_Date,
  COUNT(*) - COUNT(Created_By) AS Created_By,
  COUNT(*) - COUNT(Last_Modified_Date) AS Last_Modified_Date,
  COUNT(*) - COUNT(Last_Modified_By) AS Last_Modified_By,
  COUNT(*) - COUNT(Is_Deleted) AS Is_Deleted
FROM silver.silver_carriers

-- COMMAND ----------

-- DBTITLE 1,Nulls: Silver Carriers CTE

WITH Aljex_Loads AS (
  SELECT
    try_cast(DOT_Number as bigint) AS Carrier_DOT,
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
    Ship_Date as Load_Date
  FROM silver.silver_aljex
),
Relay_Loads AS (
  SELECT
    try_cast(DOT_Number as bigint) AS Carrier_DOT,
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
    Ship_Date as Load_Date
  FROM silver.silver_relay
),
unified_loads AS (
  SELECT * FROM Aljex_Loads
  UNION ALL
  SELECT * FROM Relay_Loads
),
HubSpot_Carriers AS (
  SELECT
    coalesce(nullif(try_cast(r.Carrier_DOT as bigint),NULL), try_cast(s.Carrier_DOT as bigint)) as Carrier_DOT_final,
    coalesce(s.Carrier_MC, NULLIF(concat("MC",(TRIM(REPLACE(r.mc_number, 'MC', '')))),"MC")) as Carrier_MC_final,
    s.*
  FROM Silver.silver_hubspot_carriers s
  LEFT JOIN (
    SELECT DISTINCT Carrier_DOT, Carrier_Name, MC_Number
    FROM unified_loads
  ) r
    ON upper(s.Carrier_Name) = upper(r.Carrier_Name)
    OR s.Carrier_MC = NULLIF(concat("MC",(TRIM(REPLACE(r.mc_number, 'MC', '')))),"MC")
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
        Last_Email_Date DESC
    ) as rn
  FROM HubSpot_Carriers
  WHERE Is_Deleted = 0
),
Carrier_ID_CTE AS (
SELECT DISTINCT
    c.Carrier_DOT, 
    a.Aljex_Carrier_ID,
    NULLIF(r.Relay_Carrier_ID, 'Unknown Carrier SCAC') AS Relay_Carrier_ID,
    h.HubSpot_Company_ID
  FROM (
    SELECT  
    Carrier_DOT FROM (
      SELECT try_cast(DOT_Number as bigint) AS Carrier_DOT FROM brokeragedev.silver.silver_aljex
      UNION
      SELECT try_cast(DOT_Number as bigint) AS Carrier_DOT FROM brokeragedev.silver.silver_relay
      UNION
      SELECT try_cast(Carrier_DOT as bigint) AS Carrier_DOT FROM brokeragedev.Silver.Silver_HubSpot_Carriers
    ) AS combined
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
    SELECT DISTINCT hubspot_account_cte.Carrier_DOT, HubSpot_Company_ID, Carrier_MC
    FROM hubspot_account_cte 
    where rn=1
  ) h ON c.Carrier_DOT = h.Carrier_DOT
  where c.Carrier_DOT IS NOT NULL AND c.Carrier_DOT <> 0
),
Rep_Details_general AS (
  SELECT
    Carrier_DOT,
    COUNT(load_id) AS load_count,
    TMS_Rep_Name,
    max(load_date) as last_load_date,
    dense_rank() OVER (
      PARTITION BY Carrier_DOT
      ORDER BY COUNT(load_id) DESC
    ) AS rep_total_rank,
    dense_rank() OVER (
      PARTITION BY Carrier_DOT
      ORDER BY max(load_date) DESC
    ) AS rep_recent_rank
  FROM unified_loads
  WHERE carrier_dot IS NOT NULL AND carrier_dot <> 0
  GROUP BY Carrier_DOT, tms_rep_name
),
final_rep AS (
  SELECT
    Carrier_DOT,
    load_count,
    TMS_Rep_Name,
    last_load_date,
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
    nullif(tms_rep_name, "") as tms_rep_name
  FROM (
    SELECT *,
      dense_rank() OVER (PARTITION BY Carrier_DOT ORDER BY rep_recent_rank) as rn
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
  WHERE Office IS NOT NULL
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
      row_number() OVER (PARTITION BY Carrier_DOT ORDER BY count(distinct load_id) DESC) as rn
    FROM unified_loads
    WHERE Office IS NOT NULL
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
    rd.tms_rep_name as TMS_Rep_Name
  FROM recent_details r
  LEFT JOIN regular_details reg
    ON r.Carrier_DOT = reg.Carrier_DOT
  LEFT JOIN rep_details rd
    ON r.Carrier_DOT = rd.Carrier_DOT
),
final AS (
  SELECT
    hs.DW_HubSpot_Carrier_ID,
    hf.Carrier_DOT,
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
      coalesce(STRING(hf.Carrier_DOT), ''),
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
  FROM Carrier_ID_CTE hf
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
) 
SELECT
  COUNT(*) - COUNT(DW_HubSpot_Carrier_ID) AS DW_HubSpot_Carrier_ID,
  COUNT(*) - COUNT(Carrier_DOT) AS Carrier_DOT,
  COUNT(*) - COUNT(Aljex_Carrier_ID) AS Aljex_Carrier_ID,
  COUNT(*) - COUNT(Relay_Carrier_ID) AS Relay_Carrier_ID,
  COUNT(*) - COUNT(Carrier_MC) AS Carrier_MC,
  COUNT(*) - COUNT(Carrier_Name) AS Carrier_Name,
  COUNT(*) - COUNT(Carrier_Vertical_Industry) AS Carrier_Vertical_Industry,
  COUNT(*) - COUNT(Address_Line_1) AS Address_Line_1,
  COUNT(*) - COUNT(Address_Line_2) AS Address_Line_2,
  COUNT(*) - COUNT(City) AS City,
  COUNT(*) - COUNT(State) AS State,
  COUNT(*) - COUNT(Country) AS Country,
  COUNT(*) - COUNT(Zip_Code) AS Zip_Code,
  COUNT(*) - COUNT(Carrier_Email_Address) AS Carrier_Email_Address,
  COUNT(*) - COUNT(Carrier_Phone_Number) AS Carrier_Phone_Number,
  COUNT(*) - COUNT(Tracking_Platform) AS Tracking_Platform,
  COUNT(*) - COUNT(Tracking_Platform_Status) AS Tracking_Platform_Status,
  COUNT(*) - COUNT(Flatbed_Count) AS Flatbed_Count,
  COUNT(*) - COUNT(Reefer_Count) AS Reefer_Count,
  COUNT(*) - COUNT(Tractor_Count) AS Tractor_Count,
  COUNT(*) - COUNT(Trailer_Count) AS Trailer_Count,
  COUNT(*) - COUNT(Van_Count) AS Van_Count,
  COUNT(*) - COUNT(Current_Lifecycle_Stage) AS Current_Lifecycle_Stage,
  COUNT(*) - COUNT(Is_Managed_Carrier) AS Is_Managed_Carrier,
  COUNT(*) - COUNT(Managed_Carrier_Converted_Date) AS Managed_Carrier_Converted_Date,
  COUNT(*) - COUNT(HubSpot_Account) AS HubSpot_Account,
  COUNT(*) - COUNT(Carrier_Recent_Office) AS Carrier_Recent_Office,
  COUNT(*) - COUNT(Carrier_Regular_Office) AS Carrier_Regular_Office,
  COUNT(*) - COUNT(Last_Load_Date) AS Last_Load_Date,
  COUNT(*) - COUNT(Onboarded_Date) AS Onboarded_Date,
  COUNT(*) - COUNT(Total_Loads) AS Total_Loads,
  COUNT(*) - COUNT(TMS_Rep_Name) AS TMS_Rep_Name,
  COUNT(*) - COUNT(TMS_Rep_Relay_ID) AS TMS_Rep_Relay_ID,
  COUNT(*) - COUNT(TMS_Rep_Aljex_ID) AS TMS_Rep_Aljex_ID,
  COUNT(*) - COUNT(HubSpot_Rep_Name) AS HubSpot_Rep_Name,
  COUNT(*) - COUNT(HubSpot_Rep_Relay_ID) AS HubSpot_Rep_Relay_ID,
  COUNT(*) - COUNT(HubSpot_Rep_Aljex_ID) AS HubSpot_Rep_Aljex_ID,
  COUNT(*) - COUNT(CDR_Rep_Relay_ID) AS CDR_Rep_Relay_ID,
  COUNT(*) - COUNT(CDR_Rep_Aljex_ID) AS CDR_Rep_Aljex_ID,
  COUNT(*) - COUNT(CDR_Rep_Name) AS CDR_Rep_Name, 
  COUNT(*) - COUNT(is_active) AS is_active
FROM final

-- COMMAND ----------

select * from silver.silver_carriers where Carrier_Name is null