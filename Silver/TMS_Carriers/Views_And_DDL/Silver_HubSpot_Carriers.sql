-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver HubSpot Carriers
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

-- MAGIC %md
-- MAGIC ### Incremental Load

-- COMMAND ----------

-- DBTITLE 1,View: Silver_HubSpot_Carriers
CREATE OR REPLACE VIEW Silver.VW_Silver_HubSpot_Carrier AS  (
WITH total_calls_cte as 
(
    select hco.hs_object_id, count(Associated_ID) as Total_Calls, max(hca.hs_createdate) as Last_Call_Date 
    from bronze.hubspot_companies hco 
    left join bronze.hubspot_associations ha on hco.hs_object_id=ha.Company_ID 
    left join bronze.hubspot_calls hca on ha.Associated_ID=hca.hs_object_id 
    WHERE hco.is_deleted = 0 
      AND hco.sourcesystemid = 5 
      AND hco.archived = "false"
      AND ha.Is_Deleted=0
      AND ha.associated_type='Calls' 
      and hca.archived = "false" 
    GROUP BY hco.hs_object_id 
),
total_emails_cte as 
(
    select hco.hs_object_id, count(Associated_ID) as Total_Emails, max(ha.Created_Date) as Last_Email_Date 
    from bronze.hubspot_companies hco 
    left join bronze.hubspot_associations ha on hco.hs_object_id=ha.Company_ID 
    left join bronze.hubspot_emails hca on ha.Associated_ID=hca.hs_object_id 
    WHERE hco.is_deleted = 0 
      AND hco.sourcesystemid = 5 
      AND hco.archived = "false"
      AND ha.Is_Deleted=0
      AND ha.associated_type='Emails' 
      and hca.archived = "false" 
    GROUP BY hco.hs_object_id 
),
current_lifecycle_cte AS (
  SELECT
    l.Company_ID,
    l.Lifecycle_Stage_Value AS current_lifecyclestage,
    l.cycle_timestamp AS current_lifecyclestage_timestamp
  FROM bronze.hubspot_lifecyclestage l
  INNER JOIN (
    SELECT
      Company_ID,
      MAX(cycle_timestamp) AS max_cycle_timestamp
    FROM bronze.hubspot_lifecyclestage
    GROUP BY Company_ID
  ) lm
    ON l.Company_ID = lm.Company_ID
    AND l.cycle_timestamp = lm.max_cycle_timestamp
)
select 
    try_cast(hubspot_companies.hs_object_id as bigint) as HubSpot_Company_ID,
    try_cast(NULLIF(dot_number,"") as bigint) as Carrier_DOT, 
    NULLIF(concat("MC",(TRIM(REPLACE(mc_number, 'MC', '')))),"MC")  AS Carrier_MC, 
    try_cast(NULLIF(NULLIF(relay_carrier_id,"#N/A"),"") as string) as HubSpot_Relay_Carrier_ID,
    name AS Carrier_Name, 
    NULLIF(industry, "") as Carrier_Vertical_Industry,
    NULLIF(address, "") as Address_Line_1,
    NULLIF(address2, "") as Address_Line_2,
    NULLIF(city, "") as City,
    NULLIF(state, "") as State,
    NULLIF(country, "") as Country,
    NULLIF(zip, "") as Zip_Code,
    cam_owners.id as CAM_HubSpot_ID,
    NULLIF(cam_owners.email,'') as CAM_Email,
    concat(cam_owners.first_name, " " ,cam_owners.last_name) as CAM_Name,
    cdr_owners.id  as CDR_HubSpot_ID,
    NULLIF(cdr_owners.email,'') as CDR_Email,
    concat(cdr_owners.first_name, " " ,cdr_owners.last_name) as CDR_Name,
    NULLIF(hubspot_team_id,'') as HubSpot_Team_ID,
    NULLIF(contact_email, '') as Carrier_Email_Address,
    coalesce(NULLIF(phone, ''), NULLIF(contact_phone_number, '')) as Carrier_Phone_Number,
    automated_tracking_provider as Tracking_Platform,
    automated_tracking_status as Tracking_Platform_Status,
    try_cast(of_flatbeds as BIGINT)	as Flatbed_Count,
    try_cast(of_reefers as BIGINT)	as Reefer_Count,
    try_cast(of_tractors as BIGINT)	as Tractor_Count,
    try_cast(of_trailers as BIGINT)	as Trailer_Count,
    try_cast(of_vans as BIGINT) as Van_Count,
    hs_created_by_user_id as Account_Created_By_User_ID,
    try_cast(createdate as date) as Account_Created_Date, 
    try_cast(tc.Last_Call_Date as date) as Last_Call_Date,
    try_cast(tc.Total_Calls as int) as Total_Calls,
    try_cast(te.Last_Email_Date as date) as Last_Email_Date,
    try_cast(te.Total_Emails as int) as Total_Emails,
    NULLIF(current_lifecyclestage,"")	as Current_Lifecycle_Stage,
    CASE WHEN UPPER(NULLIF(current_lifecyclestage,"")) = "CUSTOMER" then 1 else 0
    end	as Is_Managed_Carrier,
    CASE WHEN UPPER(NULLIF(current_lifecyclestage,"")) = "CUSTOMER" then current_lifecyclestage_timestamp	else Null end as Managed_Carrier_Converted_Date,
    sha2(concat_ws('||',
            coalesce(Carrier_DOT, 0),
            coalesce(Carrier_MC, ''),
            coalesce(HubSpot_Relay_Carrier_ID, ''),
            coalesce(Carrier_Name, ''),
            coalesce(Carrier_Vertical_Industry, ''),
            coalesce(Address_Line_1, ''),
            coalesce(Address_Line_2, ''),
            coalesce(City, ''),
            coalesce(State, ''),
            coalesce(Country, ''),
            coalesce(Zip_Code, ''),
            coalesce(CAM_HubSpot_ID, 0),
            coalesce(CAM_Email, ''),
            coalesce(CAM_Name, ''),
            coalesce(CDR_HubSpot_ID, 0),
            coalesce(CDR_Email, ''),
            coalesce(CDR_Name, ''),
            coalesce(HubSpot_Team_ID, ''),
            coalesce(Carrier_Email_Address, ''),
            coalesce(Carrier_Phone_Number, ''),
            coalesce(Tracking_Platform, ''),
            coalesce(Tracking_Platform_Status, ''),
            coalesce(Flatbed_Count, 0),
            coalesce(Reefer_Count, 0),
            coalesce(Tractor_Count, 0),
            coalesce(Trailer_Count, 0),
            coalesce(Van_Count, 0),
            coalesce(Account_Created_By_User_ID, 0),
            coalesce(STRING(Account_Created_Date), ''),
            coalesce(STRING(Last_Call_Date),''),
            coalesce(Total_Calls,0),
            coalesce(STRING(Last_Email_Date)),
            coalesce(Total_Emails, 0),
            coalesce(Current_Lifecycle_Stage, ''),
            coalesce(Is_Managed_Carrier, 0),
            coalesce(STRING(Managed_Carrier_Converted_Date), '')
        ), 256) as Hashkey
from bronze.hubspot_companies
LEFT JOIN current_lifecycle_cte
    ON hubspot_companies.hs_object_id = current_lifecycle_cte.Company_ID
LEFT JOIN bronze.hubspot_owners as cam_owners
    ON cam_owners.id = COALESCE(
        NULLIF(cad_owner, ''),
        NULLIF(carrier_am, ''),
        NULLIF(carrier_am__2_, ''),
        NULLIF(ct_office_rep, ''),
        NULLIF(target_caam, ''),
        NULLIF(dray_owner, '')
    )
LEFT JOIN bronze.hubspot_owners as cdr_owners
    ON cdr_owners.id = COALESCE(
        NULLIF(cdr, ''),
        NULLIF(cdr__2_, ''),
        NULLIF(target_cdr, '')
    )
LEFT JOIN total_calls_cte tc
    ON hubspot_companies.hs_object_id = tc.hs_object_id
LEFT JOIN total_emails_cte te
    ON hubspot_companies.hs_object_id = te.hs_object_id
where hubspot_companies.is_deleted = 0
and hubspot_companies.sourcesystemid = 5
and hubspot_companies.archived = 'false'  
and hubspot_companies.hs_lastmodifieddate > (SELECT LastLoadDateValue - INTERVAL 1 DAY FROM Metadata.mastermetadata from where TableID="SL7")
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Full Load

-- COMMAND ----------

CREATE OR REPLACE VIEW Silver.VW_Silver_HubSpot_Carrier_Full_Pull AS  (
WITH total_calls_cte as 
(
    select hco.hs_object_id, count(Associated_ID) as Total_Calls, max(hca.hs_createdate) as Last_Call_Date 
    from bronze.hubspot_companies hco 
    left join bronze.hubspot_associations ha on hco.hs_object_id=ha.Company_ID 
    left join bronze.hubspot_calls hca on ha.Associated_ID=hca.hs_object_id 
    WHERE hco.is_deleted = 0 
      AND hco.sourcesystemid = 5 
      AND hco.archived = "false"
      AND ha.Is_Deleted=0
      AND ha.associated_type='Calls' 
      and hca.archived = "false" 
    GROUP BY hco.hs_object_id 
),
total_emails_cte as 
(
    select hco.hs_object_id, count(Associated_ID) as Total_Emails, max(ha.Created_Date) as Last_Email_Date 
    from bronze.hubspot_companies hco 
    left join bronze.hubspot_associations ha on hco.hs_object_id=ha.Company_ID 
    left join bronze.hubspot_emails hca on ha.Associated_ID=hca.hs_object_id 
    WHERE hco.is_deleted = 0 
      AND hco.sourcesystemid = 5 
      AND hco.archived = "false"
      AND ha.Is_Deleted=0
      AND ha.associated_type='Emails' 
      and hca.archived = "false" 
    GROUP BY hco.hs_object_id 
),
current_lifecycle_cte AS (
  SELECT
    l.Company_ID,
    l.Lifecycle_Stage_Value AS current_lifecyclestage,
    l.cycle_timestamp AS current_lifecyclestage_timestamp
  FROM bronze.hubspot_lifecyclestage l
  INNER JOIN (
    SELECT
      Company_ID,
      MAX(cycle_timestamp) AS max_cycle_timestamp
    FROM bronze.hubspot_lifecyclestage
    GROUP BY Company_ID
  ) lm
    ON l.Company_ID = lm.Company_ID
    AND l.cycle_timestamp = lm.max_cycle_timestamp
)
select 
    try_cast(hubspot_companies.hs_object_id as bigint) as HubSpot_Company_ID,
    try_cast(NULLIF(dot_number,"") as bigint) as Carrier_DOT, 
    NULLIF(concat("MC",(TRIM(REPLACE(mc_number, 'MC', '')))),"MC")  AS Carrier_MC, 
    try_cast(NULLIF(NULLIF(relay_carrier_id,"#N/A"),"") as string) as HubSpot_Relay_Carrier_ID,
    name AS Carrier_Name, 
    NULLIF(industry, "") as Carrier_Vertical_Industry,
    NULLIF(address, "") as Address_Line_1,
    NULLIF(address2, "") as Address_Line_2,
    NULLIF(city, "") as City,
    NULLIF(state, "") as State,
    NULLIF(country, "") as Country,
    NULLIF(zip, "") as Zip_Code,
    cam_owners.id as CAM_HubSpot_ID,
    NULLIF(cam_owners.email,'') as CAM_Email,
    concat(cam_owners.first_name, " " ,cam_owners.last_name) as CAM_Name,
    cdr_owners.id  as CDR_HubSpot_ID,
    NULLIF(cdr_owners.email,'') as CDR_Email,
    concat(cdr_owners.first_name, " " ,cdr_owners.last_name) as CDR_Name,
    NULLIF(hubspot_team_id,'') as HubSpot_Team_ID,
    NULLIF(contact_email, '') as Carrier_Email_Address,
    coalesce(NULLIF(phone, ''), NULLIF(contact_phone_number, '')) as Carrier_Phone_Number,
    automated_tracking_provider as Tracking_Platform,
    automated_tracking_status as Tracking_Platform_Status,
    try_cast(of_flatbeds as BIGINT)	as Flatbed_Count,
    try_cast(of_reefers as BIGINT)	as Reefer_Count,
    try_cast(of_tractors as BIGINT)	as Tractor_Count,
    try_cast(of_trailers as BIGINT)	as Trailer_Count,
    try_cast(of_vans as BIGINT) as Van_Count,
    hs_created_by_user_id as Account_Created_By_User_ID,
    try_cast(createdate as date) as Account_Created_Date, 
    try_cast(tc.Last_Call_Date as date) as Last_Call_Date,
    try_cast(tc.Total_Calls as int) as Total_Calls,
    try_cast(te.Last_Email_Date as date) as Last_Email_Date,
    try_cast(te.Total_Emails as int) as Total_Emails,
    NULLIF(current_lifecyclestage,"")	as Current_Lifecycle_Stage,
    CASE WHEN UPPER(NULLIF(current_lifecyclestage,"")) = "CUSTOMER" then 1 else 0
    end	as Is_Managed_Carrier,
    CASE WHEN UPPER(NULLIF(current_lifecyclestage,"")) = "CUSTOMER" then current_lifecyclestage_timestamp	else Null end as Managed_Carrier_Converted_Date,
    sha2(concat_ws('||',
            coalesce(Carrier_DOT, 0),
            coalesce(Carrier_MC, ''),
            coalesce(HubSpot_Relay_Carrier_ID, ''),
            coalesce(Carrier_Name, ''),
            coalesce(Carrier_Vertical_Industry, ''),
            coalesce(Address_Line_1, ''),
            coalesce(Address_Line_2, ''),
            coalesce(City, ''),
            coalesce(State, ''),
            coalesce(Country, ''),
            coalesce(Zip_Code, ''),
            coalesce(CAM_HubSpot_ID, 0),
            coalesce(CAM_Email, ''),
            coalesce(CAM_Name, ''),
            coalesce(CDR_HubSpot_ID, 0),
            coalesce(CDR_Email, ''),
            coalesce(CDR_Name, ''),
            coalesce(HubSpot_Team_ID, ''),
            coalesce(Carrier_Email_Address, ''),
            coalesce(Carrier_Phone_Number, ''),
            coalesce(Tracking_Platform, ''),
            coalesce(Tracking_Platform_Status, ''),
            coalesce(Flatbed_Count, 0),
            coalesce(Reefer_Count, 0),
            coalesce(Tractor_Count, 0),
            coalesce(Trailer_Count, 0),
            coalesce(Van_Count, 0),
            coalesce(Account_Created_By_User_ID, 0),
            coalesce(STRING(Account_Created_Date), ''),
            coalesce(STRING(Last_Call_Date),''),
            coalesce(Total_Calls,0),
            coalesce(STRING(Last_Email_Date)),
            coalesce(Total_Emails, 0),
            coalesce(Current_Lifecycle_Stage, ''),
            coalesce(Is_Managed_Carrier, 0),
            coalesce(STRING(Managed_Carrier_Converted_Date), '')
        ), 256) as Hashkey
from bronze.hubspot_companies
LEFT JOIN current_lifecycle_cte
    ON hubspot_companies.hs_object_id = current_lifecycle_cte.Company_ID
LEFT JOIN bronze.hubspot_owners as cam_owners
    ON cam_owners.id = COALESCE(
        NULLIF(cad_owner, ''),
        NULLIF(carrier_am, ''),
        NULLIF(carrier_am__2_, ''),
        NULLIF(ct_office_rep, ''),
        NULLIF(target_caam, ''),
        NULLIF(dray_owner, '')
    )
LEFT JOIN bronze.hubspot_owners as cdr_owners
    ON cdr_owners.id = COALESCE(
        NULLIF(cdr, ''),
        NULLIF(cdr__2_, ''),
        NULLIF(target_cdr, '')
    )
LEFT JOIN total_calls_cte tc
    ON hubspot_companies.hs_object_id = tc.hs_object_id
LEFT JOIN total_emails_cte te
    ON hubspot_companies.hs_object_id = te.hs_object_id
where hubspot_companies.is_deleted = 0
and hubspot_companies.sourcesystemid = 5
and hubspot_companies.archived = 'false'   
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DDL

-- COMMAND ----------

-- DBTITLE 1,DDL: Silver_HubSpot_Carriers
CREATE OR REPLACE TABLE Silver.Silver_HubSpot_Carriers (
    DW_HubSpot_Carrier_ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    HubSpot_Company_ID BIGINT,
    Carrier_DOT BIGINT,
    Carrier_MC VARCHAR(50),
    HubSpot_Relay_Carrier_ID VARCHAR(50),
    Carrier_Name VARCHAR(255),
    Carrier_Vertical_Industry VARCHAR(255),
    Address_Line_1 VARCHAR(255),
    Address_Line_2 VARCHAR(255),
    City VARCHAR(255),
    State VARCHAR(255),
    Country VARCHAR(255),
    Zip_Code VARCHAR(50),
    CAM_HubSpot_ID BIGINT,
    CAM_Email VARCHAR(255),
    CAM_Name VARCHAR(255),
    CDR_HubSpot_ID BIGINT,
    CDR_Email VARCHAR(255),
    CDR_Name VARCHAR(255),
    HubSpot_Team_ID VARCHAR(255),
    Carrier_Email_Address VARCHAR(255),
    Carrier_Phone_Number VARCHAR(255),
    Tracking_Platform VARCHAR(255),
    Tracking_Platform_Status VARCHAR(255),
    Flatbed_Count BIGINT,
    Reefer_Count BIGINT,
    Tractor_Count BIGINT,
    Trailer_Count BIGINT,
    Van_Count BIGINT,
    Account_Created_By_User_ID BIGINT,
    Account_Created_Date DATE,
    Last_Call_Date DATE,
    Total_Calls INT,
    Last_Email_Date DATE,
    Total_Emails INT,
    Current_Lifecycle_Stage VARCHAR(255),
    Is_Managed_Carrier INT,
    Managed_Carrier_Converted_Date TIMESTAMP_NTZ,
    Hashkey VARCHAR(255),
    Created_Date TIMESTAMP_NTZ,
    Created_By VARCHAR(255),
    Last_Modified_Date TIMESTAMP_NTZ,
    Last_Modified_By VARCHAR(255),
    Is_Deleted INT
);