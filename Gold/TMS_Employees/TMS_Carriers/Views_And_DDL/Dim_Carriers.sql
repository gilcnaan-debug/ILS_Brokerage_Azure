-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Dim Carriers
-- MAGIC * **Description:** Code for View and DDL for Loading Table in Gold Zone
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

-- DBTITLE 1,View: Dim_Carriers
CREATE OR REPLACE VIEW Gold.VW_Dim_Carrier AS
WITH Aljex_Carrier AS (
SELECT
    DOT_Number AS Carrier_DOT,
    Aljex_Carrier_ID,
    total_loads,
    last_load_date
FROM (
    SELECT
        DOT_Number,
        Carrier_ID as Aljex_Carrier_ID,
        COUNT(*) AS total_loads,
        MAX(ship_date) AS last_load_date, 
        max(tendered_date) as last_tendered_date,
        max(booked_date) as last_booked_date,
        max(actual_delivered_date) as last_delivered_date, 
        dense_rank() OVER (
            PARTITION BY DOT_Number
            ORDER BY COUNT(*) DESC, MAX(ship_date) desc, 
        max(tendered_date) desc,
        max(booked_date) desc,
        max(actual_delivered_date) desc
        ) AS rn
    FROM silver.silver_aljex
    WHERE is_deleted = 0 and dot_number <> 0
    GROUP BY DOT_Number, Carrier_ID
) t
WHERE rn = 1
),
Relay_Carrier AS (
    SELECT
        DOT_Number AS carrier_dot,
        Relay_Carrier_ID,
        total_loads,
        last_load_date
    FROM (
        SELECT
            DOT_Number,
            Carrier_ID AS Relay_Carrier_ID,
            COUNT(*) AS total_loads,
            MAX(ship_date) AS last_load_date, 
            max(tendered_date) as last_tendered_date,
            max(booked_date) as last_booked_date,
            max(actual_delivered_date) as last_delivered_date, 
            dense_rank() OVER (
            PARTITION BY DOT_Number
            ORDER BY COUNT(*) DESC, MAX(ship_date) desc, 
        max(tendered_date) desc,
        max(booked_date) desc,
        max(actual_delivered_date) desc
        ) AS rn
        FROM silver.silver_relay
        WHERE is_deleted = 0
          AND try_cast(dot_number AS BIGINT) IS NOT NULL
        GROUP BY DOT_Number, Carrier_ID
    ) t
    WHERE rn = 1
)
SELECT
DISTINCT
    sc.DW_Carrier_ID,
    sc.DW_HubSpot_Carrier_ID,
    sc.Carrier_DOT,
    sc.Carrier_MC, 
    sc.Aljex_Carrier_ID,
    sc.Relay_Carrier_ID,
    sc.Carrier_Name,
    sc.HubSpot_Account,
    sc.Carrier_Vertical_Industry,
    sc.Address_Line_1,
    sc.Address_Line_2,
    sc.City,
    sc.State,
    sc.Country,
    sc.Zip_Code,
    CASE 
        WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Aljex_ID
        ELSE sc.TMS_Rep_Aljex_ID
    END AS Carrier_Rep_Aljex_ID,
    CASE 
        WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Relay_ID
        ELSE sc.TMS_Rep_Relay_ID
    END AS Carrier_Rep_Relay_ID,
    CASE 
        WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Name
        ELSE sc.TMS_Rep_Name
    END AS Carrier_Rep_Name, 
    CASE
        WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN 'HubSpot'
        WHEN sc.TMS_Rep_Name IS NOT NULL THEN 'TMS'
        ELSE NULL
    END AS Carrier_Rep_Source_Flag,
    sc.CDR_Rep_Aljex_ID,
    sc.CDR_Rep_Relay_ID,
    sc.CDR_Rep_Name,
    sc.Carrier_Email_Address,
    sc.Carrier_Phone_Number,
    sc.Tracking_Platform,
    sc.Tracking_Platform_Status,
    sc.Flatbed_Count,
    sc.Reefer_Count,
    sc.Tractor_Count,
    sc.Trailer_Count,
    sc.Van_Count,
    sc.Is_Active,
    sc.Onboarded_Date,
    sc.Last_Load_Date,
    sc.Carrier_Regular_Office,
    sc.Carrier_Recent_Office,
    sc.Current_Lifecycle_Stage,
    sc.Is_Managed_Carrier,
    sc.Managed_Carrier_Converted_Date,
    sha2(concat_ws('||',
      coalesce(STRING(sc.DW_Carrier_ID), ''),
      coalesce(STRING(sc.DW_HubSpot_Carrier_ID), ''),
      coalesce(STRING(sc.Carrier_DOT), ''),
      coalesce(sc.Carrier_MC, ''),
      coalesce(ac.Aljex_Carrier_ID, ''),
      coalesce(rc.Relay_Carrier_ID, ''),
      coalesce(sc.Carrier_Name, ''),
      coalesce(sc.Carrier_Vertical_Industry, ''),
      coalesce(sc.Address_Line_1, ''),
      coalesce(sc.Address_Line_2, ''),
      coalesce(sc.City, ''),
      coalesce(sc.State, ''),
      coalesce(sc.Country, ''),
      coalesce(STRING(sc.Zip_Code), ''),
      coalesce(
        CASE 
            WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Aljex_ID
            ELSE sc.TMS_Rep_Aljex_ID
        END, ''),
      coalesce(
        CASE 
            WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Relay_ID
            ELSE sc.TMS_Rep_Relay_ID
        END, ''),
      coalesce(
        CASE 
            WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Name
            ELSE sc.TMS_Rep_Name
        END, ''),
      coalesce(sc.CDR_Rep_Aljex_ID, '' ),
      coalesce(sc.CDR_Rep_Relay_ID, ''),
      coalesce(sc.CDR_Rep_Name, ''),
      coalesce(sc.Carrier_Email_Address, ''),
      coalesce(sc.Carrier_Phone_Number, ''),
      coalesce(sc.Tracking_Platform, ''),
      coalesce(sc.Tracking_Platform_Status, ''),
      coalesce(STRING(sc.Flatbed_Count), ''),
      coalesce(STRING(sc.Reefer_Count), ''),
      coalesce(STRING(sc.Tractor_Count), ''),
      coalesce(STRING(sc.Van_Count), ''),
      coalesce(STRING(sc.Trailer_Count), ''),
      coalesce(STRING(sc.Is_Active), ''),
      coalesce(STRING(sc.Onboarded_Date), ''),
      coalesce(STRING(sc.Last_Load_Date), ''),
      coalesce(sc.Carrier_Regular_Office, ''),
      coalesce(sc.Carrier_Recent_Office, ''),
      coalesce(sc.Current_Lifecycle_Stage, ''),
      coalesce(STRING(sc.Is_Managed_Carrier), ''),
      coalesce(STRING(sc.Managed_Carrier_Converted_Date), '')
      ), 256) as Hashkey 
FROM Silver.Silver_Carriers sc
LEFT JOIN Aljex_Carrier ac ON sc.Carrier_DOT = ac.Carrier_DOT
LEFT JOIN Relay_Carrier rc ON sc.Carrier_DOT = rc.Carrier_DOT 
WHERE  
    sc.is_deleted=0
    AND sc.Last_Modified_Date > (SELECT LastLoadDateValue - INTERVAL 1 DAY FROM Metadata.mastermetadata WHERE TableID = "GL4")
    AND ((sc.Aljex_Carrier_ID = ac.Aljex_Carrier_ID) OR (sc.Aljex_Carrier_ID IS NULL AND ac.Aljex_Carrier_ID IS NULL))
    AND ((sc.Relay_Carrier_ID = rc.Relay_Carrier_ID) OR (sc.Relay_Carrier_ID IS NULL AND rc.Relay_Carrier_ID IS NULL))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Full Load

-- COMMAND ----------

CREATE OR REPLACE VIEW Gold.VW_Dim_Carrier_Full_Pull AS
WITH Aljex_Carrier AS (
SELECT
    DOT_Number AS Carrier_DOT,
    Aljex_Carrier_ID,
    total_loads,
    last_load_date
FROM (
    SELECT
        DOT_Number,
        Carrier_ID as Aljex_Carrier_ID,
        COUNT(*) AS total_loads,
        MAX(ship_date) AS last_load_date, 
        max(tendered_date) as last_tendered_date,
        max(booked_date) as last_booked_date,
        max(actual_delivered_date) as last_delivered_date, 
        dense_rank() OVER (
            PARTITION BY DOT_Number
            ORDER BY COUNT(*) DESC, MAX(ship_date) desc, 
        max(tendered_date) desc,
        max(booked_date) desc,
        max(actual_delivered_date) desc
        ) AS rn
    FROM silver.silver_aljex
    WHERE is_deleted = 0 and dot_number <> 0
    GROUP BY DOT_Number, Carrier_ID
) t
WHERE rn = 1
),
Relay_Carrier AS (
    SELECT
        DOT_Number AS carrier_dot,
        Relay_Carrier_ID,
        total_loads,
        last_load_date
    FROM (
        SELECT
            DOT_Number,
            Carrier_ID AS Relay_Carrier_ID,
            COUNT(*) AS total_loads,
            MAX(ship_date) AS last_load_date, 
            max(tendered_date) as last_tendered_date,
            max(booked_date) as last_booked_date,
            max(actual_delivered_date) as last_delivered_date, 
            dense_rank() OVER (
            PARTITION BY DOT_Number
            ORDER BY COUNT(*) DESC, MAX(ship_date) desc, 
        max(tendered_date) desc,
        max(booked_date) desc,
        max(actual_delivered_date) desc
        ) AS rn
        FROM silver.silver_relay
        WHERE is_deleted = 0
          AND try_cast(dot_number AS BIGINT) IS NOT NULL
        GROUP BY DOT_Number, Carrier_ID
    ) t
    WHERE rn = 1
)
SELECT
DISTINCT
    sc.DW_Carrier_ID,
    sc.DW_HubSpot_Carrier_ID,
    sc.Carrier_DOT,
    sc.Carrier_MC, 
    sc.Aljex_Carrier_ID,
    sc.Relay_Carrier_ID,
    sc.Carrier_Name,
    sc.HubSpot_Account,
    sc.Carrier_Vertical_Industry,
    sc.Address_Line_1,
    sc.Address_Line_2,
    sc.City,
    sc.State,
    sc.Country,
    sc.Zip_Code,
    CASE 
        WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Aljex_ID
        ELSE sc.TMS_Rep_Aljex_ID
    END AS Carrier_Rep_Aljex_ID,
    CASE 
        WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Relay_ID
        ELSE sc.TMS_Rep_Relay_ID
    END AS Carrier_Rep_Relay_ID,
    CASE 
        WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Name
        ELSE sc.TMS_Rep_Name
    END AS Carrier_Rep_Name, 
    CASE
        WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN 'HubSpot'
        WHEN sc.TMS_Rep_Name IS NOT NULL THEN 'TMS'
        ELSE NULL
    END AS Carrier_Rep_Source_Flag,
    sc.CDR_Rep_Aljex_ID,
    sc.CDR_Rep_Relay_ID,
    sc.CDR_Rep_Name,
    sc.Carrier_Email_Address,
    sc.Carrier_Phone_Number,
    sc.Tracking_Platform,
    sc.Tracking_Platform_Status,
    sc.Flatbed_Count,
    sc.Reefer_Count,
    sc.Tractor_Count,
    sc.Trailer_Count,
    sc.Van_Count,
    sc.Is_Active,
    sc.Onboarded_Date,
    sc.Last_Load_Date,
    sc.Carrier_Regular_Office,
    sc.Carrier_Recent_Office,
    sc.Current_Lifecycle_Stage,
    sc.Is_Managed_Carrier,
    sc.Managed_Carrier_Converted_Date,
    sha2(concat_ws('||',
      coalesce(STRING(sc.DW_Carrier_ID), ''),
      coalesce(STRING(sc.DW_HubSpot_Carrier_ID), ''),
      coalesce(STRING(sc.Carrier_DOT), ''),
      coalesce(sc.Carrier_MC, ''),
      coalesce(ac.Aljex_Carrier_ID, ''),
      coalesce(rc.Relay_Carrier_ID, ''),
      coalesce(sc.Carrier_Name, ''),
      coalesce(sc.Carrier_Vertical_Industry, ''),
      coalesce(sc.Address_Line_1, ''),
      coalesce(sc.Address_Line_2, ''),
      coalesce(sc.City, ''),
      coalesce(sc.State, ''),
      coalesce(sc.Country, ''),
      coalesce(STRING(sc.Zip_Code), ''),
      coalesce(
        CASE 
            WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Aljex_ID
            ELSE sc.TMS_Rep_Aljex_ID
        END, ''),
      coalesce(
        CASE 
            WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Relay_ID
            ELSE sc.TMS_Rep_Relay_ID
        END, ''),
      coalesce(
        CASE 
            WHEN sc.HubSpot_Rep_Name IS NOT NULL THEN sc.HubSpot_Rep_Name
            ELSE sc.TMS_Rep_Name
        END, ''),
      coalesce(sc.CDR_Rep_Aljex_ID, '' ),
      coalesce(sc.CDR_Rep_Relay_ID, ''),
      coalesce(sc.CDR_Rep_Name, ''),
      coalesce(sc.Carrier_Email_Address, ''),
      coalesce(sc.Carrier_Phone_Number, ''),
      coalesce(sc.Tracking_Platform, ''),
      coalesce(sc.Tracking_Platform_Status, ''),
      coalesce(STRING(sc.Flatbed_Count), ''),
      coalesce(STRING(sc.Reefer_Count), ''),
      coalesce(STRING(sc.Tractor_Count), ''),
      coalesce(STRING(sc.Van_Count), ''),
      coalesce(STRING(sc.Trailer_Count), ''),
      coalesce(STRING(sc.Is_Active), ''),
      coalesce(STRING(sc.Onboarded_Date), ''),
      coalesce(STRING(sc.Last_Load_Date), ''),
      coalesce(sc.Carrier_Regular_Office, ''),
      coalesce(sc.Carrier_Recent_Office, ''),
      coalesce(sc.Current_Lifecycle_Stage, ''),
      coalesce(STRING(sc.Is_Managed_Carrier), ''),
      coalesce(STRING(sc.Managed_Carrier_Converted_Date), '')
      ), 256) as Hashkey 
FROM Silver.Silver_Carriers sc
LEFT JOIN Aljex_Carrier ac ON sc.Carrier_DOT = ac.Carrier_DOT
LEFT JOIN Relay_Carrier rc ON sc.Carrier_DOT = rc.Carrier_DOT 
WHERE  
    sc.is_deleted=0 
    AND ((sc.Aljex_Carrier_ID = ac.Aljex_Carrier_ID) OR (sc.Aljex_Carrier_ID IS NULL AND ac.Aljex_Carrier_ID IS NULL))
    AND ((sc.Relay_Carrier_ID = rc.Relay_Carrier_ID) OR (sc.Relay_Carrier_ID IS NULL AND rc.Relay_Carrier_ID IS NULL))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DDL

-- COMMAND ----------

-- DBTITLE 1,DDL: Dim_Carriers
CREATE OR REPLACE TABLE Gold.Dim_Carriers (
    DW_Carrier_ID BIGINT PRIMARY KEY,
    DW_HubSpot_Carrier_ID BIGINT,
    Carrier_DOT BIGINT,
    Carrier_MC VARCHAR(50),
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
    Carrier_Rep_Aljex_ID VARCHAR(50),
    Carrier_Rep_Relay_ID VARCHAR(50),
    Carrier_Rep_Name VARCHAR(255), 
    Carrier_Rep_Source_Flag VARCHAR(50),
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
    Created_Date TIMESTAMP_NTZ,
    Created_By VARCHAR(255),
    Last_Modified_Date TIMESTAMP_NTZ,
    Last_Modified_By VARCHAR(255),
    Is_Deleted INT
);