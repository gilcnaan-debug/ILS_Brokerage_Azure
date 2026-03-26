# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Summary
# MAGIC * **Description:** To Transform data from Silver to Gold Summary as delta file
# MAGIC * **Created Date:** 07/10/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 07/10/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #### IMPORTING PACKAGES

# COMMAND ----------

#Importing Required packages
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC #### CALLING UTILITIES AND LOGGERS

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("NFI_Gold_Awards_Summary__")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC #### METADATA DETAILS FOR SUMMARY

# COMMAND ----------

#Getting the Table details in the dataframe that needs to be loaded into silver from Metadata Table
Table_ID_Summary = 'AW8'
DF_Metadata_Summary = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = 'AW8' and IsActive='1'")
Job_ID_Summary = DF_Metadata_Summary.select(col('JOB_ID')).where(col('TableID') == Table_ID_Summary).collect()[0].JOB_ID
Notebook_ID_Summary = DF_Metadata_Summary.select(col('NB_ID')).where(col('TableID') == Table_ID_Summary).collect()[0].NB_ID


# COMMAND ----------

TableName_Summary = DF_Metadata_Summary.select(col('DWHTableName')).where(col('TableID') == Table_ID_Summary).collect()[0].DWHTableName
MaxLoadDateColumn_Summary = DF_Metadata_Summary.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID_Summary).collect()[0].LastLoadDateColumn
MaxLoadDate_Summary = DF_Metadata_Summary.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID_Summary).collect()[0].LastLoadDateValue
Zone_Summary =DF_Metadata_Summary.select(col('Zone')).where(col('TableID') == Table_ID_Summary).collect()[0].Zone

# COMMAND ----------

# MAGIC %md
# MAGIC #### METADATA DETAILS FOR EXCEPTIONS

# COMMAND ----------

# Getting the Table details in the dataframe that needs to be loaded into silver from Metadata Table
Table_ID_Exception = 'AW4'
DF_Metadata_Exception = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = 'AW4' and IsActive='1'")
Job_ID_Exception = DF_Metadata_Exception.select(col('JOB_ID')).where(col('TableID') == Table_ID_Exception).collect()[0]['JOB_ID']
Notebook_ID_Exception = DF_Metadata_Exception.select(col('NB_ID')).where(col('TableID') == Table_ID_Exception).collect()[0]['NB_ID']


# COMMAND ----------

TableName_Exception = DF_Metadata_Exception.select(col('SourceTableName')).where(col('TableID') == Table_ID_Exception).collect()[0].SourceTableName
MaxLoadDateColumn_Exception = DF_Metadata_Exception.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID_Exception).collect()[0].LastLoadDateColumn
MaxLoadDate_Exception = DF_Metadata_Exception.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID_Exception).collect()[0].LastLoadDateValue
Zone_Exception = DF_Metadata_Exception.select(col('Zone')).where(col('TableID') == Table_ID_Exception).collect()[0].Zone

# COMMAND ----------

# MAGIC %md
# MAGIC #### REQUIRED TRANSFORMATION SCRIPTS

# COMMAND ----------

Summary_Scripts = '''
WITH Loads_with_country AS ( SELECT DISTINCT Load_id	,
    Ship_Date	,
    Mode	,
    Equipment	,
    Week_Num	,
    Customer_Office	,
    Carrier_Office	,
    Booked_By	,
    Customer_Master	,
    Customer_Name	,
    Employee	,
    load_Status	,
    Origin_City	,
    Origin_State	,
    Dest_City	,
    Dest_State	,
    load_lane	,
    Market_origin	,
    Market_Dest	,
    Market_Lane	,
    Delivery_Date	,
    rolled_date	,
    Booked_Date	,
    Tendered_Date	,
    bounced_date	,
    Cancelled_Date	,
    Days_Ahead	,
    prebookable	,
    PickUp_Start	,
    PickUp_End	,
    Drop_Start	,
    Drop_End	,
    invoiced_Date	,
    Booking_type	,
    Load_flag	,
    DOT_Num	,
    Carrier_Name	,
    Miles	,
    Revenue	,
    Expense	,
    Margin	,
    Linehaul	,
    Fuel_Surcharge	,
    Other_Fess	,
    SourceSystem_Name	,
     CASE
     -- For Origin Zip Code with Any Alphanumeric Value
     WHEN a.origin_zip RLIKE '[A-Za-z]' 
     OR 
     (a.Origin_Zip IS NULL AND UPPER(a.Origin_State) IN (SELECT DISTINCT UPPER(STATE_CODE) FROM Bronze.lookup_address_awards WHERE Country_Code = 'CA')) 
     -- (a.Origin_Zip IS NULL AND a.Origin_State IS NULL AND UPPER(a.Origin_City) IN ( select distinct UPPER(Place_Name) FROM Bronze.lookup_address_awards WHERE Country_Code = 'CA'))  
      THEN 'CA'

     -- For Origin State in Mexico (MX) based on alphanumeric Zip Code
     WHEN a.origin_state IN ("AG","BN","BS","CH","CI","CL","CP","CS","DF","DG","GE","GJ","HD","JA","MC","MR","MX","NA","NL","OA","PU","QE","QI","SI","SL","SO","TA","TB","TL","VC","YU","ZA") 
          AND a.origin_zip NOT LIKE '%[^0-9A-Za-z]%' THEN 'MX'

     -- Default to US if none of the above conditions match
     ELSE 'US'
     END AS Origin_Country,

     CASE
     -- For Destination Zip Code with Any Alphanumeric Value
     WHEN a.DEST_zip RLIKE '[A-Za-z]' 
     OR 
     (a.DEST_Zip IS NULL AND UPPER(a.Dest_State) IN (SELECT DISTINCT UPPER(STATE_CODE) FROM Bronze.lookup_address_awards WHERE Country_Code = 'CA'))
     -- (a.DEST_Zip IS NULL AND a.Dest_State IS NULL AND UPPER(a.Dest_City) IN ( select distinct UPPER(Place_Name) FROM Bronze.lookup_address_awards WHERE Country_Code = 'CA'))
     
      THEN 'CA'

     -- For Destination State in Mexico (MX) based on alphanumeric Zip Code
     WHEN a.dest_state IN ("AG","BN","BS","CH","CI","CL","CP","CS","DF","DG","GE","GJ","HD","JA","MC","MR","MX","NA","NL","OA","PU","QE","QI","SI","SL","SO","TA","TB","TL","VC","YU","ZA") 
          AND a.dest_zip NOT LIKE '%[^0-9A-Za-z]%' THEN 'MX'

     -- Default to US if none of the above conditions match
     ELSE 'US'
     END AS Destination_Country,
          CASE 
               WHEN REGEXP_LIKE(a.origin_zip, '^[0-9]+$') THEN LPAD(a.origin_zip, 5, '0')  -- If Origin_Zip is only digits, pad to 5 digits
               ELSE REPLACE(UPPER(TRIM(a.origin_zip)), ' ', '')  -- Else trim and remove spaces
          END AS Origin_Zip,
          CASE 
               WHEN REGEXP_LIKE(a.dest_zip, '^[0-9]+$') THEN LPAD(a.dest_zip, 5, '0')  -- If Origin_Zip is only digits, pad to 5 digits
               ELSE REPLACE(UPPER(TRIM(a.Dest_Zip)), ' ', '')  -- Else trim and remove spaces
        END AS dest_zip
        FROM analytics.fact_load_genai_nonsplit a
        ),
 
    Load_Details AS(
   SELECT DISTINCT
                UPPER(a.customer_master) AS Customer_Master,
                a.load_id   as load_id,
                CONCAT(coalesce(a.origin_state,''), '-', coalesce(a.dest_state,'')) AS Market_Lane,
                CONCAT(coalesce(a.origin_Zip,''), ', ',coalesce(a.origin_city,''), ', ', coalesce(a.origin_state,''), ', ', coalesce(a.origin_country,''), ' - ', coalesce(a.Dest_Zip,''), ', ',coalesce(a.dest_city,''), ', ', coalesce(a.dest_state),'', ', ', coalesce(a.Destination_Country,'')) AS Lane,
                a.origin_state,
                a.origin_city,
                a.origin_zip,
                a.origin_country,
                a.dest_state,
                a.dest_city,
                a.dest_zip,
                a.destination_country,
                a.Sourcesystem_name,
                a.booked_by as carrier_rep,
                a.customer_name,
                a.Ship_date,
                a.customer_office,
                a.load_status,
                a.carrier_office,
                a.load_lane,
                Case when a.Equipment = 'Dry Van' then 'Van' else a.Equipment end as Equipment_Type,
                a.Mode,
                a.dot_num,
                a.carrier_name,
                a.expense AS TotalExpense,
                a.margin AS TotalMargin,
                a.market_origin,
                a.market_dest,
                a.market_lane,
                a.Employee as Customer_rep,
                MAX(a.delivery_date) AS Delivery_Date,
                a.invoiced_date,
                a.Booking_type,
                a.Load_flag,
                a.miles AS TotalMiles,
                a.revenue AS Sum_Revenue,
                b.financial_period_number,
                RIGHT(b.period_year, 4) AS Financial_Year,
                b.period_year
                FROM
                Loads_with_country a
            JOIN
                analytics.dim_date b ON a.ship_date = b.Calendar_Date
            WHERE
                a.ship_date >= '2022-12-31'
                AND a.Mode = 'TL'
                AND (a.Equipment = 'Dry Van' OR a.Equipment = 'Reefer' OR a.Equipment  = 'IMDL' OR a.Equipment = 'Flatbed')
                AND a.load_flag = 1
                AND UPPER(a.SourceSystem_Name) <> 'TRANSFIX'

            GROUP BY 
                UPPER(a.Customer_Master),
                a.origin_state,
                a.origin_city,
                a.origin_zip,
                a.Origin_Country,
                a.dest_state,
                a.dest_city,
                a.dest_zip,
                a.destination_country,
                b.financial_period_number,
                RIGHT(b.period_year, 4),
                a.SourceSystem_Name,
                a.load_id,
                a.Employee,
                a.Booked_BY,
                a.customer_name,
                a.Ship_date,
                a.miles,
                a.Revenue,
                -- a.office,
                -- A.Office_Type,
                a.load_status,
                a.load_lane,
                a.Equipment,
                a.Mode,
                a.dot_num,
                a.carrier_name,
                a.Carrier_Office,
                a.expense,
                a.margin,
                a.market_origin,
                a.market_dest, 
                a.market_lane,
                a.Customer_Name,
                a.Customer_Office,
                --a.delivery_date,
                a.invoiced_date,
                a.Booking_type,
                a.Load_flag,
                b.period_year

),

Awards_Data AS (SELECT DISTINCT DW_Validated_Award_ID	,
RFP_Name	,
Customer_Slug	,
Customer_Master	,
SAM_Name	,
Equipment_Type	,
Estimated_Volume	,
Estimated_Miles	,
Linehaul	,
RPM	,
Flat_Rate	,
Min_Rate	,
Max_Rate	,
Period_Count	,
Periodical_Volume	,
Periodical_Revenue	,
Origin, 
Origin_Country,
Destination, 
Destination_Country,
Validation_Criteria	,
Award_Type	,
Award_Notes	,
Periodic_Start_Date	,
Periodic_End_Date	,
Shipper_Recurrence	,
Updated_By	,
Updated_Date	,
Period_Year	,
Start_Month	,
Start_Year	,
Start_Period_Year	,
End_Month	,
End_Year	,
End_Period_Year	,
Month	,
Year	,
Contract_Type	,
Expected_Margin	,
Awardkey	,
Agingkey	
FROM Silver.silver_nfi_awards_time_table WHERE Is_Deleted = 0
 ),

 Final AS(
                Select Distinct
                    UPPER(COALESCE(a.customer_master, B.Customer_Master)) as  Customer_Master,
                    a.load_id,
                    COALESCE(b.RFP_name,'No Awards') as RFP_Name,
					          b.SAM_Name,
                    b.DW_Validated_Award_ID,
                    b.Validation_Criteria,
                    b.Contract_Type,
                    a.Lane,
                    coalesce(a.origin_state, CASE 
                        WHEN b.Validation_Criteria LIKE 'city-%' AND LOAD_ID IS NULL THEN
                            CASE 
                                WHEN SPLIT(b.Origin, ',')[1] = 'None' THEN NULL
                                ELSE SPLIT(b.Origin, ',')[1]
                            END
                        ELSE NULL
                    END ) AS Origin_State,
                    coalesce(a.origin_city, CASE 
                        WHEN b.Validation_Criteria LIKE 'city-%' AND LOAD_ID IS NULL THEN
                            CASE 
                                WHEN SPLIT(b.Origin, ',')[0] = 'None' THEN NULL
                                ELSE SPLIT(b.Origin, ',')[0]
                            END
                        ELSE NULL
                    END) as origin_city,
                    coalesce(a.origin_zip, CASE WHEN b.Validation_Criteria LIKE '5digit-%'AND LOAD_ID IS NULL THEN b.origin ELSE NULL END) AS Origin_Zip,
                    coalesce(a.origin_country, b.origin_country) AS Origin_Country,
                    coalesce(a.dest_state, CASE 
                          WHEN b.Validation_Criteria LIKE '%-city' AND LOAD_ID IS NULL THEN
                              CASE 
                                  WHEN SPLIT(b.Destination, ',')[1] = 'None' THEN NULL
                                  ELSE SPLIT(b.Destination, ',')[1]
                              END
                          ELSE NULL
                      END) AS Dest_State,
                    coalesce(a.dest_city, CASE 
                        WHEN b.Validation_Criteria LIKE '%-city' AND LOAD_ID IS NULL THEN
                            CASE 
                                WHEN SPLIT(b.Destination, ',')[0] = 'None' THEN NULL
                                ELSE SPLIT(b.Destination, ',')[0]
                            END
                        ELSE NULL
                    END) AS Dest_City,
                    coalesce(a.dest_zip, CASE WHEN b.Validation_Criteria LIKE '5digit-%'AND LOAD_ID IS NULL THEN b.destination ELSE NULL END) AS Dest_Zip,
                    coalesce(a.destination_country, b.destination_country) AS Destination_Country,
                    a.Sourcesystem_name,
                    a.carrier_rep,
                    coalesce(a.customer_name, b.customer_master) as customer_name,
                    a.Ship_date,
                    a.customer_office,
                    a.load_status,
                    a.carrier_office,
                    a.load_lane,
                    coalesce(a.Equipment_Type,b.Equipment_Type) as Equipment_Type,
                    a.Mode,
                    a.dot_num,
                    a.carrier_name,
                    a.TotalExpense,
                    a.TotalMargin,
                    a.market_origin,
                    a.market_dest,
                    a.Customer_rep,
                    a.delivery_date,
                    a.invoiced_date,
                    a.Booking_type,
                    a.Load_flag,
                    a.TotalMiles,
                    a.Sum_Revenue,
                    coalesce(a.financial_period_number, b.Month) AS Financial_Period_Number,
                    coalesce(a.Financial_Year, b.Year) AS Financial_Year,
                    coalesce(a.period_year, b.period_year) as Period_Year,
                    b.Award_Type,
                    b.Periodical_Volume,
                    b.Periodical_Revenue,
                    b.Periodic_Start_Date,
                    b.Periodic_End_Date,
                    b.origin,
                    B.Destination

                    FROM Load_details a full outer Join Awards_Data b On
                    UPPER(a.Customer_Master) = UPPER(b.customer_master)
                    AND ((b.validation_criteria = 'city-city' 
                        AND UPPER(CONCAT(a.origin_city,",", a.origin_state)) = b.origin
                        AND UPPER(a.origin_country) = UPPER(B.Origin_Country)
                        AND UPPER(CONCAT(a.DEST_CITY,",", a.DEST_STATE)) = b.Destination
                        AND UPPER(a.destination_country) = UPPER(b.destination_country))
                    OR (b.validation_criteria = '5digit-5digit' 
                        AND UPPER(a.origin_zip) = UPPER(b.origin)) 
                        AND UPPER(a.origin_country) = UPPER(B.Origin_Country)
                        AND UPPER(a.Dest_Zip) = UPPER(b.Destination)
                        AND UPPER(a.destination_country) = UPPER(b.destination_country))
                    AND a.period_year = b.period_year
                    AND UPPER(a.Equipment_Type) = UPPER(b.Equipment_Type)
),


Clod As (
Select *, Concat(Customer_master,Coalesce(Criteria_Description, ' '),Period_year) as Awardkey ,  Concat(Customer_master,Period_year) as Agingkey from (

SELECT *,
    CASE WHEN RFP_name = 'No Awards' THEN 'NonAwarded' ELSE 'Awarded' END AS Is_Awarded,
            CASE 
                WHEN Validation_Criteria = '5digit-5digit' THEN CONCAT(
                    CASE 
                        WHEN REGEXP_LIKE(Origin_Zip, '^[0-9]+$') THEN LPAD(Origin_Zip, 5, '0')
                        ELSE REPLACE(TRIM(UPPER(Origin_Zip)), ' ', '')
                    END,', ',TRIM(UPPER(Origin_Country)), ' - ', 
                    CASE 
                        WHEN REGEXP_LIKE(Dest_Zip, '^[0-9]+$') THEN LPAD(Dest_Zip, 5, '0')
                        ELSE REPLACE(TRIM(UPPER(Dest_Zip)), ' ', '')
                    END,', ', TRIM(UPPER(Destination_Country)))
                WHEN Validation_Criteria = 'city-city' THEN CONCAT(
                        UPPER(origin_city),
                        ', ' ,
                        UPPER(Origin_State),
                        ', ' ,
                        UPPER(Origin_Country), 
                        ' - ', 
                        UPPER(dest_city),
                        ', ' ,
                        UPPER(dest_state),
                        ', ' ,
                        UPPER(destination_country)
                        )
                
            END AS Criteria_Description
FROM final)),

 Fact AS (
    SELECT 
        customer_master,
        SAM_Name,
        COALESCE(load_id, 0) AS load_id,
        RFP_Name,
        DW_Validated_Award_ID,
        COALESCE(Validation_Criteria, 'NA') AS Validation_Criteria,
        COALESCE(Contract_Type, 'NA') AS Contract_Type,
        COALESCE(Lane, 'Unknown') AS Lane,
        origin_state,
        origin_city,
        origin_zip,
        Origin_Country,
        dest_state,
        dest_city,
        dest_zip,
        destination_country,
        Sourcesystem_name,
        COALESCE(carrier_rep, 'Unknown') AS carrier_rep,
        customer_name,
        Ship_date,
        COALESCE(FIRST_VALUE(Customer_office) OVER (PARTITION BY customer_master ORDER BY ship_date DESC),'Unknown') as customer_office,
        load_status,
        carrier_office,
        load_lane,
        COALESCE(Equipment_Type, 'Unknown') AS Equipment_Type,
        COALESCE(Mode, 'Unknown') AS Mode,
        dot_num,
        carrier_name,
        TotalExpense,
        TotalMargin,
        market_origin,
        market_dest,
        COALESCE(FIRST_VALUE(Customer_rep) OVER (PARTITION BY customer_master ORDER BY ship_date DESC),'Unknown')  as dim_Customer_rep,
        delivery_date,
        invoiced_date,
        Booking_type,
        Load_flag,
        TotalMiles,
        Sum_Revenue,
        financial_period_number,
        Financial_Year,
        period_year,
        COALESCE(Award_Type, 'Unknown') AS Award_Type,
        Periodical_Volume,
        Periodical_Revenue,
        Periodic_Start_Date,
        Periodic_End_Date,
        Is_Awarded,
        COALESCE(Criteria_Description, 'NA') AS Criteria_Description,
        Awardkey,
        Agingkey,
        MAX(SAM_Name) OVER (PARTITION BY customer_master) AS filled_SAM_Name
    FROM Clod
     ORDER BY ship_date DESC
),

Final_Load as (SELECT 
    *,
    COALESCE(filled_SAM_Name, dim_Customer_rep) AS Customer_rep
FROM 
    Fact), 

ranked_data AS (
  SELECT 
    *,
    -- CONCAT(Contract_Type, '-', Validation_Criteria) AS combined_criteria,
    CASE 
      WHEN load_id <> 0 and Contract_Type = 'Primary' AND Validation_Criteria LIKE '5digit-5digit' THEN 1
      WHEN load_id <> 0 and Contract_Type = 'Primary' AND Validation_Criteria LIKE 'city-city' THEN 2
      WHEN load_id <> 0 and Contract_Type = 'Secondary' AND Validation_Criteria LIKE '5digit-5digit' THEN 3
      WHEN load_id <> 0 and Contract_Type = 'Secondary' AND Validation_Criteria LIKE 'city-city' THEN 4
      WHEN load_id <> 0 and Contract_Type = 'Backup' AND Validation_Criteria LIKE '5digit-5digit' THEN 5
      WHEN load_id <> 0 and Contract_Type = 'Backup' AND Validation_Criteria LIKE 'city-city' THEN 6
      WHEN load_id = 0 THEN 1
      WHEN load_id <> 0 AND DW_Validated_Award_ID IS NULL THEN 1 
      ELSE 7
    END AS priority_order
  FROM Final_Load
),
Priority as (SELECT 
  *,
  DENSE_RANK() OVER (PARTITION BY load_id ORDER BY priority_order) AS custom_rank
FROM ranked_data),

PreFinal AS (SELECT DISTINCT UPPER(customer_master) AS Customer_Master, SAM_Name, load_id, RFP_Name, DW_Validated_Award_ID, Validation_Criteria, Contract_Type, Lane, origin_state, origin_city, origin_zip, Origin_Country, dest_state, dest_city, dest_zip, destination_country, Sourcesystem_name, carrier_rep, customer_name, Ship_date, customer_office, load_status, carrier_office, load_lane, Equipment_Type, Mode, dot_num, carrier_name, TotalExpense, TotalMargin, market_origin, market_dest, dim_Customer_rep, delivery_date, invoiced_date, Booking_type, Load_flag, TotalMiles, Sum_Revenue, financial_period_number, Financial_Year, period_year, Award_Type, Periodical_Volume, Periodical_Revenue, Periodic_Start_Date, Periodic_End_Date, Is_Awarded, UPPER(Criteria_Description) AS Criteria_Description, Awardkey, Agingkey, filled_SAM_Name, Customer_rep FROM Priority WHERE custom_rank = 1),

Lane_Lookup AS (SELECT DISTINCT CASE WHEN Country_Code = 'CA' THEN REPLACE(Zip, ' ', '') ELSE LPAD(Zip, 5, 0) END AS ZIP, Place_Name, State_Code, country_code FROM Bronze.lookup_address_awards WHERE Country_Name in ('United States', 'Canada') AND Is_Deleted = 0 ),

Summary AS (SELECT 
Customer_Master, SAM_Name, load_id, RFP_Name, DW_Validated_Award_ID, Validation_Criteria, Contract_Type, Lane, origin_zip, Origin_Country, dest_zip, destination_country, Sourcesystem_name, carrier_rep, customer_name, Ship_date, customer_office, load_status, carrier_office, Equipment_Type, Mode, dot_num, carrier_name, TotalExpense, TotalMargin, market_origin, market_dest, dim_Customer_rep, delivery_date, invoiced_date, Booking_type, Load_flag, TotalMiles, Sum_Revenue, financial_period_number, Financial_Year, period_year, Award_Type, Periodical_Volume, Periodical_Revenue, Periodic_Start_Date, Periodic_End_Date, Is_Awarded, Criteria_Description, Awardkey, Agingkey, filled_SAM_Name, 
Customer_rep,
CASE WHEN Load_ID  = 0 
    AND DW_Validated_Award_ID is Not Null 
    THEN (CASE WHEN  Validation_Criteria = '5digit-5digit'
    THEN CONCAT(COALESCE(O_City.Place_name, ''), ',', COALESCE(O_City.State_Code,''), '>', COALESCE(D_City.Place_Name, ''), ',', COALESCE(D_City.State_Code, '') )
    ELSE CONCAT(COALESCE(Origin_City, ''), ',', COALESCE(Origin_State, ''),  '>' , COALESCE(Dest_City, ''), ',', COALESCE(Dest_State, '')) END)
    ELSE Load_Lane END AS Load_Lane,
CASE WHEN Load_ID  = 0 
    AND DW_Validated_Award_ID is Not Null 
    AND Validation_Criteria = '5digit-5digit'
    THEN O_City.State_Code  ELSE origin_state END AS Origin_State, 
CASE WHEN Load_ID  = 0 
    AND DW_Validated_Award_ID is Not Null 
    AND Validation_Criteria = '5digit-5digit'
    THEN O_city.Place_name ELSE origin_city END AS Origin_City,  
CASE WHEN Load_ID  = 0 
    AND DW_Validated_Award_ID is Not Null 
    AND Validation_Criteria = '5digit-5digit'
    THEN D_City.State_Code ELSE Dest_State END AS Dest_State, 
    
CASE WHEN Load_ID  = 0 
    AND DW_Validated_Award_ID is Not Null 
    AND Validation_Criteria = '5digit-5digit' 
    THEN D_City.Place_name ELSE dest_city END AS Dest_City
    
FROM PreFinal LEFT JOIN Lane_Lookup
                 O_City On UPPER(Prefinal.Origin_Zip) = UPPER(O_City.Zip)
                        AND Prefinal.Origin_Country = O_City.Country_Code
                        AND Load_ID  = 0 
                        AND DW_Validated_Award_ID is Not Null 
                        AND Validation_Criteria = '5digit-5digit'

    LEFT JOIN Lane_Lookup
                 D_City On UPPER(Prefinal.Dest_Zip) = UPPER(D_City.Zip) 
                        AND Prefinal.destination_country = D_City.Country_Code
                        AND Load_ID  = 0 
                        AND DW_Validated_Award_ID is Not Null 
                        AND Validation_Criteria = '5digit-5digit')


SELECT * FROM Summary
 '''



# COMMAND ----------


Summary_Final_Scripts = """ Select Distinct
                NULL AS DW_Validated_Award_ID,
                UPPER(a.customer_master) as  Customer_Master,
                NULL AS SAM_Name,
                a.load_id,
                'No Awards' as RFP_Name,
                'NA' AS Validation_Criteria,
                COALESCE(a.Lane, 'Unknown')  AS Lane,
                a.origin_state,
                a.origin_city,
                a.origin_zip,
                a.Origin_Country,
                a.dest_state,
                a.dest_city,
                a.dest_zip,
                a.Destination_Country,
                a.Sourcesystem_name,
                COALESCE(a.carrier_rep, 'Unknown') AS carrier_rep,
                a.customer_name,
                a.Ship_date,
                a.customer_office,
                a.load_status,
                a.carrier_office,
                a.load_lane,
                a.Equipment_Type,
                a.Mode,
                a.dot_num,
                a.carrier_name,
                a.TotalExpense,
                a.TotalMargin,
                a.market_origin,
                a.market_dest,
                a.Customer_rep,
                a.delivery_date,
                a.invoiced_date,
                a.Booking_type,
                a.Load_flag,
                a.TotalMiles,
                a.Sum_Revenue,
                a.financial_period_number,
                a.Financial_Year,
                a.period_year,
                'Unknown' AS Award_Type,
                NULL AS Periodical_Volume,
                NULL AS Periodical_Revenue,
                NULL AS Periodic_Start_Date,
                NULL AS Periodic_End_Date, 
                'NonAwarded' AS Is_Awarded, 
                'NA' AS Criteria_Description, 
                a.Awardkey, 
                a.Agingkey, 
                Filled_SAM_Name, 
                Dim_Customer_Rep,
                'NA' AS Contract_Type

                    FROM VW_Check a where load_id in (SELECT load_id FROM VW_CHECK WHERE load_id <> 0 GROUP BY load_id HAVING COUNT(load_id) > 1 ORDER BY load_id)

            UNION 

                SELECT DISTINCT DW_Validated_Award_ID	,
                Customer_Master	,
                SAM_Name	,
                Load_ID	,
                RFP_Name	,
                Validation_Criteria	,
                Lane	,
                Origin_State	,
                Origin_City	,
                Origin_Zip	,
                Origin_Country	,
                Dest_State	,
                Dest_City	,
                Dest_Zip	,
                Destination_Country	,
                SourceSystem_Name	,
                Carrier_Rep	,
                Customer_Name	,
                Ship_Date	,
                Customer_Office	,
                Load_Status	,
                Carrier_Office	,
                Load_Lane	,
                Equipment_Type	,
                Mode	,
                DOT_Num	,
                Carrier_Name	,
                TotalExpense	,
                TotalMargin	,
                Market_Origin	,
                Market_Dest	,
                Customer_Rep	,
                Delivery_Date	,
                InvoiceD_Date	,
                Booking_Type	,
                Load_Flag	,
                TotalMiles	,
                Sum_Revenue	,
                Financial_Period_Number	,
                Financial_Year	,
                Period_Year	,
                Award_Type	,
                Periodical_Volume	,
                Periodical_Revenue	,
                Periodic_Start_Date	,
                Periodic_End_Date	,
                Is_Awarded	,
                Criteria_Description	,
                Awardkey	,
                Agingkey	,
                Filled_SAM_Name	,
                Dim_Customer_Rep	,
                Contract_Type	
                FROM VW_CHECK WHERE load_id in (SELECT load_id FROM VW_CHECK GROUP BY Load_id having COUNT(load_id) = 1) or load_id = 0
                """




# COMMAND ----------


Exceptions_Scripts = """ 
    WITH Mutliple_Loads AS (
        SELECT load_id
        FROM VW_CHECK
        WHERE load_id <> 0
        GROUP BY load_id
        HAVING COUNT(load_id) > 1
    ),
    Validated_Id AS (
        SELECT DISTINCT DW_Validated_Award_ID
        FROM VW_CHECK
        WHERE load_id <> 0
        GROUP BY DW_Validated_Award_ID
        HAVING COUNT(load_id) > 1
    ),
    Award_Key AS (
        SELECT 
            s.Mergekey, 
            b.Load_id, 
            b.Period_Year
        FROM silver.silver_nfi_awards_validated s
        LEFT JOIN (
            SELECT DISTINCT 
                DW_Validated_Award_ID, 
                Load_id, 
                Period_Year
            FROM VW_CHECK
            WHERE load_id in (SELECT load_id FROM Mutliple_Loads)
        ) b 
        ON s.DW_Validated_Award_ID = b.DW_Validated_Award_ID
        WHERE b.DW_Validated_Award_ID IN (SELECT * FROM Validated_Id)
    )
    SELECT 
        f.DW_Award_ID, 
        f.Hashkey, 
        f.RFP_Name, 
        UPPER(f.Customer_Master) AS Customer_Master, 
        f.Customer_Slug, 
        f.SAM_Name, 
        f.Origin, 
        f.Origin_Country,
        f.Destination, 
        f.Destination_Country,
        f.Equipment_Type, 
        f.Estimated_Volume, 
        f.Estimated_Miles, 
        f.Linehaul, 
        f.RPM, 
        f.Flat_Rate, 
        f.Min_Rate, 
        f.Max_Rate, 
        f.Validation_Criteria, 
        f.Award_Type, 
        f.Award_Notes, 
        f.Periodic_Start_Date, 
        f.Periodic_End_Date, 
        f.Shipper_Recurrence, 
        f.Updated_By, 
        f.Updated_Date, 
        f.Contract_Type, 
        f.Expected_Margin, 
        f.Mergekey,
        CONCAT(
            'Single Load Matches with Multiple Awards', 
            ' Load_IDs:', concat_ws(', ', sort_array(collect_list(CAST(ak.Load_id AS STRING)))), 
            ' Period Year:', ak.Period_Year
        ) AS Exception_Type
    FROM Silver.silver_nfi_awards_filtered f
    LEFT JOIN Award_Key ak
        ON f.Mergekey = ak.Mergekey
    WHERE f.Mergekey IN (
        SELECT Mergekey
        FROM Silver.silver_nfi_awards_validated
        WHERE DW_Validated_Award_ID IN (
            SELECT DISTINCT DW_Validated_Award_ID
            FROM VW_CHECK
            WHERE load_id IN (SELECT load_id FROM Mutliple_Loads)
        )
    )
    AND f.Is_Deleted = 0
    GROUP BY 
        f.DW_Award_ID, 
        f.Hashkey, 
        f.RFP_Name, 
        f.Customer_Master, 
        f.Customer_Slug, 
        f.SAM_Name, 
        f.Origin, 
        f.Origin_Country,
        f.Destination, 
        f.Destination_Country,
        f.Equipment_Type, 
        f.Estimated_Volume, 
        f.Estimated_Miles, 
        f.Linehaul, 
        f.RPM, 
        f.Flat_Rate, 
        f.Min_Rate, 
        f.Max_Rate, 
        f.Validation_Criteria, 
        f.Award_Type, 
        f.Award_Notes, 
        f.Periodic_Start_Date, 
        f.Periodic_End_Date, 
        f.Shipper_Recurrence, 
        f.Updated_By, 
        f.Updated_Date, 
        f.Contract_Type, 
        f.Expected_Margin, 
        f.Mergekey,
        ak.period_year
    ORDER BY 
        f.DW_Award_ID;

"""



# COMMAND ----------

# MAGIC %md
# MAGIC ####MERGEKEY AND HASHKEY COLUMNS

# COMMAND ----------


Hashkey_Merge = ["customer_master","SAM_Name","load_id","RFP_Name","DW_Validated_Award_ID","Validation_Criteria","Contract_Type","Lane","origin_state","origin_city","origin_zip","Origin_Country","destination_country","dest_state","dest_city","dest_zip","Sourcesystem_name","carrier_rep","customer_name","Ship_date","customer_office","load_status","carrier_office","load_lane","Equipment_Type","Mode","dot_num","carrier_name","TotalExpense","TotalMargin","market_origin","market_dest","dim_Customer_rep","delivery_date","invoiced_date","Booking_type","Load_flag","TotalMiles","Sum_Revenue","financial_period_number","Financial_Year","period_year","Award_Type","Periodical_Volume","Periodical_Revenue","Periodic_Start_Date","Periodic_End_Date","Is_Awarded","Criteria_Description","Awardkey","Agingkey","filled_SAM_Name","Customer_rep"]


Mergekey_Merge = ["load_id", "DW_Validated_Award_ID", "Period_Year", "Origin_city", "Origin_state", "Dest_city", "Dest_state", "Dest_zip","Origin_Zip", "Origin_Country", "Destination_Country", "customer_master", "sourcesystem_name"]



# COMMAND ----------

# MAGIC %md
# MAGIC #### MERGE DATA INTO TARGET SUMMARY & EXCEPTIONS USING LOOPING

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(Table_ID_Summary,'Gold')
    if AutoSkipperCheck == 0:

        try:
            DF_Summary = spark.sql(Summary_Scripts)
            DF_Summary.createOrReplaceTempView('VW_Check')
        except Exception as e:
            print("Cant Process DF_Summary", e)

        try:     
            DF_Summary_Final = spark.sql(Summary_Final_Scripts)
        except Exception as e:
            print("Cant Process DF_Summary_Final", e)

        try:
            DF_Exceptions = spark.sql(Exceptions_Scripts)
            DF_Exceptions.createOrReplaceTempView('VW_Exceptions')
        except Exception as e:
            print("Cant process DF_Exceptions", e)
        
        try:
            DF_Summary_Final_Hash = DF_Summary_Final.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
            DF_Summary_Final_Merge = DF_Summary_Final_Hash.withColumn("Mergekey",md5(concat_ws("",*Mergekey_Merge)))
            DF_Summary_Final_Merge.createOrReplaceTempView('VW_Summary')
        except Exception as e:
            print("Cant Process DF_Summary_Final_Merge", e)
    
        try:
            # Merging the records with the actual table
            spark.sql('''
                        MERGE INTO Gold.Fact_Awards_Summary AS Target
                        USING VW_Summary AS Source
                        ON Target.Mergekey = Source.Mergekey 
                        WHEN MATCHED  and Target.Hashkey != Source.Hashkey THEN

                            UPDATE SET
                                Target.customer_master = Source.customer_master,
                                Target.SAM_Name = Source.SAM_Name,
                                Target.RFP_Name = Source.RFP_Name,
                                Target.Validation_Criteria = Source.Validation_Criteria,
                                Target.Contract_Type = Source.Contract_Type,
                                Target.Lane = Source.Lane,
                                Target.origin_state = Source.origin_state,
                                Target.origin_city = Source.origin_city,
                                Target.origin_zip = Source.origin_zip,
                                Target.Origin_Country = Source.Origin_Country,
                                Target.dest_state = Source.dest_state,
                                Target.dest_city = Source.dest_city,
                                Target.dest_zip = Source.dest_zip,
                                Target.Destination_Country = Source.Destination_Country,
                                Target.TMS_Sourcesystem_name = Source.Sourcesystem_name,
                                Target.carrier_rep = Source.carrier_rep,
                                Target.customer_name = Source.customer_name,
                                Target.Ship_date = Source.Ship_date,
                                Target.customer_office = Source.customer_office,
                                Target.load_status = Source.load_status,
                                Target.carrier_office = Source.carrier_office,
                                Target.load_lane = Source.load_lane,
                                Target.Equipment_Type = Source.Equipment_Type,
                                Target.Mode = Source.Mode,
                                Target.dot_num = Source.dot_num,
                                Target.carrier_name = Source.carrier_name,
                                Target.Total_Expense = Source.TotalExpense,
                                Target.Total_Margin = Source.TotalMargin,
                                Target.market_origin = Source.market_origin,
                                Target.market_dest = Source.market_dest,
                                Target.dim_Customer_rep = Source.dim_Customer_rep,
                                Target.delivery_date = Source.delivery_date,
                                Target.invoice_date = Source.invoiced_date,
                                Target.Booking_type = Source.Booking_type,
                                Target.Load_flag = Source.Load_flag,
                                Target.Total_Miles = Source.TotalMiles,
                                Target.Sum_Revenue = Source.Sum_Revenue,
                                Target.financial_period_number = Source.financial_period_number,
                                Target.Financial_Year = Source.Financial_Year,
                                Target.period_year = Source.period_year,
                                Target.Award_Type = Source.Award_Type,
                                Target.Periodical_Volume = Source.Periodical_Volume,
                                Target.Periodical_Revenue = Source.Periodical_Revenue,
                                Target.Periodic_Start_Date = Source.Periodic_Start_Date,
                                Target.Periodic_End_Date = Source.Periodic_End_Date,
                                Target.Is_Awarded = Source.Is_Awarded,
                                Target.Criteria_Description = Source.Criteria_Description,
                                Target.Awardkey = Source.Awardkey,
                                Target.Agingkey = Source.Agingkey,
                                Target.filled_SAM_Name = Source.filled_SAM_Name,
                                Target.Customer_rep = Source.Customer_rep,
                                Target.hashkey=Source.Hashkey, 
                                Target.Is_deleted = 0

                        WHEN MATCHED AND Source.Hashkey = Target.Hashkey AND Target.Is_Deleted = 1 THEN
                            UPDATE SET Target.Is_Deleted = 0, 
                            Target.Last_Modified_Date = current_timestamp(), 
                            Target.Last_Modified_By = 'Databricks'
            

                        WHEN NOT MATCHED  THEN
                            INSERT (
                                customer_master, SAM_Name, load_id, RFP_Name, DW_Validated_Award_ID, Validation_Criteria, Contract_Type, Lane, origin_state, origin_city, origin_zip, origin_country, dest_state, dest_city, dest_zip, destination_country, TMS_Sourcesystem_name, carrier_rep, customer_name, Ship_date, customer_office, load_status, carrier_office, load_lane, Equipment_Type, Mode, dot_num, carrier_name, Total_Expense, Total_Margin, market_origin, market_dest, dim_Customer_rep, delivery_date, invoice_date, Booking_type, Load_flag, Total_Miles, Sum_Revenue, financial_period_number, Financial_Year, period_year, Award_Type, Periodical_Volume, Periodical_Revenue, Periodic_Start_Date, Periodic_End_Date, Is_Awarded, Criteria_Description, Awardkey, Agingkey, filled_SAM_Name, Customer_rep, hashkey, mergekey, Sourcesystem_ID, Sourcesystem_Name, Created_By,Created_Date,Last_Modified_by,Last_Modified_Date, Is_Deleted
                            )
                            VALUES (
                                Source.customer_master, Source.SAM_Name, Source.load_id, Source.RFP_Name, Source.DW_Validated_Award_ID, Source.Validation_Criteria, Source.Contract_Type, Source.Lane, Source.origin_state, Source.origin_city, Source.origin_zip, Source.Origin_Country, Source.dest_state, Source.dest_city, Source.dest_zip, Source.Destination_Country, Source.Sourcesystem_name, Source.carrier_rep, Source.customer_name, Source.Ship_date, Source.customer_office, Source.load_status, Source.carrier_office, Source.load_lane, Source.Equipment_Type, Source.Mode, Source.dot_num, Source.carrier_name, Source.TotalExpense, Source.TotalMargin, Source.market_origin, Source.market_dest, Source.dim_Customer_rep, Source.delivery_date, Source.invoiced_date, Source.Booking_type, Source.Load_flag, Source.TotalMiles, Source.Sum_Revenue, Source.financial_period_number, Source.Financial_Year, Source.period_year, Source.Award_Type, Source.Periodical_Volume, Source.Periodical_Revenue, Source.Periodic_Start_Date, Source.Periodic_End_Date, Source.Is_Awarded, Source.Criteria_Description, Source.Awardkey, Source.Agingkey, Source.filled_SAM_Name, Source.Customer_rep, Source.hashkey, Source.mergekey, '9', 'Awards File', 'Databricks', current_Timestamp(), 'Databricks', CURRENT_TIMESTAMP(), 0
                            )
                    ''')


            # Source Delete Handling
            try:
                subquery = 'select DISTINCT t.Mergekey from Gold.Fact_Awards_Summary t left join VW_SUmmary s on t.Mergekey = s.Mergekey where s.Mergekey is null AND t.Is_Deleted = 0'
                df_delete= spark.sql(subquery)
                df_delete.createOrReplaceTempView('VW_delete')

                spark.sql('''MERGE INTO Gold.Fact_Awards_Summary AS target
                            USING VW_delete AS source
                            ON target.MergeKey = source.Mergekey
                            WHEN MATCHED THEN UPDATE SET 
                            target.Is_Deleted = 1,
                            target.Last_Modified_Date = current_timestamp()''')

            except Exception as e:
                print('Cant perform the delete', e)

            MaxDateQuery_Summary = "Select max({0}) as Max_Date from Gold.Fact_Awards_Summary"
            MaxDateQuery_Summary = MaxDateQuery_Summary.format(MaxLoadDateColumn_Summary)
            DF_MaxDate_Summary = spark.sql(MaxDateQuery_Summary)
            UpdateLastLoadDate(Table_ID_Summary,DF_MaxDate_Summary)
            UpdatePipelineStatusAndTime(Table_ID_Summary,'Gold')
            UpdateLogStatus(Job_ID_Summary, Table_ID_Summary, Notebook_ID_Summary, TableName_Summary, Zone_Summary, "Succeeded", None, None, 0, "Load") 

        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Summary table")
            print(e)
            Error_Statement = str(e).replace(',', '').replace("'", '')
            UpdateLogStatus(Job_ID_Summary, Table_ID_Summary, Notebook_ID_Summary, TableName_Summary, Zone_Summary, "Failed", Error_Statement, "NotIgnorable", 1, "Load")
            UpdateFailedStatus(Table_ID_Summary,'Gold')


except Exception as e:
        logger.info('Failed for Gold Summary')
        UpdateFailedStatus(Table_ID,'Gold')
        Error_Statement = str(e).replace(',', '').replace("'", '')
        UpdateLogStatus(Job_ID_Summary, Table_ID_Summary, Notebook_ID_Summary, TableName_Summary, Zone_Summary, "Failed", Error_Statement, "NotIgnorable", 1, "Load")
        print('Unable to load '+TableName_Summary)
        print(e)


# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(Table_ID_Exception,'Silver')
    if AutoSkipperCheck == 0:
    
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_NFI_Awards_Exceptions as CT using VW_Exceptions as CS ON CS.DW_Award_ID=CT.DW_Award_ID WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                    CT.Hashkey	=	CS.Hashkey,
                    CT.RFP_Name	=	CS.RFP_Name,
                    CT.Customer_Slug	=	CS.Customer_Slug,
                    CT.Customer_Master  =   CS.Customer_Master,
                    CT.SAM_Name	=	CS.SAM_Name,
                    CT.Origin	=	CS.Origin,
                    CT.Origin_Country	=	CS.Origin_Country,
                    CT.Destination	=	CS.Destination,
                    CT.Destination_Country	=	CS.Destination_Country,
                    CT.Equipment_Type	=	CS.Equipment_Type,
                    CT.Estimated_Volume	=	CS.Estimated_Volume,
                    CT.Estimated_Miles	=	CS.Estimated_Miles,
                    CT.Linehaul	=	CS.Linehaul,
                    CT.RPM	=	CS.RPM,
                    CT.Flat_Rate	=	CS.Flat_Rate,
                    CT.Min_Rate	=	CS.Min_Rate,
                    CT.Max_Rate	=	CS.Max_Rate,
                    CT.Validation_Criteria	=	CS.Validation_Criteria,
                    CT.Award_Type	=	CS.Award_Type,
                    CT.Award_Notes	=	CS.Award_Notes,
                    CT.Periodic_Start_Date	=	CS.Periodic_Start_Date,
                    CT.Periodic_End_Date	=	CS.Periodic_End_Date,
                    CT.Shipper_Recurrence	=	CS.Shipper_Recurrence,
                    CT.Updated_by	=	CS.Updated_by,
                    CT.Updated_date	=	CS.Updated_date,
                    CT.Contract_Type	=	CS.Contract_Type,
                    CT.Expected_Margin	=	CS.Expected_Margin,
                    CT.Exception_Type = CS.Exception_Type,
                    CT.Last_Modified_Date = current_timestamp(),
                    CT.Last_Modified_By = 'Databricks', 
                    CT.Is_Deleted = 0
                    
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   
                        CT.DW_Award_ID,
                        CT.Hashkey,
                        CT.RFP_Name,
                        CT.Customer_Slug,
                        CT.Customer_Master,
                        CT.SAM_Name,
                        CT.Origin,
                        CT.Origin_Country,
                        CT.Destination,
                        CT.Destination_Country,
                        CT.Equipment_Type,
                        CT.Estimated_Volume,
                        CT.Estimated_Miles,
                        CT.Linehaul,
                        CT.RPM,
                        CT.Flat_Rate,
                        CT.Min_Rate,
                        CT.Max_Rate,
                        CT.Validation_Criteria,
                        CT.Award_Type,
                        CT.Award_Notes,
                        CT.Periodic_Start_Date,
                        CT.Periodic_End_Date,
                        CT.Shipper_Recurrence,
                        CT.Updated_by,
                        CT.Updated_date,
                        CT.Contract_Type,
                        CT.Expected_Margin,
                        CT.Exception_Type,
                        CT.Sourcesystem_ID,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date,
                        CT.Is_Deleted
                        )
                        VALUES
                        (
                            CS.DW_Award_ID,
                            CS.Hashkey,
                            CS.RFP_Name,
                            CS.Customer_Slug,
                            CS.Customer_Master,
                            CS.SAM_Name,
                            CS.Origin,
                            CS.Origin_Country,
                            CS.Destination,
                            CS.Destination_Country,
                            CS.Equipment_Type,
                            CS.Estimated_Volume,
                            CS.Estimated_Miles,
                            CS.Linehaul,
                            CS.RPM,
                            CS.Flat_Rate,
                            CS.Min_Rate,
                            CS.Max_Rate,
                            CS.Validation_Criteria,
                            CS.Award_Type,
                            CS.Award_Notes,
                            CS.Periodic_Start_Date,
                            CS.Periodic_End_Date,
                            CS.Shipper_Recurrence,
                            CS.Updated_by,
                            CS.Updated_date,
                            CS.Contract_Type,
                            CS.Expected_Margin,
                            CS.Exception_Type,
                            9,
                            'Awards File',
                            'Databricks',
                            Current_Timestamp(),
                            'Databricks',
                            Current_Timestamp(),
                            0
                        )
                    ''')
            
            # Source Delete Handling
            spark.sql(f'''
                    UPDATE Silver.Silver_NFI_Awards_Exceptions
                    SET  Silver.Silver_NFI_Awards_Exceptions.Is_Deleted = '1', 
                        Silver.Silver_NFI_Awards_Exceptions.last_modified_date = Current_Timestamp()
                    where  Silver.Silver_NFI_Awards_Exceptions.DW_Award_ID in (select s.DW_Award_ID from Silver.Silver_NFI_Awards_Filtered s where S.Is_Deleted= '1') AND Silver.Silver_NFI_Awards_Exceptions.Is_Deleted = '0'
                ''')
            
            MaxDateQuery_Exception = "Select max({0}) as Max_Date from silver.Silver_NFI_Awards_Exceptions"
            MaxDateQuery_Exception = MaxDateQuery_Exception.format(MaxLoadDateColumn_Exception)
            DF_MaxDate_Exception = spark.sql(MaxDateQuery_Exception)
            UpdateLastLoadDate(Table_ID_Exception,DF_MaxDate_Exception)
            UpdatePipelineStatusAndTime(Table_ID_Exception,'Silver')
            UpdateLogStatus(Job_ID_Exception, Table_ID_Exception, Notebook_ID_Exception, TableName_Exception, Zone_Exception, "Succeeded", None, None, 0, "Load")


        except Exception as e:
            UpdateFailedStatus(Table_ID_Exception,'Silver')
            Error_Statement = str(e).replace(',', '').replace("'", '')
            UpdateLogStatus(Job_ID_Exception, Table_ID_Exception, Notebook_ID_Exception, TableName_Exception, Zone_Exception, "Failed", Error_Statement, "NotIgnorable", 1, "Load")
            print(e)
        

except Exception as e:
    logger.info('Failed for Silver load')
    UpdateFailedStatus(Table_ID_Exception,'Silver')
    print('Unable to load '+TableName_Exception)
    print(e)
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID_Exception, Table_ID_Exception, Notebook_ID_Exception, TableName_Exception, Zone_Exception, "Failed", Error_Statement, "NotIgnorable", 1, "Load")