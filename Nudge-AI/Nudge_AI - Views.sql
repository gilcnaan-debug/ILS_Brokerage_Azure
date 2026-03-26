-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### VW_Nudge_Summary_Awards

-- COMMAND ----------

CREATE OR REPLACE VIEW analytics.vw_nudge_summary_awards AS

WITH Nudge AS  ( SELECT 
        UPPER(Customer_master) AS CUSTOMER_MASTER,
        UPPER(CASE WHEN Criteria_Description <> 'NA' 
            THEN Criteria_Description 
            ELSE 
            CONCAT(
            coalesce(UPPER(origin_city), ''),
            ', ' ,
            coalesce(UPPER(Origin_State), ''),
            ', ' ,
            coalesce(UPPER(Origin_Country), ''), 
            ' - ', 
            coalesce(UPPER(dest_city), ''), 
            ', ',
            coalesce(UPPER(Dest_State), ''),
            ', ',
            coalesce(UPPER(Destination_Country), '')
            )
         END) AS Criteria_Description,
        RFP_Name,
        SUM(DISTINCT CAST(B.Periodical_Volume AS DECIMAL(10,5))) AS Periodical_Volume,
        COUNT(CASE WHEN load_id <> 0 then 1 else NULL end) AS Total,
        period_year,
        Periodic_Start_Date AS Award_Start_Date,
        Periodic_End_Date AS Award_End_Date
        
    FROM Gold.Fact_Awards_Summary LEFT JOIN (SELECT DISTINCT Equipment_Type, Awardkey, CAST(Periodical_Volume AS DECIMAL(10, 5)) AS Periodical_Volume from silver.silver_nfi_awards_time_table WHERE Is_Deleted = 0) b
    ON UPPER(Fact_Awards_Summary.Awardkey) = UPPER(b.Awardkey) and UPPER(Fact_Awards_Summary.Equipment_Type) = UPPER(b.Equipment_Type) AND RFP_Name <> 'No Awards'
    WHERE ((Is_Deleted= 1 and load_id = 0) OR Is_Deleted = 0)
    GROUP BY 
        UPPER(Customer_master),
        UPPER(CASE WHEN Criteria_Description <> 'NA' 
            THEN Criteria_Description 
            ELSE 
            CONCAT(
            coalesce(UPPER(origin_city), ''),
            ', ' ,
            coalesce(UPPER(Origin_State), ''),
            ', ' ,
            coalesce(UPPER(Origin_Country), ''), 
            ' - ', 
            coalesce(UPPER(dest_city), ''), 
            ', ',
            coalesce(UPPER(Dest_State), ''),
            ', ',
            coalesce(UPPER(Destination_Country), '')
            )
         END), 
        RFP_Name,
        -- b.Periodical_Volume,
        period_year,
        Periodic_Start_Date,
        Periodic_End_Date )

    SELECT 
        UPPER(Customer_master) as Shipper,
        Criteria_Description as `LaneCriteria`,
        RFP_Name as RFP,
        SUM(Periodical_Volume) AS EstimatedVolume,
        SUM(Total) AS ActualVolume,
        period_year as PeriodYear,
        Award_Start_Date as `AwardStartdate`,
        Award_End_Date as `AwardEnddate`,
        CONCAT(Award_Start_Date, ' - ', Award_End_Date) AS `AwardInterval`,
        CASE 
            WHEN SUM(Total) = 0 THEN 'Not Attempted'
            WHEN ABS(SUM(Total) - SUM(Periodical_Volume)) <= (SUM(Periodical_Volume) * 0.1) THEN 'Achieved'
            WHEN SUM(Total) > SUM(Periodical_Volume) THEN 'Overachieved'
            WHEN SUM(Total) < SUM(Periodical_Volume) THEN 'Underachieved'
            ELSE 'Unknown'
        END AS `PerformanceStatus`
    FROM 
        Nudge
    GROUP BY 
        Customer_master,
        Criteria_Description,
        RFP_Name,
        Award_Start_Date,
        Award_End_Date, 
        period_year


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##VW_Nudge_Summary_Awards

-- COMMAND ----------

CREATE OR REPLACE VIEW analytics.vw_nudge_summary_awards (
  Shipper,
  LaneCriteria,
  RFP,
  EstimatedVolume,
  ActualVolume,
  PeriodYear,
  AwardStartdate,
  AwardEnddate,
  AwardInterval,
  PerformanceStatus)
WITH SCHEMA COMPENSATION
AS WITH Nudge AS  ( SELECT 
        UPPER(Customer_master) AS CUSTOMER_MASTER,
        UPPER(CASE WHEN Criteria_Description <> 'NA' THEN Criteria_Description ELSE REPLACE(Load_Lane,'>','-') END) AS Criteria_Description,
        RFP_Name,
        SUM(DISTINCT CAST(B.Periodical_Volume AS DECIMAL(10,5))) AS Periodical_Volume,
        COUNT(CASE WHEN load_id <> 0 then 1 else NULL end) AS Total,
        period_year,
        Periodic_Start_Date AS Award_Start_Date,
        Periodic_End_Date AS Award_End_Date
        
    FROM Gold.Fact_Awards_Summary LEFT JOIN (SELECT DISTINCT Equipment_Type, Awardkey, CAST(Periodical_Volume AS DECIMAL(10, 5)) AS Periodical_Volume from silver.silver_nfi_awards_time_table WHERE Is_Deleted = 0) b
    ON UPPER(Fact_Awards_Summary.Awardkey) = UPPER(b.Awardkey) and UPPER(Fact_Awards_Summary.Equipment_Type) = UPPER(b.Equipment_Type)
    WHERE ((Is_Deleted= 1 and load_id = 0) OR Is_Deleted = 0)
    GROUP BY 
        UPPER(Customer_master),
        UPPER(CASE WHEN Criteria_Description <> 'NA' THEN Criteria_Description ELSE REPLACE(Load_Lane,'>','-') END),
        RFP_Name,
        -- b.Periodical_Volume,
        period_year,
        Periodic_Start_Date,
        Periodic_End_Date )

    SELECT 
        UPPER(Customer_master) as Shipper,
        Criteria_Description as `LaneCriteria`,
        RFP_Name as RFP,
        SUM(Periodical_Volume) AS EstimatedVolume,
        SUM(Total) AS ActualVolume,
        period_year as PeriodYear,
        Award_Start_Date as `AwardStartdate`,
        Award_End_Date as `AwardEnddate`,
        CONCAT(Award_Start_Date, ' - ', Award_End_Date) AS `AwardInterval`,
        CASE 
            WHEN SUM(Total) = 0 THEN 'Not Attempted'
            WHEN ABS(SUM(Total) - SUM(Periodical_Volume)) <= (SUM(Periodical_Volume) * 0.1) THEN 'Achieved'
            WHEN SUM(Total) > SUM(Periodical_Volume) THEN 'Overachieved'
            WHEN SUM(Total) < SUM(Periodical_Volume) THEN 'Underachieved'
            ELSE 'Unknown'
        END AS `PerformanceStatus`
    FROM 
        Nudge
    GROUP BY 
        Customer_master,
        Criteria_Description,
        RFP_Name,
        Award_Start_Date,
        Award_End_Date, 
        period_year


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## VW_Nudge_Awards_Lane

-- COMMAND ----------

CREATE OR REPLACE VIEW analytics.vw_nudge_awards_lane AS 
WITH Estimated_Lanes  AS (SELECT UPPER(Customer_Master) AS Customer_Master, RFP_Name, period_year, 
COUNT(DISTINCT UPPER(Criteria_Description)) AS Estimated_Lanes_Volume
FROM Gold.Fact_Awards_Summary where Is_Awarded = 'Awarded' and Is_Deleted = 0
 GROUP BY UPPER(customer_master),3,2 ORDER BY 1,3,2,4), 
Actual_Lanes AS (
  SELECT UPPER(customer_master) AS Customer_Master, RFP_Name, period_year, count(DISTINCT UPPER(Criteria_Description)) AS Actual_Lanes_Volume
    FROM Gold.Fact_Awards_Summary
    WHERE Is_Awarded = 'Awarded' AND load_id <> 0 and Is_Deleted = 0
    GROUP BY  UPPER(customer_master) , 2, 3
    ORDER BY  1, 2, 3, 4
)
SELECT
  UPPER(b.customer_master) as customer_master,
  b.RFP_Name,
  b.period_year,
  Estimated_Lanes_Volume,
  Actual_Lanes_Volume, 
  'Awarded' AS Is_Awarded
FROM
  actual_lanes a
  RIGHT JOIN Estimated_Lanes b ON UPPER(a.customer_master) = UPPER(b.customer_master)
  AND a.period_year = b.period_year
  AND a.RFP_Name = b.RFP_Name
  

UNION 

SELECT DISTINCT
  UPPER(customer_master) as customer_master,
  'No Awards' as RFP_Name,
  period_year,
  NULL AS Estimated_Lanes_Volume,
  COUNT(DISTINCT CONCAT(coalesce(UPPER(origin_city), ''), ', ', coalesce(UPPER(Origin_State), ''), ', ',coalesce(UPPER(Origin_Country), ''), ' - ', coalesce(UPPER(dest_city), ''),', ', coalesce(UPPER(Dest_State)), ''),', ',coalesce(UPPER(Destination_Country), '')) AS Actual_Lanes_Volume, 
  'NonAwarded' AS Is_Awarded
FROM 
Gold.Fact_Awards_Summary 
WHERE Is_Awarded = 'NonAwarded' AND load_id <> 0 AND DW_Validated_Award_ID is NULL AND IS_Deleted = 0
GROUP BY UPPER(customer_master), period_year


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##VW_Nudge_FactLoad_Awards

-- COMMAND ----------

Create or replace view  Analytics.Vw_Nudge_FactLoad_Awards as
Select F.customer_master as Shipper,
F.load_id as `LoadID`,
F.RFP_Name as RFP,
F.DW_Validated_Award_ID AwardID,
F.Validation_Criteria as `ValidationCriteria`,
F.Lane ,
F.origin_state as `OriginState`,
F.origin_city as `OriginCity`,
F.origin_zip as `OriginZip`,
F.origin_country as `OriginCountry`,
F.dest_state  as `DestinationState`,
F.dest_city  as `DestinationCity`,
F.dest_zip as `DestinationZip`,
F.Destination_Country as `DestinationCountry`,
F.TMS_Sourcesystem_name as `TMS`,
F.carrier_rep as `CarrierRep`,
F.customer_name as `ShipperName`,
F.Ship_date as `ShipDate`,
F.customer_office as `Office`,
F.load_status as `LoadStatus`,
F.carrier_office as `CarrierOffice`,
F.load_lane as `LoadLane`,
F.Equipment_Type as `Equipment`,
F.Mode ,
F.dot_num as `DotNumber`,
F.carrier_name as `CarrierName`,
F.Total_Expense as `TotalExpense`,
F.Total_Margin as `TotalMargin` ,
F.market_origin as `MarketOrigin`,
F.market_dest as `MarketDestination`,
F.Customer_rep  as `CustomerRep`,
F.delivery_date  as `DeliveryDate`,
F.invoice_date as `InvoiceDate`,
F.Booking_type as `BookingType`,
F.Load_flag as `LoadFlag`,
F.Total_Miles as `Total_Miles` ,
F.Sum_Revenue as `TotalRevenue`,
F.financial_period_number  as `PeriodNumber`,
F.Financial_Year as `FinancialYear`,
F.period_year  as `PeriodYear`,
F.Award_Type  as `AwardType`,
CAST(F.Periodical_Volume AS DECIMAL(10,2)) as `EstimatedVolume`,
F.Periodical_Revenue  as `EstimatedRevenue`,
F.Periodic_Start_Date as `AwardStartdate`,
F.Periodic_End_Date as `AwardEnddate`,
Concat(F.Periodic_Start_Date, '-',F.Periodic_End_Date) as `AwardInterval`,
F.Is_Awarded as `Category`,
CASE WHEN F.IS_Awarded= 'Awarded' THEN F.Criteria_Description ELSE CONCAT(Coalesce(F.Origin_City,''), ', ',coalesce(f.origin_state, ''),', ', coalesce(F.Origin_Country, ''), ' - ',coalesce(F.Dest_City,''), ', ', coalesce(F.dest_state,''), ', ', coalesce(F.Destination_Country, '')) END as `LaneCriteria`,
F.Awardkey ,
F.Agingkey,
N.`PerformanceStatus` from gold.fact_awards_summary F
LEFT JOIN analytics.vw_nudge_summary_awards AS N
  ON F.Criteria_Description = N.`LaneCriteria`
  AND F.period_year = N.`PeriodYear`
  AND  F.customer_master = N.Shipper
WHERE F.Is_Deleted = 0


 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##VW_Nudge_Status_Awards

-- COMMAND ----------

CREATE OR REPLACE VIEW analytics.vw_nudge_status_awards AS
WITH  Nudge AS (
    SELECT 
        Customer_master,
        Criteria_Description,
        RFP_Name,
        Periodical_Volume,
        SUM(CASE WHEN load_id <> 0 THEN 1 ELSE 0 END) AS Total,
        period_year,
        Periodic_Start_Date AS Award_Start_Date,
        Periodic_End_Date AS Award_End_Date
    FROM Gold.fact_awards_summary
    WHERE Is_Deleted = 0
    GROUP BY 
        Customer_master,
        Criteria_Description,
        RFP_Name,
        Periodical_Volume,
        period_year,
        Periodic_Start_Date,
        Periodic_End_Date
),

NudgeFinal as (
SELECT 
    RFP_Name as RFPName,
    Customer_master as Shipper,
    CURRENT_DATE AS Currentdate,
    Percentage_Completion as CompletionPercentage,
   SUM(EstimatedVolume) * 
try_divide(
    DATEDIFF(day, Award_Start_Date, LEAST(CURRENT_DATE, Award_End_Date)) + 1,
    DATEDIFF(day, Award_Start_Date, Award_End_Date) + 1
) AS EstimatedVolume,
 Sum(temp.ActualVolume)
    AS ActualVolume
FROM (
    SELECT 
        Customer_master,
        Criteria_Description,
        RFP_Name,
        SUM(Periodical_Volume) AS EstimatedVolume,
        SUM(Total) AS ActualVolume,
        period_year,
        Award_Start_Date,
        Award_End_Date, 
        DATEDIFF(day, Award_Start_Date, Award_End_Date) + 1 AS Number_of_Days, -- Total number of days in the award period
        DATEDIFF(day, Award_Start_Date, LEAST(CURRENT_DATE, Award_End_Date)) + 1 AS Current_Day_Number, -- Current day number within the date range
        CAST(
    try_divide(
        (DATEDIFF(day, Award_Start_Date, LEAST(CURRENT_DATE, Award_End_Date)) + 1) * 100.0, 
        (DATEDIFF(day, Award_Start_Date, Award_End_Date) + 1)
    ) AS DECIMAL(5, 2)
) AS Percentage_Completion, -- Percentage completion of the date range
        CASE 
            WHEN SUM(Total) = 0 THEN 'Not Attempted'
            WHEN ABS(SUM(Total) - SUM(Periodical_Volume)) <= (SUM(Periodical_Volume) * 0.1) THEN 'Achieved'
            WHEN SUM(Total) > SUM(Periodical_Volume) THEN 'Overachieved'
            WHEN SUM(Total) < SUM(Periodical_Volume) THEN 'Underachieved'
            ELSE 'Unknown'
        END AS Period_Performance_Flag 
    FROM 
        Nudge
    GROUP BY 
        Customer_master,
        Criteria_Description,
        RFP_Name,
        Award_Start_Date,
        Award_End_Date, 
        period_year 
) AS temp
GROUP BY 
    RFP_Name,
    Customer_master,
     Percentage_Completion,
    Award_Start_Date,
    Award_End_Date,
    CURRENT_DATE) 

Select *, try_divide(ActualVolume, EstimatedVolume) * 100 as AchievementPercentage
from NudgeFinal

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##VW_Awards_Potential_Aging

-- COMMAND ----------

CREATE OR REPLACE VIEW analytics.vw_awards_potential_aging AS

WITH Non_awarded AS  (SELECT DISTINCT a.*, CONCAT(coalesce(origin_city), ', ', coalesce(origin_state),', ', coalesce(origin_country), ' - ', coalesce(dest_city), ', ', coalesce(dest_state),', ', coalesce(destination_country)) AS lane_new,  b.financial_period_startdate
FROM Gold.Fact_Awards_Summary a LEFT JOIN analytics.dim_date b ON a.ship_Date = b.calendar_date
WHERE a.Is_Awarded = 'NonAwarded' and a.Is_Deleted = 0),


Loads_Interval AS (
    SELECT Upper(customer_master) as Customer_Master,CONCAT(coalesce(origin_city), ', ', coalesce(origin_state),', ', coalesce(origin_country), ' - ', coalesce(dest_city), ', ', coalesce(dest_state),', ', coalesce(destination_country)) AS lane, financial_period_number, financial_period_startdate, financial_period_startdate - INTERVAL '30 days' AS 30days_Interval, period_year 
    FROM non_awarded GROUP BY 1,2,3,4,5,6),

Potential_Aging  AS (
SELECT 
    l30.customer_master,
    l30.lane,
    l30.financial_period_number,
    l30.financial_period_startdate,
    l30.30days_Interval,
    RIGHT(l30.period_year, 4) as financial_year,
    l30.period_year,
    COUNT(CASE 
        WHEN l.Ship_date BETWEEN l30.30days_Interval AND l30.financial_period_startdate 
        THEN 1 
        ELSE NULL 
    END) AS load_count, 
    SUM(CASE 
        WHEN l.Ship_date BETWEEN l30.30days_Interval AND l30.financial_period_startdate 
        THEN l.total_margin 
        ELSE 0
    END) AS total_margin_sum,

    SUM(CASE 
        WHEN l.Ship_date BETWEEN l30.30days_Interval AND l30.financial_period_startdate 
        THEN l.sum_revenue
        ELSE 0
    END) AS Total_Sum_Revenue
    
FROM 
    Loads_Interval l30
RIGHT JOIN 
    Non_awarded l ON 
    l.customer_master = l30.customer_master AND
    l.Lane_new = l30.lane 
GROUP BY 
    l30.customer_master,
    l30.lane,
    l30.financial_period_number,
    l30.financial_period_startdate,
    l30.30days_Interval,
    l.Financial_Year,
    l30.period_year

HAVING COUNT(CASE 
        WHEN l.Ship_date BETWEEN l30.30days_Interval AND l30.financial_period_startdate 
        THEN 1 
        ELSE NULL 
    END) >= 10

ORDER BY 1,2,3,4,5,6 
),
lagged_data AS (
    SELECT *,
           LAG(Financial_Year) OVER (PARTITION BY customer_master, lane ORDER BY Financial_Year, financial_period_number) AS prev_year,
           LAG(financial_period_number) OVER (PARTITION BY customer_master, lane ORDER BY Financial_Year, financial_period_number) AS prev_month
    FROM (SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_master, lane ORDER BY Financial_Year, financial_period_number) AS row_num
    FROM Potential_Aging)
),
consecutive_flag AS (
    SELECT *,
           CASE 
               WHEN (Financial_Year = prev_year AND financial_period_number = prev_month + 1) 
                    OR (Financial_Year = prev_year + 1 AND financial_period_number = 1 AND prev_month = 12)
               THEN 1
               ELSE 0
           END AS is_consecutive
    FROM lagged_data
),
consecutive_groups AS (
    SELECT *,
           SUM(CASE WHEN is_consecutive = 0 THEN 1 ELSE 0 END) OVER (
               PARTITION BY customer_master, lane
               ORDER BY Financial_Year, financial_period_number
           ) AS group_id
    FROM consecutive_flag
),
cumulative_consecutive AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_master, lane, group_id
               ORDER BY Financial_Year, financial_period_number
           ) AS cumulative_consecutive
    FROM consecutive_groups
),
final_result AS (
    SELECT 
        customer_master,
        lane,
        financial_period_number,
        financial_period_startdate,
        30days_Interval,
        Financial_Year, 
        period_year,
        load_count,
        total_margin_sum,
        Total_Sum_Revenue,
        cumulative_consecutive AS age
    FROM cumulative_consecutive
),

Aging AS ( SELECT Customer_Master AS Customer_Name,
Lane AS Load_Lane,
financial_period_number AS Financial_Period,
Financial_Year,
Period_Year,
Load_Count,
Age,
Total_Sum_Revenue AS Total_Revenue ,
total_margin_sum AS Total_Margin  FROM final_result
ORDER BY customer_master, lane, Financial_Year, financial_period_number ) 

SELECT *, 
CASE
              WHEN Age BETWEEN 1 AND 5 THEN '1-5'
              WHEN Age BETWEEN 6 AND 11 THEN '6-11'
              WHEN Age BETWEEN 12 AND 17 THEN '12-17'
              WHEN Age  >= 18 THEN '18+'
              ELSE 'Not Eligible'
          END AS AgingBucket, 
Concat(Customer_Name,Period_year) as Agingkey

FROM 
Aging