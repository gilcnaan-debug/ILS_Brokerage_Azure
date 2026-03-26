-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Reporting Views
-- MAGIC * **Description:** To bulid a code that extracts req data from source for Carrier KPI report
-- MAGIC * **Created Date:** 06/25/2025
-- MAGIC * **Created By:** Gagana Nair
-- MAGIC * **Modified Date:** 07/15/2024
-- MAGIC * **Modified By:** Gagana Nair
-- MAGIC * **Changes Made:** 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact_Carrier_Loads

-- COMMAND ----------

create or replace table analytics.Fact_Carrier_Loads as
 (
 with temp2 as (
  select
l.Load_id,
l.Ship_Date,
l.Mode,
l.Equipment,
l.Week_Num,
l.Customer_Office,
l.Carrier_Office,
l.Booked_By,
l.Customer_Master,
l.Customer_Name,
l.Employee,
l.Salesrep,
l.load_Status,
l.Origin_City,
l.Origin_State,
l.Dest_City,
l.Dest_State,
l.Origin_Zip,
l.Dest_Zip,
l.load_lane,
l.Market_origin,
l.Market_Dest,
l.Market_Lane,
l.Delivery_Date,
l.rolled_date,
l.Booked_Date,
l.Tendered_Date,
l.bounced_date,
l.Cancelled_Date,
l.Pickup_Appointment_Date,
l.scheduled_Date,
l.PickUp_Start,
l.PickUp_End,
l.pickup_datetime,
l.ot_pu,
l.Drop_Start,
l.Drop_End,
l.Delivery_Datetime,
l.ot_del,
l.invoiced_Date,
l.Booking_type,
l.Load_flag,
l.DOT_Num,
l.Carrier_Name,
l.Miles,
l.Revenue,
l.Expense,
l.Margin,
l.Linehaul,
l.Fuel_Surcharge,
l.Other_Fess,
l.Trailer_Num,
l.Po_Number,
l.consignee_name,
l.Delivery_Address,
l.pickup_Address,
l.Mc_Number,
l.SourceSystem_Name,
l.prebookable,
l.Days_Ahead,
    Role_Power_BI,
    CASE
    WHEN Sourcesystem_name = 'ALJEX' 
         AND TRY_CAST(Booked_Date AS DATE) < TRY_CAST(Ship_date AS DATE) 
         AND TRY_CAST(Tendered_date AS DATE) < TRY_CAST(Ship_date AS DATE) THEN 'PreBooked'
    WHEN Sourcesystem_name = 'RELAY'
         AND TRY_CAST(Booked_Date AS DATE) < TRY_CAST(Ship_date AS DATE) 
         AND TRY_CAST(Scheduled_date AS DATE) < TRY_CAST(Ship_date AS DATE) THEN 'PreBooked'
    ELSE 'SameDay'
    END AS PreBookStatus,
    CASE
    WHEN Sourcesystem_name = 'ALJEX' 
         AND TRY_CAST(Tendered_Date AS DATE) < TRY_CAST(Ship_date AS DATE) THEN 'Include' 
    WHEN Sourcesystem_name = 'RELAY' THEN 
        CASE
            WHEN TRY_CAST(Scheduled_Date AS DATE) IS NULL 
                 OR TRY_CAST(Scheduled_Date AS DATE) > TRY_CAST(Ship_date AS DATE) THEN
                 CASE 
                     WHEN TRY_CAST(Tendered_Date AS DATE) < TRY_CAST(Ship_date AS DATE) THEN 'Include'
                     ELSE 'Exclude' 
                 END
            WHEN TRY_CAST(Scheduled_Date AS DATE) IS NOT NULL 
                 AND TRY_CAST(Scheduled_Date AS DATE) < TRY_CAST(Ship_date AS DATE) THEN 'Include'
            ELSE 'Exclude'
        END
    ELSE 'Exclude'
    END AS PreBookStatus_Include,
    datediff(
      try_cast(Ship_Date as date),
      try_cast(Tendered_Date as date)
    ) as Days_ahead_V2,
    coalesce(
      case
        when mb.Max_buy_rate >= mr.Market_Buy_Rate *.70
        and mb.Max_buy_rate <= mr.Market_Buy_Rate * 1.3 then mb.Max_buy_rate
        else null
      end,
      case
        when mb.Max_buy_rate >= Expense *.70
        and mb.Max_buy_rate <= Expense * 1.3 then mb.Max_buy_rate
        else null
      end
    ) as Max_buy_rate,
    case
      when coalesce(
        case
          when mb.Max_buy_rate >= mr.Market_Buy_Rate *.70
          and mb.Max_buy_rate <= mr.Market_Buy_Rate * 1.3 then mb.Max_buy_rate
          else null
        end,
        case
          when mb.Max_buy_rate >= Expense *.70
          and mb.Max_buy_rate <= Expense * 1.3 then mb.Max_buy_rate
          else null
        end
      ) is not null then 1
      else 0
    end as Max_buy_Flag,
    case
      when mr.Market_Buy_Rate >= Expense *.70
      and mr.Market_Buy_Rate <= Expense * 1.3 then mr.Market_Buy_Rate
      else null
    end as Market_Buy_Rate,
    mr.Market_Buy_Rate as original_Market_Buy_Rate,
    mb.Max_buy_rate as original_Max_buy_rate,
    case
      when (
        case
          when mr.Market_Buy_Rate >= Expense *.70
          and mr.Market_Buy_Rate <= Expense * 1.3 then mr.Market_Buy_Rate
          else null
        end
      ) is not null then 1
      else 0
    end as Markey_buy_rate_flag,
    assi.Assignee,
    case
      when L.Booked_By = assi.Assignee then 1
      else 0
    end as SameAssignedRep_or_diffAssignedRep,
    off.*,
    case
      when coalesce(
        case
          when mb.Max_buy_rate >= mr.Market_Buy_Rate *.70
          and mb.Max_buy_rate <= mr.Market_Buy_Rate * 1.3 then mb.Max_buy_rate
          else null
        end,
        case
          when mb.Max_buy_rate >= Expense *.70
          and mb.Max_buy_rate <= Expense * 1.3 then mb.Max_buy_rate
          else null
        end
      ) >= L.Expense then 1
      else 0
    end as Max_buy_Over_status_Flag,
    Case
      when mr.Market_Buy_Rate >= L.Expense then 1
      else 0
    end as Market_Buy_Over_status_Flag,
    dte.*,
    emp.*
  from
    `analytics`.`fact_load_genai_nonsplit` as L
    left join (
      select
        relay_reference_number,
        max(coalesce(max_buy, 0) / 100) as Max_buy_rate,
        1 as Max_Buy_Flag
      from
        `bronze`.`max_buy_projection`
      group by
        relay_reference_number
    ) as MB on l.load_id = MB.relay_reference_number
    left join (
      select
        external_load_id as MR_id,
        max(high_buy_rate_gsn) * max(miles_predicted) as Market_Buy_Rate,
        1 as Markey_buy_rate_flag
      from
        `bronze`.`pricing_nfi_load_predictions`
      group by
        external_load_id
    ) as mr on l.load_id = mr.MR_id
    left join (
      select
        relay_reference_number as offer_id,
        offered_by_name,
        count(1) as Offers
      from
        analytics.vw_reporting_Fact_load_v9
      group by
        relay_reference_number,
        offered_by_name
    ) as off on L.load_id = off.offer_id
    and l.Booked_By = off.offered_by_name
    left join (
      select
        a.relay_reference_number as Assi_id,
        max(b.AssigneeName) as Assignee
      from
        `bronze`.`planning_current_assignment` as a
        left join (
          select
            distinct offered_by,
            offered_by_name as AssigneeName
          from
            `bronze`.`offer_negotiation_reflected`
        ) as b on try_cast(a.assigned_by as int) = b.offered_by
      group by
        relay_reference_number
    ) as assi on l.load_id = assi.Assi_id
    left join (
      select distinct
        calendar_date,
        Financial_Weekenddate as Week_End_Date,
        period_year
      from
        analytics.dim_date_carrier
    ) as dte on l.Ship_Date = dte.calendar_date
    left join (
      select
        *,
    case
        when lower(Group) = 'capacity account management' AND Carrier_KPI_Flag = 1 then 'CAM'
        when lower(Group) = 'capacity sales' AND Carrier_KPI_Flag = 1 then 'CDR'
        else 'Others'
    end as Role_Power_BI,
        case
          when date_diff(DAY, date_in_job, current_timestamp()) > 365 * 2 then "2+"
          else "0-2"
        end as 2plusyearsflag,
        concat_WS(
          case
            when date_diff(DAY, date_in_job, current_timestamp()) > 365 * 2 then "2+"
            else "0-2"
          end,
          "-",case
            when lower(Job_Title) like '%dev%'
            and lower(Job_Title) like 'ca%' then "CDR"
            else "CAAM"
          end
        ) as Target_Key
      from
        `analytics`.`dim_employee_brokerage_mapping`
    ) as emp on l.booked_by = emp.TMS_Name

),

huspot_cycle as ( 
Select dot_number, lifecycle_stage_value, cycle_timestamp, calendar_date, period_year as hubspot_period_year 
 From (
   select dot_number,
   Lifecycle_Stage_Value,
   from_utc_timestamp(cycle_timestamp, 'CST') as cycle_timestamp,
   row_number() OVER (PARTITION BY dot_number, Lifecycle_Stage_Value ORDER BY  from_utc_timestamp(cycle_timestamp, 'CST')) AS rank
   From bronze.hubspot_lifecyclestage 
   where Lifecycle_Stage_Value = 'customer' )
 join 
 analytics.dim_date on date_format(cycle_timestamp,'yyyy-MM-dd') = dim_date.calendar_date
 where rank = 1),

Hubspot_lifecyclestage2 AS (
Select * from 
(
Select 
Calendar_date, dot_number, Lifecycle_Stage_Value, cycle_date, Cast(start_date as date) Start_date, Cast(End_date as date) End_date,
        ROW_NUMBER() OVER (PARTITION BY D.Calendar_Date, dot_number ORDER BY Cycle_date desc) AS rn
FROM 
analytics.dim_date D
Right Join
(
    Select
    hb1.dot_number, Lifecycle_Stage_Value, (from_utc_timestamp(cycle_timestamp, 'CST')) AS cycle_date, max_cycle_timestamp Start_date,
    CASE 
        WHEN Next_timestamp IS NULL THEN GETDATE() 
        ELSE DATEADD(DAY, -1, Next_timestamp) 
    END AS End_date
    From bronze.hubspot_lifecyclestage hb1
    Right Join 
    (
        SELECT
        dot_number,
        MAX(from_utc_timestamp(cycle_timestamp, 'CST')) AS max_cycle_timestamp,
        LEAD(MAX(from_utc_timestamp(cycle_timestamp, 'CST'))) OVER (PARTITION BY dot_number ORDER BY DATE(from_utc_timestamp(cycle_timestamp, 'CST'))) AS Next_timestamp
        FROM bronze.hubspot_lifecyclestage
        GROUP BY
        dot_number,
        DATE(from_utc_timestamp(cycle_timestamp, 'CST'))
    ) hb2 
    on hb1.Dot_Number = hb2.dot_number 
    AND from_utc_timestamp(hb1.cycle_timestamp, 'CST') = hb2.max_cycle_timestamp
) Hb 
on D.Calendar_Date Between Cast(Hb.Start_date as date) AND Cast(Hb.End_date as date)
Where Calendar_date Between '2020-01-01' and getdate() 
        AND hb.Dot_Number is not null
        AND hb.Dot_Number != '' 
Order by Calendar_date, Hb.dot_number
)
Where rn = 1
),

Carrier as ( select
Load_ID As Load_Number	,
Ship_Date	,
Mode	,
Equipment	,
Week_Num	,
Customer_Office	,
Carrier_Office	,
Booked_By	,
customer_master	,
customer_name	,
Employee AS customer_rep	,
load_status	,
Origin_City	,
Origin_State	,
Dest_City	,
Dest_State	,
Origin_Zip	,
Dest_Zip	,
load_lane	,
Market_origin	,
Market_Dest as Market_Deat	,
Market_Lane	,
Delivery_Date	,
rolled_date	,
Booked_Date	,
Tendered_Date	,
scheduled_date  ,
bounced_date	,
Cancelled_Date	,
Days_Ahead	,
prebookable	,
Pickup_Start as Pickup_Date	,
Pickup_End as Pickup_EndDate	,
Drop_Start	,
Drop_End	,
invoiced_Date	,
Booking_type	,
Load_flag	,
DOT_Num	,
Carrier_Name as Carrier	,
Miles	,
Revenue	,
Expense	,
Margin	,
Linehaul	,
Fuel_Surcharge	,
Other_Fess	,
SourceSystem_Name as TMS_System	,
PreBookStatus	,
Prebookstatus_Include,
Days_ahead_V2	,
temp2.Max_buy_rate	,
Max_buy_Flag	,
temp2.Market_Buy_Rate	,
original_Market_Buy_Rate	,
original_Max_buy_rate	,
Markey_buy_rate_flag	,
Assignee	,
SameAssignedRep_or_diffAssignedRep	,
offer_id	,
offered_by_name	,
Offers	,
Max_buy_Over_status_Flag	,
Market_Buy_Over_status_Flag	,
temp2.calendar_date	,
Week_End_Date	,
period_year	,
Employee_Number	,
Employee_Name	,
temp2.TMS_Name	,
Employment_Status_Code	,
Job_Code	,
Job_Title	,
Group	,
Internal_Group	,
Company	,
Business	,
Division	,
Region	,
Dept_Code	,
Department	,
Location_Code	,
Location	,
temp2.State	,
Employee_Type_Code	,
Full_Part_Time_Code	,
Last_Hire_Date	,
Original_Hire_Date	,
Benefits_Seniority_Date	,
Seniority_Date	,
Exp_Years	,
Seniority_Years	,
Date_In_Job	,
Email_Address	,
`2plusyearsflag`	,
Role_Power_BI	,
Target_Key	,
  concat_ws(
    Mode,
    Equipment,
    Customer_Office,
    Carrier_Office,
    Booked_By,
    customer_master,
    customer_name,
    Employee,
    load_status,
    Origin_City,
    Origin_State,
    Dest_City,
    Dest_State,
    Origin_Zip,
    Dest_Zip,
    load_lane,
    Market_origin,
    Market_Dest,
    Market_Lane,
    Booking_type,
    Load_flag,
    DOT_Num,
    Carrier_name,
    SourceSystem_Name,
    PreBookStatus,
    Days_ahead_V2,
    Max_Buy_Flag,
    Markey_buy_rate_flag,
    Assignee,
    SameAssignedRep_or_diffAssignedRep,
    Max_buy_Over_status_Flag,
    Market_Buy_Over_status_Flag,
    Week_End_Date,
    period_year
  ) as Fact_key,
  -- hbl.Company_id,
  hbl.lifecycle_stage_value,
  hbl.Cycle_Timestamp,
  hb2.Lifecycle_Stage_Value lifecycle_stage_value_Hb,
  hb2.cycle_date cycle_timestamp_hb

from
  temp2 
  left join  huspot_cycle hbl on temp2.DOT_Num = hbl.dot_number and temp2.period_year = hbl.hubspot_period_year 
  left join Hubspot_lifecyclestage2 Hb2 on temp2.DOT_Num = Hb2.dot_number and Hb2.Calendar_Date = temp2.ship_date
  ),
  

 final as (SELECT
    Load_Number,
    Ship_Date,
    Mode,
    Equipment,
    Week_Num,
    Customer_Office,
    Carrier_Office,
    Booked_By,
    customer_master,
    customer_name,
    customer_rep,
    load_status,
    Origin_City,
    Origin_State,
    Dest_City,
    Dest_State,
    Origin_Zip,
    Dest_Zip,
    load_lane,
    Market_origin,
    Market_Deat,
    Market_Lane,
    Delivery_Date,
    rolled_date,
    Booked_Date,
    Tendered_Date,
    scheduled_date,
    bounced_date,
    Cancelled_Date,
    Days_Ahead,
    prebookable,
    Pickup_Date,
    Pickup_EndDate,
    Drop_Start,
    Drop_End,
    invoiced_Date,
    Booking_type,
    Load_flag,
    Carrier.DOT_Num,
    Carrier,
    Miles,
    Revenue,
    Expense,
    Margin,
    Linehaul,
    Fuel_Surcharge,
    Other_Fess,
    TMS_System,
    PreBookStatus,
    Prebookstatus_Include,
    Days_ahead_V2,
    Carrier.Max_buy_rate,
    Max_buy_Flag,
    Market_Buy_Rate,
    original_Market_Buy_Rate,
    original_Max_buy_rate,
    Markey_buy_rate_flag,
    Assignee,
    SameAssignedRep_or_diffAssignedRep,
    offer_id,
    offered_by_name,
    Offers,
    Max_buy_Over_status_Flag,
    Market_Buy_Over_status_Flag,
    calendar_date,
    Week_End_Date,
    period_year,
    Employee_Number,
    Employee_Name,
    TMS_Name,
    Employment_Status_Code,
    Job_Code,
    Job_Title,
    `Group`,
    Internal_Group,
    Company,
    Business,
    Division,
    Region,
    Dept_Code,
    Department,
    Location_Code,
    Location,
    `State`,
    Employee_Type_Code,
    Full_Part_Time_Code,
    Last_Hire_Date,
    Original_Hire_Date,
    Benefits_Seniority_Date,
    Seniority_Date,
    Exp_Years,
    Seniority_Years,
    Date_In_Job,
    Email_Address,
    `2plusyearsflag`,
    Role_Power_BI,
    Target_Key,
    Fact_key,
    -- company_id,
    carrier.Lifecycle_Stage_Value,
    cycle_timestamp,
    cycle_timestamp_hb,
    lifecycle_stage_value_Hb
FROM
    Carrier ) ,
Ordered as (SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY Load_Number, TMS_System, week_end_date ORDER BY cycle_timestamp,cycle_timestamp_hb DESC) AS rank,
    CASE 
            WHEN Lifecycle_Stage_Value = 'customer' THEN 1 
            ELSE 0 
        END AS New_Managed_Carrier_Flag,
    CASE 
            WHEN lifecycle_stage_value_Hb = 'customer' THEN 1 
            ELSE 0 
        END AS Managed_Carrier_Flag
FROM
    final
ORDER BY
    Load_Number, TMS_System, cycle_timestamp)
select * from Ordered where rank = 1 and year(week_end_date) >= 2022
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact_Carrier_Employees

-- COMMAND ----------

create
or replace table analytics.Fact_Carrier_Employees as 
with carrier_employees_cte as (
  select
    Mode,
    Equipment,
    Customer_Office,
    Carrier_Office,
    Booked_By,
    customer_master,
    customer_name,
    customer_rep,
    load_status,
    Origin_City,
    Origin_State,
    Dest_City,
    Dest_State,
    Origin_Zip,
    Dest_Zip,
    load_lane,
    Market_origin,
    Market_Deat,
    Market_Lane,
    Booking_type,
    Load_flag,
    DOT_Num,
    Carrier,
    TMS_System,
    PreBookStatus,
    PreBookStatus_Include,
    Days_ahead_V2,
    Max_Buy_Flag,
    Markey_buy_rate_flag,
    Assignee,
    SameAssignedRep_or_diffAssignedRep,
    Max_buy_Over_status_Flag,
    Market_Buy_Over_status_Flag,
    Managed_Carrier_Flag,
    Employee_Number,
    Employee_Name,
    TMS_Name,
    Employment_Status_Code,
    Job_Code,
    Job_Title,
    `Group`,
    Internal_Group,
    Company,
    Business,
    Division,
    Region,
    Dept_Code,
    Department,
    Location_Code,
    Location,
    State,
    Employee_Type_Code,
    Full_Part_Time_Code,
    Last_Hire_Date,
    Original_Hire_Date,
    Benefits_Seniority_Date,
    Seniority_Date,
    Exp_Years,
    Seniority_Years,
    Date_In_Job,
    Email_Address,
    `2plusyearsflag`,
    Role_Power_BI,
    Target_Key,
    Week_End_Date,
    period_year,
    New_Managed_Carrier_Flag,
    concat_ws(period_year, email_address) as touches_key,
    concat_ws(
      Mode,
      Equipment,
      Customer_Office,
      Carrier_Office,
      Booked_By,
      customer_master,
      customer_name,
      customer_rep,
      load_status,
      Origin_City,
      Origin_State,
      Dest_City,
      Dest_State,
      Origin_Zip,
      Dest_Zip,
      load_lane,
      Market_origin,
      Market_Deat,
      Market_Lane,
      Booking_type,
      Load_flag,
      DOT_Num,
      Carrier,
      TMS_System,
      PreBookStatus,
      Days_ahead_V2,
      Max_Buy_Flag,
      Markey_buy_rate_flag,
      Assignee,
      SameAssignedRep_or_diffAssignedRep,
      Max_buy_Over_status_Flag,
      Market_Buy_Over_status_Flag,
      Week_End_Date,
      period_year
    ) as fact_key,
    count(Load_Number) as Loads,
    sum(Revenue) as Revenue,
    sum(Expense) as Expense,
    sum(Margin) as Margin,
    sum(Linehaul) as Linehaul,
    sum(Fuel_Surcharge) as Fuel_Surcharge,
    sum(Other_Fess) as Other_Fess,
    sum(Max_buy_rate) as Max_buy_rate,
    sum(Market_Buy_Rate) as Market_Buy_Rate,
    sum(Offers) as offers,
    case
      when count(Load_Number) > 1 and Managed_Carrier_Flag = 1 then 1
      else 0
    end as Managed_Carrier_booked_more_than_one
  from analytics.Fact_Carrier_Loads
  WHERE booked_by NOT IN ( "Bill Gutierrez", "Bo Firman" , "Anthony Tabery", "Omair Bangee", "Daniel Chanthachack", "Paul Garza", "Timber Roland", "Vlad Flint")
  group by
    Mode,
    Equipment,
    Customer_Office,
    Carrier_Office,
    Booked_By,
    customer_master,
    customer_name,
    customer_rep,
    load_status,
    Origin_City,
    Origin_State,
    Dest_City,
    Dest_State,
    Origin_Zip,
    Dest_Zip,
    load_lane,
    Market_origin,
    Market_Deat,
    Market_Lane,
    Booking_type,
    Load_flag,
    DOT_Num,
    Carrier,
    TMS_System,
    PreBookStatus,
    PreBookStatus_Include,
    Days_ahead_V2,
    Max_Buy_Flag,
    Markey_buy_rate_flag,
    Assignee,
    SameAssignedRep_or_diffAssignedRep,
    Max_buy_Over_status_Flag,
    Market_Buy_Over_status_Flag,
    Managed_Carrier_Flag,
    Employee_Number,
    Employee_Name,
    TMS_Name,
    Employment_Status_Code,
    Job_Code,
    Job_Title,
    `Group`,
    Internal_Group,
    Company,
    Business,
    Division,
    Region,
    Dept_Code,
    Department,
    Location_Code,
    Location,
    State,
    Employee_Type_Code,
    Full_Part_Time_Code,
    Last_Hire_Date,
    Original_Hire_Date,
    Benefits_Seniority_Date,
    Seniority_Date,
    Exp_Years,
    Seniority_Years,
    Date_In_Job,
    Email_Address,
    `2plusyearsflag`,
    Role_Power_BI,
    Target_Key,
    New_Managed_Carrier_Flag,
    Week_End_Date,
    period_year
),
carrier_supervisor_cte as (
  select 
    cec.*,
    ccts.`Supervisor Name` as Supervisor,
    concat(cec.email_address, cec.Booked_By, cec.Week_End_Date) as Touch_Key
  from carrier_employees_cte cec
  left join analytics.carrier_capacity_team_strengths ccts
    on cec.Employee_Name = ccts.`Employee Name`
  where year(cec.Week_End_Date) >= 2022
)
select * from carrier_supervisor_cte

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact_Carriers

-- COMMAND ----------

create or replace table analytics.Fact_Carriers
as

with carrier as (
select distinct 
    DOT_Num,
    first_value(Load_Number) over (partition by DOT_Num order by Ship_Date asc) as first_load_id,
    first_value(Ship_Date) over (partition by DOT_Num order by Ship_Date asc ) as first_load_date,
    nth_value(Load_Number, 4) over (partition by DOT_Num order by Ship_Date asc) as third_load_id,
    nth_value(Ship_Date, 4) over (partition by DOT_Num order by Ship_Date asc) as third_load_date,
    first_value(Load_Number) over (partition by DOT_Num order by Ship_Date desc)  as last_load_id,
    first_value(Ship_Date) over (partition by DOT_Num order by Ship_Date desc )  as last_load_date,
        first_value(carrier) over (partition by DOT_Num order by Ship_Date desc )  as last_load_carrier,
            first_value(booked_by) over (partition by DOT_Num order by Ship_Date desc )  as last_load_booked_by,
                nth_value(Ship_Date, 10) over (partition by DOT_Num order by Ship_Date asc) as tenth_load_date,
				Managed_Carrier_Flag
from analytics.Fact_Carrier_Loads) 

select 
DOT_Num,
min(first_load_date) as Onboarded_date,
max(third_load_date) as Third_load_date,
max(tenth_load_date) as  tenth_load_date,
max(last_load_date) as Last_load_date,
max(last_load_carrier) as Carrier_name,
max(last_load_booked_by) as Carrier_POC,
date_diff(YEAR,min(first_load_date) ,current_timestamp() ) as Years_with_NFI,
Max(Managed_Carrier_Flag) Managed_Carrier_Flag
from carrier
group by DOT_Num

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact_Carrier_Offers

-- COMMAND ----------

create or replace table analytics.Fact_Carrier_Offers
as 
WITH OFFER AS (
select 
	off.*,
	total_rate_in_pennies/100 as Offer_amount,Fact_key 
from `bronze`.`offer_negotiation_reflected` as off
left join
(select distinct Load_Number,Fact_key  from analytics.Fact_Carrier_Loads) as load
on off.relay_reference_number = load.Load_Number
)
Select offer .* , Dt.Financial_WeekendDate 
from
Offer
Left Join brokerageprod.analytics.dim_date Dt on cast
(Dt.Calendar_Date as date) = cast(Offer.offered_at as date)

-- COMMAND ----------

create or replace table analytics.vw_reporting_Fact_load_v8
as

with Temp as 
(
    select 

touch_id as Touches_ID,
 Touch_type as Touches_Type,
  full_name as Employee_Name,
  EMP.Email_Address  as Email,
  
    w.Weight * 1 as Touches_Score,
     date as Touch_Timestamp,

 --case when touch_type like 'Aljex%' then 'Aljex' else 'Relay' end as Source_System, 
 case when touch_type like 'Aljex%' then 1 else 2 end as Sourcesystem_id

from Bronze.Load_Touch as LT

Left Join

analytics.dim_employee_brokerage_mapping as EMP

on LT.full_name = EMP.TMS_Name     --- all the TMS name are matched with the Full name in load_Touch

left join 

analytics.tms_events_weightage as W

on LT.Touch_type  = W.Events
)

,
temp2 as 
(
select touches_ID,Touches_Type, Employee_Name, Email, Touches_score, Touch_Timestamp,sourcesystem_id from gold.fact_touches where sourcesystem_id not in (1,2,6)
)
,

 selected_data_Public_Private AS (
SELECT 
        REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
        c.Channel_Name,
        h.Conversation_Type AS Channel_type,
        h.Channel_ID,
        h.Actual_Text,
        TRY_CAST(h.DW_Timestamp AS Date) AS DT1,
        CASE
            WHEN MINUTE(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
            WHEN MINUTE(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
            WHEN MINUTE(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
            WHEN MINUTE(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
        END AS FifteenMinGroup
    FROM silver.silver_slack_conversations_history_new AS h
    LEFT JOIN silver.silver_slack_channels_new AS c ON h.Channel_ID = c.Channel_ID
    LEFT JOIN silver.silver_slack_users_new AS u ON h.User_ID = u.User_ID
)

,Selected_Data_DM As (
SELECT 
    REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
    c.Channel_Name,
    DM.Conversation_Type AS Channel_type,
    DM.Channel_ID,
    DM.Actual_Text AS DMText,
    TRY_CAST(DM.DW_Timestamp AS Date) AS DT1,
    CASE
        WHEN MINUTE(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
        WHEN MINUTE(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
        WHEN MINUTE(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
        WHEN MINUTE(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
    END AS FifteenMinGroup
FROM 
    silver.silver_slack_conversations_history_dm DM
LEFT JOIN 
    silver.silver_slack_channels_new AS c ON DM.Channel_ID = c.Channel_ID
LEFT JOIN 
    silver.silver_slack_users_new AS u ON DM.User_ID = u.User_ID
)

,Selected_Data_Group As 
(
SELECT 
    REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
    c.Channel_Name,
    GRP.Conversation_Type AS Channel_type,
    GRP.Channel_ID,
    GRP.Actual_Text AS GrpText,
    TRY_CAST(GRP.DW_Timestamp AS Date) AS DT1,
    CASE
        WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
        WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
        WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
        WHEN MINUTE(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
    END AS FifteenMinGroup
FROM 
    Silver.silver_slack_conversations_history_groups GRP
LEFT JOIN 
    silver.silver_slack_channels_new AS c ON GRP.Channel_ID = c.Channel_ID
LEFT JOIN 
    silver.silver_slack_users_new AS u ON GRP.User_ID = u.User_ID
)



    -- Aggregate the dataframe based on User_ID, Channel_name, Channel_type, and datetime (every 15 mins)
    -- Calculate the count of action ID
    ,aggregated_data AS (
    SELECT 
        User_ID,
        Channel_name,
        Channel_ID,
        Channel_type,
        DT1,
        FifteenMinGroup,
        CAST(1 AS INT) AS Touches_Score,
        Actual_Text
    FROM selected_data_Public_Private
    GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, Actual_Text, DT1, FifteenMinGroup

    UNION

    SELECT 
        User_ID,
        Channel_name,
        Channel_ID,
        Channel_type,
        DT1,
        FifteenMinGroup,
        CAST(1 AS INT) AS Touches_Score,
        DMText AS Actual_Text
    FROM Selected_Data_DM
    GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, DMText, DT1, FifteenMinGroup

    UNION

    SELECT 
        User_ID,
        Channel_name,
        Channel_ID,
        Channel_type,
        DT1,
        FifteenMinGroup,
        CAST(1 AS INT) AS Touches_Score,
        GrpText AS Actual_Text
    FROM Selected_Data_Group
    GROUP BY User_ID, Channel_name, Channel_ID, Channel_type, GrpText, DT1, FifteenMinGroup
),
	
slack_meassages as (
    -- Store the result in the DataFrame DF_Touches_Slack
    SELECT
        
        CONCAT_WS(User_ID, Channel_name, Channel_type, DT1,FifteenMinGroup) AS Touches_id,
        'Slack - Activities' AS Touch_Type,
        Null as full_name,
        User_ID as Email,
        Touches_Score,
        DT1 AS Date,
        CAST(6 AS INT) AS Source_system_id,
		CASE WHEN Channel_type = 'Public' THEN Channel_ID END AS Publicchannel,
		CASE WHEN Channel_type = 'Private' THEN Channel_ID END AS Privatechannel,
		CASE WHEN Channel_type = 'Direct Message' THEN Channel_ID END AS DirectMessages,
		CASE WHEN Channel_type = 'Group Message' THEN Channel_ID END AS GroupMessages,
		CASE WHEN Channel_type = 'Public' THEN Actual_Text END AS Publicchannelconversation,
		CASE WHEN Channel_type = 'Private' THEN Actual_Text END AS Privatechannelconversation,
		CASE WHEN Channel_type = 'Direct Message' THEN Actual_Text END AS DMConversations,
		CASE WHEN Channel_type = 'Group Message' THEN Actual_Text END AS GroupConverstation
		
    FROM aggregated_data)

----Select * from slack_meassages  where Date >= '2024-07-28' and Date <= '2024-08-24'



--- Reactions

,
 reactions_selected_data AS (select REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,c.Channel_Name,
h.Conversation_Type as Channel_type,	
h.Channel_ID,
h.Reaction_Name,
--DM.Reactions as DMReaction,

            try_cast(h.DW_Timestamp AS Date) as DT1,
        CASE
            WHEN minute(h.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
            WHEN minute(h.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
            WHEN minute(h.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
            WHEN minute(h.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
        END AS FifteenMinGroup
from  silver.silver_slack_reactions_new as h
left join  silver.silver_slack_channels_new as c
on h.Channel_ID = c.Channel_ID ---and c.Is_Private = false
left join silver.silver_slack_users_new as u
on h.User_ID = u.User_ID
---where h.reply_count is null

    )

, reactions_selected_data_DM AS (
select REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
c.Channel_Name,
DM.Conversation_Type as Channel_type,	
DM.Channel_ID,
--h.Reaction_Name,
DM.Reactions as Reaction_Name,

            try_cast(DM.DW_Timestamp AS Date) as DT1,
        CASE
            WHEN minute(DM.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
            WHEN minute(DM.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
            WHEN minute(DM.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
            WHEN minute(DM.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
        END AS FifteenMinGroup

FROM 
    silver.silver_slack_conversations_history_dm DM
LEFT JOIN 
    silver.silver_slack_channels_new AS c ON DM.Channel_ID = c.Channel_ID
LEFT JOIN 
    silver.silver_slack_users_new AS u ON DM.User_ID = u.User_ID
where Is_Reactions = '1'
---where h.reply_count is null

    )

,reactions_selected_data_Group as (
select REGEXP_EXTRACT(profile, 'email=([^,]+)') AS User_ID,
c.Channel_Name,
GRP.Conversation_Type as Channel_type,	
GRP.Channel_ID,
--h.Reaction_Name,
GRP.Reactions as Reaction_Name,

            try_cast(GRP.DW_Timestamp AS Date) as DT1,
        CASE
            WHEN minute(GRP.DW_Timestamp) BETWEEN 0 AND 14 THEN '00 - 15'
            WHEN minute(GRP.DW_Timestamp) BETWEEN 15 AND 29 THEN '15 - 30'
            WHEN minute(GRP.DW_Timestamp) BETWEEN 30 AND 44 THEN '30 - 45'
            WHEN minute(GRP.DW_Timestamp) BETWEEN 45 AND 59 THEN '45 - 60'
        END AS FifteenMinGroup

FROM 
    silver.silver_slack_conversations_history_groups GRP
LEFT JOIN 
    silver.silver_slack_channels_new AS c ON GRP.Channel_ID = c.Channel_ID
LEFT JOIN 
    silver.silver_slack_users_new AS u ON GRP.User_ID = u.User_ID
where Is_Reactions = '1'
)

    -- Aggregate the dataframe based on User_ID, Channel_name, Channel_type, and datetime (every 15 mins)
    -- Calculate the count of action ID
    , reaction_aggregated_data AS (
        SELECT
            User_ID,
			Channel_ID,
            Channel_name,
            Channel_type,
            DT1,
            FifteenMinGroup,
            CAST(1 AS INT) AS Touches_Score,
			Reaction_Name
            --DMReaction
        FROM reactions_selected_data
        GROUP BY User_ID,Channel_ID, Channel_name, Channel_type,Reaction_Name, DT1,FifteenMinGroup
		
		Union 
		
		 SELECT
            User_ID,
			Channel_ID,
            Channel_name,
            Channel_type,
            DT1,
            FifteenMinGroup,
            CAST(1 AS INT) AS Touches_Score,
			Reaction_Name  
        FROM reactions_selected_data_DM
        GROUP BY User_ID,Channel_ID, Channel_name, Channel_type,Reaction_Name, DT1,FifteenMinGroup
		
		UNION
		SELECT
            User_ID,
			Channel_ID,
            Channel_name,
            Channel_type,
            DT1,
            FifteenMinGroup,
            CAST(1 AS INT) AS Touches_Score,
			Reaction_Name           
        FROM reactions_selected_data_Group
        GROUP BY User_ID,Channel_ID, Channel_name, Channel_type,Reaction_Name, DT1,FifteenMinGroup
			
    )
	
--Select * from reaction_aggregated_data limit 100
,
Slack_reactions as (
    -- Store the result in the DataFrame DF_Touches_Slack
    SELECT
        
        CONCAT_WS(User_ID, Channel_name, Channel_type, DT1,FifteenMinGroup) AS Touches_id,
        'Slack - Reactions' AS Touch_Type,
        Null as full_name,
        User_ID as Email,
        Touches_Score,
        DT1 AS Date,
        CAST(6 AS INT) AS Source_system_id,
		CASE WHEN Channel_type = 'Public' THEN Channel_ID END AS Publicchannel,
		CASE WHEN Channel_type = 'Private' THEN Channel_ID END AS Privatechannel,
		CASE WHEN Channel_type = 'Direct Message' THEN Channel_ID END AS DirectMessages,
		CASE WHEN Channel_type = 'Group Message' THEN Channel_ID END AS GroupMessages,
		CASE WHEN Channel_type = 'Public' THEN Reaction_Name END AS Publicchannelconversation,
		CASE WHEN Channel_type = 'Private' THEN Reaction_Name END AS Privatechannelconversation,
        CASE WHEN Channel_type = 'Direct Message' THEN Reaction_Name END AS DMConversations,
		CASE WHEN Channel_type = 'Group Message' THEN Reaction_Name END AS GroupConverstation		
    FROM reaction_aggregated_data)

,

slack_consolidated as (

    select * from Slack_reactions

    union 

select * from slack_meassages
)
,
hubspot_calls_df( select 
hs_activity_type,
hs_object_id,
hubspot_owner_id,
hs_createdate,
from_utc_timestamp(hs_createdate, 'CST') AS hs_createdate_cst,
SourcesystemID
from bronze.hubspot_calls where sourcesystemid = '5' and hs_activity_type = 'Introduce NFI'),

hubspot_owners_df as (
select id,email from bronze.hubspot_owners ),

Hubspot_call as (select distinct
'Hubspot-Introcalls' as Touches_Type,
CONCAT_WS(hs_object_id,email,'Hubspot-Introcalls',hs_createdate_cst) AS Touches_id,
email AS email,
'' as Employee_Name,
1 as Touches_score,
hs_createdate_cst,
CAST(hs_createdate_cst AS TIMESTAMP) AS Touch_Timestamp,
SourcesystemID AS sourcesystem_id
FROM hubspot_calls_df n
JOIN Hubspot_Owners_DF o ON n.hubspot_owner_id = o.id),

consolidate as (
select touches_ID,Touches_Type, Employee_Name, Email, Touches_score, Touch_Timestamp,sourcesystem_id,Null as Publicchannel,Null as Privatechannel, Null as DirectMessages, 
Null as GroupMessages , Null as Publicchannelconversation,Null as Privatechannelconversation,
Null as DMConversations,
Null as GroupConverstation
from gold.fact_touches

union

select *,Null as Publicchannel,Null as Privatechannel, Null as DirectMessages, 
Null as GroupMessages , Null as Publicchannelconversation,Null as Privatechannelconversation,
Null as DMConversations,
Null as GroupConverstation
 from temp 

union 
select touches_ID,Touches_Type, Employee_Name, Email, Touches_score, Touch_Timestamp,sourcesystem_id,Null as Publicchannel,Null as Privatechannel, Null as DirectMessages, 
Null as GroupMessages , Null as Publicchannelconversation,Null as Privatechannelconversation,
Null as DMConversations,
Null as GroupConverstation
from Hubspot_call
union 
select * from slack_consolidated )

--Select * from slack_consolidated where DMConversations is not null;
,

 EmployeeQuartiles AS (
    SELECT
        lower(email) AS Email,
        m.`Group` AS Role,
        period_year,
        financial_period_number,
        Calendar_Year,
        sum(Touches_Score) AS Touch_Score,
		COUNT (distinct Publicchannel) as Publicchannel,
		COUNT (distinct Privatechannel) as Privatechannel,
		COUNT (distinct DirectMessages) as DirectMessages,
		COUNT (distinct GroupMessages) as GroupMessages,
		COUNT (Publicchannelconversation) as Publicchannel_conversation,
		COUNT (Privatechannelconversation) as Privatechannel_conversation,
		COUNT (DMConversations) as DMConversations,
		COUNT (GroupConverstation) as GroupConverstation,
		(COUNT(Publicchannelconversation) + COUNT(Privatechannelconversation) + COUNT (DMConversations) + COUNT (GroupConverstation) ) AS Total_conversation,
        NTILE(4) OVER (PARTITION BY m.`Group`, period_year, financial_period_number, Calendar_Year ORDER BY sum(Touches_Score) DESC) AS Quartile_Number
    FROM 
        consolidate AS T
        LEFT JOIN `gold`.`lookup_sourcesystem` AS S ON T.Sourcesystem_ID = S.DW_Sourcesystem_ID
        INNER JOIN analytics.dim_employee_brokerage_mapping AS m ON lower(T.email) = m.Email_Address
        LEFT JOIN (
            SELECT calendar_date, period_year, financial_period_number, Calendar_Year 
            FROM `analytics`.`dim_date`
        ) AS Cal ON cast(T.Touch_Timestamp AS Date) = Cal.calendar_date
    WHERE 
        CAST(T.Touch_Timestamp AS Date) BETWEEN DATEADD(year, -2, CURRENT_DATE()) AND CURRENT_DATE()
    GROUP BY 
        lower(email), period_year, financial_period_number, Calendar_Year, m.`Group`
)
,
 TouchesSummary AS (
    -- The provided CTE to summarize touch data
    -- You can keep this CTE as is
    SELECT 
        lower(email) AS Email,
        Touches_Type,
        COALESCE(S.Sourcesystem_Name, 'Slack') AS Sourcesystem_Name,
        Sourcesystem_ID,
        CAST(Touch_Timestamp AS Date) AS Touch_Date,
        SUM(Touches_Score) AS Touch_Score,
        CASE 
            WHEN Touches_Type IN ('Aljex - Load Booked', 'Aljex - Marked Delivered', 'Aljex - Marked Driver_Dispatched', 'Aljex - Marked Loaded') THEN 'TMS - Aljex'
            WHEN Touches_Type IN ('Hubspot-Deals', 'Hubspot-Email', 'Hubspot-Meeting', 'Hubspot-Notes','Hubspot-Introcalls') THEN 'Hubspot'
            WHEN Touches_Type IN ('Relay - Appt Scheduled', 'Relay - Assigned load', 'Relay - Bounced a Load', 'Relay - Detention Reported', 'Relay - Driver Dispatched', 'Relay - Driver Info Captured', 'Relay - Equipment Assigned', 'Relay - In Transit Update Captured', 'Relay - Load Booked', 'Relay - Load Cancelled', 'Relay - Manual Tender Created', 'Relay - Marked Arrived At Stop', 'Relay - Marked Delivered', 'Relay - Marked Loaded', 'Relay - Put Offer on a Load', 'Relay - Reserved a Load', 'Relay - Rolled a Load', 'Relay - Set a Max Buy', 'Relay - Stop Marked Delivered', 'Relay - Stop Recorded', 'Relay - Tender Accepted', 'Relay - Tracking Contact Info Captured', 'Relay - Tracking Note Captured', 'Relay - Tracking Stopped') THEN 'TMS - Relay'
            WHEN Touches_Type in ('Slack - Activities','Slack - Reactions')  THEN 'Slack'
            ELSE 'Other'
        END AS Event_Group,
        CASE 
            WHEN Touches_Type IN ('Aljex - Load Booked', 'Aljex - Marked Delivered', 'Aljex - Marked Driver_Dispatched', 'Aljex - Marked Loaded') THEN 'Operational'
            WHEN Touches_Type IN ('Hubspot-Deals', 'Hubspot-Email', 'Hubspot-Meeting', 'Hubspot-Notes','Hubspot-Introcalls') THEN 'Hubspot'
            WHEN Touches_Type IN ('Relay - Appt Scheduled', 'Relay - Assigned load', 'Relay - Bounced a Load', 'Relay - Detention Reported', 'Relay - Driver Dispatched', 'Relay - Driver Info Captured', 'Relay - Equipment Assigned', 'Relay - In Transit Update Captured', 'Relay - Load Booked', 'Relay - Load Cancelled', 'Relay - Manual Tender Created', 'Relay - Marked Arrived At Stop', 'Relay - Marked Delivered', 'Relay - Marked Loaded', 'Relay - Put Offer on a Load', 'Relay - Reserved a Load', 'Relay - Rolled a Load', 'Relay - Set a Max Buy', 'Relay - Stop Marked Delivered', 'Relay - Stop Recorded', 'Relay - Tender Accepted', 'Relay - Tracking Contact Info Captured', 'Relay - Tracking Note Captured', 'Relay - Tracking Stopped') THEN 'Operational'
            WHEN Touches_Type in ('Slack - Activities','Slack - Reactions') THEN 'Slack'
            ELSE 'Other'
        END AS Event_Type
    FROM 
        consolidate AS T
        LEFT JOIN  `gold`.`lookup_sourcesystem` AS S ON T.Sourcesystem_ID = S.DW_Sourcesystem_ID
    WHERE 
        CAST(T.Touch_Timestamp AS Date) BETWEEN DATEADD(year, -2, CURRENT_DATE()) AND CURRENT_DATE()
    GROUP BY 
        lower(email), Touches_Type, CAST(Touch_Timestamp AS Date), S.Sourcesystem_Name, Sourcesystem_ID
), 

TouchesAggregated AS (
    -- CTE to aggregate touch data according to specifications
    SELECT 
        D.Calendar_Year AS Calendar_Year,
        D.financial_period_number AS Period,
        CONCAT(D.financial_period_number, '-', D.Calendar_Year) AS Period_Year,
        SUM(Touch_Score) AS Total_Touch_Score,
        SUM(CASE WHEN event_type = 'Operational' THEN Touch_Score ELSE 0 END) AS Operational_Touches,
        SUM(CASE WHEN Event_Group = 'TMS - Relay' THEN Touch_Score ELSE 0 END) AS Relay_Touches,
        SUM(CASE WHEN Event_Group = 'TMS - Aljex' THEN Touch_Score ELSE 0 END) AS Aljex_Touches,
        SUM(CASE WHEN Event_Group = 'Hubspot' THEN Touch_Score ELSE 0 END) AS Hubspot_Touches,
        SUM(CASE WHEN Event_Group = 'Slack' THEN Touch_Score ELSE 0 END) AS Slack_Touches,
        SUM(CASE WHEN Touches_Type = 'Hubspot-Email' THEN Touch_Score ELSE 0 END) AS Hubspot_Email_Touches,
        SUM(CASE WHEN Touches_Type = 'Hubspot-Deals' THEN Touch_Score ELSE 0 END) AS Hubspot_Deals,
        SUM(CASE WHEN Touches_Type = 'Hubspot-Notes' THEN Touch_Score ELSE 0 END) AS Hubspot_Notes,
        SUM(CASE WHEN Touches_Type = 'Hubspot-Meeting' THEN Touch_Score ELSE 0 END) AS Hubspot_Meetings,
		SUM(CASE WHEN Touches_Type = 'Hubspot-Introcalls' THEN Touch_Score ELSE 0 END) AS Hubspot_Introcalls,
        SUM(CASE WHEN Touches_Type = 'Slack - Activities' THEN Touch_Score ELSE 0 END) AS Slack_Message_Touches,
        SUM(CASE WHEN Touches_Type = 'Slack - Reactions' THEN Touch_Score ELSE 0 END) AS Slack_Reaction_Touches,
        Email AS Email

    FROM 
        TouchesSummary TS
        JOIN `analytics`.`dim_date` D ON TS.Touch_Date = D.calendar_date
    GROUP BY 
        D.Calendar_Year, D.financial_period_number, D.Calendar_Year, Email
),


RevenueSummary AS (
Select period_year, financial_period_number, Calendar_Year,emp.Email_address,(Sum(Revenue)/2) as Revenue,count(distinct loadnum) as Volume,((sum(Revenue)/2) - (sum(expense)/2) ) as Margin  from 
(
select 'Carrier' as Revenue_Type ,
CASE 
        WHEN Full_name IS NULL THEN booked_by 
        ELSE Full_name 
    END AS Full_name, l.Ship_date as shipdate , l.Revenue,l.load_id as loadnum, l.expense from analytics.Fact_Load_GenAI_NonSplit as l 
left join `bronze`.`aljex_user_report_listing` as u1 
on l.booked_by = u1.full_name
where dot_num is not null

UNION ALL

select 'Shipper' as Shipper ,
CASE 
        WHEN Full_name IS NULL THEN l.Employee 
        ELSE Full_name 
    END AS Full_name, l.Ship_date as shipdate, l.Revenue,l.load_id as loadnum, l.expense from analytics.Fact_Load_GenAI_NonSplit as l 
left join `bronze`.`aljex_user_report_listing` as u1 
on l.Employee = u1.Full_name
where dot_num is not null and full_name is not null
)
as S

LEFT JOIN (
            SELECT calendar_date, period_year, financial_period_number, Calendar_Year 
            FROM `analytics`.`dim_date`
        ) AS Cal ON cast(S.Shipdate AS Date) = Cal.calendar_date
Left JOIN
analytics.dim_employee_brokerage_mapping as Emp
on s.Full_name = emp.TMS_name
where s.Shipdate BETWEEN DATEADD(year, -2, CURRENT_DATE()) AND CURRENT_DATE()

group by
period_year, financial_period_number, Calendar_Year,emp.Email_address

)
,

templast as (
SELECT concat("Quartile - ",Quartile_Number) as Quartile_Name,Quartile_Number, A2.*,A3.Revenue,A3.margin, A3.Volume,emp2.*,Concat(emp2.TMS_Name,',',A2.period_year) as TMS_period,
Concat(emp2.Employee_Name,',',A2.period_year) as Employee_period,A1.Publicchannel,A1.Privatechannel,A1.DirectMessages,A1.GroupMessages,
A1.Publicchannel_conversation,A1.Privatechannel_conversation,A1.DMConversations, A1.GroupConverstation,
A1.Total_conversation
FROM 
    EmployeeQuartiles as A1
    
    left join 
    
    TouchesAggregated as A2
    
    on A1.Email  = a2.Email and a1.period_year = a2.Period_Year
    
    left join 

    RevenueSummary  as A3
    
    on A1.Email  = a3.Email_address and a1.period_year = a3.Period_Year
    
    left join analytics.dim_employee_brokerage_mapping as Emp2
on A1.Email = Emp2.Email_Address )


select *, concat_ws(Period_Year,email) as Touches_key from templast 


-- COMMAND ----------

create or replace table analytics.vw_reporting_Fact_load_v9
as


select offer_id,
authority,
authority_id,
carrier_name,
customer,
master_carrier_id,
notes,
cast(offered_by as string) as offered_by ,
offered_by_name,
offered_system,
offered_at,
relay_reference_number,
total_rate_currency,
total_rate_in_pennies,
carrier_profile_id,
inserted_at,
updated_at,total_rate_in_pennies/100 as Offer_amount,'Relay' as source from `bronze`.`offer_negotiation_reflected` as off

union all 

select 
concat_ws(date_of_bid,bidding_rep,pro_num,bid_amount,vender) as offer_id,"Aljex" as authority,null as authority_id,vender as carrier_name,customer_name as customer,null as master_carrier_id,null as notes,bidding_rep as offered_by,full_name as offered_by_name,'Aljex' as offered_system,
date_of_bid as offered_at,pro_num as relay_reference_number, 'USD' as total_rate_currency,bid_amount*100 as total_rate_in_pennies,'' as carrier_profile_id,null as inserted_at,null as updated_at,bid_amount as offer_amount,'Aljex' as Source
from Bronze.Aljex_bids_posted as b
left join 
bronze.aljex_user_report_listing as u
on b.bidding_rep = u.aljex_id
left join 
 `analytics`.`fact_load_genai_nonsplit` as l
on b.pro_num= l.load_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact_Carrier_Onboarding

-- COMMAND ----------

CREATE OR REPLACE TABLE analytics.Fact_Carrier_Onboarding AS
Select * from 
(
WITH carrier AS (
    SELECT
        COUNT(DISTINCT(load_number)) AS total_loads,
        Booked_By,
        Carrier,
        DOT_NUM,
        period_year,
        Week_End_Date,
        TO_DATE(period_year, 'M-yyyy') AS period_date
    FROM analytics.Fact_Carrier_Loads
    GROUP BY DOT_NUM, period_year, Carrier, Week_End_Date, Booked_By
),
RollingLoads AS (
    SELECT
        DOT_NUM,
        Carrier,
        period_year,
        period_date,
        Booked_By,
        total_loads,
        Week_End_Date,
        SUM(total_loads) OVER (
            PARTITION BY DOT_NUM, Booked_By
            ORDER BY period_date
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS Rolling2MonthLoads
    FROM carrier
),
ExceedingLoads AS (
    SELECT
        rl.DOT_NUM,
        rl.Carrier,
        rl.period_year,
        rl.period_date,
        rl.week_end_date,
        rl.Booked_By,
        MIN(el.period_date) AS FirstPeriodExceeding6Loads
    FROM RollingLoads rl
    JOIN RollingLoads el
        ON rl.DOT_NUM = el.DOT_NUM
        AND rl.Booked_By = el.Booked_By
        AND el.period_date BETWEEN ADD_MONTHS(rl.period_date, -11) AND rl.period_date
        AND el.Rolling2MonthLoads > 6
    GROUP BY rl.DOT_NUM, rl.Carrier, rl.period_year, rl.period_date, rl.week_end_date, rl.Booked_By
)
SELECT DISTINCT
    rl.total_loads Loads,
    rl.Carrier,
    rl.DOT_NUM,
    rl.period_year,
    rl.Booked_By,
    rl.Week_End_Date,
    TO_CHAR(el.FirstPeriodExceeding6Loads, 'M-yyyy') AS FirstPeriodExceeding6Loads
FROM RollingLoads rl
LEFT JOIN ExceedingLoads el ON rl.DOT_NUM = el.DOT_NUM AND rl.Booked_By = el.Booked_By AND rl.period_date = el.period_date
WHERE rl.Week_End_Date >= '2022-01-01'
  AND el.FirstPeriodExceeding6Loads IS NOT NULL
ORDER BY rl.DOT_NUM, rl.Week_End_Date
)
Where period_year = FirstPeriodExceeding6Loads