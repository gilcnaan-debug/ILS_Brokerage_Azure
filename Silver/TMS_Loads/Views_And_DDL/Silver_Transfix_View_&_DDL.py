# Databricks notebook source
# MAGIC %md
# MAGIC ## Transfix
# MAGIC * **Description:** Load from Bronze to Silver as delta file
# MAGIC * **Created Date:** 03/07/2025
# MAGIC * **Created By:** Uday
# MAGIC * **Modified Date:** 06/10/2025
# MAGIC * **Modified By:** Uday
# MAGIC * **Changes Made:** Logic Update

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full Load View

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC SET ansi_mode = false; 
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Transfix AS
# MAGIC WITH to_get_avg AS (
# MAGIC   SELECT
# MAGIC     DISTINCT canada_conversions.ship_date,
# MAGIC     canada_conversions.conversion AS us_to_cad,
# MAGIC     canada_conversions.us_to_cad AS cad_to_us
# MAGIC   FROM
# MAGIC     canada_conversions
# MAGIC   ORDER BY
# MAGIC     canada_conversions.ship_date DESC
# MAGIC   LIMIT
# MAGIC     7
# MAGIC ), average_conversions AS (
# MAGIC   SELECT
# MAGIC     avg(to_get_avg.us_to_cad) AS avg_us_to_cad,
# MAGIC     avg(to_get_avg.cad_to_us) AS avg_cad_to_us
# MAGIC   FROM
# MAGIC     to_get_avg
# MAGIC ), Transfix AS (
# MAGIC   select distinct 
# MAGIC     Loadnum::float as Load_Number,
# MAGIC     coalesce(arrive_pu_date::date, depart_pu_date::date, pu_appt::date) as Ship_DATE,
# MAGIC     load_mode as Mode,
# MAGIC     service_line as Equipment,
# MAGIC     date_range_two.week_num as week_num,
# MAGIC     'DBG' as customer_office,
# MAGIC     'DBG' as Carrier_Office,
# MAGIC     null as key_c_user,
# MAGIC     Shipper as mastername,
# MAGIC     shipper as customer_name,
# MAGIC     Null as customer_rep,
# MAGIC     Null as salesrep,
# MAGIC     Load_status as load_status,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     origin_zip as Origin_Zip,
# MAGIC     dest_zip as Dest_Zip,
# MAGIC     CONCAT(
# MAGIC       INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC     ) AS load_lane,
# MAGIC     Null as Market_origin,
# MAGIC     Null as Market_Dest,
# MAGIC     Null as Market_Lane,
# MAGIC     del_appt as Delivery_Date,
# MAGIC     rolled_date as rolled_date,
# MAGIC     Booked_Date as Booked_Date,
# MAGIC     Tendered_Date as Tendered_Date,
# MAGIC     bounced_date as bounced_date,
# MAGIC     Cancelled_Date as Cancelled_Date,
# MAGIC     pu_appt as Pickup_Appointment_Date,
# MAGIC     pu_appt as scheduled_Date,
# MAGIC     --Null as Days_Ahead,
# MAGIC     -- Null as prebookable,
# MAGIC     arrive_pu_date as Pickup_Date,
# MAGIC     depart_pu_date as Pickup_EndDate,
# MAGIC     CAST(Pickup_Appointment_Date AS TIMESTAMP) as pickup_datetime,
# MAGIC     case
# MAGIC       when pickup_datetime::date > COALESCE(arrive_pu_date::date, pu_appt::date) then 'Late'
# MAGIC       else 'OnTime'
# MAGIC     end as ot_pu,
# MAGIC     arrive_del_date as Delivery_In,
# MAGIC     depart_del_date as Delivery_out,
# MAGIC     cast(depart_del_date as TIMESTAMP) as Delivery_Datetime,
# MAGIC     case
# MAGIC       when delivery_datetime::date > del_appt::date then 'Late'
# MAGIC       else 'OnTime'
# MAGIC     end as ot_del,
# MAGIC     first_invoice_sent_date::date as invoiced_Date,
# MAGIC     --load_pickup.Days_ahead as Days_Ahead,
# MAGIC     Null as Booking_type,
# MAGIC     case
# MAGIC       when carrier is not null then '1'
# MAGIC       else '0'
# MAGIC     end as Load_flag,
# MAGIC     dot as DOT_Number,
# MAGIC     transfix_data.carrier as Carrier,
# MAGIC     calc_miles::float,
# MAGIC     transfix_data.total_rev::float as split_rev,
# MAGIC     split_rev * coalesce(canada_conversions.conversion, avg_us_to_cad) as revenue_cad,
# MAGIC     transfix_data.total_exp::float::float as split_exp,
# MAGIC     split_exp * coalesce(canada_conversions.conversion, avg_us_to_cad) as expense_cad,
# MAGIC     coalesce(transfix_data.total_rev, 0)::float
# MAGIC     - coalesce((transfix_data.total_exp::float), 0)::float as split_margin,
# MAGIC     split_margin * coalesce(canada_conversions.conversion, avg_us_to_cad) as margin_cad,
# MAGIC     transfix_data.carr_lhc as Linehaul,
# MAGIC     linehaul * coalesce(canada_conversions.conversion, avg_us_to_cad) as linehaul_cad,
# MAGIC     transfix_data.carr_fuel as Fuel_Surcharge,
# MAGIC     fuel_surcharge * coalesce(canada_conversions.conversion, avg_us_to_cad) as fuel_surcharge_cad,
# MAGIC     transfix_data.carr_acc as Other_Fess,
# MAGIC     Other_Fess * coalesce(canada_conversions.conversion, avg_us_to_cad) as Other_fees_cad,
# MAGIC     cust_fuel as Customer_Fuel,
# MAGIC     Customer_Fuel * coalesce(canada_conversions.conversion, avg_us_to_cad) as Customer_Fuel_CAD,
# MAGIC     cust_lhc as Customer_Linehaul,
# MAGIC     Customer_Linehaul * coalesce(canada_conversions.conversion, avg_us_to_cad) as Customer_Linehual_CAD,
# MAGIC     cust_acc as Customer_Other_Fees,
# MAGIC     cust_acc * coalesce(canada_conversions.conversion, avg_us_to_cad) as Customer_Other_Fees_CAD,
# MAGIC     Null as Trailer_Num,
# MAGIC     null as Po_Number,
# MAGIC     null as consignee_name,
# MAGIC     Null as Delivery_consignee_Address,
# MAGIC     Null as pickup_consignee_Address,
# MAGIC     null as Mc_Number,
# MAGIC     Null as Spot_Revenue,
# MAGIC     Null as Spot_Margin,
# MAGIC     Null as Max_buy_rate,
# MAGIC     Null as Market_Buy_Rate,
# MAGIC     'Transfix' as TMS_System,
# MAGIC     Period_Year
# MAGIC   from
# MAGIC     transfix_data
# MAGIC       join analytics.dim_financial_calendar
# MAGIC         on coalesce(arrive_pu_date::date, depart_pu_date::date, pu_appt::date)::date
# MAGIC         = analytics.dim_financial_calendar.date::date
# MAGIC       left join date_range_two
# MAGIC         on analytics.dim_financial_calendar.date::date
# MAGIC         = date_range_two.date_date::date
# MAGIC          JOIN average_conversions ON 1=1 
# MAGIC      LEFT JOIN canada_conversions ON coalesce(arrive_pu_date::date, depart_pu_date::date, pu_appt::date) = canada_conversions.ship_date
# MAGIC      LEFT JOIN analytics.dim_date ON coalesce(arrive_pu_date::date, depart_pu_date::date, pu_appt::date) = dim_date.calendar_date
# MAGIC ),
# MAGIC maxbuy as (
# MAGIC   select
# MAGIC     *,
# MAGIC     CASE
# MAGIC       WHEN TRY_CAST(Booked_date AS DATE) < TRY_CAST(ship_date AS DATE) THEN 'PreBooked'
# MAGIC       ELSE 'SameDay'
# MAGIC     END AS PreBookStatus,
# MAGIC     datediff(try_cast(Ship_Date as date), try_cast(Tendered_Date as date)) as Days_ahead
# MAGIC   from
# MAGIC     Transfix      
# MAGIC )
# MAGIC
# MAGIC   ,
# MAGIC tender as (
# MAGIC   SELECT
# MAGIC     tender_reference_numbers_projection.relay_reference_number as id,
# MAGIC     tenderer,
# MAGIC     tendering_acceptance.accepted_at,
# MAGIC     integration_tender_mapped_projection_v2.tendered_at
# MAGIC   FROM
# MAGIC     bronze.tender_reference_numbers_projection
# MAGIC       LEFT JOIN bronze.tendering_acceptance
# MAGIC         ON tender_reference_numbers_projection.tender_id = tendering_acceptance.tender_id
# MAGIC       LEFT JOIN bronze.integration_tender_mapped_projection_v2
# MAGIC         ON tendering_acceptance.shipment_id = integration_tender_mapped_projection_v2.shipment_id
# MAGIC         AND event_type = 'tender_mapped'
# MAGIC       JOIN analytics.dim_date
# MAGIC         ON cast(accepted_At as DATE) = analytics.dim_date.Calendar_Date
# MAGIC   WHERE
# MAGIC     `accepted?` = 'true'
# MAGIC     AND is_split = 'false'
# MAGIC ),
# MAGIC max as (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     rank() OVER (PARTITION BY id ORDER BY tendered_at, accepted_at DESC) AS rank
# MAGIC   from
# MAGIC     tender
# MAGIC ),
# MAGIC tender_final AS (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     tenderer,
# MAGIC       CASE
# MAGIC         WHEN tenderer IN ('orderful', '3gtms') THEN 'EDI'
# MAGIC         WHEN tenderer = 'vooma-orderful' THEN 'Vooma'
# MAGIC         WHEN tenderer IS NULL THEN 'Manual'
# MAGIC         ELSE tenderer
# MAGIC       END AS Tender_Source_Type
# MAGIC   FROM
# MAGIC     max
# MAGIC   WHERE
# MAGIC     rank = 1
# MAGIC ),
# MAGIC Tender_Source AS (select
# MAGIC   maxbuy.*,tender_final.Tender_Source_Type
# MAGIC from
# MAGIC   maxbuy
# MAGIC     left join tender_final
# MAGIC       on maxbuy.load_number = tender_final.id)
# MAGIC
# MAGIC SELECT
# MAGIC     CONCAT('T', Load_Number::int) AS DW_Load_ID,  -- Placeholder for DW_Load_ID (not in original)
# MAGIC     Load_Number,
# MAGIC     Delivery_Datetime AS Actual_Delivered_Date,
# MAGIC     Booked_Date,
# MAGIC     Fuel_Surcharge_CAD AS Carrier_Fuel_Surcharge_CAD,
# MAGIC     Fuel_Surcharge AS Carrier_Fuel_Surcharge_USD,
# MAGIC     Linehaul_CAD AS Carrier_Linehaul_CAD,
# MAGIC     Linehaul AS Carrier_Linehaul_USD,
# MAGIC     Carrier AS Carrier_Name,
# MAGIC     Carrier_Office,
# MAGIC     Other_Fees_CAD AS Carrier_Other_Fees_CAD,
# MAGIC     Other_Fess AS Carrier_Other_Fees_USD,
# MAGIC     Customer_Fuel AS Customer_Fuel_Surcharge_USD,
# MAGIC     Customer_Fuel_CAD AS Customer_Fule_Surcharge_CAD,
# MAGIC     Customer_Linehual_CAD AS Customer_Linehaul_CAD,
# MAGIC     Customer_Linehaul AS Customer_Linehaul_USD,
# MAGIC     mastername AS Customer_Master,
# MAGIC     customer_name AS Customer_Name,
# MAGIC     customer_office AS Customer_Office,
# MAGIC     Customer_Other_Fees_CAD AS Customer_Other_Fees_CAD,
# MAGIC     Customer_Other_Fees AS Customer_Other_Fees_USD,
# MAGIC     Days_ahead,
# MAGIC     bounced_date AS Bounced_Date,
# MAGIC     Cancelled_Date,
# MAGIC     Delivery_Date AS Delivery_Appointment_Date,
# MAGIC     Delivery_out AS Delivery_EndDate,
# MAGIC     dest_city AS Destination_City,
# MAGIC     dest_state AS Destination_State,
# MAGIC     Dest_Zip AS Destination_Zip,
# MAGIC     DOT_Number,
# MAGIC     Delivery_In AS Delivery_StartDate,
# MAGIC     Equipment,
# MAGIC     expense_CAD AS Expense_CAD,
# MAGIC     split_exp AS Expense_USD,
# MAGIC     Period_Year AS Financial_Period_Year,
# MAGIC     invoiced_Date AS Invoiced_Date,
# MAGIC     'USD' AS Load_Currency, 
# MAGIC     Load_flag,
# MAGIC     load_status AS Load_Status,
# MAGIC     split_margin AS Margin_CAD,
# MAGIC     Margin_CAD AS Margin_USD,
# MAGIC     calc_miles AS Miles,
# MAGIC     Mode,
# MAGIC     ot_del AS On_Time_Delivery,
# MAGIC     ot_pu AS On_Time_Pickup,
# MAGIC     origin_city AS Origin_City,
# MAGIC     origin_state AS Origin_State,
# MAGIC     Origin_Zip,
# MAGIC     Pickup_Appointment_Date,
# MAGIC     pickup_datetime AS Actual_Pickup_Date,
# MAGIC     Pickup_EndDate,
# MAGIC     Pickup_Date AS Pickup_StartDate,
# MAGIC     PreBookStatus,
# MAGIC     revenue_CAD AS Revenue_CAD,
# MAGIC     split_rev AS Revenue_USD,
# MAGIC     rolled_date AS Rolled_Date,
# MAGIC     Ship_Date,
# MAGIC     Tendered_Date,
# MAGIC     TMS_System,
# MAGIC     week_num AS Week_Number,
# MAGIC     Expense_USD AS Carrier_Final_Rate_USD,
# MAGIC     Expense_CAD AS Carrier_Final_Rate_CAD,
# MAGIC      SHA1(CONCAT(
# MAGIC         CAST(COALESCE(Delivery_Datetime, '') AS STRING),
# MAGIC         CAST(COALESCE(Booked_Date, '') AS STRING),
# MAGIC         CAST(COALESCE(Fuel_Surcharge_CAD, '') AS STRING),
# MAGIC         CAST(COALESCE(Fuel_Surcharge, '') AS STRING),
# MAGIC         CAST(COALESCE(Linehaul_CAD, '') AS STRING),
# MAGIC         CAST(COALESCE(Linehaul, '') AS STRING),
# MAGIC         CAST(COALESCE(Carrier, '') AS STRING),
# MAGIC         CAST(COALESCE(Carrier_Office, '') AS STRING),
# MAGIC         CAST(COALESCE(Other_Fees_CAD, '') AS STRING),
# MAGIC         CAST(COALESCE(Other_Fess, '') AS STRING),
# MAGIC         CAST(COALESCE(Customer_Fuel, '') AS STRING),
# MAGIC         CAST(COALESCE(Customer_Fuel_CAD, '') AS STRING),
# MAGIC         CAST(COALESCE(Customer_Linehaul_CAD, '') AS STRING),
# MAGIC         CAST(COALESCE(Customer_Linehaul, '') AS STRING),
# MAGIC         CAST(COALESCE(mastername, '') AS STRING),
# MAGIC         CAST(COALESCE(customer_name, '') AS STRING),
# MAGIC         CAST(COALESCE(customer_office, '') AS STRING),
# MAGIC         CAST(COALESCE(Customer_Other_Fees_CAD, '') AS STRING),
# MAGIC         CAST(COALESCE(Customer_Other_Fees, '') AS STRING),
# MAGIC         CAST(COALESCE(Days_ahead, '') AS STRING),
# MAGIC         CAST(COALESCE(bounced_date, '') AS STRING),
# MAGIC         CAST(COALESCE(Cancelled_Date, '') AS STRING),
# MAGIC         CAST(COALESCE(Delivery_Date, '') AS STRING),
# MAGIC         CAST(COALESCE(Delivery_out, '') AS STRING),
# MAGIC         CAST(COALESCE(dest_city, '') AS STRING),
# MAGIC         CAST(COALESCE(dest_state, '') AS STRING),
# MAGIC         CAST(COALESCE(Dest_Zip, '') AS STRING),
# MAGIC         CAST(COALESCE(DOT_Number, '') AS STRING),
# MAGIC         CAST(COALESCE(Delivery_In, '') AS STRING),
# MAGIC         CAST(COALESCE(Equipment, '') AS STRING),
# MAGIC         CAST(COALESCE(expense_CAD, '') AS STRING),
# MAGIC         CAST(COALESCE(split_exp, '') AS STRING),
# MAGIC         CAST(COALESCE(Period_Year, '') AS STRING),
# MAGIC         CAST(COALESCE(invoiced_Date, '') AS STRING),
# MAGIC         CAST(COALESCE(Load_flag, '') AS STRING),
# MAGIC         CAST(COALESCE(load_status, '') AS STRING),
# MAGIC         CAST(COALESCE(split_margin, '') AS STRING),
# MAGIC         CAST(COALESCE(Margin_CAD, '') AS STRING),
# MAGIC         CAST(COALESCE(calc_miles, '') AS STRING),
# MAGIC         CAST(COALESCE(Mode, '') AS STRING),
# MAGIC         CAST(COALESCE(ot_del, '') AS STRING),
# MAGIC         CAST(COALESCE(ot_pu, '') AS STRING),
# MAGIC         CAST(COALESCE(origin_city, '') AS STRING),
# MAGIC         CAST(COALESCE(origin_state, '') AS STRING),
# MAGIC         CAST(COALESCE(Origin_Zip, '') AS STRING),
# MAGIC         CAST(COALESCE(Pickup_Appointment_Date, '') AS STRING),
# MAGIC         CAST(COALESCE(Pickup_Date, '') AS STRING),
# MAGIC         CAST(COALESCE(pickup_datetime, '') AS STRING),
# MAGIC         CAST(COALESCE(Pickup_EndDate, '') AS STRING),
# MAGIC         CAST(COALESCE(PreBookStatus, '') AS STRING),
# MAGIC         CAST(COALESCE(revenue_CAD, '') AS STRING),
# MAGIC         CAST(COALESCE(split_rev, '') AS STRING),
# MAGIC         CAST(COALESCE(rolled_date, '') AS STRING),
# MAGIC         CAST(COALESCE(Ship_Date, '') AS STRING),
# MAGIC         CAST(COALESCE(Tendered_Date, '') AS STRING),
# MAGIC         CAST(COALESCE(TMS_System, '') AS STRING),
# MAGIC         CAST(COALESCE(week_num, '') AS STRING),
# MAGIC         CAST(COALESCE(Expense_USD, '') AS STRING),
# MAGIC         CAST(COALESCE(Expense_CAD, '') AS STRING)
# MAGIC     )) AS HashKey
# MAGIC     FROM Tender_Source

# COMMAND ----------

# MAGIC %md
# MAGIC ###DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver.Silver_Transfix (
# MAGIC     DW_Load_ID  VARCHAR(255) NOT NULL,
# MAGIC     Load_Number BIGINT NOT NULL,
# MAGIC     Actual_Delivered_Date TIMESTAMP_NTZ,
# MAGIC     Booked_Date TIMESTAMP_NTZ,
# MAGIC     Carrier_Fuel_Surcharge_CAD DECIMAL(10,2),
# MAGIC     Carrier_Fuel_Surcharge_USD DECIMAL(10,2),
# MAGIC     Carrier_Linehaul_CAD DECIMAL(10,2),
# MAGIC     Carrier_Linehaul_USD DECIMAL(10,2),
# MAGIC     Carrier_Name VARCHAR(255),
# MAGIC     Carrier_Office VARCHAR(255),
# MAGIC     Carrier_Other_Fees_CAD DECIMAL(10,2),
# MAGIC     Carrier_Other_Fees_USD DECIMAL(10,2),
# MAGIC     Customer_Fuel_Surcharge_USD DECIMAL(10,2),
# MAGIC     Customer_Fule_Surcharge_CAD DECIMAL(10,2),
# MAGIC     Customer_Linehaul_CAD DECIMAL(10,2),
# MAGIC     Customer_Linehaul_USD DECIMAL(10,2),
# MAGIC     Customer_Master VARCHAR(255),
# MAGIC     Customer_Name VARCHAR(255),
# MAGIC     Customer_Office VARCHAR(255),
# MAGIC     Customer_Other_Fees_CAD DECIMAL(10,2),
# MAGIC     Customer_Other_Fees_USD DECIMAL(10,2),
# MAGIC     Days_Ahead VARCHAR(255),
# MAGIC     Bounced_Date TIMESTAMP_NTZ,
# MAGIC     Cancelled_Date TIMESTAMP_NTZ,
# MAGIC     Delivery_Appointment_Date TIMESTAMP_NTZ,
# MAGIC     Delivery_EndDate TIMESTAMP_NTZ,
# MAGIC     Destination_City VARCHAR(255),
# MAGIC     Destination_State VARCHAR(255),
# MAGIC     Destination_Zip VARCHAR(255),
# MAGIC     DOT_Number VARCHAR(255),
# MAGIC     Delivery_StartDate TIMESTAMP_NTZ,
# MAGIC     Equipment VARCHAR(255),
# MAGIC     Expense_CAD DECIMAL(10,2),
# MAGIC     Expense_USD DECIMAL(10,2),
# MAGIC     Financial_Period_Year VARCHAR(255),
# MAGIC     Invoiced_Date TIMESTAMP_NTZ,
# MAGIC     Load_Currency VARCHAR(255),
# MAGIC     Load_Flag VARCHAR(255),
# MAGIC     Load_Status VARCHAR(255),
# MAGIC     Margin_CAD DECIMAL(10,2),
# MAGIC     Margin_USD DECIMAL(10,2),
# MAGIC     Miles VARCHAR(255),
# MAGIC     Mode VARCHAR(255),
# MAGIC     On_Time_Delivery VARCHAR(255),
# MAGIC     On_Time_Pickup VARCHAR(255),
# MAGIC     Origin_City VARCHAR(255),
# MAGIC     Origin_State VARCHAR(255),
# MAGIC     Origin_Zip VARCHAR(255),
# MAGIC     Pickup_Appointment_Date TIMESTAMP_NTZ,
# MAGIC     Actual_Pickup_Date TIMESTAMP_NTZ,
# MAGIC     Pickup_EndDate TIMESTAMP_NTZ,
# MAGIC     Pickup_StartDate TIMESTAMP_NTZ,
# MAGIC     PreBookStatus VARCHAR(255),
# MAGIC     Revenue_CAD DECIMAL(10,2),
# MAGIC     Revenue_USD DECIMAL(10,2),
# MAGIC     Rolled_Date TIMESTAMP_NTZ,
# MAGIC     Ship_Date TIMESTAMP_NTZ,
# MAGIC     Tendered_Date TIMESTAMP_NTZ,
# MAGIC     TMS_System VARCHAR(50),
# MAGIC     Week_Number VARCHAR(10),
# MAGIC     Carrier_Final_Rate_USD DECIMAL(10,2),
# MAGIC     Carrier_Final_Rate_CAD DECIMAL(10,2),
# MAGIC     Hashkey STRING NOT NULL,
# MAGIC     Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Created_By VARCHAR(50) NOT NULL,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Last_Modified_By VARCHAR(50) NOT NULL,
# MAGIC     Is_Deleted SMALLINT NOT NULL,
# MAGIC     CONSTRAINT PK_Silver_Transfix PRIMARY KEY (DW_Load_ID)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC