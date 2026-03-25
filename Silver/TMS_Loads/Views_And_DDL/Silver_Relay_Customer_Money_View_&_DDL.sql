-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Relay_Customer_Money
-- MAGIC * **Description:** To extract data from Bronze to Silver as delta file
-- MAGIC * **Created Date:** 03/07/2025
-- MAGIC * **Created By:** Uday
-- MAGIC * **Modified Date:** 06/10/2025
-- MAGIC * **Modified By:** Uday
-- MAGIC * **Changes Made:** Logic Update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Incremental Load View

-- COMMAND ----------

USE SCHEMA bronze;
CREATE OR REPLACE View Silver.VW_Silver_Relay_Customer_Money AS
WITH to_get_avg AS (
  SELECT
    DISTINCT canada_conversions.ship_date,
    canada_conversions.conversion AS us_to_cad,
    canada_conversions.us_to_cad AS cad_to_us
  FROM
    canada_conversions
  ORDER BY
    canada_conversions.ship_date DESC
  LIMIT
    7
), average_conversions AS (
  SELECT
    avg(to_get_avg.us_to_cad) AS avg_us_to_cad,
    avg(to_get_avg.cad_to_us) AS avg_cad_to_us
  FROM
    to_get_avg
),
invoice_start AS (
  SELECT
    DISTINCT relay_reference_number,
    'tl' AS mode,
    invoiced_at :: DATE,
    invoice_number,
    (
      accessorial_amount :: NUMERIC + fuel_surcharge_amount :: NUMERIC + linehaul_amount :: NUMERIC
    ) / 100.00 AS total_invoice_amt
  FROM
    tl_invoice_projection
  UNION
  SELECT
    DISTINCT relay_reference_number,
    'ltl',
    invoiced_at :: DATE,
    invoice_number,
    REPLACE(invoiced_amount, ',', '') :: NUMERIC
  FROM
    ltl_invoice_projection
  ORDER BY
    relay_reference_number,
    invoice_number
),
basic_invoice_info AS (
  SELECT
    DISTINCT relay_reference_number,
    MAX(invoiced_at) AS first_invoiced,
    SUM(total_invoice_amt) AS total_amt_invoiced
  FROM
    invoice_start
  GROUP BY
    relay_reference_number
),
-- min_invoice_date AS (
--   SELECT
--     DISTINCT booking_id,
--     MIN(invoice_date) :: DATE AS min_invoice_date,
--     SUM(total_approved_amount_to_pay) :: FLOAT / 100.00 AS approved_amt
--   FROM
--     integration_hubtran_vendor_invoice_approved
--   GROUP BY
--     booking_id
-- ),
max_schedule_del AS (
  SELECT
    DISTINCT delivery_projection.relay_reference_number,
    max(delivery_projection.scheduled_at :: timestamp) AS max_schedule_del
  FROM
    delivery_projection
  GROUP BY
    delivery_projection.relay_reference_number
),
max_schedule AS (
  SELECT
    DISTINCT pickup_projection.relay_reference_number,
    max(pickup_projection.scheduled_at :: timestamp) AS max_schedule
  FROM
    pickup_projection
  GROUP BY
    pickup_projection.relay_reference_number
),
last_delivery AS (
  SELECT
    DISTINCT planning_stop_schedule.relay_reference_number,
    max(planning_stop_schedule.sequence_number) AS last_delivery
  FROM
    planning_stop_schedule
  WHERE
    planning_stop_schedule.`removed?` = false
  GROUP BY
    planning_stop_schedule.relay_reference_number
),
truckload_proj_del AS (
  SELECT
    DISTINCT truckload_projection.booking_id,
    max(
      truckload_projection.last_update_date_time :: STRING
    ) :: timestamp AS truckload_proj_del
  FROM
    truckload_projection
  WHERE
    lower(
      truckload_projection.last_update_event_name :: STRING
    ) = 'markeddelivered' :: STRING
  GROUP BY
    truckload_projection.booking_id
),
combined_loads_mine AS (
  SELECT
    DISTINCT plan_combination_projection.relay_reference_number_one,
    plan_combination_projection.relay_reference_number_two,
    plan_combination_projection.resulting_plan_id,
    canonical_plan_projection.relay_reference_number AS combine_new_rrn,
    canonical_plan_projection.mode,
    b.tender_on_behalf_of_id,
    'COMBINED' AS combined,
    COALESCE(one_money.one_money, 0.0) AS one_money,
    COALESCE(one_lhc_money.one_lhc_money, 0.0) AS one_lhc_money,
    COALESCE(one_fuel_money.one_fuel_money, 0.0) AS one_fuel_money,
    COALESCE(two_money.two_money, 0.0) AS two_money,
    COALESCE(two_lhc_money.two_lhc_money, 0.0) AS two_lhc_money,
    COALESCE(two_fuel_money.two_fuel_money, 0.0) AS two_fuel_money,
    COALESCE(one_money.one_money, 0.0) + COALESCE(two_money.two_money, 0.0) AS total_load_rev,
    COALESCE(one_lhc_money.one_lhc_money, 0.0) + COALESCE(two_lhc_money.two_lhc_money, 0.0) AS total_load_lhc_rev,
    COALESCE(one_fuel_money.one_fuel_money, 0.0) + COALESCE(two_fuel_money.two_fuel_money, 0.0) AS total_load_fuel_rev,
    COALESCE(carrier_money.carrier_money, 0.0) AS final_carrier_rate,
    COALESCE(carrier_linehaul.carrier_linehaul, 0.0) AS carrier_linehaul,
    COALESCE(fuel_surcharge.fuel_surcharge, 0.0) AS carrier_fuel_surcharge
  FROM
    plan_combination_projection
    JOIN canonical_plan_projection ON plan_combination_projection.resulting_plan_id = canonical_plan_projection.plan_id
    LEFT JOIN booking_projection b ON canonical_plan_projection.relay_reference_number = b.relay_reference_number
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.`voided?` = false
    ) one_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_fuel_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.charge_code = 'fuel_surcharge'
        AND moneying_billing_party_transaction.`voided?` = false
    ) one_fuel_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_lhc_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.charge_code = 'linehaul'
        AND moneying_billing_party_transaction.`voided?` = false
    ) one_lhc_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_lhc_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.charge_code = 'linehaul'
        AND moneying_billing_party_transaction.`voided?` = false
    ) two_lhc_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_fuel_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.charge_code = 'fuel_surcharge'
        AND moneying_billing_party_transaction.`voided?` = false
    ) two_fuel_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.`voided?` = false
    ) two_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(vendor_transaction_projection.amount) / 100.00 AS carrier_money
      FROM
        vendor_transaction_projection
      WHERE
        canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
        AND vendor_transaction_projection.`voided?` = false
    ) carrier_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(vendor_transaction_projection.amount) / 100.00 AS carrier_linehaul
      FROM
        vendor_transaction_projection
      WHERE
        canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
        AND vendor_transaction_projection.charge_code = 'linehaul'
        AND vendor_transaction_projection.`voided?` = false
    ) carrier_linehaul ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(vendor_transaction_projection.amount) / 100.00 AS fuel_surcharge
      FROM
        vendor_transaction_projection
      WHERE
        canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
        AND vendor_transaction_projection.charge_code = 'fuel_surcharge'
        AND vendor_transaction_projection.`voided?` = false
    ) fuel_surcharge ON true
  WHERE
    plan_combination_projection.is_combined = true
),
invoicing_cred AS (
  SELECT
    DISTINCT invoicing_credits.relay_reference_number,
    SUM(
      CASE
        WHEN invoicing_credits.currency = 'CAD' THEN invoicing_credits.total_amount * COALESCE(
          CASE
            WHEN canada_conversions.us_to_cad = 0 THEN NULL
            ELSE canada_conversions.us_to_cad
          END,
          average_conversions.avg_cad_to_us
        )
        ELSE invoicing_credits.total_amount
      END
    ) / 100.00 AS invoicing_cred,
    SUM(
      CASE
        WHEN invoicing_credits.currency = 'USD' THEN invoicing_credits.total_amount * COALESCE(
           canada_conversions.conversion,
            average_conversions.avg_us_to_cad
        )
        ELSE invoicing_credits.total_amount
      END
    ) / 100.00 AS invoicing_cred_CAD
  FROM
    invoicing_credits
    LEFT JOIN canada_conversions ON DATE(invoicing_credits.credited_at) = canada_conversions.ship_date
    JOIN average_conversions ON TRUE
  GROUP BY
    invoicing_credits.relay_reference_number
),

relay_customer_money AS (
  SELECT
    DISTINCT b.booking_id AS Booking_ID,
    b.relay_reference_number as Relay_Reference_Number,
    b.tender_on_behalf_of_id AS Customer_ID,
    customer_profile_projection.Lawson_ID,
    b.status AS Status,
    'TL' AS LTL_or_TL,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS Total_Customer_Rate_WO_Cred_USD,
        COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS Total_Customer_Rate_WO_Cred_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS Customer_Linehaul_USD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS Customer_Linehaul_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS Customer_FuelSurcharge_USD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS Customer_FuelSurcharge_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS Customer_Accessorial_USD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS Customer_Accessorial_CAD,
    COALESCE(invoicing_cred.invoicing_cred, 0) AS Invoicing_Credits_USD,
    COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Invoicing_Credits_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) - COALESCE(invoicing_cred.invoicing_cred, 0) AS Customer_Final_Rate_USD,
    
    COALESCE(SUM(CASE WHEN m.currency = 'CAD' THEN M.amount 
    ELSE m.amount * COALESCE(
        CM.conversion, average_conversions.avg_us_to_cad

    ) END ) FILTER (
        WHERE
          m.`voided?` = false
      ) /100.00, 0) - COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Customer_Final_Rate_CAD,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code = 'stop_off'
    ) AS Stop_Off_Acc,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code IN (
          'detention',
          'detention_at_origin',
          'detention_at_destination'
        )
    ) AS Det_Acc
  FROM
    booking_projection b
    LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
    LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
    LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
  WHERE
    b.status <> 'cancelled' AND b.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R40') 
    AND (
      canonical_plan_projection.mode <> 'ltl'
      OR canonical_plan_projection.mode IS NULL
    )
    AND (
      canonical_plan_projection.status <> 'voided'
      OR canonical_plan_projection.status IS NULL
    )
    AND b.booking_id <> 7370325
    AND m.`voided?` = false
    AND B.Is_Deleted = 0
  GROUP BY
    b.booking_id,
    b.relay_reference_number,
    b.status,
    invoicing_cred.invoicing_cred,
    invoicing_cred.invoicing_cred_CAD,
   
    b.tender_on_behalf_of_id,
    customer_profile_projection.lawson_id
  
  UNION
  SELECT
    DISTINCT b.load_number AS booking_id,
    b.load_number AS relay_reference_number,
    b.customer_id AS Customer_ID,
    CASE 
            WHEN customer_id = 'hain' THEN '880177713'
            WHEN customer_id = 'roland' THEN '880181037'
            WHEN customer_id = 'deb' THEN '880171761' 
            ELSE customer_profile_projection.Lawson_ID END AS Lawson_ID,
    b.dispatch_status AS status,
   
    'LTL' AS ltl_or_tl,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred,
        COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred_USD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END 
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END 
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc_CAD,
    COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
    COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS invoicing_cred_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
        COALESCE(SUM(CASE WHEN m.currency = 'CAD' THEN M.amount 
    ELSE m.amount * COALESCE(
        CM.conversion, average_conversions.avg_us_to_cad

    ) END ) FILTER (
        WHERE
          m.`voided?` = false
      ) /100.00, 0) - COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Converted_CAD_Amount,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code = 'stop_off'
    ) AS stop_off_acc,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code IN (
          'detention',
          'detention_at_origin',
          'detention_at_destination'
        )
    ) AS det_acc
  FROM
    big_export_projection b
    LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
    LEFT JOIN moneying_billing_party_transaction m ON b.load_number = m.relay_reference_number
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN customer_profile_projection ON b.customer_id = customer_profile_projection.customer_slug 
    LEFT JOIN invoicing_cred ON b.load_number = invoicing_cred.relay_reference_number
  WHERE
    b.dispatch_status <> 'Cancelled' AND b.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R46') 
    AND (
      canonical_plan_projection.mode = 'ltl'
      OR canonical_plan_projection.mode IS NULL
    )
    AND (
      canonical_plan_projection.status <> 'voided'
      OR canonical_plan_projection.status IS NULL
    )
    AND (b.customer_id NOT IN ('hain', 'deb', 'roland'))
    AND m.`voided?` = false
    AND B.Is_Deleted = 0
  GROUP BY
    b.load_number,
    b.dispatch_status,
    invoicing_cred.invoicing_cred,
    invoicing_cred.invoicing_cred_CAD,
   
    b.customer_id,
    customer_profile_projection.lawson_id
  UNION
  SELECT
    DISTINCT e.load_number AS booking_id,
    e.load_number AS relay_reference_number,
    e.customer_ID AS Customer_ID,
    CASE 
            WHEN customer_id = 'hain' THEN '880177713'
            WHEN customer_id = 'roland' THEN '880181037'
            WHEN customer_id = 'deb' THEN '880171761' 
            ELSE customer_profile_projection.Lawson_ID END AS Lawson_ID,
    e.dispatch_status AS status,
    'LTL' AS ltl_or_tl,
    (
      e.carrier_accessorial_expense + CEIL(
        CASE
          e.customer_id
          WHEN 'deb' THEN CASE
            e.carrier_id
            WHEN '300003979' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1500
            )
            ELSE CASE
              WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
              ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
            END
          END
          WHEN 'hain' THEN GREATEST(
            e.carrier_linehaul_expense * (10.0 / 100.0),
            1000
          )
          WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
          ELSE 0
        END + e.carrier_linehaul_expense
      ) + CEIL(
        CEIL(
          CASE
            e.customer_id
            WHEN 'deb' THEN CASE
              e.carrier_id
              WHEN '300003979' THEN GREATEST(
                e.carrier_linehaul_expense * (10.0 / 100.0),
                1500
              )
              ELSE CASE
                WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
                ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
              END
            END
            WHEN 'hain' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1000
            )
            WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
            ELSE 0
          END + e.carrier_linehaul_expense
        ) * CASE
          e.carrier_linehaul_expense
          WHEN 0 THEN 0
          ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
        END
      )
    ) / 100 AS total_cust_rate_wo_cred,
    total_cust_rate_wo_cred * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ) as total_cust_rate_wo_cred_CAD, 
    CEIL(
      CASE
        e.customer_id
        WHEN 'deb' THEN CASE
          e.carrier_id
          WHEN '300003979' THEN GREATEST(
            e.carrier_linehaul_expense * (10.0 / 100.0),
            1500
          )
          ELSE CASE
            WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
            ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
          END
        END
        WHEN 'hain' THEN GREATEST(
          e.carrier_linehaul_expense * (10.0 / 100.0),
          1000
        )
        WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
        ELSE 0
      END + e.carrier_linehaul_expense
    ) / 100 AS customer_lhc,
    customer_lhc * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ),
    CEIL(
      CEIL(
        CASE
          e.customer_id
          WHEN 'deb' THEN CASE
            e.carrier_id
            WHEN '300003979' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1500
            )
            ELSE CASE
              WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
              ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
            END
          END
          WHEN 'hain' THEN GREATEST(
            e.carrier_linehaul_expense * (10.0 / 100.0),
            1000
          )
          WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
          ELSE 0
        END + e.carrier_linehaul_expense
      ) * CASE
        e.carrier_linehaul_expense
        WHEN 0 THEN 0
        ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
      END
    ) / 100 AS customer_fsc,
    customer_fsc * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ),
    e.carrier_accessorial_expense / 100 AS customer_acc,
    customer_acc * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ),
    0 AS invoicing_cred,
    0,
    (
      e.carrier_accessorial_expense + CEIL(
        CASE
          e.customer_id
          WHEN 'deb' THEN CASE
            e.carrier_id
            WHEN '300003979' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1500
            )
            ELSE CASE
              WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
              ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
            END
          END
          WHEN 'hain' THEN GREATEST(
            e.carrier_linehaul_expense * (10.0 / 100.0),
            1000
          )
          WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
          ELSE 0
        END + e.carrier_linehaul_expense
      ) + CEIL(
        CEIL(
          CASE
            e.customer_id
            WHEN 'deb' THEN CASE
              e.carrier_id
              WHEN '300003979' THEN GREATEST(
                e.carrier_linehaul_expense * (10.0 / 100.0),
                1500
              )
              ELSE CASE
                WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
                ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
              END
            END
            WHEN 'hain' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1000
            )
            WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
            ELSE 0
          END + e.carrier_linehaul_expense
        ) * CASE
          e.carrier_linehaul_expense
          WHEN 0 THEN 0
          ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
        END
      )
    ) / 100 AS final_customer_rate,
    final_customer_rate * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ), 
    NULL AS stop_off_acc,
    NULL AS det_acc
  FROM
    big_export_projection e
    LEFT JOIN canonical_plan_projection ON e.load_number = canonical_plan_projection.relay_reference_number
    LEFT JOIN moneying_billing_party_transaction m ON e.load_number = m.relay_reference_number
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN customer_profile_projection ON e.customer_id = customer_profile_projection.customer_slug 
  WHERE
    e.dispatch_status <> 'Cancelled' AND e.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R46') 
    AND (
      canonical_plan_projection.mode = 'ltl'
      OR canonical_plan_projection.mode IS NULL
    )
    AND (
      canonical_plan_projection.status <> 'voided'
      OR canonical_plan_projection.status IS NULL
    )
    AND (e.customer_id IN ('hain', 'deb', 'roland'))
  UNION
  SELECT
    DISTINCT b.booking_id,
    b.relay_reference_number,
    b.tender_on_behalf_of_id AS Customer_ID,
    customer_profile_projection.Lawson_ID,
    b.status,
   
    'TL' AS ltl_or_tl,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred,
     COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred_cad,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc_CAD,
    COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
    COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS invoicing_cred_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    )  - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
        COALESCE(SUM(CASE WHEN m.currency = 'CAD' THEN M.amount 
    ELSE m.amount * COALESCE(
        CM.conversion, average_conversions.avg_us_to_cad

    ) END ) FILTER (
        WHERE
          m.`voided?` = false
      ) /100.00, 0) - COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Converted_CAD_Amount,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code = 'stop_off'
    ) AS stop_off_acc,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code IN (
          'detention',
          'detention_at_origin',
          'detention_at_destination'
        )
    ) AS det_acc
  FROM
    booking_projection b
    JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_one
    LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
    LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
  WHERE
    m.`voided?` = false and B.Is_Deleted = 0 AND b.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R40') 
  GROUP BY
    b.booking_id,
    b.relay_reference_number,
    b.status,
    invoicing_cred.invoicing_cred,
    invoicing_cred.invoicing_cred_CAD,
   
    b.tender_on_behalf_of_id,
    customer_profile_projection.lawson_id
  UNION
  SELECT
    DISTINCT b.booking_id,
    b.relay_reference_number,
    b.tender_on_behalf_of_id AS Customer_ID,
    customer_profile_projection.Lawson_ID,
    b.status,
   
    'TL' AS ltl_or_tl,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred_CAD,
    
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc_CAD,
    COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
    COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS invoicing_cred_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
        COALESCE(SUM(CASE WHEN m.currency = 'CAD' THEN M.amount 
    ELSE m.amount * COALESCE(
        CM.conversion, average_conversions.avg_us_to_cad

    ) END )FILTER (
        WHERE
          m.`voided?` = false
      ) /100.00, 0) - COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Converted_CAD_Amount,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code = 'stop_off'
    ) AS stop_off_acc,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code IN (
          'detention',
          'detention_at_origin',
          'detention_at_destination'
        )
    ) AS det_acc
  FROM
    booking_projection b 
    JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_two
    LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
    LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
  WHERE
    m.`voided?` = 'false' AND B.Is_Deleted = 0
    AND b.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R40') 

  GROUP BY
    b.booking_id,
    b.relay_reference_number,
    b.status,
    invoicing_cred.invoicing_cred,
    invoicing_cred.invoicing_cred_CAD,
   
    b.tender_on_behalf_of_id,
    customer_profile_projection.lawson_id
),

Load_Currency as (SELECT  DISTINCT relay_reference_number, currency FROM
(SELECT
  relay_reference_number, Currency,
  RANK() over (PARTITION BY relay_reference_number ORDER BY amount, updated_at DESC) as rank
FROM
  bronze.moneying_billing_party_transaction
WHERE
  `voided?` = false) WHERE rank = 1),

Final as (
  SELECT relay_customer_money.*, CASE WHEN customer_id IN ('hain', 'deb', 'roland') THEN 'USD'  ELSE currency END as Load_Currency FROM relay_customer_money LEFT JOIN Load_Currency ON relay_customer_money.relay_reference_number = Load_Currency.relay_reference_number 
)

SELECT DISTINCT *, MD5(CONCAT(
  COALESCE(Customer_ID, ''),
  COALESCE(Lawson_ID, ''),
  COALESCE(Status, ''),
  COALESCE(Load_Currency, ''),
  COALESCE(LTL_or_TL, ''),
  COALESCE(Total_Customer_Rate_WO_Cred_CAD, 0),
  COALESCE(Total_Customer_Rate_WO_Cred_USD, 0),
  COALESCE(Customer_Linehaul_USD, 0),
  COALESCE(Customer_Linehaul_CAD, 0),
  COALESCE(Customer_FuelSurcharge_USD, 0),
  COALESCE(Customer_FuelSurcharge_CAD, 0),
  COALESCE(Customer_Accessorial_USD, 0),
  COALESCE(Customer_Accessorial_CAD, 0),
  COALESCE(Invoicing_Credits_USD, 0),
  COALESCE(Invoicing_Credits_CAD, 0),
  COALESCE(Customer_Final_Rate_USD, 0),
  COALESCE(Customer_Final_Rate_CAD, 0),
  COALESCE(Stop_Off_Acc, 0),
  COALESCE(Det_Acc, 0)
)) AS Hashkey 

FROM Final;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Full Load View

-- COMMAND ----------

USE SCHEMA bronze;
CREATE OR REPLACE VIEW Silver.VW_Silver_Relay_Customer_Money_Full_Load AS
WITH to_get_avg AS (
  SELECT
    DISTINCT canada_conversions.ship_date,
    canada_conversions.conversion AS us_to_cad,
    canada_conversions.us_to_cad AS cad_to_us
  FROM
    canada_conversions
  ORDER BY
    canada_conversions.ship_date DESC
  LIMIT
    7
), average_conversions AS (
  SELECT
    avg(to_get_avg.us_to_cad) AS avg_us_to_cad,
    avg(to_get_avg.cad_to_us) AS avg_cad_to_us
  FROM
    to_get_avg
),
invoice_start AS (
  SELECT
    DISTINCT relay_reference_number,
    'tl' AS mode,
    invoiced_at :: DATE,
    invoice_number,
    (
      accessorial_amount :: NUMERIC + fuel_surcharge_amount :: NUMERIC + linehaul_amount :: NUMERIC
    ) / 100.00 AS total_invoice_amt
  FROM
    tl_invoice_projection
  UNION
  SELECT
    DISTINCT relay_reference_number,
    'ltl',
    invoiced_at :: DATE,
    invoice_number,
    REPLACE(invoiced_amount, ',', '') :: NUMERIC
  FROM
    ltl_invoice_projection
  ORDER BY
    relay_reference_number,
    invoice_number
),
basic_invoice_info AS (
  SELECT
    DISTINCT relay_reference_number,
    MAX(invoiced_at) AS first_invoiced,
    SUM(total_invoice_amt) AS total_amt_invoiced
  FROM
    invoice_start
  GROUP BY
    relay_reference_number
),
-- min_invoice_date AS (
--   SELECT
--     DISTINCT booking_id,
--     MIN(invoice_date) :: DATE AS min_invoice_date,
--     SUM(total_approved_amount_to_pay) :: FLOAT / 100.00 AS approved_amt
--   FROM
--     integration_hubtran_vendor_invoice_approved
--   GROUP BY
--     booking_id
-- ),
max_schedule_del AS (
  SELECT
    DISTINCT delivery_projection.relay_reference_number,
    max(delivery_projection.scheduled_at :: timestamp) AS max_schedule_del
  FROM
    delivery_projection
  GROUP BY
    delivery_projection.relay_reference_number
),
max_schedule AS (
  SELECT
    DISTINCT pickup_projection.relay_reference_number,
    max(pickup_projection.scheduled_at :: timestamp) AS max_schedule
  FROM
    pickup_projection
  GROUP BY
    pickup_projection.relay_reference_number
),
last_delivery AS (
  SELECT
    DISTINCT planning_stop_schedule.relay_reference_number,
    max(planning_stop_schedule.sequence_number) AS last_delivery
  FROM
    planning_stop_schedule
  WHERE
    planning_stop_schedule.`removed?` = false
  GROUP BY
    planning_stop_schedule.relay_reference_number
),
truckload_proj_del AS (
  SELECT
    DISTINCT truckload_projection.booking_id,
    max(
      truckload_projection.last_update_date_time :: STRING
    ) :: timestamp AS truckload_proj_del
  FROM
    truckload_projection
  WHERE
    lower(
      truckload_projection.last_update_event_name :: STRING
    ) = 'markeddelivered' :: STRING
  GROUP BY
    truckload_projection.booking_id
),
combined_loads_mine AS (
  SELECT
    DISTINCT plan_combination_projection.relay_reference_number_one,
    plan_combination_projection.relay_reference_number_two,
    plan_combination_projection.resulting_plan_id,
    canonical_plan_projection.relay_reference_number AS combine_new_rrn,
    canonical_plan_projection.mode,
    b.tender_on_behalf_of_id,
    'COMBINED' AS combined,
    COALESCE(one_money.one_money, 0.0) AS one_money,
    COALESCE(one_lhc_money.one_lhc_money, 0.0) AS one_lhc_money,
    COALESCE(one_fuel_money.one_fuel_money, 0.0) AS one_fuel_money,
    COALESCE(two_money.two_money, 0.0) AS two_money,
    COALESCE(two_lhc_money.two_lhc_money, 0.0) AS two_lhc_money,
    COALESCE(two_fuel_money.two_fuel_money, 0.0) AS two_fuel_money,
    COALESCE(one_money.one_money, 0.0) + COALESCE(two_money.two_money, 0.0) AS total_load_rev,
    COALESCE(one_lhc_money.one_lhc_money, 0.0) + COALESCE(two_lhc_money.two_lhc_money, 0.0) AS total_load_lhc_rev,
    COALESCE(one_fuel_money.one_fuel_money, 0.0) + COALESCE(two_fuel_money.two_fuel_money, 0.0) AS total_load_fuel_rev,
    COALESCE(carrier_money.carrier_money, 0.0) AS final_carrier_rate,
    COALESCE(carrier_linehaul.carrier_linehaul, 0.0) AS carrier_linehaul,
    COALESCE(fuel_surcharge.fuel_surcharge, 0.0) AS carrier_fuel_surcharge
  FROM
    plan_combination_projection
    JOIN canonical_plan_projection ON plan_combination_projection.resulting_plan_id = canonical_plan_projection.plan_id
    LEFT JOIN booking_projection b ON canonical_plan_projection.relay_reference_number = b.relay_reference_number
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.`voided?` = false
    ) one_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_fuel_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.charge_code = 'fuel_surcharge'
        AND moneying_billing_party_transaction.`voided?` = false
    ) one_fuel_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_lhc_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.charge_code = 'linehaul'
        AND moneying_billing_party_transaction.`voided?` = false
    ) one_lhc_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_lhc_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.charge_code = 'linehaul'
        AND moneying_billing_party_transaction.`voided?` = false
    ) two_lhc_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_fuel_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.charge_code = 'fuel_surcharge'
        AND moneying_billing_party_transaction.`voided?` = false
    ) two_fuel_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_money
      FROM
        moneying_billing_party_transaction
      WHERE
        plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
        AND moneying_billing_party_transaction.`voided?` = false
    ) two_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(vendor_transaction_projection.amount) / 100.00 AS carrier_money
      FROM
        vendor_transaction_projection
      WHERE
        canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
        AND vendor_transaction_projection.`voided?` = false
    ) carrier_money ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(vendor_transaction_projection.amount) / 100.00 AS carrier_linehaul
      FROM
        vendor_transaction_projection
      WHERE
        canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
        AND vendor_transaction_projection.charge_code = 'linehaul'
        AND vendor_transaction_projection.`voided?` = false
    ) carrier_linehaul ON true
    LEFT JOIN LATERAL (
      SELECT
        SUM(vendor_transaction_projection.amount) / 100.00 AS fuel_surcharge
      FROM
        vendor_transaction_projection
      WHERE
        canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
        AND vendor_transaction_projection.charge_code = 'fuel_surcharge'
        AND vendor_transaction_projection.`voided?` = false
    ) fuel_surcharge ON true
  WHERE
    plan_combination_projection.is_combined = true
),
invoicing_cred AS (
  SELECT
    DISTINCT invoicing_credits.relay_reference_number,
    SUM(
      CASE
        WHEN invoicing_credits.currency = 'CAD' THEN invoicing_credits.total_amount * COALESCE(
          CASE
            WHEN canada_conversions.us_to_cad = 0 THEN NULL
            ELSE canada_conversions.us_to_cad
          END,
          average_conversions.avg_cad_to_us
        )
        ELSE invoicing_credits.total_amount
      END
    ) / 100.00 AS invoicing_cred,
    SUM(
      CASE
        WHEN invoicing_credits.currency = 'USD' THEN invoicing_credits.total_amount * COALESCE(
           canada_conversions.conversion,
            average_conversions.avg_us_to_cad
        )
        ELSE invoicing_credits.total_amount
      END
    ) / 100.00 AS invoicing_cred_CAD
  FROM
    invoicing_credits
    LEFT JOIN canada_conversions ON DATE(invoicing_credits.credited_at) = canada_conversions.ship_date
    JOIN average_conversions ON TRUE
  GROUP BY
    invoicing_credits.relay_reference_number
),

relay_customer_money AS ( 
  SELECT
    DISTINCT b.booking_id AS Booking_ID,
    b.relay_reference_number as Relay_Reference_Number,
    b.tender_on_behalf_of_id AS Customer_ID,
    customer_profile_projection.Lawson_ID,
    b.status AS Status,

    'TL' AS LTL_or_TL,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS Total_Customer_Rate_WO_Cred_USD,
        COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS Total_Customer_Rate_WO_Cred_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS Customer_Linehaul_USD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS Customer_Linehaul_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS Customer_FuelSurcharge_USD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS Customer_FuelSurcharge_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS Customer_Accessorial_USD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS Customer_Accessorial_CAD,
    COALESCE(invoicing_cred.invoicing_cred, 0) AS Invoicing_Credits_USD,
    COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Invoicing_Credits_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) - COALESCE(invoicing_cred.invoicing_cred, 0) AS Customer_Final_Rate_USD,
    
    COALESCE(SUM(CASE WHEN m.currency = 'CAD' THEN M.amount 
    ELSE m.amount * COALESCE(
        CM.conversion, average_conversions.avg_us_to_cad

    ) END ) FILTER (
        WHERE
          m.`voided?` = false
      ) /100.00, 0) - COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Customer_Final_Rate_CAD,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code = 'stop_off'
    ) AS Stop_Off_Acc,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code IN (
          'detention',
          'detention_at_origin',
          'detention_at_destination'
        )
    ) AS Det_Acc
  FROM
    booking_projection b
    LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
    LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number 
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
    LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
  WHERE
    b.status <> 'cancelled' 
    AND (
      canonical_plan_projection.mode <> 'ltl'
      OR canonical_plan_projection.mode IS NULL
    )
    AND (
      canonical_plan_projection.status <> 'voided'
      OR canonical_plan_projection.status IS NULL
    )
    AND b.booking_id <> 7370325
    AND m.`voided?` = "false"
    AND B.Is_Deleted = 0
  GROUP BY
    b.booking_id,
    b.relay_reference_number,
    b.status,
    invoicing_cred.invoicing_cred,
    invoicing_cred.invoicing_cred_CAD,
  
    b.tender_on_behalf_of_id,
    customer_profile_projection.lawson_id
  
  UNION
  SELECT
    DISTINCT b.load_number AS booking_id,
    b.load_number AS relay_reference_number,
    b.customer_id AS Customer_ID,
    CASE 
            WHEN customer_id = 'hain' THEN '880177713'
            WHEN customer_id = 'roland' THEN '880181037'
            WHEN customer_id = 'deb' THEN '880171761' 
            ELSE customer_profile_projection.Lawson_ID END AS Lawson_ID,
    b.dispatch_status AS status,
  
    'LTL' AS ltl_or_tl,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred,
        COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred_USD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END 
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END 
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc_CAD,
    COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
    COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS invoicing_cred_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
        COALESCE(SUM(CASE WHEN m.currency = 'CAD' THEN M.amount 
    ELSE m.amount * COALESCE(
        CM.conversion, average_conversions.avg_us_to_cad

    ) END ) FILTER (
        WHERE
          m.`voided?` = false
      ) /100.00, 0) - COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Converted_CAD_Amount,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code = 'stop_off'
    ) AS stop_off_acc,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code IN (
          'detention',
          'detention_at_origin',
          'detention_at_destination'
        )
    ) AS det_acc
  FROM
    big_export_projection b
    LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
    LEFT JOIN moneying_billing_party_transaction m ON b.load_number = m.relay_reference_number
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN customer_profile_projection ON b.customer_id = customer_profile_projection.customer_slug 
    LEFT JOIN invoicing_cred ON b.load_number = invoicing_cred.relay_reference_number
  WHERE
    b.dispatch_status <> 'Cancelled' 
    AND (
      canonical_plan_projection.mode = 'ltl'
      OR canonical_plan_projection.mode IS NULL
    )
    AND (
      canonical_plan_projection.status <> 'voided'
      OR canonical_plan_projection.status IS NULL
    )
    AND (b.customer_id NOT IN ('hain', 'deb', 'roland'))
    AND m.`voided?` = false
    AND B.Is_Deleted = 0
  GROUP BY
    b.load_number,
    b.dispatch_status,
    invoicing_cred.invoicing_cred,
    invoicing_cred.invoicing_cred_CAD,
  
    b.customer_id,
    customer_profile_projection.lawson_id
  UNION
  SELECT
    DISTINCT e.load_number AS booking_id,
    e.load_number AS relay_reference_number,
    e.customer_ID AS Customer_ID,
    CASE 
            WHEN customer_id = 'hain' THEN '880177713'
            WHEN customer_id = 'roland' THEN '880181037'
            WHEN customer_id = 'deb' THEN '880171761' 
            ELSE customer_profile_projection.Lawson_ID END AS Lawson_ID,
    e.dispatch_status AS status,
    'LTL' AS ltl_or_tl,
    (
      e.carrier_accessorial_expense + CEIL(
        CASE
          e.customer_id
          WHEN 'deb' THEN CASE
            e.carrier_id
            WHEN '300003979' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1500
            )
            ELSE CASE
              WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
              ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
            END
          END
          WHEN 'hain' THEN GREATEST(
            e.carrier_linehaul_expense * (10.0 / 100.0),
            1000
          )
          WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
          ELSE 0
        END + e.carrier_linehaul_expense
      ) + CEIL(
        CEIL(
          CASE
            e.customer_id
            WHEN 'deb' THEN CASE
              e.carrier_id
              WHEN '300003979' THEN GREATEST(
                e.carrier_linehaul_expense * (10.0 / 100.0),
                1500
              )
              ELSE CASE
                WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
                ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
              END
            END
            WHEN 'hain' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1000
            )
            WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
            ELSE 0
          END + e.carrier_linehaul_expense
        ) * CASE
          e.carrier_linehaul_expense
          WHEN 0 THEN 0
          ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
        END
      )
    ) / 100 AS total_cust_rate_wo_cred,
    total_cust_rate_wo_cred * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ) as total_cust_rate_wo_cred_CAD, 
    CEIL(
      CASE
        e.customer_id
        WHEN 'deb' THEN CASE
          e.carrier_id
          WHEN '300003979' THEN GREATEST(
            e.carrier_linehaul_expense * (10.0 / 100.0),
            1500
          )
          ELSE CASE
            WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
            ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
          END
        END
        WHEN 'hain' THEN GREATEST(
          e.carrier_linehaul_expense * (10.0 / 100.0),
          1000
        )
        WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
        ELSE 0
      END + e.carrier_linehaul_expense
    ) / 100 AS customer_lhc,
    customer_lhc * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ),
    CEIL(
      CEIL(
        CASE
          e.customer_id
          WHEN 'deb' THEN CASE
            e.carrier_id
            WHEN '300003979' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1500
            )
            ELSE CASE
              WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
              ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
            END
          END
          WHEN 'hain' THEN GREATEST(
            e.carrier_linehaul_expense * (10.0 / 100.0),
            1000
          )
          WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
          ELSE 0
        END + e.carrier_linehaul_expense
      ) * CASE
        e.carrier_linehaul_expense
        WHEN 0 THEN 0
        ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
      END
    ) / 100 AS customer_fsc,
    customer_fsc * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ),
    e.carrier_accessorial_expense / 100 AS customer_acc,
    customer_acc * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ),
    0 AS invoicing_cred,
    0,
    (
      e.carrier_accessorial_expense + CEIL(
        CASE
          e.customer_id
          WHEN 'deb' THEN CASE
            e.carrier_id
            WHEN '300003979' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1500
            )
            ELSE CASE
              WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
              ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
            END
          END
          WHEN 'hain' THEN GREATEST(
            e.carrier_linehaul_expense * (10.0 / 100.0),
            1000
          )
          WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
          ELSE 0
        END + e.carrier_linehaul_expense
      ) + CEIL(
        CEIL(
          CASE
            e.customer_id
            WHEN 'deb' THEN CASE
              e.carrier_id
              WHEN '300003979' THEN GREATEST(
                e.carrier_linehaul_expense * (10.0 / 100.0),
                1500
              )
              ELSE CASE
                WHEN DATE(e.ship_date) < DATE('2019-03-09') THEN GREATEST(e.carrier_linehaul_expense * (8.0 / 100.0), 1000)
                ELSE GREATEST(e.carrier_linehaul_expense * (9.0 / 100.0), 1000)
              END
            END
            WHEN 'hain' THEN GREATEST(
              e.carrier_linehaul_expense * (10.0 / 100.0),
              1000
            )
            WHEN 'roland' THEN e.carrier_linehaul_expense * 0.1365
            ELSE 0
          END + e.carrier_linehaul_expense
        ) * CASE
          e.carrier_linehaul_expense
          WHEN 0 THEN 0
          ELSE e.carrier_fuel_expense / e.carrier_linehaul_expense
        END
      )
    ) / 100 AS final_customer_rate,
    final_customer_rate * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          ), 
    NULL AS stop_off_acc,
    NULL AS det_acc
  FROM
    big_export_projection e
    LEFT JOIN canonical_plan_projection ON e.load_number = canonical_plan_projection.relay_reference_number
    LEFT JOIN moneying_billing_party_transaction m ON e.load_number = m.relay_reference_number 
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN customer_profile_projection ON e.customer_id = customer_profile_projection.customer_slug 
  WHERE
    e.dispatch_status <> 'Cancelled' 
    AND (
      canonical_plan_projection.mode = 'ltl'
      OR canonical_plan_projection.mode IS NULL
    )
    AND (
      canonical_plan_projection.status <> 'voided'
      OR canonical_plan_projection.status IS NULL
    )
    AND (e.customer_id IN ('hain', 'deb', 'roland'))
  UNION
  SELECT
    DISTINCT b.booking_id,
    b.relay_reference_number,
    b.tender_on_behalf_of_id AS Customer_ID,
    customer_profile_projection.Lawson_ID,
    b.status,
  
    'TL' AS ltl_or_tl,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred,
     COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred_cad,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc_CAD,
    COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
    COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS invoicing_cred_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    )  - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
        COALESCE(SUM(CASE WHEN m.currency = 'CAD' THEN M.amount 
    ELSE m.amount * COALESCE(
        CM.conversion, average_conversions.avg_us_to_cad

    ) END ) FILTER (
        WHERE
          m.`voided?` = false
      ) /100.00, 0) - COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Converted_CAD_Amount,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code = 'stop_off'
    ) AS stop_off_acc,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code IN (
          'detention',
          'detention_at_origin',
          'detention_at_destination'
        )
    ) AS det_acc
  FROM
    booking_projection b
    JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_one
    LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number 
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
    LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
  WHERE
    m.`voided?` = false and B.Is_Deleted = 0 
  GROUP BY
    b.booking_id,
    b.relay_reference_number,
    b.status,
    invoicing_cred.invoicing_cred,
    invoicing_cred.invoicing_cred_CAD,
  
    b.tender_on_behalf_of_id,
    customer_profile_projection.lawson_id
  UNION
  SELECT
    DISTINCT b.booking_id,
    b.relay_reference_number,
    b.tender_on_behalf_of_id AS Customer_ID,
    customer_profile_projection.Lawson_ID,
    b.status,
  
    'TL' AS ltl_or_tl,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.conversion = 0 THEN NULL
              ELSE cm.conversion
            END,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) AS total_cust_rate_wo_cred_CAD,
    
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'linehaul'
      ) / 100.00,
      0
    ) AS customer_lhc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND m.charge_code = 'fuel_surcharge'
      ) / 100.00,
      0
    ) AS customer_fsc_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'USD' THEN m.amount * COALESCE(
            cm.conversion,
            average_conversions.avg_us_to_cad
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
          AND (
            m.charge_code NOT IN ('linehaul', 'fuel_surcharge')
          )
      ) / 100.00,
      0
    ) AS customer_acc_CAD,
    COALESCE(invoicing_cred.invoicing_cred, 0) AS invoicing_cred,
    COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS invoicing_cred_CAD,
    COALESCE(
      SUM(
        CASE
          WHEN m.currency = 'CAD' THEN m.amount * COALESCE(
            CASE
              WHEN cm.us_to_cad = 0 THEN NULL
              ELSE cm.us_to_cad
            END,
            average_conversions.avg_cad_to_us
          )
          ELSE m.amount
        END
      ) FILTER (
        WHERE
          m.`voided?` = false
      ) / 100.00,
      0
    ) - COALESCE(invoicing_cred.invoicing_cred, 0) AS final_customer_rate,
        COALESCE(SUM(CASE WHEN m.currency = 'CAD' THEN M.amount 
    ELSE m.amount * COALESCE(
        CM.conversion, average_conversions.avg_us_to_cad

    ) END )FILTER (
        WHERE
          m.`voided?` = 'false'
      ) /100.00, 0) - COALESCE(invoicing_cred.invoicing_cred_CAD, 0) AS Converted_CAD_Amount,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code = 'stop_off'
    ) AS stop_off_acc,
    SUM(m.amount) FILTER (
      WHERE
        m.charge_code IN (
          'detention',
          'detention_at_origin',
          'detention_at_destination'
        )
    ) AS det_acc
  FROM
    booking_projection b 
    JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.relay_reference_number_two
    LEFT JOIN moneying_billing_party_transaction m ON b.relay_reference_number = m.relay_reference_number
    LEFT JOIN canada_conversions cm ON DATE(m.incurred_at) = cm.ship_date
    JOIN average_conversions ON TRUE
    LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
    LEFT JOIN invoicing_cred ON b.relay_reference_number = invoicing_cred.relay_reference_number
  WHERE
    m.`voided?` = 'false' AND B.Is_Deleted = 0
  
  GROUP BY
    b.booking_id,
    b.relay_reference_number,
    b.status,
    invoicing_cred.invoicing_cred,
    invoicing_cred.invoicing_cred_CAD,
  
    b.tender_on_behalf_of_id,
    customer_profile_projection.lawson_id
),

Load_Currency as (SELECT  DISTINCT relay_reference_number, currency FROM
(SELECT
  relay_reference_number, Currency,
  RANK() over (PARTITION BY relay_reference_number ORDER BY amount, updated_at DESC) as rank
FROM
  bronze.moneying_billing_party_transaction
WHERE
  `voided?` = false) WHERE rank = 1),

Final as (
  SELECT relay_customer_money.*, CASE WHEN customer_id IN ('hain', 'deb', 'roland') THEN 'USD'  ELSE currency END as Load_Currency FROM relay_customer_money LEFT JOIN Load_Currency ON relay_customer_money.relay_reference_number = Load_Currency.relay_reference_number 
)

SELECT DISTINCT *, MD5(CONCAT(
  COALESCE(Customer_ID, ''),
  COALESCE(Lawson_ID, ''),
  COALESCE(Status, ''),
  COALESCE(Load_Currency, ''),
  COALESCE(LTL_or_TL, ''),
  COALESCE(Total_Customer_Rate_WO_Cred_CAD, 0),
  COALESCE(Total_Customer_Rate_WO_Cred_USD, 0),
  COALESCE(Customer_Linehaul_USD, 0),
  COALESCE(Customer_Linehaul_CAD, 0),
  COALESCE(Customer_FuelSurcharge_USD, 0),
  COALESCE(Customer_FuelSurcharge_CAD, 0),
  COALESCE(Customer_Accessorial_USD, 0),
  COALESCE(Customer_Accessorial_CAD, 0),
  COALESCE(Invoicing_Credits_USD, 0),
  COALESCE(Invoicing_Credits_CAD, 0),
  COALESCE(Customer_Final_Rate_USD, 0),
  COALESCE(Customer_Final_Rate_CAD, 0),
  COALESCE(Stop_Off_Acc, 0),
  COALESCE(Det_Acc, 0)
)) AS Hashkey
  FROM Final;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####DDL

-- COMMAND ----------

CREATE OR REPLACE TABLE Silver.Silver_Relay_Customer_Money (
    Booking_ID BIGINT NOT NULL,
    Relay_Reference_Number BIGINT ,
    Customer_ID VARCHAR(255) ,
    Lawson_ID VARCHAR(255) ,
    Status VARCHAR(50) ,
    Load_Currency VARCHAR(50) ,
    LTL_or_TL VARCHAR(50) ,
    Total_Customer_Rate_WO_Cred_CAD DECIMAL(10,2) ,
    Total_Customer_Rate_WO_Cred_USD DECIMAL(10,2) ,
    Customer_Linehaul_USD DECIMAL(10,2) ,
    Customer_Linehaul_CAD DECIMAL(10,2) ,
    Customer_FuelSurcharge_USD DECIMAL(10,2) ,
    Customer_FuelSurcharge_CAD DECIMAL(10,2) ,
    Customer_Accessorial_USD DECIMAL(10,2) ,
    Customer_Accessorial_CAD DECIMAL(10,2) ,
    Invoicing_Credits_USD DECIMAL(10,2) ,
    Invoicing_Credits_CAD DECIMAL(10,2) ,
    Customer_Final_Rate_USD DECIMAL(10,2) ,
    Customer_Final_Rate_CAD DECIMAL(10,2) ,
    Stop_Off_Acc DECIMAL(10,2) ,
    Det_Acc DECIMAL(10,2) ,
    Hashkey STRING NOT NULL,
    Created_Date TIMESTAMP_NTZ NOT NULL,
    Created_By VARCHAR(50) NOT NULL,
    Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
    Last_Modified_By VARCHAR(50) NOT NULL,
    Is_Deleted SMALLINT NOT NULL,

    CONSTRAINT PK_booking_id PRIMARY KEY (Booking_ID)
);