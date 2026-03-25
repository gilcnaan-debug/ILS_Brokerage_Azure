# Databricks notebook source
# MAGIC %md
# MAGIC ### Relay
# MAGIC * **Description:** To incrementally extract data from Bronze to Silver as delta file
# MAGIC * **Created Date:** 03/07/2025
# MAGIC * **Created By:** Uday
# MAGIC * **Modified Date:** 06/10/2025
# MAGIC * **Modified By:** Uday
# MAGIC * **Changes Made:** Logic Update

# COMMAND ----------

# MAGIC %md
# MAGIC ####Incremental Load View

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema bronze;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Relay AS
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
# MAGIC ),
# MAGIC -- min_invoice_date AS (
# MAGIC --   SELECT
# MAGIC --     DISTINCT booking_id,
# MAGIC --     MIN(invoice_date) :: DATE AS min_invoice_date,
# MAGIC --     SUM(total_approved_amount_to_pay) :: FLOAT / 100.00 AS approved_amt
# MAGIC --   FROM
# MAGIC --     integration_hubtran_vendor_invoice_approved
# MAGIC --   GROUP BY
# MAGIC --     booking_id
# MAGIC -- ),
# MAGIC max_schedule_del AS (
# MAGIC   SELECT
# MAGIC     DISTINCT delivery_projection.relay_reference_number,
# MAGIC     max(delivery_projection.scheduled_at :: timestamp) AS max_schedule_del
# MAGIC   FROM
# MAGIC     delivery_projection
# MAGIC   GROUP BY
# MAGIC     delivery_projection.relay_reference_number
# MAGIC ),
# MAGIC max_schedule AS (
# MAGIC   SELECT
# MAGIC     DISTINCT pickup_projection.relay_reference_number,
# MAGIC     max(pickup_projection.scheduled_at :: timestamp) AS max_schedule
# MAGIC   FROM
# MAGIC     pickup_projection
# MAGIC   GROUP BY
# MAGIC     pickup_projection.relay_reference_number
# MAGIC ),
# MAGIC last_delivery AS (
# MAGIC   SELECT
# MAGIC     DISTINCT planning_stop_schedule.relay_reference_number,
# MAGIC     max(planning_stop_schedule.sequence_number) AS last_delivery
# MAGIC   FROM
# MAGIC     planning_stop_schedule
# MAGIC   WHERE
# MAGIC     planning_stop_schedule.`removed?` = false
# MAGIC   GROUP BY
# MAGIC     planning_stop_schedule.relay_reference_number
# MAGIC ),
# MAGIC truckload_proj_del AS (
# MAGIC   SELECT
# MAGIC     DISTINCT truckload_projection.booking_id,
# MAGIC     max(
# MAGIC       truckload_projection.last_update_date_time :: STRING
# MAGIC     ) :: timestamp AS truckload_proj_del
# MAGIC   FROM
# MAGIC     truckload_projection
# MAGIC   WHERE
# MAGIC     lower(
# MAGIC       truckload_projection.last_update_event_name :: STRING
# MAGIC     ) = 'markeddelivered' :: STRING
# MAGIC   GROUP BY
# MAGIC     truckload_projection.booking_id
# MAGIC ),
# MAGIC master_dates as (
# MAGIC   select
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(
# MAGIC       try_cast(c.in_date_time AS TIMESTAMP),
# MAGIC       try_cast(c.out_date_time AS TIMESTAMP),
# MAGIC       try_cast(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       try_cast(pp.appointment_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN b.ready_time IS NULL
# MAGIC         OR b.ready_time = '' THEN try_cast(b.ready_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             try_cast(SUBSTRING(b.ready_time, 1, 2) AS INT) * 3600
# MAGIC           ) + (
# MAGIC             try_cast(SUBSTRING(b.ready_time, 4, 2) AS INT) * 60
# MAGIC           ),
# MAGIC           try_cast(b.ready_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS use_date,
# MAGIC     CASE
# MAGIC       WHEN dayofweek(
# MAGIC         COALESCE(
# MAGIC           try_cast(c.in_date_time AS DATE),
# MAGIC           try_cast(c.out_date_time AS DATE),
# MAGIC           try_cast(pss.appointment_datetime AS DATE),
# MAGIC           try_cast(pss.window_end_datetime AS DATE),
# MAGIC           try_cast(pss.window_start_datetime AS DATE),
# MAGIC           try_cast(pp.appointment_datetime AS DATE),
# MAGIC           b.ready_date
# MAGIC         )
# MAGIC       ) = 1 THEN DATE_ADD(
# MAGIC         COALESCE(
# MAGIC           try_cast(c.in_date_time AS DATE),
# MAGIC           try_cast(c.out_date_time AS DATE),
# MAGIC           try_cast(pss.appointment_datetime AS DATE),
# MAGIC           try_cast(pss.window_end_datetime AS DATE),
# MAGIC           try_cast(pss.window_start_datetime AS DATE),
# MAGIC           try_cast(pp.appointment_datetime AS DATE),
# MAGIC           b.ready_date
# MAGIC         ),
# MAGIC         6
# MAGIC       )
# MAGIC       ELSE DATE_ADD(
# MAGIC         date_trunc(
# MAGIC           'WEEK',
# MAGIC           COALESCE(
# MAGIC             try_cast(c.in_date_time AS TIMESTAMP),
# MAGIC             try_cast(c.out_date_time AS TIMESTAMP),
# MAGIC             try_cast(pss.appointment_datetime AS TIMESTAMP),
# MAGIC             try_cast(pss.window_end_datetime AS TIMESTAMP),
# MAGIC             try_cast(pss.window_start_datetime AS TIMESTAMP),
# MAGIC             try_cast(pp.appointment_datetime AS TIMESTAMP),
# MAGIC             b.ready_date
# MAGIC           )
# MAGIC         ),
# MAGIC         5
# MAGIC       )
# MAGIC     END AS end_date,
# MAGIC     CASE
# MAGIC       WHEN dayofweek(CAST(b.booked_at AS DATE)) = 1 THEN DATE_ADD(CAST(b.booked_at AS DATE), 6)
# MAGIC       ELSE DATE_ADD(
# MAGIC         date_trunc('WEEK', CAST(b.booked_at AS TIMESTAMP)),
# MAGIC         5
# MAGIC       )
# MAGIC     END AS booked_end_date,
# MAGIC     CASE
# MAGIC       WHEN b.ready_time IS NULL
# MAGIC       OR b.ready_time = '' THEN try_cast(b.ready_date AS TIMESTAMP)
# MAGIC       ELSE TIMESTAMPADD(
# MAGIC         SECOND,
# MAGIC         (
# MAGIC           try_cast(SUBSTRING(b.ready_time, 1, 2) AS INT) * 3600
# MAGIC         ) + (
# MAGIC           try_cast(SUBSTRING(b.ready_time, 4, 2) AS INT) * 60
# MAGIC         ),
# MAGIC         try_cast(b.ready_date AS TIMESTAMP)
# MAGIC       )
# MAGIC     END AS ready_date,
# MAGIC     COALESCE(
# MAGIC       try_cast(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       try_cast(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_start_datetime,
# MAGIC     try_cast(pss.window_end_datetime AS TIMESTAMP) AS pu_appt_end_datetime,
# MAGIC     COALESCE(
# MAGIC       try_cast(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       try_cast(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_date,
# MAGIC     pss.schedule_type AS pu_schedule_type,
# MAGIC     pss.scheduled_at as Scheduled_Date,
# MAGIC     try_cast(c.in_date_time AS TIMESTAMP) AS pickup_in,
# MAGIC     try_cast(c.out_date_time AS TIMESTAMP) AS pickup_out,
# MAGIC     COALESCE(
# MAGIC       try_cast(c.in_date_time AS TIMESTAMP),
# MAGIC       try_cast(c.out_date_time AS TIMESTAMP)
# MAGIC     ) AS pickup_datetime,
# MAGIC     COALESCE(
# MAGIC       try_cast(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC       try_cast(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN dp.appointment_time_local IS NULL
# MAGIC         OR dp.appointment_time_local = '' THEN try_cast(dp.appointment_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             try_cast(
# MAGIC               SUBSTRING(dp.appointment_time_local, 1, 2) AS INT
# MAGIC             ) * 3600
# MAGIC           ) + (
# MAGIC             try_cast(
# MAGIC               SUBSTRING(dp.appointment_time_local, 4, 2) AS INT
# MAGIC             ) * 60
# MAGIC           ),
# MAGIC           try_cast(dp.appointment_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS del_appt_start_datetime,
# MAGIC     try_cast(dpss.window_end_datetime AS TIMESTAMP) AS del_appt_end_datetime,
# MAGIC     COALESCE(
# MAGIC       try_cast(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC       try_cast(dpss.window_end_datetime AS TIMESTAMP),
# MAGIC       try_cast(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN dp.appointment_time_local IS NULL
# MAGIC         OR dp.appointment_time_local = '' THEN try_cast(dp.appointment_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             try_cast(
# MAGIC               SUBSTRING(dp.appointment_time_local, 1, 2) AS INT
# MAGIC             ) * 3600
# MAGIC           ) + (
# MAGIC             try_cast(
# MAGIC               SUBSTRING(dp.appointment_time_local, 4, 2) AS INT
# MAGIC             ) * 60
# MAGIC           ),
# MAGIC           try_cast(dp.appointment_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS del_appt_date,
# MAGIC     dpss.schedule_type AS del_schedule_type,
# MAGIC     try_cast(
# MAGIC       hain_tracking_accuracy_projection.delivery_by_date_at_tender AS DATE
# MAGIC     ) AS Delivery_By_Date,
# MAGIC     try_cast(cd.in_date_time AS TIMESTAMP) AS delivery_in,
# MAGIC     try_cast(cd.out_date_time AS TIMESTAMP) AS delivery_out,
# MAGIC     COALESCE(
# MAGIC       try_cast(cd.in_date_time AS TIMESTAMP),
# MAGIC       try_cast(cd.out_date_time AS TIMESTAMP)
# MAGIC     ) AS delivery_datetime,
# MAGIC     t.truckload_proj_del AS truckload_proj_delivered,
# MAGIC     try_cast(b.booked_at AS TIMESTAMP) AS booked_at
# MAGIC   FROM
# MAGIC     booking_projection b
# MAGIC     LEFT JOIN max_schedule ON b.relay_reference_number = max_schedule.relay_reference_number
# MAGIC     LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC     LEFT JOIN pickup_projection pp ON b.booking_id = pp.booking_id
# MAGIC     AND b.first_shipper_name :: string = pp.shipper_name :: string
# MAGIC     AND max_schedule.max_schedule = pp.scheduled_at :: timestamp
# MAGIC     AND pp.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule pss ON b.relay_reference_number = pss.relay_reference_number
# MAGIC     AND b.first_shipper_name :: string = pss.stop_name :: string
# MAGIC     AND pss.sequence_number = 1
# MAGIC     AND pss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop c ON pss.stop_id :: string = c.stop_id :: string
# MAGIC     AND c.`stale?` = false
# MAGIC     AND lower(c.stop_type :: string) = 'pickup' :: string
# MAGIC     LEFT JOIN last_delivery ON b.relay_reference_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number
# MAGIC     AND b.receiver_name :: string = dp.receiver_name :: string
# MAGIC     AND max_schedule_del.max_schedule_del = dp.scheduled_at :: timestamp
# MAGIC     LEFT JOIN planning_stop_schedule dpss ON b.relay_reference_number = dpss.relay_reference_number
# MAGIC     AND b.receiver_name :: string = dpss.stop_name :: string
# MAGIC     AND b.receiver_city :: string = dpss.locality :: string
# MAGIC     AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC     AND lower(dpss.stop_type :: string) = 'delivery' :: string
# MAGIC     AND dpss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop cd ON dpss.stop_id :: string = cd.stop_id :: string
# MAGIC     AND b.receiver_city :: string = cd.locality :: string
# MAGIC     AND cd.`stale?` = false
# MAGIC     AND lower(cd.stop_type :: string) = 'delivery' :: string
# MAGIC     LEFT JOIN hain_tracking_accuracy_projection ON b.relay_reference_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC     LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC     LEFT JOIN truckload_proj_del t ON b.booking_id = t.booking_id
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     AND lower(b.status :: string) <> 'cancelled' :: string
# MAGIC     AND (
# MAGIC       lower(canonical_plan_projection.mode :: string) <> 'ltl' :: string
# MAGIC       OR canonical_plan_projection.mode IS NULL
# MAGIC     )
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     DISTINCT b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC     COALESCE(
# MAGIC       CAST(b.ship_date AS TIMESTAMP),
# MAGIC       CAST(c.in_date_time AS TIMESTAMP),
# MAGIC       CAST(c.out_date_time AS TIMESTAMP),
# MAGIC       CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       CAST(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS use_date,
# MAGIC     CASE
# MAGIC       WHEN dayofweek(
# MAGIC         CAST(
# MAGIC           COALESCE(
# MAGIC             b.ship_date,
# MAGIC             c.in_date_time,
# MAGIC             c.out_date_time,
# MAGIC             pss.appointment_datetime,
# MAGIC             pss.window_end_datetime,
# MAGIC             pss.window_start_datetime,
# MAGIC             pp.appointment_datetime
# MAGIC           ) AS DATE
# MAGIC         )
# MAGIC       ) = 1 THEN DATE_ADD(
# MAGIC         CAST(
# MAGIC           COALESCE(
# MAGIC             b.ship_date,
# MAGIC             c.in_date_time,
# MAGIC             c.out_date_time,
# MAGIC             pss.appointment_datetime,
# MAGIC             pss.window_end_datetime,
# MAGIC             pss.window_start_datetime,
# MAGIC             pp.appointment_datetime
# MAGIC           ) AS DATE
# MAGIC         ),
# MAGIC         6
# MAGIC       )
# MAGIC       ELSE DATE_ADD(
# MAGIC         date_trunc(
# MAGIC           'WEEK',
# MAGIC           CAST(
# MAGIC             COALESCE(
# MAGIC               b.ship_date,
# MAGIC               c.in_date_time,
# MAGIC               c.out_date_time,
# MAGIC               pss.appointment_datetime,
# MAGIC               pss.window_end_datetime,
# MAGIC               pss.window_start_datetime,
# MAGIC               pp.appointment_datetime
# MAGIC             ) AS TIMESTAMP
# MAGIC           )
# MAGIC         ),
# MAGIC         5
# MAGIC       )
# MAGIC     END AS end_date,
# MAGIC     NULL AS booked_end_date,
# MAGIC     NULL AS ready_date,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_start_datetime,
# MAGIC     TRY_CAST(pss.window_end_datetime AS TIMESTAMP) AS pu_appt_end_datetime,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_date,
# MAGIC     pss.schedule_type AS pu_schedule_type,
# MAGIC     pss.scheduled_at as Scheduled_Date,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(b.ship_date AS TIMESTAMP),
# MAGIC       TRY_CAST(c.in_date_time AS TIMESTAMP)
# MAGIC     ) AS pickup_in,
# MAGIC     TRY_CAST(c.out_date_time AS TIMESTAMP) AS pickup_out,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(b.ship_date AS TIMESTAMP),
# MAGIC       TRY_CAST(c.in_date_time AS TIMESTAMP),
# MAGIC       TRY_CAST(c.out_date_time AS TIMESTAMP)
# MAGIC     ) AS pickup_datetime,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN dp.appointment_time_local IS NULL
# MAGIC         OR dp.appointment_time_local = '' THEN TRY_CAST(dp.appointment_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             TRY_CAST(
# MAGIC               SUBSTRING(dp.appointment_time_local, 1, 2) AS INT
# MAGIC             ) * 3600
# MAGIC           ) + (
# MAGIC             TRY_CAST(
# MAGIC               SUBSTRING(dp.appointment_time_local, 4, 2) AS INT
# MAGIC             ) * 60
# MAGIC           ),
# MAGIC           TRY_CAST(dp.appointment_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS del_appt_start_datetime,
# MAGIC     TRY_CAST(dpss.window_end_datetime AS TIMESTAMP) AS del_appt_end_datetime,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(dpss.window_end_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN dp.appointment_time_local IS NULL
# MAGIC         OR dp.appointment_time_local = '' THEN TRY_CAST(dp.appointment_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             TRY_CAST(
# MAGIC               SUBSTRING(dp.appointment_time_local, 1, 2) AS INT
# MAGIC             ) * 3600
# MAGIC           ) + (
# MAGIC             TRY_CAST(
# MAGIC               SUBSTRING(dp.appointment_time_local, 4, 2) AS INT
# MAGIC             ) * 60
# MAGIC           ),
# MAGIC           TRY_CAST(dp.appointment_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS del_appt_date,
# MAGIC     dpss.schedule_type AS del_schedule_type,
# MAGIC     TRY_CAST(
# MAGIC       hain_tracking_accuracy_projection.delivery_by_date_at_tender AS DATE
# MAGIC     ) AS Delivery_By_Date,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(b.delivered_date AS TIMESTAMP),
# MAGIC       TRY_CAST(cd.in_date_time AS TIMESTAMP)
# MAGIC     ) AS delivery_in,
# MAGIC     TRY_CAST(cd.out_date_time AS TIMESTAMP) AS delivery_out,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(b.delivered_date AS TIMESTAMP),
# MAGIC       TRY_CAST(cd.in_date_time AS TIMESTAMP),
# MAGIC       TRY_CAST(cd.out_date_time AS TIMESTAMP)
# MAGIC     ) AS delivery_datetime,
# MAGIC     NULL AS truckload_proj_delivered,
# MAGIC     NULL AS booked_at
# MAGIC   FROM
# MAGIC     big_export_projection b
# MAGIC     LEFT JOIN max_schedule ON b.load_number = max_schedule.relay_reference_number
# MAGIC     LEFT JOIN max_schedule_del ON b.load_number = max_schedule_del.relay_reference_number
# MAGIC     LEFT JOIN pickup_projection pp ON b.load_number = pp.relay_reference_number
# MAGIC     AND b.pickup_name :: string = pp.shipper_name :: string
# MAGIC     AND b.weight = pp.weight_to_pickup_amount
# MAGIC     AND b.piece_count :: double = pp.pieces_to_pickup_count
# MAGIC     AND max_schedule.max_schedule = pp.scheduled_at :: timestamp
# MAGIC     AND pp.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule pss ON b.load_number = pss.relay_reference_number
# MAGIC     AND b.pickup_name :: string = pss.stop_name :: string
# MAGIC     AND pss.sequence_number = 1
# MAGIC     AND pss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop c ON pss.stop_id :: string = c.stop_id :: string
# MAGIC     LEFT JOIN delivery_projection dp ON b.load_number = dp.relay_reference_number
# MAGIC     AND b.consignee_name :: string = dp.receiver_name :: string
# MAGIC     AND max_schedule_del.max_schedule_del = dp.scheduled_at :: timestamp
# MAGIC     LEFT JOIN last_delivery ON b.load_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN planning_stop_schedule dpss ON b.load_number = dpss.relay_reference_number
# MAGIC     AND b.consignee_name :: string = dpss.stop_name :: string
# MAGIC     AND b.consignee_city :: string = dpss.locality :: string
# MAGIC     AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC     AND lower(dpss.stop_type :: string) = 'delivery' :: string
# MAGIC     AND dpss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop cd ON dpss.stop_id :: string = cd.stop_id :: string
# MAGIC     LEFT JOIN hain_tracking_accuracy_projection ON b.load_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC     LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC   WHERE
# MAGIC     lower(b.dispatch_status :: string) <> 'cancelled' :: string
# MAGIC     AND (
# MAGIC       lower(canonical_plan_projection.mode :: string) = 'ltl' :: string
# MAGIC       OR canonical_plan_projection.mode IS NULL
# MAGIC     )
# MAGIC ),
# MAGIC combined_loads_mine AS (
# MAGIC   SELECT
# MAGIC     DISTINCT plan_combination_projection.relay_reference_number_one,
# MAGIC     plan_combination_projection.relay_reference_number_two,
# MAGIC     plan_combination_projection.resulting_plan_id,
# MAGIC     canonical_plan_projection.relay_reference_number AS combine_new_rrn,
# MAGIC     canonical_plan_projection.mode,
# MAGIC     b.tender_on_behalf_of_id,
# MAGIC     'COMBINED' AS combined,
# MAGIC     COALESCE(one_money.one_money, 0.0) AS one_money,
# MAGIC     COALESCE(one_lhc_money.one_lhc_money, 0.0) AS one_lhc_money,
# MAGIC     COALESCE(one_fuel_money.one_fuel_money, 0.0) AS one_fuel_money,
# MAGIC     COALESCE(two_money.two_money, 0.0) AS two_money,
# MAGIC     COALESCE(two_lhc_money.two_lhc_money, 0.0) AS two_lhc_money,
# MAGIC     COALESCE(two_fuel_money.two_fuel_money, 0.0) AS two_fuel_money,
# MAGIC     COALESCE(one_money.one_money, 0.0) + COALESCE(two_money.two_money, 0.0) AS total_load_rev,
# MAGIC     COALESCE(one_lhc_money.one_lhc_money, 0.0) + COALESCE(two_lhc_money.two_lhc_money, 0.0) AS total_load_lhc_rev,
# MAGIC     COALESCE(one_fuel_money.one_fuel_money, 0.0) + COALESCE(two_fuel_money.two_fuel_money, 0.0) AS total_load_fuel_rev,
# MAGIC     COALESCE(carrier_money.carrier_money, 0.0) AS final_carrier_rate,
# MAGIC     COALESCE(carrier_linehaul.carrier_linehaul, 0.0) AS carrier_linehaul,
# MAGIC     COALESCE(fuel_surcharge.fuel_surcharge, 0.0) AS carrier_fuel_surcharge
# MAGIC   FROM
# MAGIC     plan_combination_projection
# MAGIC     JOIN canonical_plan_projection ON plan_combination_projection.resulting_plan_id = canonical_plan_projection.plan_id
# MAGIC     LEFT JOIN booking_projection b ON canonical_plan_projection.relay_reference_number = b.relay_reference_number
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) one_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_fuel_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.charge_code = 'fuel_surcharge'
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) one_fuel_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_lhc_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.charge_code = 'linehaul'
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) one_lhc_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_lhc_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.charge_code = 'linehaul'
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) two_lhc_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_fuel_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.charge_code = 'fuel_surcharge'
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) two_fuel_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) two_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(vendor_transaction_projection.amount) / 100.00 AS carrier_money
# MAGIC       FROM
# MAGIC         vendor_transaction_projection
# MAGIC       WHERE
# MAGIC         canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC         AND vendor_transaction_projection.`voided?` = false
# MAGIC     ) carrier_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(vendor_transaction_projection.amount) / 100.00 AS carrier_linehaul
# MAGIC       FROM
# MAGIC         vendor_transaction_projection
# MAGIC       WHERE
# MAGIC         canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC         AND vendor_transaction_projection.charge_code = 'linehaul'
# MAGIC         AND vendor_transaction_projection.`voided?` = false
# MAGIC     ) carrier_linehaul ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(vendor_transaction_projection.amount) / 100.00 AS fuel_surcharge
# MAGIC       FROM
# MAGIC         vendor_transaction_projection
# MAGIC       WHERE
# MAGIC         canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC         AND vendor_transaction_projection.charge_code = 'fuel_surcharge'
# MAGIC         AND vendor_transaction_projection.`voided?` = false
# MAGIC     ) fuel_surcharge ON true
# MAGIC   WHERE
# MAGIC     plan_combination_projection.is_combined = true
# MAGIC ),
# MAGIC invoicing_cred AS (
# MAGIC   SELECT
# MAGIC     DISTINCT invoicing_credits.relay_reference_number,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN invoicing_credits.currency = 'CAD' THEN invoicing_credits.total_amount * COALESCE(
# MAGIC           CASE
# MAGIC             WHEN canada_conversions.us_to_cad = 0 THEN NULL
# MAGIC             ELSE canada_conversions.us_to_cad
# MAGIC           END,
# MAGIC           average_conversions.avg_cad_to_us
# MAGIC         )
# MAGIC         ELSE invoicing_credits.total_amount
# MAGIC       END
# MAGIC     ) / 100.00 AS invoicing_cred,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN invoicing_credits.currency = 'USD' THEN invoicing_credits.total_amount * COALESCE(
# MAGIC            canada_conversions.conversion,
# MAGIC             average_conversions.avg_us_to_cad
# MAGIC         )
# MAGIC         ELSE invoicing_credits.total_amount
# MAGIC       END
# MAGIC     ) / 100.00 AS invoicing_cred_CAD
# MAGIC   FROM
# MAGIC     invoicing_credits
# MAGIC     LEFT JOIN canada_conversions ON DATE(invoicing_credits.credited_at) = canada_conversions.ship_date
# MAGIC     JOIN average_conversions ON TRUE
# MAGIC   GROUP BY
# MAGIC     invoicing_credits.relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC invoice_start AS (
# MAGIC   SELECT
# MAGIC     DISTINCT relay_reference_number,
# MAGIC     'tl' AS mode,
# MAGIC     invoiced_at :: DATE,
# MAGIC     invoice_number,
# MAGIC     (
# MAGIC       accessorial_amount :: NUMERIC + fuel_surcharge_amount :: NUMERIC + linehaul_amount :: NUMERIC
# MAGIC     ) / 100.00 AS total_invoice_amt
# MAGIC   FROM
# MAGIC     tl_invoice_projection
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     DISTINCT relay_reference_number,
# MAGIC     'ltl',
# MAGIC     invoiced_at :: DATE,
# MAGIC     invoice_number,
# MAGIC     REPLACE(invoiced_amount, ',', '') :: NUMERIC
# MAGIC   FROM
# MAGIC     ltl_invoice_projection
# MAGIC   ORDER BY
# MAGIC     relay_reference_number,
# MAGIC     invoice_number
# MAGIC ),
# MAGIC basic_invoice_info AS (
# MAGIC   SELECT
# MAGIC     DISTINCT relay_reference_number,
# MAGIC     MAX(invoiced_at) AS first_invoiced,
# MAGIC     SUM(total_invoice_amt) AS total_amt_invoiced
# MAGIC   FROM
# MAGIC     invoice_start
# MAGIC   GROUP BY
# MAGIC     relay_reference_number
# MAGIC ),
# MAGIC raw_data_one AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN customer_name = 'TJX Companies c/o Blueyonder' THEN 'IL'
# MAGIC       ELSE COALESCE(
# MAGIC         relay_users.office_id,
# MAGIC         aljex_user_report_listing.pnl_code
# MAGIC       )
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     COALESCE(master_customer_name, customer_name) AS mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at
# MAGIC   FROM
# MAGIC     bronze.spot_quotes
# MAGIC     JOIN analytics.dim_date ON CAST(spot_quotes.created_at AS DATE) = dim_date.Calendar_Date
# MAGIC     LEFT JOIN bronze.relay_users ON spot_quotes.created_by = relay_users.full_name
# MAGIC     LEFT JOIN bronze.aljex_user_report_listing ON spot_quotes.created_by = aljex_user_report_listing.full_name
# MAGIC     LEFT JOIN bronze.customer_lookup ON LEFT(customer_name, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC raw_data AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC       WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC       ELSE rep_office
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at,
# MAGIC     rank() OVER (
# MAGIC       PARTITION BY Load_Number,
# MAGIC       status_str
# MAGIC       ORDER BY
# MAGIC         last_updated_at DESC
# MAGIC     ) AS rank
# MAGIC   FROM
# MAGIC     raw_data_one
# MAGIC     LEFT JOIN bronze.days_to_pay_offices ON CASE
# MAGIC       WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC       WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC       ELSE rep_office
# MAGIC     END = days_to_pay_offices.office
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC -- Select * from raw_data
# MAGIC relay_spot_number AS (
# MAGIC   SELECT
# MAGIC     DISTINCT b.relay_reference_number,
# MAGIC     t.shipment_id,
# MAGIC     r.customer_name,
# MAGIC     master_customer_name,
# MAGIC     ncm.Customer_Final_Rate_USD AS revenue,
# MAGIC     NULL AS column_null,
# MAGIC     rd.created_at,
# MAGIC     rd.quote_id,
# MAGIC     1 AS counter,
# MAGIC     rd.quoted_price,
# MAGIC     rd.margin,
# MAGIC     rd.created_by,
# MAGIC     rd.rep_office,
# MAGIC     rd.load_number,
# MAGIC     rd.shipper_ref_number,
# MAGIC     rd.status_str,
# MAGIC     rd.last_updated_at
# MAGIC   FROM
# MAGIC     bronze.booking_projection AS b
# MAGIC     LEFT JOIN bronze.customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug
# MAGIC     LEFT JOIN bronze.customer_lookup AS cl ON LEFT(r.customer_name, 32) = LEFT(cl.aljex_customer_name, 32)
# MAGIC     LEFT JOIN bronze.tendering_acceptance AS t ON b.relay_reference_number = t.relay_reference_number
# MAGIC     JOIN raw_data AS rd ON CAST(b.relay_reference_number AS STRING) = rd.load_number
# MAGIC     AND master_customer_name = rd.mastername
# MAGIC     AND b.first_shipper_state = rd.shipper_state
# MAGIC     AND b.receiver_state = rd.receiver_state
# MAGIC     LEFT JOIN Silver.Silver_Relay_Customer_Money AS ncm ON b.relay_reference_number = ncm.relay_reference_number
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     and rd.status_str = 'WON'
# MAGIC     and rd.rank = 1
# MAGIC ),
# MAGIC
# MAGIC pricing_nfi_load_predictions as (
# MAGIC   select
# MAGIC     external_load_id as MR_id,
# MAGIC     max(target_buy_rate_gsn) * max(miles_predicted) as Market_Buy_Rate,
# MAGIC     1 as Markey_buy_rate_flag
# MAGIC   from
# MAGIC     bronze.pricing_nfi_load_predictions
# MAGIC   group by
# MAGIC     external_load_id
# MAGIC ),
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
# MAGIC
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
# MAGIC cell1 as (
# MAGIC 	  select
# MAGIC 		b.relay_reference_number,
# MAGIC 		tn.is_split,
# MAGIC 		tn.order_numbers,
# MAGIC 		tn.updated_at
# MAGIC 	  from
# MAGIC 		booking_projection b
# MAGIC 		  left join bronze.tender_reference_numbers_projection tn
# MAGIC 			on b.relay_reference_number = tn.relay_reference_number
# MAGIC 			AND tn.is_split = 'false'
# MAGIC       AND b.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R40') 
# MAGIC 	),
# MAGIC 	rank as (
# MAGIC 	  select
# MAGIC 		cell1.*,
# MAGIC 		row_number() over (
# MAGIC 			partition by cell1.relay_reference_number
# MAGIC 			order by cell1.updated_at desc
# MAGIC 		  ) as rank
# MAGIC 	  from
# MAGIC 		cell1
# MAGIC 	),
# MAGIC 	tender_reference_numbers_projection as (
# MAGIC 	  select
# MAGIC 		*
# MAGIC 	  from
# MAGIC 		rank
# MAGIC 	  where
# MAGIC 		rank = 1
# MAGIC 	),
# MAGIC
# MAGIC Final as (SELECT DISTINCT
# MAGIC     CONCAT("R", b.relay_reference_number) AS DW_Load_ID,
# MAGIC     
# MAGIC    'RELAY' AS TMS_Sourcesystem,
# MAGIC    b.relay_reference_number :: INT AS Load_Number,
# MAGIC   customer_profile_projection.customer_name AS Customer_Name,
# MAGIC   case
# MAGIC     when customer_lookup.master_customer_name is null then customer_profile_projection.customer_name
# MAGIC     else customer_lookup.master_customer_name
# MAGIC   end as Customer_Master,
# MAGIC   CASE WHEN new_office_lookup.new_office = 'TGT' THEN 'PHI' ELSE new_office_lookup.new_office end as Customer_New_Office,
# MAGIC   new_office_lookup.old_office as Customer_Old_Office,
# MAGIC   z.full_name as Customer_Rep,
# MAGIC   'TL' as Mode,
# MAGIC   case
# MAGIC     when canonical_plan_projection.mode = 'prtl' then 'Drayage'
# MAGIC     else case
# MAGIC       when canonical_plan_projection.mode = 'imdl' then 'Intermodal'
# MAGIC       else 'Dry Van'
# MAGIC     end
# MAGIC   end as Equipment,
# MAGIC   CASE
# MAGIC     WHEN combined_loads_mine.combine_new_rrn is null THEN "NO"
# MAGIC     WHEN combined_loads_mine.combine_new_rrn is not null then combined_loads_mine.combine_new_rrn :: STRING
# MAGIC     ELSE NULL
# MAGIC   END as Combined_Load,
# MAGIC   carr.old_office AS Carrier_Old_Office,
# MAGIC   CASE WHEN carr.new_office = 'TGT' THEN 'PHI' ELSE carr.new_office END AS Carrier_New_Office,
# MAGIC   b.booked_by_name as Carrier_Rep,
# MAGIC   sales.full_name as Sales_Rep,
# MAGIC   upper(b.status) as Load_Status,
# MAGIC   initcap(b.first_shipper_city) as Origin_City,
# MAGIC   upper(b.first_shipper_state) as Origin_State,
# MAGIC   b.first_shipper_zip as Origin_Zip,
# MAGIC   initcap(b.receiver_city) as Dest_City,
# MAGIC   upper(b.receiver_state) as Dest_State,
# MAGIC   b.receiver_zip as Dest_Zip,
# MAGIC   o.market as Market_Origin,
# MAGIC   d.market AS Market_Dest,
# MAGIC   CONCAT(o.market, '>', d.market) AS Market_Lane,
# MAGIC   master_dates.delivery_datetime :: DATE AS Delivery_Date,
# MAGIC   rolled_at :: date as Rolled_Date,
# MAGIC   ta.accepted_at :: string as Tendering_Date,
# MAGIC   master_dates.booked_at::date as Booked_Date,
# MAGIC   b.bounced_at :: date as Bounced_Date,
# MAGIC   b.bounced_at :: date + 2 as Cancelled_Date,
# MAGIC   master_dates.pu_appt_date::DATE as Pickup_Appointment_Date,
# MAGIC   master_dates.scheduled_date::DATE as Scheduled_Date,
# MAGIC   master_dates.pickup_in::DATE as Pickup_Start,
# MAGIC   master_dates.pickup_out::DATE as Pickup_End,
# MAGIC   master_dates.pickup_datetime::DATE AS Pickup_Datetime,
# MAGIC   case
# MAGIC     when master_dates.pickup_datetime :: date > COALESCE(
# MAGIC       master_dates.pu_appt_date :: date,
# MAGIC       master_dates.use_date :: date
# MAGIC     ) then 'Late'
# MAGIC     else 'OnTime'
# MAGIC   end as On_Time_Pickup,
# MAGIC   master_dates.delivery_in::DATE as Delivery_Start,
# MAGIC   master_dates.delivery_out::DATE as Delivery_End,
# MAGIC   master_dates.delivery_datetime::DATE as Delivery_Datetime,
# MAGIC   case
# MAGIC     when master_dates.delivery_datetime :: date > master_dates.del_appt_date :: date then 'late'
# MAGIC     else 'OnTime'
# MAGIC   end as On_Time_Delivery,
# MAGIC   CASE
# MAGIC     WHEN cust.new_office <> carr.new_office THEN 'Crossbooked'
# MAGIC     ELSE 'Same Office'
# MAGIC   END AS Booking_Type,
# MAGIC   carrier_projection.dot_number :: string as DOT_Number,
# MAGIC   
# MAGIC   b.booked_carrier_name as Carrier_Name,
# MAGIC   coalesce(
# MAGIC     tendering_planned_distance.planned_distance_amount :: float,
# MAGIC     b.total_miles :: float
# MAGIC   ) as Miles,
# MAGIC   Custm.Customer_Final_Rate_USD as Revenue_USD,
# MAGIC   Custm.Customer_Final_Rate_CAD as Revenue_CAD, 
# MAGIC   CarM.Carrier_Final_Rate_USD  :: float as Expense_USD,
# MAGIC   CarM.Carrier_Final_Rate_CAD::float as Expense_CAD,
# MAGIC   CASE
# MAGIC     WHEN combine_new_rrn is null THEN (
# MAGIC       
# MAGIC         coalesce(Custm.Customer_Final_Rate_USD, 0) - (coalesce(CarM.Carrier_Final_Rate_USD, 0)) :: float
# MAGIC     )
# MAGIC     ELSE (
# MAGIC       coalesce(Custm.Customer_Final_Rate_USD, 0) - (coalesce(
# MAGIC         CarM.Carrier_Final_Rate_USD:: float / 2,0))
# MAGIC     ) :: float
# MAGIC   END as Margin_USD,
# MAGIC   CASE
# MAGIC     WHEN combine_new_rrn is null THEN (
# MAGIC       coalesce(Custm.Customer_Final_Rate_CAD, 0) - (coalesce(CarM.Carrier_Final_Rate_CAD, 0)) :: float
# MAGIC     )
# MAGIC     ELSE (
# MAGIC       coalesce(Custm.Customer_Final_Rate_CAD, 0) - (coalesce(
# MAGIC         CarM.Carrier_Final_Rate_CAD:: float / 2,0))
# MAGIC     ) :: floaT
# MAGIC   END as Margin_CAD,
# MAGIC   CarM.Carrier_Linehaul_USD as Carrier_Linehaul_USD,
# MAGIC   CarM.Carrier_Linehaul_CAD as Carrier_Linehaul_CAD,
# MAGIC   CarM.Carrier_Final_Rate_USD as Carrier_Fuel_Surcharge_USD,
# MAGIC   CarM.Carrier_Final_Rate_CAD as Carrier_Fuel_Surcharge_CAD,
# MAGIC   CarM.Carrier_Accessorial_USD as Carrier_Other_Fees_USD,
# MAGIC   CarM.Carrier_Accessorial_CAD as Carrier_Other_Fees_CAD,
# MAGIC     Custm.Customer_Linehaul_USD,
# MAGIC     Custm.Customer_Linehaul_CAD,
# MAGIC     Custm.Customer_FuelSurcharge_USD,
# MAGIC     Custm.Customer_FuelSurcharge_CAD,
# MAGIC     Custm.Customer_Accessorial_USD as Customer_Other_Fees_USD,
# MAGIC     Custm.Customer_Accessorial_CAD as Customer_Other_Fees_CAD,
# MAGIC   t.Trailer_Number AS Trailer_Number,
# MAGIC   tn.order_numbers as PO_Number,
# MAGIC   b.receiver_name as Consignee_Name,
# MAGIC   CONCAT(
# MAGIC     re.address1,
# MAGIC     ' ,',
# MAGIC     re.city,
# MAGIC     ' ,',
# MAGIC     re.state_code,
# MAGIC     ' ,',
# MAGIC     re.zip_code
# MAGIC   ) AS Consignee_Delivery_Address,
# MAGIC   CONCAT(
# MAGIC     'Pickuplocation1: ',
# MAGIC     s1.address1,
# MAGIC     ' ,',
# MAGIC     s1.city,
# MAGIC     ' ,',
# MAGIC     s1.state_code,
# MAGIC     ' ,',
# MAGIC     s1.zip_code,
# MAGIC     '\n' 'Pickuplocation2: ',
# MAGIC     s2.address1,
# MAGIC     ' ,',
# MAGIC     s2.city,
# MAGIC     ' ,',
# MAGIC     s2.state_code,
# MAGIC     ' ,',
# MAGIC     s2.zip_code
# MAGIC   ) AS Consignee_Pickup_Address,
# MAGIC   carrier_projection.MC_Number AS MC_Number,
# MAGIC   sp.quoted_price as Spot_Revenue,
# MAGIC   sp.margin as Spot_Margin,
# MAGIC   mx.max_buy :: float / 100.00 as Max_buy_rate,
# MAGIC   mr.Market_Buy_Rate AS Market_Buy_Rate,
# MAGIC   customer_profile_projection.Lawson_ID,
# MAGIC   pickup_datetime :: DATE AS Ship_Date,
# MAGIC   basic_invoice_info.first_invoiced :: DATE AS Invoice_Date,
# MAGIC --   financial_calendar.financial_period_sorting,
# MAGIC   COALESCE(delivery_datetime :: DATE, pickup_datetime :: DATE) AS Use_Dates,
# MAGIC   Custm.Customer_Final_Rate_USD AS Invoice_Amount_USD,
# MAGIC   Custm.Load_Currency AS Load_Currency,
# MAGIC   Custm.Customer_Final_Rate_CAD AS Invoice_Amount_CAD,
# MAGIC   DATEDIFF(CURRENT_DATE, use_dates) AS Days_Aged_To_Invoice,
# MAGIC   CASE WHEN  basic_invoice_info.first_invoiced IS NULL THEN 
# MAGIC    (CASE 
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) <= 29 THEN '0 - 30 Days'
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 30 AND 59 THEN '30 - 60 Days' 
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 60 AND 89 THEN '60 - 90 Days' 
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 90 AND 119 THEN '90 - 120 Days' 
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) >= 120 THEN '120 + Days' 
# MAGIC    END ) 
# MAGIC    ELSE NULL END AS Unbilled_Aging_Bucket,
# MAGIC    CASE WHEN basic_invoice_info.first_invoiced IS NULL THEN "Unbilled" ELSE "Billed" END AS Billing_Status,
# MAGIC    Dim_date.Period_Year AS Financial_Period_Year,
# MAGIC    sac.AdminFees_Carrier_USD AS Admin_Fees_Carrier_USD,
# MAGIC    sac.AdminFees_Carrier_CAD as Admin_Fees_Carrier_CAD,
# MAGIC    sacc.AdminFees_Cust_USD as Admin_Fees_Customer_USD,
# MAGIC    sacc.AdminFees_Cust_CAD as Admin_Fees_Customer_CAD,
# MAGIC    b.booked_carrier_id as Carrier_ID,
# MAGIC    relay_users.user_ID AS Carrier_Rep_ID,
# MAGIC    b.tender_on_behalf_of_id AS Customer_ID,
# MAGIC    z.user_id as Customer_Rep_ID,
# MAGIC    datediff(try_cast(Ship_Date as date), try_cast(ta.accepted_at as date)) as Days_ahead,
# MAGIC    sac.DelandPickup_Carrier_CAD,
# MAGIC    sac.DelandPickup_Carrier_USD,
# MAGIC    sacc.DelandPickup_Cust_CAD,
# MAGIC    sacc.DelandPickup_Cust_USD,
# MAGIC    master_dates.del_appt_date as Delivery_Appointment_Date,
# MAGIC    sac.EquipmentandVehicle_Carrier_CAD,
# MAGIC sac.EquipmentandVehicle_Carrier_USD,
# MAGIC sacc.EquipmentandVehicle_Cust_CAD,
# MAGIC sacc.EquipmentandVehicle_Cust_USD,
# MAGIC mr.Market_Buy_Rate * COALESCE(canada_conversions.conversion, avg_us_to_cad) AS Market_Buy_Rate_CAD, 
# MAGIC case
# MAGIC       when carrier_NAME is not null then '1'
# MAGIC       else '0'
# MAGIC     end as Load_flag,
# MAGIC CONCAT(
# MAGIC       INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC     ) AS load_lane,
# MAGIC sac.Loadandunload_carrier_cad,
# MAGIC sac.Loadandunload_carrier_USD,
# MAGIC sacc.Loadandunload_Cust_CAD,
# MAGIC sacc.Loadandunload_Cust_USD,
# MAGIC Max_buy_rate * COALESCE(canada_conversions.conversion, avg_us_to_cad) AS Max_Buy_Rate_CAD,
# MAGIC sac.Miscellaneous_Carrier_CAD,
# MAGIC sac.Miscellaneous_Carrier_USD,
# MAGIC sacc.Miscellaneous_Cust_CAD,
# MAGIC sacc.Miscellaneous_Cust_USD,
# MAGIC sac.PermitsandCompliance_Carrier_CAD,
# MAGIC sac.PermitsandCompliance_Carrier_USD,
# MAGIC sacc.PermitsandCompliance_cust_CAD,
# MAGIC sacc.PermitsandCompliance_cust_USD,
# MAGIC CASE
# MAGIC       WHEN TRY_CAST(Booked_date AS DATE) < TRY_CAST(ship_date AS DATE) THEN 'PreBooked'
# MAGIC       ELSE 'SameDay'
# MAGIC     END AS PreBookStatus,
# MAGIC sales.user_id as Sales_Rep_ID,
# MAGIC tender_final.Tender_Source_Type,
# MAGIC sac.TransitandRouting_Carrier_CAD,
# MAGIC sac.TransitandRouting_Carrier_USD,
# MAGIC sacc.TransitandRouting_Cust_CAD,
# MAGIC sacc.TransitandRouting_Cust_USD 
# MAGIC FROM
# MAGIC   booking_projection b
# MAGIC   LEFT JOIN master_dates ON b.relay_reference_number = master_dates.relay_reference_number
# MAGIC   AND b.status = master_dates.status
# MAGIC   LEFT JOIN Silver.Silver_Relay_Customer_Money Custm ON b.relay_reference_number = Custm.relay_reference_number
# MAGIC   AND b.status = Custm.status
# MAGIC   LEFT JOIN Silver.Silver_Relay_Carrier_Money CarM 
# MAGIC   ON b.relay_reference_number = CarM.relay_reference_number
# MAGIC   AND b.status = CarM.status 
# MAGIC   and b.booking_id = CarM.booking_id
# MAGIC   LEFT JOIN basic_invoice_info ON b.relay_reference_number = basic_invoice_info.relay_reference_number
# MAGIC   LEFT JOIN financial_calendar ON COALESCE(delivery_datetime :: DATE, pickup_datetime :: DATE) :: DATE = financial_calendar.date :: DATE
# MAGIC   LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC   LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC   LEFT JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC   LEFT JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.combine_new_rrn
# MAGIC   LEFT JOIN relay_users z on customer_profile_projection.primary_relay_user_id = z.user_id
# MAGIC   and z.`active?` = 'true' -- Customer Perspective
# MAGIC   LEFT JOIN new_office_lookup_w_tgt cust on customer_profile_projection.profit_center = cust.old_office -- Customer Perspective
# MAGIC   LEFT JOIN relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC   AND relay_users.`active?` = 'true' -- Carrier Perspective
# MAGIC   LEFT JOIN bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office -- Carrier Perspective
# MAGIC   LEFT JOIN bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number -- Mode
# MAGIC   LEFT JOIN relay_users sales on customer_profile_projection.sales_relay_user_id = sales.user_id
# MAGIC   and sales.`active?` = 'true' --Sales Perspective
# MAGIC   LEFT JOIN bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip -- Market Origin
# MAGIC   LEFT JOIN bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip -- Market Destination
# MAGIC   LEFT JOIN bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number -- Tender Details
# MAGIC   LEFT JOIN bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id -- Carrier Details
# MAGIC   LEFT JOIN bronze.tendering_planned_distance on b.relay_reference_number = tendering_planned_distance.relay_reference_number -- Miles
# MAGIC   LEFT JOIN truckload_projection t on b.booking_id = t.booking_id
# MAGIC   and t.status != 'CancelledOrBounced'
# MAGIC   and t.trailer_number is not null
# MAGIC   and lower(b.status) != 'bounced' -- Trailer_Number
# MAGIC   LEFT JOIN tender_reference_numbers_projection tn on b.relay_reference_number = tn.relay_reference_number --PO Number
# MAGIC   LEFT JOIN receivers re on b.receiver_id = re.uuid -- Consignee Address
# MAGIC   LEFT JOIN bronze.shippers s1 on b.first_shipper_id = s1.uuid -- Shipper Addres
# MAGIC   LEFT JOIN bronze.shippers s2 on b.second_shipper_id = s2.uuid
# MAGIC   LEFT JOIN relay_spot_number sp on b.relay_reference_number = sp.relay_reference_number -- Spot Details
# MAGIC   LEFT JOIN bronze.sourcing_max_buy_v2 mx on b.relay_reference_number = mx.relay_reference_number
# MAGIC   LEFT JOIN pricing_nfi_load_predictions mr on b.relay_reference_number = mr.MR_id
# MAGIC   LEFT JOIN analytics.dim_date ON  pickup_datetime :: DATE = dim_date.Calendar_Date::DATE
# MAGIC   LEFT JOIN superinsight.accessorial_carriers sac on b.relay_reference_number = sac.load_number and sac.tms = 'RELAY'
# MAGIC   LEFT JOIN superinsight.accessorial_customers sacc on b.relay_reference_number = sacc.load_number  and sacc.tms = 'RELAY'
# MAGIC   LEFT JOIN canada_conversions ON pickup_datetime::DATE = canada_conversions.ship_date::DATE
# MAGIC   LEFT JOIN average_conversions ON 1=1
# MAGIC   left join tender_final on b.relay_reference_number = tender_final.id
# MAGIC   WHERE
# MAGIC   b.status NOT IN ('cancelled', "bounced")
# MAGIC   AND b.Is_Deleted = 0
# MAGIC   AND b.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R40') 
# MAGIC   AND canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC         CONCAT("R", big_export_projection.load_number::INT),
# MAGIC         'RELAY',
# MAGIC         big_export_projection.load_number::INT,
# MAGIC         CASE 
# MAGIC             WHEN big_export_projection.customer_id = 'hain' THEN 'Hain Celestial' 
# MAGIC             WHEN big_export_projection.customer_id = 'roland' THEN 'Roland Corporation' 
# MAGIC             ELSE customer_profile_projection.customer_name 
# MAGIC         END,
# MAGIC        case
# MAGIC           WHEN big_export_projection.customer_id = 'hain' THEN 'Hain Celestial' 
# MAGIC           WHEN big_export_projection.customer_id = 'roland' THEN 'Roland Corporation'
# MAGIC           when customer_lookup.master_customer_name is null then customer_profile_projection.customer_name   
# MAGIC           else customer_lookup.master_customer_name
# MAGIC         end as mastername,
# MAGIC         CASE 
# MAGIC             WHEN big_export_projection.customer_id = 'hain' THEN 'PHI'
# MAGIC             WHEN big_export_projection.customer_id = 'roland' THEN 'LTL'
# MAGIC             WHEN big_export_projection.customer_id = 'deb' THEN 'PHI'
# MAGIC             WHEN new_office_lookup.new_office  = 'TGT' THEN 'PHI'
# MAGIC             ELSE new_office_lookup.new_office
# MAGIC         END AS Customer_New_Office,
# MAGIC         CASE 
# MAGIC             WHEN big_export_projection.customer_id = 'hain' THEN 'PHI'
# MAGIC             WHEN big_export_projection.customer_id = 'roland' THEN 'LTL'
# MAGIC             WHEN big_export_projection.customer_id = 'deb' THEN 'PHI' 
# MAGIC             ELSE new_office_lookup.old_office END AS Customer_Old_Office,
# MAGIC         z.full_name as customer_rep,
# MAGIC         'LTL' as mode,
# MAGIC          case
# MAGIC            when canonical_plan_projection.mode = 'prtl' then 'Drayage'
# MAGIC            else
# MAGIC                 case
# MAGIC                 when canonical_plan_projection.mode = 'imdl' then 'Intermodal'
# MAGIC                 else 'Dry Van'
# MAGIC                 end
# MAGIC             end as Equipment,
# MAGIC         'NO' as combined_load,
# MAGIC         "LTL" as Carrier_Old_Office,
# MAGIC         "LTL" AS Carrier_New_Office,
# MAGIC         'ltl_team' as Carrier_Rep,
# MAGIC         sales.full_name as salesrep,
# MAGIC         UPPER(big_export_projection.dispatch_status) AS Load_Status,
# MAGIC         initcap(big_export_projection.pickup_city) as origin_city,
# MAGIC         upper(big_export_projection.pickup_state) as origin_state,
# MAGIC         big_export_projection.pickup_zip as Pickup_Zip,
# MAGIC         initcap(big_export_projection.consignee_city) as dest_city,
# MAGIC         upper(big_export_projection.consignee_state) as dest_state,
# MAGIC         big_export_projection.consignee_zip as Consignee_Zip,
# MAGIC         o.market as market_origin,
# MAGIC         d.market AS market_dest,
# MAGIC         CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC         master_dates.delivery_datetime :: DATE AS Delivery_Date,
# MAGIC         NULL AS rolled_at,
# MAGIC         NULL AS Tendering_Date,
# MAGIC         NULL AS Booked_Date,
# MAGIC         NULL AS Bounced_Date,
# MAGIC         NULL AS Cancelled_Date,
# MAGIC         master_dates.pu_appt_date::DATE as Pickup_Appointment_Date,
# MAGIC         master_dates.scheduled_date ::DATE as Scheduled_Date,
# MAGIC         master_dates.pickup_in::DATE as Pickup_Start,
# MAGIC         master_dates.pickup_out::DATE as Pickup_End,
# MAGIC         master_dates.pickup_datetime::DATE AS Pickup_Datetime,
# MAGIC         case
# MAGIC             when master_dates.pickup_datetime :: date > COALESCE(
# MAGIC               master_dates.pu_appt_date :: date,
# MAGIC               master_dates.use_date :: date
# MAGIC             ) then 'Late'
# MAGIC             else 'OnTime'
# MAGIC           end as On_Time_Pickup,
# MAGIC           master_dates.delivery_in::DATE as Delivery_Start,
# MAGIC           master_dates.delivery_out::DATE as Delivery_End,
# MAGIC           master_dates.delivery_datetime::DATE as Delivery_Datetime,
# MAGIC           case
# MAGIC             when master_dates.delivery_datetime :: date > master_dates.del_appt_date :: date then 'Late'
# MAGIC             else 'OnTime'
# MAGIC           end as On_Time_Delivery,
# MAGIC           CASE
# MAGIC             WHEN new_office_lookup.new_office <> 'LTL' THEN 'Crossbooked'
# MAGIC             ELSE 'Same Office'
# MAGIC           END AS Booking_type,
# MAGIC           projection_carrier.dot_num::string AS DOT_NUM,
# MAGIC           big_export_projection.carrier_name AS Carrier_name,
# MAGIC           big_export_projection.miles::float AS Miles,
# MAGIC           CustM.Customer_Final_Rate_USD as Revenue_USD,
# MAGIC           CustM.Customer_Final_Rate_CAD as Revenue_CAD,
# MAGIC           (projected_expense::float / 100.0) as Expense_USD,
# MAGIC           (projected_expense::float / 100.0) * COALESCE(canada_conversions.conversion, avg_us_to_cad) as Expense_CAD,
# MAGIC         
# MAGIC               (CustM.Customer_Final_Rate_USD::float
# MAGIC               - coalesce(projected_expense::float / 100.0, 0)::float
# MAGIC             ) as Margin_USD,
# MAGIC           (
# MAGIC               (CustM.Customer_Final_Rate_CAD)::float
# MAGIC               - Expense_CAD::float
# MAGIC             ) AS Margin_CAD,
# MAGIC           CarM.Carrier_Linehaul_USD as Carrier_Linehaul_USD,
# MAGIC           CarM.Carrier_Linehaul_CAD as Carrier_Linehaul_CAD,
# MAGIC           CarM.Carrier_FuelSurcharge_USD as Carrier_Fuel_Surcharge_USD,
# MAGIC           CarM.Carrier_FuelSurcharge_CAD as Carrier_Fuel_Surcharge_CAD,
# MAGIC           CarM.Carrier_Accessorial_USD as Carrier_Other_Fees_USD,
# MAGIC           CarM.Carrier_Accessorial_CAD as Carrier_Other_Fees_CAD,
# MAGIC           CustM.Customer_Linehaul_USD AS Customer_Linehaul_USD,
# MAGIC           CustM.Customer_Linehaul_CAD AS Customer_Linehaul_CAD,
# MAGIC           CustM.Customer_FuelSurcharge_USD AS Customer_Fuel_Surcharge_USD,
# MAGIC           CustM.Customer_FuelSurcharge_CAD AS Customer_Fuel_Surcharge_CAD,
# MAGIC           CustM.Customer_Accessorial_USD AS Customer_Other_Fees_USD,
# MAGIC           CustM.Customer_Accessorial_CAD AS Customer_Other_Fees_CAD,
# MAGIC           t.Trailer_Number AS Trailer_Number,
# MAGIC           tn.order_numbers as PO_Number,
# MAGIC           big_export_projection.Consignee_Name,
# MAGIC           CONCAT(
# MAGIC               dpss.address_1,
# MAGIC               CASE
# MAGIC                 WHEN
# MAGIC                   dpss.address_2 IS NOT NULL
# MAGIC                   AND dpss.address_2 <> ''
# MAGIC                 THEN
# MAGIC                   CONCAT(' ,', dpss.address_2)
# MAGIC                 ELSE ''
# MAGIC               END,
# MAGIC               ' ,',
# MAGIC               dpss.locality,
# MAGIC               ' ,',
# MAGIC               dpss.administrative_region,
# MAGIC               ' ,',
# MAGIC               dpss.postal_code
# MAGIC             ) AS Delivery_Consignee_Address,
# MAGIC             CONCAT(
# MAGIC               pss.address_1,
# MAGIC               ' ,',
# MAGIC               pss.address_2,
# MAGIC               ' ,',
# MAGIC               pss.locality,
# MAGIC               ' ,',
# MAGIC               pss.administrative_region,
# MAGIC               ' ,',
# MAGIC               pss.postal_code
# MAGIC             ) AS pickup_Consignee_Address,
# MAGIC         projection_carrier.mc_num as Mc_Number,
# MAGIC         sp.quoted_price as Spot_Revenue,
# MAGIC         sp.margin as Spot_Margin,
# MAGIC         mx.max_buy::float / 100.00 as Max_buy_rate,
# MAGIC         mr.Market_Buy_Rate,
# MAGIC         CASE 
# MAGIC             WHEN big_export_projection.customer_id = 'hain' THEN '880177713'
# MAGIC             WHEN big_export_projection.customer_id = 'roland' THEN '880181037'
# MAGIC             WHEN big_export_projection.customer_id = 'deb' THEN '880171761' 
# MAGIC             ELSE customer_profile_projection.Lawson_ID END AS Lawson_ID,
# MAGIC         big_export_projection.ship_date::DATE,
# MAGIC         basic_invoice_info.first_invoiced::DATE,
# MAGIC         -- financial_calendar.financial_period_sorting,
# MAGIC         -- delivery_datetime::DATE,
# MAGIC         COALESCE(delivery_datetime::DATE, big_export_projection.ship_date::DATE) AS use_dates,
# MAGIC         CustM.Customer_Final_Rate_USD AS Unbilled_Amount,
# MAGIC         CustM.Load_currency AS Load_Currency,
# MAGIC         CustM.Customer_Final_Rate_CAD AS CAD_Amount,
# MAGIC           DATEDIFF(CURRENT_DATE, use_dates) AS Days_Aged,
# MAGIC           CASE WHEN  basic_invoice_info.first_invoiced IS NULL THEN 
# MAGIC           (CASE 
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) <= 29 THEN '0 - 30 Days'
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 30 AND 59 THEN '30 - 60 Days' 
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 60 AND 89 THEN '60 - 90 Days' 
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 90 AND 119 THEN '90 - 120 Days' 
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) >= 120 THEN '120 + Days' 
# MAGIC           END ) 
# MAGIC           ELSE NULL END AS Unbilled_Aging_Bucket,
# MAGIC           CASE WHEN basic_invoice_info.first_invoiced::DATE IS NULL THEN "Unbilled" ELSE "Billed" END AS Billing_Status,
# MAGIC           dim_date.period_year as Financial_Period_Year,
# MAGIC           sac.AdminFees_Carrier_USD AS Admin_Fees_Carrier_USD,
# MAGIC    sac.AdminFees_Carrier_CAD as Admin_Fees_Carrier_CAD,
# MAGIC    sacc.AdminFees_Cust_USD as Admin_Fees_Customer_USD,
# MAGIC    sacc.AdminFees_Cust_CAD as Admin_Fees_Customer_CAD,
# MAGIC    big_export_projection.carrier_id as Carrier_ID,
# MAGIC    NULL AS Carrier_Rep_ID,
# MAGIC    big_export_projection.Customer_ID AS Customer_ID,
# MAGIC    z.user_id as Customer_Rep_ID,
# MAGIC    datediff(try_cast(master_dates.use_date as date), try_cast(Tendering_Date as date)) as Days_ahead,
# MAGIC    sac.DelandPickup_Carrier_CAD,
# MAGIC    sac.DelandPickup_Carrier_USD,
# MAGIC    sacc.DelandPickup_Cust_CAD,
# MAGIC    sacc.DelandPickup_Cust_USD,
# MAGIC    master_dates.del_appt_date as Delivery_Appointment_Date,
# MAGIC    sac.EquipmentandVehicle_Carrier_CAD,
# MAGIC     sac.EquipmentandVehicle_Carrier_USD,
# MAGIC     sacc.EquipmentandVehicle_Cust_CAD,
# MAGIC     sacc.EquipmentandVehicle_Cust_USD,
# MAGIC     mr.Market_Buy_Rate * COALESCE(canada_conversions.conversion, avg_us_to_cad) AS Market_Buy_Rate_CAD, 
# MAGIC     case
# MAGIC         when carrier_NAME is not null then '1'
# MAGIC         else '0'
# MAGIC         end as Load_flag,
# MAGIC     CONCAT(
# MAGIC         INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC         ) AS load_lane,
# MAGIC     sac.Loadandunload_carrier_cad,
# MAGIC     sac.Loadandunload_carrier_USD,
# MAGIC     sacc.Loadandunload_Cust_CAD,
# MAGIC     sacc.Loadandunload_Cust_USD,
# MAGIC     Max_buy_rate * COALESCE(canada_conversions.conversion, avg_us_to_cad) AS Max_Buy_Rate_CAD,
# MAGIC     sac.Miscellaneous_Carrier_CAD,
# MAGIC     sac.Miscellaneous_Carrier_USD,
# MAGIC     sacc.Miscellaneous_Cust_CAD,
# MAGIC     sacc.Miscellaneous_Cust_USD,
# MAGIC     sac.PermitsandCompliance_Carrier_CAD,
# MAGIC     sac.PermitsandCompliance_Carrier_USD,
# MAGIC     sacc.PermitsandCompliance_cust_CAD,
# MAGIC     sacc.PermitsandCompliance_cust_USD,
# MAGIC     CASE
# MAGIC         WHEN TRY_CAST(Booked_date AS DATE) < TRY_CAST(master_dates.use_date AS DATE) THEN 'PreBooked'
# MAGIC         ELSE 'SameDay'
# MAGIC         END AS PreBookStatus,
# MAGIC     sales.user_id as Sales_Rep_ID,
# MAGIC     tender_final.Tender_Source_Type,
# MAGIC     sac.TransitandRouting_Carrier_CAD,
# MAGIC     sac.TransitandRouting_Carrier_USD,
# MAGIC     sacc.TransitandRouting_Cust_CAD,
# MAGIC     sacc.TransitandRouting_Cust_USD 
# MAGIC     FROM big_export_projection
# MAGIC     JOIN master_dates ON big_export_projection.load_number = master_dates.booking_id AND big_export_projection.dispatch_status = master_dates.status 
# MAGIC     LEFT JOIN Silver.Silver_Relay_Customer_Money CustM ON big_export_projection.load_number = CustM.relay_reference_number AND big_export_projection.dispatch_status = CustM.status 
# MAGIC     LEFT JOIN financial_calendar ON COALESCE(delivery_datetime::DATE, big_export_projection.ship_date::DATE)::DATE = financial_calendar.date::DATE 
# MAGIC     LEFT JOIN customer_profile_projection ON big_export_projection.customer_id = customer_profile_projection.customer_slug 
# MAGIC     -- LEFT JOIN min_invoice_date ON big_export_projection.load_number::FLOAT = min_invoice_date.booking_id::FLOAT
# MAGIC     LEFT JOIN basic_invoice_info ON big_export_projection.load_number::FLOAT = basic_invoice_info.relay_reference_number
# MAGIC     LEFT JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC     LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN bronze.relay_users z on customer_profile_projection.primary_relay_user_id = z.user_id and z.`active?` = 'true' -- Customer Pespective
# MAGIC     LEFT JOIN relay_users sales on customer_profile_projection.sales_relay_user_id = sales.user_id and sales.`active?` = 'true' -- Sales Perspective
# MAGIC     LEFT JOIN bronze.canonical_plan_projection on big_export_projection.load_number = canonical_plan_projection.relay_reference_number --Equipment
# MAGIC     LEFT JOIN bronze.market_lookup o ON LEFT(big_export_projection.pickup_zip, 3) = o.pickup_zip -- Market Origin
# MAGIC     LEFT JOIN bronze.market_lookup d ON LEFT(big_export_projection.consignee_zip, 3) = d.pickup_zip -- Market Dest
# MAGIC     LEFT JOIN bronze.projection_carrier on big_export_projection.carrier_id = projection_carrier.id -- Carrier_Details
# MAGIC     LEFT JOIN Silver.Silver_Relay_Carrier_Money CarM 
# MAGIC     on big_export_projection.load_number = CarM.relay_reference_number
# MAGIC     -- Projection _expense of Carrier
# MAGIC     LEFT JOIN tender_reference_numbers_projection tn on big_export_projection.load_number = tn.relay_reference_number
# MAGIC     LEFT JOIN truckload_projection t  on big_export_projection.load_number = t.relay_reference_number and t.status != 'CancelledOrBounced' and t.trailer_number is not null
# MAGIC     LEFT JOIN last_delivery ON big_export_projection.load_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN bronze.planning_stop_schedule dpss
# MAGIC         ON big_export_projection.load_number = dpss.relay_reference_number
# MAGIC         AND big_export_projection.consignee_name::string = dpss.stop_name::string
# MAGIC         AND big_export_projection.consignee_city::string = dpss.locality::string
# MAGIC         AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC         AND dpss.stop_type::string = 'delivery'::string
# MAGIC         AND dpss.`removed?` = false
# MAGIC     LEFT JOIN bronze.planning_stop_schedule pss
# MAGIC             on (
# MAGIC               big_export_projection.load_number = pss.relay_reference_number
# MAGIC               AND big_export_projection.pickup_name = pss.stop_name
# MAGIC               AND pss.sequence_number = 1
# MAGIC               AND pss.`removed?` = 'false'
# MAGIC             )
# MAGIC     LEFT JOIN relay_spot_number sp  on big_export_projection.load_number = sp.relay_reference_number
# MAGIC     LEFT JOIN bronze.sourcing_max_buy_v2 mx on big_export_projection.Load_Number = mx.relay_reference_number 
# MAGIC     LEFT JOIN pricing_nfi_load_predictions mr on big_export_projection.Load_Number = mr.MR_id
# MAGIC     LEFT JOIN analytics.dim_date ON big_export_projection.ship_date::DATE = dim_date.calendar_date::DATE
# MAGIC     LEFT JOIN canada_conversions ON big_export_projection.ship_date::DATE = canada_conversions.ship_date::DATE
# MAGIC     LEFT JOIN average_conversions ON 1=1
# MAGIC     LEFT JOIN superinsight.accessorial_carriers sac on big_export_projection.load_number = sac.load_number and sac.tms = 'RELAY'
# MAGIC     LEFT JOIN superinsight.accessorial_customers sacc on big_export_projection.load_number = sacc.load_number and sacc.tms = 'RELAY'
# MAGIC     left join tender_final on big_export_projection.load_number = tender_final.id
# MAGIC
# MAGIC     WHERE 
# MAGIC       -- COALESCE(basic_invoice_info.first_invoiced::DATE, min_invoice_date.min_invoice_date::DATE) IS NULL
# MAGIC       --   AND 
# MAGIC       big_export_projection.carrier_name IS NOT NULL AND 
# MAGIC       big_export_projection.load_number != '2352492'
# MAGIC       AND big_export_projection.updated_At >= (SELECT MIN(LastLoadDateValue) - INTERVAL 1 DAY FROM Metadata.mastermetadata where TableID = 'R46') 
# MAGIC       AND dispatch_status NOT IN ('Cancelled')
# MAGIC       AND big_export_projection.Is_Deleted = 0
# MAGIC       AND canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC Relay AS 
# MAGIC (SELECT
# MAGIC   DW_Load_ID,
# MAGIC   Load_Number AS Load_ID,
# MAGIC   Admin_Fees_Carrier_USD AS Admin_Fees_Carrier_USD,
# MAGIC   Admin_Fees_Carrier_CAD AS Admin_Fees_Carrier_CAD,
# MAGIC   Admin_Fees_Customer_USD AS Admin_Fees_Customer_USD,
# MAGIC   Admin_Fees_Customer_CAD AS Admin_Fees_Customer_CAD,
# MAGIC   Billing_Status AS Billing_Status,
# MAGIC   Delivery_Date AS Actual_Delivered_Date,
# MAGIC   Booking_Type AS Booking_Type,
# MAGIC   Booked_Date AS Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_CAD AS Carrier_Fuel_Surcharge_CAD,
# MAGIC   Carrier_Fuel_Surcharge_USD AS Carrier_Fuel_Surcharge_USD,
# MAGIC   Carrier_Linehaul_CAD AS Carrier_Linehaul_CAD,
# MAGIC   Carrier_Linehaul_USD AS Carrier_Linehaul_USD,
# MAGIC   Carrier_ID AS Carrier_ID,
# MAGIC   Carrier_Name AS Carrier_Name,
# MAGIC   Carrier_New_Office AS Carrier_New_Office,
# MAGIC   Carrier_Old_Office AS Carrier_Old_Office,
# MAGIC   Carrier_Other_Fees_CAD AS Carrier_Other_Fees_CAD,
# MAGIC   Carrier_Other_Fees_USD AS Carrier_Other_Fees_USD,
# MAGIC   Carrier_Rep_ID AS Carrier_Rep_ID,
# MAGIC   Carrier_Rep AS Carrier_Rep,
# MAGIC   Combined_Load AS Combined_Load,
# MAGIC   Consignee_Delivery_Address AS Consignee_Delivery_Address,
# MAGIC   Consignee_Name AS Consignee_Name,
# MAGIC   Consignee_Pickup_Address AS Consignee_Pickup_Address,
# MAGIC   Customer_FuelSurcharge_USD AS Customer_Fuel_Surcharge_USD,
# MAGIC   Customer_FuelSurcharge_CAD AS Customer_Fule_Surcharge_CAD,
# MAGIC   Customer_Linehaul_CAD AS Customer_Linehaul_CAD,
# MAGIC   Customer_Linehaul_USD AS Customer_Linehaul_USD,
# MAGIC   Customer_ID AS Customer_ID,
# MAGIC   Customer_Master AS Customer_Master,
# MAGIC   Customer_Name AS Customer_Name,
# MAGIC   Customer_New_Office AS Customer_New_Office,
# MAGIC   Customer_Old_Office AS Customer_Old_Office,
# MAGIC   Customer_Other_Fees_CAD AS Customer_Other_Fees_CAD,
# MAGIC   Customer_Other_Fees_USD AS Customer_Other_Fees_USD,
# MAGIC   Customer_Rep_ID AS Customer_Rep_ID,
# MAGIC   Customer_Rep AS Customer_Rep,
# MAGIC   Days_ahead AS Days_Ahead,
# MAGIC   Bounced_Date AS Bounced_Date,
# MAGIC   Cancelled_Date AS Cancelled_Date,
# MAGIC   DelandPickup_Carrier_CAD AS Delivery_Pickup_Charges_Carrier_CAD,
# MAGIC   DelandPickup_Carrier_USD AS Delivery_Pickup_Charges_Carrier_USD,
# MAGIC   DelandPickup_Cust_CAD AS Delivery_Pickup_Charges_Customer_CAD,
# MAGIC   DelandPickup_Cust_USD AS Delivery_Pickup_Charges_Customer_USD,
# MAGIC   Delivery_Appointment_Date AS Delivery_Appointment_Date,
# MAGIC   Delivery_End AS Delivery_EndDate,
# MAGIC   Dest_City AS Destination_City,
# MAGIC   Dest_State AS Destination_State,
# MAGIC   Dest_Zip AS Destination_Zip,
# MAGIC   DOT_Number AS DOT_Number,
# MAGIC   Delivery_Start AS Delivery_StartDate,
# MAGIC   Equipment AS Equipment,
# MAGIC   EquipmentandVehicle_Carrier_CAD AS Equipment_Vehicle_Charges_Carrier_CAD,
# MAGIC   EquipmentandVehicle_Carrier_USD AS Equipment_Vehicle_Charges_Carrier_USD,
# MAGIC   EquipmentandVehicle_Cust_CAD AS Equipment_Vehicle_Charges_Customer_CAD,
# MAGIC   EquipmentandVehicle_Cust_USD AS Equipment_Vehicle_Charges_Customer_USD,
# MAGIC   Expense_CAD AS Expense_CAD,
# MAGIC   Expense_USD AS Expense_USD,
# MAGIC   Financial_Period_Year AS Financial_Period_Year,
# MAGIC   Max_Buy_Rate_CAD AS GreenScreen_Rate_CAD,
# MAGIC   Max_Buy_Rate AS GreenScreen_Rate_USD,
# MAGIC   Invoice_Amount_CAD AS Invoice_Amount_CAD,
# MAGIC   Invoice_Amount_USD AS Invoice_Amount_USD,
# MAGIC   Invoice_Date AS Invoiced_Date,
# MAGIC   Lawson_ID AS Lawson_ID,
# MAGIC   Load_Currency AS Load_Currency,
# MAGIC   Load_flag AS Load_Flag,
# MAGIC   load_lane AS Load_Lane,
# MAGIC   Load_Status AS Load_Status,
# MAGIC   Loadandunload_carrier_cad AS Load_Unload_Charges_Carrier_CAD,
# MAGIC   Loadandunload_carrier_USD AS Load_Unload_Charges_Carrier_USD,
# MAGIC   Loadandunload_Cust_CAD AS Load_Unload_Charges_Customer_CAD,
# MAGIC   Loadandunload_Cust_CAD AS Load_Unload_Charges_Customer_USD,
# MAGIC   Margin_CAD AS Margin_CAD,
# MAGIC   Margin_USD AS Margin_USD,
# MAGIC   Market_Buy_Rate_CAD AS Market_Buy_Rate_CAD,
# MAGIC   Market_Buy_Rate AS Market_Buy_Rate_USD,
# MAGIC   Market_Dest AS Market_Destination,
# MAGIC   Market_Lane AS Market_Lane,
# MAGIC   Market_Origin AS Market_Origin,
# MAGIC   Max_buy_rate AS MC_Number,
# MAGIC   Miles AS Miles,
# MAGIC   Miscellaneous_Carrier_CAD AS Miscellaneous_Chargers_Carrier_CAD,
# MAGIC   Miscellaneous_Carrier_USD AS Miscellaneous_Chargers_Carrier_USD,
# MAGIC   Miscellaneous_Cust_CAD AS Miscellaneous_Chargers_Customers_CAD,
# MAGIC   Miscellaneous_Cust_USD AS Miscellaneous_Chargers_Customers_USD,
# MAGIC   Mode AS Mode,
# MAGIC   On_Time_Delivery AS On_Time_Delivery,
# MAGIC   On_Time_Pickup AS On_Time_Pickup,
# MAGIC   Origin_City AS Origin_City,
# MAGIC   Origin_State AS Origin_State,
# MAGIC   Origin_Zip AS Origin_Zip,
# MAGIC   PermitsandCompliance_Carrier_CAD AS Permits_Compliance_Charges_Carrier_CAD,
# MAGIC   PermitsandCompliance_Carrier_USD AS Permits_Compliance_Charges_Carrier_USD,
# MAGIC   PermitsandCompliance_cust_CAD AS Permits_Compliance_Charges_Customer_CAD,
# MAGIC   PermitsandCompliance_cust_USD AS Permits_Compliance_Charges_Customer_USD,
# MAGIC   Pickup_Appointment_Date AS Pickup_Appointment_Date,
# MAGIC   Pickup_Datetime AS Actual_Pickup_Date,
# MAGIC   Pickup_End AS Pickup_EndDate,
# MAGIC   Pickup_Start AS Pickup_StartDate,
# MAGIC   PO_Number AS PO_Number,
# MAGIC   PreBookStatus AS PreBookStatus,
# MAGIC   Revenue_CAD AS Revenue_CAD,
# MAGIC   Revenue_USD AS Revenue_USD,
# MAGIC   Rolled_Date AS Rolled_Date,
# MAGIC   Sales_Rep_ID AS Sales_Rep_ID,
# MAGIC   Sales_Rep AS Sales_Rep,
# MAGIC   Ship_Date AS Ship_Date,
# MAGIC   Spot_Margin AS Spot_Margin,
# MAGIC   Spot_Revenue AS Spot_Revenue,
# MAGIC   Tender_Source_Type AS Tender_Source_Type,
# MAGIC   MAX(Tendering_Date) AS Tendered_Date,
# MAGIC   TMS_Sourcesystem AS TMS_System,
# MAGIC   Trailer_Number AS Trailer_Number,
# MAGIC   TransitandRouting_Carrier_CAD AS Transit_Routing_Chargers_Carrier_CAD,
# MAGIC   TransitandRouting_Carrier_USD AS Transit_Routing_Chargers_Carrier_USD,
# MAGIC   TransitandRouting_Cust_CAD AS Transit_Routing_Chargers_Customer_CAD,
# MAGIC   TransitandRouting_Cust_USD AS Transit_Routing_Chargers_Customer_USD,
# MAGIC   Days_Aged_To_Invoice AS Week_Number,
# MAGIC   Market_Buy_Rate_USD AS Carrier_Final_Rate_USD,
# MAGIC   Market_Buy_Rate_CAD AS Carrier_Final_Rate_CAD,
# MAGIC SHA1(CONCAT(
# MAGIC   COALESCE(Admin_Fees_Carrier_USD::string, ''),
# MAGIC   COALESCE(Admin_Fees_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Admin_Fees_Customer_USD::string, ''),
# MAGIC   COALESCE(Admin_Fees_Customer_CAD::string, ''),
# MAGIC   COALESCE(Billing_Status::string, ''),
# MAGIC   COALESCE(Actual_Delivered_Date::string, ''),
# MAGIC   COALESCE(Booking_Type::string, ''),
# MAGIC   COALESCE(Booked_Date::string, ''),
# MAGIC   COALESCE(Carrier_Fuel_Surcharge_CAD::string, ''),
# MAGIC   COALESCE(Carrier_Fuel_Surcharge_USD::string, ''),
# MAGIC   COALESCE(Carrier_Linehaul_CAD::string, ''),
# MAGIC   COALESCE(Carrier_Linehaul_USD::string, ''),
# MAGIC   COALESCE(Carrier_ID::string, ''),
# MAGIC   COALESCE(Carrier_Name::string, ''),
# MAGIC   COALESCE(Carrier_New_Office::string, ''),
# MAGIC   COALESCE(Carrier_Old_Office::string, ''),
# MAGIC   COALESCE(Carrier_Other_Fees_CAD::string, ''),
# MAGIC   COALESCE(Carrier_Other_Fees_USD::string, ''),
# MAGIC   COALESCE(Carrier_Rep_ID::string, ''),
# MAGIC   COALESCE(Carrier_Rep::string, ''),
# MAGIC   COALESCE(Combined_Load::string, ''),
# MAGIC   COALESCE(Consignee_Delivery_Address::string, ''),
# MAGIC   COALESCE(Consignee_Name::string, ''),
# MAGIC   COALESCE(Consignee_Pickup_Address::string, ''),
# MAGIC   COALESCE(Customer_Fuel_Surcharge_USD::string, ''),
# MAGIC   COALESCE(Customer_Fule_Surcharge_CAD::string, ''),
# MAGIC   COALESCE(Customer_Linehaul_CAD::string, ''),
# MAGIC   COALESCE(Customer_Linehaul_USD::string, ''),
# MAGIC   COALESCE(Customer_ID::string, ''),
# MAGIC   COALESCE(Customer_Master::string, ''),
# MAGIC   COALESCE(Customer_Name::string, ''),
# MAGIC   COALESCE(Customer_New_Office::string, ''),
# MAGIC   COALESCE(Customer_Old_Office::string, ''),
# MAGIC   COALESCE(Customer_Other_Fees_CAD::string, ''),
# MAGIC   COALESCE(Customer_Other_Fees_USD::string, ''),
# MAGIC   COALESCE(Customer_Rep_ID::string, ''),
# MAGIC   COALESCE(Customer_Rep::string, ''),
# MAGIC   COALESCE(Days_Ahead::string, ''),
# MAGIC   COALESCE(Bounced_Date::string, ''),
# MAGIC   COALESCE(Cancelled_Date::string, ''),
# MAGIC   COALESCE(Delivery_Pickup_Charges_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Delivery_Pickup_Charges_Carrier_USD::string, ''),
# MAGIC   COALESCE(Delivery_Pickup_Charges_Customer_CAD::string, ''),
# MAGIC   COALESCE(Delivery_Pickup_Charges_Customer_USD::string, ''),
# MAGIC   COALESCE(Delivery_Appointment_Date::string, ''),
# MAGIC   COALESCE(Delivery_EndDate::string, ''),
# MAGIC   COALESCE(Destination_City::string, ''),
# MAGIC   COALESCE(Destination_State::string, ''),
# MAGIC   COALESCE(Destination_Zip::string, ''),
# MAGIC   COALESCE(DOT_Number::string, ''),
# MAGIC   COALESCE(Delivery_StartDate::string, ''),
# MAGIC   COALESCE(Equipment::string, ''),
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Carrier_USD::string, ''),
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Customer_CAD::string, ''),
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Customer_USD::string, ''),
# MAGIC   COALESCE(Expense_CAD::string, ''),
# MAGIC   COALESCE(Expense_USD::string, ''),
# MAGIC   COALESCE(Financial_Period_Year::string, ''),
# MAGIC   COALESCE(GreenScreen_Rate_CAD::string, ''),
# MAGIC   COALESCE(GreenScreen_Rate_USD::string, ''),
# MAGIC   COALESCE(Invoice_Amount_CAD::string, ''),
# MAGIC   COALESCE(Invoice_Amount_USD::string, ''),
# MAGIC   COALESCE(Invoiced_Date::string, ''),
# MAGIC   COALESCE(Lawson_ID::string, ''),
# MAGIC   COALESCE(Load_Currency::string, ''),
# MAGIC   COALESCE(Load_Flag::string, ''),
# MAGIC   COALESCE(Load_Lane::string, ''),
# MAGIC   COALESCE(Load_Status::string, ''),
# MAGIC   COALESCE(Load_Unload_Charges_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Load_Unload_Charges_Carrier_USD::string, ''),
# MAGIC   COALESCE(Load_Unload_Charges_Customer_CAD::string, ''),
# MAGIC   COALESCE(Load_Unload_Charges_Customer_USD::string, ''),
# MAGIC   COALESCE(Margin_CAD::string, ''),
# MAGIC   COALESCE(Margin_USD::string, ''),
# MAGIC   COALESCE(Market_Buy_Rate_CAD::string, 'NA'),
# MAGIC   COALESCE(Market_Buy_Rate_USD::string, 'NA'),
# MAGIC   COALESCE(Market_Destination::string, ''),
# MAGIC   COALESCE(Market_Lane::string, ''),
# MAGIC   COALESCE(Market_Origin::string, ''),
# MAGIC   COALESCE(MC_Number::string, ''),
# MAGIC   COALESCE(Miles::string, ''),
# MAGIC   COALESCE(Miscellaneous_Chargers_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Miscellaneous_Chargers_Carrier_USD::string, ''),
# MAGIC   COALESCE(Miscellaneous_Chargers_Customers_CAD::string, ''),
# MAGIC   COALESCE(Miscellaneous_Chargers_Customers_USD::string, ''),
# MAGIC   COALESCE(Mode::string, ''),
# MAGIC   COALESCE(On_Time_Delivery::string, ''),
# MAGIC   COALESCE(On_Time_Pickup::string, ''),
# MAGIC   COALESCE(Origin_City::string, ''),
# MAGIC   COALESCE(Origin_State::string, ''),
# MAGIC   COALESCE(Origin_Zip::string, ''),
# MAGIC   COALESCE(Permits_Compliance_Charges_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Permits_Compliance_Charges_Carrier_USD::string, ''),
# MAGIC   COALESCE(Permits_Compliance_Charges_Customer_CAD::string, ''),
# MAGIC   COALESCE(Permits_Compliance_Charges_Customer_USD::string, ''),
# MAGIC   COALESCE(Pickup_Appointment_Date::string, ''),
# MAGIC   COALESCE(Actual_Pickup_Date::string, ''),
# MAGIC   COALESCE(Pickup_EndDate::string, ''),
# MAGIC   COALESCE(Pickup_StartDate::string, ''),
# MAGIC   COALESCE(PO_Number::string, ''),
# MAGIC   COALESCE(PreBookStatus::string, ''),
# MAGIC   COALESCE(Revenue_CAD::string, ''),
# MAGIC   COALESCE(Revenue_USD::string, ''),
# MAGIC   COALESCE(Rolled_Date::string, ''),
# MAGIC   COALESCE(Sales_Rep_ID::string, ''),
# MAGIC   COALESCE(Sales_Rep::string, ''),
# MAGIC   COALESCE(Ship_Date::string, ''),
# MAGIC   COALESCE(Spot_Margin::string, ''),
# MAGIC   COALESCE(Spot_Revenue::string, ''),
# MAGIC   COALESCE(Tender_Source_Type::string, ''),
# MAGIC   COALESCE(Tendered_Date::string, ''),
# MAGIC   COALESCE(TMS_System::string, ''),
# MAGIC   COALESCE(Trailer_Number::string, ''),
# MAGIC   COALESCE(Transit_Routing_Chargers_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Transit_Routing_Chargers_Carrier_USD::string, ''),
# MAGIC   COALESCE(Transit_Routing_Chargers_Customer_CAD::string, ''),
# MAGIC   COALESCE(Transit_Routing_Chargers_Customer_USD::string, ''),
# MAGIC   COALESCE(Week_Number::string, ''),
# MAGIC   COALESCE(Carrier_Final_Rate_USD::string, ''),
# MAGIC   COALESCE(Carrier_Final_Rate_CAD::string, '')
# MAGIC )) AS HashKey
# MAGIC FROM Final
# MAGIC GROUP BY Load_Number	,
# MAGIC Admin_Fees_Carrier_USD	,
# MAGIC Admin_Fees_Carrier_CAD	,
# MAGIC Admin_Fees_Customer_USD	,
# MAGIC Admin_Fees_Customer_CAD	,
# MAGIC Billing_Status	,
# MAGIC Delivery_Date	,
# MAGIC Booking_Type	,
# MAGIC Booked_Date	,
# MAGIC Carrier_Fuel_Surcharge_CAD	,
# MAGIC Carrier_Fuel_Surcharge_USD	,
# MAGIC Carrier_Linehaul_CAD	,
# MAGIC Carrier_Linehaul_USD	,
# MAGIC Carrier_ID	,
# MAGIC Carrier_Name	,
# MAGIC Carrier_New_Office	,
# MAGIC Carrier_Old_Office	,
# MAGIC Carrier_Other_Fees_CAD	,
# MAGIC Carrier_Other_Fees_USD	,
# MAGIC Carrier_Rep_ID	,
# MAGIC Carrier_Rep	,
# MAGIC Combined_Load	,
# MAGIC Consignee_Delivery_Address	,
# MAGIC Consignee_Name	,
# MAGIC Consignee_Pickup_Address	,
# MAGIC Customer_FuelSurcharge_USD	,
# MAGIC Customer_FuelSurcharge_CAD	,
# MAGIC Customer_Linehaul_CAD	,
# MAGIC Customer_Linehaul_USD	,
# MAGIC Customer_ID	,
# MAGIC Customer_Master	,
# MAGIC Customer_Name	,
# MAGIC Customer_New_Office	,
# MAGIC Customer_Old_Office	,
# MAGIC Customer_Other_Fees_CAD	,
# MAGIC Customer_Other_Fees_USD	,
# MAGIC Customer_Rep_ID	,
# MAGIC Customer_Rep	,
# MAGIC Days_ahead	,
# MAGIC Bounced_Date	,
# MAGIC Cancelled_Date	,
# MAGIC DelandPickup_Carrier_CAD	,
# MAGIC DelandPickup_Carrier_USD	,
# MAGIC DelandPickup_Cust_CAD	,
# MAGIC DelandPickup_Cust_USD	,
# MAGIC Delivery_Appointment_Date	,
# MAGIC Delivery_End	,
# MAGIC Dest_City	,
# MAGIC Dest_State	,
# MAGIC Dest_Zip	,
# MAGIC DOT_Number	,
# MAGIC Delivery_Start	,
# MAGIC Equipment	,
# MAGIC EquipmentandVehicle_Carrier_CAD	,
# MAGIC EquipmentandVehicle_Carrier_USD	,
# MAGIC EquipmentandVehicle_Cust_CAD	,
# MAGIC EquipmentandVehicle_Cust_USD	,
# MAGIC Expense_CAD	,
# MAGIC Expense_USD	,
# MAGIC Financial_Period_Year	,
# MAGIC Max_Buy_Rate_CAD	,
# MAGIC Max_Buy_Rate	,
# MAGIC Invoice_Amount_CAD	,
# MAGIC Invoice_Amount_USD	,
# MAGIC Invoice_Date	,
# MAGIC Lawson_ID	,
# MAGIC Load_Currency	,
# MAGIC Load_flag	,
# MAGIC load_lane	,
# MAGIC Load_Status	,
# MAGIC Loadandunload_carrier_cad	,
# MAGIC Loadandunload_carrier_USD	,
# MAGIC Loadandunload_Cust_CAD	,
# MAGIC Loadandunload_Cust_CAD	,
# MAGIC Margin_CAD	,
# MAGIC Margin_USD	,
# MAGIC Market_Buy_Rate_CAD	,
# MAGIC Market_Buy_Rate	,
# MAGIC Market_Dest	,
# MAGIC Market_Lane	,
# MAGIC Market_Origin	,
# MAGIC Max_buy_rate	,
# MAGIC Miles	,
# MAGIC Miscellaneous_Carrier_CAD	,
# MAGIC Miscellaneous_Carrier_USD	,
# MAGIC Miscellaneous_Cust_CAD	,
# MAGIC Miscellaneous_Cust_USD	,
# MAGIC Mode	,
# MAGIC On_Time_Delivery	,
# MAGIC On_Time_Pickup	,
# MAGIC Origin_City	,
# MAGIC Origin_State	,
# MAGIC Origin_Zip	,
# MAGIC PermitsandCompliance_Carrier_CAD	,
# MAGIC PermitsandCompliance_Carrier_USD	,
# MAGIC PermitsandCompliance_cust_CAD	,
# MAGIC PermitsandCompliance_cust_USD	,
# MAGIC Pickup_Appointment_Date	,
# MAGIC Pickup_Datetime	,
# MAGIC Pickup_End	,
# MAGIC Pickup_Start	,
# MAGIC PO_Number	,
# MAGIC PreBookStatus	,
# MAGIC Revenue_CAD	,
# MAGIC Revenue_USD	,
# MAGIC Rolled_Date	,
# MAGIC Sales_Rep_ID	,
# MAGIC Sales_Rep	,
# MAGIC Ship_Date	,
# MAGIC Spot_Margin	,
# MAGIC Spot_Revenue	,
# MAGIC Tender_Source_Type	,
# MAGIC TMS_Sourcesystem	,
# MAGIC Trailer_Number	,
# MAGIC TransitandRouting_Carrier_CAD	,
# MAGIC TransitandRouting_Carrier_USD	,
# MAGIC TransitandRouting_Cust_CAD	,
# MAGIC TransitandRouting_Cust_USD	,
# MAGIC Days_Aged_To_Invoice	,
# MAGIC Market_Buy_Rate_USD,
# MAGIC Market_Buy_Rate_CAD,
# MAGIC DW_Load_ID,
# MAGIC MC_Number)
# MAGIC
# MAGIC
# MAGIC SELECT * FROM Relay WHERE Load_ID IN (SELECT Load_ID FROM Relay GROUP BY 1 HAVING COUNT(*) = 1)
# MAGIC UNION
# MAGIC SELECT * FROM Relay WHERE Load_ID IN (SELECT Load_ID FROM Relay GROUP BY 1 HAVING COUNT(*) > 1) And Load_Status = 'BOOKED' AND Carrier_Name IS NOT NULL AND Mode = 'TL'

# COMMAND ----------

# MAGIC %md
# MAGIC ####Full Load View

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use schema bronze;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Silver.VW_Silver_Relay_Full_Load AS
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
# MAGIC ),
# MAGIC -- min_invoice_date AS (
# MAGIC --   SELECT
# MAGIC --     DISTINCT booking_id,
# MAGIC --     MIN(invoice_date) :: DATE AS min_invoice_date,
# MAGIC --     SUM(total_approved_amount_to_pay) :: FLOAT / 100.00 AS approved_amt
# MAGIC --   FROM
# MAGIC --     integration_hubtran_vendor_invoice_approved
# MAGIC --   GROUP BY
# MAGIC --     booking_id
# MAGIC -- ),
# MAGIC max_schedule_del AS (
# MAGIC   SELECT
# MAGIC     DISTINCT delivery_projection.relay_reference_number,
# MAGIC     max(delivery_projection.scheduled_at :: timestamp) AS max_schedule_del
# MAGIC   FROM
# MAGIC     delivery_projection
# MAGIC   GROUP BY
# MAGIC     delivery_projection.relay_reference_number
# MAGIC ),
# MAGIC max_schedule AS (
# MAGIC   SELECT
# MAGIC     DISTINCT pickup_projection.relay_reference_number,
# MAGIC     max(pickup_projection.scheduled_at :: timestamp) AS max_schedule
# MAGIC   FROM
# MAGIC     pickup_projection
# MAGIC   GROUP BY
# MAGIC     pickup_projection.relay_reference_number
# MAGIC ),
# MAGIC last_delivery AS (
# MAGIC   SELECT
# MAGIC     DISTINCT planning_stop_schedule.relay_reference_number,
# MAGIC     max(planning_stop_schedule.sequence_number) AS last_delivery
# MAGIC   FROM
# MAGIC     planning_stop_schedule
# MAGIC   WHERE
# MAGIC     planning_stop_schedule.`removed?` = false
# MAGIC   GROUP BY
# MAGIC     planning_stop_schedule.relay_reference_number
# MAGIC ),
# MAGIC truckload_proj_del AS (
# MAGIC   SELECT
# MAGIC     DISTINCT truckload_projection.booking_id,
# MAGIC     max(
# MAGIC       truckload_projection.last_update_date_time :: STRING
# MAGIC     ) :: timestamp AS truckload_proj_del
# MAGIC   FROM
# MAGIC     truckload_projection
# MAGIC   WHERE
# MAGIC     lower(
# MAGIC       truckload_projection.last_update_event_name :: STRING
# MAGIC     ) = 'markeddelivered' :: STRING
# MAGIC   GROUP BY
# MAGIC     truckload_projection.booking_id
# MAGIC ),
# MAGIC master_dates as (
# MAGIC   select
# MAGIC     b.booking_id,
# MAGIC     b.relay_reference_number,
# MAGIC     b.status,
# MAGIC     'TL' AS ltl_or_tl,
# MAGIC     COALESCE(
# MAGIC       try_cast(c.in_date_time AS TIMESTAMP),
# MAGIC       try_cast(c.out_date_time AS TIMESTAMP),
# MAGIC       try_cast(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       try_cast(pp.appointment_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN b.ready_time IS NULL
# MAGIC         OR b.ready_time = '' THEN try_cast(b.ready_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             try_cast(SUBSTRING(b.ready_time, 1, 2) AS INT) * 3600
# MAGIC           ) + (
# MAGIC             try_cast(SUBSTRING(b.ready_time, 4, 2) AS INT) * 60
# MAGIC           ),
# MAGIC           try_cast(b.ready_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS use_date,
# MAGIC     CASE
# MAGIC       WHEN dayofweek(
# MAGIC         COALESCE(
# MAGIC           try_cast(c.in_date_time AS DATE),
# MAGIC           try_cast(c.out_date_time AS DATE),
# MAGIC           try_cast(pss.appointment_datetime AS DATE),
# MAGIC           try_cast(pss.window_end_datetime AS DATE),
# MAGIC           try_cast(pss.window_start_datetime AS DATE),
# MAGIC           try_cast(pp.appointment_datetime AS DATE),
# MAGIC           b.ready_date
# MAGIC         )
# MAGIC       ) = 1 THEN DATE_ADD(
# MAGIC         COALESCE(
# MAGIC           try_cast(c.in_date_time AS DATE),
# MAGIC           try_cast(c.out_date_time AS DATE),
# MAGIC           try_cast(pss.appointment_datetime AS DATE),
# MAGIC           try_cast(pss.window_end_datetime AS DATE),
# MAGIC           try_cast(pss.window_start_datetime AS DATE),
# MAGIC           try_cast(pp.appointment_datetime AS DATE),
# MAGIC           b.ready_date
# MAGIC         ),
# MAGIC         6
# MAGIC       )
# MAGIC       ELSE DATE_ADD(
# MAGIC         date_trunc(
# MAGIC           'WEEK',
# MAGIC           COALESCE(
# MAGIC             try_cast(c.in_date_time AS TIMESTAMP),
# MAGIC             try_cast(c.out_date_time AS TIMESTAMP),
# MAGIC             try_cast(pss.appointment_datetime AS TIMESTAMP),
# MAGIC             try_cast(pss.window_end_datetime AS TIMESTAMP),
# MAGIC             try_cast(pss.window_start_datetime AS TIMESTAMP),
# MAGIC             try_cast(pp.appointment_datetime AS TIMESTAMP),
# MAGIC             b.ready_date
# MAGIC           )
# MAGIC         ),
# MAGIC         5
# MAGIC       )
# MAGIC     END AS end_date,
# MAGIC     CASE
# MAGIC       WHEN dayofweek(CAST(b.booked_at AS DATE)) = 1 THEN DATE_ADD(CAST(b.booked_at AS DATE), 6)
# MAGIC       ELSE DATE_ADD(
# MAGIC         date_trunc('WEEK', CAST(b.booked_at AS TIMESTAMP)),
# MAGIC         5
# MAGIC       )
# MAGIC     END AS booked_end_date,
# MAGIC     CASE
# MAGIC       WHEN b.ready_time IS NULL
# MAGIC       OR b.ready_time = '' THEN try_cast(b.ready_date AS TIMESTAMP)
# MAGIC       ELSE TIMESTAMPADD(
# MAGIC         SECOND,
# MAGIC         (
# MAGIC           try_cast(SUBSTRING(b.ready_time, 1, 2) AS INT) * 3600
# MAGIC         ) + (
# MAGIC           try_cast(SUBSTRING(b.ready_time, 4, 2) AS INT) * 60
# MAGIC         ),
# MAGIC         try_cast(b.ready_date AS TIMESTAMP)
# MAGIC       )
# MAGIC     END AS ready_date,
# MAGIC     COALESCE(
# MAGIC       try_cast(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       try_cast(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_start_datetime,
# MAGIC     try_cast(pss.window_end_datetime AS TIMESTAMP) AS pu_appt_end_datetime,
# MAGIC     COALESCE(
# MAGIC       try_cast(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       try_cast(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       try_cast(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_date,
# MAGIC     pss.schedule_type AS pu_schedule_type,
# MAGIC     pss.scheduled_at as Scheduled_Date,
# MAGIC     try_cast(c.in_date_time AS TIMESTAMP) AS pickup_in,
# MAGIC     try_cast(c.out_date_time AS TIMESTAMP) AS pickup_out,
# MAGIC     COALESCE(
# MAGIC       try_cast(c.in_date_time AS TIMESTAMP),
# MAGIC       try_cast(c.out_date_time AS TIMESTAMP)
# MAGIC     ) AS pickup_datetime,
# MAGIC     COALESCE(
# MAGIC       try_cast(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC       try_cast(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN dp.appointment_time_local IS NULL
# MAGIC         OR dp.appointment_time_local = '' THEN try_cast(dp.appointment_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             try_cast(
# MAGIC               SUBSTRING(dp.appointment_time_local, 1, 2) AS INT
# MAGIC             ) * 3600
# MAGIC           ) + (
# MAGIC             try_cast(
# MAGIC               SUBSTRING(dp.appointment_time_local, 4, 2) AS INT
# MAGIC             ) * 60
# MAGIC           ),
# MAGIC           try_cast(dp.appointment_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS del_appt_start_datetime,
# MAGIC     try_cast(dpss.window_end_datetime AS TIMESTAMP) AS del_appt_end_datetime,
# MAGIC     COALESCE(
# MAGIC       try_cast(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC       try_cast(dpss.window_end_datetime AS TIMESTAMP),
# MAGIC       try_cast(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN dp.appointment_time_local IS NULL
# MAGIC         OR dp.appointment_time_local = '' THEN try_cast(dp.appointment_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             try_cast(
# MAGIC               SUBSTRING(dp.appointment_time_local, 1, 2) AS INT
# MAGIC             ) * 3600
# MAGIC           ) + (
# MAGIC             try_cast(
# MAGIC               SUBSTRING(dp.appointment_time_local, 4, 2) AS INT
# MAGIC             ) * 60
# MAGIC           ),
# MAGIC           try_cast(dp.appointment_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS del_appt_date,
# MAGIC     dpss.schedule_type AS del_schedule_type,
# MAGIC     try_cast(
# MAGIC       hain_tracking_accuracy_projection.delivery_by_date_at_tender AS DATE
# MAGIC     ) AS Delivery_By_Date,
# MAGIC     try_cast(cd.in_date_time AS TIMESTAMP) AS delivery_in,
# MAGIC     try_cast(cd.out_date_time AS TIMESTAMP) AS delivery_out,
# MAGIC     COALESCE(
# MAGIC       try_cast(cd.in_date_time AS TIMESTAMP),
# MAGIC       try_cast(cd.out_date_time AS TIMESTAMP)
# MAGIC     ) AS delivery_datetime,
# MAGIC     t.truckload_proj_del AS truckload_proj_delivered,
# MAGIC     try_cast(b.booked_at AS TIMESTAMP) AS booked_at
# MAGIC   FROM
# MAGIC     booking_projection b
# MAGIC     LEFT JOIN max_schedule ON b.relay_reference_number = max_schedule.relay_reference_number
# MAGIC     LEFT JOIN max_schedule_del ON b.relay_reference_number = max_schedule_del.relay_reference_number
# MAGIC     LEFT JOIN pickup_projection pp ON b.booking_id = pp.booking_id
# MAGIC     AND b.first_shipper_name :: string = pp.shipper_name :: string
# MAGIC     AND max_schedule.max_schedule = pp.scheduled_at :: timestamp
# MAGIC     AND pp.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule pss ON b.relay_reference_number = pss.relay_reference_number
# MAGIC     AND b.first_shipper_name :: string = pss.stop_name :: string
# MAGIC     AND pss.sequence_number = 1
# MAGIC     AND pss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop c ON pss.stop_id :: string = c.stop_id :: string
# MAGIC     AND c.`stale?` = false
# MAGIC     AND lower(c.stop_type :: string) = 'pickup' :: string
# MAGIC     LEFT JOIN last_delivery ON b.relay_reference_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN delivery_projection dp ON b.relay_reference_number = dp.relay_reference_number
# MAGIC     AND b.receiver_name :: string = dp.receiver_name :: string
# MAGIC     AND max_schedule_del.max_schedule_del = dp.scheduled_at :: timestamp
# MAGIC     LEFT JOIN planning_stop_schedule dpss ON b.relay_reference_number = dpss.relay_reference_number
# MAGIC     AND b.receiver_name :: string = dpss.stop_name :: string
# MAGIC     AND b.receiver_city :: string = dpss.locality :: string
# MAGIC     AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC     AND lower(dpss.stop_type :: string) = 'delivery' :: string
# MAGIC     AND dpss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop cd ON dpss.stop_id :: string = cd.stop_id :: string
# MAGIC     AND b.receiver_city :: string = cd.locality :: string
# MAGIC     AND cd.`stale?` = false
# MAGIC     AND lower(cd.stop_type :: string) = 'delivery' :: string
# MAGIC     LEFT JOIN hain_tracking_accuracy_projection ON b.relay_reference_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC     LEFT JOIN canonical_plan_projection ON b.relay_reference_number = canonical_plan_projection.relay_reference_number
# MAGIC     LEFT JOIN truckload_proj_del t ON b.booking_id = t.booking_id
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     AND lower(b.status :: string) <> 'cancelled' :: string
# MAGIC     AND (
# MAGIC       lower(canonical_plan_projection.mode :: string) <> 'ltl' :: string
# MAGIC       OR canonical_plan_projection.mode IS NULL
# MAGIC     )
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     DISTINCT b.load_number AS booking_id,
# MAGIC     b.load_number AS relay_reference_number,
# MAGIC     b.dispatch_status AS status,
# MAGIC     'LTL' AS ltl_or_tl,
# MAGIC     COALESCE(
# MAGIC       CAST(b.ship_date AS TIMESTAMP),
# MAGIC       CAST(c.in_date_time AS TIMESTAMP),
# MAGIC       CAST(c.out_date_time AS TIMESTAMP),
# MAGIC       CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       CAST(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS use_date,
# MAGIC     CASE
# MAGIC       WHEN dayofweek(
# MAGIC         CAST(
# MAGIC           COALESCE(
# MAGIC             b.ship_date,
# MAGIC             c.in_date_time,
# MAGIC             c.out_date_time,
# MAGIC             pss.appointment_datetime,
# MAGIC             pss.window_end_datetime,
# MAGIC             pss.window_start_datetime,
# MAGIC             pp.appointment_datetime
# MAGIC           ) AS DATE
# MAGIC         )
# MAGIC       ) = 1 THEN DATE_ADD(
# MAGIC         CAST(
# MAGIC           COALESCE(
# MAGIC             b.ship_date,
# MAGIC             c.in_date_time,
# MAGIC             c.out_date_time,
# MAGIC             pss.appointment_datetime,
# MAGIC             pss.window_end_datetime,
# MAGIC             pss.window_start_datetime,
# MAGIC             pp.appointment_datetime
# MAGIC           ) AS DATE
# MAGIC         ),
# MAGIC         6
# MAGIC       )
# MAGIC       ELSE DATE_ADD(
# MAGIC         date_trunc(
# MAGIC           'WEEK',
# MAGIC           CAST(
# MAGIC             COALESCE(
# MAGIC               b.ship_date,
# MAGIC               c.in_date_time,
# MAGIC               c.out_date_time,
# MAGIC               pss.appointment_datetime,
# MAGIC               pss.window_end_datetime,
# MAGIC               pss.window_start_datetime,
# MAGIC               pp.appointment_datetime
# MAGIC             ) AS TIMESTAMP
# MAGIC           )
# MAGIC         ),
# MAGIC         5
# MAGIC       )
# MAGIC     END AS end_date,
# MAGIC     NULL AS booked_end_date,
# MAGIC     NULL AS ready_date,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_start_datetime,
# MAGIC     TRY_CAST(pss.window_end_datetime AS TIMESTAMP) AS pu_appt_end_datetime,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(pss.appointment_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pss.window_end_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pss.window_start_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(pp.appointment_datetime AS TIMESTAMP)
# MAGIC     ) AS pu_appt_date,
# MAGIC     pss.schedule_type AS pu_schedule_type,
# MAGIC     pss.scheduled_at as Scheduled_Date,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(b.ship_date AS TIMESTAMP),
# MAGIC       TRY_CAST(c.in_date_time AS TIMESTAMP)
# MAGIC     ) AS pickup_in,
# MAGIC     TRY_CAST(c.out_date_time AS TIMESTAMP) AS pickup_out,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(b.ship_date AS TIMESTAMP),
# MAGIC       TRY_CAST(c.in_date_time AS TIMESTAMP),
# MAGIC       TRY_CAST(c.out_date_time AS TIMESTAMP)
# MAGIC     ) AS pickup_datetime,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN dp.appointment_time_local IS NULL
# MAGIC         OR dp.appointment_time_local = '' THEN TRY_CAST(dp.appointment_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             TRY_CAST(
# MAGIC               SUBSTRING(dp.appointment_time_local, 1, 2) AS INT
# MAGIC             ) * 3600
# MAGIC           ) + (
# MAGIC             TRY_CAST(
# MAGIC               SUBSTRING(dp.appointment_time_local, 4, 2) AS INT
# MAGIC             ) * 60
# MAGIC           ),
# MAGIC           TRY_CAST(dp.appointment_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS del_appt_start_datetime,
# MAGIC     TRY_CAST(dpss.window_end_datetime AS TIMESTAMP) AS del_appt_end_datetime,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(dpss.appointment_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(dpss.window_end_datetime AS TIMESTAMP),
# MAGIC       TRY_CAST(dpss.window_start_datetime AS TIMESTAMP),
# MAGIC       CASE
# MAGIC         WHEN dp.appointment_time_local IS NULL
# MAGIC         OR dp.appointment_time_local = '' THEN TRY_CAST(dp.appointment_date AS TIMESTAMP)
# MAGIC         ELSE TIMESTAMPADD(
# MAGIC           SECOND,
# MAGIC           (
# MAGIC             TRY_CAST(
# MAGIC               SUBSTRING(dp.appointment_time_local, 1, 2) AS INT
# MAGIC             ) * 3600
# MAGIC           ) + (
# MAGIC             TRY_CAST(
# MAGIC               SUBSTRING(dp.appointment_time_local, 4, 2) AS INT
# MAGIC             ) * 60
# MAGIC           ),
# MAGIC           TRY_CAST(dp.appointment_date AS TIMESTAMP)
# MAGIC         )
# MAGIC       END
# MAGIC     ) AS del_appt_date,
# MAGIC     dpss.schedule_type AS del_schedule_type,
# MAGIC     TRY_CAST(
# MAGIC       hain_tracking_accuracy_projection.delivery_by_date_at_tender AS DATE
# MAGIC     ) AS Delivery_By_Date,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(b.delivered_date AS TIMESTAMP),
# MAGIC       TRY_CAST(cd.in_date_time AS TIMESTAMP)
# MAGIC     ) AS delivery_in,
# MAGIC     TRY_CAST(cd.out_date_time AS TIMESTAMP) AS delivery_out,
# MAGIC     COALESCE(
# MAGIC       TRY_CAST(b.delivered_date AS TIMESTAMP),
# MAGIC       TRY_CAST(cd.in_date_time AS TIMESTAMP),
# MAGIC       TRY_CAST(cd.out_date_time AS TIMESTAMP)
# MAGIC     ) AS delivery_datetime,
# MAGIC     NULL AS truckload_proj_delivered,
# MAGIC     NULL AS booked_at
# MAGIC   FROM
# MAGIC     big_export_projection b
# MAGIC     LEFT JOIN max_schedule ON b.load_number = max_schedule.relay_reference_number
# MAGIC     LEFT JOIN max_schedule_del ON b.load_number = max_schedule_del.relay_reference_number
# MAGIC     LEFT JOIN pickup_projection pp ON b.load_number = pp.relay_reference_number
# MAGIC     AND b.pickup_name :: string = pp.shipper_name :: string
# MAGIC     AND b.weight = pp.weight_to_pickup_amount
# MAGIC     AND b.piece_count :: double = pp.pieces_to_pickup_count
# MAGIC     AND max_schedule.max_schedule = pp.scheduled_at :: timestamp
# MAGIC     AND pp.sequence_number = 1
# MAGIC     LEFT JOIN planning_stop_schedule pss ON b.load_number = pss.relay_reference_number
# MAGIC     AND b.pickup_name :: string = pss.stop_name :: string
# MAGIC     AND pss.sequence_number = 1
# MAGIC     AND pss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop c ON pss.stop_id :: string = c.stop_id :: string
# MAGIC     LEFT JOIN delivery_projection dp ON b.load_number = dp.relay_reference_number
# MAGIC     AND b.consignee_name :: string = dp.receiver_name :: string
# MAGIC     AND max_schedule_del.max_schedule_del = dp.scheduled_at :: timestamp
# MAGIC     LEFT JOIN last_delivery ON b.load_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN planning_stop_schedule dpss ON b.load_number = dpss.relay_reference_number
# MAGIC     AND b.consignee_name :: string = dpss.stop_name :: string
# MAGIC     AND b.consignee_city :: string = dpss.locality :: string
# MAGIC     AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC     AND lower(dpss.stop_type :: string) = 'delivery' :: string
# MAGIC     AND dpss.`removed?` = false
# MAGIC     LEFT JOIN canonical_stop cd ON dpss.stop_id :: string = cd.stop_id :: string
# MAGIC     LEFT JOIN hain_tracking_accuracy_projection ON b.load_number = hain_tracking_accuracy_projection.relay_reference_number
# MAGIC     LEFT JOIN canonical_plan_projection ON b.load_number = canonical_plan_projection.relay_reference_number
# MAGIC   WHERE
# MAGIC     lower(b.dispatch_status :: string) <> 'cancelled' :: string
# MAGIC     AND (
# MAGIC       lower(canonical_plan_projection.mode :: string) = 'ltl' :: string
# MAGIC       OR canonical_plan_projection.mode IS NULL
# MAGIC     )
# MAGIC ),
# MAGIC combined_loads_mine AS (
# MAGIC   SELECT
# MAGIC     DISTINCT plan_combination_projection.relay_reference_number_one,
# MAGIC     plan_combination_projection.relay_reference_number_two,
# MAGIC     plan_combination_projection.resulting_plan_id,
# MAGIC     canonical_plan_projection.relay_reference_number AS combine_new_rrn,
# MAGIC     canonical_plan_projection.mode,
# MAGIC     b.tender_on_behalf_of_id,
# MAGIC     'COMBINED' AS combined,
# MAGIC     COALESCE(one_money.one_money, 0.0) AS one_money,
# MAGIC     COALESCE(one_lhc_money.one_lhc_money, 0.0) AS one_lhc_money,
# MAGIC     COALESCE(one_fuel_money.one_fuel_money, 0.0) AS one_fuel_money,
# MAGIC     COALESCE(two_money.two_money, 0.0) AS two_money,
# MAGIC     COALESCE(two_lhc_money.two_lhc_money, 0.0) AS two_lhc_money,
# MAGIC     COALESCE(two_fuel_money.two_fuel_money, 0.0) AS two_fuel_money,
# MAGIC     COALESCE(one_money.one_money, 0.0) + COALESCE(two_money.two_money, 0.0) AS total_load_rev,
# MAGIC     COALESCE(one_lhc_money.one_lhc_money, 0.0) + COALESCE(two_lhc_money.two_lhc_money, 0.0) AS total_load_lhc_rev,
# MAGIC     COALESCE(one_fuel_money.one_fuel_money, 0.0) + COALESCE(two_fuel_money.two_fuel_money, 0.0) AS total_load_fuel_rev,
# MAGIC     COALESCE(carrier_money.carrier_money, 0.0) AS final_carrier_rate,
# MAGIC     COALESCE(carrier_linehaul.carrier_linehaul, 0.0) AS carrier_linehaul,
# MAGIC     COALESCE(fuel_surcharge.fuel_surcharge, 0.0) AS carrier_fuel_surcharge
# MAGIC   FROM
# MAGIC     plan_combination_projection
# MAGIC     JOIN canonical_plan_projection ON plan_combination_projection.resulting_plan_id = canonical_plan_projection.plan_id
# MAGIC     LEFT JOIN booking_projection b ON canonical_plan_projection.relay_reference_number = b.relay_reference_number
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) one_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_fuel_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.charge_code = 'fuel_surcharge'
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) one_fuel_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS one_lhc_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_one = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.charge_code = 'linehaul'
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) one_lhc_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_lhc_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.charge_code = 'linehaul'
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) two_lhc_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_fuel_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.charge_code = 'fuel_surcharge'
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) two_fuel_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(moneying_billing_party_transaction.amount) / 100.00 AS two_money
# MAGIC       FROM
# MAGIC         moneying_billing_party_transaction
# MAGIC       WHERE
# MAGIC         plan_combination_projection.relay_reference_number_two = moneying_billing_party_transaction.relay_reference_number
# MAGIC         AND moneying_billing_party_transaction.`voided?` = false
# MAGIC     ) two_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(vendor_transaction_projection.amount) / 100.00 AS carrier_money
# MAGIC       FROM
# MAGIC         vendor_transaction_projection
# MAGIC       WHERE
# MAGIC         canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC         AND vendor_transaction_projection.`voided?` = false
# MAGIC     ) carrier_money ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(vendor_transaction_projection.amount) / 100.00 AS carrier_linehaul
# MAGIC       FROM
# MAGIC         vendor_transaction_projection
# MAGIC       WHERE
# MAGIC         canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC         AND vendor_transaction_projection.charge_code = 'linehaul'
# MAGIC         AND vendor_transaction_projection.`voided?` = false
# MAGIC     ) carrier_linehaul ON true
# MAGIC     LEFT JOIN LATERAL (
# MAGIC       SELECT
# MAGIC         SUM(vendor_transaction_projection.amount) / 100.00 AS fuel_surcharge
# MAGIC       FROM
# MAGIC         vendor_transaction_projection
# MAGIC       WHERE
# MAGIC         canonical_plan_projection.relay_reference_number = vendor_transaction_projection.relay_reference_number
# MAGIC         AND vendor_transaction_projection.charge_code = 'fuel_surcharge'
# MAGIC         AND vendor_transaction_projection.`voided?` = false
# MAGIC     ) fuel_surcharge ON true
# MAGIC   WHERE
# MAGIC     plan_combination_projection.is_combined = true
# MAGIC ),
# MAGIC invoicing_cred AS (
# MAGIC   SELECT
# MAGIC     DISTINCT invoicing_credits.relay_reference_number,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN invoicing_credits.currency = 'CAD' THEN invoicing_credits.total_amount * COALESCE(
# MAGIC           CASE
# MAGIC             WHEN canada_conversions.us_to_cad = 0 THEN NULL
# MAGIC             ELSE canada_conversions.us_to_cad
# MAGIC           END,
# MAGIC           average_conversions.avg_cad_to_us
# MAGIC         )
# MAGIC         ELSE invoicing_credits.total_amount
# MAGIC       END
# MAGIC     ) / 100.00 AS invoicing_cred,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN invoicing_credits.currency = 'USD' THEN invoicing_credits.total_amount * COALESCE(
# MAGIC            canada_conversions.conversion,
# MAGIC             average_conversions.avg_us_to_cad
# MAGIC         )
# MAGIC         ELSE invoicing_credits.total_amount
# MAGIC       END
# MAGIC     ) / 100.00 AS invoicing_cred_CAD
# MAGIC   FROM
# MAGIC     invoicing_credits
# MAGIC     LEFT JOIN canada_conversions ON DATE(invoicing_credits.credited_at) = canada_conversions.ship_date
# MAGIC     JOIN average_conversions ON TRUE
# MAGIC   GROUP BY
# MAGIC     invoicing_credits.relay_reference_number
# MAGIC ),
# MAGIC
# MAGIC invoice_start AS (
# MAGIC   SELECT
# MAGIC     DISTINCT relay_reference_number,
# MAGIC     'tl' AS mode,
# MAGIC     invoiced_at :: DATE,
# MAGIC     invoice_number,
# MAGIC     (
# MAGIC       accessorial_amount :: NUMERIC + fuel_surcharge_amount :: NUMERIC + linehaul_amount :: NUMERIC
# MAGIC     ) / 100.00 AS total_invoice_amt
# MAGIC   FROM
# MAGIC     tl_invoice_projection
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     DISTINCT relay_reference_number,
# MAGIC     'ltl',
# MAGIC     invoiced_at :: DATE,
# MAGIC     invoice_number,
# MAGIC     REPLACE(invoiced_amount, ',', '') :: NUMERIC
# MAGIC   FROM
# MAGIC     ltl_invoice_projection
# MAGIC   ORDER BY
# MAGIC     relay_reference_number,
# MAGIC     invoice_number
# MAGIC ),
# MAGIC basic_invoice_info AS (
# MAGIC   SELECT
# MAGIC     DISTINCT relay_reference_number,
# MAGIC     MAX(invoiced_at) AS first_invoiced,
# MAGIC     SUM(total_invoice_amt) AS total_amt_invoiced
# MAGIC   FROM
# MAGIC     invoice_start
# MAGIC   GROUP BY
# MAGIC     relay_reference_number
# MAGIC ),
# MAGIC raw_data_one AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN customer_name = 'TJX Companies c/o Blueyonder' THEN 'IL'
# MAGIC       ELSE COALESCE(
# MAGIC         relay_users.office_id,
# MAGIC         aljex_user_report_listing.pnl_code
# MAGIC       )
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     COALESCE(master_customer_name, customer_name) AS mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at
# MAGIC   FROM
# MAGIC     bronze.spot_quotes
# MAGIC     JOIN analytics.dim_date ON CAST(spot_quotes.created_at AS DATE) = dim_date.Calendar_Date
# MAGIC     LEFT JOIN bronze.relay_users ON spot_quotes.created_by = relay_users.full_name
# MAGIC     LEFT JOIN bronze.aljex_user_report_listing ON spot_quotes.created_by = aljex_user_report_listing.full_name
# MAGIC     LEFT JOIN bronze.customer_lookup ON LEFT(customer_name, 32) = LEFT(customer_lookup.aljex_customer_name, 32)
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC raw_data AS (
# MAGIC   SELECT
# MAGIC     CAST(created_at AS DATE) AS created_at,
# MAGIC     quote_id,
# MAGIC     created_by,
# MAGIC     CASE
# MAGIC       WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC       WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC       ELSE rep_office
# MAGIC     END AS rep_office,
# MAGIC     load_number,
# MAGIC     shipper_ref_number,
# MAGIC     1 AS counter,
# MAGIC     customer_name,
# MAGIC     mastername,
# MAGIC     quoted_price,
# MAGIC     margin,
# MAGIC     shipper_state,
# MAGIC     status_str,
# MAGIC     receiver_state,
# MAGIC     last_updated_at,
# MAGIC     rank() OVER (
# MAGIC       PARTITION BY Load_Number,
# MAGIC       status_str
# MAGIC       ORDER BY
# MAGIC         last_updated_at DESC
# MAGIC     ) AS rank
# MAGIC   FROM
# MAGIC     raw_data_one
# MAGIC     LEFT JOIN bronze.days_to_pay_offices ON CASE
# MAGIC       WHEN rep_office IN ('WA', 'OR') THEN 'POR'
# MAGIC       WHEN rep_office IN ('SC', 'SCR') THEN 'SEA'
# MAGIC       ELSE rep_office
# MAGIC     END = days_to_pay_offices.office
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC   ORDER BY
# MAGIC     created_at DESC
# MAGIC ),
# MAGIC -- Select * from raw_data
# MAGIC relay_spot_number AS (
# MAGIC   SELECT
# MAGIC     DISTINCT b.relay_reference_number,
# MAGIC     t.shipment_id,
# MAGIC     r.customer_name,
# MAGIC     master_customer_name,
# MAGIC     ncm.Customer_Final_Rate_USD AS revenue,
# MAGIC     NULL AS column_null,
# MAGIC     rd.created_at,
# MAGIC     rd.quote_id,
# MAGIC     1 AS counter,
# MAGIC     rd.quoted_price,
# MAGIC     rd.margin,
# MAGIC     rd.created_by,
# MAGIC     rd.rep_office,
# MAGIC     rd.load_number,
# MAGIC     rd.shipper_ref_number,
# MAGIC     rd.status_str,
# MAGIC     rd.last_updated_at
# MAGIC   FROM
# MAGIC     bronze.booking_projection AS b
# MAGIC     LEFT JOIN bronze.customer_profile_projection r ON b.tender_on_behalf_of_id = r.customer_slug
# MAGIC     LEFT JOIN bronze.customer_lookup AS cl ON LEFT(r.customer_name, 32) = LEFT(cl.aljex_customer_name, 32)
# MAGIC     LEFT JOIN bronze.tendering_acceptance AS t ON b.relay_reference_number = t.relay_reference_number
# MAGIC     JOIN raw_data AS rd ON CAST(b.relay_reference_number AS STRING) = rd.load_number
# MAGIC     AND master_customer_name = rd.mastername
# MAGIC     AND b.first_shipper_state = rd.shipper_state
# MAGIC     AND b.receiver_state = rd.receiver_state
# MAGIC     LEFT JOIN Silver.Silver_Relay_Customer_Money AS ncm ON b.relay_reference_number = ncm.relay_reference_number
# MAGIC   WHERE
# MAGIC     1 = 1
# MAGIC     and rd.status_str = 'WON'
# MAGIC     and rd.rank = 1
# MAGIC ),
# MAGIC
# MAGIC pricing_nfi_load_predictions as (
# MAGIC   select
# MAGIC     external_load_id as MR_id,
# MAGIC     max(target_buy_rate_gsn) * max(miles_predicted) as Market_Buy_Rate,
# MAGIC     1 as Markey_buy_rate_flag
# MAGIC   from
# MAGIC     bronze.pricing_nfi_load_predictions
# MAGIC   group by
# MAGIC     external_load_id
# MAGIC ),
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
# MAGIC
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
# MAGIC cell1 as (
# MAGIC 	  select
# MAGIC 		b.relay_reference_number,
# MAGIC 		tn.is_split,
# MAGIC 		tn.order_numbers,
# MAGIC 		tn.updated_at
# MAGIC 	  from
# MAGIC 		booking_projection b
# MAGIC 		  left join bronze.tender_reference_numbers_projection tn
# MAGIC 			on b.relay_reference_number = tn.relay_reference_number
# MAGIC 			AND tn.is_split = 'false'
# MAGIC 	),
# MAGIC 	rank as (
# MAGIC 	  select
# MAGIC 		cell1.*,
# MAGIC 		row_number() over (
# MAGIC 			partition by cell1.relay_reference_number
# MAGIC 			order by cell1.updated_at desc
# MAGIC 		  ) as rank
# MAGIC 	  from
# MAGIC 		cell1
# MAGIC 	),
# MAGIC 	tender_reference_numbers_projection as (
# MAGIC 	  select
# MAGIC 		*
# MAGIC 	  from
# MAGIC 		rank
# MAGIC 	  where
# MAGIC 		rank = 1
# MAGIC 	),
# MAGIC Final as (SELECT DISTINCT
# MAGIC     CONCAT("R", b.relay_reference_number) AS DW_Load_ID,
# MAGIC     
# MAGIC    'RELAY' AS TMS_Sourcesystem,
# MAGIC    b.relay_reference_number :: INT AS Load_Number,
# MAGIC   customer_profile_projection.customer_name AS Customer_Name,
# MAGIC   case
# MAGIC     when customer_lookup.master_customer_name is null then customer_profile_projection.customer_name
# MAGIC     else customer_lookup.master_customer_name
# MAGIC   end as Customer_Master,
# MAGIC   CASE WHEN new_office_lookup.new_office = 'TGT' THEN 'PHI' ELSE new_office_lookup.new_office end as Customer_New_Office,
# MAGIC   new_office_lookup.old_office as Customer_Old_Office,
# MAGIC   z.full_name as Customer_Rep,
# MAGIC   'TL' as Mode,
# MAGIC   case
# MAGIC     when canonical_plan_projection.mode = 'prtl' then 'Drayage'
# MAGIC     else case
# MAGIC       when canonical_plan_projection.mode = 'imdl' then 'Intermodal'
# MAGIC       else 'Dry Van'
# MAGIC     end
# MAGIC   end as Equipment,
# MAGIC   CASE
# MAGIC     WHEN combined_loads_mine.combine_new_rrn is null THEN "NO"
# MAGIC     WHEN combined_loads_mine.combine_new_rrn is not null then combined_loads_mine.combine_new_rrn :: STRING
# MAGIC     ELSE NULL
# MAGIC   END as Combined_Load,
# MAGIC   carr.old_office AS Carrier_Old_Office,
# MAGIC   CASE WHEN carr.new_office = 'TGT' THEN 'PHI' ELSE carr.new_office END AS Carrier_New_Office,
# MAGIC   b.booked_by_name as Carrier_Rep,
# MAGIC   sales.full_name as Sales_Rep,
# MAGIC   upper(b.status) as Load_Status,
# MAGIC   initcap(b.first_shipper_city) as Origin_City,
# MAGIC   upper(b.first_shipper_state) as Origin_State,
# MAGIC   b.first_shipper_zip as Origin_Zip,
# MAGIC   initcap(b.receiver_city) as Dest_City,
# MAGIC   upper(b.receiver_state) as Dest_State,
# MAGIC   b.receiver_zip as Dest_Zip,
# MAGIC   o.market as Market_Origin,
# MAGIC   d.market AS Market_Dest,
# MAGIC   CONCAT(o.market, '>', d.market) AS Market_Lane,
# MAGIC   master_dates.delivery_datetime :: DATE AS Delivery_Date,
# MAGIC   rolled_at :: date as Rolled_Date,
# MAGIC   ta.accepted_at :: string as Tendering_Date,
# MAGIC   master_dates.booked_at::date as Booked_Date,
# MAGIC   b.bounced_at :: date as Bounced_Date,
# MAGIC   b.bounced_at :: date + 2 as Cancelled_Date,
# MAGIC   master_dates.pu_appt_date::DATE as Pickup_Appointment_Date,
# MAGIC   master_dates.scheduled_date::DATE as Scheduled_Date,
# MAGIC   master_dates.pickup_in::DATE as Pickup_Start,
# MAGIC   master_dates.pickup_out::DATE as Pickup_End,
# MAGIC   master_dates.pickup_datetime::DATE AS Pickup_Datetime,
# MAGIC   case
# MAGIC     when master_dates.pickup_datetime :: date > COALESCE(
# MAGIC       master_dates.pu_appt_date :: date,
# MAGIC       master_dates.use_date :: date
# MAGIC     ) then 'Late'
# MAGIC     else 'OnTime'
# MAGIC   end as On_Time_Pickup,
# MAGIC   master_dates.delivery_in::DATE as Delivery_Start,
# MAGIC   master_dates.delivery_out::DATE as Delivery_End,
# MAGIC   master_dates.delivery_datetime::DATE as Delivery_Datetime,
# MAGIC   case
# MAGIC     when master_dates.delivery_datetime :: date > master_dates.del_appt_date :: date then 'late'
# MAGIC     else 'OnTime'
# MAGIC   end as On_Time_Delivery,
# MAGIC   CASE
# MAGIC     WHEN cust.new_office <> carr.new_office THEN 'Crossbooked'
# MAGIC     ELSE 'Same Office'
# MAGIC   END AS Booking_Type,
# MAGIC   carrier_projection.dot_number :: string as DOT_Number,
# MAGIC   
# MAGIC   b.booked_carrier_name as Carrier_Name,
# MAGIC   coalesce(
# MAGIC     tendering_planned_distance.planned_distance_amount :: float,
# MAGIC     b.total_miles :: float
# MAGIC   ) as Miles,
# MAGIC   Custm.Customer_Final_Rate_USD as Revenue_USD,
# MAGIC   Custm.Customer_Final_Rate_CAD as Revenue_CAD, 
# MAGIC   CarM.Carrier_Final_Rate_USD  :: float as Expense_USD,
# MAGIC   CarM.Carrier_Final_Rate_CAD::float as Expense_CAD,
# MAGIC   CASE
# MAGIC     WHEN combine_new_rrn is null THEN (
# MAGIC       
# MAGIC         coalesce(Custm.Customer_Final_Rate_USD, 0) - (coalesce(CarM.Carrier_Final_Rate_USD, 0)) :: float
# MAGIC     )
# MAGIC     ELSE (
# MAGIC       coalesce(Custm.Customer_Final_Rate_USD, 0) - (coalesce(
# MAGIC         CarM.Carrier_Final_Rate_USD:: float / 2,0))
# MAGIC     ) :: float
# MAGIC   END as Margin_USD,
# MAGIC   CASE
# MAGIC     WHEN combine_new_rrn is null THEN (
# MAGIC       coalesce(Custm.Customer_Final_Rate_CAD, 0) - (coalesce(CarM.Carrier_Final_Rate_CAD, 0)) :: float
# MAGIC     )
# MAGIC     ELSE (
# MAGIC       coalesce(Custm.Customer_Final_Rate_CAD, 0) - (coalesce(
# MAGIC         CarM.Carrier_Final_Rate_CAD:: float / 2,0))
# MAGIC     ) :: floaT
# MAGIC   END as Margin_CAD,
# MAGIC   CarM.Carrier_Linehaul_USD as Carrier_Linehaul_USD,
# MAGIC   CarM.Carrier_Linehaul_CAD as Carrier_Linehaul_CAD,
# MAGIC   CarM.Carrier_Final_Rate_USD as Carrier_Fuel_Surcharge_USD,
# MAGIC   CarM.Carrier_Final_Rate_CAD as Carrier_Fuel_Surcharge_CAD,
# MAGIC   CarM.Carrier_Accessorial_USD as Carrier_Other_Fees_USD,
# MAGIC   CarM.Carrier_Accessorial_CAD as Carrier_Other_Fees_CAD,
# MAGIC     Custm.Customer_Linehaul_USD,
# MAGIC     Custm.Customer_Linehaul_CAD,
# MAGIC     Custm.Customer_FuelSurcharge_USD,
# MAGIC     Custm.Customer_FuelSurcharge_CAD,
# MAGIC     Custm.Customer_Accessorial_USD as Customer_Other_Fees_USD,
# MAGIC     Custm.Customer_Accessorial_CAD as Customer_Other_Fees_CAD,
# MAGIC   t.Trailer_Number AS Trailer_Number,
# MAGIC   tn.order_numbers as PO_Number,
# MAGIC   b.receiver_name as Consignee_Name,
# MAGIC   CONCAT(
# MAGIC     re.address1,
# MAGIC     ' ,',
# MAGIC     re.city,
# MAGIC     ' ,',
# MAGIC     re.state_code,
# MAGIC     ' ,',
# MAGIC     re.zip_code
# MAGIC   ) AS Consignee_Delivery_Address,
# MAGIC   CONCAT(
# MAGIC     'Pickuplocation1: ',
# MAGIC     s1.address1,
# MAGIC     ' ,',
# MAGIC     s1.city,
# MAGIC     ' ,',
# MAGIC     s1.state_code,
# MAGIC     ' ,',
# MAGIC     s1.zip_code,
# MAGIC     '\n' 'Pickuplocation2: ',
# MAGIC     s2.address1,
# MAGIC     ' ,',
# MAGIC     s2.city,
# MAGIC     ' ,',
# MAGIC     s2.state_code,
# MAGIC     ' ,',
# MAGIC     s2.zip_code
# MAGIC   ) AS Consignee_Pickup_Address,
# MAGIC   carrier_projection.MC_Number AS MC_Number,
# MAGIC   sp.quoted_price as Spot_Revenue,
# MAGIC   sp.margin as Spot_Margin,
# MAGIC   mx.max_buy :: float / 100.00 as Max_buy_rate,
# MAGIC   mr.Market_Buy_Rate AS Market_Buy_Rate,
# MAGIC   customer_profile_projection.Lawson_ID,
# MAGIC   pickup_datetime :: DATE AS Ship_Date,
# MAGIC   basic_invoice_info.first_invoiced :: DATE AS Invoice_Date,
# MAGIC --   financial_calendar.financial_period_sorting,
# MAGIC   COALESCE(delivery_datetime :: DATE, pickup_datetime :: DATE) AS Use_Dates,
# MAGIC   Custm.Customer_Final_Rate_USD AS Invoice_Amount_USD,
# MAGIC   Custm.Load_Currency AS Load_Currency,
# MAGIC   Custm.Customer_Final_Rate_CAD AS Invoice_Amount_CAD,
# MAGIC   DATEDIFF(CURRENT_DATE, use_dates) AS Days_Aged_To_Invoice,
# MAGIC   CASE WHEN  basic_invoice_info.first_invoiced IS NULL THEN 
# MAGIC    (CASE 
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) <= 29 THEN '0 - 30 Days'
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 30 AND 59 THEN '30 - 60 Days' 
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 60 AND 89 THEN '60 - 90 Days' 
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 90 AND 119 THEN '90 - 120 Days' 
# MAGIC        WHEN DATEDIFF(CURRENT_DATE, use_dates) >= 120 THEN '120 + Days' 
# MAGIC    END ) 
# MAGIC    ELSE NULL END AS Unbilled_Aging_Bucket,
# MAGIC    CASE WHEN basic_invoice_info.first_invoiced IS NULL THEN "Unbilled" ELSE "Billed" END AS Billing_Status,
# MAGIC    Dim_date.Period_Year AS Financial_Period_Year,
# MAGIC    sac.AdminFees_Carrier_USD AS Admin_Fees_Carrier_USD,
# MAGIC    sac.AdminFees_Carrier_CAD as Admin_Fees_Carrier_CAD,
# MAGIC    sacc.AdminFees_Cust_USD as Admin_Fees_Customer_USD,
# MAGIC    sacc.AdminFees_Cust_CAD as Admin_Fees_Customer_CAD,
# MAGIC    b.booked_carrier_id as Carrier_ID,
# MAGIC    relay_users.user_ID AS Carrier_Rep_ID,
# MAGIC    b.tender_on_behalf_of_id AS Customer_ID,
# MAGIC    z.user_id as Customer_Rep_ID,
# MAGIC    datediff(try_cast(Ship_Date as date), try_cast(ta.accepted_at as date)) as Days_ahead,
# MAGIC    sac.DelandPickup_Carrier_CAD,
# MAGIC    sac.DelandPickup_Carrier_USD,
# MAGIC    sacc.DelandPickup_Cust_CAD,
# MAGIC    sacc.DelandPickup_Cust_USD,
# MAGIC    master_dates.del_appt_date as Delivery_Appointment_Date,
# MAGIC    sac.EquipmentandVehicle_Carrier_CAD,
# MAGIC sac.EquipmentandVehicle_Carrier_USD,
# MAGIC sacc.EquipmentandVehicle_Cust_CAD,
# MAGIC sacc.EquipmentandVehicle_Cust_USD,
# MAGIC mr.Market_Buy_Rate * COALESCE(canada_conversions.conversion, avg_us_to_cad) AS Market_Buy_Rate_CAD, 
# MAGIC case
# MAGIC       when carrier_NAME is not null then '1'
# MAGIC       else '0'
# MAGIC     end as Load_flag,
# MAGIC CONCAT(
# MAGIC       INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC     ) AS load_lane,
# MAGIC sac.Loadandunload_carrier_cad,
# MAGIC sac.Loadandunload_carrier_USD,
# MAGIC sacc.Loadandunload_Cust_CAD,
# MAGIC sacc.Loadandunload_Cust_USD,
# MAGIC Max_buy_rate * COALESCE(canada_conversions.conversion, avg_us_to_cad) AS Max_Buy_Rate_CAD,
# MAGIC sac.Miscellaneous_Carrier_CAD,
# MAGIC sac.Miscellaneous_Carrier_USD,
# MAGIC sacc.Miscellaneous_Cust_CAD,
# MAGIC sacc.Miscellaneous_Cust_USD,
# MAGIC sac.PermitsandCompliance_Carrier_CAD,
# MAGIC sac.PermitsandCompliance_Carrier_USD,
# MAGIC sacc.PermitsandCompliance_cust_CAD,
# MAGIC sacc.PermitsandCompliance_cust_USD,
# MAGIC CASE
# MAGIC       WHEN TRY_CAST(Booked_date AS DATE) < TRY_CAST(ship_date AS DATE) THEN 'PreBooked'
# MAGIC       ELSE 'SameDay'
# MAGIC     END AS PreBookStatus,
# MAGIC sales.user_id as Sales_Rep_ID,
# MAGIC tender_final.Tender_Source_Type,
# MAGIC sac.TransitandRouting_Carrier_CAD,
# MAGIC sac.TransitandRouting_Carrier_USD,
# MAGIC sacc.TransitandRouting_Cust_CAD,
# MAGIC sacc.TransitandRouting_Cust_USD 
# MAGIC FROM
# MAGIC   booking_projection b
# MAGIC   LEFT JOIN master_dates ON b.relay_reference_number = master_dates.relay_reference_number
# MAGIC   AND b.status = master_dates.status
# MAGIC   LEFT JOIN Silver.Silver_Relay_Customer_Money Custm ON b.relay_reference_number = Custm.relay_reference_number
# MAGIC   AND b.status = Custm.status
# MAGIC   LEFT JOIN Silver.Silver_Relay_Carrier_Money CarM ON b.relay_reference_number = CarM.relay_reference_number
# MAGIC   AND b.status = CarM.status
# MAGIC   and b.booking_id = CarM.booking_id
# MAGIC   LEFT JOIN basic_invoice_info ON b.relay_reference_number = basic_invoice_info.relay_reference_number
# MAGIC   LEFT JOIN financial_calendar ON COALESCE(delivery_datetime :: DATE, pickup_datetime :: DATE) :: DATE = financial_calendar.date :: DATE
# MAGIC   LEFT JOIN customer_profile_projection ON b.tender_on_behalf_of_id = customer_profile_projection.customer_slug
# MAGIC   LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC   LEFT JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC   LEFT JOIN combined_loads_mine ON b.relay_reference_number = combined_loads_mine.combine_new_rrn
# MAGIC   LEFT JOIN relay_users z on customer_profile_projection.primary_relay_user_id = z.user_id
# MAGIC   and z.`active?` = 'true' -- Customer Perspective
# MAGIC   LEFT JOIN new_office_lookup_w_tgt cust on customer_profile_projection.profit_center = cust.old_office -- Customer Perspective
# MAGIC   LEFT JOIN relay_users on upper(b.booked_by_name) = upper(relay_users.full_name)
# MAGIC   AND relay_users.`active?` = 'true' -- Carrier Perspective
# MAGIC   LEFT JOIN bronze.new_office_lookup_w_tgt carr on relay_users.office_id = carr.old_office -- Carrier Perspective
# MAGIC   LEFT JOIN bronze.canonical_plan_projection on b.relay_reference_number = canonical_plan_projection.relay_reference_number -- Mode
# MAGIC   LEFT JOIN relay_users sales on customer_profile_projection.sales_relay_user_id = sales.user_id
# MAGIC   and sales.`active?` = 'true' --Sales Perspective
# MAGIC   LEFT JOIN bronze.market_lookup o ON LEFT(first_shipper_zip, 3) = o.pickup_zip -- Market Origin
# MAGIC   LEFT JOIN bronze.market_lookup d ON LEFT(receiver_zip, 3) = d.pickup_zip -- Market Destination
# MAGIC   LEFT JOIN bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number -- Tender Details
# MAGIC   LEFT JOIN bronze.carrier_projection on b.booked_carrier_id = carrier_projection.carrier_id -- Carrier Details
# MAGIC   LEFT JOIN bronze.tendering_planned_distance on b.relay_reference_number = tendering_planned_distance.relay_reference_number -- Miles
# MAGIC   LEFT JOIN truckload_projection t on b.booking_id = t.booking_id
# MAGIC   and t.status != 'CancelledOrBounced'
# MAGIC   and t.trailer_number is not null
# MAGIC   and lower(b.status) != 'bounced' -- Trailer_Number
# MAGIC   LEFT JOIN tender_reference_numbers_projection tn on b.relay_reference_number = tn.relay_reference_number --PO Number
# MAGIC   LEFT JOIN receivers re on b.receiver_id = re.uuid -- Consignee Address
# MAGIC   LEFT JOIN bronze.shippers s1 on b.first_shipper_id = s1.uuid -- Shipper Addres
# MAGIC   LEFT JOIN bronze.shippers s2 on b.second_shipper_id = s2.uuid
# MAGIC   LEFT JOIN relay_spot_number sp on b.relay_reference_number = sp.relay_reference_number -- Spot Details
# MAGIC   LEFT JOIN bronze.sourcing_max_buy_v2 mx on b.relay_reference_number = mx.relay_reference_number
# MAGIC   LEFT JOIN pricing_nfi_load_predictions mr on b.relay_reference_number = mr.MR_id
# MAGIC   LEFT JOIN analytics.dim_date ON  pickup_datetime :: DATE = dim_date.Calendar_Date::DATE
# MAGIC   LEFT JOIN superinsight.accessorial_carriers sac on b.relay_reference_number = sac.load_number and sac.tms = 'RELAY'
# MAGIC   LEFT JOIN superinsight.accessorial_customers sacc on b.relay_reference_number = sacc.load_number  and sacc.tms = 'RELAY'
# MAGIC   LEFT JOIN canada_conversions ON pickup_datetime::DATE = canada_conversions.ship_date::DATE
# MAGIC   LEFT JOIN average_conversions ON 1=1
# MAGIC   left join tender_final on b.relay_reference_number = tender_final.id
# MAGIC   WHERE
# MAGIC   b.status NOT IN ('cancelled', "bounced")
# MAGIC   AND b.Is_Deleted = 0
# MAGIC   AND canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC         CONCAT("R", big_export_projection.load_number::INT),
# MAGIC         'RELAY',
# MAGIC         big_export_projection.load_number::INT,
# MAGIC         CASE 
# MAGIC             WHEN big_export_projection.customer_id = 'hain' THEN 'Hain Celestial' 
# MAGIC             WHEN big_export_projection.customer_id = 'roland' THEN 'Roland Corporation' 
# MAGIC             ELSE customer_profile_projection.customer_name 
# MAGIC         END,
# MAGIC        case
# MAGIC           WHEN big_export_projection.customer_id = 'hain' THEN 'Hain Celestial' 
# MAGIC           WHEN big_export_projection.customer_id = 'roland' THEN 'Roland Corporation'
# MAGIC           when customer_lookup.master_customer_name is null then customer_profile_projection.customer_name   
# MAGIC           else customer_lookup.master_customer_name
# MAGIC         end as mastername,
# MAGIC         CASE 
# MAGIC             WHEN big_export_projection.customer_id = 'hain' THEN 'PHI'
# MAGIC             WHEN big_export_projection.customer_id = 'roland' THEN 'LTL'
# MAGIC             WHEN big_export_projection.customer_id = 'deb' THEN 'PHI'
# MAGIC             WHEN new_office_lookup.new_office  = 'TGT' THEN 'PHI'
# MAGIC             ELSE new_office_lookup.new_office
# MAGIC         END AS Customer_New_Office,
# MAGIC         CASE 
# MAGIC             WHEN big_export_projection.customer_id = 'hain' THEN 'PHI'
# MAGIC             WHEN big_export_projection.customer_id = 'roland' THEN 'LTL'
# MAGIC             WHEN big_export_projection.customer_id = 'deb' THEN 'PHI' 
# MAGIC             ELSE new_office_lookup.old_office END AS Customer_Old_Office,
# MAGIC         z.full_name as customer_rep,
# MAGIC         'LTL' as mode,
# MAGIC          case
# MAGIC            when canonical_plan_projection.mode = 'prtl' then 'Drayage'
# MAGIC            else
# MAGIC                 case
# MAGIC                 when canonical_plan_projection.mode = 'imdl' then 'Intermodal'
# MAGIC                 else 'Dry Van'
# MAGIC                 end
# MAGIC             end as Equipment,
# MAGIC         'NO' as combined_load,
# MAGIC         "LTL" as Carrier_Old_Office,
# MAGIC         "LTL" AS Carrier_New_Office,
# MAGIC         'ltl_team' as Carrier_Rep,
# MAGIC         sales.full_name as salesrep,
# MAGIC         UPPER(big_export_projection.dispatch_status) AS Load_Status,
# MAGIC         initcap(big_export_projection.pickup_city) as origin_city,
# MAGIC         upper(big_export_projection.pickup_state) as origin_state,
# MAGIC         big_export_projection.pickup_zip as Pickup_Zip,
# MAGIC         initcap(big_export_projection.consignee_city) as dest_city,
# MAGIC         upper(big_export_projection.consignee_state) as dest_state,
# MAGIC         big_export_projection.consignee_zip as Consignee_Zip,
# MAGIC         o.market as market_origin,
# MAGIC         d.market AS market_dest,
# MAGIC         CONCAT(o.market, '>', d.market) AS market_lane,
# MAGIC         master_dates.delivery_datetime :: DATE AS Delivery_Date,
# MAGIC         NULL AS rolled_at,
# MAGIC         NULL AS Tendering_Date,
# MAGIC         NULL AS Booked_Date,
# MAGIC         NULL AS Bounced_Date,
# MAGIC         NULL AS Cancelled_Date,
# MAGIC         master_dates.pu_appt_date::DATE as Pickup_Appointment_Date,
# MAGIC         master_dates.scheduled_date ::DATE as Scheduled_Date,
# MAGIC         master_dates.pickup_in::DATE as Pickup_Start,
# MAGIC         master_dates.pickup_out::DATE as Pickup_End,
# MAGIC         master_dates.pickup_datetime::DATE AS Pickup_Datetime,
# MAGIC         case
# MAGIC             when master_dates.pickup_datetime :: date > COALESCE(
# MAGIC               master_dates.pu_appt_date :: date,
# MAGIC               master_dates.use_date :: date
# MAGIC             ) then 'Late'
# MAGIC             else 'OnTime'
# MAGIC           end as On_Time_Pickup,
# MAGIC           master_dates.delivery_in::DATE as Delivery_Start,
# MAGIC           master_dates.delivery_out::DATE as Delivery_End,
# MAGIC           master_dates.delivery_datetime::DATE as Delivery_Datetime,
# MAGIC           case
# MAGIC             when master_dates.delivery_datetime :: date > master_dates.del_appt_date :: date then 'Late'
# MAGIC             else 'OnTime'
# MAGIC           end as On_Time_Delivery,
# MAGIC           CASE
# MAGIC             WHEN new_office_lookup.new_office <> 'LTL' THEN 'Crossbooked'
# MAGIC             ELSE 'Same Office'
# MAGIC           END AS Booking_type,
# MAGIC           projection_carrier.dot_num::string AS DOT_NUM,
# MAGIC           big_export_projection.carrier_name AS Carrier_name,
# MAGIC           big_export_projection.miles::float AS Miles,
# MAGIC           CustM.Customer_Final_Rate_USD as Revenue_USD,
# MAGIC           CustM.Customer_Final_Rate_CAD as Revenue_CAD,
# MAGIC           (projected_expense::float / 100.0) as Expense_USD,
# MAGIC           (projected_expense::float / 100.0) * COALESCE(canada_conversions.conversion, avg_us_to_cad) as Expense_CAD,
# MAGIC         
# MAGIC               (CustM.Customer_Final_Rate_USD::float
# MAGIC               - coalesce(projected_expense::float / 100.0, 0)::float
# MAGIC             ) as Margin_USD,
# MAGIC           (
# MAGIC               (CustM.Customer_Final_Rate_CAD)::float
# MAGIC               - Expense_CAD::float
# MAGIC             ) AS Margin_CAD,
# MAGIC           CarM.Carrier_Linehaul_USD as Carrier_Linehaul_USD,
# MAGIC           CarM.Carrier_Linehaul_CAD as Carrier_Linehaul_CAD,
# MAGIC           CarM.Carrier_FuelSurcharge_USD as Carrier_Fuel_Surcharge_USD,
# MAGIC           CarM.Carrier_FuelSurcharge_CAD as Carrier_Fuel_Surcharge_CAD,
# MAGIC           CarM.Carrier_Accessorial_USD as Carrier_Other_Fees_USD,
# MAGIC           CarM.Carrier_Accessorial_CAD as Carrier_Other_Fees_CAD,
# MAGIC           CustM.Customer_Linehaul_USD AS Customer_Linehaul_USD,
# MAGIC           CustM.Customer_Linehaul_CAD AS Customer_Linehaul_CAD,
# MAGIC           CustM.Customer_FuelSurcharge_USD AS Customer_Fuel_Surcharge_USD,
# MAGIC           CustM.Customer_FuelSurcharge_CAD AS Customer_Fuel_Surcharge_CAD,
# MAGIC           CustM.Customer_Accessorial_USD AS Customer_Other_Fees_USD,
# MAGIC           CustM.Customer_Accessorial_CAD AS Customer_Other_Fees_CAD,
# MAGIC           t.Trailer_Number AS Trailer_Number,
# MAGIC           tn.order_numbers as PO_Number,
# MAGIC           big_export_projection.Consignee_Name,
# MAGIC           CONCAT(
# MAGIC               dpss.address_1,
# MAGIC               CASE
# MAGIC                 WHEN
# MAGIC                   dpss.address_2 IS NOT NULL
# MAGIC                   AND dpss.address_2 <> ''
# MAGIC                 THEN
# MAGIC                   CONCAT(' ,', dpss.address_2)
# MAGIC                 ELSE ''
# MAGIC               END,
# MAGIC               ' ,',
# MAGIC               dpss.locality,
# MAGIC               ' ,',
# MAGIC               dpss.administrative_region,
# MAGIC               ' ,',
# MAGIC               dpss.postal_code
# MAGIC             ) AS Delivery_Consignee_Address,
# MAGIC             CONCAT(
# MAGIC               pss.address_1,
# MAGIC               ' ,',
# MAGIC               pss.address_2,
# MAGIC               ' ,',
# MAGIC               pss.locality,
# MAGIC               ' ,',
# MAGIC               pss.administrative_region,
# MAGIC               ' ,',
# MAGIC               pss.postal_code
# MAGIC             ) AS pickup_Consignee_Address,
# MAGIC         projection_carrier.mc_num as Mc_Number,
# MAGIC         sp.quoted_price as Spot_Revenue,
# MAGIC         sp.margin as Spot_Margin,
# MAGIC         mx.max_buy::float / 100.00 as Max_buy_rate,
# MAGIC         mr.Market_Buy_Rate,
# MAGIC         CASE 
# MAGIC             WHEN big_export_projection.customer_id = 'hain' THEN '880177713'
# MAGIC             WHEN big_export_projection.customer_id = 'roland' THEN '880181037'
# MAGIC             WHEN big_export_projection.customer_id = 'deb' THEN '880171761' 
# MAGIC             ELSE customer_profile_projection.Lawson_ID END AS Lawson_ID,
# MAGIC         big_export_projection.ship_date::DATE,
# MAGIC         basic_invoice_info.first_invoiced::DATE,
# MAGIC         -- financial_calendar.financial_period_sorting,
# MAGIC         -- delivery_datetime::DATE,
# MAGIC         COALESCE(delivery_datetime::DATE, big_export_projection.ship_date::DATE) AS use_dates,
# MAGIC         CustM.Customer_Final_Rate_USD AS Unbilled_Amount,
# MAGIC         CustM.Load_currency AS Load_Currency,
# MAGIC         CustM.Customer_Final_Rate_CAD AS CAD_Amount,
# MAGIC           DATEDIFF(CURRENT_DATE, use_dates) AS Days_Aged,
# MAGIC           CASE WHEN  basic_invoice_info.first_invoiced IS NULL THEN 
# MAGIC           (CASE 
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) <= 29 THEN '0 - 30 Days'
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 30 AND 59 THEN '30 - 60 Days' 
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 60 AND 89 THEN '60 - 90 Days' 
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) BETWEEN 90 AND 119 THEN '90 - 120 Days' 
# MAGIC               WHEN DATEDIFF(CURRENT_DATE, use_dates) >= 120 THEN '120 + Days' 
# MAGIC           END ) 
# MAGIC           ELSE NULL END AS Unbilled_Aging_Bucket,
# MAGIC           CASE WHEN basic_invoice_info.first_invoiced::DATE IS NULL THEN "Unbilled" ELSE "Billed" END AS Billing_Status,
# MAGIC           dim_date.period_year as Financial_Period_Year,
# MAGIC           sac.AdminFees_Carrier_USD AS Admin_Fees_Carrier_USD,
# MAGIC    sac.AdminFees_Carrier_CAD as Admin_Fees_Carrier_CAD,
# MAGIC    sacc.AdminFees_Cust_USD as Admin_Fees_Customer_USD,
# MAGIC    sacc.AdminFees_Cust_CAD as Admin_Fees_Customer_CAD,
# MAGIC    big_export_projection.carrier_id as Carrier_ID,
# MAGIC    NULL AS Carrier_Rep_ID,
# MAGIC    big_export_projection.Customer_ID AS Customer_ID,
# MAGIC    z.user_id as Customer_Rep_ID,
# MAGIC    datediff(try_cast(master_dates.use_date as date), try_cast(Tendering_Date as date)) as Days_ahead,
# MAGIC    sac.DelandPickup_Carrier_CAD,
# MAGIC    sac.DelandPickup_Carrier_USD,
# MAGIC    sacc.DelandPickup_Cust_CAD,
# MAGIC    sacc.DelandPickup_Cust_USD,
# MAGIC    master_dates.del_appt_date as Delivery_Appointment_Date,
# MAGIC    sac.EquipmentandVehicle_Carrier_CAD,
# MAGIC     sac.EquipmentandVehicle_Carrier_USD,
# MAGIC     sacc.EquipmentandVehicle_Cust_CAD,
# MAGIC     sacc.EquipmentandVehicle_Cust_USD,
# MAGIC     mr.Market_Buy_Rate * COALESCE(canada_conversions.conversion, avg_us_to_cad) AS Market_Buy_Rate_CAD, 
# MAGIC     case
# MAGIC         when carrier_NAME is not null then '1'
# MAGIC         else '0'
# MAGIC         end as Load_flag,
# MAGIC     CONCAT(
# MAGIC         INITCAP(origin_city), ',', origin_state, '>', INITCAP(dest_city), ',', dest_state
# MAGIC         ) AS load_lane,
# MAGIC     sac.Loadandunload_carrier_cad,
# MAGIC     sac.Loadandunload_carrier_USD,
# MAGIC     sacc.Loadandunload_Cust_CAD,
# MAGIC     sacc.Loadandunload_Cust_USD,
# MAGIC     Max_buy_rate * COALESCE(canada_conversions.conversion, avg_us_to_cad) AS Max_Buy_Rate_CAD,
# MAGIC     sac.Miscellaneous_Carrier_CAD,
# MAGIC     sac.Miscellaneous_Carrier_USD,
# MAGIC     sacc.Miscellaneous_Cust_CAD,
# MAGIC     sacc.Miscellaneous_Cust_USD,
# MAGIC     sac.PermitsandCompliance_Carrier_CAD,
# MAGIC     sac.PermitsandCompliance_Carrier_USD,
# MAGIC     sacc.PermitsandCompliance_cust_CAD,
# MAGIC     sacc.PermitsandCompliance_cust_USD,
# MAGIC     CASE
# MAGIC         WHEN TRY_CAST(Booked_date AS DATE) < TRY_CAST(master_dates.use_date AS DATE) THEN 'PreBooked'
# MAGIC         ELSE 'SameDay'
# MAGIC         END AS PreBookStatus,
# MAGIC     sales.user_id as Sales_Rep_ID,
# MAGIC     tender_final.Tender_Source_Type,
# MAGIC     sac.TransitandRouting_Carrier_CAD,
# MAGIC     sac.TransitandRouting_Carrier_USD,
# MAGIC     sacc.TransitandRouting_Cust_CAD,
# MAGIC     sacc.TransitandRouting_Cust_USD 
# MAGIC     FROM big_export_projection
# MAGIC     LEFT JOIN master_dates ON big_export_projection.load_number = master_dates.booking_id AND big_export_projection.dispatch_status = master_dates.status 
# MAGIC     LEFT JOIN Silver.Silver_Relay_Customer_Money CustM ON big_export_projection.load_number = CustM.relay_reference_number AND big_export_projection.dispatch_status = CustM.status 
# MAGIC     LEFT JOIN financial_calendar ON COALESCE(delivery_datetime::DATE, big_export_projection.ship_date::DATE)::DATE = financial_calendar.date::DATE 
# MAGIC     LEFT JOIN customer_profile_projection ON big_export_projection.customer_id = customer_profile_projection.customer_slug 
# MAGIC     -- LEFT JOIN min_invoice_date ON big_export_projection.load_number::FLOAT = min_invoice_date.booking_id::FLOAT
# MAGIC     LEFT JOIN basic_invoice_info ON big_export_projection.load_number::FLOAT = basic_invoice_info.relay_reference_number
# MAGIC     LEFT JOIN new_office_lookup ON customer_profile_projection.profit_center = new_office_lookup.old_office
# MAGIC     LEFT JOIN customer_lookup ON LEFT(customer_profile_projection.customer_name, 32) = LEFT(aljex_customer_name, 32)
# MAGIC     LEFT JOIN bronze.relay_users z on customer_profile_projection.primary_relay_user_id = z.user_id and z.`active?` = 'true' -- Customer Pespective
# MAGIC     LEFT JOIN relay_users sales on customer_profile_projection.sales_relay_user_id = sales.user_id and sales.`active?` = 'true' -- Sales Perspective
# MAGIC     LEFT JOIN bronze.canonical_plan_projection on big_export_projection.load_number = canonical_plan_projection.relay_reference_number --Equipment
# MAGIC     LEFT JOIN bronze.market_lookup o ON LEFT(big_export_projection.pickup_zip, 3) = o.pickup_zip -- Market Origin
# MAGIC     LEFT JOIN bronze.market_lookup d ON LEFT(big_export_projection.consignee_zip, 3) = d.pickup_zip -- Market Dest
# MAGIC     LEFT JOIN bronze.projection_carrier on big_export_projection.carrier_id = projection_carrier.id -- Carrier_Details
# MAGIC     LEFT JOIN Silver.Silver_Relay_Carrier_Money CarM on big_export_projection.load_number = CarM.relay_reference_number -- Projection _expense of Carrier
# MAGIC     LEFT JOIN tender_reference_numbers_projection tn on big_export_projection.load_number = tn.relay_reference_number
# MAGIC     LEFT JOIN truckload_projection t  on big_export_projection.load_number = t.relay_reference_number and t.status != 'CancelledOrBounced' and t.trailer_number is not null
# MAGIC     LEFT JOIN last_delivery ON big_export_projection.load_number = last_delivery.relay_reference_number
# MAGIC     LEFT JOIN bronze.planning_stop_schedule dpss
# MAGIC         ON big_export_projection.load_number = dpss.relay_reference_number
# MAGIC         AND big_export_projection.consignee_name::string = dpss.stop_name::string
# MAGIC         AND big_export_projection.consignee_city::string = dpss.locality::string
# MAGIC         AND last_delivery.last_delivery = dpss.sequence_number
# MAGIC         AND dpss.stop_type::string = 'delivery'::string
# MAGIC         AND dpss.`removed?` = false
# MAGIC     LEFT JOIN bronze.planning_stop_schedule pss
# MAGIC             on (
# MAGIC               big_export_projection.load_number = pss.relay_reference_number
# MAGIC               AND big_export_projection.pickup_name = pss.stop_name
# MAGIC               AND pss.sequence_number = 1
# MAGIC               AND pss.`removed?` = 'false'
# MAGIC             )
# MAGIC     LEFT JOIN relay_spot_number sp  on big_export_projection.load_number = sp.relay_reference_number
# MAGIC     LEFT JOIN bronze.sourcing_max_buy_v2 mx on big_export_projection.Load_Number = mx.relay_reference_number 
# MAGIC     LEFT JOIN pricing_nfi_load_predictions mr on big_export_projection.Load_Number = mr.MR_id
# MAGIC     LEFT JOIN analytics.dim_date ON big_export_projection.ship_date::DATE = dim_date.calendar_date::DATE
# MAGIC     LEFT JOIN canada_conversions ON big_export_projection.ship_date::DATE = canada_conversions.ship_date::DATE
# MAGIC     LEFT JOIN average_conversions ON 1=1
# MAGIC     LEFT JOIN superinsight.accessorial_carriers sac on big_export_projection.load_number = sac.load_number and sac.tms = 'RELAY'
# MAGIC     LEFT JOIN superinsight.accessorial_customers sacc on big_export_projection.load_number = sacc.load_number and sacc.tms = 'RELAY'
# MAGIC     left join tender_final on big_export_projection.load_number = tender_final.id
# MAGIC
# MAGIC     WHERE 
# MAGIC       -- COALESCE(basic_invoice_info.first_invoiced::DATE, min_invoice_date.min_invoice_date::DATE) IS NULL
# MAGIC       --   AND 
# MAGIC       big_export_projection.carrier_name <> NULL AND
# MAGIC       big_export_projection.load_number != '2352492'
# MAGIC       AND dispatch_status NOT IN ('Cancelled')
# MAGIC       AND big_export_projection.Is_Deleted = 0
# MAGIC       AND canonical_plan_projection.status not in ('cancelled', 'voided', 'held')
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC Relay AS 
# MAGIC (SELECT
# MAGIC   DW_Load_ID,
# MAGIC   Load_Number AS Load_ID,
# MAGIC   Admin_Fees_Carrier_USD AS Admin_Fees_Carrier_USD,
# MAGIC   Admin_Fees_Carrier_CAD AS Admin_Fees_Carrier_CAD,
# MAGIC   Admin_Fees_Customer_USD AS Admin_Fees_Customer_USD,
# MAGIC   Admin_Fees_Customer_CAD AS Admin_Fees_Customer_CAD,
# MAGIC   Billing_Status AS Billing_Status,
# MAGIC   Delivery_Date AS Actual_Delivered_Date,
# MAGIC   Booking_Type AS Booking_Type,
# MAGIC   Booked_Date AS Booked_Date,
# MAGIC   Carrier_Fuel_Surcharge_CAD AS Carrier_Fuel_Surcharge_CAD,
# MAGIC   Carrier_Fuel_Surcharge_USD AS Carrier_Fuel_Surcharge_USD,
# MAGIC   Carrier_Linehaul_CAD AS Carrier_Linehaul_CAD,
# MAGIC   Carrier_Linehaul_USD AS Carrier_Linehaul_USD,
# MAGIC   Carrier_ID AS Carrier_ID,
# MAGIC   Carrier_Name AS Carrier_Name,
# MAGIC   Carrier_New_Office AS Carrier_New_Office,
# MAGIC   Carrier_Old_Office AS Carrier_Old_Office,
# MAGIC   Carrier_Other_Fees_CAD AS Carrier_Other_Fees_CAD,
# MAGIC   Carrier_Other_Fees_USD AS Carrier_Other_Fees_USD,
# MAGIC   Carrier_Rep_ID AS Carrier_Rep_ID,
# MAGIC   Carrier_Rep AS Carrier_Rep,
# MAGIC   Combined_Load AS Combined_Load,
# MAGIC   Consignee_Delivery_Address AS Consignee_Delivery_Address,
# MAGIC   Consignee_Name AS Consignee_Name,
# MAGIC   Consignee_Pickup_Address AS Consignee_Pickup_Address,
# MAGIC   Customer_FuelSurcharge_USD AS Customer_Fuel_Surcharge_USD,
# MAGIC   Customer_FuelSurcharge_CAD AS Customer_Fule_Surcharge_CAD,
# MAGIC   Customer_Linehaul_CAD AS Customer_Linehaul_CAD,
# MAGIC   Customer_Linehaul_USD AS Customer_Linehaul_USD,
# MAGIC   Customer_ID AS Customer_ID,
# MAGIC   Customer_Master AS Customer_Master,
# MAGIC   Customer_Name AS Customer_Name,
# MAGIC   Customer_New_Office AS Customer_New_Office,
# MAGIC   Customer_Old_Office AS Customer_Old_Office,
# MAGIC   Customer_Other_Fees_CAD AS Customer_Other_Fees_CAD,
# MAGIC   Customer_Other_Fees_USD AS Customer_Other_Fees_USD,
# MAGIC   Customer_Rep_ID AS Customer_Rep_ID,
# MAGIC   Customer_Rep AS Customer_Rep,
# MAGIC   Days_ahead AS Days_Ahead,
# MAGIC   Bounced_Date AS Bounced_Date,
# MAGIC   Cancelled_Date AS Cancelled_Date,
# MAGIC   DelandPickup_Carrier_CAD AS Delivery_Pickup_Charges_Carrier_CAD,
# MAGIC   DelandPickup_Carrier_USD AS Delivery_Pickup_Charges_Carrier_USD,
# MAGIC   DelandPickup_Cust_CAD AS Delivery_Pickup_Charges_Customer_CAD,
# MAGIC   DelandPickup_Cust_USD AS Delivery_Pickup_Charges_Customer_USD,
# MAGIC   Delivery_Appointment_Date AS Delivery_Appointment_Date,
# MAGIC   Delivery_End AS Delivery_EndDate,
# MAGIC   Dest_City AS Destination_City,
# MAGIC   Dest_State AS Destination_State,
# MAGIC   Dest_Zip AS Destination_Zip,
# MAGIC   DOT_Number AS DOT_Number,
# MAGIC   Delivery_Start AS Delivery_StartDate,
# MAGIC   Equipment AS Equipment,
# MAGIC   EquipmentandVehicle_Carrier_CAD AS Equipment_Vehicle_Charges_Carrier_CAD,
# MAGIC   EquipmentandVehicle_Carrier_USD AS Equipment_Vehicle_Charges_Carrier_USD,
# MAGIC   EquipmentandVehicle_Cust_CAD AS Equipment_Vehicle_Charges_Customer_CAD,
# MAGIC   EquipmentandVehicle_Cust_USD AS Equipment_Vehicle_Charges_Customer_USD,
# MAGIC   Expense_CAD AS Expense_CAD,
# MAGIC   Expense_USD AS Expense_USD,
# MAGIC   Financial_Period_Year AS Financial_Period_Year,
# MAGIC   Max_Buy_Rate_CAD AS GreenScreen_Rate_CAD,
# MAGIC   Max_Buy_Rate AS GreenScreen_Rate_USD,
# MAGIC   Invoice_Amount_CAD AS Invoice_Amount_CAD,
# MAGIC   Invoice_Amount_USD AS Invoice_Amount_USD,
# MAGIC   Invoice_Date AS Invoiced_Date,
# MAGIC   Lawson_ID AS Lawson_ID,
# MAGIC   Load_Currency AS Load_Currency,
# MAGIC   Load_flag AS Load_Flag,
# MAGIC   load_lane AS Load_Lane,
# MAGIC   Load_Status AS Load_Status,
# MAGIC   Loadandunload_carrier_cad AS Load_Unload_Charges_Carrier_CAD,
# MAGIC   Loadandunload_carrier_USD AS Load_Unload_Charges_Carrier_USD,
# MAGIC   Loadandunload_Cust_CAD AS Load_Unload_Charges_Customer_CAD,
# MAGIC   Loadandunload_Cust_CAD AS Load_Unload_Charges_Customer_USD,
# MAGIC   Margin_CAD AS Margin_CAD,
# MAGIC   Margin_USD AS Margin_USD,
# MAGIC   Market_Buy_Rate_CAD AS Market_Buy_Rate_CAD,
# MAGIC   Market_Buy_Rate AS Market_Buy_Rate_USD,
# MAGIC   Market_Dest AS Market_Destination,
# MAGIC   Market_Lane AS Market_Lane,
# MAGIC   Market_Origin AS Market_Origin,
# MAGIC   Max_buy_rate AS MC_Number,
# MAGIC   Miles AS Miles,
# MAGIC   Miscellaneous_Carrier_CAD AS Miscellaneous_Chargers_Carrier_CAD,
# MAGIC   Miscellaneous_Carrier_USD AS Miscellaneous_Chargers_Carrier_USD,
# MAGIC   Miscellaneous_Cust_CAD AS Miscellaneous_Chargers_Customers_CAD,
# MAGIC   Miscellaneous_Cust_USD AS Miscellaneous_Chargers_Customers_USD,
# MAGIC   Mode AS Mode,
# MAGIC   On_Time_Delivery AS On_Time_Delivery,
# MAGIC   On_Time_Pickup AS On_Time_Pickup,
# MAGIC   Origin_City AS Origin_City,
# MAGIC   Origin_State AS Origin_State,
# MAGIC   Origin_Zip AS Origin_Zip,
# MAGIC   PermitsandCompliance_Carrier_CAD AS Permits_Compliance_Charges_Carrier_CAD,
# MAGIC   PermitsandCompliance_Carrier_USD AS Permits_Compliance_Charges_Carrier_USD,
# MAGIC   PermitsandCompliance_cust_CAD AS Permits_Compliance_Charges_Customer_CAD,
# MAGIC   PermitsandCompliance_cust_USD AS Permits_Compliance_Charges_Customer_USD,
# MAGIC   Pickup_Appointment_Date AS Pickup_Appointment_Date,
# MAGIC   Pickup_Datetime AS Actual_Pickup_Date,
# MAGIC   Pickup_End AS Pickup_EndDate,
# MAGIC   Pickup_Start AS Pickup_StartDate,
# MAGIC   PO_Number AS PO_Number,
# MAGIC   PreBookStatus AS PreBookStatus,
# MAGIC   Revenue_CAD AS Revenue_CAD,
# MAGIC   Revenue_USD AS Revenue_USD,
# MAGIC   Rolled_Date AS Rolled_Date,
# MAGIC   Sales_Rep_ID AS Sales_Rep_ID,
# MAGIC   Sales_Rep AS Sales_Rep,
# MAGIC   Ship_Date AS Ship_Date,
# MAGIC   Spot_Margin AS Spot_Margin,
# MAGIC   Spot_Revenue AS Spot_Revenue,
# MAGIC   Tender_Source_Type AS Tender_Source_Type,
# MAGIC   MAX(Tendering_Date) AS Tendered_Date,
# MAGIC   TMS_Sourcesystem AS TMS_System,
# MAGIC   Trailer_Number AS Trailer_Number,
# MAGIC   TransitandRouting_Carrier_CAD AS Transit_Routing_Chargers_Carrier_CAD,
# MAGIC   TransitandRouting_Carrier_USD AS Transit_Routing_Chargers_Carrier_USD,
# MAGIC   TransitandRouting_Cust_CAD AS Transit_Routing_Chargers_Customer_CAD,
# MAGIC   TransitandRouting_Cust_USD AS Transit_Routing_Chargers_Customer_USD,
# MAGIC   Days_Aged_To_Invoice AS Week_Number,
# MAGIC   Market_Buy_Rate_USD AS Carrier_Final_Rate_USD,
# MAGIC   Market_Buy_Rate_CAD AS Carrier_Final_Rate_CAD,
# MAGIC SHA1(CONCAT(
# MAGIC   COALESCE(Admin_Fees_Carrier_USD::string, ''),
# MAGIC   COALESCE(Admin_Fees_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Admin_Fees_Customer_USD::string, ''),
# MAGIC   COALESCE(Admin_Fees_Customer_CAD::string, ''),
# MAGIC   COALESCE(Billing_Status::string, ''),
# MAGIC   COALESCE(Actual_Delivered_Date::string, ''),
# MAGIC   COALESCE(Booking_Type::string, ''),
# MAGIC   COALESCE(Booked_Date::string, ''),
# MAGIC   COALESCE(Carrier_Fuel_Surcharge_CAD::string, ''),
# MAGIC   COALESCE(Carrier_Fuel_Surcharge_USD::string, ''),
# MAGIC   COALESCE(Carrier_Linehaul_CAD::string, ''),
# MAGIC   COALESCE(Carrier_Linehaul_USD::string, ''),
# MAGIC   COALESCE(Carrier_ID::string, ''),
# MAGIC   COALESCE(Carrier_Name::string, ''),
# MAGIC   COALESCE(Carrier_New_Office::string, ''),
# MAGIC   COALESCE(Carrier_Old_Office::string, ''),
# MAGIC   COALESCE(Carrier_Other_Fees_CAD::string, ''),
# MAGIC   COALESCE(Carrier_Other_Fees_USD::string, ''),
# MAGIC   COALESCE(Carrier_Rep_ID::string, ''),
# MAGIC   COALESCE(Carrier_Rep::string, ''),
# MAGIC   COALESCE(Combined_Load::string, ''),
# MAGIC   COALESCE(Consignee_Delivery_Address::string, ''),
# MAGIC   COALESCE(Consignee_Name::string, ''),
# MAGIC   COALESCE(Consignee_Pickup_Address::string, ''),
# MAGIC   COALESCE(Customer_Fuel_Surcharge_USD::string, ''),
# MAGIC   COALESCE(Customer_Fule_Surcharge_CAD::string, ''),
# MAGIC   COALESCE(Customer_Linehaul_CAD::string, ''),
# MAGIC   COALESCE(Customer_Linehaul_USD::string, ''),
# MAGIC   COALESCE(Customer_ID::string, ''),
# MAGIC   COALESCE(Customer_Master::string, ''),
# MAGIC   COALESCE(Customer_Name::string, ''),
# MAGIC   COALESCE(Customer_New_Office::string, ''),
# MAGIC   COALESCE(Customer_Old_Office::string, ''),
# MAGIC   COALESCE(Customer_Other_Fees_CAD::string, ''),
# MAGIC   COALESCE(Customer_Other_Fees_USD::string, ''),
# MAGIC   COALESCE(Customer_Rep_ID::string, ''),
# MAGIC   COALESCE(Customer_Rep::string, ''),
# MAGIC   COALESCE(Days_Ahead::string, ''),
# MAGIC   COALESCE(Bounced_Date::string, ''),
# MAGIC   COALESCE(Cancelled_Date::string, ''),
# MAGIC   COALESCE(Delivery_Pickup_Charges_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Delivery_Pickup_Charges_Carrier_USD::string, ''),
# MAGIC   COALESCE(Delivery_Pickup_Charges_Customer_CAD::string, ''),
# MAGIC   COALESCE(Delivery_Pickup_Charges_Customer_USD::string, ''),
# MAGIC   COALESCE(Delivery_Appointment_Date::string, ''),
# MAGIC   COALESCE(Delivery_EndDate::string, ''),
# MAGIC   COALESCE(Destination_City::string, ''),
# MAGIC   COALESCE(Destination_State::string, ''),
# MAGIC   COALESCE(Destination_Zip::string, ''),
# MAGIC   COALESCE(DOT_Number::string, ''),
# MAGIC   COALESCE(Delivery_StartDate::string, ''),
# MAGIC   COALESCE(Equipment::string, ''),
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Carrier_USD::string, ''),
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Customer_CAD::string, ''),
# MAGIC   COALESCE(Equipment_Vehicle_Charges_Customer_USD::string, ''),
# MAGIC   COALESCE(Expense_CAD::string, ''),
# MAGIC   COALESCE(Expense_USD::string, ''),
# MAGIC   COALESCE(Financial_Period_Year::string, ''),
# MAGIC   COALESCE(GreenScreen_Rate_CAD::string, ''),
# MAGIC   COALESCE(GreenScreen_Rate_USD::string, ''),
# MAGIC   COALESCE(Invoice_Amount_CAD::string, ''),
# MAGIC   COALESCE(Invoice_Amount_USD::string, ''),
# MAGIC   COALESCE(Invoiced_Date::string, ''),
# MAGIC   COALESCE(Lawson_ID::string, ''),
# MAGIC   COALESCE(Load_Currency::string, ''),
# MAGIC   COALESCE(Load_Flag::string, ''),
# MAGIC   COALESCE(Load_Lane::string, ''),
# MAGIC   COALESCE(Load_Status::string, ''),
# MAGIC   COALESCE(Load_Unload_Charges_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Load_Unload_Charges_Carrier_USD::string, ''),
# MAGIC   COALESCE(Load_Unload_Charges_Customer_CAD::string, ''),
# MAGIC   COALESCE(Load_Unload_Charges_Customer_USD::string, ''),
# MAGIC   COALESCE(Margin_CAD::string, ''),
# MAGIC   COALESCE(Margin_USD::string, ''),
# MAGIC   COALESCE(Market_Buy_Rate_CAD::string, 'NA'),
# MAGIC   COALESCE(Market_Buy_Rate_USD::string, 'NA'),
# MAGIC   COALESCE(Market_Destination::string, ''),
# MAGIC   COALESCE(Market_Lane::string, ''),
# MAGIC   COALESCE(Market_Origin::string, ''),
# MAGIC   COALESCE(MC_Number::string, ''),
# MAGIC   COALESCE(Miles::string, ''),
# MAGIC   COALESCE(Miscellaneous_Chargers_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Miscellaneous_Chargers_Carrier_USD::string, ''),
# MAGIC   COALESCE(Miscellaneous_Chargers_Customers_CAD::string, ''),
# MAGIC   COALESCE(Miscellaneous_Chargers_Customers_USD::string, ''),
# MAGIC   COALESCE(Mode::string, ''),
# MAGIC   COALESCE(On_Time_Delivery::string, ''),
# MAGIC   COALESCE(On_Time_Pickup::string, ''),
# MAGIC   COALESCE(Origin_City::string, ''),
# MAGIC   COALESCE(Origin_State::string, ''),
# MAGIC   COALESCE(Origin_Zip::string, ''),
# MAGIC   COALESCE(Permits_Compliance_Charges_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Permits_Compliance_Charges_Carrier_USD::string, ''),
# MAGIC   COALESCE(Permits_Compliance_Charges_Customer_CAD::string, ''),
# MAGIC   COALESCE(Permits_Compliance_Charges_Customer_USD::string, ''),
# MAGIC   COALESCE(Pickup_Appointment_Date::string, ''),
# MAGIC   COALESCE(Actual_Pickup_Date::string, ''),
# MAGIC   COALESCE(Pickup_EndDate::string, ''),
# MAGIC   COALESCE(Pickup_StartDate::string, ''),
# MAGIC   COALESCE(PO_Number::string, ''),
# MAGIC   COALESCE(PreBookStatus::string, ''),
# MAGIC   COALESCE(Revenue_CAD::string, ''),
# MAGIC   COALESCE(Revenue_USD::string, ''),
# MAGIC   COALESCE(Rolled_Date::string, ''),
# MAGIC   COALESCE(Sales_Rep_ID::string, ''),
# MAGIC   COALESCE(Sales_Rep::string, ''),
# MAGIC   COALESCE(Ship_Date::string, ''),
# MAGIC   COALESCE(Spot_Margin::string, ''),
# MAGIC   COALESCE(Spot_Revenue::string, ''),
# MAGIC   COALESCE(Tender_Source_Type::string, ''),
# MAGIC   COALESCE(Tendered_Date::string, ''),
# MAGIC   COALESCE(TMS_System::string, ''),
# MAGIC   COALESCE(Trailer_Number::string, ''),
# MAGIC   COALESCE(Transit_Routing_Chargers_Carrier_CAD::string, ''),
# MAGIC   COALESCE(Transit_Routing_Chargers_Carrier_USD::string, ''),
# MAGIC   COALESCE(Transit_Routing_Chargers_Customer_CAD::string, ''),
# MAGIC   COALESCE(Transit_Routing_Chargers_Customer_USD::string, ''),
# MAGIC   COALESCE(Week_Number::string, ''),
# MAGIC   COALESCE(Carrier_Final_Rate_USD::string, ''),
# MAGIC   COALESCE(Carrier_Final_Rate_CAD::string, '')
# MAGIC )) AS HashKey
# MAGIC FROM Final
# MAGIC GROUP BY Load_Number	,
# MAGIC Admin_Fees_Carrier_USD	,
# MAGIC Admin_Fees_Carrier_CAD	,
# MAGIC Admin_Fees_Customer_USD	,
# MAGIC Admin_Fees_Customer_CAD	,
# MAGIC Billing_Status	,
# MAGIC Delivery_Date	,
# MAGIC Booking_Type	,
# MAGIC Booked_Date	,
# MAGIC Carrier_Fuel_Surcharge_CAD	,
# MAGIC Carrier_Fuel_Surcharge_USD	,
# MAGIC Carrier_Linehaul_CAD	,
# MAGIC Carrier_Linehaul_USD	,
# MAGIC Carrier_ID	,
# MAGIC Carrier_Name	,
# MAGIC Carrier_New_Office	,
# MAGIC Carrier_Old_Office	,
# MAGIC Carrier_Other_Fees_CAD	,
# MAGIC Carrier_Other_Fees_USD	,
# MAGIC Carrier_Rep_ID	,
# MAGIC Carrier_Rep	,
# MAGIC Combined_Load	,
# MAGIC Consignee_Delivery_Address	,
# MAGIC Consignee_Name	,
# MAGIC Consignee_Pickup_Address	,
# MAGIC Customer_FuelSurcharge_USD	,
# MAGIC Customer_FuelSurcharge_CAD	,
# MAGIC Customer_Linehaul_CAD	,
# MAGIC Customer_Linehaul_USD	,
# MAGIC Customer_ID	,
# MAGIC Customer_Master	,
# MAGIC Customer_Name	,
# MAGIC Customer_New_Office	,
# MAGIC Customer_Old_Office	,
# MAGIC Customer_Other_Fees_CAD	,
# MAGIC Customer_Other_Fees_USD	,
# MAGIC Customer_Rep_ID	,
# MAGIC Customer_Rep	,
# MAGIC Days_ahead	,
# MAGIC Bounced_Date	,
# MAGIC Cancelled_Date	,
# MAGIC DelandPickup_Carrier_CAD	,
# MAGIC DelandPickup_Carrier_USD	,
# MAGIC DelandPickup_Cust_CAD	,
# MAGIC DelandPickup_Cust_USD	,
# MAGIC Delivery_Appointment_Date	,
# MAGIC Delivery_End	,
# MAGIC Dest_City	,
# MAGIC Dest_State	,
# MAGIC Dest_Zip	,
# MAGIC DOT_Number	,
# MAGIC Delivery_Start	,
# MAGIC Equipment	,
# MAGIC EquipmentandVehicle_Carrier_CAD	,
# MAGIC EquipmentandVehicle_Carrier_USD	,
# MAGIC EquipmentandVehicle_Cust_CAD	,
# MAGIC EquipmentandVehicle_Cust_USD	,
# MAGIC Expense_CAD	,
# MAGIC Expense_USD	,
# MAGIC Financial_Period_Year	,
# MAGIC Max_Buy_Rate_CAD	,
# MAGIC Max_Buy_Rate	,
# MAGIC Invoice_Amount_CAD	,
# MAGIC Invoice_Amount_USD	,
# MAGIC Invoice_Date	,
# MAGIC Lawson_ID	,
# MAGIC Load_Currency	,
# MAGIC Load_flag	,
# MAGIC load_lane	,
# MAGIC Load_Status	,
# MAGIC Loadandunload_carrier_cad	,
# MAGIC Loadandunload_carrier_USD	,
# MAGIC Loadandunload_Cust_CAD	,
# MAGIC Loadandunload_Cust_CAD	,
# MAGIC Margin_CAD	,
# MAGIC Margin_USD	,
# MAGIC Market_Buy_Rate_CAD	,
# MAGIC Market_Buy_Rate	,
# MAGIC Market_Dest	,
# MAGIC Market_Lane	,
# MAGIC Market_Origin	,
# MAGIC Max_buy_rate	,
# MAGIC Miles	,
# MAGIC Miscellaneous_Carrier_CAD	,
# MAGIC Miscellaneous_Carrier_USD	,
# MAGIC Miscellaneous_Cust_CAD	,
# MAGIC Miscellaneous_Cust_USD	,
# MAGIC Mode	,
# MAGIC On_Time_Delivery	,
# MAGIC On_Time_Pickup	,
# MAGIC Origin_City	,
# MAGIC Origin_State	,
# MAGIC Origin_Zip	,
# MAGIC PermitsandCompliance_Carrier_CAD	,
# MAGIC PermitsandCompliance_Carrier_USD	,
# MAGIC PermitsandCompliance_cust_CAD	,
# MAGIC PermitsandCompliance_cust_USD	,
# MAGIC Pickup_Appointment_Date	,
# MAGIC Pickup_Datetime	,
# MAGIC Pickup_End	,
# MAGIC Pickup_Start	,
# MAGIC PO_Number	,
# MAGIC PreBookStatus	,
# MAGIC Revenue_CAD	,
# MAGIC Revenue_USD	,
# MAGIC Rolled_Date	,
# MAGIC Sales_Rep_ID	,
# MAGIC Sales_Rep	,
# MAGIC Ship_Date	,
# MAGIC Spot_Margin	,
# MAGIC Spot_Revenue	,
# MAGIC Tender_Source_Type	,
# MAGIC TMS_Sourcesystem	,
# MAGIC Trailer_Number	,
# MAGIC TransitandRouting_Carrier_CAD	,
# MAGIC TransitandRouting_Carrier_USD	,
# MAGIC TransitandRouting_Cust_CAD	,
# MAGIC TransitandRouting_Cust_USD	,
# MAGIC Days_Aged_To_Invoice	,
# MAGIC Market_Buy_Rate_USD,
# MAGIC Market_Buy_Rate_CAD,
# MAGIC DW_Load_ID,
# MAGIC MC_Number)
# MAGIC
# MAGIC SELECT * FROM Relay WHERE Load_ID IN (SELECT Load_ID FROM Relay GROUP BY 1 HAVING COUNT(*) = 1)
# MAGIC UNION
# MAGIC SELECT * FROM Relay WHERE Load_ID IN (SELECT Load_ID FROM Relay GROUP BY 1 HAVING COUNT(*) > 1) And Load_Status = 'BOOKED' AND Carrier_Name IS NOT NULL AND Mode = 'TL'

# COMMAND ----------

# MAGIC %md
# MAGIC ####DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver.Silver_Relay (
# MAGIC     DW_Load_ID  VARCHAR(255) NOT NULL,
# MAGIC     Load_ID BIGINT,
# MAGIC     Admin_Fees_Carrier_USD DECIMAL(10,2),
# MAGIC     Admin_Fees_Carrier_CAD DECIMAL(10,2),
# MAGIC     Admin_Fees_Customer_USD DECIMAL(10,2),
# MAGIC     Admin_Fees_Customer_CAD DECIMAL(10,2),
# MAGIC     Billing_Status VARCHAR(255),
# MAGIC     Actual_Delivered_Date TIMESTAMP_NTZ,
# MAGIC     Booking_Type VARCHAR(255),
# MAGIC     Booked_Date TIMESTAMP_NTZ,
# MAGIC     Carrier_Fuel_Surcharge_CAD DECIMAL(10,2),
# MAGIC     Carrier_Fuel_Surcharge_USD DECIMAL(10,2),
# MAGIC     Carrier_Linehaul_CAD DECIMAL(10,2),
# MAGIC     Carrier_Linehaul_USD DECIMAL(10,2),
# MAGIC     Carrier_ID VARCHAR(255),
# MAGIC     Carrier_Name VARCHAR(255),
# MAGIC     Carrier_New_Office VARCHAR(255),
# MAGIC     Carrier_Old_Office VARCHAR(255),
# MAGIC     Carrier_Other_Fees_CAD DECIMAL(10,2),
# MAGIC     Carrier_Other_Fees_USD DECIMAL(10,2),
# MAGIC     Carrier_Rep_ID VARCHAR(255),
# MAGIC     Carrier_Rep VARCHAR(255),
# MAGIC     Combined_Load VARCHAR(255),
# MAGIC     Consignee_Delivery_Address VARCHAR(255),
# MAGIC     Consignee_Name VARCHAR(255),
# MAGIC     Consignee_Pickup_Address VARCHAR(255),
# MAGIC     Customer_Fuel_Surcharge_USD DECIMAL(10,2),
# MAGIC     Customer_Fule_Surcharge_CAD DECIMAL(10,2),
# MAGIC     Customer_Linehaul_CAD DECIMAL(10,2),
# MAGIC     Customer_Linehaul_USD DECIMAL(10,2),
# MAGIC     Customer_ID VARCHAR(255),
# MAGIC     Customer_Master VARCHAR(255),
# MAGIC     Customer_Name VARCHAR(255),
# MAGIC     Customer_New_Office VARCHAR(255),
# MAGIC     Customer_Old_Office VARCHAR(255),
# MAGIC     Customer_Other_Fees_CAD DECIMAL(10,2),
# MAGIC     Customer_Other_Fees_USD DECIMAL(10,2),
# MAGIC     Customer_Rep_ID VARCHAR(255),
# MAGIC     Customer_Rep VARCHAR(255),
# MAGIC     Days_Ahead VARCHAR(255),
# MAGIC     Bounced_Date TIMESTAMP_NTZ,
# MAGIC     Cancelled_Date TIMESTAMP_NTZ,
# MAGIC     Delivery_Pickup_Charges_Carrier_CAD DECIMAL(10,2),
# MAGIC     Delivery_Pickup_Charges_Carrier_USD DECIMAL(10,2),
# MAGIC     Delivery_Pickup_Charges_Customer_CAD DECIMAL(10,2),
# MAGIC     Delivery_Pickup_Charges_Customer_USD DECIMAL(10,2),
# MAGIC     Delivery_Appointment_Date TIMESTAMP_NTZ,
# MAGIC     Delivery_EndDate TIMESTAMP_NTZ,
# MAGIC     Destination_City VARCHAR(255),
# MAGIC     Destination_State VARCHAR(255),
# MAGIC     Destination_Zip VARCHAR(255),
# MAGIC     DOT_Number VARCHAR(255),
# MAGIC     Delivery_StartDate TIMESTAMP_NTZ,
# MAGIC     Equipment VARCHAR(255),
# MAGIC     Equipment_Vehicle_Charges_Carrier_CAD DECIMAL(10,2),
# MAGIC     Equipment_Vehicle_Charges_Carrier_USD DECIMAL(10,2),
# MAGIC     Equipment_Vehicle_Charges_Customer_CAD DECIMAL(10,2),
# MAGIC     Equipment_Vehicle_Charges_Customer_USD DECIMAL(10,2),
# MAGIC     Expense_CAD DECIMAL(10,2),
# MAGIC     Expense_USD DECIMAL(10,2),
# MAGIC     Financial_Period_Year VARCHAR(255),
# MAGIC     GreenScreen_Rate_CAD DECIMAL(10,2),
# MAGIC     GreenScreen_Rate_USD DECIMAL(10,2),
# MAGIC     Invoice_Amount_CAD DECIMAL(10,2),
# MAGIC     Invoice_Amount_USD DECIMAL(10,2),
# MAGIC     Invoiced_Date TIMESTAMP_NTZ,
# MAGIC     Lawson_ID VARCHAR(50),
# MAGIC     Load_Currency VARCHAR(255),
# MAGIC     Load_Flag VARCHAR(255),
# MAGIC     Load_Lane VARCHAR(255),
# MAGIC     Load_Status VARCHAR(255),
# MAGIC     Load_Unload_Charges_Carrier_CAD DECIMAL(10,2),
# MAGIC     Load_Unload_Charges_Carrier_USD DECIMAL(10,2),
# MAGIC     Load_Unload_Charges_Customer_CAD DECIMAL(10,2),
# MAGIC     Load_Unload_Charges_Customer_USD DECIMAL(10,2),
# MAGIC     Margin_CAD DECIMAL(10,2),
# MAGIC     Margin_USD DECIMAL(10,2),
# MAGIC     Market_Buy_Rate_CAD DECIMAL(10,2),
# MAGIC     Market_Buy_Rate_USD DECIMAL(10,2),
# MAGIC     Market_Destination VARCHAR(255),
# MAGIC     Market_Lane VARCHAR(255),
# MAGIC     Market_Origin VARCHAR(255),
# MAGIC     MC_Number VARCHAR(255),
# MAGIC     Miles VARCHAR(255),
# MAGIC     Miscellaneous_Chargers_Carrier_CAD DECIMAL(10,2),
# MAGIC     Miscellaneous_Chargers_Carrier_USD DECIMAL(10,2),
# MAGIC     Miscellaneous_Chargers_Customers_CAD DECIMAL(10,2),
# MAGIC     Miscellaneous_Chargers_Customers_USD DECIMAL(10,2),
# MAGIC     Mode VARCHAR(255),
# MAGIC     On_Time_Delivery VARCHAR(255),
# MAGIC     On_Time_Pickup VARCHAR(255),
# MAGIC     Origin_City VARCHAR(255),
# MAGIC     Origin_State VARCHAR(255),
# MAGIC     Origin_Zip VARCHAR(255),
# MAGIC     Permits_Compliance_Charges_Carrier_CAD DECIMAL(10,2),
# MAGIC     Permits_Compliance_Charges_Carrier_USD DECIMAL(10,2),
# MAGIC     Permits_Compliance_Charges_Customer_CAD DECIMAL(10,2),
# MAGIC     Permits_Compliance_Charges_Customer_USD DECIMAL(10,2),
# MAGIC     Pickup_Appointment_Date TIMESTAMP_NTZ,
# MAGIC     Actual_Pickup_Date TIMESTAMP_NTZ,
# MAGIC     Pickup_EndDate TIMESTAMP_NTZ,
# MAGIC     Pickup_StartDate TIMESTAMP_NTZ,
# MAGIC     PO_Number STRING,
# MAGIC     PreBookStatus VARCHAR(255),
# MAGIC     Revenue_CAD DECIMAL(10,2),
# MAGIC     Revenue_USD DECIMAL(10,2),
# MAGIC     Rolled_Date TIMESTAMP_NTZ,
# MAGIC     Sales_Rep_ID VARCHAR(255),
# MAGIC     Sales_Rep VARCHAR(255),
# MAGIC     Ship_Date TIMESTAMP_NTZ,
# MAGIC     Spot_Margin VARCHAR(255),
# MAGIC     Spot_Revenue VARCHAR(255),
# MAGIC     Tender_Source_Type VARCHAR(255),
# MAGIC     Tendered_Date TIMESTAMP_NTZ,
# MAGIC     TMS_System VARCHAR(50),
# MAGIC     Trailer_Number VARCHAR(255),
# MAGIC     Transit_Routing_Chargers_Carrier_CAD DECIMAL(10,2),
# MAGIC     Transit_Routing_Chargers_Carrier_USD DECIMAL(10,2),
# MAGIC     Transit_Routing_Chargers_Customer_CAD DECIMAL(10,2),
# MAGIC     Transit_Routing_Chargers_Customer_USD DECIMAL(10,2),
# MAGIC     Week_Number VARCHAR(10),
# MAGIC     Carrier_Final_Rate_USD DECIMAL(10,2),
# MAGIC     Carrier_Final_Rate_CAD DECIMAL(10,2),
# MAGIC      Hashkey STRING NOT NULL,
# MAGIC     Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Created_By VARCHAR(50) NOT NULL,
# MAGIC     Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC     Last_Modified_By VARCHAR(50) NOT NULL,
# MAGIC     Is_Deleted SMALLINT NOT NULL,
# MAGIC     CONSTRAINT PK_Silver_Relay PRIMARY KEY (DW_Load_ID)
# MAGIC ) USING DELTA;
# MAGIC