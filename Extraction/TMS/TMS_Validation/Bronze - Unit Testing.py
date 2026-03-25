# Databricks notebook source
# MAGIC %md
# MAGIC ##TRANSFIX TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ##Null count check in Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'loadnum' AS column_name, COUNT(*) AS null_count FROM bronze.transfix_data WHERE loadnum IS NULL UNION ALL
# MAGIC SELECT 'shipper', COUNT(*) FROM bronze.transfix_data WHERE shipper IS NULL UNION ALL
# MAGIC SELECT 'carrier', COUNT(*) FROM bronze.transfix_data WHERE carrier IS NULL UNION ALL
# MAGIC SELECT 'dot', COUNT(*) FROM bronze.transfix_data WHERE dot IS NULL UNION ALL
# MAGIC SELECT 'service_line', COUNT(*) FROM bronze.transfix_data WHERE service_line IS NULL UNION ALL
# MAGIC SELECT 'load_mode', COUNT(*) FROM bronze.transfix_data WHERE load_mode IS NULL UNION ALL
# MAGIC SELECT 'pu_appt', COUNT(*) FROM bronze.transfix_data WHERE pu_appt IS NULL UNION ALL
# MAGIC SELECT 'del_appt', COUNT(*) FROM bronze.transfix_data WHERE del_appt IS NULL UNION ALL
# MAGIC SELECT 'origin_city', COUNT(*) FROM bronze.transfix_data WHERE origin_city IS NULL UNION ALL
# MAGIC SELECT 'origin_state', COUNT(*) FROM bronze.transfix_data WHERE origin_state IS NULL UNION ALL
# MAGIC SELECT 'origin_zip', COUNT(*) FROM bronze.transfix_data WHERE origin_zip IS NULL UNION ALL
# MAGIC SELECT 'pu_drop', COUNT(*) FROM bronze.transfix_data WHERE pu_drop IS NULL UNION ALL
# MAGIC SELECT 'dest_city', COUNT(*) FROM bronze.transfix_data WHERE dest_city IS NULL UNION ALL
# MAGIC SELECT 'dest_state', COUNT(*) FROM bronze.transfix_data WHERE dest_state IS NULL UNION ALL
# MAGIC SELECT 'dest_zip', COUNT(*) FROM bronze.transfix_data WHERE dest_zip IS NULL UNION ALL
# MAGIC SELECT 'del_drop', COUNT(*) FROM bronze.transfix_data WHERE del_drop IS NULL UNION ALL
# MAGIC SELECT 'weight', COUNT(*) FROM bronze.transfix_data WHERE weight IS NULL UNION ALL
# MAGIC SELECT 'total_rev', COUNT(*) FROM bronze.transfix_data WHERE total_rev IS NULL UNION ALL
# MAGIC SELECT 'cust_lhc', COUNT(*) FROM bronze.transfix_data WHERE cust_lhc IS NULL UNION ALL
# MAGIC SELECT 'cust_fuel', COUNT(*) FROM bronze.transfix_data WHERE cust_fuel IS NULL UNION ALL
# MAGIC SELECT 'total_exp', COUNT(*) FROM bronze.transfix_data WHERE total_exp IS NULL UNION ALL
# MAGIC SELECT 'carr_lhc', COUNT(*) FROM bronze.transfix_data WHERE carr_lhc IS NULL UNION ALL
# MAGIC SELECT 'carr_fuel', COUNT(*) FROM bronze.transfix_data WHERE carr_fuel IS NULL UNION ALL
# MAGIC SELECT 'finance_date', COUNT(*) FROM bronze.transfix_data WHERE finance_date IS NULL UNION ALL
# MAGIC SELECT 'shipper_miles', COUNT(*) FROM bronze.transfix_data WHERE shipper_miles IS NULL UNION ALL
# MAGIC SELECT 'calc_miles', COUNT(*) FROM bronze.transfix_data WHERE calc_miles IS NULL UNION ALL
# MAGIC SELECT 'arrive_pu_date', COUNT(*) FROM bronze.transfix_data WHERE arrive_pu_date IS NULL UNION ALL
# MAGIC SELECT 'depart_pu_date', COUNT(*) FROM bronze.transfix_data WHERE depart_pu_date IS NULL UNION ALL
# MAGIC SELECT 'arrive_del_date', COUNT(*) FROM bronze.transfix_data WHERE arrive_del_date IS NULL UNION ALL
# MAGIC SELECT 'depart_del_date', COUNT(*) FROM bronze.transfix_data WHERE depart_del_date IS NULL UNION ALL
# MAGIC SELECT 'cust_acc', COUNT(*) FROM bronze.transfix_data WHERE cust_acc IS NULL UNION ALL
# MAGIC SELECT 'carr_acc', COUNT(*) FROM bronze.transfix_data WHERE carr_acc IS NULL UNION ALL
# MAGIC SELECT 'first_invoice_sent_date', COUNT(*) FROM bronze.transfix_data WHERE first_invoice_sent_date IS NULL UNION ALL
# MAGIC SELECT 'shipper_shipment_reference', COUNT(*) FROM bronze.transfix_data WHERE shipper_shipment_reference IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Null count check in Source

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'loadnum' AS column_name, COUNT(*) AS null_count FROM transfix_data WHERE loadnum IS NULL UNION ALL
# MAGIC SELECT 'shipper', COUNT(*) FROM transfix_data WHERE shipper IS NULL UNION ALL
# MAGIC SELECT 'carrier', COUNT(*) FROM transfix_data WHERE carrier IS NULL UNION ALL
# MAGIC SELECT 'dot', COUNT(*) FROM transfix_data WHERE dot IS NULL UNION ALL
# MAGIC SELECT 'service_line', COUNT(*) FROM transfix_data WHERE service_line IS NULL UNION ALL
# MAGIC SELECT 'load_mode', COUNT(*) FROM transfix_data WHERE load_mode IS NULL UNION ALL
# MAGIC SELECT 'pu_appt', COUNT(*) FROM transfix_data WHERE pu_appt IS NULL UNION ALL
# MAGIC SELECT 'del_appt', COUNT(*) FROM transfix_data WHERE del_appt IS NULL UNION ALL
# MAGIC SELECT 'origin_city', COUNT(*) FROM transfix_data WHERE origin_city IS NULL UNION ALL
# MAGIC SELECT 'origin_state', COUNT(*) FROM transfix_data WHERE origin_state IS NULL UNION ALL
# MAGIC SELECT 'origin_zip', COUNT(*) FROM transfix_data WHERE origin_zip IS NULL UNION ALL
# MAGIC SELECT 'pu_drop', COUNT(*) FROM transfix_data WHERE pu_drop IS NULL UNION ALL
# MAGIC SELECT 'dest_city', COUNT(*) FROM transfix_data WHERE dest_city IS NULL UNION ALL
# MAGIC SELECT 'dest_state', COUNT(*) FROM transfix_data WHERE dest_state IS NULL UNION ALL
# MAGIC SELECT 'dest_zip', COUNT(*) FROM transfix_data WHERE dest_zip IS NULL UNION ALL
# MAGIC SELECT 'del_drop', COUNT(*) FROM transfix_data WHERE del_drop IS NULL UNION ALL
# MAGIC SELECT 'weight', COUNT(*) FROM transfix_data WHERE weight IS NULL UNION ALL
# MAGIC SELECT 'total_rev', COUNT(*) FROM transfix_data WHERE total_rev IS NULL UNION ALL
# MAGIC SELECT 'cust_lhc', COUNT(*) FROM transfix_data WHERE cust_lhc IS NULL UNION ALL
# MAGIC SELECT 'cust_fuel', COUNT(*) FROM transfix_data WHERE cust_fuel IS NULL UNION ALL
# MAGIC SELECT 'total_exp', COUNT(*) FROM transfix_data WHERE total_exp IS NULL UNION ALL
# MAGIC SELECT 'carr_lhc', COUNT(*) FROM transfix_data WHERE carr_lhc IS NULL UNION ALL
# MAGIC SELECT 'carr_fuel', COUNT(*) FROM transfix_data WHERE carr_fuel IS NULL UNION ALL
# MAGIC SELECT 'finance_date', COUNT(*) FROM transfix_data WHERE finance_date IS NULL UNION ALL
# MAGIC SELECT 'shipper_miles', COUNT(*) FROM transfix_data WHERE shipper_miles IS NULL UNION ALL
# MAGIC SELECT 'calc_miles', COUNT(*) FROM transfix_data WHERE calc_miles IS NULL UNION ALL
# MAGIC SELECT 'arrive_pu_date', COUNT(*) FROM transfix_data WHERE arrive_pu_date IS NULL UNION ALL
# MAGIC SELECT 'depart_pu_date', COUNT(*) FROM transfix_data WHERE depart_pu_date IS NULL UNION ALL
# MAGIC SELECT 'arrive_del_date', COUNT(*) FROM transfix_data WHERE arrive_del_date IS NULL UNION ALL
# MAGIC SELECT 'depart_del_date', COUNT(*) FROM transfix_data WHERE depart_del_date IS NULL UNION ALL
# MAGIC SELECT 'cust_acc', COUNT(*) FROM transfix_data WHERE cust_acc IS NULL UNION ALL
# MAGIC SELECT 'carr_acc', COUNT(*) FROM transfix_data WHERE carr_acc IS NULL UNION ALL
# MAGIC SELECT 'first_invoice_sent_date', COUNT(*) FROM transfix_data WHERE first_invoice_sent_date IS NULL UNION ALL
# MAGIC SELECT 'shipper_shipment_reference', COUNT(*) FROM transfix_data WHERE shipper_shipment_reference IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Count check

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Count(*) from bronze.transfix_data

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from public.transfix_data

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.transfix_data order by loadnum limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from public.transfix_data order by loadnum limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT loadnum, COUNT(*) AS duplicate_count
# MAGIC FROM bronze.transfix_data
# MAGIC GROUP BY loadnum
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT loadnum, COUNT(*) AS duplicate_count
# MAGIC FROM transfix_data
# MAGIC GROUP BY public.loadnum
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ##PROJECTION TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ##Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.tmw_pro_projection;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from public.tmw_pro_projection

# COMMAND ----------

# MAGIC %md
# MAGIC ##Null count check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'booking_id' AS column_name, COUNT(*) AS null_count FROM bronze.tmw_pro_projection WHERE booking_id IS NULL UNION ALL
# MAGIC SELECT 'tmw_pro_number', COUNT(*) FROM bronze.tmw_pro_projection WHERE tmw_pro_number IS NULL UNION ALL
# MAGIC SELECT 'inserted_at', COUNT(*) FROM bronze.tmw_pro_projection WHERE inserted_at IS NULL UNION ALL
# MAGIC SELECT 'updated_at', COUNT(*) FROM bronze.tmw_pro_projection WHERE updated_at IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'booking_id' AS column_name, COUNT(*) AS null_count FROM public.tmw_pro_projection WHERE booking_id IS NULL UNION ALL
# MAGIC SELECT 'tmw_pro_number', COUNT(*) FROM public.tmw_pro_projection WHERE tmw_pro_number IS NULL UNION ALL
# MAGIC SELECT 'inserted_at', COUNT(*) FROM public.tmw_pro_projection WHERE inserted_at IS NULL UNION ALL
# MAGIC SELECT 'updated_at', COUNT(*) FROM public.tmw_pro_projection WHERE updated_at IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicate check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT booking_id, COUNT(*) AS duplicate_count
# MAGIC FROM bronze.tmw_pro_projection
# MAGIC GROUP BY booking_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT booking_id, COUNT(*) AS duplicate_count
# MAGIC FROM public.tmw_pro_projection
# MAGIC GROUP BY booking_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.tmw_pro_projection order by booking_id desc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from public.tmw_pro_projection order by booking_id desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##FOURKITE CARRIER

# COMMAND ----------

# MAGIC %md
# MAGIC ##Row count check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.fourkite_carriers

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from public.fourkite_carriers

# COMMAND ----------

# MAGIC %md
# MAGIC ##Null count check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'carrier_name' AS column_name, COUNT(*) AS null_count FROM public.fourkite_carriers WHERE carrier_name IS NULL UNION ALL
# MAGIC SELECT 'us_dot', COUNT(*) FROM public.fourkite_carriers WHERE us_dot IS NULL UNION ALL
# MAGIC SELECT 'network_status', COUNT(*) FROM public.fourkite_carriers WHERE network_status IS NULL UNION ALL
# MAGIC SELECT 'networked_on', COUNT(*) FROM public.fourkite_carriers WHERE networked_on IS NULL UNION ALL
# MAGIC SELECT 'email', COUNT(*) FROM public.fourkite_carriers WHERE email IS NULL UNION ALL
# MAGIC SELECT 'tracking_method', COUNT(*) FROM public.fourkite_carriers WHERE tracking_method IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'carrier_name' AS column_name, COUNT(*) AS null_count FROM bronze.fourkite_carriers WHERE carrier_name IS NULL UNION ALL
# MAGIC SELECT 'us_dot', COUNT(*) FROM bronze.fourkite_carriers WHERE us_dot IS NULL UNION ALL
# MAGIC SELECT 'network_status', COUNT(*) FROM bronze.fourkite_carriers WHERE network_status IS NULL UNION ALL
# MAGIC SELECT 'networked_on', COUNT(*) FROM bronze.fourkite_carriers WHERE networked_on IS NULL UNION ALL
# MAGIC SELECT 'email', COUNT(*) FROM bronze.fourkite_carriers WHERE email IS NULL UNION ALL
# MAGIC SELECT 'tracking_method', COUNT(*) FROM bronze.fourkite_carriers WHERE tracking_method IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT carrier_name, us_dot, network_status, networked_on, email, tracking_method, COUNT(*) AS duplicate_count
# MAGIC FROM bronze.fourkite_carriers
# MAGIC GROUP BY carrier_name, us_dot, network_status, networked_on, email, tracking_method
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT carrier_name, us_dot, network_status, networked_on, email, tracking_method, COUNT(*) AS duplicate_count
# MAGIC FROM public.fourkite_carriers
# MAGIC GROUP BY carrier_name, us_dot, network_status, networked_on, email, tracking_method
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.fourkite_carriers order by carrier_name asc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from public.fourkite_carriers order by carrier_name desc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_ID = 'R69' order by Created_At desc 

# COMMAND ----------

