# Databricks notebook source
# MAGIC %md
# MAGIC ##Row Count Check - Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from public.spot_quotes

# COMMAND ----------

# MAGIC %md
# MAGIC ##Row Count Check - Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'

# COMMAND ----------

# MAGIC %md
# MAGIC ##Null Count Check - Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'quote_id' AS column_name, COUNT(*) - COUNT(quote_id) AS null_count FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_ref_number', COUNT(*) - COUNT(shipper_ref_number) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'status', COUNT(*) - COUNT(status) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'status_str', COUNT(*) - COUNT(status_str) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'created_at', COUNT(*) - COUNT(created_at) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'quoted_price', COUNT(*) - COUNT(quoted_price) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'quote_type', COUNT(*) - COUNT(quote_type) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'load_number', COUNT(*) - COUNT(load_number) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'last_updated_at', COUNT(*) - COUNT(last_updated_at) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'margin', COUNT(*) - COUNT(margin) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'quote_source_str', COUNT(*) - COUNT(quote_source_str) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'customer_slug', COUNT(*) - COUNT(customer_slug) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'customer_name', COUNT(*) - COUNT(customer_name) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'equipment_name', COUNT(*) - COUNT(equipment_name) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'created_by', COUNT(*) - COUNT(created_by) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'low_buy_predicted', COUNT(*) - COUNT(low_buy_predicted) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'target_buy_predicted', COUNT(*) - COUNT(target_buy_predicted) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'high_buy_predicted', COUNT(*) - COUNT(high_buy_predicted) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'fuel_rate_predicted', COUNT(*) - COUNT(fuel_rate_predicted) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'weight', COUNT(*) - COUNT(weight) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'confidence_level', COUNT(*) - COUNT(confidence_level) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'miles', COUNT(*) - COUNT(miles) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'pc_miler_miles', COUNT(*) - COUNT(pc_miler_miles) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'multistop', COUNT(*) - COUNT(multistop) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_city', COUNT(*) - COUNT(shipper_city) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_state', COUNT(*) - COUNT(shipper_state) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_zip', COUNT(*) - COUNT(shipper_zip) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_country', COUNT(*) - COUNT(shipper_country) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_quote_id', COUNT(*) - COUNT(shipper_quote_id) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_activity_type_str', COUNT(*) - COUNT(intermediate_stop_one_activity_type_str) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_city', COUNT(*) - COUNT(intermediate_stop_one_city) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_state', COUNT(*) - COUNT(intermediate_stop_one_state) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_postal_code', COUNT(*) - COUNT(intermediate_stop_one_postal_code) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_country', COUNT(*) - COUNT(intermediate_stop_one_country) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_activity_type_str', COUNT(*) - COUNT(intermediate_stop_two_activity_type_str) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_city', COUNT(*) - COUNT(intermediate_stop_two_city) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_state', COUNT(*) - COUNT(intermediate_stop_two_state) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_postal_code', COUNT(*) - COUNT(intermediate_stop_two_postal_code) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_country', COUNT(*) - COUNT(intermediate_stop_two_country) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_city', COUNT(*) - COUNT(receiver_city) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_state', COUNT(*) - COUNT(receiver_state) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_zip', COUNT(*) - COUNT(receiver_zip) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_country', COUNT(*) - COUNT(receiver_country) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_order', COUNT(*) - COUNT(receiver_order) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'origin_date', COUNT(*) - COUNT(origin_date) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'dest_date', COUNT(*) - COUNT(dest_date) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'responded_at', COUNT(*) - COUNT(responded_at) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'responded_by', COUNT(*) - COUNT(responded_by) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'profit_center', COUNT(*) - COUNT(profit_center) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'live_handling', COUNT(*) - COUNT(live_handling) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'notes', COUNT(*) - COUNT(notes) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'inbound_market_strength', COUNT(*) - COUNT(inbound_market_strength) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'outbound_market_strength', COUNT(*) - COUNT(outbound_market_strength) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'combined_market_strength', COUNT(*) - COUNT(combined_market_strength) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'request_source', COUNT(*) - COUNT(request_source) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'commodity', COUNT(*) - COUNT(commodity) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'team', COUNT(*) - COUNT(team) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'effective_date', COUNT(*) - COUNT(effective_date) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'expiration_date', COUNT(*) - COUNT(expiration_date) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'lost_reason', COUNT(*) - COUNT(lost_reason) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'is_deleted', COUNT(*) - COUNT(is_deleted) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'prediction_rate', COUNT(*) - COUNT(prediction_rate) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'winning_bid', COUNT(*) - COUNT(winning_bid) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'declared_value', COUNT(*) - COUNT(declared_value) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'hazmat', COUNT(*) - COUNT(hazmat) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'ruleset_price_prediction', COUNT(*) - COUNT(ruleset_price_prediction) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'currency', COUNT(*) - COUNT(currency) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'transit_time', COUNT(*) - COUNT(transit_time) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'origin_time', COUNT(*) - COUNT(origin_time) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'destination_time', COUNT(*) - COUNT(destination_time) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'customer_tms', COUNT(*) - COUNT(customer_tms) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'notifying_channel_id', COUNT(*) - COUNT(notifying_channel_id) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'marked_won_by', COUNT(*) - COUNT(marked_won_by) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC UNION ALL
# MAGIC SELECT 'nfi_pricing_contact', COUNT(*) - COUNT(nfi_pricing_contact) FROM bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC ORDER BY column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Null Count Check - Source

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC     'quote_id' AS column_name, COUNT(*) - COUNT(quote_id) AS null_count FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_ref_number', COUNT(*) - COUNT(shipper_ref_number) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'status', COUNT(*) - COUNT(status) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'status_str', COUNT(*) - COUNT(status_str) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'created_at', COUNT(*) - COUNT(created_at) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'quoted_price', COUNT(*) - COUNT(quoted_price) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'quote_type', COUNT(*) - COUNT(quote_type) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'load_number', COUNT(*) - COUNT(load_number) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'last_updated_at', COUNT(*) - COUNT(last_updated_at) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'margin', COUNT(*) - COUNT(margin) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'quote_source_str', COUNT(*) - COUNT(quote_source_str) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'customer_slug', COUNT(*) - COUNT(customer_slug) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'customer_name', COUNT(*) - COUNT(customer_name) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'equipment_name', COUNT(*) - COUNT(equipment_name) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'created_by', COUNT(*) - COUNT(created_by) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'low_buy_predicted', COUNT(*) - COUNT(low_buy_predicted) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'target_buy_predicted', COUNT(*) - COUNT(target_buy_predicted) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'high_buy_predicted', COUNT(*) - COUNT(high_buy_predicted) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'fuel_rate_predicted', COUNT(*) - COUNT(fuel_rate_predicted) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'weight', COUNT(*) - COUNT(weight) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'confidence_level', COUNT(*) - COUNT(confidence_level) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'miles', COUNT(*) - COUNT(miles) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'pc_miler_miles', COUNT(*) - COUNT(pc_miler_miles) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'multistop', COUNT(*) - COUNT(multistop) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_city', COUNT(*) - COUNT(shipper_city) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_state', COUNT(*) - COUNT(shipper_state) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_zip', COUNT(*) - COUNT(shipper_zip) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_country', COUNT(*) - COUNT(shipper_country) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'shipper_quote_id', COUNT(*) - COUNT(shipper_quote_id) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_activity_type_str', COUNT(*) - COUNT(intermediate_stop_one_activity_type_str) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_city', COUNT(*) - COUNT(intermediate_stop_one_city) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_state', COUNT(*) - COUNT(intermediate_stop_one_state) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_postal_code', COUNT(*) - COUNT(intermediate_stop_one_postal_code) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_one_country', COUNT(*) - COUNT(intermediate_stop_one_country) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_activity_type_str', COUNT(*) - COUNT(intermediate_stop_two_activity_type_str) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_city', COUNT(*) - COUNT(intermediate_stop_two_city) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_state', COUNT(*) - COUNT(intermediate_stop_two_state) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_postal_code', COUNT(*) - COUNT(intermediate_stop_two_postal_code) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'intermediate_stop_two_country', COUNT(*) - COUNT(intermediate_stop_two_country) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_city', COUNT(*) - COUNT(receiver_city) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_state', COUNT(*) - COUNT(receiver_state) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_zip', COUNT(*) - COUNT(receiver_zip) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_country', COUNT(*) - COUNT(receiver_country) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'receiver_order', COUNT(*) - COUNT(receiver_order) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'origin_date', COUNT(*) - COUNT(origin_date) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'dest_date', COUNT(*) - COUNT(dest_date) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'responded_at', COUNT(*) - COUNT(responded_at) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'responded_by', COUNT(*) - COUNT(responded_by) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'profit_center', COUNT(*) - COUNT(profit_center) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'live_handling', COUNT(*) - COUNT(live_handling) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'notes', COUNT(*) - COUNT(notes) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'inbound_market_strength', COUNT(*) - COUNT(inbound_market_strength) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'outbound_market_strength', COUNT(*) - COUNT(outbound_market_strength) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'combined_market_strength', COUNT(*) - COUNT(combined_market_strength) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'request_source', COUNT(*) - COUNT(request_source) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'commodity', COUNT(*) - COUNT(commodity) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'team', COUNT(*) - COUNT(team) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'effective_date', COUNT(*) - COUNT(effective_date) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'expiration_date', COUNT(*) - COUNT(expiration_date) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'lost_reason', COUNT(*) - COUNT(lost_reason) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'is_deleted', COUNT(*) - COUNT(is_deleted) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'prediction_rate', COUNT(*) - COUNT(prediction_rate) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'winning_bid', COUNT(*) - COUNT(winning_bid) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'declared_value', COUNT(*) - COUNT(declared_value) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'hazmat', COUNT(*) - COUNT(hazmat) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'ruleset_price_prediction', COUNT(*) - COUNT(ruleset_price_prediction) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'currency', COUNT(*) - COUNT(currency) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'transit_time', COUNT(*) - COUNT(transit_time) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'origin_time', COUNT(*) - COUNT(origin_time) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'destination_time', COUNT(*) - COUNT(destination_time) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'customer_tms', COUNT(*) - COUNT(customer_tms) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'notifying_channel_id', COUNT(*) - COUNT(notifying_channel_id) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'marked_won_by', COUNT(*) - COUNT(marked_won_by) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC UNION ALL
# MAGIC SELECT 'nfi_pricing_contact', COUNT(*) - COUNT(nfi_pricing_contact) FROM public.spot_quotes where last_updated_at < '2024-10-20T11:25:11.613'
# MAGIC ORDER BY column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplication Check - Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT quote_id,count(*) FROM bronze.spot_quotes
# MAGIC group by quote_id
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplication Check - Source

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT quote_id,count(*) FROM public.spot_quotes
# MAGIC group by quote_id
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check - Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select quote_id,quoted_price,status_str,created_by from bronze.spot_quotes where last_updated_at < '2024-10-22T10:47:42.304'
# MAGIC order by quote_id desc limit 10 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sample Data Check - Source

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from public.spot_quotes order by quote_id desc limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Error Logger Table Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_ID = 'SQ-1' order by Created_At desc limit 2

# COMMAND ----------

