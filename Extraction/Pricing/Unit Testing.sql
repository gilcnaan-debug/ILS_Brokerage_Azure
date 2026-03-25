-- Databricks notebook source
-- MAGIC %python
-- MAGIC ## Mention the catalog used, getting the catalog name from key vault
-- MAGIC CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
-- MAGIC spark.sql(f"USE CATALOG {CatalogName}")

-- COMMAND ----------

select 'FINANCE_GLSYSTEM' as TableName,count(*) as Rowcount from bronze.FINANCE_GLSYSTEM union
select 'FINANCE_GLCHARTDTL' as TableName,count(*) as Rowcount from bronze.FINANCE_GLCHARTDTL union
select 'FINANCE_GLAMOUNTS' as TableName,count(*) as Rowcount from bronze.FINANCE_GLAMOUNTS union
select 'FINANCE_GLUNITS' as TableName,count(*) as Rowcount from bronze.FINANCE_GLUNITS union
select 'FINANCE_GLNAMES' as TableName,count(*) as Rowcount from bronze.FINANCE_GLNAMES union
select 'FINANCE_GLTRANS' as TableName,count(*) as Rowcount from bronze.FINANCE_GLTRANS union
select 'FINANCE_GLSRCCODE' as TableName,count(*) as Rowcount from bronze.FINANCE_GLSRCCODE union
select 'FINANCE_APDISTRIB' as TableName,count(*) as Rowcount from bronze.FINANCE_APDISTRIB union
select 'FINANCE_APVENMAST' as TableName,count(*) as Rowcount from bronze.FINANCE_APVENMAST union
select 'FINANCE_APVENCLASS' as TableName,count(*) as Rowcount from bronze.FINANCE_APVENCLASS union
select 'FINANCE_ARDISTRIB' as TableName,count(*) as Rowcount from bronze.FINANCE_ARDISTRIB union
select 'FINANCE_ARCUSTOMER' as TableName,count(*) as Rowcount from bronze.FINANCE_ARCUSTOMER order by TableName

-- COMMAND ----------

select 'external_load_id' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where external_load_id is null union 
select 'high_buy_rate_predicted' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where high_buy_rate_predicted is null union 
select 'low_buy_rate_predicted' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where low_buy_rate_predicted is null union 
select 'start_buy_rate_predicted' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where start_buy_rate_predicted is null union 
select 'target_buy_rate_predicted' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where target_buy_rate_predicted is null union 
select 'confidence_predicted' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where confidence_predicted is null union 
select 'miles_predicted' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where miles_predicted is null union 
select 'rate_predicted_checked' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where rate_predicted_checked is null union 
select 'created_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where created_date is null union 
select 'last_updated_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where last_updated_date is null union 
select 'rate_predicted_request_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where rate_predicted_request_date is null union 
select 'high_buy_rate_gsn' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where high_buy_rate_gsn is null union 
select 'low_buy_rate_gsn' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where low_buy_rate_gsn is null union 
select 'start_buy_rate_gsn' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where start_buy_rate_gsn is null union 
select 'target_buy_rate_gsn' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where target_buy_rate_gsn is null union 
select 'confidence_level_gsn' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where confidence_level_gsn is null union 
select 'gsn_request_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where gsn_request_date is null union 
select 'gsn_predicted_checked' as columnname, count(1) as NullCount from bronze.Pricing_nfi_load_predictions where gsn_predicted_checked is null order by columnname 

-- COMMAND ----------

select 'id' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where id is null union 
select 'external_carrier_company_id' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where external_carrier_company_id is null union 
select 'driver_name' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where driver_name is null union 
select 'driver_phone' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where driver_phone is null union 
select 'trailer_number' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where trailer_number is null union 
select 'truck_number' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where truck_number is null union 
select 'commodity' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where commodity is null union 
select 'height' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where height is null union 
select 'length' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where length is null union 
select 'quantity' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where quantity is null union 
select 'ref_numbers' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where ref_numbers is null union 
select 'special_instruction' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where special_instruction is null union 
select 'status' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where status is null union 
select 'temperature_low' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where temperature_low is null union 
select 'temperature_high' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where temperature_high is null union 
select 'tord_flag' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where tord_flag is null union 
select 'weight' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where weight is null union 
select 'width' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where width is null union 
select 'external_load_id' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where external_load_id is null union 
select 'external_shipper_id' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where external_shipper_id is null union 
select 'shipper_name' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where shipper_name is null union 
select 'booked_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where booked_date is null union 
select 'miles' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where miles is null union 
select 'pickup_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where pickup_date is null union 
select 'accessorials' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where accessorials is null union 
select 'total_carrier_cost' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where total_carrier_cost is null union 
select 'total_carrier_rate' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where total_carrier_rate is null union 
select 'total_fuel_cost' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where total_fuel_cost is null union 
select 'total_linehaul_cost' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where total_linehaul_cost is null union 
select 'total_shipper_cost' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where total_shipper_cost is null union 
select 'transport_mode' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where transport_mode is null union 
select 'origin_city' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where origin_city is null union 
select 'origin_state' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where origin_state is null union 
select 'origin_kma' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where origin_kma is null union 
select 'origin_zip' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where origin_zip is null union 
select 'origin_country' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where origin_country is null union 
select 'destination_city' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where destination_city is null union 
select 'destination_state' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where destination_state is null union 
select 'destination_kma' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where destination_kma is null union 
select 'destination_zip' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where destination_zip is null union 
select 'destination_country' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where destination_country is null union 
select 'created_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where created_date is null union 
select 'last_updated_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where last_updated_date is null union 
select 'carrier_name' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where carrier_name is null union 
select 'carrier_phone' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where carrier_phone is null union 
select 'carrier_account_manager' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where carrier_account_manager is null union 
select 'covered_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where covered_date is null union 
select 'shipper_account_manager' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where shipper_account_manager is null union 
select 'with_extra_stops' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where with_extra_stops is null union 
select 'stop_count' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where stop_count is null union 
select 'tms_type' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where tms_type is null union 
select 'delivered_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where delivered_date is null union 
select 'office' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where office is null union 
select 'transport_type_customer' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where transport_type_customer is null union 
select 'transport_type' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where transport_type is null union 
select 'valid' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid is null union 
select 'currency' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where currency is null union 
select 'exchange_rate' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where exchange_rate is null union 
select 'valid_rate' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_rate is null union 
select 'valid_distance' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_distance is null union 
select 'exist_kma' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where exist_kma is null union 
select 'not_tord' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where not_tord is null union 
select 'valid_weight' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_weight is null union 
select 'valid_distance_with_rate' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_distance_with_rate is null union 
select 'valid_price' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_price is null union 
select 'meets_customer_filter' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where meets_customer_filter is null union 
select 'valid_transport_type' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_transport_type is null union 
select 'valid_country_and_zips' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_country_and_zips is null union 
select 'valid_status' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_status is null union 
select 'valid_pickup_date' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_pickup_date is null union 
select 'with_out_extra_stop' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where with_out_extra_stop is null union 
select 'valid_min_rate' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_min_rate is null union 
select 'valid_max_rate' as columnname, count(1) as NullCount from bronze.Pricing_nfi_loads where valid_max_rate is null order by columnname 

-- COMMAND ----------

select 'SOURCE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSRCCODE where SOURCE_CODE = ' ' union 
select 'DESCRIPTION' as columnname, count(1) as NullCount from bronze.FINANCE_GLSRCCODE where DESCRIPTION = ' ' union 
select 'SOURCE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSRCCODE where SOURCE_TYPE = ' ' union 
select 'EDIT_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSRCCODE where EDIT_TYPE = ' ' union 
select 'USED_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_GLSRCCODE where USED_FLAG = ' ' union 
select 'LAST_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_GLSRCCODE where LAST_SEQ = ' ' union 
select 'LAST_FIELD_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_GLSRCCODE where LAST_FIELD_NBR = ' ' union 
select 'PRIMARY_SYSTEM' as columnname, count(1) as NullCount from bronze.FINANCE_GLSRCCODE where PRIMARY_SYSTEM = ' ' union 
select 'SOURCE_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_GLSRCCODE where SOURCE_GROUP = ' ' order by columnname

-- COMMAND ----------

select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where COMPANY = ' ' union 
select 'ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where ACCT_UNIT = ' ' union 
select 'VAR_LEVELS' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where VAR_LEVELS = ' ' union 
select 'DESCRIPTION' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where DESCRIPTION = ' ' union 
select 'CHART_SECTION' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where CHART_SECTION = ' ' union 
select 'CURRENCY_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where CURRENCY_CODE = ' ' union 
select 'PERSON_RESP' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where PERSON_RESP = ' ' union 
select 'ACTIVE_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where ACTIVE_STATUS = ' ' union 
select 'ACCT_GENERATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where ACCT_GENERATE = ' ' union 
select 'POSTING_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where POSTING_FLAG = ' ' union 
select 'VAR_LEVEL_DISP' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where VAR_LEVEL_DISP = ' ' union 
select 'LEVEL_DEPTH' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where LEVEL_DEPTH = ' ' union 
select 'LEVEL_DETAIL_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where LEVEL_DETAIL_01 = ' ' union 
select 'LEVEL_DETAIL_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where LEVEL_DETAIL_02 = ' ' union 
select 'LEVEL_DETAIL_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where LEVEL_DETAIL_03 = ' ' union 
select 'LEVEL_DETAIL_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where LEVEL_DETAIL_04 = ' ' union 
select 'LEVEL_DETAIL_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where LEVEL_DETAIL_05 = ' ' union 
select 'OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where OBJ_ID = ' ' union 
select 'PARENT_OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where PARENT_OBJ_ID = ' ' union 
select 'CREATED_BY' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where CREATED_BY = ' ' union 
select 'LAST_UPDT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where LAST_UPDT_DATE = ' ' union 
select 'LAST_UPDT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where LAST_UPDT_TIME is null union 
select 'LAST_UPDATE_BY' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where LAST_UPDATE_BY = ' ' union 
select 'L_INDEX' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where L_INDEX = ' ' union 
select 'GLNSET3_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where GLNSET3_SS_SW = ' ' union 
select 'L_ATGLN_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_GLNAMES where L_ATGLN_SS_SW = ' ' order by columnname 

-- COMMAND ----------

select 'VENDOR_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where VENDOR_GROUP = ' ' union 
select 'VEN_CLASS' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where VEN_CLASS = ' ' union 
select 'VEN_PRIORITY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where VEN_PRIORITY = ' ' union 
select 'DESCRIPTION' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where DESCRIPTION = ' ' union 
select 'ZERO_CHECK' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where ZERO_CHECK = ' ' union 
select 'POST_OPTION' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where POST_OPTION = ' ' union 
select 'DIST_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where DIST_CODE = ' ' union 
select 'MAX_PMT_HOLD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where MAX_PMT_HOLD = ' ' union 
select 'INCOME_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where INCOME_CODE = ' ' union 
select 'CURRENCY_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where CURRENCY_CODE = ' ' union 
select 'TAX_EXEMPT_CD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where TAX_EXEMPT_CD = ' ' union 
select 'TAX_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where TAX_CODE = ' ' union 
select 'CASH_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where CASH_CODE = ' ' union 
select 'BANK_INST_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where BANK_INST_CODE = ' ' union 
select 'ZERO_PMT_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where ZERO_PMT_CODE = ' ' union 
select 'ENCLOSURE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where ENCLOSURE = ' ' union 
select 'TAX_USAGE_CD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where TAX_USAGE_CD = ' ' union 
select 'HANDLING_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where HANDLING_CODE = ' ' union 
select 'VALIDATE_PO' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where VALIDATE_PO = ' ' union 
select 'REQUIRE_PO' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where REQUIRE_PO = ' ' union 
select 'MTCH_PREPAY_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where MTCH_PREPAY_FL = ' ' union 
select 'MTCH_PREPAY_MT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where MTCH_PREPAY_MT = ' ' union 
select 'MATCH_TABLE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where MATCH_TABLE = ' ' union 
select 'RULE_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where RULE_GROUP = ' ' union 
select 'RETAIL_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where RETAIL_FLAG = ' ' union 
select 'EAM_VEN_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where EAM_VEN_FL = ' ' union 
select 'MIN_PMT_HOLD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where MIN_PMT_HOLD = ' ' union 
select 'TAX_OVERRIDE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where TAX_OVERRIDE = ' ' union 
select 'MAX_PMT_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where MAX_PMT_AMT = ' ' union 
select 'MIN_PMT_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENCLASS where MIN_PMT_AMT = ' ' order by columnname 

-- COMMAND ----------

select 'CHART_NAME' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where CHART_NAME = ' ' union 
select 'SUMRY_ACCT_ID' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where SUMRY_ACCT_ID = ' ' union 
select 'CHART_SECTION' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where CHART_SECTION = ' ' union 
select 'ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where ACCOUNT = ' ' union 
select 'SUB_ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where SUB_ACCOUNT = ' ' union 
select 'ACCOUNT_DESC' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where ACCOUNT_DESC = ' ' union 
select 'SPACE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where SPACE_CODE = ' ' union 
select 'SIGN_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where SIGN_CODE = ' ' union 
select 'UNITS_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where UNITS_CODE = ' ' union 
select 'ACTIVITY_REQ' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where ACTIVITY_REQ = ' ' union 
select 'POSTING_LEVEL' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where POSTING_LEVEL = ' ' union 
select 'CURRENCY_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where CURRENCY_CODE = ' ' union 
select 'CURR_CONTROL' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where CURR_CONTROL = ' ' union 
select 'TRANSL_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where TRANSL_CODE = ' ' union 
select 'REVALUE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where REVALUE = ' ' union 
select 'ACTIVE_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where ACTIVE_STATUS = ' ' union 
select 'DYNAMIC_LIST' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where DYNAMIC_LIST = ' ' union 
select 'RESTRICT_SYS' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where RESTRICT_SYS = ' ' union 
select 'ACCT_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where ACCT_CATEGORY = ' ' union 
select 'DAILY_BALANCE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where DAILY_BALANCE = ' ' union 
select 'RELATION' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where RELATION = ' ' union 
select 'UNARY_OPER' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where UNARY_OPER = ' ' union 
select 'NAT_BALANCE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where NAT_BALANCE = ' ' union 
select 'SL_ACCT_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where SL_ACCT_FLAG = ' ' union 
select 'LEDGER_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where LEDGER_GROUP = ' ' union 
select 'GL_BUD_FL' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where GL_BUD_FL = ' ' union 
select 'GL_ACCT_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where GL_ACCT_TYPE = ' ' union 
select 'XBRL_TAG_ID' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where XBRL_TAG_ID = ' ' union 
select 'CREATED_BY' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where CREATED_BY = ' ' union 
select 'LAST_UPDT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where LAST_UPDT_DATE = ' ' union 
select 'LAST_UPDT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where LAST_UPDT_TIME = ' ' union 
select 'LAST_UPDATE_BY' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where LAST_UPDATE_BY = ' ' union 
select 'GDTSET6_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where GDTSET6_SS_SW = ' ' union 
select 'GDTSET7_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where GDTSET7_SS_SW = ' ' union 
select 'OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_GLCHARTDTL where OBJ_ID = ' ' ORDER BY columnname 

-- COMMAND ----------

select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where COMPANY = ' ' union 
select 'FISCAL_YEAR' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where FISCAL_YEAR = ' ' union 
select 'ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACCT_UNIT = ' ' union 
select 'ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACCOUNT = ' ' union 
select 'SUB_ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where SUB_ACCOUNT = ' ' union 
select 'VAR_LEVELS' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where VAR_LEVELS = ' ' union 
select 'CHART_NAME' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CHART_NAME = ' ' union 
select 'ACTIVITY_FLAG_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_01 = ' ' union 
select 'ACTIVITY_FLAG_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_02 = ' ' union 
select 'ACTIVITY_FLAG_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_03 = ' ' union 
select 'ACTIVITY_FLAG_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_04 = ' ' union 
select 'ACTIVITY_FLAG_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_05 = ' ' union 
select 'ACTIVITY_FLAG_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_06 = ' ' union 
select 'ACTIVITY_FLAG_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_07 = ' ' union 
select 'ACTIVITY_FLAG_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_08 = ' ' union 
select 'ACTIVITY_FLAG_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_09 = ' ' union 
select 'ACTIVITY_FLAG_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_10 = ' ' union 
select 'ACTIVITY_FLAG_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_11 = ' ' union 
select 'ACTIVITY_FLAG_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_12 = ' ' union 
select 'ACTIVITY_FLAG_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where ACTIVITY_FLAG_13 = ' ' union 
select 'MAINT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where MAINT_DATE = ' ' union 
select 'PARENT_VAR_LEV' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where PARENT_VAR_LEV = ' ' union 
select 'DB_BEG_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_BEG_BAL = ' ' union 
select 'CR_BEG_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_BEG_BAL = ' ' union 
select 'DB_AMOUNT_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_01 = ' ' union 
select 'DB_AMOUNT_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_02 = ' ' union 
select 'DB_AMOUNT_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_03 = ' ' union 
select 'DB_AMOUNT_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_04 = ' ' union 
select 'DB_AMOUNT_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_05 = ' ' union 
select 'DB_AMOUNT_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_06 = ' ' union 
select 'DB_AMOUNT_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_07 = ' ' union 
select 'DB_AMOUNT_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_08 = ' ' union 
select 'DB_AMOUNT_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_09 = ' ' union 
select 'DB_AMOUNT_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_10 = ' ' union 
select 'DB_AMOUNT_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_11 = ' ' union 
select 'DB_AMOUNT_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_12 = ' ' union 
select 'DB_AMOUNT_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where DB_AMOUNT_13 = ' ' union 
select 'CR_AMOUNT_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_01 = ' ' union 
select 'CR_AMOUNT_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_02 = ' ' union 
select 'CR_AMOUNT_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_03 = ' ' union 
select 'CR_AMOUNT_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_04 = ' ' union 
select 'CR_AMOUNT_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_05 = ' ' union 
select 'CR_AMOUNT_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_06 = ' ' union 
select 'CR_AMOUNT_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_07 = ' ' union 
select 'CR_AMOUNT_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_08 = ' ' union 
select 'CR_AMOUNT_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_09 = ' ' union 
select 'CR_AMOUNT_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_10 = ' ' union 
select 'CR_AMOUNT_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_11 = ' ' union 
select 'CR_AMOUNT_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_12 = ' ' union 
select 'CR_AMOUNT_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where CR_AMOUNT_13 = ' ' union 
select 'MAINT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_GLAMOUNTS where MAINT_TIME = ' ' order by columnname 

-- COMMAND ----------

select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where COMPANY = ' ' union 
select 'FISCAL_YEAR' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where FISCAL_YEAR = ' ' union 
select 'ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACCT_UNIT = ' ' union 
select 'ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACCOUNT = ' ' union 
select 'SUB_ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where SUB_ACCOUNT = ' ' union 
select 'VAR_LEVELS' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where VAR_LEVELS = ' ' union 
select 'CHART_NAME' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CHART_NAME = ' ' union 
select 'ACTIVITY_FLAG_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_01 = ' ' union 
select 'ACTIVITY_FLAG_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_02 = ' ' union 
select 'ACTIVITY_FLAG_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_03 = ' ' union 
select 'ACTIVITY_FLAG_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_04 = ' ' union 
select 'ACTIVITY_FLAG_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_05 = ' ' union 
select 'ACTIVITY_FLAG_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_06 = ' ' union 
select 'ACTIVITY_FLAG_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_07 = ' ' union 
select 'ACTIVITY_FLAG_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_08 = ' ' union 
select 'ACTIVITY_FLAG_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_09 = ' ' union 
select 'ACTIVITY_FLAG_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_10 = ' ' union 
select 'ACTIVITY_FLAG_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_11 = ' ' union 
select 'ACTIVITY_FLAG_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_12 = ' ' union 
select 'ACTIVITY_FLAG_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_13 = ' ' union 
select 'MAINT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where MAINT_DATE = ' ' union 
select 'PARENT_VAR_LEV' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where PARENT_VAR_LEV = ' ' union 
select 'DB_BEG_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_BEG_BAL = ' ' union 
select 'CR_BEG_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_BEG_BAL = ' ' union 
select 'DB_UNITS_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_01 = ' ' union 
select 'DB_UNITS_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_02 = ' ' union 
select 'DB_UNITS_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_03 = ' ' union 
select 'DB_UNITS_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_04 = ' ' union 
select 'DB_UNITS_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_05 = ' ' union 
select 'DB_UNITS_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_06 = ' ' union 
select 'DB_UNITS_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_07 = ' ' union 
select 'DB_UNITS_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_08 = ' ' union 
select 'DB_UNITS_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_09 = ' ' union 
select 'DB_UNITS_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_10 = ' ' union 
select 'DB_UNITS_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_11 = ' ' union 
select 'DB_UNITS_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_12 = ' ' union 
select 'DB_UNITS_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_13 = ' ' union 
select 'CR_UNITS_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_01 = ' ' union 
select 'CR_UNITS_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_02 = ' ' union 
select 'CR_UNITS_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_03 = ' ' union 
select 'CR_UNITS_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_04 = ' ' union 
select 'CR_UNITS_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_05 = ' ' union 
select 'CR_UNITS_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_06 = ' ' union 
select 'CR_UNITS_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_07 = ' ' union 
select 'CR_UNITS_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_08 = ' ' union 
select 'CR_UNITS_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_09 = ' ' union 
select 'CR_UNITS_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_10 = ' ' union 
select 'CR_UNITS_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_11 = ' ' union 
select 'CR_UNITS_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_12 = ' ' union 
select 'CR_UNITS_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_13 = ' ' union 
select 'MAINT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where MAINT_TIME = ' ' order by columnname 

-- COMMAND ----------

select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where COMPANY = ' ' union 
select 'NAME' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where NAME = ' ' union 
select 'BS_ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BS_ACCT_UNIT = ' ' union 
select 'NBR_ACCT_PERS' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where NBR_ACCT_PERS = ' ' union 
select 'ACCT_PERIOD' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where ACCT_PERIOD = ' ' union 
select 'FISCAL_YEAR' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where FISCAL_YEAR = ' ' union 
select 'LST_YR_CLOSED' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LST_YR_CLOSED = ' ' union 
select 'PER_END_DATE_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_01 = ' ' union 
select 'PER_END_DATE_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_02 = ' ' union 
select 'PER_END_DATE_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_03 = ' ' union 
select 'PER_END_DATE_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_04 = ' ' union 
select 'PER_END_DATE_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_05 = ' ' union 
select 'PER_END_DATE_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_06 = ' ' union 
select 'PER_END_DATE_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_07 = ' ' union 
select 'PER_END_DATE_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_08 = ' ' union 
select 'PER_END_DATE_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_09 = ' ' union 
select 'PER_END_DATE_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_10 = ' ' union 
select 'PER_END_DATE_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_11 = ' ' union 
select 'PER_END_DATE_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_12 = ' ' union 
select 'PER_END_DATE_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_13 = ' ' union 
select 'PER_END_DATE_14' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_14 = ' ' union 
select 'PER_END_DATE_15' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_15 = ' ' union 
select 'PER_END_DATE_16' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_16 = ' ' union 
select 'PER_END_DATE_17' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_17 = ' ' union 
select 'PER_END_DATE_18' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_18 = ' ' union 
select 'PER_END_DATE_19' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_19 = ' ' union 
select 'PER_END_DATE_20' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_20 = ' ' union 
select 'PER_END_DATE_21' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_21 = ' ' union 
select 'PER_END_DATE_22' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_22 = ' ' union 
select 'PER_END_DATE_23' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_23 = ' ' union 
select 'PER_END_DATE_24' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_24 = ' ' union 
select 'PER_END_DATE_25' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_25 = ' ' union 
select 'PER_END_DATE_26' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_26 = ' ' union 
select 'PER_END_DATE_27' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_27 = ' ' union 
select 'PER_END_DATE_28' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_28 = ' ' union 
select 'PER_END_DATE_29' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_29 = ' ' union 
select 'PER_END_DATE_30' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_30 = ' ' union 
select 'PER_END_DATE_31' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_31 = ' ' union 
select 'PER_END_DATE_32' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_32 = ' ' union 
select 'PER_END_DATE_33' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_33 = ' ' union 
select 'PER_END_DATE_34' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_34 = ' ' union 
select 'PER_END_DATE_35' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_35 = ' ' union 
select 'PER_END_DATE_36' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_36 = ' ' union 
select 'PER_END_DATE_37' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_37 = ' ' union 
select 'PER_END_DATE_38' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_38 = ' ' union 
select 'PER_END_DATE_39' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_DATE_39 = ' ' union 
select 'PER_END_CODE_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_01 = ' ' union 
select 'PER_END_CODE_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_02 = ' ' union 
select 'PER_END_CODE_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_03 = ' ' union 
select 'PER_END_CODE_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_04 = ' ' union 
select 'PER_END_CODE_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_05 = ' ' union 
select 'PER_END_CODE_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_06 = ' ' union 
select 'PER_END_CODE_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_07 = ' ' union 
select 'PER_END_CODE_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_08 = ' ' union 
select 'PER_END_CODE_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_09 = ' ' union 
select 'PER_END_CODE_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_10 = ' ' union 
select 'PER_END_CODE_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_11 = ' ' union 
select 'PER_END_CODE_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_12 = ' ' union 
select 'PER_END_CODE_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_13 = ' ' union 
select 'PER_END_CODE_14' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_14 = ' ' union 
select 'PER_END_CODE_15' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_15 = ' ' union 
select 'PER_END_CODE_16' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_16 = ' ' union 
select 'PER_END_CODE_17' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_17 = ' ' union 
select 'PER_END_CODE_18' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_18 = ' ' union 
select 'PER_END_CODE_19' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_19 = ' ' union 
select 'PER_END_CODE_20' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_20 = ' ' union 
select 'PER_END_CODE_21' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_21 = ' ' union 
select 'PER_END_CODE_22' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_22 = ' ' union 
select 'PER_END_CODE_23' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_23 = ' ' union 
select 'PER_END_CODE_24' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_24 = ' ' union 
select 'PER_END_CODE_25' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_25 = ' ' union 
select 'PER_END_CODE_26' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_26 = ' ' union 
select 'PER_END_CODE_27' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_27 = ' ' union 
select 'PER_END_CODE_28' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_28 = ' ' union 
select 'PER_END_CODE_29' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_29 = ' ' union 
select 'PER_END_CODE_30' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_30 = ' ' union 
select 'PER_END_CODE_31' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_31 = ' ' union 
select 'PER_END_CODE_32' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_32 = ' ' union 
select 'PER_END_CODE_33' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_33 = ' ' union 
select 'PER_END_CODE_34' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_34 = ' ' union 
select 'PER_END_CODE_35' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_35 = ' ' union 
select 'PER_END_CODE_36' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_36 = ' ' union 
select 'PER_END_CODE_37' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_37 = ' ' union 
select 'PER_END_CODE_38' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_38 = ' ' union 
select 'PER_END_CODE_39' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PER_END_CODE_39 = ' ' union 
select 'CONTROL_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CONTROL_CODE = ' ' union 
select 'INTLV_BAL_FLG' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where INTLV_BAL_FLG = ' ' union 
select 'CURRENCY_TABLE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURRENCY_TABLE = ' ' union 
select 'CURR_CONV_FLG' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURR_CONV_FLG = ' ' union 
select 'CURR_TRAN_FLG' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURR_TRAN_FLG = ' ' union 
select 'CURRENCY_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURRENCY_CODE = ' ' union 
select 'COMPANY_ND' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where COMPANY_ND = ' ' union 
select 'IS_ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where IS_ACCT_UNIT = ' ' union 
select 'ACTIVE_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where ACTIVE_STATUS = ' ' union 
select 'AVG_DAILY_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where AVG_DAILY_BAL = ' ' union 
select 'AUTO_JE_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where AUTO_JE_NBR = ' ' union 
select 'LEVEL_DESC_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LEVEL_DESC_01 = ' ' union 
select 'LEVEL_DESC_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LEVEL_DESC_02 = ' ' union 
select 'LEVEL_DESC_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LEVEL_DESC_03 = ' ' union 
select 'LEVEL_DESC_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LEVEL_DESC_04 = ' ' union 
select 'LEVEL_DESC_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LEVEL_DESC_05 = ' ' union 
select 'NBR_DIGITS_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where NBR_DIGITS_01 = ' ' union 
select 'NBR_DIGITS_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where NBR_DIGITS_02 = ' ' union 
select 'NBR_DIGITS_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where NBR_DIGITS_03 = ' ' union 
select 'NBR_DIGITS_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where NBR_DIGITS_04 = ' ' union 
select 'NBR_DIGITS_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where NBR_DIGITS_05 = ' ' union 
select 'GL190_RUN_DT' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where GL190_RUN_DT = ' ' union 
select 'POSTED_TH_DT' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where POSTED_TH_DT = ' ' union 
select 'PROJ_ACCT_FLG' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where PROJ_ACCT_FLG = ' ' union 
select 'BASE_ZONE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BASE_ZONE = ' ' union 
select 'BACKPOST_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_01 = ' ' union 
select 'BACKPOST_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_02 = ' ' union 
select 'BACKPOST_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_03 = ' ' union 
select 'BACKPOST_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_04 = ' ' union 
select 'BACKPOST_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_05 = ' ' union 
select 'BACKPOST_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_06 = ' ' union 
select 'BACKPOST_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_07 = ' ' union 
select 'BACKPOST_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_08 = ' ' union 
select 'BACKPOST_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_09 = ' ' union 
select 'BACKPOST_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_10 = ' ' union 
select 'BACKPOST_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_11 = ' ' union 
select 'BACKPOST_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_12 = ' ' union 
select 'BACKPOST_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_13 = ' ' union 
select 'BACKPOST_14' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_14 = ' ' union 
select 'BACKPOST_15' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_15 = ' ' union 
select 'BACKPOST_16' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_16 = ' ' union 
select 'BACKPOST_17' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_17 = ' ' union 
select 'BACKPOST_18' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_18 = ' ' union 
select 'BACKPOST_19' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_19 = ' ' union 
select 'BACKPOST_20' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_20 = ' ' union 
select 'BACKPOST_21' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_21 = ' ' union 
select 'BACKPOST_22' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_22 = ' ' union 
select 'BACKPOST_23' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_23 = ' ' union 
select 'BACKPOST_24' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_24 = ' ' union 
select 'BACKPOST_25' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_25 = ' ' union 
select 'BACKPOST_26' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BACKPOST_26 = ' ' union 
select 'GL150_RUN_DTE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where GL150_RUN_DTE = ' ' union 
select 'CHART_NAME' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CHART_NAME = ' ' union 
select 'DEFAULT_PERIOD' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where DEFAULT_PERIOD = ' ' union 
select 'JOURNAL_BY_DOC' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where JOURNAL_BY_DOC = ' ' union 
select 'UPDATING' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where UPDATING = ' ' union 
select 'CLOSE_SEQ_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_01 = ' ' union 
select 'CLOSE_SEQ_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_02 = ' ' union 
select 'CLOSE_SEQ_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_03 = ' ' union 
select 'CLOSE_SEQ_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_04 = ' ' union 
select 'CLOSE_SEQ_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_05 = ' ' union 
select 'CLOSE_SEQ_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_06 = ' ' union 
select 'CLOSE_SEQ_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_07 = ' ' union 
select 'CLOSE_SEQ_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_08 = ' ' union 
select 'CLOSE_SEQ_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_09 = ' ' union 
select 'CLOSE_SEQ_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_10 = ' ' union 
select 'CLOSE_SEQ_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_11 = ' ' union 
select 'CLOSE_SEQ_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_12 = ' ' union 
select 'CLOSE_SEQ_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_13 = ' ' union 
select 'CLOSE_SEQ_14' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_14 = ' ' union 
select 'CLOSE_SEQ_15' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_15 = ' ' union 
select 'CLOSE_SEQ_16' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_16 = ' ' union 
select 'CLOSE_SEQ_17' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_17 = ' ' union 
select 'CLOSE_SEQ_18' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_18 = ' ' union 
select 'CLOSE_SEQ_19' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_19 = ' ' union 
select 'CLOSE_SEQ_20' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_20 = ' ' union 
select 'CLOSE_SEQ_21' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_21 = ' ' union 
select 'CLOSE_SEQ_22' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_22 = ' ' union 
select 'CLOSE_SEQ_23' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_23 = ' ' union 
select 'CLOSE_SEQ_24' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_24 = ' ' union 
select 'CLOSE_SEQ_25' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_25 = ' ' union 
select 'CLOSE_SEQ_26' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_26 = ' ' union 
select 'CLOSE_SEQ_27' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_27 = ' ' union 
select 'CLOSE_SEQ_28' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_28 = ' ' union 
select 'CLOSE_SEQ_29' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_29 = ' ' union 
select 'CLOSE_SEQ_30' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_30 = ' ' union 
select 'CLOSE_SEQ_31' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_31 = ' ' union 
select 'CLOSE_SEQ_32' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_32 = ' ' union 
select 'CLOSE_SEQ_33' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_33 = ' ' union 
select 'CLOSE_SEQ_34' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_34 = ' ' union 
select 'CLOSE_SEQ_35' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_35 = ' ' union 
select 'CLOSE_SEQ_36' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_36 = ' ' union 
select 'CLOSE_SEQ_37' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_37 = ' ' union 
select 'CLOSE_SEQ_38' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_38 = ' ' union 
select 'CLOSE_SEQ_39' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CLOSE_SEQ_39 = ' ' union 
select 'LEVEL_DEPTH' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LEVEL_DEPTH = ' ' union 
select 'JRNL_BOOK_REQ' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where JRNL_BOOK_REQ = ' ' union 
select 'SEQ_TRNS_CD' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where SEQ_TRNS_CD = ' ' union 
select 'RESET_SEQ_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where RESET_SEQ_NBR = ' ' union 
select 'CHG_POST_TRAN' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CHG_POST_TRAN = ' ' union 
select 'CHG_REL_TRAN' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CHG_REL_TRAN = ' ' union 
select 'ERROR_SP_JBOOK' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where ERROR_SP_JBOOK = ' ' union 
select 'YEAR_END_JBOOK' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where YEAR_END_JBOOK = ' ' union 
select 'AUTO_JBK_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where AUTO_JBK_SEQ = ' ' union 
select 'WORKFLOW_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where WORKFLOW_FLAG = ' ' union 
select 'CURR_NAME_1' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURR_NAME_1 = ' ' union 
select 'CURR_CODE_1' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURR_CODE_1 = ' ' union 
select 'CURR_CODE_ND_1' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURR_CODE_ND_1 = ' ' union 
select 'CURR_NAME_2' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURR_NAME_2 = ' ' union 
select 'CURR_CODE_2' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURR_CODE_2 = ' ' union 
select 'CURR_CODE_ND_2' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURR_CODE_ND_2 = ' ' union 
select 'AUTO_BASE_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where AUTO_BASE_BAL = ' ' union 
select 'CURR_RE_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CURR_RE_FLAG = ' ' union 
select 'BUD_EDIT_RANGE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BUD_EDIT_RANGE = ' ' union 
select 'BUD_EDIT_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BUD_EDIT_TYPE = ' ' union 
select 'FB_UPDATING' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where FB_UPDATING = ' ' union 
select 'ML_UPDATING' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where ML_UPDATING = ' ' union 
select 'BUD_ACT' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BUD_ACT = ' ' union 
select 'CREATED_BY' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where CREATED_BY = ' ' union 
select 'LAST_UPDT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LAST_UPDT_DATE = ' ' union 
select 'LAST_UPDT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LAST_UPDT_TIME = ' ' union 
select 'LAST_UPDATE_BY' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where LAST_UPDATE_BY = ' ' union 
select 'Z4_RPT_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where Z4_RPT_FLAG = ' ' union 
select 'GL190_RUN_TM' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where GL190_RUN_TM = ' ' union 
select 'JE_APPROVE_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where JE_APPROVE_AMT = ' ' union 
select 'BUD_TOLERANCE' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where BUD_TOLERANCE = ' ' union 
select 'Z4_MIN_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where Z4_MIN_AMT = ' ' union 
select 'TRANS_NBR_LOC' as columnname, count(1) as NullCount from bronze.FINANCE_GLSYSTEM where TRANS_NBR_LOC = ' ' order by columnname 

-- COMMAND ----------

select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where COMPANY = ' ' union 
select 'FISCAL_YEAR' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where FISCAL_YEAR = ' ' union 
select 'ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACCT_UNIT = ' ' union 
select 'ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACCOUNT = ' ' union 
select 'SUB_ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where SUB_ACCOUNT = ' ' union 
select 'VAR_LEVELS' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where VAR_LEVELS = ' ' union 
select 'CHART_NAME' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CHART_NAME = ' ' union 
select 'ACTIVITY_FLAG_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_01 = ' ' union 
select 'ACTIVITY_FLAG_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_02 = ' ' union 
select 'ACTIVITY_FLAG_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_03 = ' ' union 
select 'ACTIVITY_FLAG_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_04 = ' ' union 
select 'ACTIVITY_FLAG_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_05 = ' ' union 
select 'ACTIVITY_FLAG_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_06 = ' ' union 
select 'ACTIVITY_FLAG_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_07 = ' ' union 
select 'ACTIVITY_FLAG_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_08 = ' ' union 
select 'ACTIVITY_FLAG_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_09 = ' ' union 
select 'ACTIVITY_FLAG_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_10 = ' ' union 
select 'ACTIVITY_FLAG_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_11 = ' ' union 
select 'ACTIVITY_FLAG_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_12 = ' ' union 
select 'ACTIVITY_FLAG_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where ACTIVITY_FLAG_13 = ' ' union 
select 'MAINT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where MAINT_DATE = ' ' union 
select 'PARENT_VAR_LEV' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where PARENT_VAR_LEV = ' ' union 


select 'DB_BEG_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_BEG_BAL = ' ' union 
select 'CR_BEG_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_BEG_BAL = ' ' union 
select 'DB_UNITS_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_01 = ' ' union 
select 'DB_UNITS_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_02 = ' ' union 
select 'DB_UNITS_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_03 = ' ' union 
select 'DB_UNITS_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_04 = ' ' union 
select 'DB_UNITS_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_05 = ' ' union 
select 'DB_UNITS_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_06 = ' ' union 
select 'DB_UNITS_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_07 = ' ' union 
select 'DB_UNITS_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_08 = ' ' union 
select 'DB_UNITS_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_09 = ' ' union 
select 'DB_UNITS_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_10 = ' ' union 
select 'DB_UNITS_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_11 = ' ' union 
select 'DB_UNITS_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_12 = ' ' union 
select 'DB_UNITS_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where DB_UNITS_13 = ' ' union 
select 'CR_UNITS_01' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_01 = ' ' union 
select 'CR_UNITS_02' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_02 = ' ' union 
select 'CR_UNITS_03' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_03 = ' ' union 
select 'CR_UNITS_04' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_04 = ' ' union 
select 'CR_UNITS_05' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_05 = ' ' union 
select 'CR_UNITS_06' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_06 = ' ' union 
select 'CR_UNITS_07' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_07 = ' ' union 
select 'CR_UNITS_08' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_08 = ' ' union 
select 'CR_UNITS_09' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_09 = ' ' union 
select 'CR_UNITS_10' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_10 = ' ' union 
select 'CR_UNITS_11' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_11 = ' ' union 
select 'CR_UNITS_12' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_12 = ' ' union 
select 'CR_UNITS_13' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where CR_UNITS_13 = ' ' union 
select 'MAINT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_GLUNITS where MAINT_TIME = ' '  order by columnname

-- COMMAND ----------

-- select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where COMPANY = ' ' union 
-- select 'FISCAL_YEAR' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where FISCAL_YEAR = ' ' union 
-- select 'ACCT_PERIOD' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ACCT_PERIOD = ' ' union 
-- select 'CONTROL_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where CONTROL_GROUP = ' ' union 
-- select 'SYSTEM' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where SYSTEM = ' ' union 
-- select 'JE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where JE_TYPE = ' ' union 
-- select 'JE_SEQUENCE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where JE_SEQUENCE = ' ' union 
-- select 'LINE_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where LINE_NBR = ' ' union 
-- select 'STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where STATUS = ' ' union 
-- select 'VAR_LEVELS' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where VAR_LEVELS = ' ' union 
-- select 'ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ACCT_UNIT = ' ' union 
-- select 'ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ACCOUNT = ' ' union 
-- select 'SUB_ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where SUB_ACCOUNT = ' ' union 
-- select 'SOURCE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where SOURCE_CODE = ' ' union 
-- select 'R_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where R_DATE = ' ' union 
-- select 'REFERENCE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where REFERENCE = ' ' union 
-- select 'DESCRIPTION' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where DESCRIPTION = ' ' union 
-- select 'BASE_ND' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where BASE_ND = ' ' union 
-- select 'AUTO_REV' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where AUTO_REV = ' ' union 
-- select 'TO_COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where TO_COMPANY = ' ' union 
-- select 'BASE_ZONE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where BASE_ZONE = ' ' 
-- order by columnname
-- select 'POSTING_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where POSTING_DATE = ' ' union 
-- select 'ACTIVITY' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ACTIVITY = ' ' union 
-- select 'ACCT_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ACCT_CATEGORY = ' ' union 
-- select 'CURRENCY_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where CURRENCY_CODE = ' ' union 
-- select 'TRAN_ND' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where TRAN_ND = ' ' union 
-- select 'ACCT_CURRENCY' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ACCT_CURRENCY = ' ' union 
-- select 'ACCT_ND' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ACCT_ND = ' ' union 
-- select 'ORIG_COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ORIG_COMPANY = ' ' union 
-- select 'ORIG_PROGRAM' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ORIG_PROGRAM = ' ' union 
-- select 'OPERATOR' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where OPERATOR = ' ' union 
-- select 'TO_BASE_ND' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where TO_BASE_ND = ' ' union 
-- select 'TO_BASE_CURR' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where TO_BASE_CURR = ' ' union 
-- select 'RECONCILE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where RECONCILE = ' ' union 
-- select 'NEGATIVE_ADJ' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where NEGATIVE_ADJ = ' ' union 
-- select 'EFFECT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where EFFECT_DATE = ' '  order by columnname 
-- select 'UPDATE_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where UPDATE_DATE = ' ' union 
-- select 'CONSOL_AC_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where CONSOL_AC_FLAG = ' ' union 
-- select 'CT_FR_COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where CT_FR_COMPANY = ' ' union 
-- select 'RPT_ND_1' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where RPT_ND_1 = ' ' union 
-- select 'RPT_ND_2' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where RPT_ND_2 = ' ' union 
-- select 'CREATED_BY' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where CREATED_BY = ' ' union 
-- select 'LAST_UPDT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where LAST_UPDT_DATE = ' ' union 
-- select 'LAST_UPDT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where LAST_UPDT_TIME = ' ' union 
-- select 'LAST_UPDATE_BY' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where LAST_UPDATE_BY = ' ' union 
-- select 'L_INDEX' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where L_INDEX = ' ' union 
-- select 'GLTSET2_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where GLTSET2_SS_SW = ' ' union 
-- select 'GLTSET3_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where GLTSET3_SS_SW = ' ' union 
-- select 'GLTSET6_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where GLTSET6_SS_SW = ' ' union 
-- select 'GLTSET8_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where GLTSET8_SS_SW = ' ' union 
-- select 'L_ATGLT_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where L_ATGLT_SS_SW = ' ' 
-- order by columnname

select 'OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where OBJ_ID = ' ' union 
select 'BASE_AMOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where BASE_AMOUNT = ' ' union 
select 'UNITS_AMOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where UNITS_AMOUNT = ' ' union 
select 'BASERATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where BASERATE = ' ' union 
select 'TRAN_AMOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where TRAN_AMOUNT = ' ' union 
select 'ACCT_RATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ACCT_RATE = ' ' union 
select 'ACCT_AMOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where ACCT_AMOUNT = ' ' union 
select 'TO_BASE_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where TO_BASE_AMT = ' ' union 
select 'TO_BASERATE' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where TO_BASERATE = ' ' union 
select 'SEQ_TRNS_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where SEQ_TRNS_NBR = ' ' union 
select 'JBK_TRNS_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where JBK_TRNS_NBR = ' ' union 
select 'RPT_AMOUNT_1' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where RPT_AMOUNT_1 = ' ' union 
select 'RPT_RATE_1' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where RPT_RATE_1 = ' ' union 
select 'RPT_AMOUNT_2' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where RPT_AMOUNT_2 = ' ' union 
select 'RPT_RATE_2' as columnname, count(1) as NullCount from bronze.FINANCE_GLTRANS where RPT_RATE_2 = ' ' order by columnname

-- COMMAND ----------

select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where COMPANY = ' ' union 
select 'VENDOR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where VENDOR = ' ' union 
select 'INVOICE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where INVOICE = ' ' union 
select 'SUFFIX' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where SUFFIX = ' ' union 
select 'CANCEL_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where CANCEL_SEQ = ' ' union 
select 'ORIG_DIST_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ORIG_DIST_SEQ = ' ' union 
select 'DIST_SEQ_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_SEQ_NBR = ' ' union 
select 'DIST_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_TYPE = ' ' union 
select 'PROC_LEVEL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PROC_LEVEL = ' ' union 
select 'POST_OPTION' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where POST_OPTION = ' ' union 
select 'INVOICE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where INVOICE_TYPE = ' ' union 
select 'REC_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where REC_STATUS = ' ' union 
select 'INV_CURRENCY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where INV_CURRENCY = ' ' union 
select 'BASE_ND' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where BASE_ND = ' ' union 
select 'TRAN_ND' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TRAN_ND = ' ' union 
select 'TO_BASE_ND' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TO_BASE_ND = ' ' union 
select 'DIST_COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_COMPANY = ' ' 
order by columnname 
-- select 'DIS_ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIS_ACCT_UNIT = ' ' union 
-- select 'DIS_ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIS_ACCOUNT = ' ' union 
-- select 'DIS_SUB_ACCT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIS_SUB_ACCT = ' ' union 
-- select 'DISTRIB_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DISTRIB_DATE = ' ' union 
-- select 'TAX_INDICATOR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_INDICATOR = ' ' union 
-- select 'TAX_SEQ_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_SEQ_NBR = ' ' union 
-- select 'TAX_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_CODE = ' ' union 
-- select 'TAX_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_TYPE = ' ' union 
-- select 'DESCRIPTION' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DESCRIPTION = ' ' union 
-- select 'DST_REFERENCE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DST_REFERENCE = ' ' union 
-- select 'ACTIVITY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ACTIVITY = ' ' union 
-- select 'ACCT_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ACCT_CATEGORY = ' ' union 
-- select 'BILL_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where BILL_CATEGORY = ' ' union 
-- select 'ACCR_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ACCR_CODE = ' ' union 
-- select 'PO_NUMBER' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_NUMBER = ' ' union 
-- select 'PO_RELEASE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_RELEASE = ' ' union 
-- select 'PO_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_CODE = ' ' union 
-- select 'PO_LINE_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_LINE_NBR = ' ' union 
-- select 'PO_AOC_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_AOC_CODE = ' ' union 
-- select 'ASSET_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ASSET_FLAG = ' ' union 
-- select 'DIST_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_CODE = ' ' union 
-- select 'TAX_POINT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_POINT = ' ' union 
-- select 'VAT_REVERSE_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where VAT_REVERSE_FL = ' ' union 
-- select 'MA_CREATE_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where MA_CREATE_FL = ' ' union 
-- select 'TAX_USAGE_CD' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_USAGE_CD = ' ' union 
-- select 'TRAN_DIST_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TRAN_DIST_FLAG = ' ' union 
 

-- COMMAND ----------

-- select 'PULL_FOR_FR_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PULL_FOR_FR_FL = ' ' union 
-- select 'AC_UPD_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where AC_UPD_STATUS = ' ' union 
-- select 'AC_UPD_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where AC_UPD_DATE = ' ' union 
-- select 'DIVERSE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIVERSE_CODE = ' ' union 
-- select 'DISTRIB_ADJ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DISTRIB_ADJ = ' ' union 
-- select 'TAX_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_FLAG = ' ' union 
-- select 'ICN_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ICN_CODE = ' ' union 
-- select 'LINE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LINE_TYPE = ' ' union 
-- select 'STATEMENT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where STATEMENT = ' ' union 
-- select 'TXN_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TXN_NBR = ' ' union 
-- select 'RETAINAGE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where RETAINAGE = ' ' union 
-- select 'PROD_TAX_CAT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PROD_TAX_CAT = ' ' union 
-- select 'CREATED_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where CREATED_BY = ' ' union 
-- select 'LAST_UPDT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDT_DATE = ' ' union 
-- select 'LAST_UPDT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDT_TIME = ' ' union 
-- select 'LAST_UPDATE_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDATE_BY = ' ' union 
-- select 'LTR_OF_GUARAN' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LTR_OF_GUARAN = ' ' union 
-- select 'L_INDEX' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where L_INDEX = ' ' order by columnname 
select 'APDSET10_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET10_SS_SW = ' ' union 
select 'APDSET11_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET11_SS_SW = ' ' union 
select 'APDSET12_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET12_SS_SW = ' ' union 
select 'APDSET5_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET5_SS_SW = ' ' union 
select 'APDSET6_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET6_SS_SW = ' ' union 
select 'APDSET8_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET8_SS_SW = ' ' union 
select 'APDSET9_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET9_SS_SW = ' ' union 
select 'L_ATAPD_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where L_ATAPD_SS_SW = ' ' order by columnname

-- COMMAND ----------

select 'API_OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where API_OBJ_ID = ' ' union 
select 'TAX_RATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_RATE = ' ' union 
select 'CURR_RATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where CURR_RATE = ' ' union 
select 'ORIG_BASE_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ORIG_BASE_AMT = ' ' union 
select 'ORIG_TRAN_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ORIG_TRAN_AMT = ' ' union 
select 'TO_BASE_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TO_BASE_AMT = ' ' union 
select 'TAXABLE_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAXABLE_AMT = ' ' union 
select 'UNT_AMOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where UNT_AMOUNT = ' ' union 
select 'GLT_OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where GLT_OBJ_ID = ' ' union 
select 'ATN_OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ATN_OBJ_ID = ' ' union 
select 'DST_OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DST_OBJ_ID = ' ' union 
select 'AC_UPD_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where AC_UPD_TIME = ' ' union 
select 'WEIGHT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where WEIGHT = ' ' union 
select 'SUPLMNTARY_QTY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where SUPLMNTARY_QTY = ' ' order by columnname

-- COMMAND ----------

select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COMPANY = ' ' union 
select 'CUSTOMER' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CUSTOMER = ' ' union 
select 'ACTIVE_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where ACTIVE_STATUS = ' ' union 
select 'NAT_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where NAT_FLAG = ' ' union 
select 'SEARCH_NAME' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where SEARCH_NAME = ' ' union 
select 'HOLD_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HOLD_CODE = ' ' union 
select 'CONTACT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CONTACT = ' ' union 
select 'INT_PREFIX' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INT_PREFIX = ' ' union 
select 'PHONE_NMBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PHONE_NMBR = ' ' union 
select 'PHONE_EXT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PHONE_EXT = ' ' union 
select 'TELEX_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where TELEX_NBR = ' ' union 
select 'EDI_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where EDI_NBR = ' ' union 
select 'ALT_EDI_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where ALT_EDI_NBR = ' ' union 
select 'ALT_EDI_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where ALT_EDI_TYPE = ' ' union 
select 'FAX_NMBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where FAX_NMBR = ' ' union 
select 'FAX_EXT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where FAX_EXT = ' ' union 
select 'START_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where START_DATE = ' ' union 
select 'CREDIT_ANLYST' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CREDIT_ANLYST = ' ' union 
select 'CURRENCY_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CURRENCY_CD = ' ' union 
select 'SALESMAN' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where SALESMAN = ' ' union 
select 'RISK_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where RISK_CD = ' ' union 
select 'DEFAULT_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DEFAULT_CODE = ' ' union 
select 'MAJ_CLASS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MAJ_CLASS = ' ' union 
select 'MIN_CLASS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MIN_CLASS = ' ' union 
select 'CREDIT_LIM_DAT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CREDIT_LIM_DAT = ' ' union 
select 'LAST_PMT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_PMT_DATE = ' ' union 
select 'LAST_INV_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_INV_DATE = ' ' union 
select 'LAST_STA_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_STA_DATE = ' ' union 
select 'REVIEW_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where REVIEW_DATE = ' ' union 
select 'REVIEW_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where REVIEW_CODE = ' ' union 
select 'LAST_COMMENT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_COMMENT = ' ' union 
select 'PL_EXCL_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PL_EXCL_FL = ' ' union 
select 'AGE_DISPUTES' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AGE_DISPUTES = ' ' union 
select 'DISPUTES_FIN' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DISPUTES_FIN = ' ' union 
select 'AUTO_APP_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AUTO_APP_FL = ' ' union 
select 'AUTO_DUNN_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AUTO_DUNN_FL = ' ' union 
select 'DISC_GRACE_PD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DISC_GRACE_PD = ' ' union 
select 'CR_VAR_PCT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CR_VAR_PCT = ' ' union 
select 'MEMO_TERM' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MEMO_TERM = ' ' union 
select 'CHRGBK_PRT_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CHRGBK_PRT_FL = ' ' union 
select 'OPEN_BAL_FWD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where OPEN_BAL_FWD = ' ' union 
select 'STATEMENT_REQ' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where STATEMENT_REQ = ' ' union 
select 'STMNT_CYCLE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where STMNT_CYCLE = ' ' union 
select 'LOCK_BOX' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LOCK_BOX = ' ' union 
select 'ZERO_STMNT_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where ZERO_STMNT_FL = ' ' union 
select 'CRED_STMNT_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CRED_STMNT_FL = ' ' union 
select 'PAST_STMNT_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAST_STMNT_FL = ' ' union 
select 'OVER_STMNT_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where OVER_STMNT_FL = ' ' union 
select 'AUTO_REAS_CD_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AUTO_REAS_CD_01 = ' ' union 
select 'AUTO_REAS_CD_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AUTO_REAS_CD_02 = ' ' union 
select 'AUTO_REAS_CD_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AUTO_REAS_CD_03 = ' ' union 
select 'DISC_REAS_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DISC_REAS_CD = ' ' union 
select 'BANK_INST_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where BANK_INST_CODE = ' ' union 
select 'DRAFT_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DRAFT_FLAG = ' ' union 
select 'AR_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AR_CODE = ' ' union 
select 'TERMS_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where TERMS_CD = ' ' union 
select 'LATE_PAY_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LATE_PAY_FL = ' ' union 
select 'FIN_CALC_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where FIN_CALC_TYPE = ' ' union 
select 'FIN_CHRG_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where FIN_CHRG_CD = ' ' union 
select 'FIN_GRAC_DAYS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where FIN_GRAC_DAYS = ' ' union 
select 'FIN_DOC_PRNT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where FIN_DOC_PRNT = ' ' union 
select 'LAST_DUNN' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_DUNN = ' ' union 
select 'LAST_DUN_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_DUN_DATE = ' ' union 
select 'TAX_EXEMPT_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where TAX_EXEMPT_CD = ' ' union 
select 'TAX_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where TAX_CODE = ' ' union 
select 'TERRITORY' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where TERRITORY = ' ' union 
select 'AGING_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AGING_CODE = ' ' union 
select 'REVALUE_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where REVALUE_FL = ' ' union 
select 'MINIMUM' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MINIMUM = ' ' union 
select 'FIN_CYCLE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where FIN_CYCLE = ' ' union 
select 'DUN_CYCLE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DUN_CYCLE = ' ' union 
select 'AUTO_METHOD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AUTO_METHOD = ' ' union 
select 'AUTO_REMOVE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AUTO_REMOVE = ' ' union 
select 'CB_DTL_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CB_DTL_DATE = ' ' union 
select 'VAT_CUST_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where VAT_CUST_TYPE = ' ' union 
select 'VAT_REG_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where VAT_REG_NBR = ' ' union 
select 'VAT_REG_CTRY' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where VAT_REG_CTRY = ' ' union 
select 'DUN_LTR_IND' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DUN_LTR_IND = ' ' union 
select 'DUN_PROCESS_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DUN_PROCESS_CD = ' ' union 
select 'LANGUAGE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LANGUAGE_CODE = ' ' union 
select 'LST_DUNLTR_STM' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LST_DUNLTR_STM = ' ' union 
select 'LAST_LTR_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_LTR_DATE = ' ' union 
select 'LST_LTRTEXT_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LST_LTRTEXT_CD = ' ' union 
select 'ASSESS_DUN_FEE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where ASSESS_DUN_FEE = ' ' union 
select 'CU_NAME' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_NAME = ' ' union 
select 'CU_ADDR1' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_ADDR1 = ' ' union 
select 'CU_ADDR2' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_ADDR2 = ' ' union 
select 'CU_ADDR3' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_ADDR3 = ' ' union 
select 'CU_ADDR4' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_ADDR4 = ' ' union 
select 'CU_CITY' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_CITY = ' ' union 
select 'CU_STATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_STATE = ' ' union 
select 'CU_POSTAL_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_POSTAL_CODE = ' ' union 
select 'CU_COUNTRY_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_COUNTRY_CD = ' ' union 
select 'CU_COUNTRY' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_COUNTRY = ' ' union 
select 'THIRD_PARTY' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where THIRD_PARTY = ' ' union 
select 'E_MAIL_ADDRESS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where E_MAIL_ADDRESS = ' ' union 
select 'URL_ADDR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where URL_ADDR = ' ' union 
select 'CUST_AUDIT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CUST_AUDIT = ' ' union 
select 'APPLY_MIXED' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where APPLY_MIXED = ' ' union 
select 'PAY_REAS_CD' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAY_REAS_CD = ' ' union 
select 'MULT_DUN_LTR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MULT_DUN_LTR = ' ' union 
select 'CU_COUNTY' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CU_COUNTY = ' ' union 
select 'AFFILIATE_FL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AFFILIATE_FL = ' ' union 
select 'FOR_ECON_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where FOR_ECON_CODE = ' ' union 
select 'SOCIAL_ID1' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where SOCIAL_ID1 = ' ' union 
select 'SOCIAL_ID2' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where SOCIAL_ID2 = ' ' union 
select 'SOCIAL_ID3' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where SOCIAL_ID3 = ' ' union 
select 'SOCIAL_ID4' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where SOCIAL_ID4 = ' ' union 
select 'SOCIAL_ID5' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where SOCIAL_ID5 = ' ' union 
select 'CARRIER_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CARRIER_FLAG = ' ' union 
select 'L_INDEX' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where L_INDEX = ' ' union 
select 'ACMSET4_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where ACMSET4_SS_SW = ' ' union 
select 'ACMSET9_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where ACMSET9_SS_SW = ' ' union 
select 'L_ATACM_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where L_ATACM_SS_SW = ' ' union 
select 'CURR_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CURR_BAL = ' ' union 
select 'DRAFT_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DRAFT_BAL = ' ' union 
select 'OPEN_ORDS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where OPEN_ORDS = ' ' union 
select 'ORDER_LIM' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where ORDER_LIM = ' ' union 
select 'CREDIT_LIM' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CREDIT_LIM = ' ' union 
select 'CREDIT_REVDAYS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CREDIT_REVDAYS = ' ' union 
select 'LAST_PMT_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_PMT_AMT = ' ' union 
select 'LAST_INV_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_INV_AMT = ' ' union 
select 'LAST_STA_BAL' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_STA_BAL = ' ' union 
select 'HIGH_BAL_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_01 = ' ' union 
select 'HIGH_BAL_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_02 = ' ' union 
select 'HIGH_BAL_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_03 = ' ' union 
select 'HIGH_BAL_04' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_04 = ' ' union 
select 'HIGH_BAL_05' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_05 = ' ' union 
select 'HIGH_BAL_06' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_06 = ' ' union 
select 'HIGH_BAL_07' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_07 = ' ' union 
select 'HIGH_BAL_08' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_08 = ' ' union 
select 'HIGH_BAL_09' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_09 = ' ' union 
select 'HIGH_BAL_10' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_10 = ' ' union 
select 'HIGH_BAL_11' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_11 = ' ' union 
select 'HIGH_BAL_12' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_12 = ' ' union 
select 'HIGH_BAL_13' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where HIGH_BAL_13 = ' ' union 
select 'PER_RTMS_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_01 = ' ' union 
select 'PER_RTMS_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_02 = ' ' union 
select 'PER_RTMS_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_03 = ' ' union 
select 'PER_RTMS_04' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_04 = ' ' union 
select 'PER_RTMS_05' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_05 = ' ' union 
select 'PER_RTMS_06' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_06 = ' ' union 
select 'PER_RTMS_07' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_07 = ' ' union 
select 'PER_RTMS_08' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_08 = ' ' union 
select 'PER_RTMS_09' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_09 = ' ' union 
select 'PER_RTMS_10' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_10 = ' ' union 
select 'PER_RTMS_11' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_11 = ' ' union 
select 'PER_RTMS_12' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_12 = ' ' union 
select 'PER_RTMS_13' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PER_RTMS_13 = ' ' union 
select 'PAID_PRMPT_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_01 = ' ' union 
select 'PAID_PRMPT_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_02 = ' ' union 
select 'PAID_PRMPT_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_03 = ' ' union 
select 'PAID_PRMPT_04' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_04 = ' ' union 
select 'PAID_PRMPT_05' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_05 = ' ' union 
select 'PAID_PRMPT_06' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_06 = ' ' union 
select 'PAID_PRMPT_07' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_07 = ' ' union 
select 'PAID_PRMPT_08' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_08 = ' ' union 
select 'PAID_PRMPT_09' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_09 = ' ' union 
select 'PAID_PRMPT_10' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_10 = ' ' union 
select 'PAID_PRMPT_11' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_11 = ' ' union 
select 'PAID_PRMPT_12' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAID_PRMPT_12 = ' ' union 
select 'INV_PAID_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_01 = ' ' union 
select 'INV_PAID_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_02 = ' ' union 
select 'INV_PAID_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_03 = ' ' union 
select 'INV_PAID_04' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_04 = ' ' union 
select 'INV_PAID_05' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_05 = ' ' union 
select 'INV_PAID_06' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_06 = ' ' union 
select 'INV_PAID_07' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_07 = ' ' union 
select 'INV_PAID_08' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_08 = ' ' union 
select 'INV_PAID_09' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_09 = ' ' union 
select 'INV_PAID_10' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_10 = ' ' union 
select 'INV_PAID_11' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_11 = ' ' union 
select 'INV_PAID_12' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where INV_PAID_12 = ' ' union 
select 'COLLECT_DAYS_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_01 = ' ' union 
select 'COLLECT_DAYS_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_02 = ' ' union 
select 'COLLECT_DAYS_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_03 = ' ' union 
select 'COLLECT_DAYS_04' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_04 = ' ' union 
select 'COLLECT_DAYS_05' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_05 = ' ' union 
select 'COLLECT_DAYS_06' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_06 = ' ' union 
select 'COLLECT_DAYS_07' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_07 = ' ' union 
select 'COLLECT_DAYS_08' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_08 = ' ' union 
select 'COLLECT_DAYS_09' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_09 = ' ' union 
select 'COLLECT_DAYS_10' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_10 = ' ' union 
select 'COLLECT_DAYS_11' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where COLLECT_DAYS_11 = ' ' union 
select 'CURR_CASH' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CURR_CASH = ' ' union 
select 'CURR_CSH_DAYS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where CURR_CSH_DAYS = ' ' union 
select 'YTD_CASH' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where YTD_CASH = ' ' union 
select 'YTD_CSH_DAYS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where YTD_CSH_DAYS = ' ' union 
select 'YEAR_2_DAYS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where YEAR_2_DAYS = ' ' union 
select 'STATIC_CUR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where STATIC_CUR = ' ' union 
select 'AGING_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AGING_01 = ' ' union 
select 'AGING_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AGING_02 = ' ' union 
select 'AGING_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AGING_03 = ' ' union 
select 'AGING_04' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AGING_04 = ' ' union 
select 'AGING_05' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where AGING_05 = ' ' union 
select 'DISP_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DISP_NBR = ' ' union 
select 'DISP_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DISP_AMT = ' ' union 
select 'MAX_AUTO_AMT_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MAX_AUTO_AMT_01 = ' ' union 
select 'MAX_AUTO_AMT_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MAX_AUTO_AMT_02 = ' ' union 
select 'MAX_AUTO_AMT_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MAX_AUTO_AMT_03 = ' ' union 
select 'MAX_AUTO_PCT_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MAX_AUTO_PCT_01 = ' ' union 
select 'MAX_AUTO_PCT_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MAX_AUTO_PCT_02 = ' ' union 
select 'MAX_AUTO_PCT_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where MAX_AUTO_PCT_03 = ' ' union 
select 'TERMS_CASH' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where TERMS_CASH = ' ' union 
select 'TERMS_CSH_DAYS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where TERMS_CSH_DAYS = ' ' union 
select 'DBT_RATIO_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_01 = ' ' union 
select 'DBT_RATIO_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_02 = ' ' union 
select 'DBT_RATIO_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_03 = ' ' union 
select 'DBT_RATIO_04' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_04 = ' ' union 
select 'DBT_RATIO_05' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_05 = ' ' union 
select 'DBT_RATIO_06' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_06 = ' ' union 
select 'DBT_RATIO_07' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_07 = ' ' union 
select 'DBT_RATIO_08' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_08 = ' ' union 
select 'DBT_RATIO_09' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_09 = ' ' union 
select 'DBT_RATIO_10' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_10 = ' ' union 
select 'DBT_RATIO_11' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_RATIO_11 = ' ' union 
select 'DBT_PR_YR' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_PR_YR = ' ' union 
select 'DBT_YTD_CASH' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_YTD_CASH = ' ' union 
select 'DBT_YTD_DAYS' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DBT_YTD_DAYS = ' ' union 
select 'FIN_MIN_CHRG' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where FIN_MIN_CHRG = ' ' union 
select 'LAST_DUN_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where LAST_DUN_AMT = ' ' union 
select 'DISHONORED' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DISHONORED = ' ' union 
select 'DISHONORED_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where DISHONORED_AMT = ' ' union 
select 'RECONCILE_CASH' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where RECONCILE_CASH = ' ' union 
select 'RECONCILE_RNEG' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where RECONCILE_RNEG = ' ' union 
select 'RECONCILE_WO' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where RECONCILE_WO = ' ' union 
select 'I_FIN_MIN_CHRG' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where I_FIN_MIN_CHRG = ' ' union 
select 'OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where OBJ_ID = ' ' union 
select 'PAY_AUTO_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAY_AUTO_AMT = ' ' union 
select 'PAY_AUTO_PCT' as columnname, count(1) as NullCount from bronze.FINANCE_ARCUSTOMER where PAY_AUTO_PCT = ' ' ORDER BY columnname 

-- COMMAND ----------

select 'VENDOR_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VENDOR_GROUP = ' ' union 
select 'VENDOR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VENDOR = ' ' union 
select 'VEN_CLASS' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VEN_CLASS = ' ' union 
select 'VENDOR_VNAME' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VENDOR_VNAME = ' ' union 
select 'VENDOR_SNAME' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VENDOR_SNAME = ' ' union 
select 'VENDOR_CONTCT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VENDOR_CONTCT = ' ' union 
select 'REMIT_TO_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where REMIT_TO_CODE = ' ' union 
select 'PURCH_FR_LOC' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PURCH_FR_LOC = ' ' union 
select 'PAY_VENDOR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PAY_VENDOR = ' ' union 
select 'VENDOR_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VENDOR_STATUS = ' ' union 
select 'VEN_PRIORITY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VEN_PRIORITY = ' ' union 
select 'PHONE_PREFIX' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PHONE_PREFIX = ' ' union 
select 'PHONE_NUM' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PHONE_NUM = ' ' union 
select 'PHONE_EXT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PHONE_EXT = ' ' union 
select 'FAX_PREFIX' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FAX_PREFIX = ' ' union 
select 'FAX_NUM' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FAX_NUM = ' ' union 
select 'FAX_EXT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FAX_EXT = ' ' union 
select 'TELEX_NUM' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where TELEX_NUM = ' ' union 
select 'TERM_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where TERM_CODE = ' ' union 
select 'INV_CURRENCY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INV_CURRENCY = ' ' union 
select 'BAL_CURRENCY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BAL_CURRENCY = ' ' union 
select 'CURR_RECALC' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CURR_RECALC = ' ' union 
select 'SEP_CHK_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SEP_CHK_FLAG = ' ' union 
select 'TAX_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where TAX_ID = ' ' union 
select 'TAX_EXEMPT_CD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where TAX_EXEMPT_CD = ' ' union 
select 'TAX_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where TAX_CODE = ' ' union 
select 'HOLD_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where HOLD_FLAG = ' ' union 
select 'DIST_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where DIST_CODE = ' ' union 
select 'ACCR_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ACCR_CODE = ' ' union 
select 'BANK_INST_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BANK_INST_CODE = ' ' union 
select 'CASH_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CASH_CODE = ' ' union 
select 'BANK_ENTITY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BANK_ENTITY = ' ' union 
select 'BANK_CURRENCY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BANK_CURRENCY = ' ' union 
select 'VBANK_ACCT_NO' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VBANK_ACCT_NO = ' ' union 
select 'VBANK_ACCT_TP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VBANK_ACCT_TP = ' ' union 
select 'VBANK_IDENT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VBANK_IDENT = ' ' union 
select 'CROSS_IDENT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CROSS_IDENT = ' ' union 
select 'INCOME_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INCOME_CODE = ' ' union 
select 'INCOME_WH_FLG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INCOME_WH_FLG = ' ' union 
select 'EDI_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where EDI_NBR = ' ' union 
select 'ACH_PRENOT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ACH_PRENOT = ' ' union 
select 'CREATE_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CREATE_DATE = ' ' union 
select 'ORIGIN_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ORIGIN_DATE = ' ' union 
select 'OPERATOR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where OPERATOR = ' ' union 
select 'USER_NAME_01' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where USER_NAME_01 = ' ' union 
select 'USER_NAME_02' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where USER_NAME_02 = ' ' union 
select 'USER_NAME_03' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where USER_NAME_03 = ' ' union 
select 'USER_NAME_04' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where USER_NAME_04 = ' ' union 
select 'USER_NAME_05' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where USER_NAME_05 = ' ' union 
select 'USER_NAME_06' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where USER_NAME_06 = ' ' union 
select 'CUST_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CUST_GROUP = ' ' union 
select 'CUSTOMER' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CUSTOMER = ' ' union 
select 'LEGAL_NAME' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where LEGAL_NAME = ' ' union 
select 'INVOICE_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INVOICE_GROUP = ' ' union 
select 'DISCOUNT_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where DISCOUNT_CODE = ' ' union 
select 'ERS_CAPABLE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ERS_CAPABLE = ' ' union 
select 'INVC_REF_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INVC_REF_TYPE = ' ' union 
select 'EDI_AUTO_REL' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where EDI_AUTO_REL = ' ' union 
select 'AUTH_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where AUTH_CODE = ' ' union 
select 'APPRVL_EXISTS' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where APPRVL_EXISTS = ' ' union 
select 'ACTIVITY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ACTIVITY = ' ' union 
select 'ACCT_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ACCT_CATEGORY = ' ' union 
select 'CHARGE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CHARGE_CODE = ' ' union 
select 'PMT_CAT_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PMT_CAT_CODE = ' ' union 
select 'NORM_EXP_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where NORM_EXP_CODE = ' ' union 
select 'PMT_FORM' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PMT_FORM = ' ' union 
select 'SWIFT_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SWIFT_ID = ' ' union 
select 'PROC_GRP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PROC_GRP = ' ' union 
select 'MATCH_TABLE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where MATCH_TABLE = ' ' union 
select 'DISC_CALC_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where DISC_CALC_DATE = ' ' union 
select 'CREATE_POD_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CREATE_POD_FL = ' ' union 
select 'HANDLING_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where HANDLING_CODE = ' ' union 
select 'ENCLOSURE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ENCLOSURE = ' ' union 
select 'REQ_MATCH_REF' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where REQ_MATCH_REF = ' ' union 
select 'POOL_OPTION' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where POOL_OPTION = ' ' union 
select 'HOLD_INSP_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where HOLD_INSP_FLAG = ' ' union 
select 'VEN_CLAIM_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VEN_CLAIM_TYPE = ' ' union 
select 'CLAIM_HOLD_CD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CLAIM_HOLD_CD = ' ' union 
select 'CB_HOLD_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CB_HOLD_CODE = ' ' union 
select 'REPLACE_GOODS' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where REPLACE_GOODS = ' ' union 
select 'SHIP_OR_HOLD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SHIP_OR_HOLD = ' ' union 
select 'ERS_HANDLING' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ERS_HANDLING = ' ' union 
select 'E_MAIL_ADDRESS' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where E_MAIL_ADDRESS = ' ' union 
select 'URL_ADDR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where URL_ADDR = ' ' union 
select 'VEND_ACCT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VEND_ACCT = ' ' union 
select 'LANGUAGE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where LANGUAGE_CODE = ' ' union 
select 'TAX_USAGE_CD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where TAX_USAGE_CD = ' ' union 
select 'VAT_REG_CTRY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VAT_REG_CTRY = ' ' union 
select 'VAT_REG_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VAT_REG_NBR = ' ' union 
select 'VALIDATE_PO' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VALIDATE_PO = ' ' union 
select 'REQUIRE_PO' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where REQUIRE_PO = ' ' union 
select 'WORKFLOW_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where WORKFLOW_GROUP = ' ' union 
select 'MBL_INT_PREFIX' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where MBL_INT_PREFIX = ' ' union 
select 'MOBILE_NUM' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where MOBILE_NUM = ' ' union 
select 'MOBILE_EXT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where MOBILE_EXT = ' ' union 
select 'PAY_IMM_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PAY_IMM_FLAG = ' ' union 
select 'DIVERSE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where DIVERSE_CODE = ' ' union 
select 'INTM_BANK_ENT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_BANK_ENT = ' ' union 
select 'INTM_BANK_ACCT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_BANK_ACCT = ' ' union 
select 'INTM_BANK_IDNT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_BANK_IDNT = ' ' union 
select 'INTM_BANK_CURR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_BANK_CURR = ' ' union 
select 'INTM_PRENOT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_PRENOT = ' ' union 
select 'INTM_SWIFT_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_SWIFT_ID = ' ' union 
select 'INTM_PMT_CAT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_PMT_CAT = ' ' union 
select 'INTM_NORM_EXP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_NORM_EXP = ' ' union 
select 'INTM_PMT_FORM' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_PMT_FORM = ' ' union 
select 'INTM_CHRG_CD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_CHRG_CD = ' ' union 
select 'INTM_CRS_IDENT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_CRS_IDENT = ' ' union 
select 'RULE_GROUP' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where RULE_GROUP = ' ' union 
select 'FLOAT_DAYS' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FLOAT_DAYS = ' ' union 
select 'MTCH_PREPAY_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where MTCH_PREPAY_FL = ' ' union 
select 'MTCH_PREPAY_MT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where MTCH_PREPAY_MT = ' ' union 
select 'BUILD_SOC_REF' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BUILD_SOC_REF = ' ' union 
select 'BUILD_ACCT_NM' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BUILD_ACCT_NM = ' ' union 
select 'XREF_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where XREF_FLAG = ' ' union 
select 'GIRO_NUMBER' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where GIRO_NUMBER = ' ' union 
select 'CR_CARD_NUMBER' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CR_CARD_NUMBER = ' ' union 
select 'BANK_INSTRUCT1' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BANK_INSTRUCT1 = ' ' union 
select 'BANK_INSTRUCT2' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BANK_INSTRUCT2 = ' ' union 
select 'BANK_INSTRUCT3' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BANK_INSTRUCT3 = ' ' union 
select 'BANK_INSTRUCT4' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BANK_INSTRUCT4 = ' ' union 
select 'P_CARD_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where P_CARD_FLAG = ' ' union 
select 'PCARD_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PCARD_NBR = ' ' union 
select 'VAL_DIV_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VAL_DIV_DATE = ' ' union 
select 'RET_ACCR_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where RET_ACCR_CODE = ' ' union 
select 'GLN_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where GLN_NBR = ' ' union 
select 'TIN_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where TIN_TYPE = ' ' union 
select 'TIN_NOT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where TIN_NOT = ' ' union 
select 'TIN_VERIFIED' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where TIN_VERIFIED = ' ' union 
select 'SEC_WTH_EXEMPT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SEC_WTH_EXEMPT = ' ' union 
select 'SEC_WTH_CODE1' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SEC_WTH_CODE1 = ' ' union 
select 'SEC_WTH_CODE2' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SEC_WTH_CODE2 = ' ' union 
select 'SEC_WTH_CODE3' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SEC_WTH_CODE3 = ' ' union 
select 'CREATED_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CREATED_BY = ' ' union 
select 'LAST_UPDT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where LAST_UPDT_DATE = ' ' union 
select 'LAST_UPDT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where LAST_UPDT_TIME = ' ' union 
select 'LAST_UPDATE_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where LAST_UPDATE_BY = ' ' union 
select 'VALID_CERT_DT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VALID_CERT_DT = ' ' union 
select 'FOR_ECON_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FOR_ECON_CODE = ' ' union 
select 'SOCIAL_ID1' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SOCIAL_ID1 = ' ' union 
select 'SOCIAL_ID2' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SOCIAL_ID2 = ' ' union 
select 'SOCIAL_ID3' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SOCIAL_ID3 = ' ' union 
select 'SOCIAL_ID4' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SOCIAL_ID4 = ' ' union 
select 'SOCIAL_ID5' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SOCIAL_ID5 = ' ' union 
select 'CARRIER_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CARRIER_FLAG = ' ' union 
select 'BANK_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BANK_CODE = ' ' union 
select 'BANK_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where BANK_ID = ' ' union 
select 'ASSIGNMENT_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ASSIGNMENT_NBR = ' ' union 
select 'DEBITING_SIGN' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where DEBITING_SIGN = ' ' union 
select 'INTM_BANK_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_BANK_CODE = ' ' union 
select 'INTM_BANK_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_BANK_ID = ' ' union 
select 'INTM_ASSIGN_NO' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_ASSIGN_NO = ' ' union 
select 'INTM_DEBITING' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where INTM_DEBITING = ' ' union 
select 'CHK_DIG_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CHK_DIG_TYPE = ' ' union 
select 'SEPA_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where SEPA_FLAG = ' ' union 
select 'W8_VERIFIED' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where W8_VERIFIED = ' ' union 
select 'FATCA_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FATCA_ID = ' ' union 
select 'FIN_INST_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FIN_INST_TYPE = ' ' union 
select 'GIIN_CAT_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where GIIN_CAT_CODE = ' ' union 
select 'ISO_CTRY_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ISO_CTRY_NBR = ' ' union 
select 'FATCA_INC_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FATCA_INC_CODE = ' ' union 
select 'FATCA_AU' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FATCA_AU = ' ' union 
select 'FATCA_ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FATCA_ACCOUNT = ' ' union 
select 'FATCA_SUB_ACCT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FATCA_SUB_ACCT = ' ' union 
select 'OPEN_ZERO_COST' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where OPEN_ZERO_COST = ' ' union 
select 'ENABLE_ASN_BOD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where ENABLE_ASN_BOD = ' ' union 
select 'EXTERNAL_KEY' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where EXTERNAL_KEY = ' ' union 
select 'EXT_ACCT_ENT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where EXT_ACCT_ENT = ' ' union 
select 'EXT_LOCATION' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where EXT_LOCATION = ' ' union 
select 'L_INDEX' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where L_INDEX = ' ' union 
select 'VENSET11_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VENSET11_SS_SW = ' ' union 
select 'VENSET8_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where VENSET8_SS_SW = ' ' union 
select 'L_ATVEN_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where L_ATVEN_SS_SW = ' ' union 
select 'MAX_INV_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where MAX_INV_AMT = ' ' union 
select 'PRIME_RATE' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where PRIME_RATE = ' ' union 
select 'WRITE_OFF_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where WRITE_OFF_AMT = ' ' union 
select 'CB_MINIMUM_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where CB_MINIMUM_AMT = ' ' union 
select 'AP_OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where AP_OBJ_ID = ' ' union 
select 'FATCA_WITHOLD' as columnname, count(1) as NullCount from bronze.FINANCE_APVENMAST where FATCA_WITHOLD = ' ' order by columnname 

-- COMMAND ----------

select 'GL_COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where GL_COMPANY = ' ' union 
select 'ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ACCT_UNIT = ' ' union 
select 'ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ACCOUNT = ' ' union 
select 'SUB_ACCT' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where SUB_ACCT = ' ' union 
select 'GL_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where GL_DATE = ' ' union 
select 'CREATE_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where CREATE_DATE = ' ' union 
select 'PROG_SEQ_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where PROG_SEQ_NBR = ' ' union 
select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where COMPANY = ' ' union 
select 'BATCH_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where BATCH_NBR = ' ' union 
select 'CUSTOMER' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where CUSTOMER = ' ' union 
select 'PROCESS_LEVEL' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where PROCESS_LEVEL = ' ' order by columnname 

-- COMMAND ----------

select 'TRANS_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where TRANS_TYPE = ' ' union 
select 'INVOICE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where INVOICE = ' ' union 
select 'TRANS_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where TRANS_DATE = ' ' union 
select 'DIST_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where DIST_SEQ = ' ' union 
select 'DST_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where DST_TYPE = ' ' union 
select 'PAYMENT_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where PAYMENT_SEQ = ' ' order by columnname 

-- COMMAND ----------

select 'ACCUM_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ACCUM_TYPE = ' ' union 
select 'R_DESC' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where R_DESC = ' ' union 
select 'ACTIVITY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ACTIVITY = ' ' union 
select 'ACCT_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ACCT_CATEGORY = ' ' union 
select 'STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where STATUS = ' ' union 
select 'ORIG_CURRENCY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ORIG_CURRENCY = ' ' union 
select 'CAT_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where CAT_TYPE = ' ' union 
select 'AR_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AR_CATEGORY = ' ' union 
select 'TAX_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where TAX_CODE = ' ' union 
select 'UPDATE_SUM' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where UPDATE_SUM = ' ' order by columnname 

-- COMMAND ----------

select 'ACCUM_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ACCUM_TYPE = ' ' union 
select 'R_DESC' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where R_DESC = ' ' union 
select 'ACTIVITY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ACTIVITY = ' ' order by columnname 

-- COMMAND ----------

select 'ACCT_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ACCT_CATEGORY = ' ' union 
select 'STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where STATUS = ' ' union 
select 'ORIG_CURRENCY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ORIG_CURRENCY = ' ' union 
select 'CAT_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where CAT_TYPE = ' ' union 
select 'AR_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AR_CATEGORY = ' ' order by columnname 

-- COMMAND ----------

select 'TAX_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where TAX_CODE = ' ' union 
select 'UPDATE_SUM' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where UPDATE_SUM = ' ' union 
select 'ORIG_ND' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ORIG_ND = ' ' union 
select 'AUTO_REV' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AUTO_REV = ' ' union 
select 'ORIG_COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ORIG_COMPANY = ' ' order by columnname 

-- COMMAND ----------

select 'DOCUMENT_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where DOCUMENT_NBR = ' ' union 
select 'DST_SOURCE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where DST_SOURCE = ' ' union 
select 'BASE_ACCTUNIT' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where BASE_ACCTUNIT = ' ' union 
select 'JRNL_BOOK_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where JRNL_BOOK_NBR = ' ' union 
select 'MX_VALUE_01' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where MX_VALUE_01 = ' ' order by columnname 

-- COMMAND ----------

select 'MX_VALUE_02' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where MX_VALUE_02 = ' ' union 
select 'MX_VALUE_03' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where MX_VALUE_03 = ' ' union 
select 'DRAFT_SOURCE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where DRAFT_SOURCE = ' ' union 
select 'TAX_POINT' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where TAX_POINT = ' ' union 
select 'LINE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where LINE_TYPE = ' ' order by columnname 

-- COMMAND ----------

select 'ICN_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ICN_CODE = ' ' union 
select 'BAL_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where BAL_DATE = ' ' union 
select 'AMDSET10_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AMDSET10_SS_SW = ' ' union 
select 'AMDSET2_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AMDSET2_SS_SW = ' ' union 
select 'AMDSET3_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AMDSET3_SS_SW = ' ' union 
select 'AMDSET4_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AMDSET4_SS_SW = ' ' union 
select 'AMDSET6_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AMDSET6_SS_SW = ' ' union 
select 'AMDSET7_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AMDSET7_SS_SW = ' ' union 
select 'AMDSET8_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AMDSET8_SS_SW = ' ' union 
select 'AMDSET9_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where AMDSET9_SS_SW = ' ' union 
select 'CREATE_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where CREATE_TIME = ' ' union 
select 'TRAN_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where TRAN_AMT = ' ' union 
select 'TO_COMP_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where TO_COMP_AMT = ' ' union 
select 'UNITS' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where UNITS = ' ' union 
select 'ORIG_RATE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ORIG_RATE = ' ' union 
select 'ORIG_AMT' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ORIG_AMT = ' ' union 
select 'TRAN_TAXABLE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where TRAN_TAXABLE = ' ' union 
select 'ORIG_TAXABLE' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ORIG_TAXABLE = ' ' union 
select 'GLT_OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where GLT_OBJ_ID = ' ' union 
select 'ATN_OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ATN_OBJ_ID = ' ' union 
select 'JBK_SEQ_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where JBK_SEQ_NBR = ' ' union 
select 'WEIGHT' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where WEIGHT = ' ' union 
select 'SUPLMNTARY_QTY' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where SUPLMNTARY_QTY = ' ' union 
select 'ARA_OBJ_ID' as columnname, count(1) as NullCount from bronze.FINANCE_ARDISTRIB where ARA_OBJ_ID = ' ' order by columnname 

-- COMMAND ----------

-- select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where COMPANY = ' '  union 
-- select 'VENDOR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where VENDOR = ' ' union 
-- select 'INVOICE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where INVOICE = ' ' union 
-- select 'SUFFIX' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where SUFFIX = ' ' union 
-- select 'CANCEL_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where CANCEL_SEQ = ' '   union 
-- select 'ORIG_DIST_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ORIG_DIST_SEQ = ' ' union 
-- select 'DIST_SEQ_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_SEQ_NBR = ' ' union 
-- select 'DIST_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_TYPE = ' ' order by columnname
-- select 'PROC_LEVEL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PROC_LEVEL = ' ' union 
-- select 'POST_OPTION' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where POST_OPTION = ' ' union 
-- select 'INVOICE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where INVOICE_TYPE = ' ' union 
-- select 'REC_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where REC_STATUS = ' ' union 
-- select 'INV_CURRENCY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where INV_CURRENCY = ' ' union 
-- select 'BASE_ND' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where BASE_ND = ' ' union 
-- select 'TRAN_ND' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TRAN_ND = ' ' union 
-- select 'TO_BASE_ND' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TO_BASE_ND = ' ' order by columnname
-- select 'DIST_COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_COMPANY = ' ' union 
-- select 'DIS_ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIS_ACCT_UNIT = ' ' union 
-- select 'DIS_ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIS_ACCOUNT = ' ' union 
-- select 'DIS_SUB_ACCT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIS_SUB_ACCT = ' '  order by columnname

-- select 'DISTRIB_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DISTRIB_DATE = ' ' union 
-- select 'TAX_INDICATOR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_INDICATOR = ' ' union 
-- select 'TAX_SEQ_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_SEQ_NBR = ' ' union 
-- select 'TAX_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_CODE = ' ' union 
-- select 'TAX_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_TYPE = ' ' union 
-- select 'DESCRIPTION' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DESCRIPTION = ' ' union 
-- select 'DST_REFERENCE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DST_REFERENCE = ' ' union 
-- select 'ACTIVITY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ACTIVITY = ' ' union 
-- select 'ACCT_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ACCT_CATEGORY = ' ' union 
-- select 'BILL_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where BILL_CATEGORY = ' ' order by columnname
-- select 'ACCR_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ACCR_CODE = ' ' union 
-- select 'PO_NUMBER' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_NUMBER = ' ' union 
-- select 'PO_RELEASE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_RELEASE = ' ' union 
-- select 'PO_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_CODE = ' ' union 
-- select 'PO_LINE_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_LINE_NBR = ' ' union 
-- select 'PO_AOC_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_AOC_CODE = ' ' union 
-- select 'ASSET_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ASSET_FLAG = ' ' union 
-- select 'DIST_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_CODE = ' ' union 
-- select 'TAX_POINT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_POINT = ' ' union 
-- select 'VAT_REVERSE_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where VAT_REVERSE_FL = ' ' union 
-- select 'MA_CREATE_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where MA_CREATE_FL = ' ' union 
-- select 'TAX_USAGE_CD' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_USAGE_CD = ' ' union 
-- select 'TRAN_DIST_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TRAN_DIST_FLAG = ' ' order by columnname
-- select 'PULL_FOR_FR_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PULL_FOR_FR_FL = ' ' union 
-- select 'AC_UPD_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where AC_UPD_STATUS = ' ' union 
-- select 'AC_UPD_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where AC_UPD_DATE = ' ' union 
-- select 'DIVERSE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIVERSE_CODE = ' ' union 
-- select 'DISTRIB_ADJ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DISTRIB_ADJ = ' ' union 
-- select 'TAX_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_FLAG = ' ' union 
-- select 'ICN_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ICN_CODE = ' ' union 
-- select 'LINE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LINE_TYPE = ' ' union 
-- select 'STATEMENT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where STATEMENT = ' ' union 
-- select 'TXN_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TXN_NBR = ' ' union 
-- select 'RETAINAGE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where RETAINAGE = ' ' union 
-- select 'PROD_TAX_CAT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PROD_TAX_CAT = ' ' union 
-- select 'CREATED_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where CREATED_BY = ' ' union 
-- select 'LAST_UPDT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDT_DATE = ' ' union 
-- select 'LAST_UPDT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDT_TIME = ' ' union 
-- select 'LAST_UPDATE_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDATE_BY = ' ' union 
-- select 'LTR_OF_GUARAN' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LTR_OF_GUARAN = ' ' union 
-- select 'L_INDEX' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where L_INDEX = ' ' union 
-- select 'APDSET10_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET10_SS_SW = ' ' union 
-- select 'APDSET11_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET11_SS_SW = ' ' union 
-- select 'APDSET12_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET12_SS_SW = ' ' union 
-- select 'APDSET5_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET5_SS_SW = ' ' union 
-- select 'APDSET6_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET6_SS_SW = ' ' union 
-- select 'APDSET8_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET8_SS_SW = ' ' union 
-- select 'APDSET9_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET9_SS_SW = ' ' union 
-- select 'L_ATAPD_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where L_ATAPD_SS_SW = ' ' union 

-- COMMAND ----------

select 'PULL_FOR_FR_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PULL_FOR_FR_FL = ' ' union 
select 'AC_UPD_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where AC_UPD_STATUS = ' ' union 
select 'AC_UPD_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where AC_UPD_DATE = ' ' union 
select 'DIVERSE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIVERSE_CODE = ' ' union 
select 'DISTRIB_ADJ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DISTRIB_ADJ = ' ' union 
select 'TAX_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_FLAG = ' ' union 
select 'ICN_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ICN_CODE = ' ' union 
select 'LINE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LINE_TYPE = ' ' union 
select 'STATEMENT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where STATEMENT = ' ' union 
select 'TXN_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TXN_NBR = ' ' union 
select 'RETAINAGE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where RETAINAGE = ' ' union 
select 'PROD_TAX_CAT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PROD_TAX_CAT = ' ' union 
select 'CREATED_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where CREATED_BY = ' ' union 
select 'LAST_UPDT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDT_DATE = ' ' union 
select 'LAST_UPDT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDT_TIME = ' ' union 
select 'LAST_UPDATE_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDATE_BY = ' ' union 
select 'LTR_OF_GUARAN' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LTR_OF_GUARAN = ' ' union 
select 'L_INDEX' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where L_INDEX = ' ' union 
select 'APDSET10_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET10_SS_SW = ' ' union 
select 'APDSET11_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET11_SS_SW = ' ' union 
select 'APDSET12_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET12_SS_SW = ' ' union 
select 'APDSET5_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET5_SS_SW = ' ' union 
select 'APDSET6_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET6_SS_SW = ' ' union 
select 'APDSET8_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET8_SS_SW = ' ' union 
select 'APDSET9_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET9_SS_SW = ' ' union 
select 'L_ATAPD_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where L_ATAPD_SS_SW = ' ' union 

-- COMMAND ----------

select 'COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where COMPANY = ' ' union 
select 'VENDOR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where VENDOR = ' ' union 
select 'INVOICE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where INVOICE = ' ' union 
select 'SUFFIX' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where SUFFIX = ' ' union 
select 'CANCEL_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where CANCEL_SEQ = ' ' union 
select 'ORIG_DIST_SEQ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ORIG_DIST_SEQ = ' ' union 
select 'DIST_SEQ_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_SEQ_NBR = ' ' union 
select 'DIST_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_TYPE = ' ' union 
select 'PROC_LEVEL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PROC_LEVEL = ' ' union 
select 'POST_OPTION' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where POST_OPTION = ' ' union 
select 'INVOICE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where INVOICE_TYPE = ' ' union 
select 'REC_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where REC_STATUS = ' ' union 
select 'INV_CURRENCY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where INV_CURRENCY = ' ' union 
select 'BASE_ND' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where BASE_ND = ' ' union 
select 'TRAN_ND' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TRAN_ND = ' ' union 
select 'TO_BASE_ND' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TO_BASE_ND = ' ' union 
select 'DIST_COMPANY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_COMPANY = ' ' union 
select 'DIS_ACCT_UNIT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIS_ACCT_UNIT = ' ' union 
select 'DIS_ACCOUNT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIS_ACCOUNT = ' ' union 
select 'DIS_SUB_ACCT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIS_SUB_ACCT = ' ' union 
select 'DISTRIB_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DISTRIB_DATE = ' ' union 
select 'TAX_INDICATOR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_INDICATOR = ' ' union 
select 'TAX_SEQ_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_SEQ_NBR = ' ' union 
select 'TAX_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_CODE = ' ' union 
select 'TAX_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_TYPE = ' ' union 
select 'DESCRIPTION' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DESCRIPTION = ' ' union 
select 'DST_REFERENCE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DST_REFERENCE = ' ' union 
select 'ACTIVITY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ACTIVITY = ' ' union 
select 'ACCT_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ACCT_CATEGORY = ' ' union 
select 'BILL_CATEGORY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where BILL_CATEGORY = ' ' union 
select 'ACCR_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ACCR_CODE = ' ' union 
select 'PO_NUMBER' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_NUMBER = ' ' union 
select 'PO_RELEASE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_RELEASE = ' ' union 
select 'PO_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_CODE = ' ' union 
select 'PO_LINE_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_LINE_NBR = ' ' union 
select 'PO_AOC_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PO_AOC_CODE = ' ' union 
select 'ASSET_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ASSET_FLAG = ' ' union 
select 'DIST_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIST_CODE = ' ' union 
select 'TAX_POINT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_POINT = ' ' union 
select 'VAT_REVERSE_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where VAT_REVERSE_FL = ' ' union 
select 'MA_CREATE_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where MA_CREATE_FL = ' ' union 
select 'TAX_USAGE_CD' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_USAGE_CD = ' ' union 
select 'TRAN_DIST_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TRAN_DIST_FLAG = ' ' union 
select 'PULL_FOR_FR_FL' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PULL_FOR_FR_FL = ' ' union 
select 'AC_UPD_STATUS' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where AC_UPD_STATUS = ' ' union 
select 'AC_UPD_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where AC_UPD_DATE = ' ' union 
select 'DIVERSE_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DIVERSE_CODE = ' ' union 
select 'DISTRIB_ADJ' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where DISTRIB_ADJ = ' ' union 
select 'TAX_FLAG' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TAX_FLAG = ' ' union 
select 'ICN_CODE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where ICN_CODE = ' ' union 
select 'LINE_TYPE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LINE_TYPE = ' ' union 
select 'STATEMENT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where STATEMENT = ' ' union 
select 'TXN_NBR' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where TXN_NBR = ' ' union 
select 'RETAINAGE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where RETAINAGE = ' ' union 
select 'PROD_TAX_CAT' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where PROD_TAX_CAT = ' ' union 
select 'CREATED_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where CREATED_BY = ' ' union 
select 'LAST_UPDT_DATE' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDT_DATE = ' ' union 
select 'LAST_UPDT_TIME' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDT_TIME = ' ' union 
select 'LAST_UPDATE_BY' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LAST_UPDATE_BY = ' ' union 
select 'LTR_OF_GUARAN' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where LTR_OF_GUARAN = ' ' union 
select 'L_INDEX' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where L_INDEX = ' ' union 
select 'APDSET10_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET10_SS_SW = ' ' union 
select 'APDSET11_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET11_SS_SW = ' ' union 
select 'APDSET12_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET12_SS_SW = ' ' union 
select 'APDSET5_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET5_SS_SW = ' ' union 
select 'APDSET6_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET6_SS_SW = ' ' union 
select 'APDSET8_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET8_SS_SW = ' ' union 
select 'APDSET9_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where APDSET9_SS_SW = ' ' union 
select 'L_ATAPD_SS_SW' as columnname, count(1) as NullCount from bronze.FINANCE_APDISTRIB where L_ATAPD_SS_SW = ' ' union 