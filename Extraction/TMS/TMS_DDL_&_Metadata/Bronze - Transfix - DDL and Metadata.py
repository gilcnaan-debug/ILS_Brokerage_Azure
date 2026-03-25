# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract the data from Source and load to Bronze zone
# MAGIC * **Created Date:** 11/18/2024
# MAGIC * **Created By:** Hariharan
# MAGIC * **Modified Date:** 
# MAGIC * **Modified By:** 
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transfix ddl and Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE bronze.transfix_data (
# MAGIC     loadnum VARCHAR(255) NOT NULL,
# MAGIC     shipper VARCHAR(255),
# MAGIC     carrier VARCHAR(255),
# MAGIC     dot VARCHAR(255),
# MAGIC     service_line VARCHAR(255),
# MAGIC     load_mode VARCHAR(255),
# MAGIC     pu_appt DATE,
# MAGIC     del_appt DATE,
# MAGIC     origin_city VARCHAR(255),
# MAGIC     origin_state VARCHAR(255),
# MAGIC     origin_zip VARCHAR(255),
# MAGIC     pu_drop VARCHAR(255),
# MAGIC     dest_city VARCHAR(255),
# MAGIC     dest_state VARCHAR(255),
# MAGIC     dest_zip VARCHAR(255),
# MAGIC     del_drop VARCHAR(255),
# MAGIC     weight INTEGER,
# MAGIC     total_rev DECIMAL(10,2),
# MAGIC     cust_lhc DECIMAL(10,2),
# MAGIC     cust_fuel DECIMAL(10,2),
# MAGIC     total_exp DECIMAL(10,2),
# MAGIC     carr_lhc DECIMAL(10,2),
# MAGIC     carr_fuel DECIMAL(10,2),
# MAGIC     finance_date DATE,
# MAGIC     shipper_miles DECIMAL(10,2),
# MAGIC     calc_miles INTEGER,
# MAGIC     arrive_pu_date DATE,
# MAGIC     depart_pu_date DATE,
# MAGIC     arrive_del_date DATE,
# MAGIC     depart_del_date DATE,
# MAGIC     cust_acc DECIMAL(10,2),
# MAGIC     carr_acc DECIMAL(10,2),
# MAGIC     first_invoice_sent_date DATE,
# MAGIC     shipper_shipment_reference VARCHAR(255),
# MAGIC     CONSTRAINT loadnum_pkey PRIMARY KEY(loadnum)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC     metadata.MasterMetadata (
# MAGIC     SourceSystem,
# MAGIC     TableID,
# MAGIC     SubjectArea,
# MAGIC     SourceDBName,
# MAGIC     SourceSchema,
# MAGIC     SourceTableName,
# MAGIC     LoadType,
# MAGIC     IsActive,
# MAGIC     Frequency,
# MAGIC     LastLoadDateValue,
# MAGIC     MergeKey,
# MAGIC     SourceSelectQuery
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     "Postgres sql",
# MAGIC     "T1",
# MAGIC     "Load",
# MAGIC     "ad_hoc_analysis",
# MAGIC     "Bronze",
# MAGIC     "transfix_data",
# MAGIC     "truncate and load",
# MAGIC     "1",
# MAGIC     "once a day",
# MAGIC     '1970-01-01 00:00:00',
# MAGIC     "loadnum",
# MAGIC "SELECT 
# MAGIC     loadnum,
# MAGIC     shipper,
# MAGIC     carrier,
# MAGIC     dot,
# MAGIC     service_line,
# MAGIC     load_mode,
# MAGIC     pu_appt,
# MAGIC     del_appt,
# MAGIC     origin_city,
# MAGIC     origin_state,
# MAGIC     origin_zip,
# MAGIC     pu_drop,
# MAGIC     dest_city,
# MAGIC     dest_state,
# MAGIC     dest_zip,
# MAGIC     del_drop,
# MAGIC     weight,
# MAGIC     total_rev,
# MAGIC     cust_lhc,
# MAGIC     cust_fuel,
# MAGIC     total_exp,
# MAGIC     carr_lhc,
# MAGIC     carr_fuel,
# MAGIC     finance_date,
# MAGIC     shipper_miles,
# MAGIC     calc_miles,
# MAGIC     arrive_pu_date,
# MAGIC     depart_pu_date,
# MAGIC     arrive_del_date,
# MAGIC     depart_del_date,
# MAGIC     cust_acc,
# MAGIC     carr_acc,
# MAGIC     first_invoice_sent_date,
# MAGIC     shipper_shipment_reference
# MAGIC FROM 
# MAGIC     public.transfix_data"
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Projection ddl and metadata
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze.tmw_pro_projection (
# MAGIC     booking_id varchar(255) NOT NULL,
# MAGIC     tmw_pro_number varchar(255),
# MAGIC     inserted_at Timestamp_NTZ NOT NULL,
# MAGIC     updated_at Timestamp_NTZ NOT NULL,
# MAGIC    CONSTRAINT booking_id_pkey PRIMARY KEY(booking_id)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC     metadata.MasterMetadata (
# MAGIC     SourceSystem,
# MAGIC     TableID,
# MAGIC     SubjectArea,
# MAGIC     SourceDBName,
# MAGIC     SourceSchema,
# MAGIC     SourceTableName,
# MAGIC     LoadType,
# MAGIC     IsActive,
# MAGIC     Frequency,
# MAGIC     LastLoadDateColumn,
# MAGIC     LastLoadDateValue,
# MAGIC     MergeKey,
# MAGIC     Zone,
# MAGIC     MergeKeyColumn,
# MAGIC     SourceSelectQuery
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     'Postgres sql',
# MAGIC     'R68',
# MAGIC     'Load',
# MAGIC     'ad_hoc_analysis',
# MAGIC     'Bronze',
# MAGIC     'tmw_pro_projection',
# MAGIC     'incremental load',
# MAGIC     '1',
# MAGIC     'one hour once',
# MAGIC     'updated_at',
# MAGIC     '1970-01-01 00:00:00',
# MAGIC     'booking_id',
# MAGIC     'Bronze',
# MAGIC     'booking_id',
# MAGIC     'SELECT booking_id, tmw_pro_number, inserted_at, updated_at FROM public.tmw_pro_projection where "{0}" >= '
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC ##fourkite ddl and metadata
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC     metadata.MasterMetadata (
# MAGIC     SourceSystem,
# MAGIC     TableID,
# MAGIC     SubjectArea,
# MAGIC     SourceDBName,
# MAGIC     SourceSchema,
# MAGIC     SourceTableName,
# MAGIC     LoadType,
# MAGIC     IsActive,
# MAGIC     Frequency,
# MAGIC     LastLoadDateColumn,
# MAGIC     MergeKey,
# MAGIC     Zone,
# MAGIC     MergeKeyColumn,
# MAGIC     SourceSelectQuery
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     "Postgres sql",
# MAGIC     "R69",
# MAGIC     "Carrier",
# MAGIC     "ad_hoc_analysis",
# MAGIC     "Bronze",
# MAGIC     "fourkite_carriers",
# MAGIC     "truncate and load",
# MAGIC     "1",
# MAGIC     "one day once",
# MAGIC     "NA",
# MAGIC     "NA",
# MAGIC     "Bronze",
# MAGIC     "NA",
# MAGIC "SELECT
# MAGIC carrier_name,
# MAGIC us_dot,
# MAGIC network_status,
# MAGIC networked_on,
# MAGIC email,
# MAGIC tracking_method 
# MAGIC FROM public.fourkite_carriers" 
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze.fourkite_carriers (
# MAGIC     carrier_name varchar(255),
# MAGIC     us_dot varchar(150),
# MAGIC     network_status varchar(150),
# MAGIC     networked_on date,
# MAGIC     email String,
# MAGIC     tracking_method String
# MAGIC );

# COMMAND ----------

