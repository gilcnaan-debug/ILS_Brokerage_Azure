# Databricks notebook source
## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ6"	,
# MAGIC "Budget"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Budget"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ1"	,
# MAGIC "Carrier"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Carrier"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ2"	,
# MAGIC "Customer"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Customer"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ3"	,
# MAGIC "Customer"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Customer_Contacts"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ4"	,
# MAGIC "Employee"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Employee"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ5"	,
# MAGIC "Office"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Office"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ7"	,
# MAGIC "Carrier"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Carrier_Onboarding_Status"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ8"	,
# MAGIC "Customer"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Consignee"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ9"	,
# MAGIC "Invoice"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Invoice_And_Payments"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ10"	,
# MAGIC "Load"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Load"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ11"	,
# MAGIC "Offer"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Offer"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ12"	,
# MAGIC "Load"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Pricing"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ13"	,
# MAGIC "Customer"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Shippers"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ14"	,
# MAGIC "Carrier"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Stops"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ15"	,
# MAGIC "Tender"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Tender_Address"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ16"	,
# MAGIC "Tender"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Tender_Events"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ17"	,
# MAGIC "Tender"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Tender_Manual_Draft"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ18"	,
# MAGIC "Tender"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Tenders"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into metadata.mastermetadata (SourceSystem	,
# MAGIC TableID	,
# MAGIC SubjectArea	,
# MAGIC SourceDBName	,
# MAGIC SourceSchema	,
# MAGIC SourceTableName	,
# MAGIC LoadType	,
# MAGIC IsActive	,
# MAGIC Frequency	,
# MAGIC LastLoadDateColumn	
# MAGIC ) Values ("Databricks"	,
# MAGIC "SZ19"	,
# MAGIC "Transactions"	,
# MAGIC "Brokerage"	,
# MAGIC "Silver"	,
# MAGIC "Silver_Transactions"	,
# MAGIC "incremental load"	,
# MAGIC "1"	,
# MAGIC "15 Minutes"	,
# MAGIC "Last_Modified_Date")

# COMMAND ----------

