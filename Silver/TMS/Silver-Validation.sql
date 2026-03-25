-- Databricks notebook source
use catalog brokerageprod

-- COMMAND ----------

select * from brokerageprod.silver.silver_customer where Sourcesystem_ID = "3" and Customer_ID is not null order by Customer_ID 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Silver_Customer

-- COMMAND ----------

select count(*) from silver.silver_customer

-- COMMAND ----------

select count(DW_Customer_ID, Customer_ID) from silver.silver_customer
group by DW_Customer_ID, Customer_ID having count(DW_Customer_ID, Customer_ID) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Silver_Carrier

-- COMMAND ----------


select count(*) from silver.silver_carrier

-- COMMAND ----------

select count(DW_Carrier_ID, Carrier_ID) from silver.silver_carrier
group by DW_Carrier_ID, Carrier_ID having count(DW_Carrier_ID, Carrier_ID) > 1

-- COMMAND ----------

select * from brokerageprod.silver.silver_carrier where Sourcesystem_ID = "3" order by Carrier_ID limit 3

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##Silver_Customer_Contacts

-- COMMAND ----------


select count(*) from silver.silver_customer_contacts

-- COMMAND ----------

select count(DW_Custcontact_ID, Cust_ID) from silver.silver_customer_contacts
group by DW_Custcontact_ID, Cust_ID having count(DW_Custcontact_ID, Cust_ID) > 1

-- COMMAND ----------

select * from brokerageprod.silver.silver_customer_contacts where Sourcesystem_ID = "1" order by Cust_ID limit 3

-- COMMAND ----------

select count(*) from brokerageprod.silver.silver_load ---where Sourcesystem_ID = 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Silver_Employee

-- COMMAND ----------


select count(*) from silver.silver_employee

-- COMMAND ----------

select count(DW_Employee_ID, Employee_ID) from silver.silver_employee
group by DW_Employee_ID, Employee_ID having count(DW_Employee_ID, Employee_ID) > 1

-- COMMAND ----------

select * from brokerageprod.silver.silver_employee where Sourcesystem_ID = "3" order by Employee_ID limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Silver_Load

-- COMMAND ----------


select count(*) from silver.silver_load

-- COMMAND ----------

select count(DW_Load_ID, Load_ID) from brokerageprod.silver.silver_load
group by DW_Load_ID, Load_ID having count(DW_Load_ID, Load_ID) > 1

-- COMMAND ----------

select Load_ID,Carrier_Rep_Name,Load_Status,Origin_Shippper_Name from brokerageprod.silver.silver_load where Sourcesystem_ID = "3" order by Load_ID limit 2

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##Silver_Office

-- COMMAND ----------


select count(*) from silver.silver_office

-- COMMAND ----------

select count(DW_Office_ID, Office_ID) from brokerageprod.silver.silver_office
group by DW_Office_ID, Office_ID having count(DW_Office_ID, Office_ID) > 1

-- COMMAND ----------

select * from silver.silver_office where Sourcesystem_ID = "3"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Lookup_Equipment

-- COMMAND ----------


select count(*) from silver.lookup_equipment

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ##Lookup_Mode

-- COMMAND ----------


select count(*) from silver.lookup_mode

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ##Lookup_Sourcesystem

-- COMMAND ----------


select count(*) from silver.lookup_sourcesystem

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Tables Validation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dim_Customer

-- COMMAND ----------

select count(*) from gold.dim_customer

-- COMMAND ----------

select count(*) from gold.dim_carrier

-- COMMAND ----------

select count(*) from gold.dim_customer_contacts

-- COMMAND ----------

select count(*) from gold.dim_load

-- COMMAND ----------

select count(*) from gold.dim_office

-- COMMAND ----------

select * from brokerageprod.bronze.hubspot_deals

-- COMMAND ----------

