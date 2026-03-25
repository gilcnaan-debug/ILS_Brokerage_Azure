-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##silver_Customer

-- COMMAND ----------

select count(*) from brokeragedev.silver.silver_customer

-- COMMAND ----------

desc Silver.Silver_customer

-- COMMAND ----------

INSERT INTO silver.Silver_Customer (Hashkey,Customer_ID,Customer_Name,Customer_Phone1,Sourcesystem_ID,Sourcesystem_Name,Created_By,Created_Date, Last_Modified_By, Last_Modified_Date) values('testhasheky','4004','Testname','567894567','1','Aljex',"Databricks" AS Created_By, CURRENT_TIMESTAMP() AS Created_Date,"Databricks" as Last_Modified_By,CURRENT_TIMESTAMP() as Last_Modified_Date)

-- COMMAND ----------

update silver.Silver_Customer
set Hashkey="testkey",Customer_Name="Newname",Customer_Phone1="56789789"
where Customer_ID="4004"

-- COMMAND ----------

delete  from silver.Silver_Customer  where Customer_ID='4004'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##silver_Office

-- COMMAND ----------

select count(*) from brokeragedev.silver.silver_office

-- COMMAND ----------

insert into silver.silver_office ( Office_ID, Hashkey, Office_Name,Sourcesystem_ID,Sourcesystem_Name,Created_By, Created_Date, Last_Modified_By, Last_Modified_Date)
values(
 "Test1" as Hashkey,
 "Test_ID" as Office_ID,
 "Test_name" as Office_Name,
 6 as Sourcesystem_ID,
 "Test_Src" as Sourcesystem_Name,
 "Databricks" as Created_By,
 Current_Timestamp() as Created_Date,
 "Databricks" as Last_Modified_By,
 Current_Timestamp() as Last_Modified_Date
)

-- COMMAND ----------

update silver.silver_office
SET Office_Location = "Test_Location", Hashkey = "Test"
where Office_ID = "Test1"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Silver_Carrier

-- COMMAND ----------

select count(*) from brokeragedev.silver.silver_carrier

-- COMMAND ----------

INSERT INTO Silver.Silver_Carrier (Hashkey,Carrier_ID,Carrier_Name,Sourcesystem_ID,Sourcesystem_Name,Created_By,Created_Date, Last_Modified_By, Last_Modified_Date) values('testhasheky','4004','Testname','3','Edge',"Databricks" AS Created_By, CURRENT_TIMESTAMP() AS Created_Date,"Databricks" as Last_Modified_By,CURRENT_TIMESTAMP() as Last_Modified_Date)

-- COMMAND ----------

update silver.silver_Carrier
SET Carrier_Name = "NewName", Hashkey = "test2"
where Carrier_ID = '4004'

-- COMMAND ----------

delete from silver.Silver_Carrier where Carrier_ID='4004'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #CustomerContacts

-- COMMAND ----------

select count(*) from brokeragedev.silver.silver_customer_contacts

-- COMMAND ----------

desc silver.Silver_Customer_Contacts

-- COMMAND ----------

Insert into silver.Silver_Customer_Contacts (Hashkey, Cust_ID,Contact_Name1, Sourcesystem_ID, Sourcesystem_Name, Created_By, Created_Date, Last_Modified_By, Last_Modified_Date)values('testkey','4004','Test','2','Relay',"Databricks" AS Created_By, CURRENT_TIMESTAMP() AS Created_Date,"Databricks" as Last_Modified_By,CURRENT_TIMESTAMP() as Last_Modified_Date)

-- COMMAND ----------

UPDATE silver.silver_Customer_Contacts
SET Hashkey = "Test_key_2", Contact_Name1 = "Test_name_2"
where Cust_ID = '4004'

-- COMMAND ----------

delete from Silver.silver_Customer_Contacts where Cust_ID='4004'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #silver_Employee

-- COMMAND ----------

select count(*) from brokeragedev.silver.silver_employee

-- COMMAND ----------

insert into silver.silver_employee ( Hashkey, Employee_ID,Employee_Name, Sourcesystem_ID, Sourcesystem_Name, Created_By, Created_Date, Last_Modified_By, Last_Modified_Date)
values
(
 "Test_key" as Hashkey,
 "Test1" as Employee_ID,
 "Test_name" as Employee_Name,
 6 as Sourcesystem_ID,
 "Test_Src" as Sourcesystem_Name,
 "Databricks" as Created_By,
 Current_Timestamp() as Created_Date,
 "Databricks" as Last_Modified_By,
 Current_Timestamp() as Last_Modified_Date
)

-- COMMAND ----------

UPDATE silver.silver_employee
SET Hashkey = "Test_key_2", Employee_Name = "Test_name_2"
where Employee_ID = "Test1"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Fact_Load

-- COMMAND ----------

desc silver.Silver_Load

-- COMMAND ----------

-- select count(*) from brokeragedev.silver.silver_Invoice_and_payments
-- truncate table silver.silver_load
select count(1) from bronze.booking_projection

-- COMMAND ----------

select COUNT(*) from Silver.Silver_Customer where Customer_Phone1 IS NULL

-- COMMAND ----------

select count(*) from bronze.hubspot_emails where hs_email_sender_email like '%Kathryn%'

-- COMMAND ----------

desc gold.dim_load

-- COMMAND ----------

