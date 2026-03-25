-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## View

-- COMMAND ----------

CREATE OR REPLACE VIEW Gold.VW_Dim_Shipper AS
WITH final AS (
  select * from Silver.silver_shipper
  where Is_Deleted=0
  ) 
  SELECT * FROM final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DDL

-- COMMAND ----------

CREATE OR REPLACE TABLE Gold.Dim_Shipper (
    DW_Shipper_ID BIGINT PRIMARY KEY,
    Lawson_ID VARCHAR(20),
    Customer_Master VARCHAR(255), 
    Shipper_ID BIGINT,
    Customer_Name VARCHAR(255),
    Customer_Aljex_ID VARCHAR(50),
    Customer_Relay_ID VARCHAR(50),
    Shipper_Rep VARCHAR(255),
    Sales_Rep VARCHAR(255),
    Vertical VARCHAR(255),
    Shipper_Office VARCHAR(10),
    Rep_Email_Address VARCHAR(50),
    Shipper_Slack_Channel VARCHAR(50),
    HubSpot_Account VARCHAR(50),
    HubSpot_Rep_Name VARCHAR(255),
    Rep_Aljex_ID VARCHAR(50),
    Rep_Relay_ID VARCHAR(50),
    Last_Load_Date TIMESTAMP_NTZ,
    Is_Active INT, 
    Mergekey STRING,
    Hashkey STRING,
    Created_Date TIMESTAMP_NTZ,
    Created_By VARCHAR(255),
    Last_Modified_Date TIMESTAMP_NTZ,
    Last_Modified_By VARCHAR(255),
    Is_Deleted INT
);