-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## View

-- COMMAND ----------

CREATE OR REPLACE VIEW Gold.VW_Dim_Employees AS
WITH final AS (
  select * from Silver.silver_employees
  where Is_Deleted=0
  ) 
  SELECT * FROM final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DDL

-- COMMAND ----------

CREATE OR REPLACE TABLE Gold.Dim_Employees ( 
    DW_Employee_ID BIGINT,
    Employee_Number VARCHAR(100),
    Employee_Aljex_ID VARCHAR(100),
    Employee_Relay_ID VARCHAR(100),
    Employee_Name VARCHAR(255),
    Employee_Email VARCHAR(255),
    Employment_Status VARCHAR(255),
    JobCode VARCHAR(255),
    JobTitle VARCHAR(255),
    CompanyName VARCHAR(255),
    BusinessName VARCHAR(255),
    DivisionName VARCHAR(255),
    RegionName VARCHAR(255),
    DepartmentName VARCHAR(255),
    DeptCode VARCHAR(255),
    LocationCode VARCHAR(255),
    LocationName VARCHAR(255),
    LocationSite VARCHAR(255),
    EmployeeType VARCHAR(255),
    FullOrPartTime VARCHAR(255),
    LastHireDate DATE,
    OriginalHireDate DATE,
    BenefitsSenorityDate DATE,
    SenorityDate DATE,
    SenorityYears DECIMAL(10,2),
    DateInJob DATE,
    Merge_Key VARCHAR(255),
    Hash_Key VARCHAR(255),
    Created_Date TIMESTAMP,
    Created_By VARCHAR(255),
    Last_Modified_Date TIMESTAMP,
    Last_Modified_By VARCHAR(255),
    Is_Deleted INT
)