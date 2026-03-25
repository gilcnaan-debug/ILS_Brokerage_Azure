-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## View

-- COMMAND ----------

CREATE OR REPLACE VIEW Silver.VW_Silver_Employees AS
SELECT
  ultipro_list.employee_number AS Employee_Number,
  aljex_user_report_listing.aljex_id AS Employee_Aljex_ID,
  relay_users.user_id AS Employee_Relay_ID,
  ultipro_list.employee_name AS Employee_Name,
  ultipro_list.email AS Employee_Email,
  ultipro_list.employment_status AS Employment_Status,
  ultipro_list.job_code AS JobCode,
  ultipro_list.job_title AS JobTitle,
  ultipro_list.company_name AS CompanyName,
  ultipro_list.business_name AS BusinessName,
  ultipro_list.division_name AS DivisionName,
  ultipro_list.region_name AS RegionName,
  ultipro_list.department_name AS DepartmentName,
  ultipro_list.dept_code AS DeptCode,
  ultipro_list.location_code AS LocationCode,
  ultipro_list.location_name AS LocationName,
  ultipro_list.location_site AS LocationSite,
  ultipro_list.employee_type AS EmployeeType,
  ultipro_list.full_or_part_time AS FullOrPartTime,
  ultipro_list.last_hire_date AS LastHireDate,
  ultipro_list.original_hire_date AS OriginalHireDate,
  ultipro_list.benefits_senority_date AS BenefitsSenorityDate,
  ultipro_list.senority_date AS SenorityDate,
  ultipro_list.senority_years AS SenorityYears,
  ultipro_list.date_in_job AS DateInJob,
  sha2(concat_ws('|', ultipro_list.employee_name, ultipro_list.employee_number), 256) AS Merge_Key,
  sha2(concat_ws('|', 
    ultipro_list.employee_number,
    aljex_user_report_listing.aljex_id,
    relay_users.user_id,
    ultipro_list.employee_name,
    ultipro_list.email,
    ultipro_list.employment_status,
    ultipro_list.job_code,
    ultipro_list.job_title,
    ultipro_list.company_name,
    ultipro_list.business_name,
    ultipro_list.division_name,
    ultipro_list.region_name,
    ultipro_list.department_name,
    ultipro_list.dept_code,
    ultipro_list.location_code,
    ultipro_list.location_name,
    ultipro_list.location_site,
    ultipro_list.employee_type,
    ultipro_list.full_or_part_time,
    cast(ultipro_list.last_hire_date as string),
    cast(ultipro_list.original_hire_date as string),
    cast(ultipro_list.benefits_senority_date as string),
    cast(ultipro_list.senority_date as string),
    cast(ultipro_list.senority_years as string),
    cast(ultipro_list.date_in_job as string)
  ), 256) AS Hash_Key
FROM bronze.ultipro_list
LEFT JOIN bronze.aljex_user_report_listing
  ON upper(ultipro_list.employee_name) = upper(aljex_user_report_listing.ultipro_name)
  AND lower(aljex_user_report_listing.aljex_id) <> "repeat"
LEFT JOIN bronze.relay_users
  ON (
    upper(ultipro_list.employee_name) = upper(concat(relay_users.last_name, ", ", relay_users.first_name))
    OR ultipro_list.email = relay_users.email_address
  )
  AND relay_users.`active?` = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DDL

-- COMMAND ----------

CREATE OR REPLACE TABLE Silver.Silver_Employees ( 
    DW_Employee_ID BIGINT GENERATED ALWAYS AS IDENTITY,
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