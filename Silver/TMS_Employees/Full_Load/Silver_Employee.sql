-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver Employee
-- MAGIC * **Description:** To incrementally load the data from Bronze to Silver as delta file
-- MAGIC * **Created Date:** 03/07/2025
-- MAGIC * **Created By:** Uday
-- MAGIC * **Modified Date:** 06/10/2025
-- MAGIC * **Modified By:** Uday
-- MAGIC * **Changes Made:** Logic Update

-- COMMAND ----------

UPDATE metadata.mastermetadata 
SET PipelineStartTime = current_timestamp()
WHERE TableID = 'SL14'

-- COMMAND ----------

-- DBTITLE 1,Merge Script
MERGE INTO Silver.Silver_Employees AS tgt
USING Silver.VW_Silver_Employees AS src
ON tgt.Merge_Key = src.Merge_Key
WHEN MATCHED AND tgt.Hash_Key != src.Hash_Key THEN
    UPDATE SET
        tgt.Employee_Number = src.Employee_Number,
        tgt.Employee_Aljex_ID = src.Employee_Aljex_ID,
        tgt.Employee_Relay_ID = src.Employee_Relay_ID,
        tgt.Employee_Name = src.Employee_Name,
        tgt.Employee_Email = src.Employee_Email,
        tgt.Employment_Status = src.Employment_Status,
        tgt.JobCode = src.JobCode,
        tgt.JobTitle = src.JobTitle,
        tgt.CompanyName = src.CompanyName,
        tgt.BusinessName = src.BusinessName,
        tgt.DivisionName = src.DivisionName,
        tgt.RegionName = src.RegionName,
        tgt.DepartmentName = src.DepartmentName,
        tgt.DeptCode = src.DeptCode,
        tgt.LocationCode = src.LocationCode,
        tgt.LocationName = src.LocationName,
        tgt.LocationSite = src.LocationSite,
        tgt.EmployeeType = src.EmployeeType,
        tgt.FullOrPartTime = src.FullOrPartTime,
        tgt.LastHireDate = src.LastHireDate,
        tgt.OriginalHireDate = src.OriginalHireDate,
        tgt.BenefitsSenorityDate = src.BenefitsSenorityDate,
        tgt.SenorityDate = src.SenorityDate,
        tgt.SenorityYears = src.SenorityYears,
        tgt.DateInJob = src.DateInJob,
        tgt.Merge_Key = src.Merge_Key,
        tgt.Hash_Key = src.Hash_Key,
        tgt.Last_Modified_Date = current_timestamp(),
        tgt.Last_Modified_By = 'Databricks'
WHEN MATCHED AND tgt.Is_Deleted = 1 AND tgt.Hash_Key = src.Hash_Key THEN
    UPDATE SET
        tgt.Last_Modified_Date = current_timestamp(),
        tgt.Last_Modified_By = 'Databricks',
        tgt.Is_Deleted = 0
WHEN NOT MATCHED THEN
    INSERT (
        Employee_Number,
        Employee_Aljex_ID,
        Employee_Relay_ID,
        Employee_Name,
        Employee_Email,
        Employment_Status,
        JobCode,
        JobTitle,
        CompanyName,
        BusinessName,
        DivisionName,
        RegionName,
        DepartmentName,
        DeptCode,
        LocationCode,
        LocationName,
        LocationSite,
        EmployeeType,
        FullOrPartTime,
        LastHireDate,
        OriginalHireDate,
        BenefitsSenorityDate,
        SenorityDate,
        SenorityYears,
        DateInJob,
        Merge_Key,
        Hash_Key,
        Created_Date,
        Created_By,
        Last_Modified_Date,
        Last_Modified_By,
        Is_Deleted
    )
    VALUES (
        src.Employee_Number,
        src.Employee_Aljex_ID,
        src.Employee_Relay_ID,
        src.Employee_Name,
        src.Employee_Email,
        src.Employment_Status,
        src.JobCode,
        src.JobTitle,
        src.CompanyName,
        src.BusinessName,
        src.DivisionName,
        src.RegionName,
        src.DepartmentName,
        src.DeptCode,
        src.LocationCode,
        src.LocationName,
        src.LocationSite,
        src.EmployeeType,
        src.FullOrPartTime,
        src.LastHireDate,
        src.OriginalHireDate,
        src.BenefitsSenorityDate,
        src.SenorityDate,
        src.SenorityYears,
        src.DateInJob,
        src.Merge_Key,
        src.Hash_Key,
        current_timestamp(),
        'Databricks',
        current_timestamp(),
        'Databricks',
        0
    )

-- COMMAND ----------

-- DBTITLE 1,Metadata update
UPDATE metadata.mastermetadata
SET LastLoadDateValue = (SELECT MAX(Last_Modified_Date) FROM Silver.Silver_Employees where Is_Deleted = 0),
    PipelineRunStatus = 'Success'
    , PipelineEndTime = current_timestamp()
WHERE TableID = "SL14"