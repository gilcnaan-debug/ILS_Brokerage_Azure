# Databricks notebook source
# MAGIC %md #Silver_Employee

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Bronze layer and load to Silver zone to form Silver_Employee table with joins and transformations- Incremental load
# MAGIC * **Created Date:** 11/12/2023
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** - 
# MAGIC * **Modified By:**
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import the required packages

# COMMAND ----------

##Import all the required packages
from pyspark.sql.functions import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize the Utility notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initialize the Logger notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Create variables to store error log information

ErrorLogger = ErrorLogs("Silver_Employee_BronzetoSilver")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
TableID = 'SZ4'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data to Silver_Employee table

# COMMAND ----------

# Select the Employee table from the bronze layer for Edge source and perform required transformations.
try:
    DF_Edge_Employee = spark.sql('''
                                    select 
                                    ul.employee_number as Employee_ID,
                                    ul.employee_name as Employee_Name,
                                    ul.job_title as Employee_Title,
                                    ul.division_name as Employee_Division,
                                    ul.email as Employee_Email ,
                                    ul.company_name as Company_Name ,
                                    ul.department_name as Employee_Department ,
                                    ul.employment_status as Employee_Status,
                                    ul.original_hire_date as Employee_Creation_Date ,
                                    ul.last_hire_date as Employee_LastUpdated_Date ,
                                    case when ul.full_or_part_time ='F' then "Full time"
                                     when ul.full_or_part_time ='P' then "Part time" else ul.full_or_part_time end as Employment_Type,
                                    ul.Senority_Years  as  Seniority_Years,
                                    "3" as Sourcesystem_ID,
                                    "Edge" as Sourcesystem_Name,
                                    "Databricks" as Created_By,
                                    CURRENT_TIMESTAMP() as Created_Date,
                                    "Databricks" as  Last_Modified_By,
                                    CURRENT_TIMESTAMP() as Last_Modified_Date
                                    from bronze.ultipro_list ul
                                ''') 

except Exception as e:
    logger.info(f"Unable to perform the transformations for Employee table {e}")
    print(e)
try:
###Use dropduplicates function to drop the duplicate columns and store it in the dataframe
    DF_Edge_Employee =DF_Edge_Employee.dropDuplicates(['Employee_ID'])
except Exception as e:
    logger.info(f"Unable to drop duplicates for the Office table from Relay {e}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Hashkey and Merge the data to Silver_Employee

# COMMAND ----------

#Create the Hashkey to merge the data to Silver_Employee table
try:
    Hashkey_Merge = ["Employee_ID","Employee_Name","Employee_Email","Sourcesystem_ID"]

    DF_Employee = DF_Edge_Employee.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for silver_Employee {e}")
    print(e)

#Create a Temporary view for the Silver_Employee table
try:
    DF_Employee.createOrReplaceTempView('VW_silver_Employee')
except Exception as e:
    logger.info(f"Unable to create temporary view for the Silver_Employee table")
    print(e)

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Silver')
    if AutoSkipperCheck == 0:
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_Employee as CT using VW_silver_Employee as CS ON CS.Hashkey=CT.Hashkey WHEN MATCHED THEN UPDATE SET 
                    CT.Employee_ID = CS.Employee_ID,
                    CT.Hashkey = CS.Hashkey,
                    CT.Employee_Name = CS.Employee_Name,
                    CT.Employee_Title = CS.Employee_Title,
                    CT.Employee_Division = CS.Employee_Division,
                    CT.Employee_Email = CS.Employee_Email,
                    CT.Company_Name = CS.Company_Name, 
                    CT.Employee_Department = CS.Employee_Department,
                    CT.Employee_Status = CS.Employee_Status, 
                    CT.Employee_Creation_Date = CS.Employee_Creation_Date, 
                    CT.Employee_LastUpdated_Date = CS.Employee_LastUpdated_Date,
                    CT.Employment_Type = CS.Employment_Type,
                    CT.Seniority_Years = CS.Seniority_Years,
                    CT.Sourcesystem_ID = CS.Sourcesystem_ID,
                    CT.Sourcesystem_Name = CS.Sourcesystem_Name,
                    CT.Created_By = CS.Created_By,
                    CT.Created_Date = CS.Created_Date,
                    CT.Last_Modified_By = CS.Last_Modified_By, 
                    CT.Last_Modified_date = CS.Last_Modified_Date
                    
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   CT.Employee_ID,
                        CT.Hashkey,
                        CT.Employee_Name,
                        CT.Employee_Title,
                        CT.Employee_Division,
                        CT.Employee_Email,
                        CT.Company_Name,
                        CT.Employee_Department,
                        CT.Employee_Status,
                        CT.Employee_Creation_Date,
                        CT.Employee_LastUpdated_Date,
                        CT.Employment_Type,
                        CT.Seniority_Years,
                        CT.Sourcesystem_ID,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date
                        
                        )
                        VALUES
                        (
                            CS.Employee_ID,
                            CS.Hashkey,
                            CS.Employee_Name,
                            CS.Employee_Title,
                            CS.Employee_Division,
                            CS.Employee_Email,
                            CS.Company_Name,
                            CS.Employee_Department,
                            CS.Employee_Status,
                            CS.Employee_Creation_Date,
                            CS.Employee_LastUpdated_Date,
                            CS.Employment_Type,
                            CS.Seniority_Years,
                            CS.Sourcesystem_ID,
                            CS.Sourcesystem_Name,
                            CS.Created_By,
                            CS.Created_Date,
                            CS.Last_Modified_By,
                            CS.Last_Modified_Date
                            
                        )
                    ''')
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_Employee"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(TableID,DF_MaxDate)
            UpdatePipelineStatusAndTime(TableID,'Silver')
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_Employee table")
            print(e)
except Exception as e:
    logger.info('Failed for Silver_Employee')
    UpdateFailedStatus(TableID,'Silver')
    logger.info('Updated the Metadata for Failed Status '+TableName)
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

