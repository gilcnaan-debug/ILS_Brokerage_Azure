# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Time Table 
# MAGIC * **Description:** To Transform data from Validated Awards to Time Table Break down as delta file
# MAGIC * **Created Date:** 07/10/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 07/10/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #### IMPORTING REQUIRED PACKAGES

# COMMAND ----------

#Importing packages
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC #### CALLING UTILITIES AND LOGGERS NOTEBOOK

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("NFI_Silver_Awards_Time_Table")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC #### METADATA DETAILS

# COMMAND ----------

#Getting the Table details in the dataframe that needs to be loaded into silver from Metadata Table
Table_ID = 'AW5'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = 'AW5' and IsActive='1'")
Job_ID = DF_Metadata.select(col('JOB_ID')).where(col('TableID') == Table_ID).collect()[0].JOB_ID
Notebook_ID = DF_Metadata.select(col('NB_ID')).where(col('TableID') == Table_ID).collect()[0].NB_ID


# COMMAND ----------

TableName = DF_Metadata.select(col('DWHTableName')).where(col('TableID') == Table_ID).collect()[0].DWHTableName
MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateColumn
MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateValue
Zone =DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone

# COMMAND ----------

# MAGIC %md
# MAGIC #### REQUIRED TRANSFORMATIONS

# COMMAND ----------

TimeTable_Scripts = '''
    WITH Period_Start_Date AS (
        SELECT
            a.DW_Validated_Award_ID,
            a.RFP_Name,
            a.Customer_Slug,
            a.Customer_Master,
            a.SAM_Name,
            a.Origin,
            a.Origin_Country,
            a.Destination,
            a.Destination_Country,
            a.Equipment_Type,
            a.Estimated_Volume,
            a.Estimated_Miles,
            a.Linehaul,
            a.RPM,
            a.Flat_Rate,
            a.Min_Rate,
            a.Max_Rate,
            a.Validation_Criteria,
            a.Award_Type,
            a.Award_Notes,
            a.Periodic_Start_Date,
            a.Periodic_End_Date,
            a.Shipper_Recurrence,
            a.Updated_by,
            a.Updated_date,
            a.contract_type,
            a.expected_margin,
            b.financial_period_number as Start_Month,
            RIGHT(b.period_year, 4) as Start_Year,
             Concat ( b.financial_period_number,'-',RIGHT(b.period_year, 4)) as Start_Period_year

        FROM (select * from silver.silver_nfi_awards_validated where last_modified_date > '{0}' and Is_Deleted = 0 ) a left join analytics.dim_date b
        on a.Periodic_Start_Date = b.calendar_date),
        Period_End_Date AS (
        SELECT
            a.DW_Validated_Award_ID,
            a.RFP_Name,
            a.Customer_Slug,
            a.Customer_Master,
            a.SAM_Name,
            a.Origin,
            a.Origin_Country,
            a.Destination,
            a.Destination_Country,
            a.Equipment_Type,
            a.Estimated_Volume,
            a.Estimated_Miles,
            a.Linehaul,
            a.RPM,
            a.Flat_Rate,
            a.Min_Rate,
            a.Max_Rate,
            a.Validation_Criteria,
            a.Award_Type,
            a.Award_Notes,
            a.Periodic_Start_Date,
            a.Periodic_End_Date,
            a.Shipper_Recurrence,
            a.Updated_by,
            a.Updated_date,
            a.contract_type,
            a.expected_margin,
            a.Start_Period_Year,
            a.Start_Month,
            a.Start_Year,
            b.financial_period_number as End_month,
            RIGHT(b.period_year, 4) as End_Year,
        Concat( b.financial_period_number,'-',RIGHT(b.period_year, 4)) as End_Period_year
        FROM Period_Start_Date a left join analytics.dim_date b on a.Periodic_End_Date = b.calendar_date),
        Date_Multiplied_Duplicates AS (
        SELECT
            a.*,
            b.financial_period_number AS Month,
            RIGHT(b.period_year, 4) AS Year,
            CONCAT(b.financial_period_number,"-",RIGHT(b.period_year, 4)) AS period_year 
        FROM Period_End_Date a
        LEFT JOIN analytics.dim_date b
        ON b.calendar_date >= a.Periodic_Start_Date
        AND b.calendar_date <= a.Periodic_End_Date
        AND (
            (a.Start_Year = a.End_Year AND b.financial_period_number BETWEEN a.Start_Month AND a.End_Month)
            OR
            (a.Start_Year < a.End_Year AND 
                ((RIGHT(b.period_year, 4) = a.Start_Year AND b.financial_period_number >= a.Start_Month)
                OR (RIGHT(b.period_year, 4) = a.End_Year AND b.financial_period_number <= a.End_Month)
                OR (RIGHT(b.period_year, 4) > a.Start_Year AND RIGHT(b.period_year, 4) < a.End_Year))
            )
        )
        ),
        Date_Multiplied AS (
        SELECT * FROM Date_Multiplied_Duplicates GROUP BY ALL),
        Date_Range AS (
        SELECT
            *,
            ((End_Year - Start_Year) * 12 + End_Month - Start_Month + 1) AS Period_Count,
            Estimated_Volume / ((End_Year - Start_Year) * 12 + End_Month - Start_Month + 1) AS Periodical_Volume
        FROM Date_Multiplied
        ),

        RevenueCalculated AS (
        SELECT
        *,
        CASE
        WHEN Linehaul IS NOT NULL THEN Linehaul * Periodical_Volume
        WHEN Flat_Rate IS NOT NULL THEN Flat_Rate * Periodical_Volume
        WHEN Max_Rate IS NOT NULL THEN Max_Rate * Periodical_Volume
        WHEN Min_Rate IS NOT NULL THEN Min_Rate * Periodical_Volume
        WHEN RPM IS NOT NULL THEN RPM * Estimated_Miles * Periodical_Volume
        ELSE 0 
        END AS Periodical_Revenue
        FROM Date_Range),
        Key As (
        Select *,Concat(Customer_master,Coalesce(Criteria_Description, ' '),Period_year) as Awardkey ,  Concat(Customer_master,Period_year) as Agingkey from (
        SELECT *,
            CASE 
                WHEN Validation_Criteria = '5digit-5digit' THEN CONCAT(
                    CASE 
                        WHEN REGEXP_LIKE(Origin, '^[0-9]+$') THEN LPAD(Origin, 5, '0')
                        ELSE REPLACE(TRIM(UPPER(Origin)), ' ', '')
                    END,', ',TRIM(UPPER(Origin_Country)), ' - ', 
                    CASE 
                        WHEN REGEXP_LIKE(Destination, '^[0-9]+$') THEN LPAD(Destination, 5, '0')
                        ELSE REPLACE(TRIM(UPPER(Destination)), ' ', '')
                    END,', ', TRIM(UPPER(Destination_Country)))
                WHEN Validation_Criteria = 'city-city' THEN 
                UPPER(CONCAT(
                    REPLACE(ORIGIN, ',', ', '),', ',UPPER(TRIM(Origin_Country)), 
                    ' - ', 
                    REPLACE(Destination, ',', ', ') ,', ', UPPER(TRIM(Destination_Country))
                    ))       
            END AS Criteria_Description


        FROM RevenueCalculated))
        SELECT DISTINCT * FROM key '''
TimeTable_Scripts = TimeTable_Scripts.format(MaxLoadDate)


# COMMAND ----------

try:
        
    DF_timetable = spark.sql(TimeTable_Scripts)
    
except Exception as e:
    # logger error message
    logger.error(f"Unable to create view for the silver_nfi_awards_validated table: {str(e)}")
    print(e)

# COMMAND ----------

try:
    Hashkey_Merge = [
    "Estimated_Volume",
    "Estimated_Miles",
    "Linehaul",
    "RPM",
    "Flat_Rate",
    "Min_Rate",
    "Max_Rate",
    "Period_Count",
    "Periodical_Volume",
    "Periodical_Revenue",
    "Award_Type",
    "Award_Notes",
    "Shipper_Recurrence",
    "Updated_by",
    "Updated_date",
    "Start_Month",
    "Start_Year",
    "Start_Period_Year",
    "End_month",
    "End_Year",
    "End_Period_year",
    "Month",
    "Year"]

    DF_timetable = DF_timetable.withColumn("Hashkey",md5(concat_ws("",*Hashkey_Merge)))
except Exception as e:
    logger.info(f"Unable to create Hashkey for Awards_Period {e}")
    print(e)

try:

    # Concatenate all columns and generate MD5 hash
    DF_timetable = DF_timetable.withColumn(
        "Mergekey",
        md5(
            concat_ws(
                '|',
                col('DW_Validated_Award_ID'),
                col('customer_master'),
                col('customer_slug'),
                col('sam_name'),
                col('rfp_name'),
                col('Validation_Criteria'),
                col('periodic_start_date').cast('string'),
                col('periodic_end_date').cast('string'),
                col('equipment_type'),
                col('contract_type'),
                col('period_year'),
                col('Origin'), 
                col('Origin_Country'),
                col('Destination'),
                col('Destination_Country')
            )
        )
    )
    DF_timetable.createOrReplaceTempView('VW_Awards_Period')
    
except Exception as e:
    logger.info(f"Unable to create Mergekey for silver_stops {e}")
    print(e)




# COMMAND ----------

# MAGIC %md
# MAGIC #### MERGE DATA INTO TARGET

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(Table_ID,'Silver')
    if AutoSkipperCheck == 0:
       
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_NFI_Awards_Time_Table as CT using VW_Awards_Period as CS ON CS.Mergekey=CT.Mergekey WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                    CT.Estimated_Volume = CS.Estimated_Volume,
                    CT.Estimated_Miles = CS.Estimated_Miles,
                    CT.Linehaul = CS.Linehaul,
                    CT.RPM = CS.RPM,
                    CT.Flat_Rate = CS.Flat_Rate,
                    CT.Min_Rate = CS.Min_Rate,
                    CT.Max_Rate = CS.Max_Rate,
                    CT.Period_Count = CS.Period_Count,
                    CT.Periodical_Volume = CS.Periodical_Volume,
                    CT.Periodical_Revenue = CS.Periodical_Revenue,
                    CT.Award_Type = CS.Award_Type,
                    CT.Award_Notes = CS.Award_Notes,
                    CT.Shipper_Recurrence = CS.Shipper_Recurrence,
                    CT.Updated_by = CS.Updated_by,
                    CT.Updated_date = CS.Updated_date,
                    CT.Expected_Margin = CS.Expected_Margin,
                    CT.Start_Month = CS.Start_Month,
                    CT.Start_Year = CS.Start_Year,
                    CT.Start_Period_Year = CS.Start_Period_Year,
                    CT.End_month = CS.End_month,
                    CT.End_Year = CS.End_Year,
                    CT.End_Period_year = CS.End_Period_year,
                    CT.Month = CS.Month,
                    CT.Year = CS.Year,
                    CT.HashKey = CS.HashKey,
                    CT.Last_Modified_Date = current_timestamp(),
                    CT.Last_Modified_By = 'Databricks',
                    CT.Is_Deleted = 0
                    
                    
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (  
                    CT.DW_Validated_Award_ID,
                    CT.RFP_Name,
                    CT.Customer_Slug,
                    CT.Customer_Master,
                    CT.SAM_Name,
                    CT.Origin,
                    CT.Origin_Country,
                    CT.Destination,
                    CT.Destination_Country,
                    CT.Equipment_Type,
                    CT.Estimated_Volume,
                    CT.Estimated_Miles,
                    CT.Linehaul,
                    CT.RPM,
                    CT.Flat_Rate,
                    CT.Min_Rate,
                    CT.Max_Rate,
                    CT.Period_Count,
                    CT.Periodical_Volume,
                    CT.Periodical_Revenue,
                    CT.Validation_Criteria,
                    CT.Award_Type,
                    CT.Award_Notes,
                    CT.Periodic_Start_Date,
                    CT.Periodic_End_Date,
                    CT.Shipper_Recurrence,
                    CT.Updated_by,
                    CT.Updated_date,
                    CT.Period_Year,
                    CT.Start_Month,
                    CT.Start_Year,
                    CT.Start_Period_Year,
                    CT.End_month,
                    CT.End_Year,
                    CT.End_Period_year,
                    CT.Month,
                    CT.Year,
                    CT.Contract_Type,
                    CT.Expected_Margin,
                    CT.MergeKey,
                    CT.HashKey,
                    CT.Awardkey,
                    CT.Agingkey,
                    CT.Sourcesystem_Name,
                    CT.Sourcesystem_ID,
                    CT.Created_Date,
                    CT.Created_By,
                    CT.Last_Modified_Date,
                    CT.Last_Modified_By,
                    CT.Is_Deleted 
                        )
                        VALUES
                        ( 
                        CS.DW_Validated_Award_ID,
                        CS.RFP_Name,
                        CS.Customer_Slug,
                        CS.Customer_Master,
                        CS.SAM_Name,
                        CS.Origin,
                        CS.Origin_Country,
                        CS.Destination,
                        CS.Destination_Country,
                        CS.Equipment_Type,
                        CS.Estimated_Volume,
                        CS.Estimated_Miles,
                        CS.Linehaul,
                        CS.RPM,
                        CS.Flat_Rate,
                        CS.Min_Rate,
                        CS.Max_Rate,
                        CS.Period_Count,
                        CS.Periodical_Volume,
                        CS.Periodical_Revenue,
                        CS.Validation_Criteria,
                        CS.Award_Type,
                        CS.Award_Notes,
                        CS.Periodic_Start_Date,
                        CS.Periodic_End_Date,
                        CS.Shipper_Recurrence,
                        CS.Updated_by,
                        CS.Updated_date,
                        CS.Period_Year,
                        CS.Start_Month,
                        CS.Start_Year,
                        CS.Start_Period_Year,
                        CS.End_month,
                        CS.End_Year,
                        CS.End_Period_year,
                        CS.Month,
                        CS.Year,
                        CS.Contract_Type,
                        CS.Expected_Margin,
                        CS.MergeKey,
                        CS.HashKey,
                        CS.Awardkey,
                        CS.Agingkey,
                        'Awards File',
                        '9',
                        current_timestamp(),
                        "Databricks",
                        current_timestamp(),
                        "Databricks",
                        0
                            
                        )
                    ''')
            
            # Source Delete Handling
            spark.sql(f'''
                    UPDATE Silver.Silver_NFI_Awards_Time_Table
                    SET Silver.Silver_NFI_Awards_Time_Table.Is_Deleted = 1, 
                        Silver.Silver_NFI_Awards_Time_Table.Last_Modified_Date = current_timestamp()
                    where Silver_NFI_Awards_Time_Table.DW_Validated_Award_ID in (select DW_Validated_Award_ID from Silver.Silver_NFI_Awards_Validated where Is_Deleted = 1) AND  Silver_NFI_Awards_Time_Table.Is_Deleted = 0
                ''')
            
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_NFI_Awards_Time_Table"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(Table_ID,DF_MaxDate)
            UpdatePipelineStatusAndTime(Table_ID,'Silver')
            UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Load")
          
        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_NFI_Awards_Time_Table table")
            print(e)
            UpdateFailedStatus(Table_ID,'Silver')
            Error_Statement = str(e).replace(',', '').replace("'", '')
            UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Load")

except Exception as e:
    logger.info('Failed for Silver_NFI_Awards_Time_Table')
    UpdateFailedStatus(Table_ID,'Silver')
    print('Unable to load '+TableName)
    print(e)
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Load")

# COMMAND ----------

