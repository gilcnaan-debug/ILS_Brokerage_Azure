# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Filtered
# MAGIC * **Description:** To extract data from bronze to silver as delta file
# MAGIC * **Created Date:** 07/10/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 07/10/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ##IMPORTING REQUIRED PACKAGES
# MAGIC

# COMMAND ----------

#Importing packages
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pytz
import gspread
import pandas as pd
import json
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %md
# MAGIC ##CALLING UTILITIES NOTEBOOK

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ##CALLING LOGGER NOTEBOOK

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ##ERROR LOGGER PATH

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("NFI_BronzeToSilver_Awards_Filtered_")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##COLLECTING METADATA DETAILS

# COMMAND ----------

#Getting the Table details in the dataframe that needs to be loaded into bronze from Metadata Table
Table_ID = 'AW2'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = 'AW2' and IsActive='1'")
Job_ID = DF_Metadata.select(col('JOB_ID')).where(col('TableID') == Table_ID).collect()[0].JOB_ID
Notebook_ID = DF_Metadata.select(col('NB_ID')).where(col('TableID') == Table_ID).collect()[0].NB_ID
TableName = DF_Metadata.select(col('DWHTableName')).where(col('TableID') == Table_ID).collect()[0].DWHTableName
MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateColumn
MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateValue
Zone = DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone
SourceSelectQuery = DF_Metadata.select(col('SourceSelectQuery')).where(col('TableID') == Table_ID).collect()[0].SourceSelectQuery


# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED TRANFORMATIONS

# COMMAND ----------

try:
    DF_Source_Bronze= spark.sql(SourceSelectQuery.format(MaxLoadDate))

    # Concatenate all columns and generate MD5 hash
    DF_Source_Bronze = DF_Source_Bronze.withColumn(
        "Mergekey",
        md5(
            concat_ws(
                '|',
                upper(col('customer_master')).alias('customer_master'),
                upper(col('customer_slug')).alias('customer_slug'),
                upper(col('sam_name')).alias('sam_name'),
                upper(col('rfp_name')).alias('rfp_name'),
                upper(col('Validation_Criteria')).alias('Validation_Criteria'),
                col('periodic_start_date').cast('string'),
                col('periodic_end_date').cast('string'),
                upper(col('equipment_type')).alias('equipment_type'),
                upper(col('contract_type')).alias('contract_type'),
                upper(col('Origin')).alias('Origin'),
                upper(col('Origin_Country')).alias('Origin_Country'),
                upper(col('Destination')).alias('Destination'),
                upper(col('Destination_Country')).alias('Destination_Country')     )
        )
    )

    Hashkey_columns = [
        'Estimated_Volume', 'Estimated_Miles', 'Linehaul', 'RPM', 'Flat_Rate',
        'Min_Rate', 'Max_Rate', 'Award_Type', 'Award_Notes',
        'Shipper_Recurrence', 'updated_by',
        'Updated_date',  'Expected_Margin'
    ]

    DF_Source_Bronze = DF_Source_Bronze.withColumn("Hashkey",md5(concat_ws("",*Hashkey_columns)))

    DF_Source_Bronze.createOrReplaceTempView('VW_Awards_Filtered')

except Exception as e:
    logger.info('Failed for Silver Filtered')
    print(e)



# COMMAND ----------

# MAGIC %md
# MAGIC ##MERGE INTO DESTINATION TABLE

# COMMAND ----------


try:
    AutoSkipperCheck = AutoSkipper(Table_ID,'Silver')
    if AutoSkipperCheck == 0:

        #Load Data into Table
        spark.sql(f'''MERGE INTO Silver.Silver_NFI_Awards_Filtered Target USING VW_Awards_Filtered Source ON Target.DW_Award_ID = Source.DW_Award_ID WHEN NOT MATCHED THEN INSERT (
                Target.DW_Award_ID,
                Target.Hashkey,
                Target.Mergekey,
                Target.RFP_Name,
                Target.Customer_Master,
                Target.Customer_Slug,
                Target.SAM_Name,
                Target.Origin,
                Target.Origin_Country,
                Target.Destination,
                Target.Destination_Country, 
                Target.Equipment_Type,
                Target.Estimated_Volume,
                Target.Estimated_Miles,
                Target.Linehaul,
                Target.RPM,
                Target.Flat_Rate,
                Target.Min_Rate,
                Target.Max_Rate,
                Target.Validation_Criteria,
                Target.Award_Type,
                Target.Award_Notes,
                Target.Periodic_Start_Date,
                Target.Periodic_End_Date,
                Target.Shipper_Recurrence,
                Target.Updated_by,
                Target.Updated_date,
                Target.Contract_Type,
                Target.Expected_Margin,
                Target.Is_Deleted,
                Target.Sourcesystem_ID,
                Target.Sourcesystem_Name,
                Target.Created_date,
                Target.Created_by,
                Target.Last_Modified_date,
                Target.Last_Modified_by
            )
            Values
            (
                Source.DW_Award_ID,
                Source.Hashkey,
                Source.Mergekey,
                Source.RFP_Name,
                Source.Customer_Master,
                Source.Customer_Slug,
                Source.SAM_Name,
                Source.Origin,
                Source.Origin_Country,
                Source.Destination,
                Source.Destination_Country,
                Source.Equipment_Type,
                Source.Estimated_Volume,
                Source.Estimated_Miles,
                Source.Linehaul,
                Source.RPM,
                Source.Flat_Rate,
                Source.Min_Rate,
                Source.Max_Rate,
                Source.Validation_Criteria,
                Source.Award_Type,
                Source.Award_Notes,
                Source.Periodic_Start_Date,
                Source.Periodic_End_Date,
                Source.Shipper_Recurrence,
                Source.Updated_by,
                Source.Updated_date,
                Source.Contract_Type,
                Source.Expected_Margin,
                0,
                9,
                'Awards File',
                Current_timestamp(),
                'Databricks',
                Current_timestamp(),
                'Databricks'
            )
            ''')
        # Source Delete Handling
        spark.sql(f'''
                    UPDATE Silver.Silver_NFI_Awards_Filtered
                    SET Silver.Silver_NFI_Awards_Filtered.Is_Deleted = '1', 
                        Silver.Silver_NFI_Awards_Filtered.Last_Modified_date = Current_timestamp()
                    where Silver.Silver_NFI_Awards_Filtered.DW_Award_ID in (select s.DW_Award_ID from Bronze.NFI_Awards s where S.Status= 'D')
                    AND Silver.Silver_NFI_Awards_Filtered.Is_Deleted = '0'
                ''')

        # #Update The Metadata table
        MaxDateQuery = "Select max({0}) as Max_Date from Silver.{1}"
        MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn,TableName)
        DF_MaxDate = spark.sql(MaxDateQuery)
        UpdateLastLoadDate(Table_ID,DF_MaxDate)
        UpdatePipelineStatusAndTime(Table_ID,'Silver')
        UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Load")

except Exception as e:
    logger.info('Failed for Silver Filtered')
    UpdateFailedStatus(Table_ID,'Silver')
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Load")
    print('Unable to load '+TableName)
    print(e)