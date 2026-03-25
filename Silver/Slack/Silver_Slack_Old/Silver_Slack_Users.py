# Databricks notebook source
# MAGIC %md
# MAGIC ## BronzeToSilver
# MAGIC * **Description:** To extract tables from Bronze to Silver as delta file
# MAGIC * **Created Date:** 15/03/2024
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** 06/06/2024
# MAGIC * **Modified By:** Freedon Demi
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import Required Packages

# COMMAND ----------

from pyspark.sql.functions import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initializing Utilities

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initializing Logger

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

ErrorLogger = ErrorLogs("Silver_slack_users_1hr_Refresh")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Use Catalog

# COMMAND ----------

CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Metadata Details

# COMMAND ----------

TableID = 'SZ21'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

# COMMAND ----------

try:
    TablesList = DF_Metadata.select(col("TableID")).collect()
    TableName = (
        DF_Metadata.select(col("SourceTableName"))
        .where(col("TableID") == TableID)
        .collect()[0]
        .SourceTableName
    )
    ErrorPath = (
        DF_Metadata.select(col("ErrorLogPath"))
        .where(col("TableID") == TableID)
        .collect()[0]
        .ErrorLogPath
    )
except Exception as e:
    logger.info("Unable to fetch details from metadata")
    print(e)

# COMMAND ----------

try:
    Job_ID = "JOB_4"
    Notebook_ID = "NB_4"
    Zone = "Silver"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Perform Required Transformations

# COMMAND ----------

try:
    DF_Users = spark.sql(
                    '''select * FROM bronze.Slack_Users  '''
                )
    DF_Users = DF_Users \
    .withColumnRenamed("id",  "User_ID") \
	.withColumnRenamed("team_id",  "Team_ID") \
	.withColumnRenamed("name",  "User_Name") \
	.withColumnRenamed("deleted",  "Is_Deleted") \
	.withColumnRenamed("color",  "Color") \
	.withColumnRenamed("real_name",  "User_Real_Name") \
	.withColumnRenamed("tz",  "User_TimeZone") \
	.withColumnRenamed("tz_label",  "TimeZone_Label") \
	.withColumnRenamed("tz_offset",  "TimeZone_Offset") \
	.withColumnRenamed("profile",  "Profile") \
	.withColumnRenamed("is_admin",  "Is_Admin") \
	.withColumnRenamed("is_owner",  "Is_Owner") \
	.withColumnRenamed("is_primary_owner",  "Is_Primary_Owner") \
	.withColumnRenamed("is_restricted",  "Is_Restricted") \
	.withColumnRenamed("is_ultra_restricted",  "Is_Ultra_Restricted") \
	.withColumnRenamed("is_bot",  "Is_Bot") \
	.withColumnRenamed("is_app_user",  "Is_App_User") \
	.withColumnRenamed("updated",  "Updated_Date") \
	.withColumnRenamed("is_email_confirmed",  "Is_Email_Confirmed") \
	.withColumnRenamed("who_can_share_contact_card",  "Who_Can_Share_Contact_Card") \
	.withColumnRenamed("Sourcesystem_ID",  "Sourcesystem_ID") \
	.withColumnRenamed("Sourcesystem_Name",  "Sourcesystem_Name") \
    .withColumnRenamed("DW_Timestamp",  "DW_Timestamp") \
	.withColumnRenamed("Created_By",  "Created_By") \
	.withColumnRenamed("Created_Date",  "Created_Date") \
	.withColumnRenamed("Last_Modified_By",  "Last_Modified_By") \
	.withColumnRenamed("Last_Modified_Date",  "Last_Modified_Date") 
except Exception as e:
    logger.error(
        f"Unable to create dataframe for the Dim_Shippers table: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge To Target Table

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID,'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            DF_Users.createOrReplaceTempView('BronzeToSilver')
            Rowcount = DF_Users.count()
            TruncateQuery = "Truncate Table silver."+TableName
            spark.sql(TruncateQuery)
            MergeQuery = "INSERT INTO silver."+TableName+" TABLE BronzeToSilver;"
            spark.sql(MergeQuery.format(TableName))
            UpdatePipelineStatusAndTime(TableID,'silver')
            logger.info('Successfully loaded the'+TableName+'to silver')
        except Exception as e:
            Error_Statement = str(e).replace("'", "''")
            UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Merge-Target Table")
            logger.info('Failed for silver load')
            UpdateFailedStatus(TableID,'silver')
            logger.info('Updated the Metadata for Failed Status '+TableName)
            print('Unable to load '+TableName)
            print(e)
        UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Merge-Target Table")
except Exception as e:
    # logger error message
    logger.error(
        f"Table is already loaded: {str(e)}")
    print(e)
