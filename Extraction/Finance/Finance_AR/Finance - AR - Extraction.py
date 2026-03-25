# Databricks notebook source
# MAGIC %md
# MAGIC #NFI_Finance_AR_Data_Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract the data from Source and load to Bronze zone
# MAGIC * **Created Date:** 11/18/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 11/18/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Import Required Packages

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
# MAGIC ###Initialize Utility Notebook

# COMMAND ----------

# MAGIC %run
# MAGIC "/Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize Logger Notebook

# COMMAND ----------

# MAGIC %run
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create variables to store error log information

# COMMAND ----------

try:
  ErrorLogger = ErrorLogs("Bronze_Finance_AR_SourcetoBronze")
  logger = ErrorLogger[0]
  p_logfile = ErrorLogger[1]
  p_filename = ErrorLogger[2]
except Exception as e:
    logger.info("Unable to get error log variables")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Metadata Details

# COMMAND ----------

try:
    DF_Metadata= spark.sql("SELECT * FROM Metadata.MasterMetadata where TableID='F16' and IsActive='1' ")

    Table_ID  =  DF_Metadata.select(col("TableID")).where(col("TableID") == "F16").collect()[0].TableID
    Job_ID = DF_Metadata.select(col("JOB_ID")).where(col("TableID") == Table_ID).collect()[0].JOB_ID
    Notebook_ID = DF_Metadata.select(col("NB_ID")).where(col("TableID") == Table_ID).collect()[0].NB_ID
    Table_Name = (DF_Metadata.select(col("DWHTableName")).where(col("TableID") == Table_ID).collect()[0].DWHTableName)
    ErrorPath = (DF_Metadata.select(col("ErrorLogPath")).where(col("TableID") == Table_ID).collect()[0].ErrorLogPath)
    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == Table_ID).collect()[0].MergeKey) 
    Load_Type = (DF_Metadata.select(col("LoadType")).where(col("TableID") == Table_ID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateValue)
    Zone =(DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone)
    Extraction_Query = (DF_Metadata.select(col('SourceSelectQuery')).where(col('TableID') == Table_ID).collect()[0].SourceSelectQuery)
    
except Exception as e:
    logger.info("Unable to fetch metadata details", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Call Unity Catalog

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating the connection string format

# COMMAND ----------

##Collecting Secret values from the Azure keyVault and storing them in variables
try:
    #Update PipelineStartTime in Metadata
    UpdatePipelineStartTime(Table_ID, Zone)
    logger.info("Getting secret values and forming the connection string")
    
    username = dbutils.secrets.get(scope = "NFI_AR_SqlServer_Secrets", key = "FinARSQLServerUN")
    password = dbutils.secrets.get(scope = "NFI_AR_SqlServer_Secrets", key = "FinARSQLServerPass")
    hostname = dbutils.secrets.get(scope = "NFI_AR_SqlServer_Secrets", key = "FinARSQLServerHN")
    databasename = dbutils.secrets.get(scope = "NFI_AR_SqlServer_Secrets", key = "FinARSQLServerDBName")
    portnumber = dbutils.secrets.get(scope = "NFI_AR_SqlServer_Secrets", key = "FinSQLServerPortNumber")   
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    jdbcUrl = f"jdbc:sqlserver://{hostname}:{portnumber};database={databasename};encrypt=true;trustServerCertificate=true;"

except Exception as e:
    logger.error(f"Unable to connect to Database {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Extract and load into target table
# MAGIC

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(Table_ID, "Stage")
    if AutoSkipperCheck == 0:

        if Load_Type == 'truncate and load':
            print('Loading ' + Table_Name)
            DF_Source_Load = (spark.read
                                .format("jdbc")
                                .option("driver", driver)
                                .option("url", jdbcUrl)
                                .option("Query", Extraction_Query)
                                .option("user", username)
                                .option("password", password)
                                .load()
                             )
            DF_Source_Load = DF_Source_Load.withColumn("SourceSystem_ID", lit(8))
            DF_Source_Load = DF_Source_Load.withColumn("SourceSystem_Name", lit("Lawson System"))
            DF_Source_Load.createOrReplaceTempView('SourceToStage')
            Rowcount = DF_Source_Load.count()
            TruncateQuery = "Truncate Table bronze."+Table_Name
            spark.sql(TruncateQuery)
            MergeQuery = "INSERT INTO bronze."+Table_Name+" TABLE SourceToStage;"
            spark.sql(MergeQuery.format(Table_Name))
            UpdatePipelineStatusAndTime(Table_ID,'Bronze')
            logger.info('Successfully loaded the '+Table_Name+' to stage')
            # dbutils.fs.cp("file:" + p_logfile,ErrorPath+ p_filename)

            
            if Load_Type == "incremental load":
                DF_Metadata = DF_Metadata.withColumn('LastLoadDateValue',DF_Metadata.LastLoadDateValue.cast(StringType()))
                LoadDate =  DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
                ExtractionQuery = ExtractionQuery.format(MaxLoadDateColumn)
                ExtractionQuery = ExtractionQuery+"'"+LoadDate+"'"
                print('Loading ' + Table_Name)
                DF_Source_Load = (spark.read
                                    .format("jdbc")
                                    .option("url", jdbcUrl)
                                    .option("Query", Extraction_Query)
                                    .option("user", username)
                                    .option("password", password)
                                    .load()
                                    )
                DF_Source_Load = DF_Source_Load.withColumn('HashKey',md5(concat_ws("",*MergeKey.split(","))))
                DF_Source_Load.createOrReplaceTempView('SourceToBronze')
                Rowcount = DF_Source_Load.count()
                MergeQuery = 'Merge into bronze.{0} Target using SourceToBronze source on Target.{1} = Source.{2}  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
                spark.sql(MergeQuery.format(Table_Name,MergeKey,MergeKey))  
                UpdatePipelineStatusAndTime(Table_ID,'Bronze')
                logger.info('Successfully loaded the'+Table_Name+'to bronze')
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Merge-Target Table")
    
except Exception as e:
    logger.info("Failed for Bronze load")
    UpdateFailedStatus(Table_ID, "Bronze")
    Error_Statement = str(e).replace("'", "''")
    logger.info("Updated the Metadata for Failed Status " + Table_Name)
    print("Unable to load " + Table_Name)
    print(e)
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Merge-Target Table")
