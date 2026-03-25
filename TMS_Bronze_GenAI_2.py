# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from Source to Bronze as delta file
# MAGIC * **Created Date:** 11/18/2023
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 11/18/2023
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

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
# MAGIC ####Initializing Utilities

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initializing Logger

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("PostgreSQL_SourceToStage_Batch_15MinsRefresh1")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating the connection string format

# COMMAND ----------

##Collecting Secret values from the Azure keyVault and storing them in variables
try:
    logger.info("Getting secret values and forming the connection string")
    
    username = dbutils.secrets.get(scope = "NFI_PostgreSQL_Secrets", key = "PostgresqlUN")
    password = dbutils.secrets.get(scope = "NFI_PostgreSQL_Secrets", key = "PostgresqlPass")
    hostname = dbutils.secrets.get(scope = "NFI_PostgreSQL_Secrets", key = "PostgresqlHN")
    databasename = dbutils.secrets.get(scope = "NFI_PostgreSQL_Secrets", key = "PostgresqlDBName")
    portnumber = dbutils.secrets.get(scope = "NFI_PostgreSQL_Secrets", key = "PostgresqlPortNumber")   
    driver = "org.postgresql.Driver"
    jdbcUrl = f"jdbc:postgresql://{hostname}:{portnumber}/{databasename}"

except Exception as e:
    logger.error(f"Unable to connect to Database {e}")

# COMMAND ----------

# Getting the list of DMS tables in the dataframe that needs to be loaded into stage
DF_Metadata = spark.sql("""
SELECT * 
FROM Metadata.MasterMetadata 
WHERE Tableid in (
    'A10',
    'R32',
    'A4',
    'A7',
    'L13',
    'L2',
    'L4',
    'L5',
    'L6',
    'L7',
    'L8',
    'R15'
) 
AND IsActive='1'
""")

Job_ID = "JOB_TMS2"
Notebook_ID = "NB32"

# COMMAND ----------

#Getting the TableID from the data frame and iterate over them in for loop
TablesList = DF_Metadata.select(col('TableID')).collect()
for TableID in TablesList:
    TableID = TableID.TableID
    TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName
    ErrorPath = DF_Metadata.select(col('ErrorLogPath')).where(col('TableID') == TableID).collect()[0].ErrorLogPath

    try:
        AutoSkipperCheck = AutoSkipper(TableID,'Stage')

        if AutoSkipperCheck == 0:

            ExtractionQuery = DF_Metadata.select(col('SourceSelectQuery')).where(col('TableID') == TableID).collect()[0].SourceSelectQuery
            MergeKey = DF_Metadata.select(col('MergeKey')).where(col('TableID') == TableID).collect()[0].MergeKey
            LoadType = DF_Metadata.select(col('LoadType')).where(col('TableID') == TableID).collect()[0].LoadType
            MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
            Zone = DF_Metadata.select(col('Zone')).where(col('TableID') == TableID).collect()[0].Zone

            if LoadType == 'truncate and load':
                print('Loading ' + TableName)
                DF_Source_Load = (spark.read
                                    .format("jdbc")
                                    .option("driver", driver)
                                    .option("url", jdbcUrl)
                                    .option("Query", ExtractionQuery)
                                    .option("user", username)
                                    .option("password", password)
                                    .load()
                                )

                DF_Source_Load.createOrReplaceTempView('SourceToStage')
                Rowcount = DF_Source_Load.count()
                TruncateQuery = "Truncate Table bronze."+TableName
                spark.sql(TruncateQuery)
                MergeQuery = "INSERT INTO bronze."+TableName+" TABLE SourceToStage;"
                spark.sql(MergeQuery.format(TableName))
                UpdatePipelineStatusAndTime(TableID,'Bronze')
                logger.info('Successfully loaded the '+TableName+' to stage')
                UpdateLogStatus(Job_ID,TableID,Notebook_ID,TableName,Zone,"Succeeded","NULL","NULL",0,"Merge-Target Table")

            if LoadType == "incremental load":

                DF_Metadata = DF_Metadata.withColumn('LastLoadDateValue',DF_Metadata.LastLoadDateValue.cast(StringType()))
                MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue   
                MaxLoadDate_Converted = datetime.strptime(MaxLoadDate, "%Y-%m-%d %H:%M:%S.%f")# Convert to datetime object
                MaxLoadDate_Converted = MaxLoadDate_Converted - timedelta(hours=1) # Subtract 1 hour   
                MaxLoadDate_Offset = MaxLoadDate_Converted.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]# Convert back to string

                ExtractionQuery = ExtractionQuery.format(MaxLoadDateColumn)
                ExtractionQuery = ExtractionQuery+"'"+MaxLoadDate_Offset+"'"
  
                print('Loading ' + TableName)
                DF_Source_Load = (spark.read
                                    .format("jdbc")
                                    .option("url", jdbcUrl)
                                    .option("Query", ExtractionQuery)
                                    .option("user", username)
                                    .option("password", password)
                                    .load()
                                    )

                DF_Source_Load = DF_Source_Load.withColumn("Is_Deleted", lit("0"))
                DF_Source_Load.createOrReplaceTempView('SourceToBronze')
                Rowcount = DF_Source_Load.count()
                MergeQuery = 'Merge into bronze.{0} Target using SourceToBronze source on Target.{1} = Source.{2}  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
                spark.sql(MergeQuery.format(TableName,MergeKey,MergeKey))  
                logger.info('Successfully loaded the '+TableName+' to bronze')

                MaxDateQuery = "Select max({0}) as Max_Date from Bronze.{1}"
                MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn, TableName)
                DF_MaxDate = spark.sql(MaxDateQuery)
                UpdateLastLoadDate(TableID,DF_MaxDate)
                UpdatePipelineStatusAndTime(TableID,'Bronze')
                UpdateLogStatus(Job_ID,TableID,Notebook_ID,TableName,Zone,"Succeeded","NULL","NULL",0,"Merge-Target Table")

       
    except Exception as e:
        logger.info('Failed for Bronze load')
        UpdateFailedStatus(TableID,'Bronze')
        logger.info('Updated the Metadata for Failed Status '+TableName)
        Error_Statement = str(e).replace("'", "''")
        UpdateLogStatus(Job_ID,TableID,Notebook_ID,TableName,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Merge-Target Table")
        print('Unable to load '+TableName)
        print(e)
        continue 