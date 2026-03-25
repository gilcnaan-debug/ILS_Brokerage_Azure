# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from Source to Bronze as delta file
# MAGIC * **Created Date:** 01/29/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 01/29/2024
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
ErrorLogger = ErrorLogs("PostgreSQL_SourceToStage_Batch_15MinsRefresh_1_Pricing")
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
    
    username = dbutils.secrets.get(scope = "NFI_Pricing_PostgreSQL_Secrets", key = "PricingPostgresqlUN")
    password = dbutils.secrets.get(scope = "NFI_Pricing_PostgreSQL_Secrets", key = "PricingPostgresqlPass")
    hostname = dbutils.secrets.get(scope = "NFI_Pricing_PostgreSQL_Secrets", key = "PricingPostgresqlHN")
    databasename = dbutils.secrets.get(scope = "NFI_Pricing_PostgreSQL_Secrets", key = "PricingPostgresqlDBName")
    portnumber = dbutils.secrets.get(scope = "NFI_Pricing_PostgreSQL_Secrets", key = "PricingPostgresqlPortNumber")   
    driver = "org.postgresql.Driver"
    jdbcUrl = f"jdbc:postgresql://{hostname}:{portnumber}/{databasename}"

except Exception as e:
    logger.error(f"Unable to connect to Database {e}")

# COMMAND ----------

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid in ('P1','P2') and IsActive='1' ")

# COMMAND ----------

#Getting the TableID from the data frame and iterate over them in for loop
#Tableslist = [1,2,3]
TablesList = DF_Metadata.select(col('TableID')).collect()
for TableID in TablesList:
    TableID = TableID.TableID
    TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName
    ErrorPath = DF_Metadata.select(col('ErrorLogPath')).where(col('TableID') == TableID).collect()[0].ErrorLogPath

##If AutoSkipper is true, get the stage path,ExtractionQuery,Mergekey from the Metadata data frame and store it into variables
    
    try:
        AutoSkipperCheck = AutoSkipper(TableID,'Stage')
        if AutoSkipperCheck == 0:
            # StagePath = DF_Metadata.select(col('StagePath')).where(col('TableID') == TableID).collect()[0].StagePath
            ExtractionQuery = DF_Metadata.select(col('SourceSelectQuery')).where(col('TableID') == TableID).collect()[0].SourceSelectQuery
            MergeKey = DF_Metadata.select(col('MergeKey')).where(col('TableID') == TableID).collect()[0].MergeKey
            LoadType = DF_Metadata.select(col('LoadType')).where(col('TableID') == TableID).collect()[0].LoadType
            MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn
            MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue

##Based on the load type the loads will happen
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
    ##Generate Haskey value for the respective table based on the Unique column which is specified in the Metadata table 

                # DF_Source_Load = DF_Source_Load.withColumn('HashKey',md5(concat_ws("",*MergeKey.split(","))))
                DF_Source_Load.createOrReplaceTempView('SourceToStage')
                Rowcount = DF_Source_Load.count()
                TruncateQuery = "Truncate Table bronze."+TableName
                spark.sql(TruncateQuery)
                MergeQuery = "INSERT INTO bronze."+TableName+" TABLE SourceToStage;"
                spark.sql(MergeQuery.format(TableName))
                UpdatePipelineStatusAndTime(TableID,'Bronze')
                logger.info('Successfully loaded the'+TableName+'to stage')
                # dbutils.fs.cp("file:" + p_logfile,ErrorPath+ p_filename)
            

            if LoadType == "incremental load":
                DF_Metadata = DF_Metadata.withColumn('LastLoadDateValue',DF_Metadata.LastLoadDateValue.cast(StringType()))
                LoadDate =  DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
                ExtractionQuery = ExtractionQuery.format(MaxLoadDateColumn)
                ExtractionQuery = ExtractionQuery+"'"+LoadDate+"'"
                #ExtractionQuery = ExtractionQuery+" "+MaxLoadDateColumn+" > '"+LoadDate+"'"
                print('Loading ' + TableName)
                DF_Source_Load = (spark.read
                                    .format("jdbc")
                                    .option("url", jdbcUrl)
                                    .option("Query", ExtractionQuery)
                                    .option("user", username)
                                    .option("password", password)
                                    .load()
                                    )
                DF_Source_Load = DF_Source_Load.withColumn('HashKey',md5(concat_ws("",*MergeKey.split(","))))
                DF_Source_Load.createOrReplaceTempView('SourceToBronze')
                Rowcount = DF_Source_Load.count()
                MergeQuery = 'Merge into bronze.{0} Target using SourceToBronze source on Target.HashKey = Source.HashKey  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
                spark.sql(MergeQuery.format(TableName,MergeKey,MergeKey))  
                logger.info('Successfully loaded the'+TableName+'to bronze')
#                 # dbutils.fs.cp("file:" + p_logfile,ErrorPath+ p_filename)

# ##Insert the rowcount and TableStatus value of the individual table into the LogTable
#             Query = "Insert into Metadata.LogTable values ('"+TableName+"',{0},'Stage',current_timestamp())"
#             LogQuery = Query.format(Rowcount)
#             spark.sql(LogQuery)
#             print('Loaded ' + TableName)  
                MaxDateQuery = "Select max({0}) as Max_Date from SourceToBronze"
                MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
                DF_MaxDate = spark.sql(MaxDateQuery)
                UpdateLastLoadDate(TableID,DF_MaxDate)
                UpdatePipelineStatusAndTime(TableID,'Bronze')
#             logger.info('Updated the Metadata for'+TableName)
#             # dbutils.fs.cp("file:" + p_logfile,ErrorPath+ p_filename)

# ##If the DMS stage table load got failed, log the error in the error path  
       
    except Exception as e:
        logger.info('Failed for Bronze load')
        UpdateFailedStatus(TableID,'Bronze')
        logger.info('Updated the Metadata for Failed Status '+TableName)
        print('Unable to load '+TableName)
        print(e)
        # dbutils.fs.cp("file:" + p_logfile,ErrorPath+ p_filename)  
        continue 

# COMMAND ----------

