# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract tables from Source to Bronze as delta file and to perform DQC
# MAGIC * **Created Date:** 29/01/2025
# MAGIC * **Created By:** Parthasarathy
# MAGIC * **Modified Date:** 29/01/2025
# MAGIC * **Modified By:** Parthasarathy
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

# MAGIC %run "/Workspace/Shared/Common Notebooks DQC/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initializing Logger

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("PostgreSQL_SourceToStage_Batch_15MinsRefresh_31")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

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

#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid in ('R29','R30','R31','R33','R35','R37','R38','R39','R42') and IsActive='1' ")

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
            Sourcesystem = DF_Metadata.select(col('SourceSystem')).where(col('TableID') == TableID).collect()[0].SourceSystem
            Zone = DF_Metadata.select(col('Zone')).where(col('TableID') == TableID).collect()[0].Zone

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
                try:
                    dataframe  = DF_Source_Load
                    sourcesystem = Sourcesystem
                    schemaname = 'source'
                    table = TableName
                    PKs = MergeKey

                    previous_metrics_df = spark.sql(f"select * from metadata.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemaname}' and table_name = '{table}' and Validation_Date = (select max(Validation_Date) from metadata.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemaname}' and table_name = '{table}') ")
                    
                    targetdataframe = get_data_quality_metrics(dataframe, sourcesystem, schemaname, table, previous_metrics_df, None, threshold_percentage=10)        
                    targetdataframe.createOrReplaceTempView("LogTable")
                    spark.sql("INSERT INTO metadata.DQlogs select * from LogTable")
                    print("Inserting into LogTable")
                except Exception as e:
                    logger.error(f"Unable to log the record count: {str(e)}")
                    logger.error(f"Error type: {type(e)}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                                # DF_Source_Load = DF_Source_Load.withColumn('HashKey',md5(concat_ws("",*MergeKey.split(","))))
                try:
                    DF_Source_Load.createOrReplaceTempView('SourceToStage')
                    Rowcount = DF_Source_Load.count()
                    TruncateQuery = "Truncate Table bronze."+TableName
                    spark.sql(TruncateQuery)
                    MergeQuery = "INSERT INTO bronze."+TableName+" TABLE SourceToStage;"
                    spark.sql(MergeQuery.format(TableName))
                    UpdatePipelineStatusAndTime(TableID,'Bronze')
                    logger.info('Successfully loaded the'+TableName+'to stage')
                    # dbutils.fs.cp("file:" + p_logfile,ErrorPath+ p_filename)

                # ##If the DMS stage table load got failed, log the error in the error path  
                except Exception as e:
                    logger.info('Failed for Bronze load')
                    UpdateFailedStatus(TableID,'Bronze')
                    logger.info('Updated the Metadata for Failed Status '+TableName)
                    print('Unable to load '+TableName)
                    print(e)

            if LoadType == "incremental load":
                
                DF_Metadata = DF_Metadata.withColumn('LastLoadDateValue',DF_Metadata.LastLoadDateValue.cast(StringType()))
                LoadDate =  DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
                MaxLoadDate_Converted = datetime.strptime(LoadDate, "%Y-%m-%d %H:%M:%S.%f")# Convert to datetime object
                MaxLoadDate_Converted = MaxLoadDate_Converted - timedelta(hours=1) # Subtract 1 hour   
                MaxLoadDate_Offset = MaxLoadDate_Converted.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]# Convert back to string
                ExtractionQuery = ExtractionQuery.format(MaxLoadDateColumn)
                # DQ_Max =  DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == TableID).collect()[0].LastLoadDateValue
                Max_Update = spark.sql(f" update metadata.dqmetadata set LastLoadDate = '{MaxLoadDate_Offset}' where Table_ID = '{TableID}'")
                ExtractionQueryDel = f"SELECT {MergeKey} FROM public.{TableName}"
                ExtractionQuery = ExtractionQuery+"'"+MaxLoadDate_Offset+"'"
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
                DF_Source_Load_Del = (spark.read
                                .format("jdbc")
                                .option("driver", driver)
                                .option("url", jdbcUrl)
                                .option("Query", ExtractionQueryDel)
                                .option("user", username)
                                .option("password", password)
                                .load()
                            )
                try:
                    dataframe  = DF_Source_Load
                    sourcesystem = Sourcesystem
                    schemaname = 'source'
                    table = TableName
                    PKs = MergeKey

                    previous_metrics_df = spark.sql(f"select * from metadata.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemaname}' and table_name = '{table}' and Validation_Date = (select max(Validation_Date) from metadata.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemaname}' and table_name = '{table}') ")
                    
                    targetdataframe = get_data_quality_metrics(dataframe, sourcesystem, schemaname, table, previous_metrics_df, None, threshold_percentage=10)        
                    targetdataframe.createOrReplaceTempView("LogTable")
                    spark.sql("INSERT INTO metadata.DQlogs select * from LogTable")
                    print("Inserting into LogTable")
                except Exception as e:
                    logger.error(f"Unable to log the record count: {str(e)}")
                    logger.error(f"Error type: {type(e)}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                                # DF_Source_Load = DF_Source_Load.withColumn('HashKey',md5(concat_ws("",*MergeKey.split(","))))
                try:
                    DF_Source_Load = DF_Source_Load.withColumn('HashKey',md5(concat_ws("",*MergeKey.split(","))))
                    DF_Source_Load = DF_Source_Load.withColumn("Is_Deleted", lit("0"))
                    DF_Source_Load.createOrReplaceTempView('SourceToBronze')
                    DF_Source_Load_Del.createOrReplaceTempView('SourceToDel')
                    Rowcount = DF_Source_Load.count()
                    MergeQuery = 'Merge into bronze.{0} Target using SourceToBronze source on Target.{1} = Source.{2}  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
                    spark.sql(MergeQuery.format(TableName,MergeKey,MergeKey))
                    spark.sql(f"""
                                    UPDATE bronze.{TableName}
                                    SET Is_Deleted = 1
                                    WHERE {MergeKey} IN (
                                        SELECT sc.{MergeKey}
                                        FROM bronze.{TableName} sc
                                        LEFT JOIN SourceToDel vc ON sc.{MergeKey} = vc.{MergeKey}
                                        WHERE vc.{MergeKey} IS NULL
                                    )""") 
                    logger.info('Successfully loaded the'+TableName+'to bronze')


                    MaxDateQuery = "Select max({0}) as Max_Date from  Bronze.{1}"
                    MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn, TableName)
                    DF_MaxDate = spark.sql(MaxDateQuery)
                    UpdateLastLoadDate(TableID,DF_MaxDate)
                    UpdatePipelineStatusAndTime(TableID,'Bronze')
 
                except Exception as e:
                    logger.info('Failed for Bronze load')
                    UpdateFailedStatus(TableID,'Bronze')
                    logger.info('Updated the Metadata for Failed Status '+TableName)
                    print('Unable to load '+TableName)
                    print(e)
                
                
    except Exception as e:
        logger.info('Failed for Bronze load')
        UpdateFailedStatus(TableID,'Bronze')
        logger.info('Updated the Metadata for Failed Status '+TableName)
        print('Unable to load '+TableName)
        print(e)
        continue 

# COMMAND ----------

