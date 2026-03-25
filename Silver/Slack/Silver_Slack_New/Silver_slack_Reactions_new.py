# Databricks notebook source
# MAGIC %md
# MAGIC ## BronzeToSilver
# MAGIC * **Description:** To extract tables from Bronze to Silver as delta file
# MAGIC * **Created Date:** 18/07/2024
# MAGIC * **Created By:** Hariharan
# MAGIC * **Modified Date:** 24/07/2024
# MAGIC * **Modified By:** Hariharan
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #Importing Packages

# COMMAND ----------

from pyspark.sql.functions import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, explode
import pytz

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

try:
    ErrorLogger = ErrorLogs("Silver_slack_Conversaions_Reaction_log")
    logger = ErrorLogger[0]
    p_logfile = ErrorLogger[1]
    p_filename = ErrorLogger[2]
except Exception as e:
    logger.info("Unable to create variable",e)
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #Use Catalog

# COMMAND ----------

CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Fetch Metadata details

# COMMAND ----------

try:
  TableID = 'SZ23-N'
  DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
except Exception as e:
  logger.info("Unable to fetch metadata details",e)
  print(e)

# COMMAND ----------

try:
    TablesList = DF_Metadata.select(col("TableID")).collect()
    TableName = (
        DF_Metadata.select(col("SourceTableName"))
        .where(col("TableID") == TableID)
        .collect()[0]
        .SourceTableName
    )

    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == TableID).collect()[0].MergeKey) 
    LoadType = (DF_Metadata.select(col("LoadType")).where(col("TableID") == TableID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('UnixTime')).where(col('TableID') == TableID).collect()[0].UnixTime)
except Exception as e:
    logger.info("Unable to fetch details from metadata")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #vaiables to log error in error table

# COMMAND ----------

try:
    Job_ID = "SJOB_8"
    Notebook_ID = "SNB_8"
    Zone = "Silver"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC #Required Transformations

# COMMAND ----------

try:
    DF_Reaction =  spark.sql(
                    '''select
                       ch.DW_Reactions_ID AS DW_Reactions_ID,
                        ch.Channel_ID AS Channel_ID,
                        ch.team AS Team_ID,
                        ch.user AS User_ID,
                        ch.name AS Reaction_Name,
                        ch.date_create AS Reaction_Date,
                        ch.ts AS Message_Timestamp,
                        ch.Sourcesystem_Name as Sourcesystem_Name,
                        ch.Conversation_Type AS Conversation_Type,
                        ch.HashKey as HashKey,
                        ch.MergeKey AS MergeKey,
                        ch.DW_Timestamp AS DW_Timestamp,
                        ch.Created_By as Created_By,
                        ch.Created_Date as Created_Date,
                        ch.Last_Modified_By as Last_Modified_By,
                        ch.Last_Modified_Date as Last_Modified_Date
                        from bronze.slack_Reactions_New ch'''
                )
except Exception as e:
    logger.info("unable to fetch data from bronze")
    print(e)

# COMMAND ----------

try:
    DF_Reaction = DF_Reaction.withColumn("Workspace_ID", lit("None"))
    DF_Reaction = DF_Reaction.withColumn("Workspace_Name", lit("None"))

except Exception as e:
    print("unable to add new columns",e)
DF_Reaction.createOrReplaceTempView('VW_Reactions')

# COMMAND ----------

# MAGIC %md
# MAGIC #Merge data into Target Table

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            
            # Add HashKey column
            
            # Count the number of rows
            Rowcount = DF_Reaction.count()
            
            # Define the merge query
            MergeQuery = '''
                MERGE INTO Silver.Silver_Slack_Reactions_New AS Target
                USING VW_Reactions AS Source ON Target.DW_Reactions_ID=Source.DW_Reactions_ID  When Matched and  Target.Hashkey!= Source.Hashkey THEN
                UPDATE SET
                    Target.DW_Reactions_ID = Source.DW_Reactions_ID,
                        Target.Channel_ID = Source.Channel_ID,
                        Target.Team_ID = Source.Team_ID,
                        Target.User_ID = Source.User_ID,
                        Target.Reaction_Name = Source.Reaction_Name,
                        Target.Reaction_Date = Source.Reaction_Date,
                        Target.Message_Timestamp = Source.Message_Timestamp,
                        Target.Sourcesystem_Name = Source.Sourcesystem_Name,
                        Target.Conversation_Type = source.Conversation_Type,
                        Target.HashKey = Source.HashKey,
                        Target.MergeKey = Source.MergeKey,
                        Target.DW_Timestamp = Source.DW_Timestamp,
                        Target.Workspace_ID = Source.Workspace_ID,
                        Target.Workspace_Name = Source.Workspace_Name,
                        Target.Created_By = Source.Created_By,
                        Target.Created_Date = Source.Created_Date,
                        Target.Last_Modified_By = Source.Last_Modified_By,
                        Target.Last_Modified_Date = Source.Last_Modified_Date
                WHEN NOT MATCHED 
                    THEN INSERT (
                        DW_Reactions_ID,
                        Channel_ID,
                        Team_ID,
                        User_ID,
                        Reaction_Name,
                        Reaction_Date,
                        Message_Timestamp,
                        Sourcesystem_Name,
                        Conversation_Type,
                        HashKey,
                        MergeKey,
                        DW_Timestamp,
                        Workspace_ID,
                        Workspace_Name,
                        Created_By,
                        Created_Date,
                        Last_Modified_By,
                        Last_Modified_Date
                    )
                    VALUES (
                        Source.DW_Reactions_ID,
                        Source.Channel_ID,
                        Source.Team_ID,
                        Source.User_ID,
                        Source.Reaction_Name,
                        Source.Reaction_Date,
                        Source.Message_Timestamp,
                        Source.Sourcesystem_Name,
                        source.Conversation_Type,
                        Source.HashKey,
                        Source.MergeKey,
                        Source.DW_Timestamp,
                        Source.Workspace_ID,
                        Source.Workspace_Name,
                        Source.Created_By,
                        Source.Created_Date,
                        Source.Last_Modified_By,
                        Source.Last_Modified_Date
                    )'''
            spark.sql(MergeQuery)
            DF_Workspace=spark.sql('''
                UPDATE Silver.Silver_Slack_Reactions_New wc
                SET 
                    wc.Workspace_ID = CASE
                        WHEN wc.Team_ID = 'T1D2RFN9H' THEN 'W1'
                        WHEN wc.Team_ID = 'T046156LPKM' THEN 'W2'
                        WHEN wc.Team_ID = 'T02RUKYS21Y' THEN 'W3'
                        WHEN wc.Team_ID = 'T05G1BTTU3T' THEN 'W4'
                        WHEN wc.Team_ID = 'T03EF2ZDFBJ' THEN 'W5'
                        WHEN wc.Team_ID = 'T036CCPJ7DH' THEN 'W6'
                        WHEN wc.Team_ID = 'T060H7TQXKK' THEN 'W7'
                        WHEN wc.Team_ID = 'T044ZHKAVTN' THEN 'W8'
                        WHEN wc.Team_ID = 'T030MHW5A86' THEN 'W9'
                        WHEN wc.Team_ID = 'T06ET71TBT8' THEN 'W10'
                        WHEN wc.Team_ID = 'T039KT5S2MQ' THEN 'W11'
                        WHEN wc.Team_ID = 'E02F5ESJP0F' THEN 'W12'
                        WHEN Wc.Team_ID is NULL THEN "No Workspace_ID"
                        ELSE 'W13'
                    END,
                               wc.Workspace_Name = CASE
                        WHEN wc.Team_ID = 'T1D2RFN9H' THEN 'Brokerage'
                        WHEN wc.Team_ID = 'T046156LPKM' THEN 'Distribution'
                        WHEN wc.Team_ID = 'T02RUKYS21Y' THEN 'Integrated Logistics'
                        WHEN wc.Team_ID = 'T05G1BTTU3T' THEN 'Dryage'
                        WHEN wc.Team_ID = 'T03EF2ZDFBJ' THEN 'Global'
                        WHEN wc.Team_ID = 'T036CCPJ7DH' THEN 'Intermodal'
                        WHEN wc.Team_ID = 'T060H7TQXKK' THEN 'IT'
                        WHEN wc.Team_ID = 'T044ZHKAVTN' THEN 'NFI ILS Sandbox'
                        WHEN wc.Team_ID = 'T030MHW5A86' THEN 'Relay'
                        WHEN wc.Team_ID = 'T06ET71TBT8' THEN 'Slack Integration Team'
                        WHEN wc.Team_ID = 'T039KT5S2MQ' THEN 'TM'
                        WHEN wc.Team_ID = 'E02F5ESJP0F' THEN 'NFI Integrated Logistics'
                        WHEN Wc.Team_ID is NULL THEN "Slack"
                        ELSE 'Slack'
                    END
            ''')          
           

            logger.info('Successfully loaded the ' + TableName + ' to bronze')
            print('Loaded ' + TableName)

            # Find the maximum date
            MaxDateQuery = "SELECT MAX({0}) AS Max_Date FROM VW_Reactions".format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
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
    logger.error(f"Table is already loaded: {str(e)}")
    print(e)


# COMMAND ----------

