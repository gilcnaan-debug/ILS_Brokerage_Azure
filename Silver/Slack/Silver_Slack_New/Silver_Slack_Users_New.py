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

ErrorLogger = ErrorLogs("Silver_slack_users")
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

try:
    TableID = 'SZ21-N'
    DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = TableID and IsActive='1' ")
    TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName
except Exception as e:
    logger.info("unable to fetch details",e)
    print(e)

# COMMAND ----------

try:
    TableName = (DF_Metadata.select(col("SourceTableName")).where(col("TableID") == TableID).collect()[0].SourceTableName)
    MergeKey = (DF_Metadata.select(col('MergeKey')).where(col("TableID") == TableID).collect()[0].MergeKey) 
    LoadType = (DF_Metadata.select(col("LoadType")).where(col("TableID") == TableID).collect()[0].LoadType)
    MaxLoadDateColumn = (DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == TableID).collect()[0].LastLoadDateColumn) 
    MaxLoadDate =(DF_Metadata.select(col('UnixTime')).where(col('TableID') == TableID).collect()[0].UnixTime)
except Exception as e:
    logger.info("Unable to fetch metadata details")
    print(e)

# COMMAND ----------

try:
    Job_ID = "SJOB_6"
    Notebook_ID = "SNB_6"
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
    DF_Users =  spark.sql(
                    '''select
                        u.DW_User_ID ,
                        u.id AS User_ID,
						u.teams AS Teams_ID,
						u.name AS User_Name,
						u.deleted AS Is_Deleted,
						u.color AS Color,
						u.real_name AS User_Real_Name,
						u.tz AS User_TimeZone,
						u.tz_label AS TimeZone_Label,
						u.tz_offset AS TimeZone_Offset,
						u.profile AS Profile,
                        regexp_extract(u.profile, 'email=([^,]+)') as Email,
                        regexp_extract(u.profile, 'title=([^,]+)') as Title,
                        regexp_extract(u.profile, 'phone=([^,]+)') as Phone,
                        regexp_extract(u.profile, 'skype=([^,]+)') as Skype,
                        regexp_extract(u.profile, 'real_name=([^,]+)') as Real_Name,
                        regexp_extract(u.profile, 'real_name_normalized=([^,]+)') as Real_Name_Normalized,
                        regexp_extract(u.profile, 'display_name=([^,]+)') as Display_Name,
                        regexp_extract(u.profile, 'diaplay_name_normalized=([^,]+)') as Display_Name_Normalized,
                        regexp_extract(u.profile, 'fields=([^,]+)') as Fields,
                        regexp_extract(u.profile, 'status_text=([^,]+)') as Status_Text,
                        regexp_extract(u.profile, 'status_emoji=([^,]+)') as Status_Emoji,
                        regexp_extract(u.profile, 'status_emoji_display_info=([^,]+)') as Status_Emoji_Display_Info,
                        regexp_extract(u.profile, 'status_expiration=([^,]+)') as Status_Expiration,
                        regexp_extract(u.profile, 'avatar_hash=([^,]+)') as Avatar_Hash,
                        regexp_extract(u.profile, 'api_app_id=([^,]+)') as Api_App_ID,
                        regexp_extract(u.profile, 'always_active=([^,]+)') as Always_Active,
                        regexp_extract(u.profile, 'image_original=([^,]+)') as Image_Original,
                        regexp_extract(u.profile, 'is_custom_image=([^,]+)') as Is_Custom_Image,
                        regexp_extract(u.profile, 'bot_id=([^,]+)') as Bot_ID,
                        regexp_extract(u.profile, 'first_name=([^,]+)') as First_Name,
                        regexp_extract(u.profile, 'last_name=([^,]+)') as Last_Name,
                        regexp_extract(u.profile, 'image_24=([^,]+)') as Image_24,
                        regexp_extract(u.profile, 'image_32=([^,]+)') as Image_32,
                        regexp_extract(u.profile, 'image_48=([^,]+)') as Image_48,
                        regexp_extract(u.profile, 'image_72=([^,]+)') as Image_72,
                        regexp_extract(u.profile, 'image_192=([^,]+)') as Image_192,
                        regexp_extract(u.profile, 'image_512=([^,]+)') as Image_512,
                        regexp_extract(u.profile, 'image_1024=([^,]+)') as Image_1024,
                        regexp_extract(u.profile, 'status_text_canonical=([^,]+)') as Status_Text_Canonical,
                        regexp_extract(u.profile, 'team=([^,]+)') as Team_ID,
						u.is_admin AS Is_Admin,
						u.is_owner AS Is_Owner,
						u.is_primary_owner AS Is_Primary_Owner,
						u.is_restricted AS Is_Restricted,
						u.is_ultra_restricted AS Is_Ultra_Restricted,
						u.is_bot AS Is_Bot,
						u.is_app_user AS Is_App_User,
						u.updated AS Updated_Date,
						u.is_email_confirmed AS Is_Email_Confirmed,
						u.who_can_share_contact_card AS Who_Can_Share_Contact_Card,
                        u.HashKey AS HashKey,
                        u.DW_Timestamp AS DW_Timestamp,
                        u.Sourcesystem_Name AS Sourcesystem_Name,
                        u.Created_By AS Created_By,
                        u.Created_Date AS Created_Date,
                        u.Last_Modified_By AS Last_Modified_By,
                        u.Last_Modified_Date AS Last_Modified_Date
                        from bronze.Slack_Users_New u
                         '''
                )
except Exception as e:
    logger.info("unable to fetch data from bronze")
    print(e)

# COMMAND ----------

try:
    DF_Users = DF_Users.withColumn("Workspace_ID", lit("None"))
    DF_Users = DF_Users.withColumn("Workspace_Name", lit("None"))
except Exception as e:
    print("unable to add new columns",e)
DF_Users.createOrReplaceTempView('VW_Users')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge To Target Table

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(TableID, 'Stage')
    if AutoSkipperCheck == 0:
        try:
            print('Loading ' + TableName)
            
            # Add HashKey column
            
            # Count the number of rows
            Rowcount = DF_Users.count()
            
            # Define the merge query
            MergeQuery = '''
                MERGE INTO silver.Silver_Slack_Users_New AS Target
                USING VW_Users AS Source ON Target.DW_User_ID=Source.DW_User_ID  
                WHEN Matched and Target.Hashkey!= Source.Hashkey THEN
                UPDATE SET
                    Target.DW_User_ID = Source.DW_User_ID,
                    Target.User_ID = Source.User_ID,
                    Target.Teams_ID = Source.Teams_ID,
                    Target.User_Name = Source.User_Name,
                    Target.Is_Deleted = Source.Is_Deleted,
                    Target.Color = Source.Color,
                    Target.User_Real_Name = Source.User_Real_Name,
                    Target.User_TimeZone = Source.User_TimeZone,
                    Target.TimeZone_Label = Source.TimeZone_Label,
                    Target.TimeZone_Offset = Source.TimeZone_Offset,
                    Target.Profile = Source.Profile,
                    Target.Email = Source.Email,
                    Target.Title = Source.Title,
                    Target.Phone = Source.Phone,
                    Target.Skype = Source.Skype,
                    Target.Real_Name = Source.Real_Name,
                    Target.Real_Name_Normalized = Source.Real_Name_Normalized,
                    Target.Display_Name = Source.Display_Name,
                    Target.Display_Name_Normalized = Source.Display_Name_Normalized,
                    Target.Fields = Source.Fields,
                    Target.Status_Text = Source.Status_Text,
                    Target.Status_Emoji = Source.Status_Emoji,
                    Target.Status_Emoji_Display_Info = Source.Status_Emoji_Display_Info,
                    Target.Status_Expiration = Source.Status_Expiration,
                    Target.Avatar_Hash = Source.Avatar_Hash,
                    Target.Api_App_ID = Source.Api_App_ID,
                    Target.Always_Active = Source.Always_Active,
                    Target.Image_Original = Source.Image_Original,
                    Target.Is_Custom_Image = Source.Is_Custom_Image,
                    Target.Bot_ID = Source.Bot_ID,
                    Target.First_Name = Source.First_Name,
                    Target.Last_Name = Source.Last_Name,
                    Target.Image_24 = Source.Image_24,
                    Target.Image_32 = Source.Image_32,
                    Target.Image_48 = Source.Image_48,
                    Target.Image_72 = Source.Image_72,
                    Target.Image_192 = Source.Image_192,
                    Target.Image_512 = Source.Image_512,
                    Target.Image_1024 = Source.Image_1024,
                    Target.Status_Text_Canonical = Source.Status_Text_Canonical,
                    Target.Team_ID = Source.Team_ID,
                    Target.Is_Admin = Source.Is_Admin,
                    Target.Is_Owner = Source.Is_Owner,
                    Target.Is_Primary_Owner = Source.Is_Primary_Owner,
                    Target.Is_Restricted = Source.Is_Restricted,
                    Target.Is_Ultra_Restricted = Source.Is_Ultra_Restricted,
                    Target.Is_Bot = Source.Is_Bot,
                    Target.Is_App_User = Source.Is_App_User,
                    Target.Updated_Date = Source.Updated_Date,
                    Target.Is_Email_Confirmed = Source.Is_Email_Confirmed,
                    Target.Who_Can_Share_Contact_Card = Source.Who_Can_Share_Contact_Card,
                    Target.HashKey = Source.HashKey,
                    Target.DW_Timestamp = Source.DW_Timestamp,
                    Target.Sourcesystem_Name = Source.Sourcesystem_Name,
                    Target.Workspace_ID = Source.Workspace_ID,
                    Target.Workspace_Name = Source.Workspace_Name,
                    Target.Created_By = Source.Created_By,
                    Target.Created_Date = Source.Created_Date,
                    Target.Last_Modified_By = Source.Last_Modified_By,
                    Target.Last_Modified_Date = Source.Last_Modified_Date
                WHEN NOT MATCHED 
                    THEN INSERT (
                        DW_User_ID, User_ID, Teams_ID, User_Name, Is_Deleted, Color, User_Real_Name, User_TimeZone, 
                        TimeZone_Label, TimeZone_Offset, Profile, Email, Title, Phone, Skype, Real_Name, 
                        Real_Name_Normalized, Display_Name, Display_Name_Normalized, Fields, Status_Text, 
                        Status_Emoji, Status_Emoji_Display_Info, Status_Expiration, Avatar_Hash, Api_App_ID, 
                        Always_Active, 
                        Image_Original, Is_Custom_Image, Bot_ID, First_Name, Last_Name, Image_24, Image_32, 
                        Image_48, Image_72, Image_192, Image_512, Image_1024, Status_Text_Canonical, Team_ID, 
                        Is_Admin, Is_Owner, Is_Primary_Owner, Is_Restricted, Is_Ultra_Restricted, Is_Bot, 
                        Is_App_User, Updated_Date, Is_Email_Confirmed, Who_Can_Share_Contact_Card, HashKey, 
                        DW_Timestamp, Sourcesystem_Name, Workspace_ID, Workspace_Name, Created_By, Created_Date, 
                        Last_Modified_By, Last_Modified_Date
                    )
                    VALUES (
                        Source.DW_User_ID, Source.User_ID, Source.Teams_ID, Source.User_Name, Source.Is_Deleted, 
                        Source.Color, Source.User_Real_Name, Source.User_TimeZone, Source.TimeZone_Label, 
                        Source.TimeZone_Offset, Source.Profile, Source.Email, Source.Title, Source.Phone, 
                        Source.Skype, Source.Real_Name, Source.Real_Name_Normalized, Source.Display_Name, 
                        Source.Display_Name_Normalized, Source.Fields, Source.Status_Text, Source.Status_Emoji, 
                        Source.Status_Emoji_Display_Info, Source.Status_Expiration, Source.Avatar_Hash, 
                        Source.Api_App_ID,Source.Always_Active, Source.Image_Original, Source.Is_Custom_Image, Source.Bot_ID, 
                        Source.First_Name, Source.Last_Name, Source.Image_24, Source.Image_32, Source.Image_48, 
                        Source.Image_72, Source.Image_192, Source.Image_512, Source.Image_1024, 
                        Source.Status_Text_Canonical, Source.Team_ID, Source.Is_Admin, Source.Is_Owner, 
                        Source.Is_Primary_Owner, Source.Is_Restricted, Source.Is_Ultra_Restricted, Source.Is_Bot, 
                        Source.Is_App_User, Source.Updated_Date, Source.Is_Email_Confirmed, 
                        Source.Who_Can_Share_Contact_Card, Source.HashKey, Source.DW_Timestamp, 
                        Source.Sourcesystem_Name, Source.Workspace_ID, Source.Workspace_Name, Source.Created_By, 
                        Source.Created_Date, Source.Last_Modified_By, Source.Last_Modified_Date
                    )
                '''
            spark.sql(MergeQuery)
            dg = spark.sql('''
                UPDATE silver.Silver_Slack_Users_New wc
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
            MaxDateQuery = "SELECT MAX({0}) AS Max_Date FROM VW_Users".format(MaxLoadDateColumn)
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