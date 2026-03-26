# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract data from Source to Bronze as delta file
# MAGIC * **Created Date:** 03/15/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 10/10/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ##IMPORTING REQUIRED PACKAGES

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
from oauth2client.service_account import ServiceAccountCredentials
import json
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %md
# MAGIC ##CALLING UTILITES NOTEBOOK

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
# MAGIC ##CATALOG DETAILS

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ERROR LOGGER PATH

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("NFI_Awards_Extraction")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##COLLECTING METADATA DETAILS

# COMMAND ----------

Table_ID = 'AW1'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = 'AW1' and IsActive='1'")
Job_ID = DF_Metadata.select(col('Job_ID')).where(col('TableID') == Table_ID).collect()[0].Job_ID
Notebook_ID = DF_Metadata.select(col('NB_ID')).where(col('TableID') == Table_ID).collect()[0].NB_ID
TableName = DF_Metadata.select(col('DWHTableName')).where(col('TableID') == Table_ID).collect()[0].DWHTableName
MergeKey = DF_Metadata.select(col('MergeKey')).where(col('TableID') == Table_ID).collect()[0].MergeKey
MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateColumn
MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateValue
Zone = DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ##RETRIEVEING CREDS FROM KEY VAULT

# COMMAND ----------

# Retrieve the secrets from Azure Key Vault
try:
    #Update PipelineStartTime in Metadata
    UpdatePipelineStartTime(Table_ID, Zone)
    ProjectID = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="AwardsProjectID")
    PrivateKeyID = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="AwardsPrivateKeyID")
    ClientEmail = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="AwardsClientServiceEmail")
    ClientID = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="AwardsClientID")
    PrivateKey = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="AwardsPrivateKey")
except Exception as e:
    logger.info("unable to fetch creds from azure keyvault")
    print(e)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### FORMING CONNECTION STRINGS 

# COMMAND ----------


#Collecting Secret values from the Azure keyVault and storing them in variables
try:
    logger.info("Getting secret values and forming the connection string")
    # Construct the JSON key file content
    PrivateKey = PrivateKey.replace("\\n", "\n")  # This assumes the private key was stored with \n

    # Construct the JSON key file content
    json_keyfile_content = json.dumps({
        "type": "service_account",
        "project_id": ProjectID,
        "private_key_id": PrivateKeyID,
        "private_key": PrivateKey,  # Ensure this is formatted correctly now
        "client_email": ClientEmail,
        "client_id": ClientID,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{ClientEmail}",
        "universe_domain": "googleapis.com"
    })
except Exception as e:
    logger.error(f"Unable to Fetch secret from Azure Key Vault {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEFINING FUNCTIONS

# COMMAND ----------

#Authenticate with the service account using the Json_Keyfile and scope
def authenticate_with_service_account(json_keyfile_content, private_key):
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials_dict = json.loads(json_keyfile_content)
    credentials_dict["private_key"] = private_key  # Update private key
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gc = gspread.authorize(credentials)
    return gc
#Read the data from the spreadsheet
def get_spreadsheet_data(gc, spreadsheet_id):
    sh = gc.open_by_key(spreadsheet_id)
    worksheet = sh.get_worksheet(0)
    data = worksheet.get_all_values()
    return data
#Convert the response obtained from list to dataframe
def convert_to_dataframe(data):
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##MERGE INTO DESTINATION TABLE

# COMMAND ----------

#To Load data into Bronze Table.
try:
    AutoSkipperCheck = AutoSkipper(Table_ID,'Bronze')
    if AutoSkipperCheck == 0:

        gc = authenticate_with_service_account(json_keyfile_content, PrivateKey)
        data = get_spreadsheet_data(gc, '1Z1XQM2C2qnCP7qbbSGloyR8XAAgi_bHmg5tJzjOWKiA') 
        DF_Source_data = convert_to_dataframe(data)
        DF_Source = spark.createDataFrame(DF_Source_data)
        DF_Source.createOrReplaceTempView("Source")   

        DF_Source = DF_Source.withColumn(
            "Min_Rate",
            when(col("Min_Rate").isNotNull(),
                translate(
                    regexp_replace(
                        trim(col("Min_Rate")),
                        "\\,",  # Try to remove , with regexp_replace
                        ""
                    ),
                    ",",  # Use translate as a backup to remove ,
                    ""
                )
            ).otherwise(col("Min_Rate"))
        )

        DF_Source = DF_Source.withColumn(
            "Estimated_Miles",
            when(col("Estimated_Miles").isNotNull(),
                translate(
                    regexp_replace(
                        trim(col("Estimated_Miles")),
                        "\\,",  # Try to remove , with regexp_replace
                        ""
                    ),
                    ",",  # Use translate as a backup to remove ,
                    ""
                )
            ).otherwise(col("Estimated_Miles"))
        )

        # Then, attempt to cast to double, replacing with null if it fails
        DF_Source = DF_Source.withColumn(
            "Min_Rate",
            when(col("Min_Rate").cast("double").isNotNull(),
                col("Min_Rate").cast("double")
            ).otherwise(col("Min_Rate"))
        )
      

        DF_Source = DF_Source.withColumn(
            "RPM",
            when(col("RPM").isNotNull(),
                translate(
                    regexp_replace(
                        trim(col("RPM")),
                        "\\$",  # Try to remove $ with regexp_replace
                        ""
                    ),
                    "$",  # Use translate as a backup to remove $
                    ""
                )
            ).otherwise(col("RPM"))
        )

        # Then, attempt to cast to double, replacing with null if it fails
        DF_Source = DF_Source.withColumn(
            "RPM",
            when(col("RPM").cast("double").isNotNull(),
                col("RPM").cast("double")
            ).otherwise(col("RPM"))
        )

        # # Convert string to Float for Float data type columns
        DF_Source_Load = DF_Source \
            .withColumn("Estimated_Volume", col("Estimated_Volume").cast(FloatType())) \
            .withColumn("Estimated_Miles", col("Estimated_Miles").cast(FloatType())) \
            .withColumn("Linehaul", col("Linehaul").cast(FloatType())) \
            .withColumn("RPM", col("RPM").cast(FloatType())) \
            .withColumn("Flat_Rate", col("Flat_Rate").cast(FloatType())) \
            .withColumn("Min_Rate", col("Min_Rate").cast(FloatType())) \
            .withColumn("Max_Rate", col("Max_Rate").cast(FloatType()))

        ## Convert string to date for Date data type columns
        DF_Source_Load = DF_Source_Load \
            .withColumn("Periodic_Start_Date", to_date(col("Periodic_Start_Date"), "M/d/yyyy")) \
            .withColumn("Periodic_End_Date", to_date(col("Periodic_End_Date"), "M/d/yyyy")) \
            .withColumn("Updated_date", to_date(col("Updated_date"), "M/d/yyyy"))
         

        # List of all column names in the DataFrame
        columns = DF_Source_Load.columns

        # Iterate over all columns, replacing empty strings with null
        for column in columns:
            DF_Source_Load = DF_Source_Load.withColumn(column, when(col(column) == "", None).otherwise(col(column)))
        #Create Hashkey and Status Column
        DF_Source_Load = DF_Source_Load.withColumn('HashKey',md5(concat_ws("",*MergeKey.split(","))))
        DF_Source_Load = DF_Source_Load.withColumn('Status',lit('I'))
        DF_Source_Load = DF_Source_Load.dropDuplicates()
        print("Total Row Count:", DF_Source_Load.count())
        DF_Source_Load.createOrReplaceTempView('VW_Source')
    
        #Load Data into Table
        spark.sql(f'''MERGE INTO bronze.{TableName} Target USING VW_Source Source ON Target.HashKey = Source.HashKey WHEN NOT MATCHED THEN      INSERT (
                Target.HashKey,
                Target.RFP_Name,
                Target.Customer_Master,
                Target.Customer_Slug,
                Target.SAM_Name,
                Target.Lane_Description,
                Target.Origin_Facility,
                Target.Origin_Address,
                Target.Origin_City,
                Target.Origin_State,
                Target.Origin_Zip,
                Target.Origin_Country,
                Target.Destination_Facility,
                Target.Destination_Address,
                Target.Destination_City,
                Target.Destination_State,
                Target.Destination_Zip,
                Target.Destination_Country,
                Target.Origin_Live_Drop,
                Target.Destination_Live_Drop,
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
                Target.Status,
                Target.Sourcesystem_ID,
                Target.Sourcesystem_Name,
                Target.Created_date,
                Target.Created_by,
                Target.Last_Modified_date,
                Target.Last_Modified_by
            )
            Values
            (
                Source.HashKey,
                Source.RFP_Name,
                Source.Customer_Master,
                Source.Customer_Slug,
                Source.SAM_Name,
                Source.Lane_Description,
                Source.Origin_Facility,
                Source.Origin_Address,
                Source.Origin_City,
                Source.Origin_State,
                Source.Origin_Zip,
                Source.Origin_Country,
                Source.Destination_Facility,
                Source.Destination_Address,
                Source.Destination_City,
                Source.Destination_State,
                Source.Destination_Zip,
                Source.Destination_Country,
                Source.Origin_Live_Drop,
                Source.Destination_Live_Drop,
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
                Source.`Expected Margin`,
                Source.Status,
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
                    UPDATE bronze.{TableName}
                    SET bronze.{TableName}.Status = 'D', 
                        bronze.{TableName}.Last_Modified_date = Current_timestamp()
                    where bronze.{TableName}.HashKey in (select t.HashKey from bronze.{TableName} t Left join VW_Source s on t.HashKey = s.HashKey where s.HashKey is null AND t.Status <> 'D')
                ''')

        #Update The Metadata table
        MaxDateQuery = "Select max({0}) as Max_Date from VW_Source"
        MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
        DF_MaxDate = spark.sql(MaxDateQuery)
        UpdateLastLoadDate(Table_ID,DF_MaxDate)
        UpdatePipelineStatusAndTime(Table_ID,'Bronze')
        UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Load")

except Exception as e:
    logger.info('Failed for Bronze load')
    UpdateFailedStatus(Table_ID,'Bronze')
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Load")
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

