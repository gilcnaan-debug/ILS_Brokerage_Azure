# Databricks notebook source
# MAGIC %md
# MAGIC ##IMPORTING PACKAGES

# COMMAND ----------

from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import time
import gspread
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col, concat_ws, lower

# COMMAND ----------

# MAGIC %md
# MAGIC ##CALLING UTILITIES

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ##CALLING LOGGER

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ##GETTING CATALOG INFO 

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##CREATING ERROR LOGGER VARIABLE

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("NFI_Shipper_Mapping_Mail_Automation_Bronze")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##RETRIEVEING SECRET VALUES FROM KEY VALUT

# COMMAND ----------

##Collecting Secret values from the Azure keyVault and storing them in variables
try:
    logger.info("Getting secret values and forming the connection string")
    
    client_email = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "ClientEmail")
    private_key = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "PrivateKey")
    project_id = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "ProjectId")
    client_id = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "ClientIdSM")
    private_key_id = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "PrivateKeyId")   

except Exception as e:
    logger.error(f"Unable to retrieve secret values from keyvault {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##GETTING METADATA DETAILS

# COMMAND ----------

try:
#Getting the list of DMS tables in the dataframe that needs to be loaded into stage
    DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid in ( 'SP-1') and IsActive='1' ")
except:
    logger.error(f"Unable to retrieve metadata from the table {e}")

# COMMAND ----------

#Getting the TableID from the data frame and iterate over them in for loop
#Tableslist = [1,2,3]
TablesList = DF_Metadata.select(col('TableID')).collect()
for TableID in TablesList:
    TableID = TableID.TableID
    TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == TableID).collect()[0].SourceTableName

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
    except Exception as e:
        logger.error(f"Unable to retrieve variables from metadata table {e}")
        continue

# COMMAND ----------

# MAGIC %md
# MAGIC ##CREATING ERROR LOGGER TABLE VARIABLES

# COMMAND ----------

try:
    Job_ID = "SM-1"
    Notebook_ID = "SM-Automation-Bronze"
    Zone = "Bronze"
    Table_ID=TableID
    Table_Name=TableName
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATING CREDS AND SCOPES

# COMMAND ----------


try:
  print("Creating a credentials dict and scopes needed")
  # Define the scope
  scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
  private_key = private_key.replace("\\n", "\n") 
  credentials_dict = {
    "type": "service_account",
    "project_id": project_id,
    "private_key_id": private_key_id,
    "private_key": private_key,
    "client_email": client_email,
    "client_id": client_id,
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email}",
  }
except Exception as e:
  logger.error(f"Error in creating credentials dict and scopes needed {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##MERGING INTO BRONZE TABLE

# COMMAND ----------

try:
  # Create credentials object
  # Define the scope
  scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

  credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scopes=scope)
# Authorize the client
  client = gspread.authorize(credentials)

  # Open the Google Sheet by ID
  spreadsheet_id = '1T6Hz3OjbCGymwuO1_pGDs2Qd8Oo29egSE9A7np4BduE' 
  spreadsheet = client.open_by_key(spreadsheet_id)

  # Select the worksheet where you want to read data
  worksheet = spreadsheet.get_worksheet(0)  # Change index if needed

  # Read all values from the worksheet
  data = worksheet.get_all_values()

  # Convert to DataFrame
  df = pd.DataFrame(data[1:], columns=data[0])  # Use first row as header

  # Create a temporary view in Spark
  df_spark = spark.createDataFrame(df)
  df_spark = spark.createDataFrame(df).withColumn("is_deleted", lit(0))

  # Create a temporary view with a merge key
  df_spark_with_key = df_spark.withColumn(
      "merge_key",
      concat_ws("_", lower(col("Customer Master")), lower(col("Verified AM")), lower(col("Verified Office")))
  )

  df_spark_with_key.createOrReplaceTempView("Mapped_shipper")

  # Read existing data from bronze.shipper_mapping and create a merge key
  bronze_shipper_mapping = spark.table("bronze.shipper_mapping")

  bronze_shipper_mapping.createOrReplaceTempView("bronze_shipper_mapping_with_key")

  # Perform the merge using Spark SQL
  merge_sql = f"""
  MERGE INTO bronze.{TableName} AS target
  USING Mapped_shipper AS source
  ON target.{MergeKey} = source.{MergeKey}
  WHEN MATCHED THEN
    UPDATE SET 
      target.Customer_Master = source.`Customer Master`,
      target.Verified_AM = source.`Verified AM`,
      target.Verified_Office = source.`Verified Office`,
      target.Sales_Resource = source.`Sales Resource`,
      target.Email_Address = source.`Email Address of AM`,
      target.Hubspot_Company_Name = source.`Hubspot Company Name`,
      target.Hubspot_AM = source.`Hubspot AM`,
      target.Slack_Channel_Name = source.`Slack Channel Name`,
      target.Vertical = source.`Vertical`,
      target.Merge_Key = source.merge_key,
      target.Is_deleted = source.is_deleted,
      target.Updated_At = current_timestamp()
  WHEN NOT MATCHED THEN
    INSERT (
      Customer_Master, Verified_AM, Verified_Office, Sales_Resource, Email_Address,
      Hubspot_Company_Name, Hubspot_AM, Slack_Channel_Name, Vertical,
      Merge_Key, Is_deleted, Inserted_At, Updated_At
    )
    VALUES (
      source.`Customer Master`, source.`Verified AM`, source.`Verified Office`, source.`Sales Resource`,
      source.`Email Address of AM`, source.`Hubspot Company Name`, source.`Hubspot AM`,
      source.`Slack Channel Name`, source.`Vertical`, source.merge_key, source.is_deleted,
      current_timestamp(), current_timestamp()
    )
  """

  # Execute the merge statement
  spark.sql(merge_sql)
  UpdatePipelineStatusAndTime(TableID,'Bronze')
  UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Shipper Mapping-Merge")
  
  print("Data read from Google Sheet and merged with bronze.shipper_mapping successfully!")
except Exception as e:
  Error_Statement = str(e).replace("'", "''")
  logger.error(f"Error in reading data from Google Sheet and merging with Target table: {e}")
  raise RuntimeError(f"Job failed: {Error_Statement}")
  UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Attention Needed",1,"Shipper Mapping-Merge")

# COMMAND ----------

# MAGIC %md
# MAGIC ##CAPTURING DELETES

# COMMAND ----------

try:
    deleted = f"""
    UPDATE bronze.{TableName} AS target
    SET target.Is_deleted = 1
    WHERE target.merge_key NOT IN (
        SELECT merge_key FROM mapped_shipper
    )
    """

    # Execute the update statement for is_deleted
    spark.sql(deleted)
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Shipper Mapping-Merge")
    print("Data read from Google Sheet and merged with Target table successfully!")
except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    logger.error(f"Error in updating is_deleted column: {e}")
    raise RuntimeError(f"Job failed: {Error_Statement}")
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Attention Needed",1,"Shipper Mapping-Merge")