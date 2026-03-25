# Databricks notebook source
# MAGIC %md
# MAGIC ###Finance_UnitTest

# COMMAND ----------

# MAGIC %md
# MAGIC #### Description: Unit testing for Finance_UnitTest
# MAGIC ##### Created Date: 19-07-2024
# MAGIC ##### Created By: Hariharan
# MAGIC ##### Modified Date: 19-07-2024
# MAGIC ##### Modified By: Hariharan
# MAGIC ##### Changes made:

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Packages

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from decimal import Decimal
from io import BytesIO
import os
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use Catalog

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Metadata Information

# COMMAND ----------

#Metadata scripts 
MaxLoadDate_str = '2020-07-03 00:00:00'
MaxLoadDate = datetime.strptime(MaxLoadDate_str, "%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Collecting Secrets

# COMMAND ----------

# Retrieve the secrets from Azure Key Vault
try:
    tenant_id = dbutils.secrets.get(scope="NFI_Finance_Storage", key="TenantID")
    client_id = dbutils.secrets.get(scope="NFI_Finance_Storage", key="ClientID")
    client_secret = dbutils.secrets.get(scope="NFI_Finance_Storage", key="Client-SecretID")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Azure Connection Test

# COMMAND ----------


# Create a credential using your AD credentials
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Storage account URL
storage_account_url = "https://dlsfindatacomeus1.blob.core.windows.net/"

# Create the Blob service client
blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)

def test_blob_service_connection(blob_service_client):
    try:
        # List all containers in the blob storage
        containers = blob_service_client.list_containers()
        print("Successfully connected! Here are the containers:")
        for container in containers:
            print(f"- {container.name}")
        return True
    except Exception as e:
        print(f"Failed to connect or list containers: {e}")
        return False
    raise

# Use the function to test the connection
if test_blob_service_connection(blob_service_client):
    print("Connection test successful.")

else:
    print("Connection test failed.")
 

# COMMAND ----------

# MAGIC %md
# MAGIC #### GLSummary 

# COMMAND ----------


container_name = "finance" 
container_client = blob_service_client.get_container_client(container_name)

try: 
    # Container name and folder path
    # UpdatePipelineStartTime(TableID, Zone)
    container_name = "finance"
    summary_folder_path = "Finance Prod/NFI_ILS_GLSummary_"

    # Create a container client
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs in the container and filter by folder path
    summary_blob_list = container_client.list_blobs(name_starts_with=summary_folder_path)

    summary_blobs = [result.name for result in summary_blob_list]

    summary_latest_blobs = []
    for summary_blob_name in summary_blobs:
        timestamp_str = summary_blob_name.split("_")[-1].split(".")[0]
        summary_fiscal_year = summary_blob_name.split("GLSummary_")[1].split("_")[0]
        summary_timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        if summary_timestamp > MaxLoadDate:
            summary_latest_blobs.append((summary_blob_name, summary_fiscal_year, summary_timestamp))
    summary_latest_blobs.sort(key=lambda x: (x[1], x[2]))
    
 
except Exception as e:
    print(e)


# COMMAND ----------

resultsummary =[]
try:
    for summary_blob_name, summary_fiscal_year, summary_timestamp in summary_latest_blobs:
        blob_client = container_client.get_blob_client(summary_blob_name)
        downloaded_blob = blob_client.download_blob().readall()
        
        summary_decimal_columns = ['TRAN_AMOUNT', 'BASE_AMOUNT', 'RPT_AMOUNT_1', 'UNITS_AMOUNT']
        converters = {col: Decimal for col in summary_decimal_columns}

        summary_df = pd.read_csv(BytesIO(downloaded_blob), sep='|', encoding='ISO-8859-1', decimal='.', converters=converters)

        summary_existing_columns = ['OBJ_ID','COMPANY','ACCT_UNIT','ACCT_UNIT_DESCR','ACCT_SUB','ACCOUNT_DESC','ACCT_PERIOD','FISCAL_YEAR','CURRENCY_CODE','TRAN_AMOUNT', 'BASE_AMOUNT', 'RPT_AMOUNT_1','UNITS_AMOUNT']

        summary_new_columns = [col for col in summary_df.columns if col not in summary_existing_columns]
        
        if summary_new_columns:
            error_message = f"New columns found in file {blob_name}: {', '.join(new_columns)}"
            print(error_message)
            UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Failed", error_message, "NotIgnorable", 1, "New Columns Added")
            logger.info(error_message)
            raise
    

        summary_filename = os.path.basename(summary_blob_name)  # Get the filename from the blob name
        summary_df['SOURCE_FILE_NAME'] = summary_filename
        summary_pd_df = summary_df[['OBJ_ID','COMPANY','ACCT_UNIT','ACCT_UNIT_DESCR','ACCT_SUB','ACCOUNT_DESC','ACCT_PERIOD','FISCAL_YEAR','CURRENCY_CODE','TRAN_AMOUNT', 'BASE_AMOUNT', 'RPT_AMOUNT_1', 'UNITS_AMOUNT','SOURCE_FILE_NAME']]

        summary_schema = StructType([
            StructField("OBJ_ID", IntegerType(), True),
            StructField("COMPANY", IntegerType(), True),
            StructField("ACCT_UNIT", IntegerType(), True),
            StructField("ACCT_UNIT_DESCR", StringType(), True),
            StructField("ACCT_SUB", StringType(), True),
            StructField("ACCOUNT_DESC", StringType(), True),
            StructField("ACCT_PERIOD", IntegerType(), True),
            StructField("FISCAL_YEAR", ShortType(), True),
            StructField("CURRENCY_CODE", StringType(), True),
            StructField("TRAN_AMOUNT", DecimalType(18,2), True),
            StructField("BASE_AMOUNT", DecimalType(18,2), True),
            StructField("RPT_AMOUNT_1", DecimalType(18,2), True),
            StructField("UNITS_AMOUNT", DecimalType(18,2), True),
            StructField("SOURCE_FILE_NAME", StringType(),True)     
        ])
        
        # Convert the pandas DataFrame to a list of dictionaries
        summary_data = summary_pd_df.to_dict('records')

        # Convert the list of dictionaries to a list of Row objects
        summary_rows = [Row(**dict_) for dict_ in summary_data]

        # Create the Spark DataFrame
        summary_df = spark.createDataFrame(summary_rows, summary_schema)

        columns_to_replace_summary = [
            "OBJ_ID",
            "COMPANY",
            "ACCT_UNIT",
            "ACCT_UNIT_DESCR",
            "ACCT_SUB",
            "ACCOUNT_DESC",
            "ACCT_PERIOD",
            "FISCAL_YEAR",
            "CURRENCY_CODE",
            "TRAN_AMOUNT",
            "BASE_AMOUNT",
            "RPT_AMOUNT_1",
            "UNITS_AMOUNT",
            "SOURCE_FILE_NAME"
        ]

        for col_name in columns_to_replace_summary:
            summary_df = summary_df.withColumn(col_name, when(col(col_name) == "NaN", None).otherwise(col(col_name)))
            DF_GLSummary = summary_df


        try:
            Hashkey_Columns = ["COMPANY","ACCT_UNIT","ACCT_UNIT_DESCR","ACCT_SUB","ACCOUNT_DESC","ACCT_PERIOD","FISCAL_YEAR","CURRENCY_CODE","TRAN_AMOUNT","BASE_AMOUNT","RPT_AMOUNT_1","UNITS_AMOUNT"]

            DF_GLSummary = DF_GLSummary.withColumn("HASHKEY",md5(concat_ws("",*Hashkey_Columns)))
    
        except Exception as e:
            print(e)
        try:
           
            DF_GLSummary.dropDuplicates(['OBJ_ID'])
    
        except Exception as e:
            print(e)

        try: 
            DF_GLSummary.createOrReplaceTempView('VW_GLSummary')
        except Exception as e:
            print(e)

        try: 
            summary_row_count = spark.sql("SELECT COUNT(*) as count FROM VW_GLSummary").collect()[0]['count']

            # Null counts
            summary_null_counts = {}
            for column in DF_GLSummary.columns:
                null_count = spark.sql(f"SELECT COUNT(*) as count FROM VW_GLSummary WHERE {column} IS NULL").collect()[0]['count']
                summary_null_counts[column] = null_count

            # Duplicate count 
            summary_duplicate_count = spark.sql("""
                SELECT COUNT(*) as count
                FROM (
                    SELECT OBJ_ID, COUNT(*) as cnt
                    FROM VW_GLSummary
                    GROUP BY OBJ_ID
                    HAVING COUNT(*) > 1
                ) subquery
            """).collect()[0]['count']

            # Sample data
            summary_sample_data = spark.sql('''
                                    SELECT OBJ_ID, COMPANY,UNITS_AMOUNT,TRAN_AMOUNT,BASE_AMOUNT,ACCT_UNIT
                                    FROM VW_GLSummary 
                                    ORDER BY UNITS_AMOUNT
                                    LIMIT 2;''').collect()

            # Append results to the list
            resultsummary.append({
                'filename': summary_filename,
                'row_count': summary_row_count,
                'null_counts': summary_null_counts,
                'duplicate_count': summary_duplicate_count,
                'sample_data': [row.asDict() for row in summary_sample_data]
            })

        except Exception as e:
            print(e)
        
except Exception as e:
    print(e)



# COMMAND ----------


# Convert the results to a list of Row objects
summary_file_names = ", ".join(f"'{item['filename']}'" for item in resultsummary)
summary_rows = []
for file_result in resultsummary:
    summary_rows.append(Row(
        filename=file_result['filename'],
        row_count=file_result['row_count'],
        duplicate_count=file_result['duplicate_count'],
        null_counts=str(file_result['null_counts'])  # Convert dict to string for simplicity
    ))

# Create a DataFrame from the rows and remove duplicates
summary_df = spark.createDataFrame(summary_rows).dropDuplicates(['filename'])

# Register the DataFrame as a temporary view
summary_df.createOrReplaceTempView("file_summary")


# COMMAND ----------

# MAGIC %md
# MAGIC #### GLSummary - Row Count Check

# COMMAND ----------


print("File GLSummary - Source:")
spark.sql("""
    SELECT filename, row_count
    FROM file_summary
    ORDER BY filename
""").display(truncate=False)

print("Table GLSummary - Brone:")

rowcount_bronze =   '''SELECT SOURCE_FILE_NAME,COUNT(*) AS Row_count
    FROM Bronze.Finance_GLSummary
    WHERE SOURCE_FILE_NAME IN ({0})
    GROUP BY SOURCE_FILE_NAME
    ORDER BY SOURCE_FILE_NAME'''
rowcount_bronze = rowcount_bronze.format(summary_file_names)
spark.sql(rowcount_bronze).display()




# COMMAND ----------

# MAGIC %md
# MAGIC #### GLSummary - Total Record Count

# COMMAND ----------


print("\nTotal records across all GLSmmary Source files:")
spark.sql("""
    SELECT SUM(row_count) as total_records
    FROM file_summary
""").display()

print("\nTotal records across the tables for that specific loaded files :")
    
total_rowcount_bronze ='''
    SELECT count(*) as total_records
    FROM Bronze.Finance_GLSummary 
    WHERE SOURCE_FILE_NAME IN ({0})'''
total_rowcount_bronze=total_rowcount_bronze.format(summary_file_names)
spark.sql(total_rowcount_bronze).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### GLSummary Null Count Check

# COMMAND ----------

summary_df = summary_df.withColumn("null_counts_map", from_json(col("null_counts"), MapType(StringType(), IntegerType())))

# Register the updated DataFrame as a view
summary_df.createOrReplaceTempView("file_summary")

print("\nFiles with null values:")
result = spark.sql("""
    SELECT filename, null_counts_map
    FROM file_summary
    WHERE null_counts != '{}'
""")

summary_rows = result.collect()

#Explode the map entries into rows
exploded_df = summary_df.select("filename", explode("null_counts_map"))

transposed_df = exploded_df.select(
    col("key").alias("column_name"),
    col("filename"),
    col("value").alias("null_count")
)

# Group by the column names and pivot based on the filenames to get the transposed view
transposed_pivot_df = transposed_df.groupBy("column_name").pivot("filename").agg(first("null_count"))

# Replace null with zeros since a file may not have all the same columns leading to null entries
transposed_pivot_df = transposed_pivot_df.fillna(0)

# Show the transposed DataFrame
transposed_pivot_df.display()

# COMMAND ----------

nullcount_bronze = '''
SELECT SOURCE_FILE_NAME, 'OBJ_ID_null_count' AS Column_Name, SUM(CASE WHEN OBJ_ID IS NULL THEN 1 ELSE 0 END) AS Null_Count FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'COMPANY_null_count', SUM(CASE WHEN COMPANY IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'ACCT_UNIT_null_count', SUM(CASE WHEN ACCT_UNIT IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'ACCT_UNIT_DESCR_null_count', SUM(CASE WHEN ACCT_UNIT_DESCR IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'ACCT_SUB_null_count', SUM(CASE WHEN ACCT_SUB IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'ACCOUNT_DESC_null_count', SUM(CASE WHEN ACCOUNT_DESC IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'ACCT_PERIOD_null_count', SUM(CASE WHEN ACCT_PERIOD IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'FISCAL_YEAR_null_count', SUM(CASE WHEN FISCAL_YEAR IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'CURRENCY_CODE_null_count', SUM(CASE WHEN CURRENCY_CODE IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'TRAN_AMOUNT_null_count', SUM(CASE WHEN TRAN_AMOUNT IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'BASE_AMOUNT_null_count', SUM(CASE WHEN BASE_AMOUNT IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'RPT_AMOUNT_1_null_count', SUM(CASE WHEN RPT_AMOUNT_1 IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'UNITS_AMOUNT_null_count', SUM(CASE WHEN UNITS_AMOUNT IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
UNION ALL
SELECT SOURCE_FILE_NAME, 'SOURCE_FILE_NAME_null_count', SUM(CASE WHEN SOURCE_FILE_NAME IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLSummary WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME
ORDER BY SOURCE_FILE_NAME ASC;

'''

nullcount_bronze = nullcount_bronze.format(summary_file_names)
spark.sql(nullcount_bronze).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### GLSummary - Duplicate Check 

# COMMAND ----------

print("\nFiles with duplicates:")
spark.sql("""
    SELECT filename, duplicate_count
    FROM file_summary
    WHERE duplicate_count > 0
    ORDER BY duplicate_count DESC
""").display(truncate=False)

print("\n GLSummary Table - Bronze with duplicates:")
duplicate_check_bronze ='''
SELECT OBJ_ID, COUNT(*) AS duplicate_count
FROM bronze.Finance_GLSummary
WHERE SOURCE_FILE_NAME IN ({0})
GROUP BY OBJ_ID,SOURCE_FILE_NAME
HAVING COUNT(*) > 1; '''

duplicate_check_bronze = duplicate_check_bronze.format(summary_file_names)
spark.sql(duplicate_check_bronze).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####GLSummary - Sample Data Check

# COMMAND ----------

for file_result in resultsummary:
    print(f"\nSample data for file: {file_result['filename']}")
    sample_data = file_result['sample_data']
    
    # If sample_data is a list of dictionaries, convert to Spark DataFrame and show
    if isinstance(sample_data, list) and sample_data and isinstance(sample_data[0], dict):
        sample_df = spark.createDataFrame(sample_data)
        sample_df.display(truncate=False)
    else:
        print("No sample data available.")

# COMMAND ----------

sample_data_bronze = ''' SELECT SOURCE_FILE_NAME,OBJ_ID, COMPANY,UNITS_AMOUNT,TRAN_AMOUNT,BASE_AMOUNT,ACCT_UNIT FROM 
(SELECT  SOURCE_FILE_NAME,OBJ_ID, COMPANY,UNITS_AMOUNT,TRAN_AMOUNT,BASE_AMOUNT,ACCT_UNIT, ROW_NUMBER() OVER( PARTITION BY SOURCE_FILE_NAME ORDER BY MAX(UNITS_AMOUNT) ) AS RANK
FROM Bronze.Finance_GLSummary
WHERE SOURCE_FILE_NAME IN ({0})
GROUP BY 1,2,3,4,5,6,7
ORDER BY SOURCE_FILE_NAME ) WHERE RANK IN (1,2) '''
sample_data_bronze = sample_data_bronze.format(summary_file_names)
spark.sql(sample_data_bronze).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GLDetail

# COMMAND ----------


try: 
    container_name = "finance"
    detail_folder_path = "Finance Prod/NFI_ILS_GLDetail_"

    # Create a container client
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs in the container and filter by folder path
    detail_blob_list = container_client.list_blobs(name_starts_with=detail_folder_path)

    details_blobs = [result.name for result in detail_blob_list]

    detail_latest_blobs = []
    for detail_blob_name in details_blobs:
        timestamp_str = detail_blob_name.split("_")[-1].split(".")[0]
        detail_fiscal_year = detail_blob_name.split("GLDetail_")[1].split("_")[0]
        detail_timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        if detail_timestamp > MaxLoadDate:
            detail_latest_blobs.append((detail_blob_name, detail_fiscal_year, detail_timestamp))

    detail_latest_blobs.sort(key=lambda x: (x[1], x[2]))
    
 
except Exception as e:
    print(e)



# COMMAND ----------

resultsdetail = []
try:
    for detail_blob_name, detail_fiscal_year,detail_timestamp in detail_latest_blobs:
        blob_client = container_client.get_blob_client(detail_blob_name)
        downloaded_blob = blob_client.download_blob().readall()
        detail_decimal_columns = ['TRAN_AMOUNT', 'BASE_AMOUNT', 'RPT_AMOUNT_1', 'UNITS_AMOUNT']
        converters = {col: Decimal for col in detail_decimal_columns}

        detail_df = pd.read_csv(BytesIO(downloaded_blob), sep='|', encoding='ISO-8859-1', decimal='.', converters=converters)

        detail_existing_columns = ['OBJ_ID','COMPANY','FISCAL_YEAR','ACCT_PERIOD','JE_NUMBER','SOURCE_SYSTEM','JE_TYPE','ACCT_UNIT','ACCT_UNIT_DESCR','ACCT_SUB','ACCOUNT_DESC','SOURCE_CODE','REFERENCE','SOURCE_DESC','AUTO_REV','POSTING_DATE','TRAN_AMOUNT', 'BASE_AMOUNT', 'RPT_AMOUNT_1', 'UNITS_AMOUNT', 'TRANS_DESC','CURRENCY_CODE']

        detail_new_columns = [col for col in detail_df.columns if col not in detail_existing_columns]
        
        if detail_new_columns:
            error_message = f"New columns found in file {blob_name}: {', '.join(detail_new_columns)}"
            print(error_message)
            UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Failed", error_message, "NotIgnorable", 1, "New Columns Added")
            logger.info(error_message)
            raise
    
        detail_filename = os.path.basename(detail_blob_name)  # Get the filename from the blob name
        detail_df['SOURCE_FILE_NAME'] = detail_filename
        detail_pd_df = detail_df[['OBJ_ID','COMPANY','FISCAL_YEAR','ACCT_PERIOD','JE_NUMBER','SOURCE_SYSTEM','JE_TYPE','ACCT_UNIT','ACCT_UNIT_DESCR','ACCT_SUB','ACCOUNT_DESC','SOURCE_CODE','REFERENCE','SOURCE_DESC','AUTO_REV','POSTING_DATE','TRAN_AMOUNT', 'BASE_AMOUNT', 'RPT_AMOUNT_1', 'UNITS_AMOUNT', 'TRANS_DESC','CURRENCY_CODE','SOURCE_FILE_NAME']]

        detail_schema = StructType([
            StructField("OBJ_ID", IntegerType(), True),
            StructField("COMPANY", IntegerType(), True),
            StructField("FISCAL_YEAR", ShortType(), True),
            StructField("ACCT_PERIOD", IntegerType(), True),
            StructField("JE_NUMBER", IntegerType(), True),
            StructField("SOURCE_SYSTEM", StringType(), True),
            StructField("JE_TYPE", StringType(), True),
            StructField("ACCT_UNIT", IntegerType(), True),
            StructField("ACCT_UNIT_DESCR", StringType(), True),
            StructField("ACCT_SUB", StringType(), True),
            StructField("ACCOUNT_DESC", StringType(), True),
            StructField("SOURCE_CODE", StringType(), True),
            StructField("REFERENCE", StringType(), True),
            StructField("SOURCE_DESC", StringType(), True),
            StructField("AUTO_REV", StringType(), True),
            StructField("POSTING_DATE", StringType(), True),
            StructField("TRAN_AMOUNT", DecimalType(18,2), True),
            StructField("BASE_AMOUNT",  DecimalType(18,2), True),
            StructField("RPT_AMOUNT_1", DecimalType(18,2), True),
            StructField("UNITS_AMOUNT",  DecimalType(18,2), True),
            StructField("TRANS_DESC", StringType(), True),
            StructField("CURRENCY_CODE", StringType(), True),
            StructField("SOURCE_FILE_NAME", StringType(),True)     
        ])
        # Convert the pandas DataFrame to a list of dictionaries
        detail_data = detail_pd_df.to_dict('records')

        # Convert the list of dictionaries to a list of Row objects
        detail_rows = [Row(**dict_) for dict_ in detail_data]

        # Create the Spark DataFrame
        detail_df = spark.createDataFrame(detail_rows, detail_schema)

        columns_to_replace = [
            "OBJ_ID",
            "COMPANY",
            "FISCAL_YEAR",
            "ACCT_PERIOD",
            "JE_NUMBER",
            "SOURCE_SYSTEM",
            "JE_TYPE",
            "ACCT_UNIT",
            "ACCT_UNIT_DESCR",
            "ACCT_SUB",
            "ACCOUNT_DESC",
            "SOURCE_CODE",
            "REFERENCE",
            "SOURCE_DESC",
            "AUTO_REV",
            "POSTING_DATE",
            "TRAN_AMOUNT",
            "BASE_AMOUNT",
            "RPT_AMOUNT_1",
            "UNITS_AMOUNT",
            "TRANS_DESC",
            "CURRENCY_CODE",
            "SOURCE_FILE_NAME" ]

        for col_name in columns_to_replace:
            detail_df = detail_df.withColumn(col_name, when(col(col_name) == "NaN", None).otherwise(col(col_name)))

        DF_GLDetail = detail_df.withColumn("POSTING_DATE", col("POSTING_DATE").cast("timestamp")) 

        try:
            Hashkey_Columns = ["COMPANY","FISCAL_YEAR","ACCT_PERIOD","JE_NUMBER","SOURCE_SYSTEM","JE_TYPE","ACCT_UNIT","ACCT_UNIT_DESCR","ACCT_SUB","ACCOUNT_DESC","SOURCE_CODE","REFERENCE","SOURCE_DESC","AUTO_REV","POSTING_DATE","TRAN_AMOUNT","BASE_AMOUNT","RPT_AMOUNT_1","UNITS_AMOUNT","TRANS_DESC","CURRENCY_CODE"]

            DF_GLDetail = DF_GLDetail.withColumn("HASHKEY",md5(concat_ws("",*Hashkey_Columns)))
    
        except Exception as e:
            logger.info(f"Unable to create Hashkey for summ {e}")
            print(e)
        try:
           
            DF_GLDetail.dropDuplicates(['OBJ_ID'])
    
        except Exception as e:
            logger.info(f"Unable to Drop Dupliactes from the dataframe {e}")
            print(e)

        try: 
            DF_GLDetail.createOrReplaceTempView('VW_GLDetail')
        except Exception as e:
            logger.info(f"Unable to Create View {e}")
            print(e)

        try:
            row_count = spark.sql("SELECT COUNT(*) as count FROM VW_GLDetail ").collect()[0]['count']

            # Null counts
            null_counts = {}
            for column in DF_GLDetail.columns:
                null_count = spark.sql(f"SELECT COUNT(*) as count FROM VW_GLDetail WHERE {column} IS NULL").collect()[0]['count']
                null_counts[column] = null_count

            # Duplicate count (assuming OBJ_ID is the unique identifier)
            duplicate_count = spark.sql("""
                SELECT COUNT(*) as count
                FROM (
                    SELECT OBJ_ID, COUNT(*) as cnt
                    FROM VW_GLDetail
                    GROUP BY OBJ_ID
                    HAVING COUNT(*) > 1
                ) subquery
            """).collect()[0]['count']

            # Sample data
            sample_data = spark.sql('''
                                    SELECT OBJ_ID, COMPANY,UNITS_AMOUNT
                                    FROM VW_GLDetail 
                                    ORDER BY UNITS_AMOUNT
                                    LIMIT 2;''').collect()

            # Append results to the list
            resultsdetail.append({
                'filename': detail_filename,
                'row_count': row_count,
                'null_counts': null_counts,
                'duplicate_count': duplicate_count,
                'sample_data': [row.asDict() for row in sample_data]
            })

         
        except Exception as e:
            print(e)
      
except Exception as e:
    print(e)
 

# COMMAND ----------


# Convert the results to a list of Row objects
detail_file_names = ", ".join(f"'{item['filename']}'" for item in resultsdetail)
detail_rows = []
for file_result in resultsdetail:
    detail_rows.append(Row(
        filename=file_result['filename'],
        row_count=file_result['row_count'],
        duplicate_count=file_result['duplicate_count'],
        detail_null_counts=str(file_result['null_counts'])  # Convert dict to string for simplicity
    ))

# Create a DataFrame from the rows and remove duplicates
detail_df = spark.createDataFrame(detail_rows).dropDuplicates(['filename'])

# Register the DataFrame as a temporary view
detail_df.createOrReplaceTempView("file_detail")


# COMMAND ----------

# MAGIC %md
# MAGIC ####GLDetail - Row Count Check

# COMMAND ----------

# Now you can use SQL to query and print the results
print("File GLDetail - Source:")
spark.sql("""
    SELECT filename, row_count
    FROM file_detail
    ORDER BY filename
""").display(truncate=False)

print("Table GLDetail - Brone:")

rowcount_bronze =   '''SELECT SOURCE_FILE_NAME,COUNT(*) AS Row_count
    FROM Bronze.Finance_GLDetail
    WHERE SOURCE_FILE_NAME IN ({0})
    GROUP BY SOURCE_FILE_NAME
    ORDER BY SOURCE_FILE_NAME'''
rowcount_bronze = rowcount_bronze.format(detail_file_names)
spark.sql(rowcount_bronze).display()




# COMMAND ----------

# MAGIC %md
# MAGIC #### GLDetail - Total Record Count

# COMMAND ----------


print("\nTotal records across all GLSmmary Source files:")
spark.sql("""
    SELECT SUM(row_count) as total_records
    FROM file_detail
""").display()

print("\nTotal records across the tables for that specific loaded files :")
    
total_rowcount_bronze ='''
    SELECT count(*) as total_records
    FROM Bronze.Finance_GLDetail 
    WHERE SOURCE_FILE_NAME IN ({0})'''
total_rowcount_bronze=total_rowcount_bronze.format(detail_file_names)
spark.sql(total_rowcount_bronze).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### GLDetail - Null Count 

# COMMAND ----------


# First, let's convert the null_counts string to a map
detail_df = detail_df.withColumn("null_counts_map", from_json(col("detail_null_counts"), MapType(StringType(), IntegerType())))

# Register the updated DataFrame as a view
detail_df.createOrReplaceTempView("file_detail")

# Now, let's query and display the results
print("\nFiles with null values:")
result_detail_null = spark.sql("""
    SELECT filename, null_counts_map
    FROM file_detail
    WHERE detail_null_counts != '{}'
""")

detail_rows = result_detail_null.collect()

#Explode the map entries into rows
detail_exploded_df = detail_df.select("filename", explode("null_counts_map"))

# Select and alias the columns after exploding them
transposed_df = detail_exploded_df.select(
    col("key").alias("column_name"),
    col("filename"),
    col("value").alias("null_count")
)

# Group by the column names and pivot based on the filenames to get the transposed view
transposed_pivot_df = transposed_df.groupBy("column_name").pivot("filename").agg(first("null_count"))

# Replace null with zeros since a file may not have all the same columns leading to null entries
transposed_pivot_df = transposed_pivot_df.fillna(0)

# Show the transposed DataFrame
transposed_pivot_df.display()

# COMMAND ----------

detail_null_count_bronze = '''
SELECT SOURCE_FILE_NAME,'OBJ_ID_null_count' AS Column_Name, SUM(CASE WHEN OBJ_ID IS NULL THEN 1 ELSE 0 END) AS Null_Count FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'COMPANY_null_count', SUM(CASE WHEN COMPANY IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'FISCAL_YEAR_null_count', SUM(CASE WHEN FISCAL_YEAR IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'ACCT_PERIOD_null_count', SUM(CASE WHEN ACCT_PERIOD IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'JE_NUMBER_null_count', SUM(CASE WHEN JE_NUMBER IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'SOURCE_SYSTEM_null_count', SUM(CASE WHEN SOURCE_SYSTEM IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'JE_TYPE_null_count', SUM(CASE WHEN JE_TYPE IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'ACCT_UNIT_null_count', SUM(CASE WHEN ACCT_UNIT IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'ACCT_UNIT_DESCR_null_count', SUM(CASE WHEN ACCT_UNIT_DESCR IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'ACCT_SUB_null_count', SUM(CASE WHEN ACCT_SUB IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'ACCOUNT_DESC_null_count', SUM(CASE WHEN ACCOUNT_DESC IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'SOURCE_CODE_null_count', SUM(CASE WHEN SOURCE_CODE IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'REFERENCE_null_count', SUM(CASE WHEN REFERENCE IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'SOURCE_DESC_null_count', SUM(CASE WHEN SOURCE_DESC IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'AUTO_REV_null_count', SUM(CASE WHEN AUTO_REV IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'POSTING_DATE_null_count', SUM(CASE WHEN POSTING_DATE IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'TRAN_AMOUNT_null_count', SUM(CASE WHEN TRAN_AMOUNT IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'BASE_AMOUNT_null_count', SUM(CASE WHEN BASE_AMOUNT IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'RPT_AMOUNT_1_null_count', SUM(CASE WHEN RPT_AMOUNT_1 IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'UNITS_AMOUNT_null_count', SUM(CASE WHEN UNITS_AMOUNT IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'TRANS_DESC_null_count', SUM(CASE WHEN TRANS_DESC IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'CURRENCY_CODE_null_count', SUM(CASE WHEN CURRENCY_CODE IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
UNION ALL
SELECT SOURCE_FILE_NAME,'SOURCE_FILE_NAME_null_count', SUM(CASE WHEN SOURCE_FILE_NAME IS NULL THEN 1 ELSE 0 END) FROM Bronze.Finance_GLDetail WHERE SOURCE_FILE_NAME IN ({0}) GROUP BY SOURCE_FILE_NAME 
ORDER BY SOURCE_FILE_NAME ASC ;
'''

detail_null_count_bronze = detail_null_count_bronze.format(detail_file_names)
spark.sql(detail_null_count_bronze).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### GLDetail - Duplicate Check

# COMMAND ----------

print("\nFiles with duplicates:")
spark.sql("""
    SELECT filename, duplicate_count
    FROM file_detail
    WHERE duplicate_count > 0
    ORDER BY duplicate_count DESC
""").display(truncate=False)

print("\n GLDetail Table - Bronze with duplicates:")
duplicate_check_bronze ='''
SELECT OBJ_ID, COUNT(*) AS duplicate_count
FROM bronze.Finance_GLDetail
WHERE SOURCE_FILE_NAME IN ({0})
GROUP BY OBJ_ID,SOURCE_FILE_NAME
HAVING COUNT(*) > 1; '''

duplicate_check_bronze = duplicate_check_bronze.format(summary_file_names)
spark.sql(duplicate_check_bronze).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### GLDetail - Sample Data Check

# COMMAND ----------

for file_result in resultsdetail:
    print(f"\nSample data for file: {file_result['filename']}")
    sample_data = file_result['sample_data']
    
    # If sample_data is a list of dictionaries, convert to Spark DataFrame and show
    if isinstance(sample_data, list) and sample_data and isinstance(sample_data[0], dict):
        sample_df = spark.createDataFrame(sample_data)
        sample_df.display(truncate=False)
    else:
        print("No sample data available.")

# COMMAND ----------

sample_data_detail = ''' SELECT SOURCE_FILE_NAME,OBJ_ID, COMPANY,UNITS_AMOUNT FROM 
(SELECT  SOURCE_FILE_NAME,OBJ_ID,COMPANY,UNITS_AMOUNT, ROW_NUMBER() OVER( PARTITION BY SOURCE_FILE_NAME ORDER BY MAX(UNITS_AMOUNT) ) AS RANK
FROM Bronze.Finance_GLDetail
WHERE SOURCE_FILE_NAME IN ({0})
GROUP BY 1,2,3,4
ORDER BY SOURCE_FILE_NAME ) WHERE RANK IN (1,2) '''
sample_data_detail = sample_data_detail.format(detail_file_names)
spark.sql(sample_data_detail).display()

# COMMAND ----------

