# Databricks notebook source
!pip install gspreadimport gspread


from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from pyspark.sql.functions import lit
import time
import gspread

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("NFI_Shipper_Mapping_Mail_Automation")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

##Collecting Secret values from the Azure keyVault and storing them in variables
try:
    logger.info("Getting secret values and forming the connection string")
    
    client_email = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "ClientEmail")
    private_key = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "PrivateKey")
    Project_Id = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "ProjectId")
    client_id = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "ClientIdSM")
    private_key_id = dbutils.secrets.get(scope = "NFI_Shipper_Mapping", key = "PrivateKeyId")   

except Exception as e:
    logger.error(f"Unable to retrieve secret values from keyvault {e}")

# COMMAND ----------


try:
  print("Creating a credentials dict and scopes needed")
  # Define the scope
  scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
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

try:
    # Create credentials object
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scopes=scope)

    # Authorize the client
    client = gspread.authorize(credentials)

    # Open the Google Sheet by ID
    spreadsheet_id = '1Q6JsSHXWimTiM2UTMMPISiQekib6B4S8x36B1ILw9N0' 
    spreadsheet = client.open_by_key(spreadsheet_id)

    # Select the worksheet where you want to insert data
    worksheet = spreadsheet.get_worksheet(1)  # Change index if needed

    # Execute the SQL query and fetch results into a Spark DataFrame
    query = """
    WITH shipper_mapping AS (
        SELECT * 
        FROM brokerageprod.gold.dim_shipper_mapping
    ),
    fact_load AS (
        SELECT 
            COUNT(Load_id) AS load_count,
            customer_master AS Customer_Master,
            Customer_Office AS Customer_Office,
            employee AS Employee,
            SourceSystem_Name AS Source_System_Name,
            salesrep AS Sales_Rep 
        FROM analytics.fact_load_genai_nonsplit 
        WHERE Ship_Date >= '2024-06-30' 
        GROUP BY 
            customer_master,
            Customer_Office,
            employee,
            SourceSystem_Name,
            salesrep
    ),
    missed_shippers AS (
        SELECT fact_load.* 
        FROM fact_load 
        LEFT JOIN shipper_mapping 
        ON LOWER(fact_load.customer_master) = LOWER(shipper_mapping.Customer_Master) 
        WHERE shipper_mapping.Customer_Master IS NULL 
        AND fact_load.Customer_Office != 'DBG' 
        AND fact_load.Customer_Master NOT LIKE 'NFI%' 
        AND fact_load.Customer_Master NOT LIKE 'NDC%' 
        AND fact_load.Customer_Master != 'ZEB'
    ),
    hubspot_mapping AS (
        SELECT 
            missed_shippers.*,
            hubspot_companies.name AS Hubspot_Company_Name,
            hubspot_companies.account_manager AS Hubspot_AM 
        FROM missed_shippers 
        LEFT JOIN bronze.hubspot_companies 
        ON LOWER(hubspot_companies.name) = LOWER(missed_shippers.customer_master) 
        AND hubspot_companies.SourcesystemID = '4' 
        AND hubspot_companies.archived = 'false'
    ),
    final_rank AS (
        SELECT 
            hubspot_mapping.*,
            DENSE_RANK() OVER(PARTITION BY Customer_Master ORDER BY load_count DESC) AS rank 
        FROM hubspot_mapping
    ),
    Final_shipper AS (
        SELECT * 
        FROM final_rank 
        WHERE rank = 1
    ),
    Hubspot_mapping as (SELECT 
        final_shipper.Customer_Master AS `Customer Master`, 
        final_shipper.Customer_Office AS `Customer Office`, 
        final_shipper.Employee AS `Shipper Rep`, 
        final_shipper.Source_System_Name AS `Source System Name`, 
        final_shipper.Sales_Rep AS `Sales Rep`,  
        relay_users.email_address AS `Email Address` 
    FROM Final_shipper 
    LEFT JOIN bronze.relay_users 
    ON final_shipper.Employee = relay_users.full_name),

    hubspot_companies as (select name,account_manager from bronze.hubspot_companies where SourcesystemID = '4'
    AND hubspot_companies.archived = 'false' 
    AND hubspot_companies.is_deleted = '0'
    )

    select hubspot_mapping.*, hubspot_companies.name as `Hubspot Company Name`, hubspot_companies.account_manager as `Hubspot AM` 
    from hubspot_mapping
    LEFT JOIN hubspot_companies
    ON lower(Hubspot_mapping.`Customer Master`) = lower(hubspot_companies.name)
    """
    df_spark = spark.sql(query)
    df_spark = df_spark.withColumn("Slack Channel Name", lit(None))
    df_spark = df_spark.withColumn("Vertical", lit(None))
    df_spark.createOrReplaceTempView("unmapped_Shippers")
    df = df_spark.toPandas()

except Exception as e:
  logger.error(f"Error in executing SQL query and fetching results into a Spark DataFrame: {e}")




# COMMAND ----------

try:
    # Convert DataFrame to a list of lists for gspread
    print("Executing gspread update and formatting the exception sheet")

    data_to_insert = [df.columns.values.tolist()] + df.values.tolist()
    worksheet.clear()  
    batch_requests = []

    # Add update request for data
    batch_requests.append({
        'range': 'A1',
        'values': data_to_insert
    })

    # Execute batch update
    try:
        worksheet.batch_update(batch_requests)
    except Exception as e:
        print(f"Error updating sheet: {e}")

    # Formatting the Google Sheet as a table
    header_range = f'A1:{chr(65 + len(df.columns) - 1)}1'  # Adjust column range based on number of columns

    # Function to format the header row
    def format_header():
        try:
            worksheet.format(header_range, {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.0, 'green': 0.5, 'blue': 0.5}  # Example color (teal)
            })
        except Exception as e:
            if "429" in str(e):
                print("Quota exceeded. Waiting for 60 seconds before retrying...")
                time.sleep(60)
                format_header()  # Retry formatting

    # Apply formatting to header
    format_header()

    # Apply borders around the data range
    data_range = f'A1:{chr(65 + len(df.columns) - 1)}{len(data_to_insert)}'  # Adjust row range based on number of rows

    def format_borders():
        try:
            worksheet.format(data_range, {
                'borders': {
                    'top': {'style': 'SOLID'},
                    'bottom': {'style': 'SOLID'},
                    'left': {'style': 'SOLID'},
                    'right': {'style': 'SOLID'},
                }
            })
        except Exception as e:
            if "429" in str(e):
                print("Quota exceeded. Waiting for 60 seconds before retrying...")
                time.sleep(60)
                format_borders()  # Retry formatting

    # Apply borders
    format_borders()

    # Optional: Set alternating row colors for better readability
    for i in range(2, len(data_to_insert) + 1):  # Start from row 2 (after header)
        try:
            if i % 2 == 0:
                worksheet.format(f'A{i}:{chr(65 + len(df.columns) - 1)}{i}', {
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}  # Light gray for even rows
                })
        except Exception as e:
            if "429" in str(e):
                print("Quota exceeded. Waiting for 60 seconds before retrying...")
                time.sleep(60)
                # Retry formatting the specific row (you may want to implement a more robust retry mechanism here)

    print("Data inserted and formatted successfully!")
except Exception as e:
  logger.error(f"Error in updating and formatting the Google Sheet: {e}")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lower, lit
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# Create credentials object
credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scopes=scope)

# Authorize the client
client = gspread.authorize(credentials)

# Open the Google Sheet by ID
spreadsheet_id = '1Q6JsSHXWimTiM2UTMMPISiQekib6B4S8x36B1ILw9N0' 
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
merge_sql = """
MERGE INTO bronze.shipper_mapping AS target
USING Mapped_shipper AS source
ON target.Merge_Key = source.merge_key
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

print("Data read from Google Sheet and merged with bronze.shipper_mapping successfully!")

# COMMAND ----------

update_is_deleted_sql = """
UPDATE bronze.shipper_mapping AS target
SET target.Is_deleted = 1
WHERE target.merge_key NOT IN (
    SELECT merge_key FROM mapped_shipper
)
"""

# Execute the update statement for is_deleted
spark.sql(update_is_deleted_sql)

print("Data read from Google Sheet and merged with bronze.shipper_mapping successfully!")

# COMMAND ----------


import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json

# Service account key file content
json_keyfile_content = r'''{
    "type": "service_account",
    "project_id": "nfi-awards-419411",
    "private_key_id": "dabbfaeafda0f91ef6e29edb347571d2eb62be4c",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCwUD5+dctB2yhP\nLMUVoIQrLLkCrS/rJ0ZCdS3GjO9sFunFE7SZxHelVy80gZ/gEOkjevv/6TsNk89e\nM6Cfc8BcZgZbnm3yvYu73oPo+BWqOmk3KT/5K83GSp9ooWwu6TN+F+Xqim/wvgSm\niUWYF/T9jDvwOzTE1mg0H+IFs+B0c9MHLno2/kco+MiHZIrVoWulGF3uUmGuL3nZ\nTp1O/TaawuHV8wFcuQOQtMeeoA2ZU4e7oMmM+JENyEJ9Yas94k/3plgRyf+hEKfZ\nQ3+3F6dAFT7dw9i/3rmQsEhYyawzrrDxy+Dcws90VMznZ1duqaoSTVZn2sA8O80b\naM2FliS7AgMBAAECggEAGyiuimNRunoW/6SJUqywVJ2QfYQ4/T5GWTFcqkOGaT4u\n/K/C4kF8c/3LOd7ScOeZ9LWv62U9YU4zTghM2z2vfsGBLuFnf/yeRtkJX7xrS0Fb\nzhFEF06d5XK+6GqefqwRxarKZ0ezjkAZo5+XQUzwwj0KSes3fy9PIOGyZXzQ++RO\noLuIdjRDIhTWdPv2WwTmcXmNuAKUmFonVxlIitwNqNwxrr83cVyhsV12jqfiBioP\nFF5aIdMCwyO1lRfBxabJnFhQcZAGSGY71nsz1BUiMFvOoOx/qRHWYByZP4rv8+4B\nYpi4T/7sbT3Rt7ClhNeoHANzkERpnVcSdfYpMZrQYQKBgQDixzkQo3vVq57g7UYh\nkPBJ8I42TvNKcvTxKKWrox9Ni2OwBXz1JO+d6n4zXV4/lUbDvkPiVDSM21mnbCV0\nk4tLX7j4ZPg58PvSLLiLca5KriiQa9HVUyBxhI7HVZjill4qAlfXmEM1x1v10h+T\nUouLo6mXgG41FVLYqpr++3hi4QKBgQDHCFSPySRFAqID7HkedwbQubnAl8cDG1NK\ns+6rGDSBasj9aGrESlBFHk/n7D5eSd5etGy1yOaXOzChyLApOTAh2Ytxo/2+bKtO\nl2Nu7H4LDPCGCeVAaSAhFtzLQT0QxA83X8odQeW9EFEcSnpSXA0FGDUQ25Cw7P+s\nPUsMoLGXGwKBgEYrCnchfpGQdqp2ADsmk5LtQbOAHjss8qkjwI8o++iMdp3iNNXN\nRe8AvWe7PgxCbhDm8C45i8EBpe3twnEdrf32ck85Pqz+6YwQllFfLWSiGp8FHXn6\nLJGzSFJZI+MIT76D1xY4YKNlOgkHqQl2gwMD8teTv4XhQS8VKrw2pvtBAoGBAJzs\nFXfUN5Ntcv2y+P+rCqWQeUK+p/rsFWyFlvwwtlz/K36YT+15RfZo4slRew7uILP8\nfuWIgz3jPgGgcDvgSfG2SnoLuOjlVt88/vma3fmqdwzHSofRGrLcCrL6OcI5QmH/\nVSRiK1c4QSsUEYNT7jQBFP24j0jfYumS3dQT8lDXAoGAVNCffySth9e7hgyVjlYf\nCQJ5LkJSWJPlf87h8Jx5vbkUkpeqy0Dbsa+SbFnOAm+xdITSu8R+myvNiUsN65el\nGZNZW5uhbVXL2TKGlguFtcwbRixJBryF91koP/4U+0AsORrK6DSFyK9WGZ6K38O2\nFF8Cvkt/NQtFXxgpdGMYqbY=\n-----END PRIVATE KEY-----\n",
    "client_email": "nfi-awards-email@nfi-awards-419411.iam.gserviceaccount.com",
    "client_id": "104433960422212318481",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/nfi-awards-email%40nfi-awards-419411.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}'''  # Keep your existing JSON content here

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

# Email details
sender = "gagana.nair@nfiindustries.com"
recipients = [
    # "parthasarathy.koteeswaran@nfiindustries.com"
    "gagana.nair@nfiindustries.com"
]
to = ', '.join(recipients)
subject = "📌Quick Check! Un-Mapped Entries in zeb’s Shipper Mapping Repo"

# Function to fetch shipper counts from the database
def fetch_shipper_counts():
    query = """
    SELECT Customer_Office, COUNT(*) as shipper_count 
    FROM unmapped_shippers 
    GROUP BY Customer_Office
    order by shipper_count desc
    """
    spark.sql(query)
    results = spark.sql(query).collect()
    return results

# Function to create the email body with shipper counts in table format
def create_email_body(shipper_counts):
    body = """
    <html>
      <body>
        <p>Hi team,</p>
        <p>Please find the list of Un-Mapped Shippers below,<br>
        Kindly refer the exception sheet and update the final sheet with Verified AM and office.</p>
        <p>You can access the Google Sheet using the below link:<br>
        <a href="https://docs.google.com/spreadsheets/d/1Q6JsSHXWimTiM2UTMMPISiQekib6B4S8x36B1ILw9N0/edit?gid=299326868#gid=299326868" 
           style="text-decoration: underline; color: #0000EE; display: inline;">Shipper Mapping</a></p>
        <p>Summary of Shippers by Office:</p>
        <table border="1" style="border-collapse: collapse; width: 30%;">
          <tr style="background-color: #BEBEBE;">
            <th style="padding: 5px;">Office Split</th>
            <th style="padding: 5px;">Count</th>
          </tr>
    """
    
    for office, count in shipper_counts:
        body += f"<tr><td style='padding: 5px;'>{office}</td><td style='padding: 5px;'>{count}</td></tr>"
    
    body += """
        </table>
        <p>Best Regards,<br>
        Gagana Nair</p>
      </body>
    </html>
    """
    
    return body

# Fetch shipper counts from the database
shipper_counts = fetch_shipper_counts()

# Create the email body with shipper counts
email_body = create_email_body(shipper_counts)

def create_message(sender, to, subject, message_text, is_html=False):
    message = MIMEMultipart()
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    msg = MIMEText(message_text, 'html' if is_html else 'plain')
    message.attach(msg)
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_message(service, user_id, email_subject, email_body):
    try:
        message = create_message(sender, to, email_subject, email_body, is_html=True)
        sent_message = service.users().messages().send(userId=user_id, body=message).execute()
        print(f"Message Id: {sent_message['id']}")
        return sent_message
    except Exception as error:
        print(f'An error occurred: {error}')
        return None

# Load credentials and build the Gmail API service
credentials_dict = json.loads(json_keyfile_content)
credentials = service_account.Credentials.from_service_account_info(
    credentials_dict, scopes=SCOPES, subject=sender
)
service = build('gmail', 'v1', credentials=credentials)

# Send the email
send_message(service, "me", subject, email_body)


# COMMAND ----------

import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json

# Service account key file content
json_keyfile_content = r'''{
                                "type": "service_account",
                                "project_id": "nfi-awards-419411",
                                "private_key_id": "dabbfaeafda0f91ef6e29edb347571d2eb62be4c",
                                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCwUD5+dctB2yhP\nLMUVoIQrLLkCrS/rJ0ZCdS3GjO9sFunFE7SZxHelVy80gZ/gEOkjevv/6TsNk89e\nM6Cfc8BcZgZbnm3yvYu73oPo+BWqOmk3KT/5K83GSp9ooWwu6TN+F+Xqim/wvgSm\niUWYF/T9jDvwOzTE1mg0H+IFs+B0c9MHLno2/kco+MiHZIrVoWulGF3uUmGuL3nZ\nTp1O/TaawuHV8wFcuQOQtMeeoA2ZU4e7oMmM+JENyEJ9Yas94k/3plgRyf+hEKfZ\nQ3+3F6dAFT7dw9i/3rmQsEhYyawzrrDxy+Dcws90VMznZ1duqaoSTVZn2sA8O80b\naM2FliS7AgMBAAECggEAGyiuimNRunoW/6SJUqywVJ2QfYQ4/T5GWTFcqkOGaT4u\n/K/C4kF8c/3LOd7ScOeZ9LWv62U9YU4zTghM2z2vfsGBLuFnf/yeRtkJX7xrS0Fb\nzhFEF06d5XK+6GqefqwRxarKZ0ezjkAZo5+XQUzwwj0KSes3fy9PIOGyZXzQ++RO\noLuIdjRDIhTWdPv2WwTmcXmNuAKUmFonVxlIitwNqNwxrr83cVyhsV12jqfiBioP\nFF5aIdMCwyO1lRfBxabJnFhQcZAGSGY71nsz1BUiMFvOoOx/qRHWYByZP4rv8+4B\nYpi4T/7sbT3Rt7ClhNeoHANzkERpnVcSdfYpMZrQYQKBgQDixzkQo3vVq57g7UYh\nkPBJ8I42TvNKcvTxKKWrox9Ni2OwBXz1JO+d6n4zXV4/lUbDvkPiVDSM21mnbCV0\nk4tLX7j4ZPg58PvSLLiLca5KriiQa9HVUyBxhI7HVZjill4qAlfXmEM1x1v10h+T\nUouLo6mXgG41FVLYqpr++3hi4QKBgQDHCFSPySRFAqID7HkedwbQubnAl8cDG1NK\ns+6rGDSBasj9aGrESlBFHk/n7D5eSd5etGy1yOaXOzChyLApOTAh2Ytxo/2+bKtO\nl2Nu7H4LDPCGCeVAaSAhFtzLQT0QxA83X8odQeW9EFEcSnpSXA0FGDUQ25Cw7P+s\nPUsMoLGXGwKBgEYrCnchfpGQdqp2ADsmk5LtQbOAHjss8qkjwI8o++iMdp3iNNXN\nRe8AvWe7PgxCbhDm8C45i8EBpe3twnEdrf32ck85Pqz+6YwQllFfLWSiGp8FHXn6\nLJGzSFJZI+MIT76D1xY4YKNlOgkHqQl2gwMD8teTv4XhQS8VKrw2pvtBAoGBAJzs\nFXfUN5Ntcv2y+P+rCqWQeUK+p/rsFWyFlvwwtlz/K36YT+15RfZo4slRew7uILP8\nfuWIgz3jPgGgcDvgSfG2SnoLuOjlVt88/vma3fmqdwzHSofRGrLcCrL6OcI5QmH/\nVSRiK1c4QSsUEYNT7jQBFP24j0jfYumS3dQT8lDXAoGAVNCffySth9e7hgyVjlYf\nCQJ5LkJSWJPlf87h8Jx5vbkUkpeqy0Dbsa+SbFnOAm+xdITSu8R+myvNiUsN65el\nGZNZW5uhbVXL2TKGlguFtcwbRixJBryF91koP/4U+0AsORrK6DSFyK9WGZ6K38O2\nFF8Cvkt/NQtFXxgpdGMYqbY=\n-----END PRIVATE KEY-----\n",
                                "client_email": "nfi-awards-email@nfi-awards-419411.iam.gserviceaccount.com",
                                "client_id": "104433960422212318481",
                                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                                "token_uri": "https://oauth2.googleapis.com/token",
                                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/nfi-awards-email%40nfi-awards-419411.iam.gserviceaccount.com",
                                "universe_domain": "googleapis.com"
                                  }'''  # Keep your existing JSON content here

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

# Email details
sender = "gagana.nair@nfiindustries.com"
recipients = [
    "parthasarathy.koteeswaran@nfiindustries.com"
]
to = ', '.join(recipients)
subject = "Update Required in Zeb Shipper Mapping Repository"

email_body = """
<html>
  <body>
    <p>Hi team,
    <p>
      Please find the list of Un-Mapped Shippers below,<br>
      Kindly refer the exception sheet and update the final sheet with Verified AM and office.
    </p>
    <p>
      You can access the Google Sheet using the below link:<br>
      <a href="https://docs.google.com/spreadsheets/d/1Q6JsSHXWimTiM2UTMMPISiQekib6B4S8x36B1ILw9N0/edit?gid=299326868#gid=299326868" 
         style="text-decoration: underline; color: #0000EE; display: inline;">Shipper Mapping
    </a>
    </p>
    <p>
      Best Regards,<br>
      Gagana Nair
    </p>
  </body>
</html>
"""


def create_message(sender, to, subject, message_text, is_html=False):
    message = MIMEMultipart()
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    msg = MIMEText(message_text, 'html' if is_html else 'plain')
    message.attach(msg)
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_message(service, user_id, email_subject, email_body):
    try:
        message = create_message(sender, to, email_subject, email_body, is_html=True)
        sent_message = service.users().messages().send(userId=user_id, body=message).execute()
        print(f"Message Id: {sent_message['id']}")
        return sent_message
    except Exception as error:
        print(f'An error occurred: {error}')
        return None

# Load credentials and build the Gmail API service
credentials_dict = json.loads(json_keyfile_content)
credentials = service_account.Credentials.from_service_account_info(
    credentials_dict, scopes=SCOPES, subject=sender
)
service = build('gmail', 'v1', credentials=credentials)

# Send the email
send_message(service, "me", subject, email_body)

# COMMAND ----------


import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json

# Service account key file content
json_keyfile_content = r'''{
    "type": "service_account",
    "project_id": "nfi-awards-419411",
    "private_key_id": "dabbfaeafda0f91ef6e29edb347571d2eb62be4c",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCwUD5+dctB2yhP\nLMUVoIQrLLkCrS/rJ0ZCdS3GjO9sFunFE7SZxHelVy80gZ/gEOkjevv/6TsNk89e\nM6Cfc8BcZgZbnm3yvYu73oPo+BWqOmk3KT/5K83GSp9ooWwu6TN+F+Xqim/wvgSm\niUWYF/T9jDvwOzTE1mg0H+IFs+B0c9MHLno2/kco+MiHZIrVoWulGF3uUmGuL3nZ\nTp1O/TaawuHV8wFcuQOQtMeeoA2ZU4e7oMmM+JENyEJ9Yas94k/3plgRyf+hEKfZ\nQ3+3F6dAFT7dw9i/3rmQsEhYyawzrrDxy+Dcws90VMznZ1duqaoSTVZn2sA8O80b\naM2FliS7AgMBAAECggEAGyiuimNRunoW/6SJUqywVJ2QfYQ4/T5GWTFcqkOGaT4u\n/K/C4kF8c/3LOd7ScOeZ9LWv62U9YU4zTghM2z2vfsGBLuFnf/yeRtkJX7xrS0Fb\nzhFEF06d5XK+6GqefqwRxarKZ0ezjkAZo5+XQUzwwj0KSes3fy9PIOGyZXzQ++RO\noLuIdjRDIhTWdPv2WwTmcXmNuAKUmFonVxlIitwNqNwxrr83cVyhsV12jqfiBioP\nFF5aIdMCwyO1lRfBxabJnFhQcZAGSGY71nsz1BUiMFvOoOx/qRHWYByZP4rv8+4B\nYpi4T/7sbT3Rt7ClhNeoHANzkERpnVcSdfYpMZrQYQKBgQDixzkQo3vVq57g7UYh\nkPBJ8I42TvNKcvTxKKWrox9Ni2OwBXz1JO+d6n4zXV4/lUbDvkPiVDSM21mnbCV0\nk4tLX7j4ZPg58PvSLLiLca5KriiQa9HVUyBxhI7HVZjill4qAlfXmEM1x1v10h+T\nUouLo6mXgG41FVLYqpr++3hi4QKBgQDHCFSPySRFAqID7HkedwbQubnAl8cDG1NK\ns+6rGDSBasj9aGrESlBFHk/n7D5eSd5etGy1yOaXOzChyLApOTAh2Ytxo/2+bKtO\nl2Nu7H4LDPCGCeVAaSAhFtzLQT0QxA83X8odQeW9EFEcSnpSXA0FGDUQ25Cw7P+s\nPUsMoLGXGwKBgEYrCnchfpGQdqp2ADsmk5LtQbOAHjss8qkjwI8o++iMdp3iNNXN\nRe8AvWe7PgxCbhDm8C45i8EBpe3twnEdrf32ck85Pqz+6YwQllFfLWSiGp8FHXn6\nLJGzSFJZI+MIT76D1xY4YKNlOgkHqQl2gwMD8teTv4XhQS8VKrw2pvtBAoGBAJzs\nFXfUN5Ntcv2y+P+rCqWQeUK+p/rsFWyFlvwwtlz/K36YT+15RfZo4slRew7uILP8\nfuWIgz3jPgGgcDvgSfG2SnoLuOjlVt88/vma3fmqdwzHSofRGrLcCrL6OcI5QmH/\nVSRiK1c4QSsUEYNT7jQBFP24j0jfYumS3dQT8lDXAoGAVNCffySth9e7hgyVjlYf\nCQJ5LkJSWJPlf87h8Jx5vbkUkpeqy0Dbsa+SbFnOAm+xdITSu8R+myvNiUsN65el\nGZNZW5uhbVXL2TKGlguFtcwbRixJBryF91koP/4U+0AsORrK6DSFyK9WGZ6K38O2\nFF8Cvkt/NQtFXxgpdGMYqbY=\n-----END PRIVATE KEY-----\n",
    "client_email": "nfi-awards-email@nfi-awards-419411.iam.gserviceaccount.com",
    "client_id": "104433960422212318481",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/nfi-awards-email%40nfi-awards-419411.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}'''  # Keep your existing JSON content here

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

# Email details
sender = "gagana.nair@nfiindustries.com"
recipients = [
    # "parthasarathy.koteeswaran@nfiindustries.com"
    "gagana.nair@nfiindustries.com"
]
to = ', '.join(recipients)
subject = "📌Quick Check! Un-Mapped Entries in zeb’s Shipper Mapping Repo"

# Function to fetch shipper counts from the database
def fetch_shipper_counts():
    query = """
    SELECT Customer_Office, COUNT(*) as shipper_count 
    FROM unmapped_shippers 
    GROUP BY Customer_Office
    ORDER BY shipper_count DESC
    """
    spark.sql(query)
    results = spark.sql(query).collect()
    return results

# Function to create the email body with shipper counts in table format
def create_email_body(shipper_counts):
    total_shippers = sum(count for _, count in shipper_counts)  # Calculate total shippers

    body = f"""
    <html>
      <body>
        <p>Hi team,</p>
        <p>Please find the list of Un-Mapped Shippers below, Refer the exception sheet and update the final sheet with Verified AM and Office.</p>
        <p>You can access the Google Sheet using the below link:<br>
        <a href="https://docs.google.com/spreadsheets/d/1Q6JsSHXWimTiM2UTMMPISiQekib6B4S8x36B1ILw9N0/edit?gid=299326868#gid=299326868" 
           style="text-decoration: underline; color: #0000EE; display: inline;">Shipper Mapping</a></p>
        Total Un-Mapped Shippers Count: <strong>{total_shippers}</strong></p><br>  <!-- Display total shippers count -->
        Summary of Un-Mapped Shippers by Office:</p>
        <p><table border="1" style="border-collapse: collapse; width: 30%;">
          <tr style="background-color: #BEBEBE;">
            <th style="padding: 5px;">Office Split</th>
            <th style="padding: 5px;">Count</th>
          </tr>
    """
    
    for office, count in shipper_counts:
        body += f"<tr><td style='padding: 5px;'>{office}</td><td style='padding: 5px;'>{count}</td></tr>"
    
    body += """
        </table>
        <p>Best Regards,<br>
        Gagana Nair</p>
      </body>
    </html>
    """
    
    return body

# Fetch shipper counts from the database
shipper_counts = fetch_shipper_counts()

# Create the email body with shipper counts
email_body = create_email_body(shipper_counts)

def create_message(sender, to, subject, message_text, is_html=False):
    message = MIMEMultipart()
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    msg = MIMEText(message_text, 'html' if is_html else 'plain')
    message.attach(msg)
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_message(service, user_id, email_subject, email_body):
    try:
        message = create_message(sender, to, email_subject, email_body, is_html=True)
        sent_message = service.users().messages().send(userId=user_id, body=message).execute()
        print(f"Message Id: {sent_message['id']}")
        return sent_message
    except Exception as error:
        print(f'An error occurred: {error}')
        return None

# Load credentials and build the Gmail API service
credentials_dict = json.loads(json_keyfile_content)
credentials = service_account.Credentials.from_service_account_info(
    credentials_dict, scopes=SCOPES, subject=sender
)
service = build('gmail', 'v1', credentials=credentials)

# Send the email
send_message(service, "me", subject, email_body)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_shipper_mapping where Customer_Master = "LESLIE'S POOLMART  INC"

# COMMAND ----------

# MAGIC %sql
# MAGIC with load as (select count(distinct(load_id))as load_count,Customer_Master,Customer_Office,Employee,SourceSystem_Name from analytics.fact_load_genai_nonsplit where ship_date >= '2024-06-30' and Ship_Date <= '2025-06-04'and Customer_Office != 'DBG' group by Customer_Master,customer_office,Employee,SourceSystem_Name ),
# MAGIC ranked as (select *, dense_rank() over(partition by Customer_Master order by load_count desc ) as rank from load),
# MAGIC unmapped as (select * from ranked left join gold.dim_shipper_mapping on upper(ranked.Customer_Master) = upper(dim_shipper_mapping.Customer_Master) where rank = '1' and dim_shipper_mapping.Customer_Master is null and 
# MAGIC ranked.Customer_Master NOT LIKE 'NFI%' AND ranked.Customer_Master NOT LIKE 'NDC%'  AND ranked.Customer_Master != 'ZEB')
# MAGIC select * from  ranked left join gold.dim_shipper_mapping on upper(ranked.Customer_Master) = upper(dim_shipper_mapping.Customer_Master) where rank = '1' and upper(ranked.Customer_Master) = upper(dim_shipper_mapping.Customer_Master)  and upper(dim_shipper_mapping.Verified_Office) != upper(ranked.customer_office) and 
# MAGIC ranked.Customer_Master NOT LIKE 'NFI%' AND ranked.Customer_Master NOT LIKE 'NDC%' AND ranked.Customer_Master != 'ZEB' and Verified_Office != 'NA' and Verified_AM != 'NA'
# MAGIC

# COMMAND ----------

