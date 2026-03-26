# Databricks notebook source
# MAGIC %md
# MAGIC ##IMPORTING PACKAGES

# COMMAND ----------

from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from pyspark.sql.functions import lit, col
import time
import gspread
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lower, lit
import pandas as pd
from pyspark.sql.functions import concat_ws, when, lit


# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ##CALLING RESPECTIVE CATALOG

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##CREATING ERROR LOGGER VARIABLES

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("NFI_Shipper_Mapping_Mail_Automation")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ##COLLECTING SECRET FROM KEYVAULT

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
# MAGIC ##CREATING CREDS AND SCOPES

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
# MAGIC ##ERROR LOGGER TABLE VARIABLES

# COMMAND ----------

try:
    Job_ID = "SM-Mail"
    Notebook_ID = "SM-Weekly Mail Automation"
    Zone = "Bronze"
    Table_ID= 'SM-Mail'
    Table_Name= 'Null'
except Exception as e:
    logger.info("Unable to create Variables for Error Logs")
    print(e)


# COMMAND ----------

# MAGIC %md
# MAGIC ##RETRIEVEING UM-MAPPED SHIPPERS

# COMMAND ----------

try:    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key("1T6Hz3OjbCGymwuO1_pGDs2Qd8Oo29egSE9A7np4BduE")
    query = """
    WITH shipper_mapping AS (
        SELECT * 
        FROM brokerageprod.silver.silver_shipper_mapping
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
    df_spark = spark.sql(query) \
        .withColumn("Slack Channel Name", lit(None)) \
        .withColumn("Vertical", lit(None))

    # 3. Load third sheet for existing movers
    worksheets = spreadsheet.worksheets()

    # Check if the third worksheet exists
    if len(worksheets) > 2:
        worksheet3 = worksheets[2]
        records = worksheet3.get_all_records()

        if records:  # If the sheet has data
            df3 = pd.DataFrame(records)
            df3_spark = spark.createDataFrame(df3)

            # Add merge keys
            df_main = df_spark.withColumn(
                "merge_key",
                concat_ws("|", col('Customer Master'), col('Customer Office'), col('Shipper Rep'))
            )
            df_sheet3 = df3_spark.withColumn(
                "merge_key",
                concat_ws("|", df3_spark["Customer Master"], df3_spark["Verified Office"], df3_spark["Verified AM"])
            )

            # Join and flag
            df_joined = df_main.join(df_sheet3.select("merge_key"), on="merge_key", how="left")
            df_flagged = df_joined.withColumn(
                "Is_Moved",
                when(df_sheet3["merge_key"].isNotNull(), lit(1)).otherwise(lit(0))
            )
        else:
            # Sheet exists but is empty
            df_flagged = df_spark.withColumn("Is_Moved", lit(0))
    else:
        # Sheet does not exist
        df_flagged = df_spark.withColumn("Is_Moved", lit(0))

    # Continue with filtering and conversion
    df_unmapped = df_flagged.filter(df_flagged.Is_Moved == 0)
    df_unmapped = df_unmapped.withColumn(
      "merge_key",
      concat_ws("_", lower(col("Customer Master")), lower(col("Shipper Rep")), lower(col("Customer Office"))) )
    df_unmapped.createOrReplaceTempView("unmapped_Shippers")
    df = df_unmapped.toPandas()
except Exception as e:
    Error_Statement = f"Error-{e}"
    raise RuntimeError(f"Job failed: {Error_Statement}")
    print(f"Error: {e}")



# COMMAND ----------

# MAGIC %md
# MAGIC ##UPDATING EXCEPTIONS TABLE

# COMMAND ----------

try:
  merge_sql = f"""
    MERGE INTO bronze.Exception_Shipper AS target
    USING unmapped_shippers AS source
    ON target.Merge_Key = source.merge_Key
    WHEN MATCHED THEN
      UPDATE SET 
        target.Customer_Master = source.`Customer Master`,
        target.Shipper_Rep = source.`Shipper Rep`,
        target.Customer_Office = source.`Customer Office`,
        target.Sales_Rep = source.`Sales Rep`,
        target.Email_Address = source.`Email Address`,
        target.Hubspot_Company_Name = source.`Hubspot Company Name`,
        target.Hubspot_AM = source.`Hubspot AM`,
        target.Slack_Channel_Name = source.`Slack Channel Name`,
        target.Vertical = source.`Vertical`,
        target.Merge_Key = source.merge_key,
        target.Inserted_At = current_timestamp()
    WHEN NOT MATCHED THEN
      INSERT (
        Customer_Master, Shipper_Rep, Customer_Office, Sales_Rep, Email_Address,
        Hubspot_Company_Name, Hubspot_AM, Slack_Channel_Name, Vertical,
        Merge_Key, Inserted_At
      )
      VALUES (
        source.`Customer Master`, source.`Shipper Rep`,source.`Customer Office`, source.`Sales Rep`,
        source.`Email Address`, source.`Hubspot Company Name`, source.`Hubspot AM`,
        source.`Slack Channel Name`, source.`Vertical`, source.merge_key, current_timestamp()
      )
    """
  spark.sql(merge_sql)
except Exception as e:
  Error_Statement = f"Error-{e}"
  raise RuntimeError(f"Job failed: {Error_Statement}")
  print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##FORMATTING THE REULT

# COMMAND ----------

try:

    print("Executing gspread update and formatting the exception sheet")
    # Select the worksheet where you want to insert data
    worksheet = spreadsheet.get_worksheet(1) 
    df_to_upload = df.drop(columns=["Is_Moved"], errors="ignore")
    df_to_upload = df_to_upload.drop(columns=["merge_key"], errors="ignore")
    data_to_insert = [df_to_upload.columns.values.tolist()] + df_to_upload.values.tolist()
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
    header_range = f'A1:{chr(65 + len(df_to_upload.columns) - 1)}1'  # Use df_to_upload

    def format_header():
        try:
            worksheet.format(header_range, {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.0, 'green': 0.5, 'blue': 0.5}
            })
        except Exception as e:
            if "429" in str(e):
                print("Quota exceeded. Waiting for 60 seconds before retrying...")
                time.sleep(60)
                format_header()

    format_header()

    # Apply borders around the data range
    data_range = f'A1:{chr(65 + len(df_to_upload.columns) - 1)}{len(data_to_insert)}'  # Use df_to_upload

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
                format_borders()

    format_borders()

    # Alternating row colors
    for i in range(2, len(data_to_insert) + 1):
        try:
            if i % 2 == 0:
                worksheet.format(f'A{i}:{chr(65 + len(df_to_upload.columns) - 1)}{i}', {
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                })
        except Exception as e:
            if "429" in str(e):
                print("Quota exceeded. Waiting for 60 seconds before retrying...")
                time.sleep(60)
    print("Data inserted and formatted successfully!")
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Succeeded","NULL","NULL",0,"Updated Exception Sheet")
except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,"Failed",Error_Statement,"Impactable Issue",1,"Unable to update exception sheet")
    raise RuntimeError(f"Job failed: {Error_Statement}")
    print(f"Error: {e}")
    logger.error(f"Error in updating and formatting the Google Sheet: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##SENDING MAIL TO AM

# COMMAND ----------

# Retrieve the secrets from Azure Key Vault
try:
    ProjectID = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="AwardsProjectID")
    PrivateKeyID = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="AwardsPrivateKeyID")
    ClientEmail = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="AwardsClientServiceEmail")
    ClientID = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="AwardsClientID")
    PrivateKey = dbutils.secrets.get(scope="NFI_Awards_Secrets", key="GoogleSPKey")
except Exception as e:
    logger.info("unable to fetch creds from azure keyvault")
    print(e)

# COMMAND ----------

PrivateKey = PrivateKey.replace("\\n", "\n") 

# Construct the JSON key file content
json_keyfile_content = json.dumps({
    "type": "service_account",
    "project_id": ProjectID,
    "private_key_id": PrivateKeyID,
    "private_key": PrivateKey,  # Ensure this is formatted correctly now
    "client_email": "nfi-awards-email@nfi-awards-419411.iam.gserviceaccount.com",
    "client_id": ClientID,
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/nfi-awards-email@nfi-awards-419411.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
})

# COMMAND ----------

import base64
try:
    sender = dbutils.secrets.get(scope = "NFI_Awards_Secrets", key = "ValidationMailSender")
    recipients = dbutils.secrets.get(scope = "NFI_Awards_Secrets", key = "ShipperMappingReceiptents")

    # Define the scope
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']

    to = ', '.join(recipients.split(","))

    subject = "Remainder! Un-Mapped Entries in NFI Shipper Mapping Repo"

    def sum_shipper_counts_from_view():
        query = """SELECT count(`customer Master`) as total_shipper_count FROM unmapped_shippers"""
        result = spark.sql(query).collect()[0]
        return result['total_shipper_count']

    def get_shipper_counts_by_office():
        query = """SELECT `Customer Office` as Office , count(*) as count FROM unmapped_shippers GROUP BY `Customer Office`
        order by count desc """
        results = spark.sql(query).collect()
        return [(row['Office'], row['count']) for row in results]


    def create_email_body(total, shipper_counts):
        total_shippers = total

        body = f"""
        <html>
        <body>
            <p>Hi team,</p>
            <p>Attaching the list of Un-Mapped Shippers below, Kindly refer the exception sheet and update the final sheet with Verified AM, Office, Slack and Hubspot details.</p>
            <p>You can access the Google Sheet using the below link:<br>
            <a href="https://docs.google.com/spreadsheets/d/1T6Hz3OjbCGymwuO1_pGDs2Qd8Oo29egSE9A7np4BduE/edit?usp=sharing" 
            style="text-decoration: underline; color: #0000EE; display: inline;">Shipper Mapping</a></p>
            <em><strong>Note</strong> : Total Un-Mapped Shippers Count : <strong>{total_shippers}</strong></em></p><br>
            <p>Summary of Un-Mapped Shippers by Office:</p>
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
            <p>Please let us know if you have any questions.</p>
            <p>Best Regards,<br>
            Gagana Nair</p>
        </body>
        </html>
        """
        
        return body

    total = sum_shipper_counts_from_view()
    office_counts = get_shipper_counts_by_office()


    # Create the email body with shipper counts
    email_body = create_email_body(total,office_counts )

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
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, Table_Name, Zone, "Succeeded", "NULL", "NULL", 0, "Automation Mail sent")
    print("Email sent successfully!")
except Exception as e:
    Error_Statement = str(e).replace("'", "''")
    raise RuntimeError(f"Job failed: {Error_Statement}")
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, Table_Name, Zone, "Failed", Error_Statement, "Impactable Issue", 1, "Issue in Automation Mail")

