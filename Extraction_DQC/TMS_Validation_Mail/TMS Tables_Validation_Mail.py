# Databricks notebook source
# MAGIC %md
# MAGIC ##Source-Bronze Row Count Validation
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Packages

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
from functools import *
from datetime import *
from pyspark.sql import SparkSession
from delta.tables import *
from pyspark.sql.window import Window
import pytz
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import re
import json
import pandas
from pyspark.sql import Row
import smtplib
import base64
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from io import BytesIO
import os
from pyspark.sql.functions import col, abs, when, sum as spark_sum


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define UDFs 

# COMMAND ----------

# Function to get column count and row count for a table
def get_table_info(table_name):
  row_count_query = f"(SELECT count(*) as row_count FROM {table_name}) as row_count_query"
  column_count_query = f"""(SELECT table_name, count(*) as column_count 
                            FROM information_schema.columns 
                            WHERE table_name = '{table_name}' 
                            GROUP BY table_name) as column_count_query"""
  
  row_df = spark.read.jdbc(url=jdbcUrl, table=row_count_query, properties=connectionProperties)
  column_df = spark.read.jdbc(url=jdbcUrl, table=column_count_query, properties=connectionProperties)
  
  row_count = row_df.collect()[0]['row_count'] if row_df.collect() else 0
  column_count = column_df.collect()[0]['column_count'] if column_df.collect() else 0
  
  return (table_name, column_count, row_count)


def create_html_table_count(data):
    """Create an HTML table from the data."""
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Data Comparisons Report</title>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
            }
            th, td {
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
        </style>
    </head>
    <body>
        <table>
            <thead>
                <tr>
                   <th>TableName</th>
                    <th>Source_Row_Count</th>
                    <th>Bronze_Row_Count</th>
                    <th>Count_Difference</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
    """
    # Skip the first two lines (title and header)
    for line in data.split('\n')[3:]:
        if line.strip():  # Check if the line is not empty
            # Split the line into columns
            columns = line.split(None, 4)  # Split by whitespace, but only for the first 4 splits
            if len(columns) == 5:
                html += "<tr>"
                for cell in columns:
                    html += f"<td>{cell}</td>"
                html += "</tr>"
    html += """
            </tbody>
        </table>
    </body>
    </html>
    """
    return html

def create_message(sender, to, subject, message_text, is_html=False):
    """Create a message for an email."""
    message = MIMEMultipart()
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject

    if is_html:
        msg = MIMEText(message_text, 'html')
    else:
        msg = MIMEText(message_text)
    message.attach(msg)
    
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_message_count(service, user_id, email_subject, email_body):
    """Send an email message."""
    try:

        html_table = create_html_table_count(email_body)
        html_content = f"""
        <html>
        <body>
        <p>Dear Recipient,</p>
        <p>The following data comparisons have failed:</p>
        {html_table}
        <p>Best Regards,<br>Gagana Nair.<br>Thanks!</p>
        </body>
        </html>
        """

        message = create_message(sender, to, email_subject, html_content, is_html=True)
        sent_message = service.users().messages().send(userId=user_id, body=message).execute()
        print(f"Message Id: {sent_message['id']}")
        return sent_message
    except Exception as error:
        print(f'An error occurred: {error}')
        return None


def create_html_table_run(data):
    """Create an HTML table from the data."""
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Data Comparisons Report</title>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
            }
            th, td {
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
        </style>
    </head>
    <body>
        <table>
            <thead>
                <tr>
                  <th>DW_Log_ID</th>
                  <th>Job_ID</th>
                  <th>Notebook_ID</th>
                  <th>Table_Name</th>
                  <th>Zone</th>
                  <th>Run_Status</th>
                  <th>Error_Statement</th>
      
                </tr>
            </thead>
            <tbody>
    """
    # Skip the first two lines (title and header)
    for line in data.split('\n')[3:]:
        if line.strip():  # Check if the line is not empty
            # Split the line into columns
            columns = line.split(None, 6)  # Split by whitespace, but only for the first 4 splits
            if len(columns) == 7:
                html += "<tr>"
                for cell in columns:
                    html += f"<td>{cell}</td>"
                html += "</tr>"
    html += """
            </tbody>
        </table>
    </body>
    </html>
    """
    return html


def send_message_run(service, user_id,email_subject, email_body):
    """Send an email message."""
    try:
        if email_body == "No Failures Today":
            html_content = f"""
            <html>
            <body>
            <p>Dear Recipient,</p>
            <p>{email_body}</p>
            <p>Best regards,<br>parthasarathy K<br>Thanks</p>
            </body>
            </html>
            """
        else:
            html_table = create_html_table_run(email_body)
            html_content = f"""
            <html>
            <body>
            <p>Dear Recipient,</p>
            <p>The following data comparisons have failed:</p>
            {html_table}
            <p>Best Regards,<br>Gagana Nair<br>Thanks</p>
            </body>
            </html>
            """

        message = create_message(sender, to, email_subject, html_content, is_html=True)
        sent_message = service.users().messages().send(userId=user_id, body=message).execute()
        print(f"Message Id: {sent_message['id']}")
        return sent_message
    except Exception as error:
        if hasattr(error, 'res') and 'error' in error.res:
            print(f"Error details: {error.res['error']}")
        return None


def get_failed_pipelines(start_date, end_date):
    """Fetch failed pipelines from the log table for the given date range."""
    query = f""" select DW_Log_ID,Job_ID,Notebook_ID,Table_Name,Zone,Run_Status,Error_Statement,Error_Type,Instance from metadata.error_log_table where  Run_Status IN ("Failed", "Attention Required") and Error_Type!='Ignorable'
    AND Created_At BETWEEN '{start_date}' AND '{end_date}'
    """
    Log_DF = spark.sql(query)
    return Log_DF



# COMMAND ----------

# MAGIC %md
# MAGIC ##Source Connection - Postgres

# COMMAND ----------

try:
  # Define the database connection parameters
  jdbcHostname = dbutils.secrets.get(scope="NFI_PostgreSQL_Secrets", key="PostgresqlHN")
  jdbcPort = dbutils.secrets.get(scope="NFI_PostgreSQL_Secrets", key="PostgresqlPortNumber")
  jdbcDatabase = dbutils.secrets.get(scope="NFI_PostgreSQL_Secrets", key="PostgresqlDBName")
  driver = "org.postgresql.Driver"
  jdbcUrl = f"jdbc:postgresql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"
  username = dbutils.secrets.get(scope="NFI_PostgreSQL_Secrets", key="PostgresqlUN")
  password = dbutils.secrets.get(scope="NFI_PostgreSQL_Secrets", key="PostgresqlPass")

  connectionProperties = {
    "user" : username,
    "password" :password,
    "driver" : driver
  }
except Exception as e:
  print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Source Count

# COMMAND ----------

tables = [
  'projection_carrier',
  'projection_load',
  'projection_invoicing',
  'projection_invoicing_audit',
  'carrier_nomination_and_onboarding_projection',
  'shippers',
  'plan_combination_projection',
  'receivers',
  'ltl_invoice_projection',
  'offer_negotiation_reflected',
  'carrier_projection',
  'planning_note_captured',
  'sourcing_max_buy_v2',
  'planning_current_assignment',
  'carrier_load_money_projection',
  'delivery_projection',
  'pickup_projection',
  'tracking_last_reason_code',
  'tendering_service_line',
  'tendering_planned_distance',
  'truckload_projection',
  'canonical_plan_projection',
  'rolled_loads',
  'hain_tracking_accuracy_projection',
  'booking_projection',
  'customer_distance_projection',
  'tendering_orders_product_descriptions',
  'tender_reference_numbers_projection',
  'big_export_projection',
  'tendering_acceptance',
  'planning_stop_schedule',
  'canonical_stop',
  'tracking_activity_v2',
  'customer_money',
  'carrier_money_projection',
  'integration_tracking_notification_triggered',
  'tender_split_projection',
  'tender_shipper_address_projection',
  'max_buy_projection',
  'integration_tender_mapped_projection_v2',
  'shipper_in_out_projection',
  'target_customer_money',
  'carrier_projection_new',
  'tmw_pro_projection',
  # 'spot_quotes',
  'equip_mode_table',
  'aljex_mode_types',
  'customers',
  'aljex_customer_profiles',
  'aljex_dot_lawson_ref',
  'aljex_cred_debt',
  'aljex_invoice',
  'cai_mode_lookup',
  'cai_salesperson_lookup',
  'edge_mode',
  'cai_equipment',
  'ultipro_terms',
  'ultipro_list',
  'cai_data',
  'event_name_status',
  'customer_profile_external_ids',
  'customer_profile_projection',
  'relay_users',
  'invoicing_customer_profile',
  'offer_negotiation_invalidated',
  'tl_invoice_projection',
  'tendering_tender_draft_id',
  'ap_lawson_appt',
  'distance_projection',
  'target_audit_lookup',
  'tendering_manual_tender_entry_usage',
  'invoicing_invoice_eligibility',
  'integration_hubtran_vendor_invoice_approved',
  'planning_team_driver_marked',
  'invoicing_credits',
  'tracking_in_transit_reason_code',
  'tendering_origin_destination',
  'invoicing_invoice_charges',
  'planning_assignment_log',
  'booking_carrier_planned',
  'vendor_transaction_projection',
  'moneying_billing_party_transaction',
  'new_brokerage_budget',
  'am_sales_lookup',
  'integration_p44_create_shipment_succeeded',
  'invoicing_invoices',
  'tendering_tendered_assigned_carrier',
  'integration_standardized_distance',
  'aljex_am_by_acct',
  'transfix_data',
  'carrier_profile',
  'fourkite_carriers',
  'planning_schedule_activity'

]


# COMMAND ----------

# Define the schema explicitly
schema = StructType([
  StructField("table_name", StringType(), True),
  StructField("column_count", IntegerType(), True),
  StructField("row_count", IntegerType(), True)
])

# Create an empty DataFrame with the defined schema
table_info_df = spark.createDataFrame([], schema)

# Iterate over each table and collect information
for table in tables:
  table_info = get_table_info(table)
  table_info_df = table_info_df.union(spark.createDataFrame([table_info], schema))

# Displaying the results
display(table_info_df)
table_info_df.createOrReplaceTempView("Source_Count")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Bronze Count

# COMMAND ----------

# Initialize Spark session
tables = [
  'projection_carrier',
  'projection_load_1',
  'Projection_Load_2',
  'projection_invoicing',
  'projection_invoicing_audit',
  'carrier_nomination_and_onboarding_projection',
  'shippers',
  'plan_combination_projection',
  'receivers',
  'ltl_invoice_projection',
  'offer_negotiation_reflected',
  'carrier_projection',
  'planning_note_captured',
  'sourcing_max_buy_v2',
  'planning_current_assignment',
  'carrier_load_money_projection',
  'delivery_projection',
  'pickup_projection',
  'tracking_last_reason_code',
  'tendering_service_line',
  'tendering_planned_distance',
  'truckload_projection',
  'canonical_plan_projection',
  'rolled_loads',
  'hain_tracking_accuracy_projection',
  'booking_projection',
  'customer_distance_projection',
  'tendering_orders_product_descriptions',
  'tender_reference_numbers_projection',
  'big_export_projection',
  'tendering_acceptance',
  'planning_stop_schedule',
  'canonical_stop',
  'tracking_activity_v2',
  'customer_money',
  'carrier_money_projection',
  'integration_tracking_notification_triggered',
  'tender_split_projection',
  'tender_shipper_address_projection',
  'max_buy_projection',
  'integration_tender_mapped_projection_v2',
  'shipper_in_out_projection',
  'target_customer_money',
  'carrier_projection_new',
  'tmw_pro_projection',
  'equip_mode_table',
  'aljex_mode_types',
  'customers',
  'aljex_customer_profiles',
  'aljex_dot_lawson_ref',
  'aljex_cred_debt',
  'aljex_invoice',
  'cai_mode_lookup',
  'cai_salesperson_lookup',
  'edge_mode',
  'cai_equipment',
  'ultipro_terms',
  'ultipro_list',
  'cai_data',
  'event_name_status',
  'customer_profile_external_ids',
  'customer_profile_projection',
  'relay_users',
  'invoicing_customer_profile',
  'offer_negotiation_invalidated',
  'tl_invoice_projection',
  'tendering_tender_draft_id',
  'ap_lawson_appt',
  'distance_projection',
  'target_audit_lookup',
  'tendering_manual_tender_entry_usage',
  'invoicing_invoice_eligibility',
  'integration_hubtran_vendor_invoice_approved',
  'planning_team_driver_marked',
  'invoicing_credits',
  'tracking_in_transit_reason_code',
  'tendering_origin_destination',
  'invoicing_invoice_charges',
  'planning_assignment_log',
  'booking_carrier_planned',
  'vendor_transaction_projection',
  'moneying_billing_party_transaction',
  'new_brokerage_budget',
  'am_sales_lookup',
  'integration_p44_create_shipment_succeeded',
  'invoicing_invoices',
  'tendering_tendered_assigned_carrier',
  'integration_standardized_distance',
  'aljex_am_by_acct',
  'transfix_data',
  'Carrier_Profile',
  'fourkite_carriers',
  'Planning_Schedule_Activity'
]

# COMMAND ----------

schema = StructType([
  StructField("table_name", StringType(), True),
  StructField("column_count", IntegerType(), True),
  StructField("row_count", IntegerType(), True)
])

table_info_df = spark.createDataFrame([], schema)

# Iterate over each table and collect information
for table in tables:
    table_full_name = f"bronze.{table}"
    
    # Get row count
    df = spark.table(table_full_name)
    row_count = df.count()
    
    # Get column count
    column_count = len(df.columns)
    
    # Append results to the DataFrame
    table_info = [(table, column_count, row_count)]
    table_info_df = table_info_df.union(spark.createDataFrame(table_info, schema))

# Display the results
display(table_info_df)
table_info_df.createOrReplaceTempView("Bronze_Info")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Comparision between Source and Bronze

# COMMAND ----------

# Load source data
source_df = spark.sql("SELECT table_name, column_count, row_count FROM Source_Count")

# Load metadata to get load_type and sourcetablename
metadata_df = spark.sql("""SELECT sourcetablename, loadtype FROM metadata.mastermetadata where SourceTableName in (  'projection_carrier',
  'projection_load_1',
  'Projection_Load_2',
  'projection_invoicing',
  'projection_invoicing_audit',
  'carrier_nomination_and_onboarding_projection',
  'shippers',
  'plan_combination_projection',
  'receivers',
  'ltl_invoice_projection',
  'offer_negotiation_reflected',
  'carrier_projection',
  'planning_note_captured',
  'sourcing_max_buy_v2',
  'planning_current_assignment',
  'carrier_load_money_projection',
  'delivery_projection',
  'pickup_projection',
  'tracking_last_reason_code',
  'tendering_service_line',
  'tendering_planned_distance',
  'truckload_projection',
  'canonical_plan_projection',
  'rolled_loads',
  'hain_tracking_accuracy_projection',
  'booking_projection',
  'customer_distance_projection',
  'tendering_orders_product_descriptions',
  'tender_reference_numbers_projection',
  'big_export_projection',
  'tendering_acceptance',
  'planning_stop_schedule',
  'canonical_stop',
  'tracking_activity_v2',
  'customer_money',
  'carrier_money_projection',
  'integration_tracking_notification_triggered',
  'tender_split_projection',
  'tender_shipper_address_projection',
  'max_buy_projection',
  'integration_tender_mapped_projection_v2',
  'shipper_in_out_projection',
  'target_customer_money',
  'carrier_projection_new',
  'tmw_pro_projection',
--   'spot_quotes',
  'equip_mode_table',
  'aljex_mode_types',
  'customers',
  'aljex_customer_profiles',
  'aljex_dot_lawson_ref',
  'aljex_cred_debt',
  'aljex_invoice',
  'cai_mode_lookup',
  'cai_salesperson_lookup',
  'edge_mode',
  'cai_equipment',
  'ultipro_terms',
  'ultipro_list',
  'cai_data',
  'event_name_status',
  'customer_profile_external_ids',
  'customer_profile_projection',
  'relay_users',
  'invoicing_customer_profile',
  'offer_negotiation_invalidated',
  'tl_invoice_projection',
  'tendering_tender_draft_id',
  'ap_lawson_appt',
  'distance_projection',
  'target_audit_lookup',
  'tendering_manual_tender_entry_usage',
  'invoicing_invoice_eligibility',
  'integration_hubtran_vendor_invoice_approved',
  'planning_team_driver_marked',
  'invoicing_credits',
  'tracking_in_transit_reason_code',
  'tendering_origin_destination',
  'invoicing_invoice_charges',
  'planning_assignment_log',
  'booking_carrier_planned',
  'vendor_transaction_projection',
  'moneying_billing_party_transaction',
  'new_brokerage_budget',
  'am_sales_lookup',
  'integration_p44_create_shipment_succeeded',
  'invoicing_invoices',
  'tendering_tendered_assigned_carrier',
  'integration_standardized_distance',
  'aljex_am_by_acct',
  'transfix_data',
  'Carrier_Profile',
  'fourkite_carriers',
  'Planning_Schedule_Activity')""")

# Load bronze data excluding 'projection_load' rows
bronze_df = spark.sql("""
    SELECT table_name, column_count, row_count
    FROM Bronze_Info
    WHERE table_name NOT IN ('projection_load_1', 'projection_load_2')
""")

# For Incremental load, filter each table by Is_Deleted = 0
incremental_results = []
for row in metadata_df.filter(col("loadtype") == 'incremental load').collect():
    table_name = row['sourcetablename']
    filtered_df = spark.sql(f"""
        SELECT
            '{table_name}' as table_name,
            COUNT(*) as row_count
        FROM bronze.`{table_name}`
        WHERE Is_Deleted = '0'
        """)
    incremental_results.append(filtered_df)

# Union all data frames containing the incremental loads
if incremental_results:
    incremental_load_agg_df = incremental_results[0]
    for df in incremental_results[1:]:
        incremental_load_agg_df = incremental_load_agg_df.union(df)
else:
    # Create empty DataFrame
    incremental_load_agg_df = spark.createDataFrame([], bronze_df.schema)

# For Truncate and load, take all rows
truncate_load_agg_df = bronze_df.alias("bronze").join(metadata_df.filter(col("loadtype") == 'truncate and load').alias("meta"),
                                                      bronze_df.table_name == col("meta.sourcetablename")) \
    .groupBy("bronze.table_name") \
    .agg(
        spark_sum("bronze.row_count").alias("row_count")
    )

# Union the results from both incremental and truncate and load
aggregated_bronze_df = incremental_load_agg_df.union(truncate_load_agg_df)

# Additional aggregation for projection load types
bronze_df_projection_load = spark.sql("""
    SELECT 
        'projection_load' AS table_name,
        MAX(row_count) AS row_count
    FROM Bronze_Info
    WHERE table_name IN ('projection_load_1', 'Projection_Load_2')
""")
aggregated_bronze_df = aggregated_bronze_df.union(bronze_df_projection_load)

# Perform an inner join on table_name
comparison_df = source_df.alias("source").join(
    aggregated_bronze_df.alias("bronze"),
    source_df.table_name == aggregated_bronze_df.table_name,
    "inner"
).select(
    source_df.table_name,
    # source_df.column_count.alias("source_column_count"),
    # aggregated_bronze_df.column_count.alias("bronze_column_count"),
    source_df.row_count.alias("source_row_count"),
    aggregated_bronze_df.row_count.alias("bronze_row_count")
)

# Calculate the differences
comparison_df = comparison_df.withColumn(
    "row_count_difference",
    col("source_row_count") - col("bronze_row_count")
)

# Add row check status
comparison_df = comparison_df.withColumn(
    "row_check_status",
    when(
        abs(col("row_count_difference")) <= 75,
        "passed"
    ).otherwise("failed")
)

# Filter for only failed records
failed_comparison_df = comparison_df.filter((col("row_check_status") == "failed")
                                            
)
comparison_pipelines_str = failed_comparison_df.select(
        'table_name', 'Source_row_count', 'bronze_row_count', 'row_count_difference', 'row_check_status'
    ).toPandas().to_string(index=False)

# Display the results
display(comparison_df)
display(comparison_pipelines_str)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Mail Secrets

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

try:
  
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']

    credentials_dict = json.loads(json_keyfile_content)

    # Email details
    sender = dbutils.secrets.get(scope = "NFI_Awards_Secrets", key = "ValidationMailSender")

    to = dbutils.secrets.get(scope = "NFI_Awards_Secrets", key = "ValidationMailReceiver") 

    recipients = to.split(",")
    to = ', '.join(recipients)  # Join email addresses with a comma

    # subject = "NFI Brokerage-Data Quality Alert"

except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sending Mail to Respective users - Count Checks

# COMMAND ----------

email_subject = f"Data Comparison Failures - {current_date}"
email_body = f"The following row comparisons have failed:\n\n{comparison_pipelines_str}"

# COMMAND ----------

subject = "Failed Count Comparisons Report"

credentials = service_account.Credentials.from_service_account_info(
    credentials_dict,  # parse JSON string into dict
    scopes=SCOPES,
    subject=sender  # Optional: only if you're impersonating a user
    )

service = build('gmail', 'v1', credentials=credentials)

# Send the email
send_message_count(service, "me", subject, email_body) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sending Mail on Internal Failures Report

# COMMAND ----------


end_date = datetime.now().date()
start_date = end_date - timedelta(days=1)
Log_DF = get_failed_pipelines(start_date, end_date)

# Filter failed pipelines
failed_pipelines = Log_DF.filter(
    (col('Run_Status').isin(['Failed', 'Attention Required'])) & (col('Error_Type') != 'Ignorable')
)

email_subject = f"Failed Pipelines Report: {start_date} to {end_date}"
if failed_pipelines.count() > 0:
    failed_pipelines_str = failed_pipelines.toPandas().to_string(index=False)
    email_body = f"The following table have failed:\n\n{failed_pipelines_str}"
else:
    email_subject = f"No Pipelines Failures - {end_date}"
    email_body = "No Failures Today"

# COMMAND ----------

subject = "Failed Pipelines Report:"

credentials_dict = json.loads(json_keyfile_content)
credentials = service_account.Credentials.from_service_account_info(credentials_dict, scopes=SCOPES, subject=sender)
service = build('gmail', 'v1', credentials=credentials)
# Send the email
send_message_run(service, "me",email_subject, email_body)

# COMMAND ----------

