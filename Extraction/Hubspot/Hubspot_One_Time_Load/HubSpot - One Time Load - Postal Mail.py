# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract data from HubSpot to Bronze as delta file
# MAGIC * **Created Date:** 27-03-2025
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 27-03-2025
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Packages

# COMMAND ----------

# Importing packages
import hubspot
from pprint import pprint
import time
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytz
import math
from hubspot.crm.objects.emails import PublicObjectSearchRequest
from hubspot import HubSpot

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling Utilities and Loggers

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaring Error Loggers

# COMMAND ----------

# Creating variables to store error log information
ErrorLogger = ErrorLogs("HubSpot_One_Time_Load_Emails_I1")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

##Collecting Secret value from the Azure keyVault and storing them in variable
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Secrets From Key Vaults

# COMMAND ----------

try:
    logger.info("Getting secret values and forming the connection string")
    Shipper_Instance_Token = dbutils.secrets.get(scope="NFI_Hubspot_Secrets", key="tokenkey")
    Carrier_Instance_Token = dbutils.secrets.get(scope="NFI_Hubspot_Secrets", key="tokenkey-instance2")
except Exception as e:
    logger.error(f"Error in retrieving secrets: {str(e)}")
    print(str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Metadata Details

# COMMAND ----------

try: 
    Table_ID_I1 = 'H5'
    DF_Metadata_I1 = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = 'H5' and IsActive='1'")
    Job_ID_I1 = DF_Metadata_I1.select(col('JOB_ID')).where(col('TableID') == Table_ID_I1).collect()[0].JOB_ID
    Notebook_ID_I1 = DF_Metadata_I1.select(col('NB_ID')).where(col('TableID') == Table_ID_I1).collect()[0].NB_ID
    TableName_I1 = DF_Metadata_I1.select(col('DWHTableName')).where(col('TableID') == Table_ID_I1).collect()[0].DWHTableName
    MaxLoadDateColumn_I1 = DF_Metadata_I1.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID_I1).collect()[0].LastLoadDateColumn
    MaxLoadDate_I1 = DF_Metadata_I1.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID_I1).collect()[0].LastLoadDateValue
    Zone_I1 = DF_Metadata_I1.select(col('Zone')).where(col('TableID') == Table_ID_I1).collect()[0].Zone
    MergeKey_I1 = DF_Metadata_I1.filter(col("Tableid") == Table_ID_I1).select("MergeKey").collect()[0][0]
    LoadType_I1 = DF_Metadata_I1.select(col('LoadType')).where(col('TableID') == Table_ID_I1).collect()[0].LoadType
    SourceSelectQuery_I1 = DF_Metadata_I1.select(col('SourceSelectQuery')).where(col('TableID') == Table_ID_I1).collect()[0].SourceSelectQuery
except Exception as e:
    logger.info('Failed to Retrive Metadata')
    print('Failed to Retrive Metadata', str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions for Extractions

# COMMAND ----------

desired_fields = [
    "hs_object_id", "archived", "archived_at", "hs_all_accessible_team_ids",
    "hs_all_assigned_business_unit_ids", "hs_all_owner_ids", "hs_all_team_ids",
    "hs_app_id", "hs_at_mentioned_owner_ids", "hs_attachment_ids", "hs_body_preview",
    "hs_body_preview_html", "hs_body_preview_is_truncated", "hs_campaign_guid",
    "hs_created_by", "hs_created_by_user_id", "hs_createdate", "hs_direction_and_unique_id",
    "hs_email_associated_contact_id", "hs_email_attached_video_id", "hs_email_attached_video_name",
    "hs_email_attached_video_opened", "hs_email_attached_video_watched", "hs_email_bcc_email",
    "hs_email_bcc_firstname", "hs_email_bcc_lastname", "hs_email_bcc_raw",
    "hs_email_bounce_error_detail_message", "hs_email_bounce_error_detail_status_code",
    "hs_email_cc_email", "hs_email_cc_firstname", "hs_email_cc_lastname", "hs_email_cc_raw",
    "hs_email_click_count", "hs_email_click_rate", "hs_email_direction",
    "hs_email_encoded_email_associations_request", "hs_email_error_message",
    "hs_email_facsimile_send_id", "hs_email_from_email", "hs_email_from_firstname",
    "hs_email_from_lastname", "hs_email_from_raw", "hs_email_has_inline_images_stripped",
    "hs_email_headers", "hs_email_html", "hs_email_logged_from", "hs_email_media_processing_status",
    "hs_email_member_of_forwarded_subthread", "hs_email_message_id",
    "hs_email_migrated_via_portal_data_migration", "hs_email_ms_teams_payload",
    "hs_email_open_count", "hs_email_open_rate", "hs_email_pending_inline_image_ids",
    "hs_email_post_send_status", "hs_email_recipient_drop_reasons", "hs_email_reply_count",
    "hs_email_reply_rate", "hs_email_send_event_id", "hs_email_send_event_id_created",
    "hs_email_sender_email", "hs_email_sender_firstname", "hs_email_sender_lastname",
    "hs_email_sender_raw", "hs_email_sent_count", "hs_email_sent_via", "hs_email_status",
    "hs_email_stripped_attachment_count", "hs_email_subject", "hs_email_text", "hs_email_thread_id",
    "hs_email_thread_summary", "hs_email_to_email", "hs_email_to_firstname", "hs_email_to_lastname",
    "hs_email_to_raw", "hs_email_tracker_key", "hs_email_validation_skipped",
    "hs_engagement_source", "hs_engagement_source_id", "hs_follow_up_action", "hs_gdpr_deleted",
    "hs_incoming_email_is_out_of_office", "hs_lastmodifieddate", "hs_merged_object_ids",
    "hs_modified_by", "hs_not_tracking_opens_or_clicks", "hs_object_source",
    "hs_object_source_detail_1", "hs_object_source_detail_2", "hs_object_source_detail_3",
    "hs_object_source_id", "hs_object_source_label", "hs_object_source_user_id",
    "hs_owner_ids_bcc", "hs_owner_ids_cc", "hs_owner_ids_from", "hs_owner_ids_to",
    "hs_product_name", "hs_queue_membership_ids", "hs_read_only", "hs_scs_association_status",
    "hs_scs_audit_id", "hs_sequence_id", "hs_shared_team_ids", "hs_shared_user_ids",
    "hs_template_id", "hs_ticket_create_date", "hs_timestamp", "hs_unique_creation_key",
    "hs_unique_id", "hs_updated_by_user_id", "hs_user_ids_of_all_notification_followers",
    "hs_user_ids_of_all_notification_unfollowers", "hs_user_ids_of_all_owners", "hs_was_imported",
    "hubspot_owner_assigneddate", "hubspot_owner_id", "hubspot_team_id", "SourcesystemID"
]
# Define the schema for the DataFrame
schema = StructType([
    StructField("hs_object_id", StringType(), True),
    StructField("archived", StringType(), True),
    StructField("archived_at", StringType(), True),
    StructField("hs_all_accessible_team_ids", StringType(), True),
    StructField("hs_all_assigned_business_unit_ids", StringType(), True),
    StructField("hs_all_owner_ids", StringType(), True),
    StructField("hs_all_team_ids", StringType(), True),
    StructField("hs_app_id", StringType(), True),
    StructField("hs_at_mentioned_owner_ids", StringType(), True),
    StructField("hs_attachment_ids", StringType(), True),
    StructField("hs_body_preview", StringType(), True),
    StructField("hs_body_preview_html", StringType(), True),
    StructField("hs_body_preview_is_truncated", StringType(), True),
    StructField("hs_campaign_guid", StringType(), True),
    StructField("hs_created_by", StringType(), True),
    StructField("hs_created_by_user_id", StringType(), True),
    StructField("hs_createdate", StringType(), True),
    StructField("hs_direction_and_unique_id", StringType(), True),
    StructField("hs_email_associated_contact_id", StringType(), True),
    StructField("hs_email_attached_video_id", StringType(), True),
    StructField("hs_email_attached_video_name", StringType(), True),
    StructField("hs_email_attached_video_opened", StringType(), True),
    StructField("hs_email_attached_video_watched", StringType(), True),
    StructField("hs_email_bcc_email", StringType(), True),
    StructField("hs_email_bcc_firstname", StringType(), True),
    StructField("hs_email_bcc_lastname", StringType(), True),
    StructField("hs_email_bcc_raw", StringType(), True),
    StructField("hs_email_bounce_error_detail_message", StringType(), True),
    StructField("hs_email_bounce_error_detail_status_code", StringType(), True),
    StructField("hs_email_cc_email", StringType(), True),
    StructField("hs_email_cc_firstname", StringType(), True),
    StructField("hs_email_cc_lastname", StringType(), True),
    StructField("hs_email_cc_raw", StringType(), True),
    StructField("hs_email_click_count", StringType(), True),
    StructField("hs_email_click_rate", StringType(), True),
    StructField("hs_email_direction", StringType(), True),
    StructField("hs_email_encoded_email_associations_request", StringType(), True),
    StructField("hs_email_error_message", StringType(), True),
    StructField("hs_email_facsimile_send_id", StringType(), True),
    StructField("hs_email_from_email", StringType(), True),
    StructField("hs_email_from_firstname", StringType(), True),
    StructField("hs_email_from_lastname", StringType(), True),
    StructField("hs_email_from_raw", StringType(), True),
    StructField("hs_email_has_inline_images_stripped", StringType(), True),
    StructField("hs_email_headers", StringType(), True),
    StructField("hs_email_html", StringType(), True),
    StructField("hs_email_logged_from", StringType(), True),
    StructField("hs_email_media_processing_status", StringType(), True),
    StructField("hs_email_member_of_forwarded_subthread", StringType(), True),
    StructField("hs_email_message_id", StringType(), True),
    StructField("hs_email_migrated_via_portal_data_migration", StringType(), True),
    StructField("hs_email_ms_teams_payload", StringType(), True),
    StructField("hs_email_open_count", StringType(), True),
    StructField("hs_email_open_rate", StringType(), True),
    StructField("hs_email_pending_inline_image_ids", StringType(), True),
    StructField("hs_email_post_send_status", StringType(), True),
    StructField("hs_email_recipient_drop_reasons", StringType(), True),
    StructField("hs_email_reply_count", StringType(), True),
    StructField("hs_email_reply_rate", StringType(), True),
    StructField("hs_email_send_event_id", StringType(), True),
    StructField("hs_email_send_event_id_created", StringType(), True),
    StructField("hs_email_sender_email", StringType(), True),
    StructField("hs_email_sender_firstname", StringType(), True),
    StructField("hs_email_sender_lastname", StringType(), True),
    StructField("hs_email_sender_raw", StringType(), True),
    StructField("hs_email_sent_count", StringType(), True),
    StructField("hs_email_sent_via", StringType(), True),
    StructField("hs_email_status", StringType(), True),
    StructField("hs_email_stripped_attachment_count", StringType(), True),
    StructField("hs_email_subject", StringType(), True),
    StructField("hs_email_text", StringType(), True),
    StructField("hs_email_thread_id", StringType(), True),
    StructField("hs_email_thread_summary", StringType(), True),
    StructField("hs_email_to_email", StringType(), True),
    StructField("hs_email_to_firstname", StringType(), True),
    StructField("hs_email_to_lastname", StringType(), True),
    StructField("hs_email_to_raw", StringType(), True),
    StructField("hs_email_tracker_key", StringType(), True),
    StructField("hs_email_validation_skipped", BooleanType(), True),
    StructField("hs_engagement_source", StringType(), True),
    StructField("hs_engagement_source_id", StringType(), True),
    StructField("hs_follow_up_action", StringType(), True),
    StructField("hs_gdpr_deleted", StringType(), True),
    StructField("hs_incoming_email_is_out_of_office", StringType(), True),
    StructField("hs_lastmodifieddate", StringType(), True),
    StructField("hs_merged_object_ids", StringType(), True),
    StructField("hs_modified_by", StringType(), True),
    StructField("hs_not_tracking_opens_or_clicks", StringType(), True),
    StructField("hs_object_source", StringType(), True),
    StructField("hs_object_source_detail_1", StringType(), True),
    StructField("hs_object_source_detail_2", StringType(), True),
    StructField("hs_object_source_detail_3", StringType(), True),
    StructField("hs_object_source_id", StringType(), True),
    StructField("hs_object_source_label", StringType(), True),
    StructField("hs_object_source_user_id", StringType(), True),
    StructField("hs_owner_ids_bcc", StringType(), True),
    StructField("hs_owner_ids_cc", StringType(), True),
    StructField("hs_owner_ids_from", StringType(), True),
    StructField("hs_owner_ids_to", StringType(), True),
    StructField("hs_product_name", StringType(), True),
    StructField("hs_queue_membership_ids", StringType(), True),
    StructField("hs_read_only", StringType(), True),
    StructField("hs_scs_association_status", StringType(), True),
    StructField("hs_scs_audit_id", StringType(), True),
    StructField("hs_sequence_id", StringType(), True),
    StructField("hs_shared_team_ids", StringType(), True),
    StructField("hs_shared_user_ids", StringType(), True),
    StructField("hs_template_id", StringType(), True),
    StructField("hs_ticket_create_date", StringType(), True),
    StructField("hs_timestamp", StringType(), True),
    StructField("hs_unique_creation_key", StringType(), True),
    StructField("hs_unique_id", StringType(), True),
    StructField("hs_updated_by_user_id", StringType(), True),
    StructField("hs_user_ids_of_all_notification_followers", StringType(), True),
    StructField("hs_user_ids_of_all_notification_unfollowers", StringType(), True),
    StructField("hs_user_ids_of_all_owners", StringType(), True),
    StructField("hs_was_imported", StringType(), True),
    StructField("hubspot_owner_assigneddate", StringType(), True),
    StructField("hubspot_owner_id", StringType(), True),
    StructField("hubspot_team_id", StringType(), True),
    StructField("SourcesystemID", IntegerType(), True)
])

def response_to_dataframe(SourceData):
    if not SourceData:
        return pd.DataFrame()
    
    data = [result.properties for result in SourceData]
    
    # Convert archivedat to timestamp
    for entry in data:
        if 'archivedat' in entry and entry['archivedat'] is not None:
            entry['archivedat'] = datetime.fromisoformat(entry['archivedat'])

    return pd.DataFrame(data)

def fetch_and_load_data(client, max_date, archived, table_name, merge_key, job_id):
    next_page = None  # Start without a page token
    api_list = []

    while True:
        time.sleep(0.5)

        api_response = client.crm.objects.emails.basic_api.get_page(
            limit=100,
            archived=archived,
            after=next_page,  # Use the correct variable
            properties=desired_fields
        )

        if not api_response.results:
            break  # Exit if no results

        for item in api_response.results:
            item.properties['archived_at'] = item.archived_at
            item.properties['archived'] = item.archived
            api_list.append(item)

            if len(api_list) >= 10000:
                # Process the batch
                df = response_to_dataframe(api_list)

                if not df.empty:
                    df["SourcesystemID"] = access_tokens.index(client.access_token) + 4
                    # Ensure df.columns matches the schema fields
                    expected_columns = [field.name for field in schema.fields]

                    for column in expected_columns:
                        if column not in df.columns:
                            df[column] = None
                    
                    df = df[expected_columns]

                    # Convert any timestamp columns to string, if necessary
                    if 'archived_at' in df.columns:
                        df['archived_at'] = df['archived_at'].astype(str)

                    # Replace NaN values with None in the entire DataFrame
                    df['archived_at'] = df['archived_at'].replace('nan', None)
                    df['archived_at'] = df['archived_at'].replace('NaT', None)
                    df['archived_at'] = df['archived_at'].replace('None', None)

                    # Convert SourcesystemID and boolean columns if needed
                    df['archived'] = df['archived'].astype(str).apply(lambda x: x.lower())
                    DF_Source = spark.createDataFrame(df, schema=schema)
                    DF_Source = DF_Source.withColumn("Is_Deleted", lit("0"))
                    DF_Source.createOrReplaceTempView("SourceToBronze")

                    # Insert into the target table
                    MergeQuery = f"""
                    MERGE INTO bronze.Hubspot_Emails_I1 AS Target
                    USING SourceToBronze AS Source
                    ON Target.{merge_key} = Source.{merge_key}
                    WHEN MATCHED THEN
                      UPDATE SET *
                    WHEN NOT MATCHED THEN
                      INSERT *
                    """
                    spark.sql(MergeQuery)
                    logger.info(f"Loaded {df.shape[0]} records into Hubspot_Emails_I1")

                api_list.clear()

        # Update the next_page token safely
        if hasattr(api_response, 'paging') and api_response.paging and hasattr(api_response.paging, 'next'):
            next_page = api_response.paging.next.after
        else:
            break  # Exit if no more pages

    # Load any remaining records
    if api_list:
        df = response_to_dataframe(api_list)
        if not df.empty:
            df["SourcesystemID"] = access_tokens.index(client.access_token) + 4
            
            expected_columns = [field.name for field in schema.fields]

            for column in expected_columns:
                if column not in df.columns:
                    df[column] = None
            
            df = df[expected_columns]

            # Convert any timestamp columns to string, if necessary
            if 'archived_at' in df.columns:
                df['archived_at'] = df['archived_at'].astype(str)

            # Replace NaN values with None in the entire DataFrame
            df['archived_at'] = df['archived_at'].replace('nan', None)
            df['archived_at'] = df['archived_at'].replace('NaT', None)
            df['archived_at'] = df['archived_at'].replace('None', None)

            # Convert SourcesystemID and boolean columns if needed
            df['archived'] = df['archived'].astype(str).apply(lambda x: x.lower())
            DF_Source = spark.createDataFrame(df, schema=schema)
            DF_Source = DF_Source.withColumn("Is_Deleted", lit("0"))
            DF_Source = DF_Source.dropDuplicates()
            DF_Source.createOrReplaceTempView("SourceToBronze")

            MergeQuery = f"""
            MERGE INTO bronze.Hubspot_Emails_I1 AS Target
            USING SourceToBronze AS Source
            ON Target.{merge_key} = Source.{merge_key} 
            WHEN MATCHED THEN
              UPDATE SET *
            WHEN NOT MATCHED THEN
              INSERT *
            """
            spark.sql(MergeQuery)
            logger.info(f"Loaded {df.shape[0]} remaining records into Hubspot_Emails_I1")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge Data To Target

# COMMAND ----------

try : 
    # Main extraction process
    access_tokens = [Shipper_Instance_Token]
    max_dates = [MaxLoadDate_I1]

    for i, access_token in enumerate(access_tokens):
        client = HubSpot(access_token=access_token)
        max_date = max_dates[i]

        # Fetch and load non-archived data
        fetch_and_load_data(client, max_date, archived=False, 
                            table_name=TableName_I1, merge_key=MergeKey_I1, job_id=Job_ID_I1)

        # Fetch and load archived data
        fetch_and_load_data(client, max_date, archived=True, 
                            table_name=TableName_I1, merge_key=MergeKey_I1, job_id=Job_ID_I1)

    # Logging completion
    logger.info("Data extraction and loading process completed.")
    UpdateLogStatus(Job_ID_I1, Table_ID_I1, Notebook_ID_I1, TableName_I1, Zone_I1, "Succeeded", "NULL", "NULL", 0, "Account Manager")
    MaxDateQuery = "Select max({0}) as Max_Date from bronze.Hubspot_Emails_I1 WHERE SourcesystemID = '{2}'"
    MaxDateQuery_I1 = MaxDateQuery.format(MaxLoadDateColumn_I1, "Hubspot_Emails_I1", '4')
    DF_MaxDate_I1 = spark.sql(MaxDateQuery_I1)
    UpdateLastLoadDate(Table_ID_I1,DF_MaxDate_I1)
    UpdatePipelineStatusAndTime(Table_ID_I1, Zone_I1)

except Exception as e:
    logger.info(f"Data extraction and loading process failed with error: {e}")
    Error_Statement = str(e)
    UpdateLogStatus(Job_ID_I1, Table_ID_I1, Notebook_ID_I1, TableName_I1, Zone_I1,"Failed",Error_Statement,"NotIgnorable",1,"Account Manager")
    UpdateFailedStatus(Table_ID_I1, 'Bronze')
    raise RuntimeError(f"Job failed: Unable to Merge: {e}")


# COMMAND ----------

