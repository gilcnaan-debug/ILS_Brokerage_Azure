# Databricks notebook source
# MAGIC %md
# MAGIC ##Fact_Touches

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To extract the data from Silver and load to Gold for Dim tables with SCD Implementation
# MAGIC * **Created Date:** 14/12/2023
# MAGIC * **Created By:** Freedon Demi
# MAGIC * **Modified Date:** - 
# MAGIC * **Modified By:**
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import the required packages

# COMMAND ----------

##Import required Package
from pyspark.sql.functions import *
import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize the Utility notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initialize the Logger notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Create variables to store error log information

ErrorLogger = ErrorLogs("Fact_Touches_SilverToGold")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------



# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Manual Tender Created

# COMMAND ----------

try :
    DF_Relay_Manual_Tender_Created = spark.sql(''' -- Perform the joins
    WITH first_join AS (
        SELECT *
        FROM bronze.tendering_tender_draft_id
        JOIN bronze.tendering_manual_tender_entry_usage
        ON bronze.tendering_tender_draft_id.tender_draft_id = bronze.tendering_manual_tender_entry_usage.tender_draft_id
    ),
    second_join AS (
        SELECT *
        FROM first_join
        LEFT JOIN bronze.relay_users
        ON first_join.draft_by_user_id = bronze.relay_users.user_id
        WHERE relay_users.full_name IS NOT NULL
    )

    -- Declare the final dataframe and rename columns
    SELECT DISTINCT
        'Relay - Manual Tender Created' AS Touch_type,
        CONCAT_WS(relay_reference_number,full_name,completed_at, 'Relay - Manual Tender Created') AS Touches_id,
        full_name,
        CAST(completed_at AS DATE) AS Date
    FROM second_join
    where completed_at IS NOT NULL;
    ''')
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Manual_Tender_Created: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Tender Accepted

# COMMAND ----------

try:
    DF_Relay_Tender_Accepted = spark.sql("""
    -- Perform the left join
    WITH joined_data AS (
        SELECT *
        FROM bronze.tendering_acceptance
        LEFT JOIN bronze.relay_users
        ON bronze.tendering_acceptance.accepted_by = bronze.relay_users.user_id
        WHERE relay_users.full_name IS NOT NULL
        AND bronze.tendering_acceptance.`accepted?` = 'true'
    )

    -- Select and rename columns
    SELECT DISTINCT
        'Relay - Tender Accepted' AS Touch_type,
        CONCAT_WS(relay_reference_number,CASE WHEN accepted_by IS NULL THEN 'Auto Accept' ELSE full_name END,accepted_at,'Relay - Tender Accepted') AS Touches_id,
        CASE WHEN accepted_by IS NULL THEN 'Auto Accept' ELSE full_name END AS full_name,
        CAST(accepted_at AS TIMESTAMP) AS Date
    FROM joined_data
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Tender_Accepted: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Appointment Scheduled

# COMMAND ----------

try:
    DF_Relay_Appt_Scheduled = spark.sql("""
    WITH joined_data AS (
        SELECT *
        FROM bronze.planning_stop_schedule
        LEFT JOIN bronze.relay_users
        ON bronze.planning_stop_schedule.scheduled_by = bronze.relay_users.user_id
        WHERE scheduled_by IS NOT NULL
        AND stop_type = 'pickup'
    )

    -- Select and rename columns
    SELECT DISTINCT
        'Relay - Appt Scheduled' AS Touch_Type,
        CONCAT_WS(relay_reference_number,full_name,scheduled_at,'Relay - Appt Scheduled') AS Touches_id,
        full_name,
        CAST(scheduled_at AS TIMESTAMP) AS Date
    FROM joined_data
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Appt_Scheduled: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Aljex Load Booked

# COMMAND ----------

try:
    DF_Aljex_Load_Booked = spark.sql("""
    -- Joins and Filtering
    -- Joins and Filtering
    WITH joined_data AS (
        SELECT *
        FROM Bronze.projection_load_1
        LEFT JOIN Bronze.aljex_user_report_listing
        ON key_c_user = aljex_id
        WHERE key_c_user IS NOT NULL
        AND key_c_date NOT LIKE '%:%'
    )

    -- Select and Rename Columns
    SELECT DISTINCT
        'Aljex - Load Booked' AS Touch_type,
        CONCAT_WS(id,key_c_date,full_name,'Aljex - Load Booked') AS Touches_id,
        full_name,
        CAST(key_c_date AS DATE) AS date
    FROM joined_data
    where full_name is not null;
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Aljex_Load_Booked: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Load Booked

# COMMAND ----------

try:
    DF_Relay_Load_Booked = spark.sql("""
    -- Filtering and Selecting Columns
    SELECT DISTINCT
        'Relay - Load Booked' AS Touch_Type,
        CONCAT_WS(relay_reference_number,booked_by_name,booked_at,'Relay - Load Booked') AS Touches_id,
        booked_by_name AS full_name,
        CAST(booked_at AS TIMESTAMP) AS Date
    FROM Bronze.booking_projection
    WHERE booked_at IS NOT NULL;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Load_Booked: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Assigned a Load

# COMMAND ----------

try:
    DF_Relay_Assigned_Load = spark.sql("""
    -- Joins
    WITH joined_data AS (
        SELECT pal.*, ru.full_name AS full_name
        FROM Bronze.planning_assignment_log pal
        LEFT JOIN Bronze.relay_users ru ON pal.action_by = ru.user_id
        WHERE pal.action_by IS NOT NULL
        AND pal.assignment_action = 'Assigned to Open Board'
    )

    -- Select Columns
    SELECT DISTINCT
        'Relay - Assigned load' AS Touch_Type,
        CONCAT_WS(relay_reference_number,CASE
            WHEN assignment_action = 'Assigned to Open Board' THEN NULL
            ELSE full_name
        END,action_at, 'Relay - Assigned load') AS Touches_id,
        CASE
            WHEN assignment_action = 'Assigned to Open Board' THEN NULL
            ELSE full_name
        END AS full_name,
        CAST(action_at AS TIMESTAMP) AS Date
    FROM joined_data;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Assigned_Load: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Max Buy

# COMMAND ----------

try:
    DF_Relay_Max_Buy = spark.sql("""
    -- Select and Rename Columns
    SELECT DISTINCT
        'Relay - Set a Max Buy' AS Touch_Type,
        CONCAT_WS(relay_reference_number,set_by_name,set_at, 'Relay - Set a Max Buy') AS Touches_id,
        set_by_name AS full_name,
        CAST(set_at AS TIMESTAMP) AS Date
    FROM Bronze.sourcing_max_buy_v2;
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Max_Buy: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Load Cancelled

# COMMAND ----------

try:
    DF_Relay_Load_Cancelled = spark.sql("""
    -- Join with Users
    WITH joined_data AS (
        SELECT t.*, u.full_name AS cancelled_by_name
        FROM Bronze.tender_reference_numbers_projection t
        LEFT JOIN Bronze.relay_users u
        ON t.cancelled_by = u.user_id
    )

    -- Select specific columns and filter data
    SELECT DISTINCT
        'Relay - Load Cancelled' AS Touch_Type,
        CONCAT_WS('Relay - Load Cancelled',CASE WHEN cancelled_by IS NULL THEN 'EDI' ELSE cancelled_by_name END,relay_reference_number,cancelled_at) AS Touches_id,
        CASE WHEN cancelled_by IS NULL THEN 'EDI' ELSE cancelled_by_name END AS full_name,
        CAST(cancelled_at AS TIMESTAMP) AS Date
    FROM joined_data
    WHERE cancelled_at IS NOT NULL
    and relay_reference_number is not null;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Load_Cancelled: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Rate Con Sent

# COMMAND ----------

try:
    DF_Relay_Rate_Con_Sent = spark.sql("""
    -- Join with Users
    WITH joined_data AS (
        SELECT b.*, u.full_name AS full_name
        FROM Bronze.booking_projection b
        LEFT JOIN Bronze.relay_users u
        ON b.rate_con_sent_by = u.user_id
    )

    -- Select specific columns and filter data
    SELECT DISTINCT
        'Relay - Rate Con Sent' AS Touch_Type,
        CONCAT_WS(relay_reference_number,full_name,last_rate_con_sent_time, 'Relay - Rate Con Sent') AS Touches_id,
        full_name AS full_name,
        try_cast(SUBSTRING(last_rate_con_sent_time FROM 1 FOR POSITION(' ' IN last_rate_con_sent_time) - 1) as Date) AS Date
    FROM joined_data
    WHERE  try_cast(SUBSTRING(last_rate_con_sent_time FROM 1 FOR POSITION(' ' IN last_rate_con_sent_time) - 1) as Date)  IS NOT NULL AND 
    relay_reference_number IS NOT NULL;
        """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Rate_Con_Sent: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Reserved Load

# COMMAND ----------

try:
    DF_Relay_Reserved_Load = spark.sql("""
    -- Join with Users
    WITH joined_data AS (
        SELECT b.*, u.full_name AS full_name
        FROM Bronze.booking_projection b
        JOIN Bronze.relay_users u
        ON b.reserved_by = u.user_id
    )

    -- Select specific columns and filter data
    SELECT DISTINCT
        'Relay - Reserved a Load' AS Touch_Type,
        CONCAT_WS(relay_reference_number,full_name,reserved_at, 'Relay - Reserved a Load') AS Touches_id,
        full_name,
        CAST(reserved_at AS TIMESTAMP) AS Date
    FROM joined_data
    WHERE reserved_at IS NOT NULL;
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Reserved_Load: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Bounced Load

# COMMAND ----------

try:
    DF_Relay_Bounced_Load = spark.sql("""
    -- Join with Users
    WITH joined_data AS (
        SELECT b.*, u.full_name AS full_name
        FROM Bronze.booking_projection b
        JOIN Bronze.relay_users u
        ON b.bounced_by = u.user_id
    )

    -- Select specific columns and filter data
    SELECT DISTINCT
        'Relay - Bounced a Load' AS Touch_Type,
        CONCAT_WS(relay_reference_number,full_name,bounced_at, 'Relay - Bounced a Load') AS Touches_id,
        full_name,
        CAST(bounced_at AS TIMESTAMP) AS Date
    FROM joined_data
    WHERE bounced_at IS NOT NULL;
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Bounced_Load: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Rolled a Load

# COMMAND ----------

try:
    DF_Relay_Rolled_Load = spark.sql("""
    -- Join with Users
    WITH joined_data AS (
        SELECT b.*, u.full_name AS full_name
        FROM Bronze.booking_projection b
        JOIN Bronze.relay_users u
        ON b.rolled_by = u.user_id
    )

    -- Select specific columns and filter data
    SELECT DISTINCT
        'Relay - Rolled a Load' AS Touch_Type,
        CONCAT_WS(relay_reference_number,full_name,rolled_at,'Relay - Rolled a Load') AS Touches_id,
        full_name,
        CAST(rolled_at AS TIMESTAMP) AS Date
    FROM joined_data
    WHERE rolled_at IS NOT NULL;
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Rolled_Load: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Put Offer on a Load

# COMMAND ----------

try:
    DF_Relay_Put_Offer_Load = spark.sql("""
    -- Left Join with Invalidated Offers
    WITH joined_data AS (
        SELECT n.*, i.relay_reference_number AS invalidated_relay_reference_number
        FROM Bronze.offer_negotiation_reflected n
        LEFT JOIN Bronze.offer_negotiation_invalidated i
        ON n.relay_reference_number = i.relay_reference_number
    )

    -- Select specific columns and filter data
    SELECT DISTINCT
        'Relay - Put Offer on a Load' AS Touch_Type,
        CONCAT_WS(relay_reference_number,offered_by_name,offered_at, 'Relay - Put Offer on a Load') AS Touches_id,
        offered_by_name AS full_name,
        CAST(offered_at AS TIMESTAMP) AS Date
    FROM joined_data
    WHERE offered_by_name IS NOT NULL
    AND invalidated_relay_reference_number IS NULL;
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Put_Offer_Load: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Tracking

# COMMAND ----------

try:
    DF_Relay_Tracking = spark.sql("""
    -- Left Joins
    WITH joined_data AS (
        SELECT a.*, tp.relay_reference_number, a.at, u.full_name
        FROM Bronze.tracking_activity_v2 a
        LEFT JOIN Bronze.truckload_projection tp
        ON a.truck_load_thing_id = tp.truck_load_thing_id
        LEFT JOIN Bronze.relay_users u
        ON a.by_user_id = u.user_id AND u.`active?` = 'true'
    )

    -- Select and Rename Columns, Filter Data
    SELECT DISTINCT
        CASE
            WHEN type = 'Tracking.TruckLoadThing.AppointmentCaptured' THEN 'Relay - Appointment Captured'
            WHEN type = 'Tracking.TruckLoadThing.DetentionReported' THEN 'Relay - Detention Reported'
            WHEN type = 'Tracking.TruckLoadThing.InTransitUpdateCaptured' THEN 'Relay - In Transit Update Captured'
            WHEN type = 'Tracking.TruckLoadThing.TruckLoadThingChanged' THEN 'Relay - TruckLoadThing Changed'
            WHEN type = 'Tracking.TruckLoadThing.MarkedLoaded' THEN 'Relay - Marked Loaded'
            WHEN type = 'Tracking.TruckLoadThing.MarkedDepartedFromStop' THEN 'Relay - Marked Departed From Stop'
            WHEN type = 'Tracking.TruckLoadThing.MarkedArrivedAtStop' THEN 'Relay - Marked Arrived At Stop'
            WHEN type = 'Tracking.TruckLoadThing.TrackingContactInformationCaptured' THEN 'Relay - Tracking Contact Info Captured'
            WHEN type = 'Tracking.TruckLoadThing.TrackingStarted' THEN 'Relay - Tracking Started'
            WHEN type = 'Tracking.TruckLoadThing.StopMarkedDelivered' THEN 'Relay - Stop Marked Delivered'
            WHEN type = 'Tracking.TruckLoadThing.StopsReordered' THEN 'Relay - Stop Recorded'
            WHEN type = 'Tracking.TruckLoadThing.DriverInformationCaptured' THEN 'Relay - Driver Info Captured'  
            WHEN type = 'Tracking.TruckLoadThing.EquipmentAssigned' THEN 'Relay - Equipment Assigned'
            WHEN type = 'Tracking.TruckLoadThing.TrackingNoteCaptured' THEN 'Relay - Tracking Note Captured'
            WHEN type = 'Tracking.TruckLoadThing.TrackingStopped' THEN 'Relay - Tracking Stopped'
            WHEN type = 'Tracking.TruckLoadThing.MarkedDelivered' THEN 'Relay - Marked Delivered'
            WHEN type = 'Tracking.TruckLoadThing.StatusUpdateIntegrationIdentified' THEN 'Relay - Status Update Integration Identified'
            WHEN type = 'Tracking.TruckLoadThing.DriverDispatched' THEN 'Relay - Driver Dispatched'
        END AS Touch_Type,
        CONCAT_WS(full_name,relay_reference_number,joined_data.at, joined_data.type) AS Touches_id,
        full_name,
        CAST(at AS DATE) AS Date
    FROM joined_data
    WHERE full_name IS NOT NULL AND by_type = 'user';

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Tracking: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Relay Stop Marked Delivered

# COMMAND ----------

try:
    DF_Relay_Stop_Marked_Delivered = spark.sql("""
    -- Left Join
    WITH joined_data AS (
        SELECT s.*, u.full_name
        FROM Bronze.planning_stop_schedule s
        LEFT JOIN Bronze.relay_users u
        ON s.scheduled_by = u.user_id
        WHERE scheduled_by IS NOT NULL AND stop_type = 'delivery'
    )

    -- Select and Rename Columns
    SELECT DISTINCT
        'Relay - Stop Marked Delivered' AS Touch_Type,
        CONCAT_WS(relay_reference_number,full_name,scheduled_at, 'Relay - Marked Delivered') AS Touches_id,
        full_name,
        CAST(scheduled_at AS TIMESTAMP) AS Date
    FROM joined_data;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Stop_Marked_Delivered: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Hubspot email touches

# COMMAND ----------

try:
    DF_Hubspot_email_touches = spark.sql("""
    -- Selecting fields from Hubspot_emails and Hubspot_Owners
    WITH Hubspot_emails_DF AS (
        SELECT
            hs_createdate,
            hs_email_direction,
            hs_email_sender_email,
            hs_object_id,
            hubspot_owner_id,
            SourcesystemID
        FROM bronze.Hubspot_emails
    ),
    Hubspot_Owners_DF AS (
        SELECT
            id,
            email
        FROM bronze.Hubspot_Owners
    )

    -- Joining Hubspot_emails_DF with Hubspot_Owners_DF
    SELECT DISTINCT
        'Hubspot-Email' AS Touch_Type,
        CONCAT_WS(hs_object_id,email,'Hubspot-Email',hs_createdate) AS Touches_id,
        email AS User_email,
        CAST(1 AS INT) AS Touches_Score,
        CAST(hs_createdate AS TIMESTAMP) AS Timestamp,
        SourcesystemID AS Source_system_id
    FROM Hubspot_emails_DF e
    JOIN Hubspot_Owners_DF o ON e.hubspot_owner_id = o.id
    WHERE hs_email_direction = 'EMAIL';

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Hubspot_email_touches: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Hubspot Meeting Created Touches

# COMMAND ----------

try:
    DF_Hubspot_meeting_created_touches = spark.sql("""
    -- Selecting fields from Hubspot_meetings and Hubspot_Owners
    WITH Hubspot_Meeting_Created_DF AS (
        SELECT
            hs_createdate,
            hs_object_id,
            hubspot_owner_id,
            SourcesystemID
        FROM bronze.Hubspot_meetings
    ),
    Hubspot_Owners_DF AS (
        SELECT
            id,
            email
        FROM bronze.Hubspot_Owners
    )

    -- Joining Hubspot_Meeting_Created_DF with Hubspot_Owners_DF
    SELECT DISTINCT
        'Hubspot-Meeting' AS Touch_Type,
        CONCAT_WS(hs_object_id,email,'Hubspot-MeetingCreated',hs_createdate) AS Touches_id,
        email AS User_email,
        CAST(1 AS INT) AS Touches_score,
        CAST(hs_createdate AS TIMESTAMP) AS Timestamp,
        SourcesystemID AS Source_system_id
    FROM Hubspot_Meeting_Created_DF m
    JOIN Hubspot_Owners_DF o ON m.hubspot_owner_id = o.id;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Hubspot_meeting_created_touches: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Hubspot Meeting Attended Touches

# COMMAND ----------

try:
    DF_Hubspot_meeting_attended_touches = spark.sql("""
    -- Selecting fields from Hubspot_meetings and Hubspot_Owners
    WITH Hubspot_Meeting_Attended_DF AS (
        SELECT
            hs_meeting_start_time,
            hs_object_id,
            hubspot_owner_id,
            SourcesystemID
        FROM bronze.Hubspot_meetings
    ),
    Hubspot_Owners_DF AS (
        SELECT
            id,
            email
        FROM bronze.Hubspot_Owners
    )

    -- Joining Hubspot_Meeting_Attended_DF with Hubspot_Owners_DF
    SELECT DISTINCT
        'Hubspot-Meeting' AS Touch_Type,
        CONCAT_WS(hs_object_id,email,'Hubspot-MeetingAttended',hs_meeting_start_time) AS Touches_id,
        email AS User_email,
        CAST(1 AS INT) AS Touches_score,
        CAST(hs_meeting_start_time AS TIMESTAMP) AS Timestamp,
        SourcesystemID AS Source_system_id
    FROM Hubspot_Meeting_Attended_DF m
    JOIN Hubspot_Owners_DF o ON m.hubspot_owner_id = o.id;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Hubspot_meeting_attended_touches: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Hubspot Notes

# COMMAND ----------

try:
    DF_Hubspot_notes_touches = spark.sql("""
    -- Selecting fields from Hubspot_notes and Hubspot_Owners
    WITH Hubspot_notes_DF AS (
        SELECT
            hs_createdate,
            hs_object_id,
            hubspot_owner_id,
            SourcesystemID
        FROM bronze.Hubspot_notes
    ),
    Hubspot_Owners_DF AS (
        SELECT
            id,
            email
        FROM bronze.Hubspot_Owners
    )

    -- Joining Hubspot_notes_DF with Hubspot_Owners_DF
    SELECT DISTINCT
        'Hubspot-Notes' AS Touch_Type,
        CONCAT_WS(hs_object_id,email,'Hubspot-Notes',hs_createdate) AS Touches_id,
        email AS User_email,
        CAST(1 AS INT) AS Touches_score,
        CAST(hs_createdate AS TIMESTAMP) AS Timestamp,
        SourcesystemID AS Source_system_id
    FROM Hubspot_notes_DF n
    JOIN Hubspot_Owners_DF o ON n.hubspot_owner_id = o.id;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Hubspot_notes_touches: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Hubspot Deals Created

# COMMAND ----------

try:
    DF_Hubspot_deals_created_touches = spark.sql("""
    -- Selecting fields from Hubspot_Deals and Hubspot_Owners
    WITH Hubspot_Deals_Created_DF AS (
        SELECT
            createdate,
            hs_object_id,
            hubspot_owner_id,
            SourcesystemID
        FROM bronze.Hubspot_Deals
    ),
    Hubspot_Owners_DF AS (
        SELECT
            id,
            email
        FROM bronze.Hubspot_Owners
    )

    -- Joining Hubspot_Meeting_Created_DF with Hubspot_Owners_DF
    SELECT DISTINCT
        'Hubspot-Deals' AS Touch_Type,
        CONCAT_WS(hs_object_id,email,createdate,'Hubspot-DealsCreated',createdate) AS Touches_id,
        email AS User_email,
        CAST(1 AS INT) AS Touches_score,
        CAST(createdate AS TIMESTAMP) AS Timestamp,
        SourcesystemID AS Source_system_id
    FROM Hubspot_Deals_Created_DF m
    JOIN Hubspot_Owners_DF o ON m.hubspot_owner_id = o.id
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Hubspot_deals_created_touches: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Hubspot Deals Closed Touches

# COMMAND ----------

try:
    DF_Hubspot_deals_closed_touches = spark.sql("""
    -- Selecting fields from Hubspot_meetings and Hubspot_Owners
    WITH Hubspot_Deals_Closed_DF AS (
        SELECT
            Closedate,
            hs_object_id,
            hubspot_owner_id,
            SourcesystemID
        FROM bronze.Hubspot_Deals
        where Closedate is not NULL
    ),
    Hubspot_Owners_DF AS (
        SELECT
            id,
            email
        FROM bronze.Hubspot_Owners
    )

    -- Joining Hubspot_Deals_Closed_DF with Hubspot_Owners_DF
    SELECT DISTINCT
        'Hubspot-Deals' AS Touch_Type,
        CONCAT_WS(hs_object_id,email,Closedate,'Hubspot-DealsCreated',Closedate) AS Touches_id,
        email AS User_email,
        CAST(1 AS INT) AS Touches_score,
        CAST(Closedate AS TIMESTAMP) AS Timestamp,
        SourcesystemID AS Source_system_id
    FROM Hubspot_Deals_Closed_DF m
    JOIN Hubspot_Owners_DF o ON m.hubspot_owner_id = o.id;
    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Hubspot_deals_closed_touches: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Hubspot Deals Updated Touches

# COMMAND ----------

try:
    DF_Hubspot_deals_updated_touches = spark.sql("""
    -- Selecting fields from Hubspot_Deals and Hubspot_Owners
    WITH Hubspot_Deals_Updated_DF AS (
        SELECT
            hs_lastmodifieddate,
            hs_object_id,
            hubspot_owner_id,
            SourcesystemID
        FROM bronze.Hubspot_deals
        WHERE hs_lastmodifieddate IS NOT NULL
    ),
    Hubspot_Owners_DF AS (
        SELECT
            id,
            email
        FROM bronze.Hubspot_Owners
    )

    -- Joining Hubspot_Deals_Updated_DF with Hubspot_Owners_DF
    SELECT DISTINCT
        'Hubspot-Deals' AS Touch_Type,
        CONCAT_WS(hs_object_id,email,hs_lastmodifieddate, 'Hubspot-DealsClosed',hs_lastmodifieddate) AS Touches_id,
        email AS User_email,
        CAST(1 AS INT) AS Touches_score,
        CAST(hs_lastmodifieddate AS TIMESTAMP) AS Timestamp,
        SourcesystemID AS Source_system_id
    FROM Hubspot_Deals_Updated_DF m
    JOIN Hubspot_Owners_DF o ON m.hubspot_owner_id = o.id;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Hubspot_deals_updated_touches: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Aljex Marked Loaded

# COMMAND ----------

try:
    DF_Aljex_Marked_Loaded = spark.sql("""
    -- Read data from the projection_load table and aljex_user_report_listing table
    WITH joined_data AS (
        SELECT
            p.id AS load_id,
            al.full_name AS full_name,
            p.Loaded_date AS Loaded_date
        FROM bronze.projection_load_1 p
        LEFT JOIN bronze.aljex_user_report_listing al
        ON p.key_c_user = al.aljex_id
        LEFT JOIN bronze.projection_load_2 p2
        ON p.id = p2.id
        WHERE p2.tag_created_by NOT LIKE 'EDI%'
    )

    -- Select and rename columns
    SELECT DISTINCT
        'Aljex - Marked Loaded' AS Touch_Type,
        CONCAT_WS(load_id,full_name,Loaded_date, 'Aljex - Marked Loaded') AS Touches_id,
        full_name,
        CAST(Loaded_date AS DATE) AS Date
    FROM joined_data
    where Loaded_date is not NULL
    and full_name is not null;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Aljex_Marked_Loaded: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Aljex Marked Delivered

# COMMAND ----------

try:
    DF_Aljex_Marked_Delivered = spark.sql("""
    -- Read data from the projection_load table and aljex_user_report_listing table
    WITH joined_data AS (
        SELECT
            p.id AS load_id,
            al.full_name AS full_name,
            p.Delivery_date AS Delivery_date
        FROM bronze.projection_load_1 p
        LEFT JOIN bronze.aljex_user_report_listing al
        ON p.key_c_user = al.aljex_id
         LEFT JOIN bronze.projection_load_2 p2
        ON p.id = p2.id
        WHERE p2.tag_created_by NOT LIKE 'EDI%'
    )

    -- Select and rename columns
    SELECT DISTINCT
        'Aljex - Marked Delivered' AS Touch_Type,
        CONCAT_WS(load_id,full_name, 'Aljex - Marked Delivered') AS touches_id,
        full_name,
        Try_cast(Delivery_date AS DATE) AS Date
    FROM joined_data
    where Try_cast(Delivery_date AS DATE) IS NOT NULL and full_name is not null;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Aljex_Marked_Delivered: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Aljex Driver Dispatched Touches

# COMMAND ----------

try:
    DF_Aljex_Driver_Dispatched = spark.sql("""
    -- Read data from the projection_load table and aljex_user_report_listing table
    WITH joined_data AS (
        SELECT
            p.id AS load_id,
            al.full_name AS full_name,
            try_cast(p.Dispatched_date AS Date) AS Dispatched_date
        FROM bronze.projection_load_1 p
        LEFT JOIN bronze.aljex_user_report_listing al
        ON p.key_c_user = al.aljex_id
         LEFT JOIN bronze.projection_load_2 p2
        ON p.id = p2.id
        WHERE p2.tag_created_by NOT LIKE 'EDI%'
    )

    -- Select and rename columns
    SELECT DISTINCT
        'Aljex - Marked Driver_Dispatched' AS Touch_Type,
        CONCAT_WS(load_id,full_name, 'Aljex - Marked Driver_Dispatched') AS Touches_id,
        full_name,
        CAST(Dispatched_date AS DATE) AS Date
    FROM joined_data
    where Dispatched_date IS NOT NULL 
    and full_name is not null;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Aljex_Driver_Dispatched: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DataFrame Creation for Slack Touches

# COMMAND ----------

try:
    DF_Slack_Touches = spark.sql(""" 
    -- Select User_ID, Channel_name, Channel_type, Datetime from the original dataframe
    WITH selected_data AS (
        SELECT
            User_ID,
            Channel_name,
            Channel_type,
            CAST(Datetime AS Date) as DT1,
        CASE
            WHEN minute(Datetime) BETWEEN 0 AND 14 THEN '00 - 15'
            WHEN minute(Datetime) BETWEEN 15 AND 29 THEN '15 - 30'
            WHEN minute(Datetime) BETWEEN 30 AND 44 THEN '30 - 45'
            WHEN minute(Datetime) BETWEEN 45 AND 59 THEN '45 - 60'
        END AS FifteenMinGroup
        FROM bronze.slack_actions_2
        where channel_type = 'Slack Public Channel'
    )

    -- Aggregate the dataframe based on User_ID, Channel_name, Channel_type, and datetime (every 15 mins)
    -- Calculate the count of action ID
    , aggregated_data AS (
        SELECT
            User_ID,
            Channel_name,
            Channel_type,
            DT1,
            FifteenMinGroup,
            CAST(1 AS INT) AS Touches_Score
        FROM selected_data
        GROUP BY User_ID, Channel_name, Channel_type, DT1,FifteenMinGroup
    )

    -- Store the result in the DataFrame DF_Touches_Slack
    SELECT
        'Slack - Activities' AS Touch_Type,
        CONCAT_WS(User_ID, Channel_name, Channel_type, DT1,FifteenMinGroup) AS Touches_id,
        Null as full_name,
        User_ID as Email,
        Touches_Score,
        DT1 AS Date,
        CAST(6 AS INT) AS Source_system_id
    FROM aggregated_data;

    """)
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Slack_Touches: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Combine all Hubspot Dataframes

# COMMAND ----------

try:
    DF_Hubspot_Touches  = DF_Hubspot_email_touches.unionByName(DF_Hubspot_meeting_created_touches).unionByName(DF_Hubspot_meeting_attended_touches).unionByName(DF_Hubspot_notes_touches).unionByName(DF_Hubspot_deals_created_touches).unionByName(DF_Hubspot_deals_closed_touches).unionByName(DF_Hubspot_deals_updated_touches)
except Exception as e:
    # logger error message
    logger.error(f"Unable to take union of all the hubspot data frames and load into DF_Hubspot_Touches: {str(e)}")
    print(e)
try:
    DF_Hubspot_Touches_Final = DF_Hubspot_Touches.select(
        'Touch_type',
        'Touches_id',
        lit("").alias("full_name"),
        DF_Hubspot_Touches['User_email'].alias("Email"),
        DF_Hubspot_Touches['Touches_Score'].alias("Touches_score"),
        DF_Hubspot_Touches['Timestamp'].alias("Date"),
        'Source_system_id'
    )

    DF_Hubspot_Touches_Final = DF_Hubspot_Touches_Final.dropDuplicates(['Touches_id'])
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Hubspot_Touches_Final: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Combine all Aljex Dataframes

# COMMAND ----------

try:
    DF_Aljex_Touches  = DF_Aljex_Load_Booked.unionByName(DF_Aljex_Marked_Loaded).unionByName(DF_Aljex_Marked_Delivered).unionByName(DF_Aljex_Driver_Dispatched)
except Exception as e:
    # logger error message
    logger.error(f"Unable to take union of all the aljex data frames and load into DF_Aljex_Touches: {str(e)}")
    print(e)
try:
    DF_Aljex_Touches_Final = DF_Aljex_Touches.select(
        "Touch_type",
        "Touches_id",
        "full_name",  # Assuming the column is named 'full_name'
        "Date",
        lit(1).alias("Source_system_id")
    )
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Aljex_Touches_Final: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Combine all Relay Dataframes

# COMMAND ----------

try:
    DF_Relay_Touches  = DF_Relay_Manual_Tender_Created.unionByName(DF_Relay_Tender_Accepted).unionByName(DF_Relay_Appt_Scheduled).unionByName(DF_Relay_Load_Booked).unionByName(DF_Relay_Assigned_Load).unionByName(DF_Relay_Max_Buy).unionByName(DF_Relay_Load_Cancelled).unionByName(DF_Relay_Rate_Con_Sent).unionByName(DF_Relay_Reserved_Load).unionByName(DF_Relay_Bounced_Load).unionByName(DF_Relay_Rolled_Load).unionByName(DF_Relay_Put_Offer_Load).unionByName(DF_Relay_Tracking).unionByName(DF_Relay_Stop_Marked_Delivered)
except Exception as e:
    # logger error message
    logger.error(f"Unable to take union of all the relay data frames and load into DF_Relay_Touches: {str(e)}")
    print(e)
try:
    DF_Relay_Touches_Final = DF_Relay_Touches.select(
        "Touch_type",
        "Touches_id",
        "full_name",  # Assuming the column is named 'full_name'
        "Date",
        lit(2).alias("Source_system_id")
    )
except Exception as e:
    # logger error message
    logger.error(f"Unable to load data into DF_Relay_Touches_Final: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Combine all TMS Dataframes

# COMMAND ----------

try:
    DF_TMS_Touches = DF_Relay_Touches_Final.unionByName(DF_Aljex_Touches_Final)
except Exception as e:
    # logger error message
    logger.error(f"Unable to take union of DF_Relay_Touches_Final,DF_Aljex_Touches_Final data frames and load into DF_TMS_Touches: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Lookup Employee Email for TMS Users

# COMMAND ----------

try:
    # Read the bronze.employee table
    DF_Employee = spark.sql("""select LOWER(REGEXP_REPLACE(TMS_Name, "'", '')) AS TMS_Name, Email_Address as Email from
    analytics.dim_employee_brokerage_mapping""")

    # Perform the join based on the full_name column
    DF_TMS_Touches_Merged = DF_TMS_Touches.join(DF_Employee, lower(DF_TMS_Touches['full_name']) == DF_Employee['TMS_Name'] , 'left')
except Exception as e:
    # logger error message
    logger.error(f"Unable to perform join and load data into DF_TMS_Touches_Merged: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Format the Final TMS Tocuhes Table

# COMMAND ----------

try:
    DF_TMS_Touches_Final = DF_TMS_Touches_Merged.select(
        'Touch_type',
        'Touches_id',
        'full_name',
        'Email',
        lit(1).alias("Touches_score"),
        'Date',
        "Source_system_id"
    )
except Exception as e:
    # logger error message
    logger.error(f"Unable to format Final TMS Touches Merged and load data into DF_TMS_Touches_Final: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Combine all Touches (Aljex, Relay, Hubspot & Slack)

# COMMAND ----------

try:
    DF_Touches_Combined = DF_TMS_Touches_Final.unionByName(DF_Hubspot_Touches_Final).unionByName(DF_Slack_Touches)
except Exception as e:
    # logger error message
    logger.error(f"Unable to combine all Touches and load data into DF_Touches_Combined: {str(e)}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create a Temp View For Final Touches Table

# COMMAND ----------

##Create Temporary view for the Lookup_Equipment table
try:
    DF_Touches_Combined.createOrReplaceTempView('VW_Source_Fact_Touches')
except Exception as e:
    logger.info(f"Unable to create the view for the DF_Touches_Combined")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Merge the Touches data into Fact_Touches table in Gold Layer

# COMMAND ----------


try:
    # Merge the records with the actual table
    spark.sql('''Merge into gold.fact_touches as CT using VW_Source_Fact_Touches as CS ON CS.Touches_ID=CT.Touches_ID  
        WHEN MATCHED THEN 
            UPDATE SET 
            CT.Action_Code = 'U',
            CT.Last_Modified_By = 'Databricks',
            CT.Last_Modified_Date = current_timestamp()
        WHEN NOT MATCHED 
            THEN
            INSERT
            (   
            CT.Touches_ID,
            CT.Touches_Type,
            CT.Employee_Name,
            CT.Email,
            CT.Touches_Score,
            CT.Touch_Timestamp,
            CT.Action_Code,
            CT.Sourcesystem_ID,
            CT.Created_By,
            CT.Created_Date,
            CT.Last_Modified_By,
            CT.Last_Modified_Date
            )
            VALUES
            (
            CS.Touches_id,
            CS.Touch_type,
            CS.full_name,
            CS.Email,
            CS.Touches_score,
            CS.Date,
            'I',
            CS.Source_system_id ,
            'Databricks',
            current_timestamp(),
            'Databricks',
            current_timestamp()
            )
            ''')
    
except Exception as e:
    logger.info(f"Unable to merge the data for Fact_Touches")
    print(e)

# COMMAND ----------

