# Databricks notebook source
# MAGIC %md
# MAGIC #### Import the required packages

# COMMAND ----------

#Importing packages
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
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

ErrorLogger = ErrorLogs("NFI_Sam_Mapping_Slack")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope = "Brokerage-Catalog", key = "CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data from Relay

# COMMAND ----------

# Select and perform required transformations and create view out of it.
try:
    DF_Source = spark.sql('''
                with Sam_mapping AS (
                SELECT
                mastername,
                acct_mgr,
                Loadnum,
                tender_booked_date
                FROM
				(
				select
                --p1.id::float as loadnum,
                --p2.shipper as customer_name,
                case when customer_lookup.master_customer_name is null then shipper else customer_lookup.master_customer_name end as mastername,
                case when z.full_name is null then p2.srv_rep else z.full_name end as acct_mgr,
                p1.id::float as loadnum,
                (TO_DATE(TRY_CAST(p2.tag_creation_date AS DATE))) as tender_booked_date
                --p2.tag_created_by  as Tender_Date
                from bronze.projection_load_1 p1
                left join bronze.projection_load_2 p2 on p1.id = p2.id
                left join bronze.customer_lookup on left(shipper,32) = left(customer_lookup.aljex_customer_name,32)
                left join bronze.aljex_user_report_listing z on p2.srv_rep = z.aljex_id 
                where
                p2.status not like 'VOID%'
                and p1.office not in ('10','34','51','54','61','62','63','64','74','TG') and
                (TO_DATE(TRY_CAST(p2.tag_creation_date AS DATE))) >= current_date()- INTERVAL '1 Year'
                ) as subquery
                
                Union 

                SELECT mastername,
                    acct_mgr,
                    Loadnum,
                    tender_booked_date
                    FROM (
                        Select 
                        --r.customer_name as customer_name,
                case when customer_lookup.master_customer_name is
                null then upper(r.customer_name) else customer_lookup.master_customer_name end as mastername,
                z.full_name as acct_mgr,
                ta.relay_reference_number as Loadnum,
                (TO_DATE(ta.tendered_at, 'yyyy-MM-dd')) as tender_booked_date
                --to_date(ta.tendered_at) as Tendering_Date
                ---to_date(ta.accepted_at)
                from bronze.booking_projection b
                left join bronze.customer_profile_projection r on b.tender_on_behalf_of_id = r.customer_slug and r.status = 'published'
                left join bronze.customer_lookup on left(r.customer_name,32) = left(aljex_customer_name,32)
                left join bronze.tendering_acceptance ta on b.relay_reference_number = ta.relay_reference_number
                left join bronze.relay_users z on r.primary_relay_user_id = z.user_id and z.`active?` = 'true'
                where ta.`accepted?` = 'true' and ta.accepted_at >= current_date - INTERVAL '1 year'
                ) as subquery
                )
                ,Latest_Tender as(
                                Select 
                                mastername,
                                MAX(TRY_CAST( tender_booked_date AS DATE)) as TD
                                from Sam_mapping as sm
                                where acct_mgr is not null
                                group by mastername
                                )
                ,Latest_employee as(
                                Select 
                                sm.mastername,
                                --sm.td as tender_date,
                                Max(acct_mgr) as Acct_Mgr
                                from Latest_Tender as sm
                                left join
                                Sam_mapping df
                                on sm. mastername  =  df. mastername and sm.td = df.tender_booked_date
                                where acct_mgr is not null
                                group by sm.mastername
                                )
                ,summary_data(
                    Select
                                df.mastername as Shipper_Name,
                                df.acct_mgr,
                                em.Email_Address,
                                count(Loadnum) as Loadnum,
                                concat_ws(', ', collect_list(TO_DATE(tender_booked_date, 'yyyy-MM-dd'))) AS tender_booked_at,
                                0 as   is_nudging,
                                '2024-01-01' as   Last_Nudge_Date,
                                '' as Slack_Channel_ID,
                                '' as  Slack_Channel_Name,
                                '' as  Alias_Name,
                                '2024-01-01' as  Last_predicted_date
                                from Sam_mapping as sm
                                left join
                                Latest_employee as df on sm.mastername = df.mastername
                                LEFT JOIN 
                                    analytics.dim_employee_brokerage_mapping AS em
                                ON 
                                    df.acct_mgr = em.TMS_Name
                                group by df.mastername,
                                df.acct_mgr,
                                em.Email_Address
                                --order by tender_booked_at desc
                                )
                                
                Select * from summary_data''')
    DF_Source.createOrReplaceTempView("VW_Source") 
except Exception as e:
    logger.info(f"Unable to perform the transformations and create view VW_Source: {e}")
    print(e)

# COMMAND ----------

try:
    # Merging the records with the actual table
    spark.sql('''MERGE INTO nudge_ai.sam_mapping_slack AS target USING VW_Source AS source ON target.Shipper_Name = source.Shipper_Name WHEN MATCHED THEN UPDATE SET
            target.acct_mgr = source.acct_mgr,
            target.Email_Address = source.Email_Address,
            target.Loadnum = source.Loadnum,
            target.tender_booked_at = source.tender_booked_at
    WHEN NOT MATCHED 
                THEN INSERT 
                (  
                target.Shipper_Name,
                target.acct_mgr,
                target.Email_Address,
                target.Loadnum,
                target.tender_booked_at,
                target.is_nudging,
                target.Last_Nudge_Date,
                target.Slack_Channel_ID,
                target.Slack_Channel_Name,
                target.Alias_Name,
                target.Last_predicted_date
                )
                Values
                (
					source.Shipper_Name,
					source.acct_mgr,
					source.Email_Address,
					source.Loadnum,
					source.tender_booked_at,
					source.is_nudging,
					source.Last_Nudge_Date,
					source.Slack_Channel_ID,
					source.Slack_Channel_Name,
					source.Alias_Name,
					source.Last_predicted_date
                    )
                ''')
except Exception as e:
    logger.info(f"Unable to perform merge operation to load data to sam_mapping_slack table")
    print(e)

# COMMAND ----------

