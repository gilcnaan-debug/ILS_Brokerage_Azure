# Databricks notebook source
# MAGIC %md
# MAGIC # Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contacts New

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=5 and is_Deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=5 and is_Deleted=0 and archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=4 and is_Deleted=0 and archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=4 and is_Deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts group by Contact_Key having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_contacts
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Contacts"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Postal Mails

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze.hubspot_postal_mail where SourcesystemID=4 and is_deleted=0;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze.hubspot_postal_mail where SourcesystemID=5 and is_deleted=0;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze.hubspot_postal_mail where SourcesystemID=4;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH postal_mails AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and bronze.hubspot_associations.Sourcesystem_ID = bronze.hubspot_companies.SourcesystemID LEFT JOIN bronze.hubspot_postal_mail ON bronze.hubspot_associations.Associated_ID = bronze.hubspot_postal_mail.hs_object_id and bronze.hubspot_associations.Sourcesystem_ID = hubspot_postal_mail.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Postal mail') select *from postal_mails WHERE Sourcesystem_ID=5

# COMMAND ----------

# MAGIC  %sql
# MAGIC select * from bronze.hubspot_postal_mail where hs_object_id = 62352898986;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_postal_mail where hs_object_id = 42910341659;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_postal_mail group by hs_object_id having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_postal_mail;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct hs_object_id) from bronze.hubspot_postal_mail

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Load Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_postal_mail where Is_Deleted=0 and SourcesystemID=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_postal_mail where Is_Deleted=0 and SourcesystemID=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select hs_lastmodifieddate from bronze.hubspot_postal_mail

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletes and Archived Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_postal_mail where is_deleted=1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Postal_Mail'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Postal_Mail'

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Postal_Mail"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Postal_Mail" order by created_at desc limit 10
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_postal_mail
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row count check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_tasks where archived="false" and SourcesystemID=4 and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_tasks where archived="true" and SourcesystemID=4 and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_tasks where archived="true" and SourcesystemID=5 and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_tasks where SourcesystemID=5 and Is_Deleted=0 and archived="false";

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_tasks where SourcesystemID=4 and Is_Deleted=0 and archived="false";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_tasks where archived="true" and Is_Deleted=0 and hs_object_id=71305569571 and SourcesystemID=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_tasks where hs_object_id = 24170433357 and SourcesystemID=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_tasks where hs_object_id = 12364382599 and SourcesystemID=5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) as total_records from bronze.hubspot_tasks;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct hs_object_id) as total_records from bronze.hubspot_tasks;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_tasks where SourcesystemID=4 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_tasks where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletes And Archived Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_tasks where Is_Deleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_tasks where Is_Deleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select hc.hs_object_id from bronze.hubspot_tasks hc where hc.hs_object_id not in (select hs_object_id from SourceToBronze) and hc.SourcesystemID=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_tasks where archived="true" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronze.hubspot_tasks set archived="false" where archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_tasks where archived="true"

# COMMAND ----------

# %run "HubSpot - Deletes and Archived/HubSpot Deletes - Tasks"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Tasks"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Tasks"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_tasks where SourcesystemID=4 and archived="true" and is_deleted=0

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_tasks
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Tasks"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Tasks"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Tasks" order by created_at desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deals

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where SourcesystemID=4 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where SourcesystemID=4 and archived="true" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze.hubspot_deals where SourcesystemID=4 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze.hubspot_deals where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Bronze.hubspot_deals where archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze.hubspot_deals where SourcesystemID=5 and archived="true" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where SourcesystemID=5 and archived="True" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where SourcesystemID=5

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where SourcesystemID=4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_deals group by hs_object_id having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_deals;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct hs_object_id) from bronze.hubspot_deals;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_deals where hs_object_id=32257079887 and SourcesystemID=4 and Is_Deleted=0 and archived="true";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_deals where hs_object_id = 3373366924 and SourceSystemID=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_deals where hs_object_id = 10343213655 and SourceSystemID=4;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletes and Archived Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_deals where archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_deals where Is_Deleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronze.hubspot_deals set archived="false" where archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_deals where archived="true"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Load Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where archived="false" and Is_Deleted=0 and SourcesystemID=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where archived="false" and Is_Deleted=0 and SourcesystemID=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where archived="false" and Is_Deleted=0 and SourcesystemID=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where SourcesystemID=4 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_deals where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Deals'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Deals'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Deals" order by created_at desc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Deals"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_deals
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct archived from bronze.hubspot_notes

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_notes where archived="false" and Is_Deleted=0 and SourcesystemID=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_notes where archived="true" and Is_Deleted=0 and SourcesystemID=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_notes where archived="false" and Is_Deleted=0 and SourcesystemID=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_notes where archived="false" and Is_Deleted=0 and SourcesystemID=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_notes where archived="false" and Is_Deleted=0 and SourcesystemID=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT hs_object_id, hs_body_preview from bronze.hubspot_notes where SourcesystemID=5 and Is_Deleted=0 and archived=false

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_notes

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_notes group by hs_object_id having count(hs_object_id)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_notes where SourcesystemID=4 and hs_object_id=19217859628

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_notes where SourcesystemID=4 and hs_object_id in (19233774244, 19271096223, 19352621189)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_notes where SourcesystemID=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_notes where SourcesystemID=5 and hs_object_id=9606398282

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_notes where SourcesystemID=5 and hs_object_id in (9606398283, 9606398284, 9606398285)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_notes where is_deleted=1 or archived="true"

# COMMAND ----------

# MAGIC %md
# MAGIC ### metadata check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Notes'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Notes'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Notes" order by created_at desc limit 10

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Notes"

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Notes"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_notes
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Emails

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where SourcesystemID=5 and Is_Deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where SourcesystemID=4 and Is_Deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where SourcesystemID=5 and Is_Deleted=0 and archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where SourcesystemID=4 and Is_Deleted=0 and archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where sourcesystemID=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where SourcesystemID=5

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where sourcesystemID=5 and archived="true" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where archived="false" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails_i2 where archived="false" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct archived from bronze.hubspot_emails

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from hubspot_emails group by hs_object_id having count(hs_object_id)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_emails_i1 where archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails_i1 where SourcesystemID=4 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails_i1 where SourcesystemID=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where SourcesystemID=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails_i2 where SourcesystemID=5

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SourcesystemID from bronze.hubspot_emails

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select hs_object_id from bronze.hubspot_emails_i1 group by hs_object_id having count(hs_object_id)>1

# COMMAND ----------

# MAGIC %md
# MAGIC ### metadata check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where TableID in ("H5","H12")

# COMMAND ----------

# MAGIC %md
# MAGIC ### INcremental Load

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where SourcesystemID=5 and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where SourceSystemID=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_emails where SourceSystemID=5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_emails where hs_object_id = 19050072327 and archived="false"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_emails where archived = 'true' and hs_object_id =74611923534

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Emails" order by created_at desc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Emails" and Job_ID="JOB_H7"

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoad date check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Emails" and Job_ID="JOB_H7"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_emails
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_emails where hs_object_id=9609471700 and SourcesystemID=5 and Is_Deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_emails where hs_object_id=19050072327 and SourcesystemID=4 and Is_Deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_emails where hs_object_id=75396397457 and SourcesystemID=5 and Is_Deleted=0 and archived="true"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Meetings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_meetings where SourcesystemID=5 and is_deleted=0 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_meetings where SourcesystemID=4 and is_deleted=0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_meetings group by hs_object_id having count(hs_object_id)>1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_meetings;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct hs_object_id) from bronze.hubspot_meetings;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where hs_object_id in (select a.Company_ID from bronze.hubspot_meetings m inner join bronze.hubspot_associations a on m.hs_object_id=a.associated_id where a.Associated_ID=10548364732)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where hs_object_id in (select a.Company_ID from bronze.hubspot_meetings m inner join bronze.hubspot_associations a on m.hs_object_id=a.associated_id where m.SourcesystemID=4 and m.hs_object_id=62268122734)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_meetings where SourcesystemID=4 and hs_object_id=62268122734

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_meetings where SourcesystemID=5 and hs_object_id=10548612797 and archived=false and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_meetings where SourcesystemID=5 and hs_object_id=10548364732 and archived=false and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_meetings where SourcesystemID=5 and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_meetings where SourcesystemID=4 and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_meetings where is_deleted = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Meetings' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Meetings' 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Meetings" order by created_at desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Meetings"

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Meetings"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_meetings
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Owners" order by created_at desc limit 10

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Owners"

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_owners
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Owners

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=4 and archived="false" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=5 and archived="false" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=4 and archived="false" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=4 and archived="true" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=5 and archived="false" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=5

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=5 and archived="true" and Is_Deleted=0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where hs_object_id in (select a.Company_ID from bronze.hubspot_meetings m inner join bronze.hubspot_associations a on m.hs_object_id=a.associated_id where m.hs_object_id=10548364732)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where hs_object_id in (select a.Company_ID from bronze.hubspot_owners m inner join bronze.hubspot_associations a on m.id=a.associated_id where m.id=323410330)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_owners where SourcesystemID=5 and id =323410330

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_owners where SourcesystemID=4 and id in (129243809,129243810)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(id) from bronze.hubspot_owners group by id having count(id)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(id) from bronze.hubspot_owners

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct id) from bronze.hubspot_owners

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where dwhtablename="HubSpot_Owners"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Load CHeck

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=5 and archived="false" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_owners where SourcesystemID=5 and archived="true" and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where dwhtablename="HubSpot_Owners"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletes and Archvied

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_owners where archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC update bronze.hubspot_owners set archived ="false" and is_deleted=0;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_owners where archived="true" or is_deleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_owners where archived="true"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Maxloaddate check

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT MAX(updated_at) AS Max_lastmodifieddate
# MAGIC FROM bronze.hubspot_owners

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Check

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Owners"
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Owners"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### logger check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Owners" order by created_at desc limit 10
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Communications

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_communications where SourcesystemID=5 and archived="true" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_communications where SourcesystemID=5 and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_communications where SourcesystemID=4 and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_communications where SourcesystemID=5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate CHeck

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct hs_object_id)from bronze.hubspot_communications

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id)from bronze.hubspot_communications

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_communications where SourcesystemID=5 and hs_object_id in (54241693877, 54241694351, 54241695353)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_communications where SourcesystemID=4 and hs_object_id=57284102965

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_communications where SourcesystemID=5 and hs_object_id=54176868667

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_communications where is_deleted=1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Load Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select hs_lastmodifieddate from bronze.hubspot_communications

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Communications"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Communications"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Communications" order by created_at desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Communications"

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_communications
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contacts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=4 and Is_Deleted=0 and archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=5

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=5

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct archived from bronze.hubspot_contacts

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=4 and is_deleted=0 and archived="true";

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=5 and is_deleted=0 and archived="false";

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=5 and is_deleted=0 and archived="true";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_contacts where hs_object_id = 2277201

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_contacts group by SourcesystemID, hs_object_id having count(hs_object_id)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_contacts group by hs_object_id having count(hs_object_id)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select MAX(hs_object_id) from bronze.hubspot_contacts group by hs_object_id having count(hs_object_id)>1 order by hs_object_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_contacts where hs_object_id in (1106151, 1108301, 1113401, 1116251, 1127951, 1133001, 1142601, 1143501)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies hc inner join bronze.hubspot_associations ha on hc.hs_object_id = ha.Company_ID inner join bronze.hubspot_contacts hca on hca.hs_object_id=ha.Associated_ID where hca.hs_object_id=97487773764 and hca.SourceSystemID=4 and ha.Associated_type='Contact'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_contacts where hs_object_id=5470601 and SourceSystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_contacts where hs_object_id=97487773764 and SourceSystemID=4 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_contacts where hs_object_id in (1,51) and SourceSystemID=4 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Load Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=4 and Is_Deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_contacts where SourcesystemID=5 and Is_Deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Metadata Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Contacts'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Contacts'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletes and Archived Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_contacts where archived="true"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_contacts where is_deleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_contacts where is_deleted=1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Contacts" order by created_at desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate Check

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Contacts"

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(lastmodifieddate) AS Max_lastmodifieddate
# MAGIC FROM bronze.hubspot_contacts
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Companies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=4 and is_deleted=0 and archived="true";

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=4 and is_deleted=0 and archived="True";

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=4 and is_deleted=0 and archived="false";

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=4 and is_deleted=0 and archived= "false";

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=5 and is_deleted=0 and archived="false";

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=5 and is_deleted=0 and archived="True";

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=5 and Is_Deleted=0 and archived="False"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=5 and Is_Deleted=0 and archived="False"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where hs_object_id=30096214539 and SourcesystemID=5 and Is_Deleted=0 and archived="true";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where hs_object_id=30096214539 and SourcesystemID=5 and Is_Deleted=0 and archived="true";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct hs_object_id) from bronze.hubspot_companies

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_companies

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Archived Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where SOurceSystemID=4 and archived="true" and is_deleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct archived from bronze.hubspot_companies where SourcesystemID=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where archived="true" and SourcesystemID=4 and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE bronze.hubspot_companies SET archived="false" WHERE archived="true" AND SourcesystemID=5 AND Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where archived="true" and SourcesystemID=5 and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where archived="true" and SourcesystemID=5 and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where archived="true" and SourcesystemID=5 and Is_Deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where archived="true" and SourcesystemID=4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Load Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Companies';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Bronze.hubspot_companies where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Bronze.hubspot_companies where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Bronze.hubspot_companies where SourcesystemID=5 and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=5 and is_deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where hs_object_id in (5220300132,5392408476,4988683347,4988654272) and Is_Deleted=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select hc.hs_object_id from bronze.hubspot_companies hc where hc.hs_object_id not in (select hs_object_id from SourceToBronze) and hc.SourcesystemID=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_companies where hs_object_id in (5220300132,5392408476,4988683347,4988654272) and Is_deleted=0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Bronze.hubspot_companies where SourcesystemID=5 and archived="true" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Companies';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where TableID like "H%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_companies where SourcesystemID=4 and Is_Deleted=0 and archived="false"

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_companies
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Companies'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName='HubSpot_Companies'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error Logger and FailedStatus Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name='HubSpot_Companies'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Bronze.hubspot_calls where SourcesystemID=4 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Bronze.hubspot_calls where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Bronze.hubspot_calls where SourcesystemID=4 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct archived from bronze.hubspot_calls

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Bronze.hubspot_calls where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Bronze.hubspot_calls where SourcesystemID=5 and archived="true" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_calls where SourcesystemID=5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Bronze.hubspot_calls where SourcesystemID=5 and archived="false" and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_calls where SourcesystemID=4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_calls group by hs_object_id having count(hs_object_id)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(hs_object_id) from bronze.hubspot_calls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_calls where hs_object_id="9636569996"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_calls where SourcesystemID=4 and hs_object_id=19910132864

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_calls where SourcesystemID=5 and hs_object_id=9609471529

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH Companies AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and hubspot_associations.Sourcesystem_ID = hubspot_companies.SourcesystemID LEFT JOIN bronze.hubspot_calls ON hubspot_associations.Associated_ID = hubspot_calls.hs_object_id and hubspot_associations.Sourcesystem_ID = hubspot_calls.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Call' )
# MAGIC SELECT Company_Name, hs_body_preview  FROM Companies WHERE name = 'TOP-TEN EXPRESS CORP'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletes and Archived Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_calls where Is_Deleted=1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Calls"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Calls"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="HubSpot_Calls" order by created_at desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName="HubSpot_Calls"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SourcesystemID, MAX(hs_lastmodifieddate) AS Max_hs_lastmodifieddate
# MAGIC FROM bronze.hubspot_calls
# MAGIC WHERE SourcesystemID in (4,5)
# MAGIC GROUP BY SourcesystemID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Associations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Check

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName = "Hubspot_Associations"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_associations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_associations group by Association_Key HAVING count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data - Tasks

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_associations where Associated_Type="Tasks" and Company_Name = "G3 Enterprises"

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tasks AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and bronze.hubspot_associations.Sourcesystem_ID = bronze.hubspot_companies.SourcesystemID LEFT JOIN bronze.hubspot_tasks ON bronze.hubspot_associations.Associated_ID = bronze.hubspot_tasks.hs_object_id and bronze.hubspot_associations.Sourcesystem_ID = hubspot_tasks.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Tasks') 
# MAGIC select Company_ID, Company_Name, Associated_ID, hs_task_subject  from tasks WHERE Sourcesystem_ID=4 and Company_Name = "G3 Enterprises"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data - Postal Mails

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.hubspot_associations where Company_Name="DAYBREAK EXPRESS, INC." and Associated_Type="Postal mail"

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH postal_mails AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and bronze.hubspot_associations.Sourcesystem_ID = bronze.hubspot_companies.SourcesystemID LEFT JOIN bronze.hubspot_postal_mail ON bronze.hubspot_associations.Associated_ID = bronze.hubspot_postal_mail.hs_object_id and bronze.hubspot_associations.Sourcesystem_ID = hubspot_postal_mail.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Postal mail') select company_id, company_name, hs_body_preview from postal_mails WHERE Sourcesystem_ID=5 and Company_Name = "DAYBREAK EXPRESS, INC.";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count - Postal Mails

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH postal_mails AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and hubspot_associations.Sourcesystem_ID = hubspot_companies.SourcesystemID LEFT JOIN bronze.hubspot_postal_mail ON hubspot_associations.Associated_ID = hubspot_postal_mail.hs_object_id and hubspot_associations.Sourcesystem_ID = hubspot_postal_mail.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Postal_Mail') 
# MAGIC select * from postal_mails

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_associations where Associated_Type="Postal mail"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_associations where Associated_Type="Postal" and Is_Deleted=0 and Sourcesystem_ID=5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row count - Tasks

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tasks AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and bronze.hubspot_associations.Sourcesystem_ID = bronze.hubspot_companies.SourcesystemID LEFT JOIN bronze.hubspot_tasks ON bronze.hubspot_associations.Associated_ID = bronze.hubspot_tasks.hs_object_id and bronze.hubspot_associations.Sourcesystem_ID = hubspot_tasks.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Tasks') 
# MAGIC select count(*) from tasks WHERE Sourcesystem_ID=5

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tasks AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and bronze.hubspot_associations.Sourcesystem_ID = bronze.hubspot_companies.SourcesystemID LEFT JOIN bronze.hubspot_tasks ON bronze.hubspot_associations.Associated_ID = bronze.hubspot_tasks.hs_object_id and bronze.hubspot_associations.Sourcesystem_ID = hubspot_tasks.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Tasks') 
# MAGIC select count(*) from tasks WHERE Sourcesystem_ID=4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data - Communication

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH communications AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and  hubspot_companies.SourcesystemID= hubspot_associations.Sourcesystem_ID LEFT JOIN bronze.hubspot_communications ON hubspot_communications.hs_object_id = hubspot_associations.Associated_ID and hubspot_associations.Sourcesystem_ID = hubspot_communications.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Communication') 
# MAGIC select company_ID, Company_Name, Associated_ID, hs_body_preview, hs_communication_channel_type  from communications where SourceSystem_ID=4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Count - Communication

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_associations where Associated_Type="Communications" and Is_Deleted=0 and Sourcesystem_ID=5

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze.hubspot_communications where SourceSystemID=5 and is_deleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH communications AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and  hubspot_companies.SourcesystemID= hubspot_associations.Sourcesystem_ID LEFT JOIN bronze.hubspot_communications ON hubspot_communications.hs_object_id = hubspot_associations.Associated_ID and hubspot_associations.Sourcesystem_ID = hubspot_communications.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Communications') 
# MAGIC select count(*) from communications where SourceSystem_ID=5
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH communications AS (SELECT * FROM bronze.hubspot_companies left join bronze.hubspot_associations on bronze.hubspot_companies.hs_object_id= bronze.hubspot_associations.Company_ID and  hubspot_companies.SourcesystemID= hubspot_associations.Sourcesystem_ID LEFT JOIN bronze.hubspot_communications ON hubspot_communications.hs_object_id = hubspot_associations.Associated_ID and hubspot_associations.Sourcesystem_ID = hubspot_communications.SourcesystemID WHERE hubspot_associations.Associated_Type = 'Communications') 
# MAGIC select count(*) from communications where SourceSystem_ID=4

# COMMAND ----------

# MAGIC %md
# MAGIC ### ROw Count - Tasks

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze.hubspot_associations where Associated_Type="Task" and SourceSystem_ID=5

# COMMAND ----------

# MAGIC %md
# MAGIC ### MaxLoadDate Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(Last_Modified_Date) from bronze.hubspot_associations

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.mastermetadata where DWHTableName  = "Hubspot_Associations"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logger Check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from metadata.error_log_table where Table_Name="Hubspot_Associations"