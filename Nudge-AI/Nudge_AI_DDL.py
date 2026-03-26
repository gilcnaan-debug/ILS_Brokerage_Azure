# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE or replace TABLE nudge_ai.sam_mapping_slack ( 
# MAGIC 	Shipper_Name STRING, 
# MAGIC 	acct_mgr STRING, 
# MAGIC 	Email_Address STRING, 
# MAGIC 	Loadnum BIGINT, 
# MAGIC 	tender_booked_at STRING, 
# MAGIC 	is_nudging INT, 
# MAGIC 	Last_Nudge_Date STRING, 
# MAGIC 	Slack_Channel_ID STRING, 
# MAGIC 	Slack_Channel_Name STRING, 
# MAGIC 	Alias_Name STRING, 
# MAGIC 	Last_predicted_date STRING,
# MAGIC 	opt_out_feedback string,
# MAGIC 	invalid_feedback string,
# MAGIC 	snooze string)

# COMMAND ----------

