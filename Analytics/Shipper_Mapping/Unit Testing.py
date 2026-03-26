# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from bronze.shipper_mapping where Is_deleted = '0'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.shipper_mapping order by Customer_Master asc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.silver_shipper_mapping order by Customer_Master asc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from silver.silver_shipper_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), merge_key from bronze.shipper_mapping group by merge_key having count(*) > 1
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), merge_key from silver.silver_shipper_mapping group by merge_key having count(*) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC Show create table bronze.shipper_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE brokerageprod.bronze.Exception_Shipper (
# MAGIC   Customer_Master STRING,
# MAGIC   Shipper_Rep STRING,
# MAGIC   Customer_Office STRING,
# MAGIC   Sales_Rep STRING,
# MAGIC   Email_Address STRING,
# MAGIC   Hubspot_Company_Name STRING,
# MAGIC   Hubspot_AM STRING,
# MAGIC   Slack_Channel_Name STRING,
# MAGIC   Vertical STRING,
# MAGIC   Merge_Key STRING,
# MAGIC   Inserted_At TIMESTAMP)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.Exception_Shipper

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.silver_shipper_mapping where Customer_Master = 'ABB Industrial Solutions'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.silver_shipper_mapping where Customer_Master = 'ABB Industrial Solutions'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.exception_shipper

# COMMAND ----------

