# Databricks notebook source
# MAGIC %md
# MAGIC ## Shipper Lookup Automation
# MAGIC * **Description:** Extract the New Shippers from the Operational Data and Load into the Lookup Table. 
# MAGIC * **Created Date:** 09/01/2024
# MAGIC * **Created By:** Gagana Nair
# MAGIC * **Modified Date:** 09/01/2024
# MAGIC * **Modified By:** Gagana Nair
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing Packages

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calling Utilities notebook

# COMMAND ----------

# MAGIC %run 
# MAGIC "//Workspace/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calling Logger Notebooks

# COMMAND ----------

# MAGIC
# MAGIC %run 
# MAGIC "/Workspace/Shared/Common Notebooks/Logger"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Declaring Error Logger Variables

# COMMAND ----------


#Creating variables to store error log information
ErrorLogger = ErrorLogs("Shipper_Lookup")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the Metadata Details

# COMMAND ----------

DF_Metadata= spark.sql(
    "SELECT * FROM Metadata.MasterMetadata where TableID='SL1' and IsActive='1' "
)

# COMMAND ----------

TableID  =  (DF_Metadata.select(col("TableID")).where(col("TableID") == "SL1").collect()[0].TableID)
Job_ID = (DF_Metadata.select(col("Job_ID")).where(col("TableID") == "SL1").collect()[0].Job_ID)
Notebook_ID = (DF_Metadata.select(col("NB_ID")).where(col("TableID") == "SL1").collect()[0].NB_ID)
TableName = (DF_Metadata.select(col("DWHTableName")).where(col("TableID") == "SL1").collect()[0].DWHTableName)
Zone = (DF_Metadata.select(col("Zone")).where(col("TableID") == "SL1").collect()[0].Zone)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the New Shippers Data

# COMMAND ----------

try: 
  customers = spark.sql( '''(SELECT A.Customer_Master, A.Customer_Name FROM (SELECT DISTINCT UPPER(TRIM(Customer_Master)) AS Customer_Master, UPPER(TRIM(Customer_Name)) AS Customer_Name FROM analytics.fact_load_genai_nonsplit WHERE Customer_Master IS NOT NULL AND Customer_Name IS NOT NULL ) A LEFT  JOIN 
  (SELECT UPPER(TRIM(Customer_Master)) AS Customer_Master, upper(TRIM(Customer_Name)) AS Customer_Name FROM bronze.lookup_shippers) B ON A.Customer_Master = B.Customer_Master AND A.Customer_Name = B.Customer_Name WHERE B.Customer_Name IS NULL AND B.Customer_Master IS NULL) ''')
  customers.createOrReplaceTempView('customers')

except Exception as e:
  logger.error(f"Error in creating customers dataframe: {e}")
  print(f"Error in creating customers dataframe: {e}")
  Error_Statement = str(e).replace(',', '').replace("'", '')
  UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "New Shippers Extraction Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Data

# COMMAND ----------

try: 
  spark.sql( '''INSERT INTO bronze.lookup_shippers
  (SELECT * FROM customers) ''')
  UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Customer Master Data Load")
  UpdatePipelineStatusAndTime(TableID, Zone)

except Exception as e:
  logger.error(f"Error in inserting customers dataframe: {e}")
  print(f"Error in inserting customers dataframe: {e}")
  UpdateFailedStatus(TableID,Zone)
  Error_Statement = str(e).replace(',', '').replace("'", '')
  UpdateLogStatus(Job_ID, TableID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Customer Master Data Load Failed")