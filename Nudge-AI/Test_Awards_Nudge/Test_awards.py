# Databricks notebook source
# MAGIC %md
# MAGIC Awards Nudge - Test Nudge
# MAGIC

# COMMAND ----------

# Seq 1.1
from Utilities.nudgeUtility import formatEscapeString, formatLog
from Repository.nudgeRepo import executeQuery
from Controller.nudgeController import awardsNudge
from pyspark.sql import SparkSession

# COMMAND ----------

# Seq 1.2

import uuid
from datetime import datetime
import json

def logFailure(logs, reason, financial_date_result):
    """Log failure information to the awardsnudgetriggersummary table"""
    try:
        print(f"Logging failure: {reason}")
        logs.append(formatLog(f"Logging failure: {reason}"))
        
        nudgeId = str(uuid.uuid4())
        startTime = datetime.now()
        
        # Fetch Financial Year and Financial Period Number
        financial_year = financial_date_result[0].get('Financial_Year')
        financial_period_number = financial_date_result[0].get('Financial_Period_Number')
        
        # Convert to string format "8-2025" and append " - Test"
        period_year = f"{financial_period_number}-{financial_year} - Test"
        
        summaryInsertQuery = """INSERT INTO brokerageprod.nudge_ai.awardsnudgetriggersummary 
        (NudgeId, NudgeDate, TriggerQuery, NudgedShipperCount, NudgedShippers, 
        FailedShipperCount, FailedShippers, Errors, NewlyAddedSPChannelsCount, 
        NewlyAddedChannels, ExecutionTimeMin, PeriodYear, BlobLink) 
        VALUES (
            :nudge_id, :nudge_date, :triggerQuery, :nudge_shipper_count, :nudge_shippers, 
            :failed_shipper_count, :failed_shippers, :errors, 
            :newly_added_sp_channels_count, :newly_added_channels, 
            :execution_time_min, :period_year, :blob
        )"""
        
        nudgeValues = {
            "nudge_id": nudgeId,
            "nudge_date": startTime,
            "triggerQuery": formatEscapeString("No Query Executed"),
            "nudge_shipper_count": 0,
            "nudge_shippers": json.dumps([]),
            "failed_shipper_count": 0,
            "failed_shippers": json.dumps([]),
            "errors": json.dumps([reason]),
            "newly_added_sp_channels_count": 0,
            "newly_added_channels": json.dumps([]),
            "execution_time_min": 0,
            "period_year": period_year,  # Use the formatted period_year
            "blob": ""
        }
        
        executeQuery(logs, summaryInsertQuery, nudgeValues)
        print("Failure logged successfully")
        logs.append(formatLog("Failure logged successfully"))
        
    except Exception as e:
        print(f"Error logging failure: {str(e)}")
        logs.append(formatLog(f"Error logging failure: {str(e)}", "ERROR"))

# COMMAND ----------

def automate_awards_nudge(dbutils):
    logs = []
    print("Starting automated awards nudge process")
    logs.append(formatLog("Starting automated awards nudge process"))
    
    try:
        # Step 1: Get current date in YYYY-MM-DD format
        current_date = datetime.now().strftime('%Y-%m-%d')
        print(f"Current date: {current_date}")
        logs.append(formatLog(f"Current date: {current_date}"))
        
        # current_date = '2025-10-07'  # Example date for testing
                
        financial_date_query = f"""
        SELECT * FROM brokerageprod.analytics.dim_financial_calendar 
        WHERE Date = '{current_date}'
        """
        
        financial_date_result = executeQuery(logs, financial_date_query)
        
        if not financial_date_result:
            print(f"Date {current_date} not found in financial calendar")
            logs.append(formatLog(f"Date {current_date} not found in financial calendar"))
            logFailure(logs, f"Date {current_date} not found in financial calendar", [])
            return False
        
        print(f"Date {current_date} found in financial calendar")
        logs.append(formatLog(f"Date {current_date} found in financial calendar"))
        
        # Fetch financial_period_week_number
        financial_period_week_number = financial_date_result[0].get('financial_Period_week_number')
        print(f"Financial period week number: {financial_period_week_number}")
        
        # Check if financial_period_week_number is equal to 1
        if financial_period_week_number != 1:
            print(f"Financial period week number is {financial_period_week_number}, not equal to 1. Skipping nudge.")
            logs.append(formatLog(f"Financial period week number is {financial_period_week_number}, not equal to 1. Skipping nudge."))
            logFailure(logs, f"Financial period week number is {financial_period_week_number}, not equal to 1", financial_date_result)
            return False
        
        # Proceed with nudge if the condition is satisfied
        print("Financial period week number is 1. Proceeding with nudge.")
        logs.append(formatLog("Financial period week number is 1. Proceeding with nudge."))
        
        nudgeSqlQuery = f"""
        SELECT DISTINCT * FROM nudge_ai.awards_slack_sam_mappings 
        WHERE UPPER(shipper_name) IN 
        (SELECT UPPER(CUSTOMER_MASTER) FROM Gold.fact_awards_summary 
         WHERE Period_Year IN ('8-2025') AND Is_Deleted = 0) 
        LIMIT 10
        """
        
        print(f"Executing nudge with query: {nudgeSqlQuery}")
        logs.append(formatLog(f"Executing nudge with query: {nudgeSqlQuery}"))
        
        result = awardsNudge(dbutils, nudgeSqlQuery, current_date)
        
        if result:
            print("Awards nudge executed successfully")
            logs.append(formatLog("Awards nudge executed successfully"))
            return True
        else:
            print("Awards nudge execution failed")
            logs.append(formatLog("Awards nudge execution failed"))
            return False
            
    except Exception as e:
        error_message = f"Error in automate_awards_nudge: {str(e)}"
        print(error_message)
        logs.append(formatLog(error_message, "ERROR"))
        logFailure(logs, error_message, [])
        return False

# COMMAND ----------

automate_awards_nudge(dbutils)