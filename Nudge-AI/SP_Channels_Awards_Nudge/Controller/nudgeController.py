from Service.nudgeService import nudgeSpChannels
from Repository.nudgeRepo import executeQuery
from Utilities.nudgeUtility import validateInputQuery,formatEscapeString,formatLog,uploadLogToBlob
import uuid
import json
from datetime import datetime

#Sequence Number 1.05 to 1.272 : 
"""The awardsNudge function processes a list of shippers by executing a provided SQL query to 
retrieve their data, then nudges each shipper with the awards and non awards to  specific channels based on a given date. 
It logs successes and failures, including any errors encountered during the process, and stores a summary 
of the operation in a database. The function returns True if successful and False if any errors occur."""

def awardsNudge(dbutils, query, nudgeDate):
    try:
        logs = []
        print("Awards Nudge function Triggered !!!")
        logs.append(formatLog("Awards Nudge function Triggered !!!"))
        print(f"""Nudge Start Time{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}""")
        logs.append(formatLog(f"""Nudge Start Time{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""))
        

        # Validate query
        isValidQuery = validateInputQuery(query)
        if not isValidQuery:
            print("Query validation failed: Not a valid SELECT statement.")
            logs.append(formatLog("Query validation failed: Not a valid SELECT statement."))
            return False
        
        # Execute query
        shipperData = executeQuery(logs, query)
        startTime = datetime.now()

        nudgedShipper = []
        failedShippers = []
        failedError = []
        failedErrorTraceback = []
        channelAddedCount = 0
        channelAddedShippers = []
        nudgeId = str(uuid.uuid4())
        periodYear = nudgeDate
        
        # Process each shipper
        for shipper in shipperData:
            result = nudgeSpChannels(dbutils,shipper,nudgeDate,logs)
            print(f"""Processed shipper: {shipper['shipper_name']} with result: {result}""")
            logs.append(formatLog(f"""Processed shipper: {shipper['shipper_name']} with result: {result}"""))

            periodYear = result['periodYear']

            if result["status"] == "success" :
                nudgedShipper.append(shipper['shipper_name'])
                if result['channelAdded'] :
                    channelAddedCount += 1
                    channelAddedShippers.append(shipper['shipper_name'])
            else:
                failedShippers.append(shipper['shipper_name'])
                failedError.append(result['error'])
                failedErrorTraceback.append(result['errorTraceback'])

                errorInsertQuery = """INSERT INTO brokerageprod.nudge_ai.awardsnudgeerrorlog (NudgeId, NudgeDate, ShipperName, ChannelId, Error, ErrorTraceback) 
                VALUES (:nudge_id, :nudge_date, :shipper_name, :channel_id, :error, :error_traceback)"""

                error_values = {
                                "nudge_id": nudgeId,
                                "nudge_date": startTime,
                                "shipper_name": shipper['shipper_name'],
                                "channel_id": shipper['slack_channel_id'],
                                "error": formatEscapeString(str(result['error'])),
                                "error_traceback": formatEscapeString(str(result['errorTraceback']))
                            }
                
                executeQuery(logs,errorInsertQuery,error_values)
        executionTimeMin = round((datetime.now() - startTime).total_seconds() / 60,2)

        print(f"""Nudged shipper count: {len(nudgedShipper)}""")
        logs.append(formatLog(f"""Nudged shipper count: {len(nudgedShipper)}"""))
        print(f"""Failed shippers count: {len(failedShippers)}""")
        logs.append(formatLog(f"""Failed shippers count: {len(failedShippers)}"""))
        print(f"""No of channels bot integrated: {channelAddedCount}""")
        logs.append(formatLog(f"""No of channels bot integrated: {channelAddedCount}"""))
        print(f"""Newly bot integrated channels: {channelAddedShippers}""")
        logs.append(formatLog(f"""Newly bot integrated channels: {channelAddedShippers}"""))
        print(f"""Failed error: {failedError}""")
        logs.append(formatLog(f"""Failed error: {failedError}"""))

        blobUrl = uploadLogToBlob(logs,dbutils)


        summaryInsertQuery = """INSERT INTO brokerageprod.nudge_ai.awardsnudgetriggersummary (NudgeId, NudgeDate, TriggerQuery, NudgedShipperCount, NudgedShippers, FailedShipperCount, FailedShippers, Errors, NewlyAddedSPChannelsCount, NewlyAddedChannels, ExecutionTimeMin, PeriodYear,BlobLink) 
        VALUES (
                :nudge_id, :nudge_date,:triggerQuery, :nudge_shipper_count, :nudge_shippers, 
                :failed_shipper_count, :failed_shippers, :errors, 
                :newly_added_sp_channels_count, :newly_added_channels, 
                :execution_time_min, :period_year, :blob
            ) """
        
        nudgeValues = {
                        "nudge_id": nudgeId,                                   # Nudge_Id
                        "nudge_date": startTime, 
                        "triggerQuery" : formatEscapeString(query),    # Nudge_Date (timestamp)
                        "nudge_shipper_count": len(nudgedShipper),       # Nudge_Shipper_Count
                        "nudge_shippers": json.dumps(nudgedShipper),     # Nudge_Shippers
                        "failed_shipper_count": len(failedShippers),          # Failed_Shipper_Count
                        "failed_shippers": json.dumps(failedShippers),        # Failed_Shippers
                        "errors": json.dumps(failedError),                          # Errors
                        "newly_added_sp_channels_count": channelAddedCount,           # Newly_Added_SP_Channels_Count
                        "newly_added_channels": json.dumps(channelAddedShippers),     # Newly_Added_Channels
                        "execution_time_min": executionTimeMin,                # Execution_Time_Min
                        "period_year": f"{periodYear}",                               # Period_Year
                        "blob": blobUrl
                    }
        
        executeQuery(logs,summaryInsertQuery,nudgeValues)

        print("Summary execution audited..")

        return True
    except Exception as e:
        print(f"""Error in awardsNudge: {str(e)}""")
        return False