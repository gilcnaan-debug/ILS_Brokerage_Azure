#nudgeService.py
from slack_sdk import WebClient
import ssl
import certifi
import time
from datetime import datetime
import traceback
from Repository.nudgeRepo import executeQuery
from Utilities.nudgeUtility import nonAwardFormat, formatBlocks,postNudge,formatLog

#Seq number 1.59 to 1.225 : 
"""The nudgeSpChannels function sends nudges to shippers via Slack based on their performance data for a 
specified period. It retrieves relevant data, including awards and potential awards, and formats messages to be 
sent to a designated Slack channel. The function checks if the current date matches a specific nudge date and 
handles errors during the process, returning a status indicating success or failure along with any relevant error 
information."""
def nudgeSpChannels(dbutils,shipperData, nudgeDate,logs):
    try:
        print("entered shipper loop")
        logs.append(formatLog(f"""Entered into nudging shipper"""))
        # lastAwardNudgedDate = shipperData[i]['last_weekly_nudged_date']  # Get lastAwardNudgedDate
        # print(lastAwardNudgedDate)
        shipperName=shipperData.get('shipper_name')

        print(f"""Shipper Name: {shipperName}""")
        logs.append(formatLog(f"""Shipper Name: {shipperName}"""))

        channelId= "C071CM0QA3A"

        print(f"""Channel ID: {channelId}""")
        logs.append(formatLog(f"""Channel ID: {channelId}"""))

        print(f"""Channel ID: {channelId}""")
        logs.append(formatLog(f"""Channel ID: {channelId}"""))

        emailAddress=shipperData.get('email_address')
        print(f"""Email ID: {emailAddress}""")
        logs.append(formatLog(f"""Email ID: {emailAddress}"""))
        slackToken = dbutils.secrets.get(scope = "GenAI", key = "AWARDS-NUDGE-BOT-TOKEN")
        client = WebClient(token=slackToken)
        userId=None
        if emailAddress != None:
            try:
                email=client.users_lookupByEmail(email=emailAddress)
                # Extract and return the user ID
                userId = email["user"]["id"]
            except Exception as e:
                userId=None
        print(shipperName)
        awardsSets=[]
        # Prepare currentDate and convert lastAwardNudgedDate to string
        # currentDate = datetime.now().date()
        current= nudgeDate
        currentDate=datetime.strptime(current, '%Y-%m-%d').date()
        testPeriod=f"""Select distinct Period_year, financial_Period_startdate , financial_Period_Enddate from brokerageprod.analytics.dim_date where calendar_year = :calendarYear """
        testPeriodValue = {"calendarYear" : f"{currentDate.year}"}
        periodData=executeQuery(logs,testPeriod,testPeriodValue)
        print(periodData)
        periodDataSorted = sorted(periodData, key=lambda x: x['financial_Period_Enddate'])
        previousPeriod = None
        periodYear=None
        for period in periodDataSorted:
            startDate = period['financial_Period_startdate']
            endDate = period['financial_Period_Enddate']
            if startDate <= currentDate <= endDate:
                if previousPeriod:
                    print(previousPeriod)
                    periodYear= previousPeriod['Period_year']
                else:
                    periodYear= None
                break
            previousPeriod=period
        awardsNudgeDateQuery=f"""Select distinct First_Tuesday from brokerageprod.analytics.dim_date_awards where calendar_year= :currentYear and period_year= :periodYear"""
        awardsNudgeDateValue = {"currentYear" : f"{currentDate.year}" , "periodYear": f"{periodYear}" }
        awardsNudgeDateData=executeQuery(logs,awardsNudgeDateQuery,awardsNudgeDateValue)
        print(awardsNudgeDateData)
        logs.append(formatLog("Retrieved Nudge date data."))
        strawardsNudgeDate=awardsNudgeDateData[0].get('First_Tuesday')
        awardsNudgeDate=strawardsNudgeDate
        print(awardsNudgeDate)
        logs.append(formatLog(f"""First Tuesday of this period: {awardsNudgeDate}"""))
        if currentDate == awardsNudgeDate:
            print("eneterd if days differenece")
            logs.append(formatLog("Current date Matches the First tuesday of this period. Executing nudges."))
            potentialData=[]
            currentMonth = currentDate.month
            periodYearFormat = f"{currentMonth}-{currentDate.year}"
            # client = WebClient(token=slackToken)
            # time.sleep(2)
            rfpQuery = f"""SELECT DISTINCT RFP FROM brokerageprod.nudge_ai.Nudge_Summary_Awards WHERE upper(Shipper)= :shipperName AND PeriodYear = :periodYear AND RFP <> 'No Awards'"""
            rfpQueryValue = {"shipperName" : f"{shipperName.upper()}", "periodYear" : f"{periodYear}"}
            rfpData = executeQuery(logs,rfpQuery,rfpQueryValue)
            print(rfpQuery)
            # Append to RFPNameList
            print(rfpData)
            logs.append(formatLog(f"""RFP Data: \n{rfpData}"""))
            if rfpData !=[]:
                logs.append(formatLog(f"""Entered into processing rfp data"""))
                RFPNameList = [rfp['RFP'] for rfp in rfpData]
                logs.append(formatLog(f"""RFP name list: {RFPNameList}"""))
                logs.append(formatLog(f"""Processing each RFP"""))
                
                for rfpName in RFPNameList:
                    # Loop through RFPName
                    print(rfpName)
                    logs.append(formatLog(f"""RFP Name: {rfpName}"""))
                    estimatedLaneCount=0
                    actualLaneCount=0
                    estimatedLoadTotal=0
                    actualLoadTotal=0
                    awardIntervalQuery=f"""select distinct AwardInterval from brokerageprod.nudge_ai.Nudge_Summary_Awards WHERE upper(Shipper) = :shipperName and PeriodYear = :periodYear AND RFP = :rfpName ;"""
                    awardIntervalQueryValue = {"shipperName": f"{shipperName.upper()}", "periodYear": f"{periodYear}", "rfpName": f"{rfpName}"}
                    awardsIntervalData=executeQuery(logs,awardIntervalQuery,awardIntervalQueryValue)
                    awardsInterval=awardsIntervalData[0].get('AwardInterval')
                    print(awardsInterval)
                    logs.append(formatLog(f"""RFP Awards Interval: \n{awardsInterval}"""))
                    laneTotalQuery=f"""select * from brokerageprod.nudge_ai.Nudge_Awards_Lane where Customer_Master= :shipperName and Period_Year= :periodYear AND RFP_Name = :rfpName AND Is_Awarded= 'Awarded' """
                    laneTotalQueryValue = {"shipperName": f"{shipperName.upper()}", "periodYear": f"{periodYear}", "rfpName": f"{rfpName}"}
                    lateTotalData= executeQuery(logs,laneTotalQuery,laneTotalQueryValue)
                    print(lateTotalData)
                    if lateTotalData !=[]:
                        estimatedLaneCount=lateTotalData[0].get('Estimated_Lanes_Volume')
                        print(estimatedLaneCount)
                        logs.append(formatLog(f"""Estimated lane count: {estimatedLaneCount}"""))
                        actualLaneCount=lateTotalData[0].get('Actual_Lanes_Volume')
                        print(actualLaneCount)
                        logs.append(formatLog(f"""Actual lane count; {actualLaneCount}"""))
                    
                    loadTotalCountQuery=f"""select sum(EstimatedVolume) as EstimatedTotal ,sum(ActualVolume) as ActualTotal from brokerageprod.nudge_ai.Nudge_Summary_Awards where upper(Shipper)= :shipperName and PeriodYear= :periodYear AND RFP= :rfpName """
                    loadTotalCountQueryValue = {"shipperName": f"{shipperName.upper()}", "periodYear": f"{periodYear}", "rfpName": f"{rfpName}"}
                    loadTotal=executeQuery(logs,loadTotalCountQuery,loadTotalCountQueryValue)
                    print(loadTotal)
                    if loadTotal !=[]:
                        estimatedLoadTotal=loadTotal[0].get('EstimatedTotal')
                        print(estimatedLoadTotal)
                        logs.append(formatLog(f"""Estimated Load Total: {estimatedLoadTotal}"""))
                        actualLoadTotal=loadTotal[0].get('ActualTotal')
                        print(actualLoadTotal)
                        logs.append(formatLog(f"""Actual Load Total: {actualLoadTotal}"""))
                    # Begin data fetch with another query (repeats for overachieved, underachieved, not attempted)
                    awardsDict = {
                                    rfpName + f"({awardsInterval})": {
                                        "periodYear": periodYear,
                                        "estimatedLaneCount": (
                                            f"{round(float(estimatedLaneCount)):,}" if estimatedLaneCount is not None else "0"
                                        ),
                                        "actualLaneCount": (
                                            f"{round(float(actualLaneCount)):,}" if actualLaneCount is not None else "0"
                                        ),
                                        "estimatedLoadTotal": (
                                            f"{round(float(estimatedLoadTotal)):,}" if estimatedLoadTotal is not None else "0"
                                        ),
                                        "actualLoadTotal": (
                                            f"{round(float(actualLoadTotal)):,}" if actualLoadTotal is not None else "0"
                                        )
                                    }
                                }
                    awardsSets.append(awardsDict)
                    # You would repeat similar query executions and data appending for 'Underachieved' and 'Not Attempted'
                    # The following code lines are omitted for brevity.

                # After looping through RFP names
                # Begin data fetch for potential awards
            potentialAwardsQuery = f"""SELECT distinct count(Load_Lane) as PotentialCount FROM brokerageprod.nudge_ai.Nudge_Potential_Awards WHERE Customer_Name= :shipperName AND Period_Year= :periodYear AND Aging_Bucket <> 'Not Eligible'"""
            potentialAwardsQueryValue = {"shipperName": f"{shipperName.upper()}", "periodYear": f"{periodYear}"}
            PotentialAwards = executeQuery(logs,potentialAwardsQuery,potentialAwardsQueryValue)
            potentialCount=PotentialAwards[0].get('PotentialCount')
            nonAwardedLanequery=f"""select sum(Actual_Lanes_Volume) as NonAwardedLaneCount from brokerageprod.nudge_ai.Nudge_Awards_Lane where Customer_Master= :shipperName and Period_Year= :periodYear AND Is_Awarded= 'NonAwarded' """
            nonAwardedLanequeryValue = {"shipperName": f"{shipperName.upper()}", "periodYear": f"{periodYear}"}
            nonAwardedData=executeQuery(logs,nonAwardedLanequery,nonAwardedLanequeryValue)
            nonAwardedLaneCount=nonAwardedData[0].get('NonAwardedLaneCount')
            nonAwardedLoadQuery=f"""select sum(ActualVolume) as NonAwardedLoadCount from brokerageprod.nudge_ai.Nudge_Summary_Awards where upper(Shipper)= :shipperName and PeriodYear= :periodYear and RFP='No Awards' """
            nonAwardedLoadQueryValue = {"shipperName": f"{shipperName.upper()}", "periodYear": f"{periodYear}"}
            nonAwardedLoadData=executeQuery(logs,nonAwardedLoadQuery,nonAwardedLoadQueryValue)
            nonAwardedLoadCount=nonAwardedLoadData[0].get('NonAwardedLoadCount')
            potentialSets = {
                                "nonAwardedLaneCount": nonAwardedLaneCount if nonAwardedLaneCount is not None else "0",
                                "nonAwardedLoadCount": nonAwardedLoadCount if nonAwardedLoadCount is not None else "0",
                                "PotentialCount": potentialCount if potentialCount is not None else "0"
                            }
            awardsSets.append(potentialSets)
            details={
                "shipperName":shipperName,
                "periodYear":periodYear
            }
            for awards_set in awardsSets:
                for awards_key, awards_data in awards_set.items():
                    if awards_key!="N":
                        print(awards_key)
                        print("enetered value if")
                        logs.append(formatLog(f"Entered into if block, with awards data."))
                        blocks=formatBlocks(awardsSets,shipperName,details,userId,logs)
                        nonAwardBlocks=nonAwardFormat(awardsSets,shipperName,details,userId,periodYear,logs)
                    else:
                        print("entered empty else")
                        logs.append(formatLog(f"Entered into else block, no awards data"))
                        nullcountsList=[{
                                "N":{
                                    "periodYear":0,
                                    "estimatedLaneCount":0,
                                    "actualLaneCount":0,
                                    "estimatedLoadTotal":0,
                                    "actualLoadTotal":0 
                                }
                                }]
                        blocks=formatBlocks(nullcountsList,shipperName,details,userId,logs)
                            
            try:
                botAddedStatus =  False
                if rfpData ==[]:
                    logs.append(formatLog("There is no RFP data for the shipper, nudging only non awards"))
                    botAddedStatus = postNudge(dbutils,channelId,nonAwardBlocks,logs)
                    logs.append(formatLog("Only non awards posted"))
                    nonAwardedThreadblock = [
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": "*If you have updated rates on file please send them to Robert Madden <mailto:robert.madden@nfiindustries.com|robert.madden@nfiindustries.com> and Zak Fisher <mailto:zak.fisher@nfiindustries.com|zak.fisher@nfiindustries.com>*"
                                }
                            },{
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": "*Do you have any concern on the above Summary*"
                                }
                            },
                            {
                                "type": "actions",
                                "block_id": f"{details}",
                                "elements": [
                                    {
                                        "type": "button",
                                        "text": {
                                            "type": "plain_text",
                                            "text": "Yes",
                                            "emoji": True
                                        },
                                        "action_id": "nonawardYesButton",
                                        "value": "click_me_123"
                                    },
                                    {
                                        "type": "button",
                                        "text": {
                                            "type": "plain_text",
                                            "text": "No",
                                            "emoji": True
                                        },
                                        "action_id": "nonawardNoButton",
                                        "value": "click_me_123"
                                    }
                                ]
                            }
                        ]
                    channelAddedStatus = postNudge(dbutils,channelId, nonAwardedThreadblock,logs)
                    logs.append(formatLog("Concern interaction posted..."))
                    
                if rfpData !=[]:
                    logs.append(formatLog("Posting Awards with RFP data"))
                    channelAddedStatus = postNudge(dbutils,channelId,blocks,logs)
                    logs.append(formatLog("Awards posted..."))
                    awardedNudgeThreadBlock = [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "*If you have updated rates on file please send them to Robert Madden <mailto:robert.madden@nfiindustries.com|robert.madden@nfiindustries.com> and Zak Fisher <mailto:zak.fisher@nfiindustries.com|zak.fisher@nfiindustries.com>*"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "*Do you have any concern on the above Summary*"
                            }
                        },
                        {
                            "type": "actions",
                            "block_id": f"{details}",
                            "elements": [
                                {
                                    "type": "button",
                                    "text": {
                                        "type": "plain_text",
                                        "text": "Yes",
                                        "emoji": True
                                    },
                                    "action_id": "awardYesButton",
                                    "value": "click_me_123"
                                },
                                {
                                    "type": "button",
                                    "text": {
                                        "type": "plain_text",
                                        "text": "No",
                                        "emoji": True
                                    },
                                    "action_id": "awardNoButton",
                                    "value": "click_me_123"
                                }
                            ]
                        }
                    ]

                    channelAddedStatus = postNudge(dbutils,channelId,awardedNudgeThreadBlock,logs)
                    logs.append(formatLog("Concern interaction posted..."))


                # insertLastawardNudgedate=f"""UPDATE brokerageprod.nudge_ai.awards_slack_sam_mappings SET last_weekly_nudged_date = current_date() where Shipper_Name = :shipperName"""
                # updateLastAwardedValue = {"shipperName": shipperName}
                # executeQuery(insertLastawardNudgedate,updateLastAwardedValue)

                return {"status": "success", "channelAdded" : botAddedStatus ,"periodYear":periodYear}
                    
            except Exception as e:
                print(f"Error on posting for shipper: {shipperName}")
                print(f"An error occurred: {e}")
                logs.append(formatLog(f"""Error on posting for shipper: {shipperName}""","ERROR"))

                return {"status": "failed", "channelAdded" : botAddedStatus ,"error": str(e) , "errorTraceback": f"""{traceback.format_exc()}""", "periodYear":periodYear}
            
        else:
            print("First tuesday not matched the current date")
            logs.append(formatLog("First tuesday not matched the the current date"))
            return {"status": "failed", "channelAdded" : False ,"error": "Wrong Nudge Date" , "errorTraceback": "Nudge date doesn't match with the period first tuesday.","periodYear":periodYear}
    except Exception as e:
        print(f"""Error in nudgeSpChannels: {str(e)}""")
        logs.append(formatLog(f"""Error in nudgeSpChannels: {str(e)}""","ERROR"))
        return {"status": "failed", "channelAdded" : False ,"error": str(e) , "errorTraceback": f"""{traceback.format_exc()}""", "periodYear":periodYear}