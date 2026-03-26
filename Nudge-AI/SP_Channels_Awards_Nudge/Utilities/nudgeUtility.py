#nudgeUtility.py

from datetime import datetime
import os
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from azure.storage.blob import BlobServiceClient

#Seq number : 1.11 to 1.19
"""The validateInputQuery function checks if a given SQL query is a valid SELECT statement by stripping whitespace 
and converting it to lowercase before verifying the prefix. It returns True if the query is valid and False 
otherwise. If an error occurs during validation, it prints the error message and raises the exception."""
def validateInputQuery(inputQuery):
    try:
        dmlOperations = ['insert', 'update','drop', 'truncate']

        # Convert the SQL query to lower case for case-insensitive comparison
        sqlQueryLower = inputQuery.lower()

        # Check if any DML operation is in the SQL query
        if any(operation in sqlQueryLower for operation in dmlOperations):
            print("The SQL query contains DML operations other than SELECT.")
            return False
        else:
            print("No DML operations other than SELECT found in the SQL query.")
            return True
    except Exception as e:
        print(f"Error validating input query: {str(e)}")
        raise

#Seq number 1.162 to 1.180
"""The nonAwardFormat function constructs a Slack message formatted as blocks to summarize non-awarded metrics 
for a specified shipper. It includes an introduction that addresses the user (or a general audience if no user 
ID is provided) and details about non-awarded lane and load volumes, as well as potential lanes. The function 
also adds a button for users to interact with the summary. If any errors occur during execution, they are printed 
to the console."""
def nonAwardFormat(awardsSets,shippername,details,userId,periodYear,logs):
    try:
        slack_blocks = []
        print("Execution of non award format blocks function")
        logs.append(formatLog("Execution of non award format blocks function"))
        print(f"""User id: {userId}""")
        if userId:
            print("in if condition")
            print(userId)
            logs.append(formatLog(f"""Nudging with User Id: {userId}"""))
            introduction_text = f"Hello <@{userId}>, just a heads-up from :nudge:! Below is the Non-Awarded Summary for {shippername}:"
        else:
            print("in elseeee condition")
            logs.append(formatLog("Nudging without User id using everyone tag.."))
            introduction_text = f"Hello @everyone, just a heads-up from :nudge:! Below is the Non-Awarded Summary for {shippername}:"

        # Create the introduction block with the conditional text
        introduction_block = {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": introduction_text
                                }
                            }
        slack_blocks.append(introduction_block)

        potential_awards = {}
        print(awardsSets)
        for awards_set in awardsSets:
            if "nonAwardedLaneCount" in awards_set and "nonAwardedLoadCount" in awards_set and "PotentialCount" in awards_set:
                potential_awards = {
                
                    "nonAwardedLaneCount": awards_set["nonAwardedLaneCount"],
                    "nonAwardedLoadCount": awards_set["nonAwardedLoadCount"],
                    "PotentialCount": awards_set["PotentialCount"]
                }
                break
        if potential_awards:
            logs.append(formatLog("Non awarded potential awards block"))
            # Space and separator blocks

            # Combine potential awards into a single block
            potential_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"Period Year: {periodYear}\n"
                        f"Non-Awarded Lane Volume Total: {potential_awards.get('nonAwardedLaneCount', 0)}\n"
                        f"Non-Awarded Load Volume Total: {potential_awards.get('nonAwardedLoadCount', 0)}\n"
                        f"Potential Lanes: {potential_awards.get('PotentialCount', 0)}"
                    )
                }
            }
            slack_blocks.append(potential_block)
        # Append potential awards section at the end
        # Button for interaction
        button_block = {
                        "type": "actions",
                        "block_id": f"{details}",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "Click To view summary"
                                },
                                "action_id": "NonAwardButton",
                                "value": f"{awardsSets}"
                            }
                        ]
                    }
        slack_blocks.append(button_block)
        logs.append(formatLog("Returning nonawarded format blocks.."))
        return slack_blocks


    except Exception as e:
        print(e)
        logs.append(formatLog(f"""Error occurred in non awards format function {e}""","ERROR"))


#Seq number 1.138 to 1.160 
"""The formatBlocks function creates a structured Slack message formatted as blocks to summarize awarded metrics 
for a specified shipper. It includes an introduction that addresses the user (or a general audience if no user ID 
is provided) and details about awarded metrics such as estimated and actual lane and load volumes. The function 
also separates potential awards and adds a button for users to interact with the summary. If any errors occur 
during execution, they are printed to the console."""
def formatBlocks(awardsSets,shipperName,details,userId,logs):
    try:
        print(awardsSets)
        logs.append(formatLog(f"""Awards sets: \n{awardsSets}"""))
        slack_blocks = []
        currentDate=datetime.now().date()
        # Introduction section
        if userId != None:
            print("in if condition")
            print(userId)
            logs.append(formatLog(f"""Nudging with User Id: {userId}"""))
            introduction_text = f"Hello <@{userId}>, just a heads-up from :nudge:! Below is the Awarded Summary for {shipperName}:"
        else:
            print("in elseeee condition")
            logs.append(formatLog("Nudging without User id using everyone tag.."))
            introduction_text = f"Hello @everyone, just a heads-up from :nudge:! Below is the Awarded Summary for {shipperName}:"

        # Create the introduction block with the conditional text
        introduction_block = {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": introduction_text
                                }
                            }
        slack_blocks.append(introduction_block)
        

        # Loop through each awards set (excluding potential awards)
        for awards_set in awardsSets:
            for awards_key, awards_data in awards_set.items():
                if awards_key == "nonAwardedLaneCount" and awards_key == "nonAwardedLoadCount" and awards_key == "PotentialCount":
                    continue

                # Awards set header
                if awards_key !="N" and awards_key !="nonAwardedLaneCount" and  awards_key !="nonAwardedLoadCount" and  awards_key !="PotentialCount":
                    awards_set_header = {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"`Award: {awards_key}`"
                        }
                    }
                    print(awards_key)
                    logs.append(formatLog(f"""Awards Key : {awards_key}"""))
                    slack_blocks.append(awards_set_header)
                    print(awards_data)
                    logs.append(formatLog(f"""Awards Data: \n{awards_data}"""))
                    # Achieved awards section
                    details_block = {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"*Period Year*: {awards_data.get('periodYear')}\n"
                                f"*Estimated Lane Volume Total*: {awards_data.get('estimatedLaneCount')}\n"
                                f"*Actual Lane Volume Total*: {awards_data.get('actualLaneCount')}\n"
                                f"*Estimated Load Volume Total*: {awards_data.get('estimatedLoadTotal')}\n"
                                f"*Actual Load Volume Total*: {awards_data.get('actualLoadTotal')}"
                            )
                        }
                    }
                    slack_blocks.append(details_block)
        # Separate potential awards
        potential_awards = {}
        for awards_set in awardsSets:
            if "nonAwardedLaneCount" in awards_set and "nonAwardedLoadCount" in awards_set and "PotentialCount" in awards_set:
                
                potential_awards = {
                    "nonAwardedLaneCount": awards_set["nonAwardedLaneCount"],
                    "nonAwardedLoadCount": awards_set["nonAwardedLoadCount"],
                    "PotentialCount": awards_set["PotentialCount"]
                }
                break
            
        
        if potential_awards:
            logs.append(formatLog("Formatting non awarded block"))
            # Space and separator blocks
            slack_blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": " "}})
            slack_blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "-------------------------------------------------------------"}})

            # Combine potential awards into a single block
            potential_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"Non-Awarded Lane Volume Total: {potential_awards.get('nonAwardedLaneCount', 0)}\n"
                        f"Non-Awarded Load Volume Total: {potential_awards.get('nonAwardedLoadCount', 0)}\n"
                        f"Potential Lanes: {potential_awards.get('PotentialCount', 0)}"
                    )
                }
            }
            slack_blocks.append(potential_block)
        # Append potential awards section at the end


        # Button for interaction
        button_block = {
                        "type": "actions",
                        "block_id": f"{details}",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "Click To view summary"
                                },
                                "action_id": "awardButton",
                                "value": f"{awardsSets}"
                            }
                        ]
                    }
        slack_blocks.append(button_block)
        print(slack_blocks)
        logs.append(formatLog("Returning formated blocks..."))
        return slack_blocks
    except Exception as e:
        print(f"An error occurred in format blocks: {e}")
        logs.append(formatLog(f"An error occurred in format blocks: {e}","ERROR"))
        

#Seq number 1.187 to 1.209 
"""The postNudge function sends a message with formatted blocks to a specified Slack channel using the Slack API. 
It attempts to post the message up to three times, handling errors such as the bot not being a member of the 
channel or the channel not being found. If the bot is not in the channel, it tries to join the channel before 
posting the message. The function returns a status indicating whether the bot successfully added itself to the 
channel. If any errors occur during the process, they are printed to the console, and exceptions are raised as 
necessary."""
def postNudge(dbutils,channelId, inputBlocks,logs):

    print("Entered into slack file post function")
    logs.append(formatLog("Entered into slack message post function"))
    SLACKTOKEN = dbutils.secrets.get(scope = "GenAI", key = "AWARDS-NUDGE-BOT-TOKEN")
    client = WebClient(token=SLACKTOKEN)
    maxRetries = 2
    waitTime = 2
    botAddStatus = False

    for attempt in range(maxRetries + 1):

        try:
            client.chat_postMessage(channel= channelId,blocks=inputBlocks)
            logs.append(formatLog("Message posted.."))
            return botAddStatus

        except SlackApiError as e:
            if e.response["error"] == "not_in_channel":
                print("Bot isn't a member of the channel.")
                logs.append(formatLog("Bot isn't a member of the channel.","WARNING"))


                try:
                    # Bot joins the channel using conversations.join
                    join_response = client.conversations_join(channel= channelId)
                    if join_response['ok']:
                        print(f"""Bot has joined the channel {channelId} successfully.""")
                        logs.append(formatLog(f"""Bot has joined the channel {channelId} successfully."""))
                        botAddStatus =  True
      
                    else:
                        print(f"""Failed to join the channel {channelId}: {join_response['error']}""")
                        logs.append(formatLog(f"""Failed to join the channel {channelId}: {join_response['error']}""","ERROR"))
                        raise join_response['error']
                    
                except SlackApiError as e:
                    # Check specific error for private channels and invite issue
                    if e.response['error'] == 'channel_not_found':
                        print(f"""Channel {channelId} not found. Ensure the bot is invited if it's a private channel.""")
                        logs.append(formatLog(f"""Channel {channelId} not found. Ensure the bot is invited if it's a private channel.""","ERROR"))
                       
                    else:
                        print(f"""Error joining channel {channelId}: {e.response['error']}\n{str(e)}""")
                        logs.append(formatLog(f"""Error joining channel {channelId}: {e.response['error']}\n{str(e)}"""))

                    raise
                    
            elif e.response["error"] == "channel_not_found":
                print(f"""Channel not found: {e.response['channel']}. Please check the channel ID.""")
                logs.append(formatLog(f"""Channel not found: {e.response['channel']}. Please check the channel ID."""))
                raise 
            else:
                print(f"""Error posting file (attempt {attempt + 1}): {e.response['error']}""")
                logs.append(formatLog(f"""Error posting file (attempt {attempt + 1}): {e.response['error']}""","WARNING"))
                if attempt < maxRetries:
                    time.sleep(waitTime)
                else:
                    raise 

#Seq number 2.1 to 2.3
"""The format_escape_string function takes a string input, replaces single quotes with double quotes, and 
returns the string wrapped in single quotes, or returns 'NULL' if the input is None."""
def formatEscapeString(string):
    if string is None:
        return 'NULL'
    return "'" + string.replace("'", '"') + "'"


#seq 2.4 to 2.6
def formatLog(message: str, logType = "INFO") -> str:
    """
    Formats a log message with the current timestamp and log type.

    Args:
        message (str): The log message.
        logType (str): The type of log (e.g., 'INFO', 'ERROR', 'DEBUG').

    Returns:
        str: A formatted log string.
    """
    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Format the log string
    logString = f"""{timestamp} - {logType} - {message}"""
    
    return logString

#seq 1.251 to 1.265
def uploadLogToBlob(logList,dbutils):
    """
    Uploads a list of log strings to Azure Blob Storage.

    This function takes a list of log entries, concatenates them into a single string, 
    and uploads the resulting string as a text file to a specified Azure Blob Storage container. 
    The blob is named with a timestamp to ensure uniqueness. 

    Parameters:
    logList (list): A list of log strings to be uploaded.
    dbutils: An object that provides access to secrets for Azure Blob Storage configuration.

    Returns:
    str: The URL of the uploaded blob in Azure Blob Storage.

    Raises:
    Exception: If there is an error during the upload process.
    """
    try:
        
        blobData = "\n".join(logList)

        # Azure Blob Storage configuration
        blobConnString = dbutils.secrets.get(scope="GenAI", key="AWARDS-NUDGE-BLOB-CONN-STR")
        containerName = dbutils.secrets.get(scope="GenAI", key="AWARDS-NUDGE-LOG-CONTAINER")

        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        
        blobName = f'Awards_Nudge_{timestamp}.txt'

        # Create a BlobServiceClient using the account URL and key

        blobServiceClient = BlobServiceClient.from_connection_string(blobConnString)

        # Get a client to interact with the specified container
        containerClient = blobServiceClient.get_container_client(containerName)

        # Upload the string data to the blob
        blobClient = containerClient.get_blob_client(blobName)
        blobClient.upload_blob(blobData, overwrite=True)

        blobUrl = f"https://{blobServiceClient.account_name}.blob.core.windows.net/{containerName}/{blobName}"

        print(f'List of strings written to blob storage as {blobName}')
        print(f'Blob URL: {blobUrl}')
        return blobUrl
    
    except Exception as e:
        print(f"""Error in generating summary with error {e}""")
        raise e