#nudgeRepo.py

from pyspark.sql import SparkSession
import time
from Utilities.nudgeUtility import formatLog

#Seq num 1.24 to 1.48: 
"""The executeQuery function executes a given SQL query using a Spark session, 
with optional parameters for query execution. It attempts to run the query up to three times in case of failure, 
returning the results as a list of dictionaries if successful, or an empty list if no results are found. 
If all attempts fail, it raises the last encountered exception."""
def executeQuery(logs, sqlQuery: str, params=None):

    print(f"""Executing sql query: {sqlQuery}""")
    logs.append(formatLog(f"""Executing sql query: {sqlQuery}"""))
    maxRetries = 3
    for attempt in range(maxRetries):
        try:
            # Create a Spark session
            spark = SparkSession.builder.getOrCreate()
            
            # Format the SQL query with parameters if provided
            if params:
                print(f"""Executing with parameters: {params}""")
                logs.append(formatLog(f"""Executing with parameters: {params}"""))
                df = spark.sql(sqlQuery, args=params)
            else:
                print(f"""Executing without parameters""")
                logs.append(formatLog(f"""Executing without parameters"""))
                df = spark.sql(sqlQuery)
            # Execute the SQL query
            
            # Check if there are results
            if df.count() > 0:
                # Convert results to list of dictionaries (like the target function)
                columnNames = df.columns
                resultData = df.collect()
                
                # Create list of dictionaries where each dict is {column_name: value}
                result = []
                for row in resultData:
                    # Convert Row object to dictionary
                    rowDict = {columnNames[i]: row[i] for i in range(len(columnNames))}
                    result.append(rowDict)

                print(f"""Returning execution data..""") 
                logs.append(formatLog(f"""Returning execution data..""")) 
                return result
            else:
                # Return empty list for no results
                print(f"""Returning Empty data..""") 
                logs.append(formatLog(f"""Returning Empty data..""")) 
                return []
        
        except Exception as e:
            print(f"""Query attempt {attempt + 1} failed: {str(e)}""")
            logs.append(formatLog(f"""Query attempt {attempt + 1} failed: {str(e)}""","WARNING"))
            if attempt == maxRetries - 1:
                raise
            time.sleep(2)