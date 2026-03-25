# Databricks notebook source
# MAGIC %md
# MAGIC ## SourceToBronze
# MAGIC * **Description:** To extract the data from Zip File and load it into Bronze stage
# MAGIC * **Created Date:** 11/18/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 11/18/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Required Packages

# COMMAND ----------

# Import required packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, concat_ws
from pyspark.sql.types import *
from datetime import datetime
import requests
import zipfile
import pandas as pd
from io import BytesIO

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling Utilities and Logger

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

# Setting up error logger
ErrorLogger = ErrorLogs("NFI_Awards_Exctraction")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the URLs

# COMMAND ----------

# Define the URLs
url_list = [
    "https://download.geonames.org/export/zip/CA_full.csv.zip",  # URL for Canada
    "https://download.geonames.org/export/zip/US.zip",          # URL for US
    "https://download.geonames.org/export/zip/MX.zip"           # URL for Mexico
]

# Create an empty list to hold the processed DataFrames
dataframes = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to Extract the Data and Structurize the Columns

# COMMAND ----------


# Function to download and process each file
def download_and_process_file(url):
    print(f"Downloading from: {url}")
    try:
        # Fetch the ZIP file
        response = requests.get(url)
        response.raise_for_status()  # Will raise HTTPError for bad status codes (4xx, 5xx)
        
        # Unzip the content
        with zipfile.ZipFile(BytesIO(response.content)) as zf:
            # List all files inside the ZIP archive
            file_names = zf.namelist()
            print(f"Files in ZIP: {file_names}")
            
            if len(file_names) == 1:
                # Case: ZIP file contains only one file (e.g., CA_full.csv.zip)
                data_file = file_names[0]
                print(f"Processing single file: {data_file}")
                
                # Open the single file inside the ZIP and read it
                with zf.open(data_file) as f:
                    # Read a small sample of the file to inspect its structure
                    
                    # Try to read the file (assume tab-delimited, but check the structure)
                    f.seek(0)  # Go back to the start of the file
                    df = pd.read_csv(f, delimiter="\t", header=None)
                    
                    # Check if the number of columns matches the expected 12
                    if df.shape[1] == 12:
                        # Assign column names (if the number of columns is correct)
                        df.columns = [
                            "country_code", "postal_code", "place_name", 
                            "admin_name1", "admin_code1", "admin_name2", 
                            "admin_code2", "admin_name3", "admin_code3", 
                            "latitude", "longitude", "accuracy"
                        ]
                    else:
                        # Print a warning and show the number of columns in this case
                        print(f"Warning: {data_file} has {df.shape[1]} columns, which doesn't match the expected 12.")
                        return df
                    
                    # Return the DataFrame
                    return df
            elif len(file_names) > 1:
                # Case: ZIP file contains multiple files (e.g., US.zip, MX.zip)
                data_file = file_names[1]  # We want to process the second file
                print(f"Processing second file: {data_file}")
                
                # Open the second file inside the ZIP and read it
                with zf.open(data_file) as f:
                    df = pd.read_csv(f, delimiter="\t", header=None)  # Assuming TSV (tab-delimited)
                    
                    # Assign column names
                    df.columns = [
                        "country_code", "postal_code", "place_name", 
                        "admin_name1", "admin_code1", "admin_name2", 
                        "admin_code2", "admin_name3", "admin_code3", 
                        "latitude", "longitude", "accuracy"
                    ]
                    
                    # Return the DataFrame
                    return df
            else:
                print(f"Error: The ZIP file from {url} contains no files or is empty.")
                
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}: {e}")
    except zipfile.BadZipFile:
        print(f"Error: The ZIP file from {url} is corrupted or not a valid ZIP.")
    except pd.errors.ParserError:
        print(f"Error: Failed to parse the CSV file from {url}.")
    except Exception as e:
        print(f"An unexpected error occurred while processing {url}: {e}")
    
    return None

# Download and process all files
for url in url_list:
    df = download_and_process_file(url)
    if df is not None:
        dataframes.append(df)

# Combine all DataFrames (handling missing columns)
if dataframes:
    final_df = pd.concat(dataframes, ignore_index=True, sort=False)

    spark_df = spark.createDataFrame(final_df)

    # Create or replace a temporary view
    spark_df.createOrReplaceTempView("geonames")
else:
    print("No data was processed due to errors.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Required Transformations

# COMMAND ----------

# Query the DataFrame
try:
    final_df = spark.sql(f"""
                         WITH Organized as (SELECT Country_Code, 
                          CASE Country_Code 
                              WHEN 'MX' THEN 'Mexico' 
                              WHEN 'US' THEN 'United States' 
                              ELSE 'Canada' 
                          END AS Country_Name, Postal_Code AS Zip, Place_Name as City, Admin_Name1 as State_Name, Admin_Code1 as State_Code, Admin_Name2 as County_Name, Admin_Name3 as Community_Name, latitude, longitude, Accuracy
                    FROM geonames ),

                    Final as (SELECT DISTINCT Country_Code, Country_Name,Zip,  City AS Place_Name , 'City' AS Division, State_Name, State_Code, latitude, longitude, Accuracy FROM Organized
                    UNION 
                    SELECT DISTINCT Country_Code, Country_Name, Zip, County_Name AS Place_Name,'County' AS Division, State_Name, State_Code, latitude, longitude, Accuracy FROM Organized WHERE County_Name <> 'NaN'
                    UNION
                    SELECT DISTINCT Country_Code, Country_Name, Zip, Community_Name AS Place_Name,'Community' AS Division, State_Name, State_Code, latitude, longitude, Accuracy FROM Organized WHERE Community_Name <> 'NaN'
                    ORDER BY Zip )

                   SELECT 
                        Country_Code,
                        Country_Name,
                        CASE WHEN Zip = 'NaN' THEN NULL ELSE Zip END AS Zip,
                        CASE WHEN Place_Name = 'NaN' THEN NULL ELSE Place_Name END AS Place_Name,
                        CASE WHEN Division = 'NaN' THEN NULL ELSE Division END AS Division,
                        CASE WHEN State_Name = 'NaN' THEN NULL ELSE State_Name END AS State_Name,
                        CASE WHEN State_Code = 'NaN' THEN NULL 
                            WHEN country_code = 'MX' AND state_name = 'Aguascalientes' THEN 'AG'
                            WHEN country_code = 'MX' AND state_name = 'Baja California' THEN 'BN'
                            WHEN country_code = 'MX' AND state_name = 'Baja California Sur' THEN 'BS'
                            WHEN country_code = 'MX' AND state_name = 'Campeche' THEN 'CP'
                            WHEN country_code = 'MX' AND state_name = 'Chiapas' THEN 'CS'
                            WHEN country_code = 'MX' AND state_name = 'Chihuahua' THEN 'CI'
                            WHEN country_code = 'MX' AND state_name = 'Colima' THEN 'CL'
                            WHEN country_code = 'MX' AND state_name = 'Coahuila de Zaragoza' THEN 'CH'
                            WHEN country_code = 'MX' AND state_name = 'Distrito Federal' THEN 'DF'
                            WHEN country_code = 'MX' AND state_name = 'Durango' THEN 'DG'
                            WHEN country_code = 'MX' AND state_name = 'Guanajuato' THEN 'GJ'
                            WHEN country_code = 'MX' AND state_name = 'Guerrero' THEN 'GE'
                            WHEN country_code = 'MX' AND state_name = 'Hidalgo' THEN 'HD'
                            WHEN country_code = 'MX' AND state_name = 'Jalisco' THEN 'JA'
                            WHEN country_code = 'MX' AND state_name = 'Michoacán de Ocampo' THEN 'MC'
                            WHEN country_code = 'MX' AND state_name = 'Morelos' THEN 'MR'
                            WHEN country_code = 'MX' AND state_name = 'México' THEN 'MX'
                            WHEN country_code = 'MX' AND state_name = 'Nayarit' THEN 'NA'
                            WHEN country_code = 'MX' AND state_name = 'Nuevo León' THEN 'NL'
                            WHEN country_code = 'MX' AND state_name = 'Oaxaca' THEN 'OA'
                            WHEN country_code = 'MX' AND state_name = 'Puebla' THEN 'PU'
                            WHEN country_code = 'MX' AND state_name = 'Querétaro' THEN 'QE'
                            WHEN country_code = 'MX' AND state_name = 'Quintana Roo' THEN 'QI'
                            WHEN country_code = 'MX' AND state_name = 'San Luis Potosí' THEN 'SL'
                            WHEN country_code = 'MX' AND state_name = 'Sinaloa' THEN 'SI'
                            WHEN country_code = 'MX' AND state_name = 'Sonora' THEN 'SO'
                            WHEN country_code = 'MX' AND state_name = 'Tabasco' THEN 'TB'
                            WHEN country_code = 'MX' AND state_name = 'Tamaulipas' THEN 'TA'
                            WHEN country_code = 'MX' AND state_name = 'Tlaxcala' THEN 'TL'
                            WHEN country_code = 'MX' AND state_name = 'Veracruz de Ignacio de la Llave' THEN 'VC'
                            WHEN country_code = 'MX' AND state_name = 'Yucatán' THEN 'YU'
                            WHEN country_code = 'MX' AND state_name = 'Zacatecas' THEN 'ZA'
                                ELSE 
                                state_code
                            END AS state_code,
                        CASE WHEN Latitude = 'NaN' THEN NULL ELSE Latitude END AS Latitude,
                        CASE WHEN Longitude = 'NaN' THEN NULL ELSE Longitude END AS Longitude,
                        CASE WHEN Accuracy = 'NaN' THEN NULL ELSE Accuracy END AS Accuracy
                    FROM Final;



    """)

    final_df =  final_df.withColumn(
        "Addresskey",
        md5(
            concat_ws(
                '|',
                col('Country_Code'),
                col('Country_Name'),
                col('Zip'),
                col('Place_Name'),
                col('Division'),
                col('State_Name'),
                col('State_Code'),
                col('longitude'),
                col('latitude'),
                col('Accuracy')
            )
        )
    )
    final_df.dropDuplicates()
    final_df.createOrReplaceTempView("VW_Source")

except Exception as e:
    print(f"Exception occurred: {str(e)}")
    logger.error(str(e), exc_info=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Details

# COMMAND ----------

# Metadata and Job Definitions
Table_ID = "LA1"
DF_Metadata = spark.sql(f"SELECT * FROM Metadata.MasterMetadata WHERE Tableid = '{Table_ID}' AND IsActive = '1'")
Job_ID = DF_Metadata.filter(f"TableID == '{Table_ID}'").select('Job_ID').collect()[0][0]
Notebook_ID = DF_Metadata.filter(f"TableID == '{Table_ID}'").select('NB_ID').collect()[0][0]


# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge the Data into Target

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(Table_ID, "Bronze")
    TableName = DF_Metadata.filter(f"TableID == '{Table_ID}'").select('DWHTableName').collect()[0][0]
    MaxLoadDateColumn = DF_Metadata.filter(f"TableID == '{Table_ID}'").select('LastLoadDateColumn').collect()[0][0]
    MaxLoadDate = DF_Metadata.filter(f"TableID == '{Table_ID}'").select('LastLoadDateValue').collect()[0][0]
    Zone = DF_Metadata.filter(f"TableID == '{Table_ID}'").select('Zone').collect()[0][0]

    if AutoSkipperCheck == 0:
       
        
        spark.sql(f'''MERGE INTO bronze.{TableName} Target USING VW_Source Source 
                  ON Target.Addresskey = Source.Addresskey 
                  WHEN NOT MATCHED THEN      
                  INSERT ( 
                        Target.Addresskey,
                        Target.Country_Code,
                        Target.Country_Name,
                        Target.Zip,
                        Target.Place_Name, 
                        Target.Division, 
                        Target.State_Code, 
                        Target.State_Name,
                        Target.Longitude,
                        Target.Latitude,
                        Target.Accuracy,
                        Target.Is_Deleted,
                        Target.Created_date,
                        Target.Created_by,
                        Target.Last_Modified_date,
                        Target.Last_Modified_by
                    )
                    Values
                    (
                        Source.Addresskey,
                        Source.Country_Code,
                        Source.Country_Name,
                        Source.Zip,
                        Source.Place_Name, 
                        Source.Division, 
                        Source.State_Code, 
                        Source.State_Name,
                        Source.Longitude,
                        Source.Latitude,
                        Source.Accuracy,
                        0,
                        Current_timestamp(),
                        'Databricks',
                        Current_timestamp(),
                        'Databricks'
                    )
            ''')
        # Source Delete Handling
        spark.sql(f'''
                    UPDATE bronze.{TableName}
                    SET bronze.{TableName}.Is_Deleted = 1, 
                        bronze.{TableName}.Last_Modified_date = Current_timestamp()
                    where bronze.{TableName}.Addresskey in (select t.Addresskey from bronze.{TableName} t left join VW_Source s on t.Addresskey = s.Addresskey where s.Addresskey is null AND t.Is_Deleted = 0)
                ''')

        #Update The Metadata table
        MaxDateQuery = f'''Select max({0}) as Max_Date from bronze.{TableName}'''
        MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
        DF_MaxDate = spark.sql(MaxDateQuery)
        UpdateLastLoadDate(Table_ID,DF_MaxDate)
        UpdatePipelineStatusAndTime(Table_ID,'Bronze')
        UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Load")

except Exception as e:
    logger.info('Failed for Bronze load')
    UpdateFailedStatus(Table_ID,'Bronze')
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Load")
    print('Unable to load '+TableName)
    print(e)