# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Validated and Exceptions
# MAGIC * **Description:** To Transform data from Filtered Table to Validated Table as delta file
# MAGIC * **Created Date:** 10/07/2024
# MAGIC * **Created By:** Uday Raghavendra Krishna
# MAGIC * **Modified Date:** 10/07/2024
# MAGIC * **Modified By:** Uday Raghavendra Krishna
# MAGIC * **Changes Made:** 

# COMMAND ----------

# MAGIC %md
# MAGIC ###IMPORTING PACKAGES

# COMMAND ----------

#Importing packages
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pytz
from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %md
# MAGIC #### CALLING UTILITIES AND LOGGERS NOTEBOOK

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Utilities"

# COMMAND ----------

# MAGIC %run 
# MAGIC "/Shared/Common Notebooks/Logger"

# COMMAND ----------

#Creating variables to store error log information
ErrorLogger = ErrorLogs("NFI_Silver_Awards_Validation_&_Exception_")
logger = ErrorLogger[0]
p_logfile = ErrorLogger[1]
p_filename = ErrorLogger[2]

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRACT DATA FROM SILVER FILTERED TABLE

# COMMAND ----------

try: 
    DF_Silver_Source = spark.sql('''SELECT DISTINCT Mergekey, RFP_Name, Customer_Master, Customer_Slug, SAM_Name, 
        Origin, Origin_Country, Destination, Destination_Country,
        CASE WHEN UPPER(Equipment_Type) = 'DV' THEN 'VAN' WHEN TRIM(UPPER(Equipment_Type)) = 'DRY VAN' THEN 'VAN' WHEN UPPER(TRIM(Equipment_Type)) = 'DRYVAN' THEN 'VAN' ELSE UPPER(TRIM(Equipment_Type)) END AS Equipment_Type, CASE WHEN  Estimated_Volume IS NULL THEN 0 ELSE Estimated_Volume END AS Estimated_Volume, Estimated_Miles, Linehaul, RPM, Flat_Rate,
        Min_Rate, Max_Rate, Validation_Criteria, Award_Type, Award_Notes,
        Periodic_Start_Date, Periodic_End_Date, Shipper_Recurrence, updated_by,
        Updated_date, Contract_Type,Expected_Margin  FROM Silver.Silver_NFI_Awards_Filtered WHERE Is_Deleted = 0''')
   


    DF_Silver_Source = DF_Silver_Source.withColumn(
        "Expected_Margin",
        when(col("Expected_Margin").rlike("^-?\\d+%$"), 
            regexp_extract(col("Expected_Margin"), "(-?\\d+)%", 1).cast("int"))
        .when(col("Expected_Margin").isin("No Match", "-"), lit(0))
        .otherwise(col("Expected_Margin"))
    )
    DF_Silver_Source.dropDuplicates()
   


    # Get all column names except Mergekey
    all_columns = [c for c in DF_Silver_Source.columns if c != "Mergekey"]

    # Create a window spec
    window_spec = Window.partitionBy("Mergekey")

    # For each column, check if it differs within each Mergekey group
    diff_columns = []
    for column in all_columns:
        diff_columns.append(
            (size(collect_set(col(column)).over(window_spec)) > 1).alias(f"diff_{column}")
        )

    # Add these new columns to the dataframe
    df_with_diffs = DF_Silver_Source.select("Mergekey", *DF_Silver_Source.columns[1:], *diff_columns)

    #check which of these new columns are true
    result = df_with_diffs.select(
        *DF_Silver_Source.columns,
        concat_ws(", ", *[
            when(col(f"diff_{c}"), lit(c)).otherwise(None)
            for c in all_columns
        ]).alias("Differing_Columns")
    )

    # Remove duplicates
    result = result.distinct()
 
except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ROLLING UP AWARDS 

# COMMAND ----------

try: 
    # List of columns allowed to differ in the Aggregation DataFrame
    allowed_diff_columns = ['Estimated_Volume', 'Estimated_Miles', 'Linehaul', 'RPM', 'Flat_Rate', 'Min_Rate', 'Max_Rate', 'Expected_Margin']

    # Split the Differing_Columns string into an array, handling null and empty string cases
    df_split = result.withColumn("Differing_Columns_Array", 
        when((col("Differing_Columns").isNotNull()) & (length(col("Differing_Columns")) > 0), 
            split(col("Differing_Columns"), ", "))
        .otherwise(array())
    )

    # Create a column that contains differing columns not in the allowed list
    df_with_exception = df_split.withColumn(
        "Exception_Columns", 
        array_except(col("Differing_Columns_Array"), array(*[lit(col) for col in allowed_diff_columns]))
    )

    # Function to filter out allowed columns from the Exception_Type
    def filter_exception_type(differing_columns):
        if differing_columns:
            filtered = [col for col in differing_columns.split(", ") if col not in allowed_diff_columns]
            return "Differing in " + ", ".join(filtered) if filtered else None
        return None

    # Register the UDF
  
    filter_exception_type_udf = udf(filter_exception_type, StringType())

    # Create the Exception DataFrame
    DF_Differing_Columns_Exception = df_with_exception.filter(
        (size(col("Exception_Columns")) > 0) & (size(col("Differing_Columns_Array")) > 0)
    ).select(
        *DF_Silver_Source.columns,
        col("Differing_Columns_Array"),
        col("Exception_Columns"),
        filter_exception_type_udf(col("Differing_Columns")).alias("Exception_Type")
    )

    # Create the Aggregation DataFrame
    DF_Differing_Columns_Aggregation = df_with_exception.filter(
        (size(col("Exception_Columns")) == 0) | (size(col("Differing_Columns_Array")) == 0)
    ).select(
        *DF_Silver_Source.columns,
        col("Differing_Columns_Array"),
        col("Exception_Columns")
    )

    # List of columns allowed to differ in the Aggregation DataFrame
    allowed_diff_columns = ['Estimated_Volume', 'Estimated_Miles', 'Linehaul', 'RPM', 'Flat_Rate', 'Min_Rate', 'Max_Rate', 'Expected_Margin']

    # Split the Differing_Columns string into an array, handling null and empty string cases
    df_split = result.withColumn("Differing_Columns_Array", 
        when((col("Differing_Columns").isNotNull()) & (length(col("Differing_Columns")) > 0), 
            split(col("Differing_Columns"), ", "))
        .otherwise(array())
    )

    # Create a column that contains differing columns not in the allowed list
    df_with_exception = df_split.withColumn(
        "Exception_Columns", 
        array_except(col("Differing_Columns_Array"), array(*[lit(col) for col in allowed_diff_columns]))
    )

    # Function to filter out allowed columns from the Exception_Type
    def filter_exception_type(differing_columns):
        if differing_columns:
            filtered = [col for col in differing_columns.split(", ") if col not in allowed_diff_columns]
            return "Differing in " + ", ".join(filtered) if filtered else None
        return None

   
    filter_exception_type_udf = udf(filter_exception_type, StringType())

    # Create the Exception DataFrame
    DF_Differing_Columns_Exception = df_with_exception.filter(
        (size(col("Exception_Columns")) > 0) & (size(col("Differing_Columns_Array")) > 0)
    ).select(
        *DF_Silver_Source.columns,
        col("Differing_Columns_Array"),
        col("Exception_Columns"),
        filter_exception_type_udf(col("Differing_Columns")).alias("Exception_Type")
    )

    # Create the Aggregation DataFrame
    DF_Differing_Columns_Aggregation = df_with_exception.filter(
        (size(col("Exception_Columns")) == 0) | (size(col("Differing_Columns_Array")) == 0)
    ).select(
        *DF_Silver_Source.columns,
        col("Differing_Columns_Array"),
        col("Exception_Columns")
    )

except Exception as e: 
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")

# COMMAND ----------


try: 
    # Create DF_Customer_Validation
    DF_Customer_Validation = DF_Differing_Columns_Aggregation.filter(
        size(col("Differing_Columns_Array")) == 0
    )
    

    # Update DF_Differing_Columns_Aggregation
    DF_Differing_Columns_Aggregation = DF_Differing_Columns_Aggregation.filter(
        size(col("Differing_Columns_Array")) > 0
    )
except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")


# COMMAND ----------

try: 
    # First, create a DataFrame with just the sum of Estimated_Volume
    sum_df = DF_Differing_Columns_Aggregation.groupBy("Mergekey") \
        .agg(
            when(
                array_contains(first("Differing_Columns_Array"), "Estimated_Volume"),
                sum("Estimated_Volume")
            ).otherwise(first("Estimated_Volume")).alias("Sum_Estimated_Volume")
        )

    # Now, join this sum back to the original DataFrame
    DF_Aggregated = DF_Differing_Columns_Aggregation.join(sum_df, "Mergekey", "left_outer") \
        .drop("Estimated_Volume") \
        .withColumnRenamed("Sum_Estimated_Volume", "Estimated_Volume")
    DF_Aggregated.dropDuplicates()

except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")


# COMMAND ----------


try:
    # Calculate average of Expected_Margin
    avg_margin_df = DF_Aggregated.groupBy("Mergekey") \
        .agg(
            when(
                array_contains(first("Differing_Columns_Array"), "Expected_Margin"),
                avg(col("Expected_Margin").cast("double"))
            ).otherwise(first("Expected_Margin")).alias("Avg_Expected_Margin")
        )

    # Join this average back to the DF_Aggregated DataFrame
    DF_Aggregated = DF_Aggregated.join(avg_margin_df, "Mergekey", "left_outer") \
        .drop("Expected_Margin") \
        .withColumnRenamed("Avg_Expected_Margin", "Expected_Margin")

    # Round the Expected_Margin to 2 decimal places
    DF_Aggregated = DF_Aggregated.withColumn("Expected_Margin", 
        when(col("Expected_Margin").isNotNull(),
            round(col("Expected_Margin"), 2)
        ).otherwise(col("Expected_Margin"))
    )

    # Drop duplicates if necessary
    DF_Aggregated = DF_Aggregated.dropDuplicates()

except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")

# COMMAND ----------

try: 
    # Define a window spec partitioned by Mergekey
    window_spec = Window.partitionBy("Mergekey")

    # List of columns to aggregate (excluding Mergekey)
    columns_to_aggregate = [c for c in DF_Aggregated.columns if c != "Mergekey"]

    # Create a new DataFrame with max values for all columns within each Mergekey
    DF_Min_Aggregated = DF_Aggregated.select(
        "Mergekey",
        *[min(col(column)).over(window_spec).alias(column) for column in columns_to_aggregate]
    )

    # Keep only one row per Mergekey
    DF_Min_Aggregated = DF_Min_Aggregated.groupBy("Mergekey").agg(
        *[first(col(column)).alias(column) for column in DF_Min_Aggregated.columns if column != "Mergekey"]
    )

except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")





# COMMAND ----------

try: 
    #ensure both DataFrames have the same schema
    columns_customer = set(DF_Customer_Validation.columns)
    columns_aggregated = set(DF_Min_Aggregated.columns)

    # Find columns that are in one DataFrame but not in the other
    columns_only_in_customer = columns_customer - columns_aggregated
    columns_only_in_aggregated = columns_aggregated - columns_customer

    # Add missing columns to DF_Customer_Validation with null values
    for col in columns_only_in_aggregated:
        DF_Customer_Validation = DF_Customer_Validation.withColumn(col, lit(None))

    # Add missing columns to DF_Min_Aggregated with null values
    for col in columns_only_in_customer:
        DF_Min_Aggregated = DF_Min_Aggregated.withColumn(col, lit(None))

    # union the DataFrames
    DF_Customer_Validation = DF_Customer_Validation.unionByName(DF_Min_Aggregated)

    DF_Customer_Validation = DF_Customer_Validation.withColumn(
    "Expected_Margin",
    when((col("Expected_Margin").isNull()) | (col("Expected_Margin").cast("double") == 0), lit("NA"))
    .otherwise(col("Expected_Margin").cast("string"))
)


    DF_Customer_Validation.createOrReplaceTempView("Duplicates_Validated")
    DF_Customer_Validation.createOrReplaceTempView('Check_1')

except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### AWARD DETAILS VALIDATION

# COMMAND ----------

try: 


    # Execute your SQL query to create DF_Validated
    DF_Customer_Validation = spark.sql('''
        SELECT DISTINCT
            s.Mergekey,
            s.RFP_Name,
            s.Customer_Slug,
            s.Customer_Master,
            as.Customer_Master as Customer_Master_Validated,
            s.SAM_Name,
            CASE 
                WHEN s.Validation_Criteria LIKE 'city-%' THEN
                    CASE 
                        WHEN SPLIT(s.Origin, ',')[0] = 'None' THEN NULL
                        ELSE SPLIT(s.Origin, ',')[0]
                    END
                ELSE NULL
            END AS Origin_City,
            CASE 
                WHEN s.Validation_Criteria LIKE 'city-%' THEN
                    CASE 
                        WHEN SPLIT(s.Origin, ',')[1] = 'None' THEN NULL
                        ELSE SPLIT(s.Origin, ',')[1]
                    END
                ELSE NULL
            END AS Origin_State,
            CASE 
                WHEN s.Validation_Criteria LIKE '5digit%' THEN s.Origin
                ELSE NULL
            END AS Origin_Zip,
            CASE 
                WHEN s.Validation_Criteria LIKE '%-city' THEN
                    CASE 
                        WHEN SPLIT(s.Destination, ',')[0] = 'None' THEN NULL
                        ELSE SPLIT(s.Destination, ',')[0]
                    END
                ELSE NULL
            END AS Destination_City,
            CASE 
                WHEN s.Validation_Criteria LIKE '%-city' THEN
                    CASE 
                        WHEN SPLIT(s.Destination, ',')[1] = 'None' THEN NULL
                        ELSE SPLIT(s.Destination, ',')[1]
                    END
                ELSE NULL
            END AS Destination_State,
            CASE 
                WHEN s.Validation_Criteria LIKE '%-5digit' THEN s.Destination
                ELSE NULL
            END AS Destination_Zip,
            s.Origin, 
            s.Origin_Country,
            s.Destination,
            s.Destination_Country, 
            s.Equipment_Type,
            s.Estimated_Volume,
            s.Estimated_Miles,
            s.Linehaul,
            s.RPM,
            s.Flat_Rate,
            s.Min_Rate,
            s.Max_Rate,
            s.Validation_Criteria,
            s.Award_Type,
            s.Award_Notes,
            s.Periodic_Start_Date,
            s.Periodic_End_Date,
            s.Shipper_Recurrence,
            s.Contract_Type,
            s.updated_by,
            s.updated_date,
            s.Expected_Margin
        FROM
            Duplicates_Validated s
        LEFT JOIN
            bronze.lookup_shippers as ON UPPER(s.Customer_Master) = UPPER(as.Customer_Master)
        GROUP BY
            s.Mergekey,
            s.RFP_Name,
            s.Customer_Slug,
            s.Customer_Master,
            as.Customer_Master,
            s.SAM_Name,
            s.Origin,
            s.Origin_Country,
            s.Destination,
            s.Destination_Country,
            s.Equipment_Type,
            s.Estimated_Volume,
            s.Estimated_Miles,
            s.Linehaul,
            s.RPM,
            s.Flat_Rate,
            s.Min_Rate,
            s.Max_Rate,
            s.Validation_Criteria,
            s.Award_Type,
            s.Award_Notes,
            s.Periodic_Start_Date,
            s.Periodic_End_Date,
            s.Shipper_Recurrence,
            s.Contract_Type,
            s.updated_by,
            s.updated_date,
            s.Expected_Margin
    ''')

except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")

# COMMAND ----------

try: 

    def validate_row_custom(row):
        exceptions = []
        
        # Helper function to safely get values
        def safe_get(key):
            return row[key] if key in row and row[key] is not None else None
        
        # 1. Customer_Master check
        if safe_get("Customer_Master_Validated") is None:
            exceptions.append("Not a valid Customer Master")

       # 2. Validation_Criteria checks
        validation_criteria = (safe_get("Validation_Criteria") or "").strip()
        if not validation_criteria:
            exceptions.append("Validation_Criteria should not be null")
        elif validation_criteria == "5digit-5digit":
            origin_zip = safe_get("Origin_Zip")
            destination_zip = safe_get("Destination_Zip")
            origin_country = safe_get("Origin_Country")
            destination_country = safe_get("Destination_Country")
            
            # US Zip Code Checks
            if not origin_zip:
                exceptions.append("Missing Origin_Zip")
            else:
                if origin_zip.isdigit():
                    if len(origin_zip) > 5:
                        exceptions.append("US Origin_Zip exceeds 5 digits")
            
            if not destination_zip:
                exceptions.append("Missing Destination_Zip")
            else:
                if destination_zip.isdigit():
                    if len(destination_zip) > 5:
                        exceptions.append("US Destination_Zip exceeds 5 digits")

 
            # Canadian Zip Code Checks (Alphanumeric)
            if origin_zip and not origin_zip.isdigit():
                if len(origin_zip) > 6:
                    exceptions.append("Canadian Origin_Zip exceeds 6 characters")
            
            if destination_zip and not destination_zip.isdigit():
                if len(destination_zip) > 6:
                    exceptions.append("Canadian Destination_Zip exceeds 6 characters")

            if not origin_country:
                exceptions.append("Missing Origin_Country")
            elif origin_country.upper() not in ["US", "CA", "MX"]:
                exceptions.append("Invalid Origin_Country")

            if not destination_country:
                exceptions.append("Missing Destination_Country")
            elif destination_country.upper() not in ["US", "CA", "MX"]:
                exceptions.append("Invalid Destination_Country")

        elif validation_criteria == "city-city":
            origin_country = safe_get("Origin_Country")
            destination_country = safe_get("Destination_Country")
            if not safe_get("Origin_City"):
                exceptions.append("Missing Origin_City")
            if not safe_get("Destination_City"):
                exceptions.append("Missing Destination_City")

            if not origin_country:
                exceptions.append("Missing Origin_Country")
            elif origin_country.upper() not in ["US", "CA", "MX"]:
                exceptions.append("Invalid Origin_Country")

            if not destination_country:
                exceptions.append("Missing Destination_Country")
            elif destination_country.upper() not in ["US", "CA", "MX"]:
                exceptions.append("Invalid Destination_Country")
                
        # 3. Periodic_start_date and end_date checks
        if safe_get("Periodic_Start_Date") is None:
            exceptions.append("Periodic_Start_Date should not be null")
        if safe_get("Periodic_End_Date") is None:
            exceptions.append("Periodic_End_Date should not be null")
        
        # 4. RFP_Name check
        if safe_get("RFP_Name") is None:
            exceptions.append("RFP_Name should not be null")

        # 5. Periodic_Start_Date and end_date checks 
        if safe_get("Periodic_Start_Date") is not None and safe_get("Periodic_End_Date") is not None and safe_get("Periodic_Start_Date") > safe_get("Periodic_End_Date"):
            exceptions.append("Periodic_End_Date should be less than Periodic_Start_Date")

        # 6. Equipment Type Restriction
        equipment_type = safe_get("Equipment_Type")
        if equipment_type is None:
            exceptions.append("Missing Equipment Type")
        elif equipment_type.upper() not in ["VAN", "REEFER", "IMDL", "FLATBED"]:
            exceptions.append("Invalid Equipment Type")
   
        return "; ".join(exceptions) if exceptions else "NO_EXCEPTIONS"

    # Define the UDF that uses the Python function
    validate_row_udf = udf(validate_row_custom, StringType())

    # Apply the validation to DF_Validated
    DF_Customer_Validation = DF_Customer_Validation.withColumn(
        "Exception_Type", 
        validate_row_udf(struct(*DF_Customer_Validation.columns))
    )

    # Filter for records with no exceptions
    DF_Customer_Validated = DF_Customer_Validation.filter(col("Exception_Type") == "NO_EXCEPTIONS")

    # Filter for records with exceptions
    DF_Exception_Customer_Master = DF_Customer_Validation.filter(col("Exception_Type") != "NO_EXCEPTIONS")

    DF_Customer_Validated.createOrReplaceTempView("Customer_Validated")
    # DF_Customer_Validated.createOrReplaceTempView("Customer_Validated2")
    
except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### LANE VALIDATION

# COMMAND ----------

try:
    DF_Lane_Validation = spark.sql("""
        WITH address AS (
            SELECT DISTINCT Concat(REPLACE(UPPER(TRIM(Place_Name)), ' ', ''), ',', UPPER(TRIM(State_Code))) as Lane, Upper(Country_Code) as Country_code
            FROM Bronze.lookup_address_awards
        ),
        city_only AS (
            SELECT DISTINCT REPLACE(UPPER(TRIM(Place_Name)), ' ', '') as Lane, Upper(Country_Code) as Country_Code 
            FROM Bronze.lookup_address_awards
        ),
        zips AS (
            SELECT DISTINCT REPLACE(UPPER(TRIM(Zip)), ' ', '') as Zip, Upper(Country_Code) as Country_Code
            FROM Bronze.lookup_address_awards
        ), 
        Customer_Validated2 as (SELECT * FROM Customer_Validated)

        -- First UNION: Handle 'city-city' and '5digit-city' criteria
SELECT 
    a.Mergekey, a.RFP_Name, a.Customer_Slug, a.Customer_Master, a.SAM_Name,
    a.Origin_City, a.Origin_State, a.Origin_Zip, a.Destination_City, a.Destination_State,
    a.Destination_Zip, a.Origin, a.Origin_Country, a.Destination_Country, a.Destination, a.Equipment_Type, a.Estimated_Volume,
    a.Estimated_Miles, a.Linehaul, a.RPM, a.Flat_Rate, a.Min_Rate, a.Max_Rate,
    a.Validation_Criteria, a.Award_Type, a.Award_Notes, a.Periodic_Start_Date,
    a.Periodic_End_Date, a.Shipper_Recurrence, a.Contract_Type, a.updated_by,
    a.updated_date, a.Expected_Margin,
    CASE 
        WHEN a.validation_criteria = 'city-city' AND a.origin_state IS NOT NULL THEN b.Lane
        WHEN a.validation_criteria = 'city-city' AND a.origin_state IS NULL THEN d.Lane
    END as Validated_Origin,
    CASE 
        WHEN a.validation_criteria LIKE '%-city' AND a.destination_state IS NOT NULL THEN c.Lane
        WHEN a.validation_criteria LIKE '%-city' AND a.destination_state IS NULL THEN e.Lane
        -- WHEN a.validation_criteria = '5digit-city' AND a.destination_state IS NOT NULL THEN b.Lane
        -- WHEN a.validation_criteria = '5digit-city' AND a.destination_state IS NULL THEN e.Lane
        ELSE NULL
    END as Validated_Destination,
    CASE 
        WHEN a.validation_criteria = 'city-city' and a.Origin_State IS NOT NULL THEN b.Country_Code
        WHEN a.validation_criteria = 'city-city' and a.Origin_State IS NULL THEN d.Country_Code
    END AS Validated_Origin_Country, 
    CASE
        WHEN a.validation_criteria = 'city-city' and a.Destination_State IS NOT NULL THEN b.Country_Code
        WHEN a.validation_criteria = 'city-city' and a.Destination_State IS NULL THEN d.Country_Code
    END AS Validated_Destination_Country

FROM 
    Customer_Validated2 a
LEFT JOIN 
    address b ON REPLACE(UPPER(TRIM(a.Origin)), ' ', '') = b.Lane and TRIM(UPPER(a.Origin_Country)) = TRIM(UPPER(b.Country_Code))
LEFT JOIN 
    address c ON REPLACE(UPPER(TRIM(a.Destination)), ' ','') = c.Lane and TRIM(UPPER(a.Destination_Country)) = TRIM(UPPER(c.Country_Code))
LEFT JOIN  
    city_only d ON REPLACE(UPPER(TRIM(a.origin_city)), ' ', '') = d.Lane and TRIM(UPPER(a.Origin_Country)) = TRIM(UPPER(d.Country_Code))
LEFT JOIN 
    city_only e ON REPLACE(UPPER(TRIM(a.destination_city)), ' ', '') = e.Lane and TRIM(UPPER(a.Destination_Country)) = TRIM(UPPER(e.Country_Code))
WHERE 
    a.validation_criteria IN ('city-city')

UNION

-- Second UNION: Handle '5digit-5digit' and 'city-5digit' criteria
SELECT 
    a.Mergekey, a.RFP_Name, a.Customer_Slug, a.Customer_Master, a.SAM_Name,
    a.Origin_City, a.Origin_State, a.Origin_Zip, a.Destination_City, a.Destination_State,
    a.Destination_Zip, a.Origin,a.Origin_Country, a.Destination_Country, a.Destination, a.Equipment_Type, a.Estimated_Volume,
    a.Estimated_Miles, a.Linehaul, a.RPM, a.Flat_Rate, a.Min_Rate, a.Max_Rate,
    a.Validation_Criteria, a.Award_Type, a.Award_Notes, a.Periodic_Start_Date,
    a.Periodic_End_Date, a.Shipper_Recurrence, a.Contract_Type, a.updated_by,
    a.updated_date, a.Expected_Margin,
    CASE 
        WHEN a.validation_criteria = '5digit-5digit' THEN oz.Zip
        ELSE NULL
    END as Validated_Origin,
    CASE 
        WHEN a.validation_criteria LIKE '%-5digit' THEN dz.Zip
        ELSE NULL
    END as Validated_Destination, 
    CASE 
        WHEN a.validation_criteria = '5digit-5digit' THEN oz.Country_Code
        ELSE NULL
    END AS Validated_Origin_Country,
    CASE
        WHEN a.validation_criteria LIKE '%-5digit' THEN dz.Country_Code
        ELSE NULL
    END AS Validated_Destination_Country
FROM 
    Customer_Validated2 a
LEFT JOIN 
    zips oz ON 
        CASE
            WHEN REGEXP_LIKE(a.origin_zip, '^[0-9]+$') AND LENGTH(a.origin_zip) <= 5 THEN LPAD(a.origin_zip, 5, '0')
            ELSE REPLACE(UPPER(TRIM(a.origin_zip)), ' ', '')
        END = 
        CASE
            WHEN REGEXP_LIKE(oz.zip, '^[0-9]+$') AND LENGTH(oz.zip) <= 5 THEN LPAD(oz.zip, 5, '0')
            ELSE REPLACE(UPPER(TRIM(oz.zip)), ' ', '')
        END
        AND TRIM(UPPER(a.Origin_Country)) = TRIM(UPPER(oz.Country_Code))
LEFT JOIN 
    zips dz ON 
        CASE
            WHEN REGEXP_LIKE(a.destination_zip, '^[0-9]+$') AND LENGTH(a.destination_zip) <= 5 THEN LPAD(a.destination_zip, 5, '0')
            ELSE REPLACE(UPPER(TRIM(a.destination_zip)), ' ', '')
        END = 
        CASE
            WHEN REGEXP_LIKE(dz.zip, '^[0-9]+$') AND LENGTH(dz.zip) <= 5 THEN LPAD(dz.zip, 5, '0')
            ELSE REPLACE(UPPER(TRIM(dz.zip)), ' ', '')
        END
        AND TRIM(UPPER(a.Destination_Country)) = TRIM(UPPER(dz.Country_Code))
WHERE 
    a.validation_criteria IN ('5digit-5digit')

 """)
    DF_Lane_Validation.createOrReplaceTempView("Lane_Validation")

except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")





# COMMAND ----------


try: 
    DF_Lane_Validation_Final = spark.sql("""SELECT   a.Mergekey , a.RFP_Name , a.Customer_Slug , a.Customer_Master , a.SAM_Name , 
        CASE 
            WHEN a.Validation_Criteria LIKE '5digit-%' THEN 
                CASE 
                    WHEN REGEXP_LIKE(a.Origin, '^[0-9]+$') THEN LPAD(a.Origin, 5, '0')
                    ELSE REPLACE(TRIM(UPPER(a.Origin)), ' ', '') END
            WHEN a.Validation_Criteria LIKE 'city-%' THEN a.ORIGIN

        END AS Origin,
        
    CASE 
            WHEN a.Validation_Criteria LIKE '%-5digit' THEN 
                CASE 
                    WHEN REGEXP_LIKE(a.Destination, '^[0-9]+$') THEN LPAD(a.Destination, 5, '0')
                    ELSE REPLACE(TRIM(UPPER(a.Destination)), ' ', '') END
            WHEN a.Validation_Criteria LIKE '%-city' THEN a.Destination

        END AS Destination , a.Origin_Country , a.Destination_Country , a.Equipment_Type , a.Estimated_Volume , a.Estimated_Miles , a.Linehaul , a.RPM , a.Flat_Rate , a.Min_Rate , a.Max_Rate , a.Validation_Criteria , a.Award_Type , a.Award_Notes , a.Periodic_Start_Date , a.Periodic_End_Date , a.Shipper_Recurrence , a.Contract_Type , a.updated_by , a.updated_date , a.Expected_Margin,
            CASE 
                WHEN a.validated_origin IS NULL THEN 'Not a valid origin'
                WHEN a.validated_destination IS NULL THEN 'Not a valid destination'
                WHEN a.validated_origin_country IS NULL THEN 'Not a valid origin country'
                WHEN a.validated_destination_country IS NULL THEN 'Not a valid destination country'
                ELSE 0
            END AS Exception_Type
            FROM Lane_Validation a""")
    DF_Lane_Validation_Final.createOrReplaceTempView("Lane_Validation_Final")
 
    DF_Silver_Validated_Final = DF_Lane_Validation_Final.filter(col('Exception_Type') == 0 ) 

    try:
        
        Hashkey_columns = [
            'Estimated_Volume', 'Estimated_Miles', 'Linehaul', 'RPM', 'Flat_Rate',
            'Min_Rate', 'Max_Rate', 'Award_Type', 'Award_Notes',
            'Shipper_Recurrence', 'updated_by',
            'Updated_date',  'Expected_Margin'
        ]

        DF_Silver_Validated_Final = DF_Silver_Validated_Final.withColumn("Hashkey",md5(concat_ws("",*Hashkey_columns)))

    except Exception as e:
        logger.error(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
        print(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
        
    DF_Silver_Validated_Final.dropDuplicates()
    DF_Silver_Validated_Final.createOrReplaceTempView("VW_Silver_Validated_Final")

    DF_Lane_Exceptions = DF_Lane_Validation_Final.filter(lower(col('Exception_Type')).startswith("not"))
    # DF_Lane_Exceptions = spark.sql('''SELECT  * FROM Lane_Validation_Final WHERE Exception_Type <> 0''')
   
        
except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")

     


# COMMAND ----------

try: 
    # Remove Customer_Master_Validated column from DF_Exception_Customer_Master
    DF_Exception_Customer_Master = DF_Exception_Customer_Master.drop("Customer_Master_Validated","Origin_city", "Origin_State", "Origin_Zip", "Destination_State", "Destination_Zip", "Destination_City", "Destination_City", "Destination_State" )

    # Remove specified columns from DF_Lane_Exceptions
    columns_to_remove = [
        "Validated_Origin", "Validated_Destination", "Validated_Origin_Country", "Validated_Destination_Country"
    ]
    DF_Lane_Exceptions = DF_Lane_Exceptions.drop(*columns_to_remove)

    # Remove Exception_Columns and Differing_Columns_Array from DF_Differing_Columns_Exception
    DF_Differing_Columns_Exception = DF_Differing_Columns_Exception.drop("Exception_Columns", "Differing_Columns_Array")

    # Union the DataFrames
    DF_Exceptions = DF_Exception_Customer_Master.unionByName(DF_Lane_Exceptions).unionByName(DF_Differing_Columns_Exception)
    DF_Exceptions.createOrReplaceTempView("VW_Exceptions_Final")
    DF_Exceptions_Final = spark.sql('SELECT  DISTINCT a.* , b.Exception_Type FROM Silver.Silver_NFI_Awards_Filtered a RIGHT JOIN  VW_Exceptions_Final b ON a.Mergekey = b.Mergekey') 
    DF_Exceptions_Final.dropDuplicates()

    try:
        
        Hashkey_columns = [
             'RFP_Name', 'Customer_Master', 'Customer_Slug', 'SAM_Name', 
            'Equipment_Type', 'Estimated_Volume', 'Estimated_Miles', 'Linehaul', 'RPM', 'Flat_Rate',
            'Min_Rate', 'Max_Rate', 'Validation_Criteria', 'Award_Type', 'Award_Notes',
            'Periodic_Start_Date', 'Periodic_End_Date', 'Shipper_Recurrence', 'updated_by',
            'Updated_date', 'Contract_Type', 'Expected_Margin', 'Exception_Type'
        ]

        DF_Exceptions_Final = DF_Exceptions_Final.withColumn("Hashkey",md5(concat_ws("",*Hashkey_columns)))

    except Exception as e:
        logger.error(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
        print(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
    DF_Exceptions_Final.dropDuplicates()
    DF_Exceptions_Final.createOrReplaceTempView("VW_Silver_Exceptions_Final")

   

except Exception as e:
    logger.error(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")
    print(f"Error in NFI_Silver_Awards_Validation_&_ Exception: {e}")




# COMMAND ----------

# MAGIC %md
# MAGIC #### MERGE VALIDATED DATA

# COMMAND ----------

# Getting the Table details in the dataframe that needs to be loaded into silver from Metadata Table
Table_ID = 'AW3'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = 'AW3' and IsActive='1'")
Job_ID = DF_Metadata.select(col('JOB_ID')).where(col('TableID') == Table_ID).collect()[0]['JOB_ID']
Notebook_ID = DF_Metadata.select(col('NB_ID')).where(col('TableID') == Table_ID).collect()[0]['NB_ID']

# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(Table_ID,'Silver')
    if AutoSkipperCheck == 0:
        TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == Table_ID).collect()[0].SourceTableName
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateValue
        Zone = DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone
                
        spark.sql('''Merge into silver.Silver_NFI_Awards_Validated as CT using VW_Silver_Validated_Final as CS ON CS.mergekey=CT.mergekey WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                CT.hashkey	=	CS.hashkey,
                CT.Estimated_Volume	=	CS.Estimated_Volume,
                CT.Estimated_Miles	=	CS.Estimated_Miles,
                CT.Linehaul	=	CS.Linehaul,
                CT.RPM	=	CS.RPM,
                CT.Flat_Rate	=	CS.Flat_Rate,
                CT.Min_Rate	=	CS.Min_Rate,
                CT.Max_Rate	=	CS.Max_Rate,
                CT.Award_Type	=	CS.Award_Type,
                CT.Award_Notes	=	CS.Award_Notes,
                CT.Shipper_Recurrence	=	CS.Shipper_Recurrence,
                CT.Updated_by	=	CS.Updated_by,
                CT.Updated_date	=	CS.Updated_date,
                CT.Expected_Margin = CS.Expected_Margin,
                CT.Last_Modified_Date = Current_Timestamp(),
                CT.Last_Modified_BY = 'Databricks',
                CT.Is_Deleted = 0
                
                WHEN NOT MATCHED 
                THEN INSERT 
                (   
                    
                    CT.Mergekey,
                    CT.Hashkey,
                    CT.RFP_Name,
                    CT.Customer_Slug,
                    CT.Customer_Master,
                    CT.SAM_Name,
                    CT.Origin,
                    CT.Origin_Country,
                    CT.Destination,
                    CT.Destination_Country,
                    CT.Equipment_Type,
                    CT.Estimated_Volume,
                    CT.Estimated_Miles,
                    CT.Linehaul,
                    CT.RPM,
                    CT.Flat_Rate,
                    CT.Min_Rate,
                    CT.Max_Rate,
                    CT.Validation_Criteria,
                    CT.Award_Type,
                    CT.Award_Notes,
                    CT.Periodic_Start_Date,
                    CT.Periodic_End_Date,
                    CT.Shipper_Recurrence,
                    CT.Updated_by,
                    CT.Updated_date,
                    CT.Contract_Type,
                    CT.Expected_Margin,
                    CT.Sourcesystem_ID,
                    CT.Sourcesystem_Name,
                    CT.Created_Date,
                    CT.Created_By,
                    CT.Last_Modified_Date,
                    CT.Last_Modified_By,
                    CT.Is_Deleted
                    )
                    VALUES
                    (
                        CS.Mergekey,
                        CS.Hashkey,
                        CS.RFP_Name,
                        CS.Customer_Slug,
                        CS.Customer_Master,
                        CS.SAM_Name,
                        CS.Origin,
                        CS.Origin_Country,
                        CS.Destination,
                        CS.Destination_Country,
                        CS.Equipment_Type,
                        CS.Estimated_Volume,
                        CS.Estimated_Miles,
                        CS.Linehaul,
                        CS.RPM,
                        CS.Flat_Rate,
                        CS.Min_Rate,
                        CS.Max_Rate,
                        CS.Validation_Criteria,
                        CS.Award_Type,
                        CS.Award_Notes,
                        CS.Periodic_Start_Date,
                        CS.Periodic_End_Date,
                        CS.Shipper_Recurrence,
                        CS.Updated_by,
                        CS.Updated_date,
                        CS.Contract_Type,
                        CS.Expected_Margin,
                        9,
                        'Awards File',
                        current_timestamp(),
                        'Databricks',
                        current_timestamp(),
                        'Databricks',
                        0
                    )
            ''')
        try:
            subquery = 'select DISTINCT t.Mergekey from Silver.Silver_NFI_Awards_Validated t left join VW_Silver_Validated_Final s on t.Mergekey = s.Mergekey where s.Mergekey is null'
            df_delete= spark.sql(subquery)
            df_delete.createOrReplaceTempView('VW_delete')
                # Source Delete Handling

            spark.sql('''MERGE INTO Silver.Silver_NFI_Awards_Validated AS target
                            USING VW_delete AS source
                            ON target.MergeKey = source.Mergekey
                            WHEN MATCHED AND target.Is_Deleted = 0
                            THEN UPDATE SET target.Is_Deleted = 1,
                            target.Last_Modified_Date = current_timestamp()''')
        except Exception as e:
            print('Cant perform the delete', e)

        MaxDateQuery = "Select max({0}) as Max_Date from Silver.Silver_NFI_Awards_Validated"
        MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
        DF_MaxDate = spark.sql(MaxDateQuery)
        UpdateLastLoadDate(Table_ID,DF_MaxDate)
        UpdatePipelineStatusAndTime(Table_ID,'Silver')
        UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Load")
      
except Exception as e:
    logger.info('Failed for Silver load')
    UpdateFailedStatus(Table_ID,'Silver')
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Load")
    print('Unable to load '+TableName)
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### MERGE EXCEPTIONS DATA

# COMMAND ----------


# Getting the Table details in the dataframe that needs to be loaded into silver from Metadata Table
Table_ID = 'AW4'
DF_Metadata = spark.sql("SELECT * FROM Metadata.MasterMetadata WHERE Tableid = 'AW4' and IsActive='1'")
Job_ID = DF_Metadata.select(col('JOB_ID')).where(col('TableID') == Table_ID).collect()[0]['JOB_ID']
Notebook_ID = DF_Metadata.select(col('NB_ID')).where(col('TableID') == Table_ID).collect()[0]['NB_ID']


# COMMAND ----------

try:
    AutoSkipperCheck = AutoSkipper(Table_ID,'Silver')
    if AutoSkipperCheck == 0:
        TableName = DF_Metadata.select(col('SourceTableName')).where(col('TableID') == Table_ID).collect()[0].SourceTableName
        MaxLoadDateColumn = DF_Metadata.select(col('LastLoadDateColumn')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateColumn
        MaxLoadDate = DF_Metadata.select(col('LastLoadDateValue')).where(col('TableID') == Table_ID).collect()[0].LastLoadDateValue
        Zone = DF_Metadata.select(col('Zone')).where(col('TableID') == Table_ID).collect()[0].Zone
    
        try:
            # Merging the records with the actual table
            spark.sql('''Merge into silver.Silver_NFI_Awards_Exceptions as CT using VW_Silver_Exceptions_Final as CS ON CS.DW_Award_ID=CT.DW_Award_ID WHEN MATCHED AND CS.Hashkey!=CT.Hashkey THEN UPDATE SET
                    CT.Hashkey	=	CS.hashkey,
                    CT.Estimated_Volume	=	CS.Estimated_Volume,
                    CT.Estimated_Miles	=	CS.Estimated_Miles,
                    CT.Linehaul	=	CS.Linehaul,
                    CT.RPM	=	CS.RPM,
                    CT.Flat_Rate	=	CS.Flat_Rate,
                    CT.Min_Rate	=	CS.Min_Rate,
                    CT.Max_Rate	=	CS.Max_Rate,
                    CT.Award_Type	=	CS.Award_Type,
                    CT.Award_Notes	=	CS.Award_Notes,
                    CT.Shipper_Recurrence	=	CS.Shipper_Recurrence,
                    CT.Updated_by	=	CS.Updated_by,
                    CT.Updated_date	=	CS.Updated_date,
                    CT.Contract_Type	=	CS.Contract_Type,
                    CT.Expected_Margin	=	CS.Expected_Margin,
                    CT.Exception_Type = CS.Exception_Type,
                    CT.Last_Modified_Date = current_timestamp(),
                    CT.Last_Modified_By = 'Databricks', 
                    CT.Is_Deleted = 0
                    
                    WHEN NOT MATCHED 
                    THEN INSERT 
                    (   
                        CT.DW_Award_ID,
                        CT.Hashkey,
                        CT.RFP_Name,
                        CT.Customer_Slug,
                        CT.Customer_Master,
                        CT.SAM_Name,
                        CT.Origin,
                        CT.Origin_Country,
                        CT.Destination,
                        CT.Destination_Country,
                        CT.Equipment_Type,
                        CT.Estimated_Volume,
                        CT.Estimated_Miles,
                        CT.Linehaul,
                        CT.RPM,
                        CT.Flat_Rate,
                        CT.Min_Rate,
                        CT.Max_Rate,
                        CT.Validation_Criteria,
                        CT.Award_Type,
                        CT.Award_Notes,
                        CT.Periodic_Start_Date,
                        CT.Periodic_End_Date,
                        CT.Shipper_Recurrence,
                        CT.Updated_by,
                        CT.Updated_date,
                        CT.Contract_Type,
                        CT.Expected_Margin,
                        CT.Exception_Type,
                        CT.Sourcesystem_ID,
                        CT.Sourcesystem_Name,
                        CT.Created_By,
                        CT.Created_Date,
                        CT.Last_Modified_By,
                        CT.Last_Modified_Date,
                        CT.Is_Deleted
                        )
                        VALUES
                        (
                            CS.DW_Award_ID,
                            CS.Hashkey,
                            CS.RFP_Name,
                            CS.Customer_Slug,
                            CS.Customer_Master,
                            CS.SAM_Name,
                            CS.Origin,
                            CS.Origin_Country,
                            CS.Destination,
                            CS.Destination_Country,
                            CS.Equipment_Type,
                            CS.Estimated_Volume,
                            CS.Estimated_Miles,
                            CS.Linehaul,
                            CS.RPM,
                            CS.Flat_Rate,
                            CS.Min_Rate,
                            CS.Max_Rate,
                            CS.Validation_Criteria,
                            CS.Award_Type,
                            CS.Award_Notes,
                            CS.Periodic_Start_Date,
                            CS.Periodic_End_Date,
                            CS.Shipper_Recurrence,
                            CS.Updated_by,
                            CS.Updated_date,
                            CS.Contract_Type,
                            CS.Expected_Margin,
                            CS.Exception_Type,
                            9,
                            'Awards File',
                            'Databricks',
                            Current_Timestamp(),
                            'Databricks',
                            Current_Timestamp(),
                            0
                        )
                    ''')
            
            spark.sql(f'''
                        UPDATE Silver.Silver_NFI_Awards_Exceptions
                        SET  Silver.Silver_NFI_Awards_Exceptions.Is_Deleted = '1', 
                            Silver.Silver_NFI_Awards_Exceptions.Last_Modified_Date = Current_Timestamp()
                        where  Silver.Silver_NFI_Awards_Exceptions.DW_Award_ID in (select s.DW_Award_ID from Silver.Silver_NFI_Awards_Filtered s where S.Is_Deleted= '1') AND Silver.Silver_NFI_Awards_Exceptions.Is_Deleted = '0'
                    ''')
            spark.sql(f''' 
                        UPDATE Silver.Silver_NFI_Awards_Exceptions
                        SET Silver.Silver_NFI_Awards_Exceptions.Is_Deleted = '1',
                            Silver.Silver_NFI_Awards_Exceptions.Last_Modified_Date = Current_Timestamp()
                        WHERE TRIM(UPPER(Silver.Silver_NFI_Awards_Exceptions.Customer_Master)) IN (select TRIM(UPPER(s.Customer_Master)) from Silver.Silver_NFI_Awards_Exceptions s where S.Is_Deleted = 0) AND Exception_Type = "Not a valid Customer Master" AND Silver.Silver_NFI_Awards_Exceptions.Is_Deleted = 0 ''')
           
            
            MaxDateQuery = "Select max({0}) as Max_Date from silver.Silver_NFI_Awards_Exceptions"
            MaxDateQuery = MaxDateQuery.format(MaxLoadDateColumn)
            DF_MaxDate = spark.sql(MaxDateQuery)
            UpdateLastLoadDate(Table_ID,DF_MaxDate)
            UpdatePipelineStatusAndTime(Table_ID,'Silver')
            UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Succeeded", None, None, 0, "Load")

        except Exception as e:
            logger.info(f"Unable to perform merge operation to load data to Silver_NFI_Awards_Exceptions table")
            UpdateFailedStatus(Table_ID,'Silver')
            Error_Statement = str(e).replace(',', '').replace("'", '')
            UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Load")
            print(e)

except Exception as e:
    logger.info('Failed for Silver load')
    UpdateFailedStatus(Table_ID,'Silver')
    print('Unable to load '+TableName)
    print(e)
    Error_Statement = str(e).replace(',', '').replace("'", '')
    UpdateLogStatus(Job_ID, Table_ID, Notebook_ID, TableName, Zone, "Failed", Error_Statement, "NotIgnorable", 1, "Load")
