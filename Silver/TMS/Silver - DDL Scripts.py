# Databricks notebook source
## Mention the catalog used, getting the catalog name from key vault
CatalogName = dbutils.secrets.get(scope="Brokerage-Catalog", key="CatalogName")
spark.sql(f"USE CATALOG {CatalogName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver_Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Customer (
# MAGIC 	DW_Customer_ID BIGINT GENERATED ALWAYS AS IDENTITY ,
# MAGIC 	Customer_ID VARCHAR(255) NOT NULL,
# MAGIC 	Hashkey VARCHAR(255),
# MAGIC 	Customer_Name VARCHAR(255),
# MAGIC 	Customer_Phone1 VARCHAR(255),
# MAGIC 	Customer_Phone2 VARCHAR(255),
# MAGIC 	Customer_Phone3 VARCHAR(255),
# MAGIC 	Customer_Email1 VARCHAR(255),
# MAGIC 	Customer_Email2 VARCHAR(255),
# MAGIC 	Customer_Email3 VARCHAR(255),
# MAGIC 	Customer_Email4 VARCHAR(255),
# MAGIC 	Cust_Address1 VARCHAR(300),
# MAGIC 	Cust_Address2 VARCHAR(300),
# MAGIC 	Customer_City VARCHAR(255),
# MAGIC 	Customer_Country VARCHAR(255),
# MAGIC 	Customer_State VARCHAR(255),
# MAGIC 	Customer_Zipcode VARCHAR(255),
# MAGIC 	Invoice_Method VARCHAR(255),
# MAGIC 	Invoicing_CustomerProfile_ID VARCHAR(255),
# MAGIC 	Credit_Limit VARCHAR(255),
# MAGIC 	Customer_Status VARCHAR(255),
# MAGIC 	Aljex_ID Varchar(255) ,
# MAGIC 	Lawson_ID Varchar(255) ,
# MAGIC 	Been_Discarded BOOLEAN ,
# MAGIC 	External_IDs STRING,
# MAGIC 	Has_Unmapped_External_IDs BOOLEAN,
# MAGIC 	Account_Manager Varchar(255),
# MAGIC 	Office Varchar(255),
# MAGIC 	Sales_Rep Varchar(255),
# MAGIC 	Secondary_Bill_To Varchar(255),
# MAGIC 	Customer_Added_At TIMESTAMP_NTZ,
# MAGIC 	Customer_Added_By INTEGER,
# MAGIC 	Auto_Invoice_Delay INTEGER,
# MAGIC 	Is_Auto_Invoice_Enabled BOOLEAN,
# MAGIC 	To_Show_Intermediary_Stops BOOLEAN,
# MAGIC 	To_Stop_Tracking_Requirement BOOLEAN,
# MAGIC 	Document_Requirements Varchar(255),
# MAGIC 	Data_Requirements Varchar(255),
# MAGIC 	Salesperson_Name VARCHAR(255),
# MAGIC 	Sourcesystem_ID INTEGER NOT NULL,
# MAGIC 	Sourcesystem_Name VARCHAR(255) NOT NULL,
# MAGIC 	Created_By VARCHAR(255),
# MAGIC 	Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC 	Last_Modified_By VARCHAR(255),
# MAGIC 	Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC 	CONSTRAINT Silver_Customer_Pkey PRIMARY KEY (DW_Customer_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver_Customer_Contacts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Customer_Contacts (
# MAGIC DW_Custcontact_ID  BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC Cust_ID VARCHAR(255),
# MAGIC DW_Customer_ID BIGINT,
# MAGIC Hashkey VARCHAR(255),
# MAGIC Cust_Name VARCHAR(255),
# MAGIC Contact_Name1  VARCHAR(255),
# MAGIC Contact_Name2 VARCHAR(255),
# MAGIC Contact_Name3 VARCHAR(255),
# MAGIC Contact_Name4  VARCHAR(255),
# MAGIC Contact_Email VARCHAR(255),
# MAGIC Contact_Phone VARCHAR(255),
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Sourcesystem_Name VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255),
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255),
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Dim_Customer_Contacts_Pkey PRIMARY KEY(DW_Custcontact_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver_Employee

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Employee (
# MAGIC DW_Employee_ID BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC Employee_ID  VARCHAR(255),
# MAGIC Hashkey VARCHAR(255),
# MAGIC Employee_Name VARCHAR(255),
# MAGIC Employee_Title VARCHAR(255), 
# MAGIC Employee_Division VARCHAR(255),
# MAGIC Employee_Email VARCHAR(255),
# MAGIC Company_Name VARCHAR(255),
# MAGIC Employee_Department VARCHAR(255),
# MAGIC Employee_Status VARCHAR(255),
# MAGIC Employee_Creation_Date VARCHAR(255),
# MAGIC Employee_LastUpdated_Date VARCHAR(255),
# MAGIC Employment_Type VARCHAR(255),
# MAGIC Seniority_Years VARCHAR(255),
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Sourcesystem_Name VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255),
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255),
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Dim_Employee_Pkey PRIMARY KEY (DW_Employee_ID)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver_Office

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Office(
# MAGIC DW_Office_ID  BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC Office_ID  VARCHAR(255) NOT NULL ,
# MAGIC Hashkey VARCHAR(255),
# MAGIC Office_Name VARCHAR(255) ,
# MAGIC Office_Location VARCHAR(255),
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Sourcesystem_Name  VARCHAR(10)  NOT NULL,
# MAGIC Created_By VARCHAR(15),
# MAGIC Created_Date  Timestamp_ntz NOT NULL,
# MAGIC Last_Modified_By   VARCHAR(15),
# MAGIC Last_Modified_Date  Timestamp_ntz NOT NULL,
# MAGIC CONSTRAINT Dim_Office_Pkey PRIMARY KEY (DW_Office_ID)
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver_Carrier

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Carrier(
# MAGIC DW_Carrier_ID  BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC Hashkey VARCHAR(255), 
# MAGIC Carrier_ID VARCHAR(255),
# MAGIC Carrier_Name VARCHAR(255),
# MAGIC Contact_Details VARCHAR(255),
# MAGIC Carrier_Email1 VARCHAR(255),
# MAGIC Carrier_Email2 VARCHAR(255),
# MAGIC Carrier_Address1 VARCHAR(255),
# MAGIC Carrier_Address2 VARCHAR(255),
# MAGIC Carrier_State VARCHAR(255),
# MAGIC Carrier_ZipCode VARCHAR(255),
# MAGIC Carrier_City VARCHAR(255),
# MAGIC Carrier_Type VARCHAR(255),
# MAGIC Carrier_MCnumber VARCHAR(255),
# MAGIC Lawson_ID VARCHAR(255),
# MAGIC Carrier_Comments VARCHAR(255),
# MAGIC Workman_Comp_Insurer VARCHAR(255),
# MAGIC Workman_Comp_Policy VARCHAR(255),
# MAGIC Workman_Comp_Amount VARCHAR(255),
# MAGIC Workman_Comp_Deduct VARCHAR(255),
# MAGIC CarrierApproved_At VARCHAR(255),
# MAGIC CarrierApproved_By VARCHAR(255),
# MAGIC Denied_At VARCHAR(255),
# MAGIC Denied_By VARCHAR(255),
# MAGIC Prohibted_At VARCHAR(255),
# MAGIC Carrier_Status VARCHAR(255),
# MAGIC Liab_Ins_PolicyID VARCHAR(255),
# MAGIC Cargo_Ins_PolicyID VARCHAR(255),
# MAGIC CargoInsurancer_Name VARCHAR(255),
# MAGIC Liab_Insurer VARCHAR(255),
# MAGIC Liab_Ins_Amount VARCHAR(255),
# MAGIC Cargo_Ins_Amount VARCHAR(255),
# MAGIC DateOfExpiration_Cargo VARCHAR(255),
# MAGIC DateOfExpiration_Liab VARCHAR(255),
# MAGIC DOT_Number VARCHAR(255),
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Sourcesystem_Name VARCHAR(255),
# MAGIC Created_By VARCHAR(255),
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255),
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Silver_Carrier_Pkey PRIMARY KEY (DW_Carrier_ID)
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver_Load

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver.Silver_Load (
# MAGIC   DW_Load_ID BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   Load_ID VARCHAR(255),
# MAGIC   Hashkey VARCHAR(255),
# MAGIC   DW_Customer_ID BIGINT,
# MAGIC   DW_Carrier_ID BIGINT,
# MAGIC   DW_Mode_ID BIGINT,
# MAGIC   DW_Equipment_ID BIGINT,
# MAGIC   Load_Booked_At VARCHAR(255),
# MAGIC   Carrier_Rep_Name VARCHAR(255),
# MAGIC   Customer_Rep_Name VARCHAR(255),
# MAGIC   Driver_Phone_Number VARCHAR(255),
# MAGIC   DOT_Number VARCHAR(255),
# MAGIC   Load_Status VARCHAR(255),
# MAGIC   Origin_Shippper_Name VARCHAR(255),
# MAGIC   Origin_Shippper_City VARCHAR(255),
# MAGIC   Origin_Shippper_State VARCHAR(255),
# MAGIC   Origin_Shippper_Zip VARCHAR(255),
# MAGIC   Destination_Consignee_Name VARCHAR(255),
# MAGIC   Destination_Consignee_City VARCHAR(255),
# MAGIC   Destination_Consignee_State VARCHAR(255),
# MAGIC   Destination_Consignee_Zip VARCHAR(255),
# MAGIC   Ship_Date VARCHAR(255),
# MAGIC   Total_Weight VARCHAR(255),
# MAGIC   Total_Miles VARCHAR(255),
# MAGIC   Expense DECIMAL(10,2),
# MAGIC   Revenue DECIMAL(10,2),
# MAGIC   Revenue_Conversion DECIMAL(10,2),
# MAGIC   Expense_Conversion DECIMAL(10,2),
# MAGIC   Margin DECIMAL(10,2),
# MAGIC   Sourcesystem_ID VARCHAR(255),
# MAGIC   Sourcesystem_Name VARCHAR(255),
# MAGIC   Created_By VARCHAR(255) NOT NULL,
# MAGIC   Created_Date VARCHAR(255) NOT NULL,
# MAGIC   Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC   Last_Modified_Date VARCHAR(255) NOT NULL,
# MAGIC   CONSTRAINT `Silver_Load_Pkey` PRIMARY KEY (DW_Load_ID,Created_Date))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lookup_Equipment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Lookup_Equipment (
# MAGIC     DW_Equipment_ID  BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     Source_Equipment_Type VARCHAR(255),
# MAGIC     Derived_Equipment_Type VARCHAR(255),
# MAGIC     Created_By VARCHAR(255),
# MAGIC     Created_Date  Timestamp_ntz NOT NULL,
# MAGIC     Last_Modified_By   VARCHAR(255),
# MAGIC     Last_Modified_Date  Timestamp_ntz NOT NULL,
# MAGIC     CONSTRAINT Lookup_Equipment_Pkey PRIMARY KEY (DW_Equipment_ID)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lookup_Mode

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE silver.Lookup_Mode (
# MAGIC   DW_Mode_ID BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   Source_TransportMode VARCHAR(255),
# MAGIC   Derived_TransportMode VARCHAR(255),
# MAGIC   Created_By VARCHAR(255),
# MAGIC   Created_Date Timestamp_ntz NOT NULL,
# MAGIC   Last_Modified_By VARCHAR(255),
# MAGIC   Last_Modified_Date Timestamp_ntz NOT NULL,
# MAGIC   CONSTRAINT Lookup_Mode_Pkey PRIMARY KEY (DW_Mode_ID)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lookup_SourceSystem

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver.Lookup_Sourcesystem (
# MAGIC DW_Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Sourcesystem_Name VARCHAR(255),
# MAGIC Created_By VARCHAR(255),
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255),
# MAGIC Last_modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Lookup_SourceSystem_Pkey PRIMARY KEY(DW_Sourcesystem_ID)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Tender_Address

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Tender_Address( 
# MAGIC DW_Address_ID BIGINT GENERATED ALWAYS AS IDENTITY ,
# MAGIC Sourcesystem_ID	Integer,
# MAGIC Hashkey	varchar(255),
# MAGIC Tender_ID	varchar(255),
# MAGIC Load_Number	Integer,
# MAGIC Shipper_ID	varchar(255),
# MAGIC Shipper_Name	varchar(255),
# MAGIC Origin_Address_1	varchar(255),
# MAGIC Origin_Address_2	varchar(255),
# MAGIC Origin_State_Code	varchar(255),
# MAGIC Origin_city	varchar(255),
# MAGIC Origin_Country_Code	varchar(255),
# MAGIC Receiver_ID	varchar(255),
# MAGIC Receiver_Name	varchar(255),
# MAGIC Destination_Address_1	varchar(255),
# MAGIC Destination_Address_2	varchar(255),
# MAGIC Destination_City	varchar(255),
# MAGIC Destination_State_Code	varchar(255),
# MAGIC Destination_Country_Code	varchar(255),
# MAGIC Is_Cancelled	Boolean,
# MAGIC Sourcesystem_Name Varchar(255),
# MAGIC Created_By	varchar(255),
# MAGIC Created_Date	TIMESTAMP_NTZ,
# MAGIC Last_Modified_By	varchar(255),
# MAGIC Last_Modified_Date	TIMESTAMP_NTZ,
# MAGIC CONSTRAINT Silver_Tender_Address_Pkey PRIMARY KEY (DW_Address_ID )
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Stops

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Stops (
# MAGIC DW_Stop_ID BIGINT GENERATED ALWAYS AS IDENTITY ,
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Hashkey varchar(255),
# MAGIC Stop_ID  VARCHAR(255),
# MAGIC Load_Number INTEGER,
# MAGIC Truck_Number VARCHAR(255),
# MAGIC Stop_Type VARCHAR(255),
# MAGIC Stop_Location STRING,
# MAGIC Stop_City  VARCHAR(255),
# MAGIC Stop_State VARCHAR(255),
# MAGIC Stop_Name VARCHAR(255),
# MAGIC Arrival_Time TIMESTAMP_NTZ,
# MAGIC Depature_Time TIMESTAMP_NTZ,
# MAGIC Sourcesystem_Name VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255) NOT NULL,
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Silver_Stops_Pkey PRIMARY KEY (DW_Stop_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Shippers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Shippers (
# MAGIC DW_Shipper_ID BIGINT GENERATED ALWAYS AS IDENTITY ,
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Hashkey STRING ,
# MAGIC Mergekey varchar(255),
# MAGIC Schedule_ID  VARCHAR(255),
# MAGIC Shipper_ID VARCHAR(255),
# MAGIC Load_ID INTEGER,
# MAGIC Booking_ID INTEGER,
# MAGIC Truck_Load_Thing_ID VARCHAR(255),
# MAGIC Order_Number  STRING,
# MAGIC Shipper_Name   VARCHAR(255),
# MAGIC Shipper_Address   VARCHAR(255),
# MAGIC Shipper_City   VARCHAR(255),
# MAGIC Shipper_State_Code   VARCHAR(255),
# MAGIC Shipper_Zipcode   VARCHAR(255),
# MAGIC Shipper_Country_Code   VARCHAR(255),
# MAGIC Requested_Appoinment_Date TIMESTAMP_NTZ,
# MAGIC Confirmed_Appointment_DateTime TIMESTAMP_NTZ,
# MAGIC Scheduled_At TIMESTAMP_NTZ,
# MAGIC Is_Appointment_Scheduled BOOLEAN,
# MAGIC Pieces_To_Pickup_Count DOUBLE,
# MAGIC Weight_To_Pickup_Amount DOUBLE,
# MAGIC Weight_To_Pickup_Unit  VARCHAR(255),
# MAGIC Shipping_Units_To_Pickup_Type VARCHAR(255),
# MAGIC Shipping_Units_To_Pickup_Count INTEGER,
# MAGIC Volume_To_Pickup_Unit VARCHAR(255),
# MAGIC Volume_To_Pickup_Amount DOUBLE,
# MAGIC Shipper_Stop_1  VARCHAR(255),
# MAGIC Arrival_Time_1 TIMESTAMP_NTZ,
# MAGIC Depature_Time_1 TIMESTAMP_NTZ,
# MAGIC Shipper_Stop_2 VARCHAR(255),
# MAGIC Arrival_Time_2  TIMESTAMP_NTZ,
# MAGIC Depature_Time_2 TIMESTAMP_NTZ,
# MAGIC Shipper_TimeZone  VARCHAR(255),
# MAGIC UTC_Offset INTEGER,
# MAGIC Shipper_Phone_Number VARCHAR(255),
# MAGIC Shipper_Created_At TIMESTAMP_NTZ,
# MAGIC Sourcesystem_Name VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255) NOT NULL,
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Silver_Shippers_Pkey PRIMARY KEY (DW_Shipper_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Consignee

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Consignee (
# MAGIC DW_Consignee_ID BIGINT GENERATED ALWAYS AS IDENTITY ,
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Hashkey varchar(255),
# MAGIC Delivery_ID VARCHAR(255),
# MAGIC Receiver_ID VARCHAR(255),
# MAGIC Load_ID INTEGER,
# MAGIC Booking_ID INTEGER,
# MAGIC Order_Number STRING,
# MAGIC Consignee_Name VARCHAR(255),
# MAGIC Consignee_Address VARCHAR(255),
# MAGIC Consignee_City VARCHAR(255),
# MAGIC Consignee_State_Code VARCHAR(255),
# MAGIC Consignee_Zipcode VARCHAR(255),
# MAGIC Consignee_Country_Code VARCHAR(255),
# MAGIC Confirmed_Appointment_DateTime TIMESTAMP_NTZ ,
# MAGIC Scheduled_At  TIMESTAMP_NTZ ,
# MAGIC Is_Appointment_Scheduled BOOLEAN,
# MAGIC Pieces_To_Deliver_Count DOUBLE,
# MAGIC Weight_To_Deliver_Unit VARCHAR(255),
# MAGIC Weight_To_Deliver_Amount DOUBLE,
# MAGIC Shipping_Units_Type VARCHAR(255),
# MAGIC Shipping_Units_Count INTEGER,
# MAGIC Volume_To_Deliver_Unit VARCHAR(255),
# MAGIC Volume_To_Deliver_Amount DOUBLE,
# MAGIC Consignee_TimeZone VARCHAR(255),
# MAGIC UTC_Offset INTEGER,
# MAGIC Consignee_PhoneNumber VARCHAR(255),
# MAGIC Consignee_Created_At TIMESTAMP_NTZ ,
# MAGIC Sourcesystem_Name VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255) NOT NULL,
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Silver_Consignee_Pkey PRIMARY KEY (DW_Consignee_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Budget

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Budget(
# MAGIC DW_Budget_ID BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC DW_Office_ID BIGINT,
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Hashkey VARCHAR(255),
# MAGIC Office_Name VARCHAR(255),
# MAGIC Financial_Period  VARCHAR(255),
# MAGIC Volume_Budget DECIMAL(10,2),
# MAGIC Revenue_Budget DECIMAL(10,2),
# MAGIC Margin_Budget DECIMAL(10,2),
# MAGIC Margin_Percent_Budget DECIMAL(10,2),
# MAGIC Sourcesystem_Name VARCHAR(255) NOT NULL,
# MAGIC Created_By  VARCHAR(255) NOT NULL,
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By  VARCHAR(255) NOT NULL,
# MAGIC Last_Modified_Date  TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Silver_Budget_Pkey PRIMARY KEY (DW_Budget_ID)
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lookup_Equip_Mode_Types

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver.Lookup_Equip_Mode_Types  (
# MAGIC DW_Equip_Mode_ID BIGINT GENERATED  ALWAYS AS IDENTITY,
# MAGIC Equip_Mode_Type VARCHAR(255),
# MAGIC Created_By VARCHAR(255) NOT NULL,
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Lookup_Equip_Mode_Types_Pkey PRIMARY KEY (DW_Equip_Mode_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Tender_Events

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Tender_Events (
# MAGIC DW_TenderEvent_ID	BIGINT GENERATED ALWAYS AS IDENTITY ,
# MAGIC Sourcesystem_ID	INTEGER	,
# MAGIC Hashkey	Varchar(255)	,
# MAGIC Tender_ID	Varchar(255) ,
# MAGIC Event_Number	BIGINT	,
# MAGIC Customer	Varchar(255) ,
# MAGIC Shipment_ID	Varchar(255) ,
# MAGIC Tendered_At	TIMESTAMP_NTZ ,
# MAGIC Event_Type	Varchar(255) ,
# MAGIC Tenderer	Varchar(255) ,
# MAGIC Sourcesystem_Name  VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255) NOT NULL,
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Silver_Tender_Events_Pkey PRIMARY KEY (DW_TenderEvent_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Pricing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Pricing(
# MAGIC DW_Pricing_ID BIGINT GENERATED  ALWAYS AS IDENTITY,
# MAGIC DW_Load_ID BIGINT ,
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Load_Number INTEGER,
# MAGIC Hashkey VARCHAR(255),
# MAGIC Pricing_ID  VARCHAR(255),
# MAGIC Max_Buy  FLOAT,
# MAGIC Price_Set_At TIMESTAMP_NTZ,
# MAGIC Price_Set_By VARCHAR(255),
# MAGIC Currency  VARCHAR(255),
# MAGIC Notes STRING,
# MAGIC Sourcesystem_Name  VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255) NOT NULL,
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Silver_Pricing_Pkey PRIMARY KEY (DW_Pricing_ID )
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Tenders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Tenders(
# MAGIC DW_Tender_ID	BIGINT GENERATED  ALWAYS AS IDENTITY,
# MAGIC DW_Load_ID	BIGINT,
# MAGIC Sourcesystem_ID	Integer,
# MAGIC Hashkey	Varchar(255),
# MAGIC Tender_ID	Varchar(255),
# MAGIC Load_Number	Integer,
# MAGIC Is_Accepted	Boolean,
# MAGIC Accepted_At	TIMESTAMP_NTZ,
# MAGIC Accepted_By_ID	INTEGER,
# MAGIC Accepted_By_Name	Varchar(255),
# MAGIC Assigned_Carrier	Varchar(255),
# MAGIC Assigned_Carrier_SCAC	Varchar(255),
# MAGIC Command_Builder_URL	Varchar(255),
# MAGIC Shipment_ID	STRING,
# MAGIC Customer	Varchar(255),
# MAGIC Tendered_At	TIMESTAMP_NTZ,
# MAGIC Is_Split	Boolean,
# MAGIC Split_Orders	Varchar(255),
# MAGIC Remaining_Orders	Varchar(255),
# MAGIC NFI_Pro_Number	STRING,
# MAGIC Original_Shipment_ID	STRING,
# MAGIC Order_Numbers	STRING,
# MAGIC PO_Numbers	STRING,
# MAGIC Target_Shipment_ID	STRING,
# MAGIC Is_Cancelled	BOOLEAN,
# MAGIC Cancelled_By	INTEGER,
# MAGIC Cancelled_At	TIMESTAMP_NTZ,
# MAGIC Sourcesystem_Name varchar(255),
# MAGIC Created_By	Varchar(255),
# MAGIC Created_Date	TIMESTAMP_NTZ,
# MAGIC Last_Modified_By	Varchar(255),
# MAGIC Last_Modified_Date	TIMESTAMP_NTZ,
# MAGIC CONSTRAINT Silver_Tenders_Pkey PRIMARY KEY (DW_Tender_ID )
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Tender_Manual_Draft

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Tender_Manual_Draft( 
# MAGIC DW_ManualTender_ID BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC Sourcesystem_ID	Integer,
# MAGIC Hashkey	varchar(255),
# MAGIC Tender_ID	varchar(255),
# MAGIC Tender_Draft_ID	varchar(255),
# MAGIC Load_Number	Integer,
# MAGIC Tender_Drafted_by	Integer,
# MAGIC Tender_Started_At	TIMESTAMP_NTZ,
# MAGIC Tender_Completed_At	TIMESTAMP_NTZ,
# MAGIC Draft_TimeTaken_ms	Integer,
# MAGIC Is_Scratchpad_Notes_Used	Boolean,
# MAGIC TenderMade_Type	varchar(255),
# MAGIC Sourcesystem_Name varchar(255),
# MAGIC Created_By	varchar(255),
# MAGIC Created_Date	TIMESTAMP_NTZ,
# MAGIC Last_Modified_By	varchar(255),
# MAGIC Last_Modified_Date	TIMESTAMP_NTZ,
# MAGIC CONSTRAINT Silver_Tender_Manual_Draft PRIMARY KEY (DW_ManualTender_ID )
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Carrier_Onboarding_Status

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Silver.Silver_Carrier_Onboarding_Status (
# MAGIC DW_Onboarding_ID		BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC DW_Carrier_ID		BIGINT,
# MAGIC Sourcesystem_ID INTEGER,
# MAGIC Hashkey VARCHAR(255),
# MAGIC DOT_Number		Varchar(255),
# MAGIC Carrier_Name varchar(255),
# MAGIC Master_Carrier_ID VARCHAR(255),
# MAGIC Is_Approved		Boolean,
# MAGIC Approval_Description		String,
# MAGIC Carrier_Status		Varchar(255),
# MAGIC Is_Denied		Boolean,
# MAGIC Denial_Description		String,
# MAGIC Is_Duplicated		Boolean,
# MAGIC Nomination_Description		String,
# MAGIC Nomination_Count		Integer,
# MAGIC Is_Onboarded		Boolean,
# MAGIC Onboarded_Description		String,
# MAGIC Is_Onboard_Prohibited		Boolean,
# MAGIC Prohibition_Reason		String,
# MAGIC Sourcesystem_Name VARCHAR(255),
# MAGIC Created_By	varchar(255),
# MAGIC Created_Date	TIMESTAMP_NTZ,
# MAGIC Last_Modified_By	varchar(255),
# MAGIC Last_Modified_Date	TIMESTAMP_NTZ,
# MAGIC CONSTRAINT Silver_Carrier_Onboarding_Status_Pkey PRIMARY KEY (DW_Onboarding_ID )
# MAGIC );

# COMMAND ----------

# MAGIC %md ##Silver_Transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Transactions (
# MAGIC DW_Transaction_ID	BIGINT GENERATED ALWAYS AS IDENTITY ,
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Hashkey varchar(255),
# MAGIC DW_Load_ID	BIGINT	,
# MAGIC DW_Carrier_ID	BIGINT	,
# MAGIC DW_Customer_ID	BIGINT	,
# MAGIC Charge_ID	Varchar(255)	,
# MAGIC Booking_ID	Integer	,
# MAGIC Charge_Code	Varchar(255)	,
# MAGIC Charge_Incurred_At	TIMESTAMP_NTZ	,
# MAGIC Amount	Float	,
# MAGIC Currency	Varchar(255)	,
# MAGIC Billing_Party_Transaction_ID	Varchar(255)	,
# MAGIC Is_Finalized	Boolean	,
# MAGIC Finalized_At	TIMESTAMP_NTZ	,
# MAGIC Is_Voided	Boolean	,
# MAGIC Voided_At	TIMESTAMP_NTZ	,
# MAGIC Voided_By	Varchar(255)	,
# MAGIC Is_Invoiceable	Boolean	,
# MAGIC Invoiced_At	TIMESTAMP_NTZ	,
# MAGIC Invoiced_By	Integer	,
# MAGIC Sourcesystem_Name VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255),
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255),
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Silver_Transactions_Pkey PRIMARY KEY (DW_Transaction_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Invoice_And_Payemnts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  Silver.Silver_Invoice_And_Payments (
# MAGIC DW_Invoice_ID  BIGINT GENERATED ALWAYS AS IDENTITY ,
# MAGIC DW_Customer_ID BIGINT,
# MAGIC DW_Carrier_ID  BIGINT,
# MAGIC DW_Load_ID BIGINT,
# MAGIC MergeKey Varchar(255),
# MAGIC Sourcesystem_ID INTEGER,
# MAGIC Hashkey VARCHAR(255),
# MAGIC Invoice_Type varchar(255),
# MAGIC Invoice_Number  VARCHAR(255),
# MAGIC Load_Number INTEGER,
# MAGIC Invoice_Date  TIMESTAMP_NTZ,
# MAGIC Fuel_Surcharge_Amount FLOAT,
# MAGIC Linehaul_Amount  FLOAT,
# MAGIC Accessorial_Amount  FLOAT,
# MAGIC Total_Amount FLOAT,
# MAGIC Currency  VARCHAR(255),
# MAGIC Charge_ID STRING,
# MAGIC Due_Date Date,
# MAGIC Payment_Amount FLOAT,
# MAGIC Payment_Date Date,
# MAGIC Payment_Number  INTEGER,
# MAGIC Sourcesystem_Name  VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255) NOT NULL,
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT Silver_Invoice_And_Payments_Pkey PRIMARY KEY (DW_Invoice_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver_Offer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.Silver_Offer(
# MAGIC DW_Offer_ID BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC DW_Load_ID BIGINT ,
# MAGIC DW_Customer_ID  BIGINT ,
# MAGIC Sourcesystem_ID INTEGER NOT NULL,
# MAGIC Hashkey STRING,
# MAGIC Offer_ID VARCHAR(255),
# MAGIC Load_Number INTEGER,
# MAGIC Carrier_Name  VARCHAR(255),
# MAGIC Offer_Carrier_ID  INTEGER,
# MAGIC Offered_By_ID INTEGER,
# MAGIC Offered_By_Name  VARCHAR(255),
# MAGIC Offered_System VARCHAR(255),
# MAGIC Final_Rate_In_Pennies  INTEGER,
# MAGIC Currency  VARCHAR(255),
# MAGIC Carrier_Dot_Number  INTEGER,
# MAGIC Offer_Invalidated_By INTEGER,
# MAGIC Offer_Invalidated_At TIMESTAMP_NTZ,
# MAGIC Invalidated_Reason   VARCHAR(255),
# MAGIC Sourcesystem_Name  VARCHAR(255) NOT NULL,
# MAGIC Created_By VARCHAR(255) NOT NULL,
# MAGIC Created_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC Last_Modified_By VARCHAR(255) NOT NULL,
# MAGIC Last_Modified_Date TIMESTAMP_NTZ NOT NULL,
# MAGIC CONSTRAINT  Silver_Offer_Pkey PRIMARY KEY (DW_Offer_ID)
# MAGIC );  
# MAGIC

# COMMAND ----------

