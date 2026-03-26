# Databricks notebook source
# MAGIC %md
# MAGIC ## Nudge_Potential_Aging

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE nudge_ai.Nudge_Potential_Awards (
# MAGIC Customer_Name	Varchar(255),
# MAGIC Load_Lane	Varchar(255),
# MAGIC Financial_Period	SMALLINT,
# MAGIC Financial_Year	SMALLINT,
# MAGIC Period_Year	Varchar(25),
# MAGIC Load_Count	INT,
# MAGIC Age	INT,
# MAGIC Total_Revenue	decimal(10,2),
# MAGIC Total_Margin	decimal(10,2),
# MAGIC Aging_Bucket	Varchar(255),
# MAGIC Aging_key	Varchar(255)
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into nudge_ai.Nudge_Potential_Awards (select Customer_Name,	Load_Lane,	Financial_Period,	Financial_Year,	Period_Year,Load_Count,	Age, Total_Revenue, Total_Margin, AgingBucket, Agingkey from Nudge_AI.vw_awards_potential_aging)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Nudge_Summary_Awards

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR Replace TABLE nudge_ai.Nudge_Summary_Awards (
# MAGIC   Shipper varchar(255) ,
# MAGIC   LaneCriteria varchar(255),
# MAGIC   RFP varchar(255),
# MAGIC   EstimatedVolume decimal(10,2),
# MAGIC   ActualVolume int,
# MAGIC   PeriodYear varchar(255),
# MAGIC   AwardStartDate date,
# MAGIC   AwardEndDate date,
# MAGIC   AwardInterval	Varchar(50),
# MAGIC   PerformanceStatus varchar(50)
# MAGIC );
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into nudge_ai.Nudge_Summary_Awards (select * from Nudge_AI.vw_Nudge_Summary_Awards)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Nudge_Status_Awards

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nudge_ai.Nudge_Status_Awards (
# MAGIC   RFPName varchar(255) ,
# MAGIC   Shipper varchar(255),
# MAGIC   Currentdate date,
# MAGIC   CompletionPercentage Decimal(10,2),
# MAGIC   EstimatedVolume Decimal(10,2),
# MAGIC   ActualVolume INT,
# MAGIC   AchievementPercentage DECIMAL(10,2)
# MAGIC );
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into nudge_ai.Nudge_Status_Awards (select * from Nudge_AI.vw_Nudge_Status_Awards);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Nudge_FactLoad_Awards

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nudge_ai.Nudge_FactLoad_Awards (
# MAGIC Shipper	Varchar(255)	,
# MAGIC LoadID	Int	,
# MAGIC RFP	Varchar(255)	,
# MAGIC AwardID	Int	,
# MAGIC ValidationCriteria	Varchar(255)	,
# MAGIC Lane	Varchar(255)	,
# MAGIC OriginState	Varchar(255)	,
# MAGIC OriginCity	Varchar(255)	,
# MAGIC OriginZip	VARCHAR(50)	,
# MAGIC OriginCountry	Varchar(20),
# MAGIC DestinationState	Varchar(255)	,
# MAGIC DestinationCity	Varchar(255)	,
# MAGIC DestinationZip	VARCHAR(50)	,
# MAGIC DestinationCountry	Varchar(20),
# MAGIC TMS	Varchar(255)	,
# MAGIC CarrierRep	Varchar(255)	,
# MAGIC ShipperName	Varchar(255)	,
# MAGIC ShipDate	date	,
# MAGIC Office	Varchar(255)	,
# MAGIC LoadStatus	Varchar(255)	,
# MAGIC CarrierOffice	Varchar(255)	,
# MAGIC LoadLane	Varchar(255)	,
# MAGIC Equipment	Varchar(255)	,
# MAGIC Mode	Varchar(255)	,
# MAGIC DotNumber	varchar(255)	,
# MAGIC CarrierName	Varchar(255)	,
# MAGIC TotalExpense	Varchar(255)	,
# MAGIC TotalMargin	Varchar(255)	,
# MAGIC MarketOrigin	Varchar(255)	,
# MAGIC MarketDestination	Varchar(255)	,
# MAGIC CustomerRep	Varchar(255)	,
# MAGIC DeliveryDate	date	,
# MAGIC InvoiceDate	date	,
# MAGIC BookingType	Varchar(255)	,
# MAGIC LoadFlag	Varchar(255)	,
# MAGIC TotalMiles	Int	,
# MAGIC TotalRevenue	decimal(10,2)	,
# MAGIC PeriodNumber	Int	,
# MAGIC FinancialYear	Int	,
# MAGIC PeriodYear	Varchar(255)	,
# MAGIC AwardType	Varchar(255)	,
# MAGIC EstimatedVolume	decimal(10,2)	,
# MAGIC EstimatedRevenue	decimal(10,2)	,
# MAGIC AwardStartdate	date	,
# MAGIC AwardEnddate	date	,
# MAGIC AwardInterval	Varchar(50)	,
# MAGIC Category	Varchar(255)	,
# MAGIC LaneCriteria	Varchar(255)	,
# MAGIC Awardkey	Varchar(255)	,
# MAGIC Agingkey	Varchar(255)	,
# MAGIC PerformanceStatus	Varchar(255)	
# MAGIC  ) ;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into nudge_ai.Nudge_FactLoad_Awards (select * from Nudge_AI.vw_Nudge_FactLoad_Awards)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Nudge_Awards_Lane

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nudge_ai.Nudge_Awards_Lane	 (
# MAGIC Customer_Master	VARCHAR(255),
# MAGIC RFP_Name	Varchar(255),
# MAGIC Period_Year	Varchar(10),
# MAGIC Estimated_Lanes_Volume	decimal(10,2),
# MAGIC Actual_Lanes_Volume	INT,
# MAGIC Is_Awarded	Varchar(50)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into nudge_ai.nudge_awards_lane(SELECT DISTINCT * FROM Nudge_AI.vw_nudge_awards_lane)

# COMMAND ----------

