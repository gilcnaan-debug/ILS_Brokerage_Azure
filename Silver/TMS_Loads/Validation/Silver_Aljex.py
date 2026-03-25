# Databricks notebook source
# MAGIC %md
# MAGIC Count

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ((
# MAGIC    SELECT DISTINCT *,
# MAGIC    CASE
# MAGIC       WHEN u.full_name IS NULL THEN p.key_c_user
# MAGIC       ELSE u.full_name
# MAGIC     END AS user1 FROM
# MAGIC         (SELECT *, projection_load_1.id AS load_num,
# MAGIC             projection_load_1.updated_at as LMD
# MAGIC             FROM BRONZE.projection_load_1 
# MAGIC             LEFT JOIN BRONZE.projection_load_2 
# MAGIC             ON projection_load_1.id = projection_load_2.id
# MAGIC             WHERE projection_load_1.Is_Deleted = 0 AND projection_load_2.Is_Deleted = 0
# MAGIC         ) AS p
# MAGIC
# MAGIC         LEFT JOIN bronze.aljex_user_report_listing AS u 
# MAGIC         ON p.key_c_user = u.aljex_id AND u.aljex_id <> 'repeat' 
# MAGIC         WHERE (p.status NOT LIKE '%VOID%')
# MAGIC ) AS p) WHERE user1 IS NOT NULL AND p.status::string <> 'HOLD' :: string

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Silver.silver_aljex WHERE Is_Deleted = 0

# COMMAND ----------

# MAGIC %md
# MAGIC Duplicates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Load_Number FROM Silver.silver_aljex group by 1 having count(Load_Number) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC Sample Data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Silver.silver_aljex WHERE Load_Number in 
# MAGIC (1113363,
# MAGIC 1123342,
# MAGIC 1124006,
# MAGIC 1128707,
# MAGIC 1129449) 

# COMMAND ----------

# MAGIC %md
# MAGIC Is_Deleted annd Update

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Silver.silver_aljex where Is_Deleted = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Silver.silver_aljex where Created_Date <> Last_Modified_Date