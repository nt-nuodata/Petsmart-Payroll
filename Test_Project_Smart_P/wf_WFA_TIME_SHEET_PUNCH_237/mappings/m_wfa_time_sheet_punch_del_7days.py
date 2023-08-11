# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ./MappingUtility

# COMMAND ----------
mainWorkflowId = dbutils.widgets.get("mainWorkflowId")
mainWorkflowRunId = dbutils.widgets.get("mainWorkflowRunId")
parentName = dbutils.widgets.get("parentName")
preVariableAssignment = dbutils.widgets.get("preVariableAssignment")
postVariableAssignment = dbutils.widgets.get("postVariableAssignment")
truncTargetTableOptions = dbutils.widgets.get("truncTargetTableOptions")
variablesTableName = dbutils.widgets.get("variablesTableName")

# COMMAND ----------
#Truncate Target Tables
truncateTargetTables(truncTargetTableOptions)

# COMMAND ----------
#Pre presession variable updation
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_wfa_time_sheet_punch_del_7days")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_wfa_time_sheet_punch_del_7days", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  TIME_SHEET_ITEM_ID AS TIME_SHEET_ITEM_ID,
  STRT_DTM AS STRT_DTM,
  END_DTM AS END_DTM,
  STORE_NBR AS STORE_NBR,
  EMPLOYEE_ID AS EMPLOYEE_ID,
  WFA_BUSN_AREA_DESC AS WFA_BUSN_AREA_DESC,
  WFA_DEPT_DESC AS WFA_DEPT_DESC,
  WFA_TASK_DESC AS WFA_TASK_DESC,
  LOAD_DT AS LOAD_DT
FROM
  WFA_TIME_SHEET_PUNCH_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_1


query_1 = f"""SELECT
  DAY_DT AS DAY_DT,
  TIME_SHEET_ITEM_ID AS TIME_SHEET_ITEM_ID,
  STRT_DTM AS STRT_DTM,
  END_DTM AS END_DTM,
  STORE_NBR AS STORE_NBR,
  EMPLOYEE_ID AS EMPLOYEE_ID,
  WFA_BUSN_AREA_DESC AS WFA_BUSN_AREA_DESC,
  WFA_DEPT_DESC AS WFA_DEPT_DESC,
  WFA_TASK_DESC AS WFA_TASK_DESC,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_1")

# COMMAND ----------
# DBTITLE 1, AGG_DELETE_7_DAYS_2


query_2 = f"""SELECT
  DAY_DT AS DAY_DT,
  last(Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_1
GROUP BY
  DAY_DT"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("AGG_DELETE_7_DAYS_2")

# COMMAND ----------
# DBTITLE 1, UPD_STRATEGY_3


query_3 = f"""SELECT
  DAY_DT AS DAY_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  AGG_DELETE_7_DAYS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPD_STRATEGY_3")

# COMMAND ----------
# DBTITLE 1, WFA_TIME_SHEET_PUNCH


spark.sql("""MERGE INTO WFA_TIME_SHEET_PUNCH AS TARGET
USING
  UPD_STRATEGY_3 AS SOURCE ON TARGET.DAY_DT = SOURCE.DAY_DT
  WHEN MATCHED THEN DELETE""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_wfa_time_sheet_punch_del_7days")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_wfa_time_sheet_punch_del_7days", mainWorkflowId, parentName)
