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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_wfa_time_sheet_punch")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_wfa_time_sheet_punch", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, Shortcut_to_SITE_PROFILE_RPT_2


query_2 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  LOCATION_TYPE_DESC AS LOCATION_TYPE_DESC,
  STORE_NBR AS STORE_NBR,
  STORE_NAME AS STORE_NAME,
  STORE_TYPE_ID AS STORE_TYPE_ID,
  STORE_TYPE_DESC AS STORE_TYPE_DESC,
  PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
  LOCATION_NBR AS LOCATION_NBR,
  COMPANY_ID AS COMPANY_ID,
  COMPANY_DESC AS COMPANY_DESC,
  SUPER_REGION_ID AS SUPER_REGION_ID,
  SUPER_REGION_DESC AS SUPER_REGION_DESC,
  REGION_ID AS REGION_ID,
  REGION_DESC AS REGION_DESC,
  DISTRICT_ID AS DISTRICT_ID,
  DISTRICT_DESC AS DISTRICT_DESC,
  SITE_ADDRESS AS SITE_ADDRESS,
  SITE_CITY AS SITE_CITY,
  SITE_COUNTY AS SITE_COUNTY,
  STATE_CD AS STATE_CD,
  STATE_NAME AS STATE_NAME,
  POSTAL_CD AS POSTAL_CD,
  COUNTRY_CD AS COUNTRY_CD,
  COUNTRY_NAME AS COUNTRY_NAME,
  GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
  GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
  SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
  SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
  SITE_FAX_NO AS SITE_FAX_NO,
  SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
  STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
  SFT_OPEN_DT AS SFT_OPEN_DT,
  OPEN_DT AS OPEN_DT,
  GR_OPEN_DT AS GR_OPEN_DT,
  CLOSE_DT AS CLOSE_DT,
  SITE_SALES_FLAG AS SITE_SALES_FLAG,
  SALES_CURR_FLAG AS SALES_CURR_FLAG,
  SITE_OPEN_YRS_AMT AS SITE_OPEN_YRS_AMT,
  FIRST_SALE_DT AS FIRST_SALE_DT,
  FIRST_MEASURED_SALE_DT AS FIRST_MEASURED_SALE_DT,
  LAST_SALE_DT AS LAST_SALE_DT,
  COMP_CURR_FLAG AS COMP_CURR_FLAG,
  COMP_EFF_DT AS COMP_EFF_DT,
  COMP_END_DT AS COMP_END_DT,
  TP_LOC_FLAG AS TP_LOC_FLAG,
  TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
  TP_START_DT AS TP_START_DT,
  HOTEL_FLAG AS HOTEL_FLAG,
  HOTEL_OPEN_DT AS HOTEL_OPEN_DT,
  DAYCAMP_FLAG AS DAYCAMP_FLAG,
  VET_FLAG AS VET_FLAG,
  TIME_ZONE_ID AS TIME_ZONE_ID,
  TIME_ZONE AS TIME_ZONE,
  SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  TRADE_AREA AS TRADE_AREA,
  DELV_SERVICE_CLASS_ID AS DELV_SERVICE_CLASS_ID,
  PICK_SERVICE_CLASS_ID AS PICK_SERVICE_CLASS_ID,
  REPL_DC_NBR AS REPL_DC_NBR,
  REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
  REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
  PROMO_LABEL_CD AS PROMO_LABEL_CD,
  PRICE_ZONE_ID AS PRICE_ZONE_ID,
  PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
  PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
  PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
  EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
  EQUINE_MERCH_DESC AS EQUINE_MERCH_DESC,
  EQUINE_SITE_ID AS EQUINE_SITE_ID,
  EQUINE_SITE_DESC AS EQUINE_SITE_DESC,
  EQUINE_SITE_OPEN_DT AS EQUINE_SITE_OPEN_DT,
  LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
  LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
  LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
  LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
  LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
  BP_COMPANY_NBR AS BP_COMPANY_NBR,
  BP_GL_ACCT AS BP_GL_ACCT,
  SITE_LOGIN_ID AS SITE_LOGIN_ID,
  SITE_MANAGER_ID AS SITE_MANAGER_ID,
  SITE_MANAGER_NAME AS SITE_MANAGER_NAME,
  MGR_ID AS MGR_ID,
  MGR_DESC AS MGR_DESC,
  DVL_ID AS DVL_ID,
  DVL_DESC AS DVL_DESC,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
  DIST_MGR_NAME AS DIST_MGR_NAME,
  DM_EMAIL_ADDRESS AS DM_EMAIL_ADDRESS,
  DC_AREA_DIRECTOR_NAME AS DC_AREA_DIRECTOR_NAME,
  DC_AREA_DIRECTOR_EMAIL AS DC_AREA_DIRECTOR_EMAIL,
  DIST_SVC_MGR_NAME AS DIST_SVC_MGR_NAME,
  DSM_EMAIL_ADDRESS AS DSM_EMAIL_ADDRESS,
  REGION_VP_NAME AS REGION_VP_NAME,
  RVP_EMAIL_ADDRESS AS RVP_EMAIL_ADDRESS,
  REGION_TRAINER_NAME AS REGION_TRAINER_NAME,
  ASSET_PROTECT_NAME AS ASSET_PROTECT_NAME,
  ASSET_PROTECT_EMAIL AS ASSET_PROTECT_EMAIL,
  LP_SAFETY_DIRECTOR_NAME AS LP_SAFETY_DIRECTOR_NAME,
  LP_SAFETY_DIRECTOR_EMAIL AS LP_SAFETY_DIRECTOR_EMAIL,
  SR_LP_SAFETY_MGR_NAME AS SR_LP_SAFETY_MGR_NAME,
  SR_LP_SAFETY_MGR_EMAIL AS SR_LP_SAFETY_MGR_EMAIL,
  REGIONAL_LP_SAFETY_MGR_NAME AS REGIONAL_LP_SAFETY_MGR_NAME,
  REGIONAL_LP_SAFETY_MGR_EMAIL AS REGIONAL_LP_SAFETY_MGR_EMAIL,
  RETAIL_MANAGER_SAFETY_NAME AS RETAIL_MANAGER_SAFETY_NAME,
  RETAIL_MANAGER_SAFETY_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
  DC_GENERAL_MANAGER_NAME AS DC_GENERAL_MANAGER_NAME,
  DC_GENERAL_MANAGER_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
  ASST_DC_GENERAL_MANAGER_NAME1 AS ASST_DC_GENERAL_MANAGER_NAME1,
  ASST_DC_GENERAL_MANAGER_EMAIL1 AS ASST_DC_GENERAL_MANAGER_EMAIL1,
  ASST_DC_GENERAL_MANAGER_NAME2 AS ASST_DC_GENERAL_MANAGER_NAME2,
  ASST_DC_GENERAL_MANAGER_EMAIL2 AS ASST_DC_GENERAL_MANAGER_EMAIL2,
  HR_MANAGER_NAME AS HR_MANAGER_NAME,
  HR_MANAGER_EMAIL AS HR_MANAGER_EMAIL,
  HR_SUPERVISOR_NAME1 AS HR_SUPERVISOR_NAME1,
  HR_SUPERVISOR_EMAIL1 AS HR_SUPERVISOR_EMAIL1,
  HR_SUPERVISOR_NAME2 AS HR_SUPERVISOR_NAME2,
  HR_SUPERVISOR_EMAIL2 AS HR_SUPERVISOR_EMAIL2,
  LEARN_SOLUTION_MGR_NAME AS LEARN_SOLUTION_MGR_NAME,
  LEARN_SOLUTION_MGR_EMAIL AS LEARN_SOLUTION_MGR_EMAIL,
  ADD_DT AS ADD_DT,
  DELETE_DT AS DELETE_DT,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  SITE_PROFILE_RPT"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_SITE_PROFILE_RPT_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_PROFILE_RPT_3


query_3 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_PROFILE_RPT_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_SITE_PROFILE_RPT_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_WFA_TASK_4


query_4 = f"""SELECT
  WFA_TASK_ID AS WFA_TASK_ID,
  WFA_TASK_DESC AS WFA_TASK_DESC,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  WFA_TASK"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_WFA_TASK_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WFA_TASK_5


query_5 = f"""SELECT
  WFA_TASK_ID AS WFA_TASK_ID,
  WFA_TASK_DESC AS WFA_TASK_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WFA_TASK_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("SQ_Shortcut_to_WFA_TASK_5")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_WFA_DEPARTMENT_6


query_6 = f"""SELECT
  WFA_DEPT_ID AS WFA_DEPT_ID,
  WFA_DEPT_DESC AS WFA_DEPT_DESC,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  WFA_DEPARTMENT"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Shortcut_to_WFA_DEPARTMENT_6")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WFA_DEPARTMENT_7


query_7 = f"""SELECT
  WFA_DEPT_ID AS WFA_DEPT_ID,
  WFA_DEPT_DESC AS WFA_DEPT_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WFA_DEPARTMENT_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("SQ_Shortcut_to_WFA_DEPARTMENT_7")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_WFA_BUSINESS_AREA_8


query_8 = f"""SELECT
  WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  WFA_BUSN_AREA_DESC AS WFA_BUSN_AREA_DESC,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  WFA_BUSINESS_AREA"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("Shortcut_to_WFA_BUSINESS_AREA_8")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WFA_BUSINESS_AREA_9


query_9 = f"""SELECT
  WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  WFA_BUSN_AREA_DESC AS WFA_BUSN_AREA_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WFA_BUSINESS_AREA_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("SQ_Shortcut_to_WFA_BUSINESS_AREA_9")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DAYS_10


query_10 = f"""SELECT
  DAY_DT AS DAY_DT,
  BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
  HOLIDAY_FLAG AS HOLIDAY_FLAG,
  DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
  DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
  CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
  CAL_WK AS CAL_WK,
  CAL_WK_NBR AS CAL_WK_NBR,
  CAL_MO AS CAL_MO,
  CAL_MO_NBR AS CAL_MO_NBR,
  CAL_MO_NAME AS CAL_MO_NAME,
  CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
  CAL_QTR AS CAL_QTR,
  CAL_QTR_NBR AS CAL_QTR_NBR,
  CAL_HALF AS CAL_HALF,
  CAL_YR AS CAL_YR,
  FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
  FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_WK_NBR AS FISCAL_WK_NBR,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_MO_NBR AS FISCAL_MO_NBR,
  FISCAL_MO_NAME AS FISCAL_MO_NAME,
  FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
  FISCAL_QTR AS FISCAL_QTR,
  FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
  FISCAL_HALF AS FISCAL_HALF,
  FISCAL_YR AS FISCAL_YR,
  LYR_WEEK_DT AS LYR_WEEK_DT,
  LWK_WEEK_DT AS LWK_WEEK_DT,
  WEEK_DT AS WEEK_DT,
  EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
  EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
  ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
  ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
  CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
  CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
  CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
  CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
  MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
  MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
  MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
  MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
  PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
  PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS
FROM
  DAYS"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("Shortcut_to_DAYS_10")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DAYS_11


query_11 = f"""SELECT
  DAY_DT AS DAY_DT,
  WEEK_DT AS WEEK_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_DAYS_10"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("SQ_Shortcut_to_DAYS_11")

# COMMAND ----------
# DBTITLE 1, JNR_DAYS_12


query_12 = f"""SELECT
  MASTER.EMPLOYEE_ID AS PERSONNUM,
  MASTER.DAY_DT AS DAY_DT,
  MASTER.STRT_DTM AS START_DTM,
  MASTER.END_DTM AS END_DTM,
  MASTER.TIME_SHEET_ITEM_ID AS TIMESHEETITEMID,
  MASTER.STORE_NBR AS STORE_NUMBER,
  MASTER.WFA_BUSN_AREA_DESC AS BUSN_AREA_DESC,
  MASTER.WFA_DEPT_DESC AS DEPT_DESC,
  MASTER.WFA_TASK_DESC AS TASK_DESC,
  DETAIL.DAY_DT AS i_DAY_DT,
  DETAIL.WEEK_DT AS WEEK_DT,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_1 MASTER
  LEFT JOIN SQ_Shortcut_to_DAYS_11 DETAIL ON MASTER.DAY_DT = DETAIL.DAY_DT"""

df_12 = spark.sql(query_12)

df_12.createOrReplaceTempView("JNR_DAYS_12")

# COMMAND ----------
# DBTITLE 1, JNR_WFA_BUSINESS_AREA_13


query_13 = f"""SELECT
  MASTER.PERSONNUM AS PERSONNUM,
  MASTER.DAY_DT AS DAY_DT,
  MASTER.START_DTM AS START_DTM,
  MASTER.END_DTM AS END_DTM,
  MASTER.TIMESHEETITEMID AS TIMESHEETITEMID,
  MASTER.STORE_NUMBER AS STORE_NUMBER,
  MASTER.BUSN_AREA_DESC AS BUSN_AREA_DESC,
  MASTER.DEPT_DESC AS DEPT_DESC,
  MASTER.TASK_DESC AS TASK_DESC,
  MASTER.WEEK_DT AS WEEK_DT,
  DETAIL.WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  DETAIL.WFA_BUSN_AREA_DESC AS WFA_BUSN_AREA_DESC,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_DAYS_12 MASTER
  LEFT JOIN SQ_Shortcut_to_WFA_BUSINESS_AREA_9 DETAIL ON MASTER.BUSN_AREA_DESC = DETAIL.WFA_BUSN_AREA_DESC"""

df_13 = spark.sql(query_13)

df_13.createOrReplaceTempView("JNR_WFA_BUSINESS_AREA_13")

# COMMAND ----------
# DBTITLE 1, JNR_WFA_DEPARTMENT_14


query_14 = f"""SELECT
  MASTER.PERSONNUM AS PERSONNUM,
  MASTER.DAY_DT AS DAY_DT,
  MASTER.START_DTM AS START_DTM,
  MASTER.END_DTM AS END_DTM,
  MASTER.TIMESHEETITEMID AS TIMESHEETITEMID,
  MASTER.STORE_NUMBER AS STORE_NUMBER,
  MASTER.BUSN_AREA_DESC AS BUSN_AREA_DESC,
  MASTER.DEPT_DESC AS DEPT_DESC,
  MASTER.TASK_DESC AS TASK_DESC,
  MASTER.WEEK_DT AS WEEK_DT,
  MASTER.WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  DETAIL.WFA_DEPT_ID AS WFA_DEPT_ID,
  DETAIL.WFA_DEPT_DESC AS WFA_DEPT_DESC,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_WFA_BUSINESS_AREA_13 MASTER
  LEFT JOIN SQ_Shortcut_to_WFA_DEPARTMENT_7 DETAIL ON MASTER.DEPT_DESC = DETAIL.WFA_DEPT_DESC"""

df_14 = spark.sql(query_14)

df_14.createOrReplaceTempView("JNR_WFA_DEPARTMENT_14")

# COMMAND ----------
# DBTITLE 1, JNR_WFA_TASK_15


query_15 = f"""SELECT
  MASTER.PERSONNUM AS PERSONNUM,
  MASTER.DAY_DT AS DAY_DT,
  MASTER.START_DTM AS START_DTM,
  MASTER.END_DTM AS END_DTM,
  MASTER.TIMESHEETITEMID AS TIMESHEETITEMID,
  MASTER.STORE_NUMBER AS STORE_NUMBER,
  MASTER.BUSN_AREA_DESC AS BUSN_AREA_DESC,
  MASTER.DEPT_DESC AS DEPT_DESC,
  MASTER.TASK_DESC AS TASK_DESC,
  MASTER.WEEK_DT AS WEEK_DT,
  MASTER.WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  MASTER.WFA_DEPT_ID AS WFA_DEPT_ID,
  DETAIL.WFA_TASK_ID AS WFA_TASK_ID,
  DETAIL.WFA_TASK_DESC AS WFA_TASK_DESC,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_WFA_DEPARTMENT_14 MASTER
  LEFT JOIN SQ_Shortcut_to_WFA_TASK_5 DETAIL ON MASTER.TASK_DESC = DETAIL.WFA_TASK_DESC"""

df_15 = spark.sql(query_15)

df_15.createOrReplaceTempView("JNR_WFA_TASK_15")

# COMMAND ----------
# DBTITLE 1, JNR_SITE_PROFILE_RPT_16


query_16 = f"""SELECT
  MASTER.DAY_DT AS DAY_DT,
  MASTER.WEEK_DT AS WEEK_DT,
  MASTER.TIMESHEETITEMID AS TIMESHEETITEMID,
  MASTER.START_DTM AS START_DTM,
  MASTER.END_DTM AS END_DTM,
  MASTER.STORE_NUMBER AS STORE_NUMBER,
  MASTER.PERSONNUM AS PERSONNUM,
  MASTER.WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  MASTER.BUSN_AREA_DESC AS BUSN_AREA_DESC,
  MASTER.WFA_DEPT_ID AS WFA_DEPT_ID,
  MASTER.DEPT_DESC AS DEPT_DESC,
  MASTER.WFA_TASK_ID AS WFA_TASK_ID,
  MASTER.TASK_DESC AS TASK_DESC,
  DETAIL.LOCATION_ID AS LOCATION_ID,
  DETAIL.STORE_NBR AS STORE_NBR,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_WFA_TASK_15 MASTER
  LEFT JOIN SQ_Shortcut_to_SITE_PROFILE_RPT_3 DETAIL ON MASTER.STORE_NUMBER = DETAIL.STORE_NBR"""

df_16 = spark.sql(query_16)

df_16.createOrReplaceTempView("JNR_SITE_PROFILE_RPT_16")

# COMMAND ----------
# DBTITLE 1, EXP_WFA_TIME_SHEET_PUNCH_17


query_17 = f"""SELECT
  DAY_DT AS DAY_DT,
  WEEK_DT AS WEEK_DT,
  TIMESHEETITEMID AS TIMESHEETITEMID,
  START_DTM AS START_DTM,
  END_DTM AS END_DTM,
  LOCATION_ID AS LOCATION_ID,
  PERSONNUM AS PERSONNUM,
  WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  BUSN_AREA_DESC AS BUSN_AREA_DESC,
  WFA_DEPT_ID AS WFA_DEPT_ID,
  DEPT_DESC AS DEPT_DESC,
  WFA_TASK_ID AS WFA_TASK_ID,
  TASK_DESC AS TASK_DESC,
  now() AS UPDATE_DT,
  now() AS LOAD_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_SITE_PROFILE_RPT_16"""

df_17 = spark.sql(query_17)

df_17.createOrReplaceTempView("EXP_WFA_TIME_SHEET_PUNCH_17")

# COMMAND ----------
# DBTITLE 1, WFA_TIME_SHEET_PUNCH


spark.sql("""INSERT INTO
  WFA_TIME_SHEET_PUNCH
SELECT
  DAY_DT AS DAY_DT,
  WEEK_DT AS WEEK_DT,
  TIMESHEETITEMID AS TIME_SHEET_ITEM_ID,
  START_DTM AS STRT_DTM,
  END_DTM AS END_DTM,
  LOCATION_ID AS LOCATION_ID,
  PERSONNUM AS EMPLOYEE_ID,
  WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  BUSN_AREA_DESC AS WFA_BUSN_AREA_DESC,
  WFA_DEPT_ID AS WFA_DEPT_ID,
  DEPT_DESC AS WFA_DEPT_DESC,
  WFA_TASK_ID AS WFA_TASK_ID,
  TASK_DESC AS WFA_TASK_DESC,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  EXP_WFA_TIME_SHEET_PUNCH_17""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_wfa_time_sheet_punch")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_wfa_time_sheet_punch", mainWorkflowId, parentName)
