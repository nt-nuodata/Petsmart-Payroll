CREATE DATABASE IF NOT EXISTS DELTA_TRAINING;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.EMPLOYEE_PROFILE_DAY(DAY_DT TIMESTAMP,
EMPLOYEE_ID INT,
EMPL_FIRST_NAME STRING,
EMPL_MIDDLE_NAME STRING,
EMPL_LAST_NAME STRING,
EMPL_BIRTH_DT TIMESTAMP,
GENDER_CD STRING,
PS_MARITAL_STATUS_CD STRING,
ETHNIC_GROUP_ID STRING,
EMPL_ADDR_1 STRING,
EMPL_ADDR_2 STRING,
EMPL_CITY STRING,
EMPL_STATE STRING,
EMPL_PROVINCE STRING,
EMPL_ZIPCODE STRING,
COUNTRY_CD STRING,
EMPL_HOME_PHONE STRING,
EMPL_EMAIL_ADDR STRING,
EMPL_LOGIN_ID STRING,
BADGE_NBR STRING,
EMPL_STATUS_CD STRING,
STATUS_CHG_DT TIMESTAMP,
FULLPT_FLAG STRING,
FULLPT_CHG_DT TIMESTAMP,
EMPL_TYPE_CD STRING,
PS_REG_TEMP_CD STRING,
EMPL_CATEGORY_CD STRING,
EMPL_GROUP_CD STRING,
EMPL_SUBGROUP_CD STRING,
EMPL_HIRE_DT TIMESTAMP,
EMPL_REHIRE_DT TIMESTAMP,
EMPL_TERM_DT TIMESTAMP,
TERM_REASON_CD STRING,
EMPL_SENORITY_DT TIMESTAMP,
PS_ACTION_DT TIMESTAMP,
PS_ACTION_CD STRING,
PS_ACTION_REASON_CD STRING,
LOCATION_ID INT,
LOCATION_CHG_DT TIMESTAMP,
STORE_NBR INT,
STORE_DEPT_NBR STRING,
COMPANY_ID INT,
PS_PERSONNEL_AREA_ID STRING,
PS_PERSONNEL_SUBAREA_ID STRING,
PS_DEPT_CD STRING,
PS_DEPT_CHG_DT TIMESTAMP,
PS_POSITION_ID INT,
POSITION_CHG_DT TIMESTAMP,
PS_SUPERVISOR_ID INT,
JOB_CODE INT,
JOB_CODE_CHG_DT TIMESTAMP,
EMPL_JOB_ENTRY_DT TIMESTAMP,
PS_GRADE_ID INT,
EMPL_STD_BONUS_PCT INT,
EMPL_OVR_BONUS_PCT INT,
EMPL_RATING INT,
PAY_RATE_CHG_DT TIMESTAMP,
PS_PAYROLL_AREA_CD STRING,
PS_TAX_COMPANY_CD STRING,
PS_COMP_FREQ_CD STRING,
COMP_RATE_AMT INT,
ANNUAL_RATE_LOC_AMT INT,
HOURLY_RATE_LOC_AMT INT,
CURRENCY_ID STRING,
EXCH_RATE_PCT INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.EMPL_EMPL_LOC_WK_PSOFT(WEEK_DT TIMESTAMP,
LOCATION_ID INT,
EMPLOYEE_ID INT,
EARN_ID STRING,
STORE_DEPT_NBR STRING,
JOB_CODE INT,
FULLPT_FLAG STRING,
HOURS_WORKED INT,
EARNINGS_AMT INT,
EARNINGS_LOC_AMT INT,
PAY_FREQ_CD STRING,
CURRENCY_NBR INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.EARNINGS_ID(EARN_ID STRING,
PS_TAX_COMPANY_CD STRING,
PS_WAGE_TYPE_GID INT,
PS_WAGE_TYPE_CD STRING,
PS_COUNTRY_GROUP_CD STRING,
PS_WAGE_TYPE_DESC STRING) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.EMPL_EMPL_LOC_WK(WEEK_DT TIMESTAMP,
LOCATION_ID INT,
EMPLOYEE_ID INT,
EARN_ID STRING,
STORE_DEPT_NBR STRING,
JOB_CODE INT,
FULLPT_FLAG STRING,
HOURS_WORKED INT,
EARNINGS_AMT INT,
EARNINGS_LOC_AMT INT,
PAY_FREQ_CD STRING,
CURRENCY_NBR INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.EMPLOYEE_PROFILE_DAY_PSOFT(DAY_DT TIMESTAMP,
EMPLOYEE_ID INT,
EMPL_FIRST_NAME STRING,
EMPL_MIDDLE_NAME STRING,
EMPL_LAST_NAME STRING,
EMPL_BIRTH_DT TIMESTAMP,
GENDER_CD STRING,
PS_MARITAL_STATUS_CD STRING,
ETHNIC_GROUP_ID STRING,
EMPL_ADDR_1 STRING,
EMPL_ADDR_2 STRING,
EMPL_CITY STRING,
EMPL_STATE STRING,
EMPL_PROVINCE STRING,
EMPL_ZIPCODE STRING,
COUNTRY_CD STRING,
EMPL_HOME_PHONE STRING,
EMPL_EMAIL_ADDR STRING,
EMPL_LOGIN_ID STRING,
BADGE_NBR STRING,
EMPL_STATUS_CD STRING,
STATUS_CHG_DT TIMESTAMP,
FULLPT_FLAG STRING,
FULLPT_CHG_DT TIMESTAMP,
EMPL_TYPE_CD STRING,
PS_REG_TEMP_CD STRING,
EMPL_CATEGORY_CD STRING,
EMPL_GROUP_CD STRING,
EMPL_SUBGROUP_CD STRING,
EMPL_HIRE_DT TIMESTAMP,
EMPL_REHIRE_DT TIMESTAMP,
EMPL_TERM_DT TIMESTAMP,
TERM_REASON_CD STRING,
EMPL_SENORITY_DT TIMESTAMP,
PS_ACTION_DT TIMESTAMP,
PS_ACTION_CD STRING,
PS_ACTION_REASON_CD STRING,
LOCATION_ID INT,
LOCATION_CHG_DT TIMESTAMP,
STORE_NBR INT,
STORE_DEPT_NBR STRING,
COMPANY_ID INT,
PS_PERSONNEL_AREA_ID STRING,
PS_PERSONNEL_SUBAREA_ID STRING,
PS_DEPT_CD STRING,
PS_DEPT_CHG_DT TIMESTAMP,
PS_POSITION_ID INT,
POSITION_CHG_DT TIMESTAMP,
PS_SUPERVISOR_ID INT,
JOB_CODE INT,
JOB_CODE_CHG_DT TIMESTAMP,
EMPL_JOB_ENTRY_DT TIMESTAMP,
PS_GRADE_ID INT,
EMPL_STD_BONUS_PCT INT,
EMPL_OVR_BONUS_PCT INT,
EMPL_RATING INT,
PAY_RATE_CHG_DT TIMESTAMP,
PS_PAYROLL_AREA_CD STRING,
PS_TAX_COMPANY_CD STRING,
PS_COMP_FREQ_CD STRING,
COMP_RATE_AMT INT,
ANNUAL_RATE_LOC_AMT INT,
HOURLY_RATE_LOC_AMT INT,
CURRENCY_ID STRING,
EXCH_RATE_PCT INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

# COMMAND ----------


