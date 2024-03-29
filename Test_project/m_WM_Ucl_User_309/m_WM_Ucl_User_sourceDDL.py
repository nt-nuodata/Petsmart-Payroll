# Databricks notebook source
# COMMAND ----------

CREATE DATABASE IF NOT EXISTS DELTA_TRAINING;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.WM_UCL_USER_PRE(DC_NBR INT,
UCL_USER_ID INT,
COMPANY_ID INT,
USER_NAME STRING,
USER_PASSWORD STRING,
IS_ACTIVE INT,
CREATED_SOURCE_TYPE_ID INT,
CREATED_SOURCE STRING,
CREATED_DTTM TIMESTAMP,
LAST_UPDATED_SOURCE_TYPE_ID INT,
LAST_UPDATED_SOURCE STRING,
LAST_UPDATED_DTTM TIMESTAMP,
USER_TYPE_ID INT,
LOCALE_ID INT,
LOCATION_ID INT,
USER_FIRST_NAME STRING,
USER_MIDDLE_NAME STRING,
USER_LAST_NAME STRING,
USER_PREFIX STRING,
USER_TITLE STRING,
TELEPHONE_NUMBER STRING,
FAX_NUMBER STRING,
ADDRESS_1 STRING,
ADDRESS_2 STRING,
CITY STRING,
STATE_PROV_CODE STRING,
POSTAL_CODE STRING,
COUNTRY_CODE STRING,
USER_EMAIL_1 STRING,
USER_EMAIL_2 STRING,
COMM_METHOD_ID_DURING_BH_1 INT,
COMM_METHOD_ID_DURING_BH_2 INT,
COMM_METHOD_ID_AFTER_BH_1 INT,
COMM_METHOD_ID_AFTER_BH_2 INT,
COMMON_NAME STRING,
LAST_PASSWORD_CHANGE_DTTM TIMESTAMP,
LOGGED_IN INT,
LAST_LOGIN_DTTM TIMESTAMP,
DEFAULT_BUSINESS_UNIT_ID INT,
DEFAULT_WHSE_REGION_ID INT,
CHANNEL_ID INT,
HIBERNATE_VERSION INT,
NUMBER_OF_INVALID_LOGINS INT,
TAX_ID_NBR STRING,
EMP_START_DATE TIMESTAMP,
BIRTH_DATE TIMESTAMP,
GENDER_ID STRING,
PASSWORD_RESET_DATE_TIME TIMESTAMP,
PASSWORD_TOKEN STRING,
ISPASSWORDMANAGEDINTERNALLY INT,
COPY_FROM_USER STRING,
EXTERNAL_USER_ID STRING,
SECURITY_POLICY_GROUP_ID INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.WM_UCL_USER(LOCATION_ID INT,
WM_UCL_USER_ID INT,
WM_COMPANY_ID INT,
WM_LOCATION_ID INT,
WM_LOCALE_ID INT,
WM_USER_TYPE_ID INT,
ACTIVE_FLAG INT,
USER_NAME STRING,
TAX_ID_NBR STRING,
COMMON_NAME STRING,
USER_PREFIX STRING,
USER_TITLE STRING,
USER_FIRST_NAME STRING,
USER_MIDDLE_NAME STRING,
USER_LAST_NAME STRING,
BIRTH_DT DATE,
GENDER_ID STRING,
EMPLOYEE_START_DT DATE,
ADDR_1 STRING,
ADDR_2 STRING,
CITY STRING,
STATE_PROV_CD STRING,
POSTAL_CD STRING,
COUNTRY_CD STRING,
USER_EMAIL_1 STRING,
USER_EMAIL_2 STRING,
PHONE_NBR STRING,
FAX_NBR STRING,
WM_EXTERNAL_USER_ID STRING,
COPY_FROM_USER STRING,
WM_SECURITY_POLICY_GROUP_ID INT,
DEFAULT_WM_BUSINESS_UNIT_ID INT,
DEFAULT_WM_WHSE_REGION_ID INT,
WM_CHANNEL_ID INT,
WM_COMM_METHOD_ID_DURING_BH_1 INT,
WM_COMM_METHOD_ID_DURING_BH_2 INT,
WM_COMM_METHOD_ID_AFTER_BH_1 INT,
WM_COMM_METHOD_ID_AFTER_BH_2 INT,
PASSWORD_MANAGED_INTERNALLY_FLAG INT,
LOGGED_IN_FLAG INT,
LAST_LOGIN_TSTMP TIMESTAMP,
NUMBER_OF_INVALID_LOGINS INT,
PASSWORD_RESET_TSTMP TIMESTAMP,
LAST_PASSWORD_CHANGE_TSTMP TIMESTAMP,
WM_HIBERNATE_VERSION INT,
WM_CREATED_SOURCE_TYPE_ID INT,
WM_CREATED_SOURCE STRING,
WM_CREATED_TSTMP TIMESTAMP,
WM_LAST_UPDATED_SOURCE_TYPE_ID INT,
WM_LAST_UPDATED_SOURCE STRING,
WM_LAST_UPDATED_TSTMP TIMESTAMP,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.SITE_PROFILE(LOCATION_ID INT,
LOCATION_TYPE_ID INT,
STORE_NBR INT,
STORE_NAME STRING,
STORE_TYPE_ID STRING,
STORE_OPEN_CLOSE_FLAG STRING,
COMPANY_ID INT,
REGION_ID BIGINT,
DISTRICT_ID BIGINT,
PRICE_ZONE_ID STRING,
PRICE_AD_ZONE_ID STRING,
REPL_DC_NBR INT,
REPL_FISH_DC_NBR INT,
REPL_FWD_DC_NBR INT,
SQ_FEET_RETAIL FLOAT,
SQ_FEET_TOTAL FLOAT,
SITE_ADDRESS STRING,
SITE_CITY STRING,
STATE_CD STRING,
COUNTRY_CD STRING,
POSTAL_CD STRING,
SITE_MAIN_TELE_NO STRING,
SITE_GROOM_TELE_NO STRING,
SITE_EMAIL_ADDRESS STRING,
SITE_SALES_FLAG STRING,
EQUINE_MERCH_ID INT,
EQUINE_SITE_ID INT,
EQUINE_SITE_OPEN_DT TIMESTAMP,
GEO_LATITUDE_NBR INT,
GEO_LONGITUDE_NBR INT,
PETSMART_DMA_CD STRING,
LOYALTY_PGM_TYPE_ID INT,
LOYALTY_PGM_STATUS_ID INT,
LOYALTY_PGM_START_DT TIMESTAMP,
LOYALTY_PGM_CHANGE_DT TIMESTAMP,
BP_COMPANY_NBR INT,
BP_GL_ACCT INT,
TP_LOC_FLAG STRING,
TP_ACTIVE_CNT INT,
PROMO_LABEL_CD STRING,
PARENT_LOCATION_ID INT,
LOCATION_NBR STRING,
TIME_ZONE_ID STRING,
DELV_SERVICE_CLASS_ID STRING,
PICK_SERVICE_CLASS_ID STRING,
SITE_LOGIN_ID STRING,
SITE_MANAGER_ID INT,
SITE_OPEN_YRS_AMT INT,
HOTEL_FLAG INT,
DAYCAMP_FLAG INT,
VET_FLAG INT,
DIST_MGR_NAME STRING,
DIST_SVC_MGR_NAME STRING,
REGION_VP_NAME STRING,
REGION_TRAINER_NAME STRING,
ASSET_PROTECT_NAME STRING,
SITE_COUNTY STRING,
SITE_FAX_NO STRING,
SFT_OPEN_DT TIMESTAMP,
DM_EMAIL_ADDRESS STRING,
DSM_EMAIL_ADDRESS STRING,
RVP_EMAIL_ADDRESS STRING,
TRADE_AREA STRING,
FDLPS_NAME STRING,
FDLPS_EMAIL STRING,
OVERSITE_MGR_NAME STRING,
OVERSITE_MGR_EMAIL STRING,
SAFETY_DIRECTOR_NAME STRING,
SAFETY_DIRECTOR_EMAIL STRING,
RETAIL_MANAGER_SAFETY_NAME STRING,
RETAIL_MANAGER_SAFETY_EMAIL STRING,
AREA_DIRECTOR_NAME STRING,
AREA_DIRECTOR_EMAIL STRING,
DC_GENERAL_MANAGER_NAME STRING,
DC_GENERAL_MANAGER_EMAIL STRING,
ASST_DC_GENERAL_MANAGER_NAME1 STRING,
ASST_DC_GENERAL_MANAGER_EMAIL1 STRING,
ASST_DC_GENERAL_MANAGER_NAME2 STRING,
ASST_DC_GENERAL_MANAGER_EMAIL2 STRING,
REGIONAL_DC_SAFETY_MGR_NAME STRING,
REGIONAL_DC_SAFETY_MGR_EMAIL STRING,
DC_PEOPLE_SUPERVISOR_NAME STRING,
DC_PEOPLE_SUPERVISOR_EMAIL STRING,
PEOPLE_MANAGER_NAME STRING,
PEOPLE_MANAGER_EMAIL STRING,
ASSET_PROT_DIR_NAME STRING,
ASSET_PROT_DIR_EMAIL STRING,
SR_REG_ASSET_PROT_MGR_NAME STRING,
SR_REG_ASSET_PROT_MGR_EMAIL STRING,
REG_ASSET_PROT_MGR_NAME STRING,
REG_ASSET_PROT_MGR_EMAIL STRING,
ASSET_PROTECT_EMAIL STRING,
TP_START_DT TIMESTAMP,
OPEN_DT TIMESTAMP,
GR_OPEN_DT TIMESTAMP,
CLOSE_DT TIMESTAMP,
HOTEL_OPEN_DT TIMESTAMP,
ADD_DT TIMESTAMP,
DELETE_DT TIMESTAMP,
UPDATE_DT TIMESTAMP,
LOAD_DT TIMESTAMP) USING DELTA;