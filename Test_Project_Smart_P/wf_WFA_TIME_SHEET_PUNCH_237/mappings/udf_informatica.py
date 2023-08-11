# Databricks notebook source
# udf for TO_DECIMAL
def to_decimal(value, scale):
    value = str(value)
    if not scale:
        number = float(value)
        return number        
    elif value == "NULL":
        return None
    elif value.replace(".","").isnumeric():
        number = float(value)
        fmt = '{:.'+str(scale)+'f}'
        return fmt.format(round(number, scale))
    else:
        return 0

spark.udf.register("TO_DECIMAL",to_decimal)

# COMMAND ----------

# udf for IN function
def in_function(*input_str):
    search_value = input_str[0]
    list_values = input_str[1:-1]
    case_flag = input_str[-1]

    if search_value is None:
        result = None
    elif case_flag is None:
        result = search_value in list_values
        if(result == 0):
            return None     
    elif case_flag == 0:
        result = search_value.lower() in [val.lower() for val in list_values]
    elif case_flag == 1:
        result = search_value in list_values
    else:
        list_values = input_str[1:]
        result = search_value in list_values
    return result

spark.udf.register("IN",in_function)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DELTA_TRAINING;
# MAGIC -- UDF for TO_INTEGER
# MAGIC CREATE
# MAGIC OR REPLACE FUNCTION TO_INTEGER(value STRING, flag INTEGER DEFAULT 0) RETURNS INTEGER CONTAINS SQL DETERMINISTIC RETURN
# MAGIC SELECT
# MAGIC   CASE 
# MAGIC     WHEN (flag = 0) THEN cast(round(cast(value as DECIMAL)) as INTEGER)
# MAGIC     WHEN (flag > 0) THEN cast(value as INTEGER)
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UDF FOR TO_BIGINT
# MAGIC CREATE
# MAGIC OR REPLACE FUNCTION TO_BIGINT(value STRING, flag INTEGER DEFAULT 0) RETURNS BIGINT CONTAINS SQL DETERMINISTIC RETURN
# MAGIC SELECT
# MAGIC   CASE 
# MAGIC     WHEN (flag = 0) THEN cast(round(cast(value as DECIMAL(38,10))) as BIGINT)
# MAGIC     WHEN (flag > 0) THEN cast(value as BIGINT)
# MAGIC   END

# COMMAND ----------

# UDF for IIF
def iif(condition, value1, value2=None):
    if (condition ==1):
        return value1
    elif (condition ==0):
        if(value2 == None):
            if(type(value1)==int):
                return 0
            elif (type(value1)==str):
                return ''
            else:
                return None
        else:
            return value2

spark.udf.register("IIF",iif)

# COMMAND ----------

# UDF for ISNULL
# def isNull(value):
#     if value is None:
#         return 1
#     else: 
#         return 0

# spark.udf.register("ISNULL",isNull)

# COMMAND ----------


