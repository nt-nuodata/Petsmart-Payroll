# Databricks notebook source
# MAGIC %run ../databricks_workflow/Env

# COMMAND ----------

spark.sql(f"""USE {database}""")

# COMMAND ----------

import requests
import json
from time import sleep
from pyspark.sql.functions import *
import os
from IPython.display import display, Markdown
from dateutil import parser
import pandas as pd

# COMMAND ----------

def showVariables(tableName):
    spark.sql(f"""SELECT * FROM {tableName}""").display()

# COMMAND ----------


def fetchAndCreateVariables(parentElementName, elementName, variablesTableName, job_id):
    query = f"""SELECT name, value, data_type FROM {variablesTableName} where ((element_name = '{elementName}' and parent_element_name = '{parentElementName}') or (element_name = '{parentElementName}')) and job_id = {job_id} and is_user_defined = true"""
    print(query)
    result = spark.sql(query);
    variables = result.collect()
    print(variables)
    for variable in variables:
        name = variable['name']
        value = variable['value']
        dataType = variable['data_type']
        # Assign the value to the variable
        if(dataType=='integer'):
            if variable['value'] == '':
                value = 0
            else:
                value=int(variable['value'])
        elif (dataType == "date/time"):
            value = parser.parse(value)
        print(name)
        globals()[name] = value

# COMMAND ----------

def updateVariable(variables, tableName, job_id, parentName, elementName):
    variables = json.loads(variables)
    for leftOperand in variables:
        rightOperand = variables[leftOperand]
        query = f"Update {tableName} set value = (Select value from {tableName} where name = '{rightOperand}' and job_id = {job_id} and element_name = '{parentName}'), is_updated = true where name = '{leftOperand}' and element_name = '{elementName}' and (parent_element_name = '{parentName}' or parent_element_name = '')"
        print(query)
        spark.sql(query)
    showVariables(tableName)

# COMMAND ----------

def updateMappingVariable(parametersTableName, elementName, mainWorkflowId, parentElementName):
    query = f"""SELECT name from {parametersTableName} where job_id = {mainWorkflowId} and element_name = '{elementName}' and parent_element_name = '{parentElementName}'"""
    variableNames = spark.sql(query).collect()
    for row in variableNames:
        name = globals()[row['name']]
        update_query = f"""UPDATE {parametersTableName} set value = '{name}', old_value = '{name}' where job_id = {mainWorkflowId} and element_name = '{elementName}' and parent_element_name = '{parentElementName}'"""
        print(update_query)
        spark.sql(update_query)
    showVariables(parametersTableName)

# COMMAND ----------

def truncateTargetTables(targetTables):
    targetTables = json.loads(targetTables)
    for targetTable in targetTables:
        if(targetTables[targetTable] == "YES"):
            spark.sql(f"""TRUNCATE TABLE {targetTable}""")

# COMMAND ----------

def updateOldValueForPersistentVariables(parametersTableName, elementName, job_id, parentElementName):
    query = f"""UPDATE {parametersTableName} SET old_value = value where element_name = '{elementName}' and parent_element_name = '{parentElementName}' and is_updated = true and is_persistent = true and job_id = {job_id} """
    spark.sql(query)
    
def updateValueForNonPersistentVariables(parametersTableName, elementName, job_id, parentElementName):
    query = f"""UPDATE {parametersTableName} SET value = old_value where element_name = '{elementName}' and parent_element_name = '{parentElementName}' and is_updated = true and is_persistent = false and job_id = {job_id}"""
    spark.sql(query)

def persistVariables(parametersTableName, elementName,job_id, parentElement):
    updateOldValueForPersistentVariables(parametersTableName, elementName,job_id, parentElement)
    updateValueForNonPersistentVariables(parametersTableName, elementName,job_id, parentElement)
    query = f"""UPDATE {parametersTableName} SET is_updated = false WHERE element_name = '{elementName}' and parent_element_name = '{parentElement}' and job_id = {job_id} """
    spark.sql(query)
    showVariables(parametersTableName)

# COMMAND ----------

def set_variable(variableName, updatedValue):
    return str(updatedValue)
spark.udf.register("SETVARIABLE",set_variable)
