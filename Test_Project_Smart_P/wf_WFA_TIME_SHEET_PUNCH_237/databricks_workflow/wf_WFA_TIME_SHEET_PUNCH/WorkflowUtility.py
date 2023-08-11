# Databricks notebook source
# MAGIC %run ../Env

# COMMAND ----------

spark.sql(f"""USE {database}""")

# COMMAND ----------

import requests
import json
from time import sleep
from pyspark.sql.functions import *
import os
from IPython.display import display, Markdown
import pandas as pd

base_url = f"https://{instance_id}{api_version}"

params = {
 "Authorization" : f"Bearer {auth_token}",
 "Content-Type" : "application/json"
 }

def getJobDetails(job_id):
    job_detail_api = 'jobs/runs/list?job_id='
    url = f"{base_url}{job_detail_api}{job_id}"
    response = requests.get(url = url, headers = params)
    print(response.text)
    print(json.dumps(response.text))

def oneTimeRun(wklt_json):
    print(wklt_json)
    one_time_run_api = 'jobs/runs/submit'
    url = f"{base_url}{one_time_run_api}"
    response = requests.post(url = url, headers = params, data = json.dumps(wklt_json))
    json_run_id = json.loads(response.text)['run_id']
    displayRunUrl(json_run_id)
    return json_run_id

def getJobRunOutput(run_id):
    job_output_run_api = 'jobs/runs/get-output?run_id='
    url = f"{base_url}{job_output_run_api}{run_id}"
    response = requests.get(url = url, headers = params)

def createJob(wklt_json):
    print(wklt_json)
    create_job_api = 'jobs/create'
    url = f"{base_url}{create_job_api}"
    response = requests.post(url = url, headers = params, data = json.dumps(wklt_json))
    print(json.loads(response.text))
    job_id = json.loads(response.text)['job_id']
    print("job_id : " + str(job_id))
    return json.dumps(json.loads(response.text))

def runJob(job_id_json):
    run_job_api = 'jobs/run-now'
    url = f"{base_url}{run_job_api}"
    request_body = json.loads(job_id_json)
    response = requests.post(url = url, headers = params, data = json.dumps(request_body))
    run_id_json = json.loads(response.text)['run_id']
    run_id = displayRunUrl(run_id_json)
    # print(run_id)
    return run_id

def displayRunUrl(run_id_json):
    get_job_status = 'jobs/runs/get?run_id='
    url = f"{base_url}{get_job_status}{run_id_json}"
    response = requests.get(url = url, headers = params)
    json_response = json.loads(response.text)

    link_url = json_response['run_page_url']
    link_text = "Notebook job #" + str(json_response['job_id'])
    markdown_link = f"[{link_text}]({link_url})"
    display(Markdown(markdown_link))

    return json_response['run_id']

def showErrorList(tableName, mainWorkflowRunId):
    df = spark.sql(f"""SELECT * FROM {tableName} WHERE main_workflow_run_id = {mainWorkflowRunId}""")
    df.display()

def createErrorLogEntry(table_name, error_log_json):
    mainWorkflowId = error_log_json["mainWorkflowId"]
    mainWorkflowRunId = error_log_json["mainWorkflowRunId"]
    run_name = error_log_json["run_name"]
    task_key = error_log_json["task_key"]
    task_type = error_log_json["type"]
    task_page_url = error_log_json["task_page_url"] 
    query = f"""INSERT INTO {table_name} VALUES ('{mainWorkflowId}', '{mainWorkflowRunId}','{run_name}','{task_key}','{task_type}','{task_page_url}', current_timestamp())"""
    spark.sql(query)

def checkJobStatus(run_id, mainWorkflowId, mainWorkflowRunId, logTableName):
    print(run_id)
    get_job_status = 'jobs/runs/get?run_id='
    url = f"{base_url}{get_job_status}{run_id}"

    life_cycle_state = "PENDING"
    while(life_cycle_state != "TERMINATED"):
        response = requests.get(url = url, headers = params)
        json_response = json.loads(response.text)

        state = json_response['state']
        life_cycle_state = state['life_cycle_state']
        run_name = json_response['run_name']
        if(life_cycle_state == "INTERNAL_ERROR"):
            tasks = json_response['tasks']
            i = 0
            while(i < len(tasks)):
                task = tasks[i]
                task_state = task['state']
                task_life_cycle_state = task_state['life_cycle_state']
                if(task_life_cycle_state == "TERMINATED"):
                    task_result_state = task_state['result_state']
                    task_key = task['task_key']
                    if(task_result_state == "FAILED" and task_key.startswith("s_")):
                        task_page_url = task['run_page_url']
                        error_json = {}
                        error_json["mainWorkflowId"] = mainWorkflowId
                        error_json["mainWorkflowRunId"] = mainWorkflowRunId
                        error_json["run_name"] = run_name
                        error_json["task_key"] = task_key
                        error_json["type"] = "Session"
                        error_json["task_page_url"] = task_page_url
                        createErrorLogEntry(logTableName, error_json)
                i = i + 1
            showErrorList(logTableName, mainWorkflowRunId)
            raise Exception(f"Internal error occured please have a look at the {logTableName} for details")

        if(life_cycle_state == "TERMINATED"):
            result_state = state['result_state']
            if(result_state == "SUCCESS"):
                print("Job run completed successfully")
            else:
                raise Exception(f"Job run ,{result_state} please verify run = {run_id}")
        else:
            sleep(10)


# COMMAND ----------

from dateutil import parser
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

def showVariables(tableName):
    spark.sql(f"""SELECT * FROM {tableName}""").display()

# COMMAND ----------

def createLogTable(logTableName):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {logTableName}(main_workflow_job_id STRING, main_workflow_run_id STRING, run_name STRING, task_key STRING, type STRING, task_page_url STRING, occured_at TIMESTAMP)""")

# COMMAND ----------

def createVariableTable(parametersFileName, variableTableName, job_id_json):
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    folder_path = os.path.dirname(notebook_path)
    
    parameters_df = spark.createDataFrame(pd.read_json(f'./{parametersFileName}'))
    job_id = json.loads(job_id_json)['job_id']
    parameters_df = parameters_df.withColumn("job_id",lit(job_id))
    parameters_df.write.mode("append").format("delta").saveAsTable(variableTableName)


# COMMAND ----------

def readWorkflowJson(workflowName):
    path = "./workflow_jsons/" + workflowName + ".json"
    f = open(path)
    return json.load(f)

# COMMAND ----------

def addNotebookBaseParameters(variables, workflow):
    wf_json = workflow['wf_json']
    if(workflow['type'] == "mainWorkflow"):
        base_parameters = {"mainWorkflowId": "{{job_id}}", "mainWorkflowRunId": "{{run_id}}", "logTableName": "logTableName", "parentName": "name","variablesTableName": "variablesTableName","elementName" : "elementName"}
        for variable in variables:
            if(variable == 'name'):
                base_parameters['parentName'] = variables['name']
            else:
                base_parameters[variable] = variables[variable]
    elif workflow['type'] == "subWorkflow" :
        base_parameters = {"mainWorkflowId": "", "mainWorkflowRunId": "", "logTableName" : "", "parentName" : "","variablesTableName" : ""}
        for variable in variables:
            if(variable == 'parentName'):
                base_parameters['parentName'] = wf_json['run_name']
            else:
                base_parameters[variable] = variables[variable]

    for task in wf_json['tasks']:
        addClusterId(task)
        base_parameters['elementName'] = task['task_key']
        parameters_list = {}
        if('base_parameters' in task['notebook_task']):
            parameters_list.update(task['notebook_task']['base_parameters'])
        parameters_list.update(base_parameters)
        task['notebook_task']['base_parameters'] = parameters_list
    addAccessControlList(wf_json)
    return wf_json  

# COMMAND ----------

def addClusterId(task):
    task['existing_cluster_id'] = cluster_id

# COMMAND ----------

def addAccessControlList(wf_json):
    access_control_list = [
			{
				"user_name": f"{primary_contact_user_name}",
				"permission_level": "IS_OWNER"
			},
			{
				"user_name": f"{secodary_contact_user_name}",
				"permission_level": "CAN_MANAGE_RUN"
			}
		]
    wf_json['access_control_list'] = access_control_list


# COMMAND ----------


