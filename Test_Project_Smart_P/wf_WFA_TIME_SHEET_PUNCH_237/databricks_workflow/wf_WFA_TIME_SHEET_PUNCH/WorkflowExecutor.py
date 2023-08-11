# Databricks notebook source
# MAGIC %run ./WorkflowUtility

# COMMAND ----------

def main(workflowName):

    #Extract workflow json from json file.
    workflow = readWorkflowJson(workflowName)

    elementName = workflowName

    if(workflow['type'] == "mainWorkflow"):

        #Extract workflow variables.
        variables = workflow['variables']

        #Creating log table.
        logTableName = variables['logTableName']
        createLogTable(logTableName)

        wf_json = workflow['wf_json']

        #Add required notebook parametres
        wf_json = addNotebookBaseParameters(variables, workflow)

        # Create job
        job_id_json = createJob(wf_json)
        print("Job Created")

        # Create variables table using the parameters file provided.
        createVariableTable(variables['parameterFileName'], variables['variablesTableName'], job_id_json)
        print("Variables Table Created")

        #Run job
        run_id = runJob(job_id_json)
        print("Job run started with run id : "+ str(run_id))

        #Check job status
        job_id = json.loads(job_id_json)['job_id'] 

        checkJobStatus(run_id, job_id, run_id, logTableName)

        # Update Workflow Variables in database.
        persistVariables(variables['variablesTableName'], elementName, job_id, '')
    elif (workflow['type'] == "subWorkflow"):

        #Extraction of widgets
        widgets_variable_names = ["mainWorkflowId", "mainWorkflowRunId", "logTableName", "parentName", "variablesTableName"]
        widgets = {}
        for variable_name in widgets_variable_names:
            widgets[variable_name] = dbutils.widgets.get(variable_name)

        #Pre worklet variable updation
        if (len(workflow['preVariableUpdation']) != 0):
            preWorkletVariableUpdation = json.dumps(workflow['preVariableUpdation'])
            updateVariable(preWorkletVariableUpdation, widgets['variablesTableName'], widgets['mainWorkflowId'], widgets['parentName'], elementName)

        wf_json = workflow['wf_json']

        #Add required notebook parametres
        wf_json = addNotebookBaseParameters(widgets, workflow)

        # Create and submit a one time run.
        run_id = oneTimeRun(wf_json)

        #Check Run Status.
        checkJobStatus(run_id, widgets['mainWorkflowId'], widgets['mainWorkflowRunId'],widgets['logTableName'])

        #Post worklet variable updation
        if (len(workflow['postVariableUpdation']) != 0):
            postWorkletVariableUpdation = workflow['postVariableUpdation']
            updateVariable(postWorkletVariableUpdation, widgets['variablesTableName'], widgets['mainWorkflowId'], widgets['parentName'], elementName)

        # Update Workflow Variables in database.
        persistVariables(widgets['variablesTableName'], elementName, widgets['mainWorkflowId'], widgets['parentName'])


# COMMAND ----------

workflowName = dbutils.widgets.get("elementName")
main(workflowName)

# COMMAND ----------


