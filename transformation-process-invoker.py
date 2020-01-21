import json
import boto3

### init params

sf_client = boto3.client('stepfunctions')
glue_client = boto3.client('glue')

### functions

def invoke_stepFunction(arn):
    response = sf_client.start_execution(
        stateMachineArn=arn
    )
    
def invoke_glue_job(job_name):
    response = glue_client.start_job_run(
        JobName=job_name
    )

def lambda_handler(event, context):
    if event['useAthena']:
        stepfunction_arn='arn:aws:states:eu-west-1:386027091365:stateMachine:stepFunction-cuba-burgol-c2'
        invoke_stepFunction(stepfunction_arn)
        msg='used Athena for the ETL process'
    else:
        glue_job_name='stepFunction-cuba-burgol-c2'
        invoke_glue_job(glue_job_name)
        msg='used Glue for the ETL process'
    
    return {
        'statusCode': 200,
        'body': msg
    }
