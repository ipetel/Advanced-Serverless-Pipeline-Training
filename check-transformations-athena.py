import boto3
import json

### init params
client = boto3.client('athena')

### functions

def lambda_handler(event, context):

    #extract QueryExecutionIds
    QueryExecution_dict=json.loads(event['output'])
    QueryExecutionId=QueryExecution_dict['QueryExecutionId']
    
    
    # get status regard running queries
    response = client.get_query_execution(QueryExecutionId=QueryExecutionId)
    print(response)
    query_state=response['QueryExecution']['Status']['State']
    
    #extract the Status
    if query_state=='SUCCEEDED':
        return {
            'status':'SUCCEEDED',
            'output':event['output']
        }
    elif query_state=='FAILED' or query_state=='CANCELLED':
        return {
            'status':'FAILED',
            'output':event['output']
        }
    else:
        return {
            'status':'RUNNING',
            'output':event['output']
        }
