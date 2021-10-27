from time import sleep
import pandas as pd
import boto3
from config import AWS_REGION, DATABASE, STAGING_BUCKET


client = boto3.client('athena', region_name=AWS_REGION)


def run_query(query: str) -> str:
    """Asynchronous query in Athena"""
    query_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': STAGING_BUCKET})['QueryExecutionId']
    return query_id


def get_query_status(query_id: str):
    query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']

    if query_status == 'FAILED' or query_status == 'CANCELLED':
        reason = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['StateChangeReason']
        return {'state': 'FAILED', 'reason': reason, 'query_id': [query_id]}
    elif query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        return {'state': 'RUNNING', 'query_id': query_id, 'reason': None}
    else:
        return {'state': query_status, 'query_id': query_id, 'reason': None}


def wait_query_execution(query_id: str):
    sleep_time = 3
    while True:
        status = get_query_status(query_id)
        if status['state'] in ('FAILED', 'CANCELLED', 'SUCCEEDED'):
            return status
        else:
            sleep(sleep_time)
            sleep_time *= 2


def retrieve_query_results(query_id: str, pandas: bool = True):
    response = client.get_query_results(QueryExecutionId=query_id)

    if pandas:
        response = pd.DataFrame([[data.get('VarCharValue')
                                  for data in row['Data']]
                                 for row in response['ResultSet']['Rows']])
        # Set the header row as the df header
        new_header = response.iloc[0]
        response = response[1:]
        response.columns = new_header
    return response


def query_athena(query: str) -> dict:
    query_id = run_query(query)
    status = wait_query_execution(query_id)

    if status['state'] == 'SUCCEEDED':
        return retrieve_query_results(query_id)

    raise Exception('Error running query')
