from configparser import ConfigParser
from google.cloud import bigquery
import json
import glob
import datetime

############################Establishing BQConnection: Metadata#################################

def bigquery_connection(filename='database.ini', section='bigquery_metadata'):
    parser = ConfigParser()
    parser.read(filename)
    params = dict(parser.items(section))
    project_id=params['project_id']
    dataset_id=params['dataset_id']
    return project_id, dataset_id

def bigquery_connection_db(filename='database.ini', section='bigquery_metadata'):
    parser = ConfigParser()
    parser.read(filename)
    params = dict(parser.items(section))
    project_id=params['project_id']
    return project_id


#*****retrieve_views********
def bigquery_retrieve_views():
    project_id, dataset_id = bigquery_connection()
    client = bigquery.Client()
    table_name = 'view_registry'
    query = f"SELECT database_name, view_name FROM `{project_id}.{dataset_id}.{table_name}`"
    query_job = client.query(query)  # API request
    query_job = query_job.result() 
    result_dict = {row[0]: row[1] for row in query_job}
    print('view_list: ', result_dict)
    return result_dict


#********************retrieve_failure_records*******************************
def bigquery_retrieve_failure_records():
    project_id, dataset_id = bigquery_connection()
    client = bigquery.Client()
    table_name = 'access_request'
    count_query = f"SELECT count(*) FROM `{project_id}.{dataset_id}.{table_name}` where status = 'Failed' and attempt_count < 2"
    count_query_job = client.query(count_query)  # API request
    count_query_job = count_query_job.result() 
    count = list(count_query_job)[0][0]
    print('The Failure Records Count is: ', count)
    query = f"SELECT input_json,businesspartneridentifier, status FROM `{project_id}.{dataset_id}.{table_name}` where status = 'Failed' and attempt_count < 2"
    query_job = client.query(query)  # API request
    query_job = query_job.result() 
    retrieve_result = {row[0]: row[1] for row in query_job}
    return retrieve_result

#********************retrieve_final_records*******************************
def bigquery_retrieve_final_records():
    project_id, dataset_id = bigquery_connection()
    client = bigquery.Client()
    table_name = 'access_request'
    count_query = f"SELECT count(*) FROM `{project_id}.{dataset_id}.{table_name}` where status = 'Failed' and attempt_count = 2"
    count_query_job = client.query(count_query)  # API request
    count_query_job = count_query_job.result() 
    count = list(count_query_job)[0][0]
    print('The Failure Records Count is: ', count)
    query = f"SELECT input_json,businesspartneridentifier, status FROM `{project_id}.{dataset_id}.{table_name}` where status = 'Failed' and attempt_count = 2"
    query_job = client.query(query)  # API request
    query_job = query_job.result() 
    retrieve_result = {row[0]: row[1] for row in query_job}
    return retrieve_result