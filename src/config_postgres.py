from configparser import ConfigParser
import psycopg2
import json
import glob
import datetime

############################Establishing PostgresConnection: Metadata#################################
def postgres_connection(filename='database.ini', section='postgresql_metadata'):
    parser = ConfigParser()
    parser.read(filename)
    params = dict(parser.items(section))
    return params

############################Establishing PostgresConnection: OtherDatasets###############################
def postgres_connection_db(filename='database.ini', section='postgres_datasets'):
    parser = ConfigParser()
    parser.read(filename)
    params = dict(parser.items(section))
    return params

############################Defining Variables########################################################

def config_env(filename='database.ini', section='env'):
    parser = ConfigParser()
    parser.read(filename)
    params = dict(parser.items(section))
    return params

def env_variables():
    params=config_env()
    attempt_count=params["attempt_count"]
    first_increment_count=params["first_increment_count"]
    second_increment_count=params["second_increment_count"]
    return attempt_count, first_increment_count, second_increment_count
	
#############################MAIN_FUNCTIONS################################

#******connecting_postgres_database****
def postgres_connect_metadata():
    params=postgres_connection()
    connection=psycopg2.connect(**params)
    cursor=connection.cursor()
    return connection, cursor

#*****retrieve_views********
def postgres_retrieve_views():
    connection = None
    try:
        connection, cursor = postgres_connect_metadata()
        view_query="select database_name, view_name from view_registry"
        retrieve_views=cursor.execute(view_query)
        result=cursor.fetchall()
        result_dict = {row[0]: row[1] for row in result}
        connection.commit()
        print('view_list: ', result_dict)
        cursor.close()
        return result_dict
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
#********************retrieve_failure_records*******************************
def postgres_retrieve_failure_records():
    connection = None
    try:
        connection, cursor = postgres_connect_metadata()
        count_query = f"SELECT count(*) FROM access_request where status = 'Failed' and attempt_count < 2"
        cursor.execute(count_query)
        count = cursor.fetchone()[0]
        print('The Failure Records Count is: ', count)
        retrieve_query = f"SELECT input_json,businesspartneridentifier, status FROM access_request where status = 'Failed' and attempt_count < 2"
        cursor.execute(retrieve_query)
        retrieve_result=cursor.fetchall()
        retrieve_result = {row[0]: row[1] for row in retrieve_result}
        connection.commit()
        cursor.close()
        return retrieve_result
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            
#********************retrieve_final_records*******************************
def postgres_retrieve_final_records():
    connection = None
    try:
        connection, cursor = postgres_connect_metadata()
        count_query = f"SELECT count(*) FROM access_request where status = 'Failed' and attempt_count = 2"
        cursor.execute(count_query)
        count = cursor.fetchone()[0]
        print('The Failure Records Count is: ', count)
        retrieve_query = f"SELECT input_json, businesspartneridentifier, status FROM access_request where status = 'Failed' and attempt_count = 2"
        #print(retrieve_query)
        cursor.execute(retrieve_query)
        retrieve_result=cursor.fetchall()
        #print('Failed Records with attempt_count 2: ' ,retrieve_result)
        retrieve_result = {row[0]: row[1] for row in retrieve_result}
        connection.commit()
        cursor.close()
        #print("Retrieved Records: ", retrieve_result)
        return retrieve_result
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
