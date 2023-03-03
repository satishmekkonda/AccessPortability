from google.cloud import bigquery
import json
import glob
import datetime
from config_bq import bigquery_connection,bigquery_retrieve_views,bigquery_connection_db,bigquery_retrieve_failure_records,bigquery_retrieve_final_records
from config_postgres import config_env, env_variables


##################################################################################################
if __name__ == "__main__":

    # retrieve all the views from the view registry
    print('*********************************Retrieving Views from View Registry*******************************')
    view_registry = bigquery_retrieve_views()

    # retrieve all pub/sub messages i.e. xmls (create a few sample xmls)
    print('*************************Retrieving Input JSON Files*************************************************')
    src = "input_request/"
    files = glob.glob('input_request/*json',recursive=True)
    print('json_file_list: ', files) 
    
    print('*****************Attempt Count Variables****************')
    attempt_count, first_increment_count, second_increment_count = env_variables()

    print('**********************START OF FIRST RUN***********************************')
    #for each xml
    for single_json in files:
        print('***************current_input_json_file: ', single_json)
        # parse xml to read bp id, request id, request date/time.
        with open(single_json, 'r') as f:
            input_data = json.load(f)
        UniqueTransactionID=input_data['APDataRequestTransaction']['TransactionMetadata']['UniqueTransactionID']
        TransactionDateTime=input_data['APDataRequestTransaction']['TransactionMetadata']['TransactionDateTime']
        CustomerRequestIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestIdentifier']
        CustomerRequestDateTime=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestDateTime']
        BusinessPartnerIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['BusinessPartnerIdentifier']
        print(f'**********************Retrieveing JSON Key Feilds for {single_json}.....***********************')
        print("UniqueTransactionID: ",UniqueTransactionID)
        print("TransactionDateTime: ",TransactionDateTime)
        print("CustomerRequestIdentifier: ",CustomerRequestIdentifier)
        print("CustomerRequestDateTime: ",CustomerRequestDateTime)
        print("BusinessPartnerIdentifier: ",BusinessPartnerIdentifier)
 
        # insert a record into access_request table with bp id, request id and requested date_time along with created_at, updated_at and attempt_count = 0 and the xml itself as a string/clob;
        # insert a new record into access_request_attempt table with FK to access_request with created_at, updated_at.
        print('********************writing to access_request and access_request_attempt tables**********************')
        created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('created_at: ', created_at)
        updated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('updated_at: ', created_at)
        status = 'Inprogress'
        print('Status: ', status)
        print('attempt_count: ', attempt_count)
        text_json = json.dumps(input_data)
        
        write_access_request = [{'UniqueTransactionID': UniqueTransactionID, 'TransactionDateTime': TransactionDateTime, 'CustomerRequestIdentifier': CustomerRequestIdentifier, 'CustomerRequestDateTime': CustomerRequestDateTime,'BusinessPartnerIdentifier': BusinessPartnerIdentifier,'status': status, 'attempt_count': attempt_count, 'created_at': created_at, 'updated_at': updated_at, 'input_json': text_json}]
        write_access_request_attempt = [{'BusinessPartnerIdentifier': BusinessPartnerIdentifier, 'status': status, 'created_at': created_at, 'updated_at': updated_at}]
        
        try:
            project_id, dataset_id = bigquery_connection()
            client = bigquery.Client()
            config = bigquery.LoadJobConfig()
            config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            table_ref_access_request = client.dataset(dataset_id).table('access_request')
            table_ref_access_request_attempt = client.dataset(dataset_id).table('access_request_attempt')

            print('********************inserting records into access_request and access_request_attemp tables*******************')
            
            write_access_request_exec = client.load_table_from_json(write_access_request, table_ref_access_request, job_config=config)
            write_access_request_exec.result()

            write_access_request_attempt_exec = client.load_table_from_json(write_access_request_attempt, table_ref_access_request_attempt, job_config=config)
            write_access_request_attempt_exec.result()

            print('********************write to access_request and access_request_attempt tables successful**********************')
        
        except Exception as error :
            print("Error in Execution: ", error)

        # initialize output JSON structure 
        output_json = [] 
        print('initializing_output_json: ', output_json)
        # for each view in the view registry
        try:
            print(f'********************QueryingEachView in the First RUN for {single_json}....********************')
            for database_name, view_name in view_registry.items():  
                print('current_database: ', database_name)
                print('current_view: ', view_name)
                try:   
                # retrieve records from the view using the bp id and requested date/time                        
                    project_id = bigquery_connection_db()
                    dataset_id = database_name
                    client = bigquery.Client()
                    view_name = view_name
                    request_query=f"select * from `{project_id}.{dataset_id}.{view_name}` where business_partner_no = {BusinessPartnerIdentifier} and customer_request_date <= '{CustomerRequestDateTime}'"
                    #request_query=f"select * from `{project_id}.{dataset_id}.{view_name}` where business_partner_no = {BusinessPartnerIdentifier}"
                    print(request_query)
                    query_job = client.query(request_query)  # API request
                    results = query_job.result()
                    result=[dict(row.items()) for row in results]
                    print(f'Retrived Records from the View for {BusinessPartnerIdentifier}: ', result)
                    # add all the retrieved records to the output JSON against a key which is the name of the view from which records were retrieved
                    output_json.append(result)

                    try:
                        print("**********************Updating Successful Status Information to access_request and access_request_attempt_tables**********************")
                        project_id, dataset_id = bigquery_connection()
                        client = bigquery.Client()
                        table_ref_access_request = 'access_request'
                        table_ref_access_request_attempt = 'access_request_attempt'
                        update_access_request_query = f"UPDATE `{project_id}.{dataset_id}.{table_ref_access_request}` SET status='Completed', attempt_count = {first_increment_count} where BusinessPartnerIdentifier = {BusinessPartnerIdentifier}"
                        print(update_access_request_query)
                        query_job = client.query(update_access_request_query)  # API request
                        query_job = query_job.result() 

                        update_access_request_attempt_query = f"UPDATE `{project_id}.{dataset_id}.{table_ref_access_request_attempt}` SET status='Completed' where BusinessPartnerIdentifier = {BusinessPartnerIdentifier}"
                        print(update_access_request_attempt_query)
                        query_job = client.query(update_access_request_attempt_query)  # API request
                        query_job = query_job.result() 
                        print("**********************Completion Update to access_request and access_request_attemp tables successful**********************")


                    except Exception as error:
                        print("Error in Execution: ", error)
                
                # update the record in access_request with status - failed/completed and increment attempt_count  
                except Exception as error:
                    print("Error in Execution: ", error)
                    print('Failed at first run, so increasing attempt count to ', first_increment_count)  
                    try:
                        print("**********************Updating Failure Status Information to access_request and access_request_attempt_tables**********************")
                        project_id, dataset_id = bigquery_connection()
                        client = bigquery.Client()
                        table_ref_access_request = 'access_request'
                        table_ref_access_request_attempt = 'access_request_attempt'
                        update_access_request_query = f"UPDATE `{project_id}.{dataset_id}.{table_ref_access_request}` SET status='Failed', attempt_count = {first_increment_count} where BusinessPartnerIdentifier = {BusinessPartnerIdentifier}"
                        print(update_access_request_query)
                        query_job = client.query(update_access_request_query)  # API request
                        query_job = query_job.result() 

                        update_access_request_attempt_query = f"UPDATE `{project_id}.{dataset_id}.{table_ref_access_request_attempt}` SET status='Failed' where BusinessPartnerIdentifier = {BusinessPartnerIdentifier}"
                        print(update_access_request_attempt_query)
                        query_job = client.query(update_access_request_attempt_query)  # API request
                        query_job = query_job.result() 
                        print("**********************Failure Update to access_request and access_request_attemp tables successful********************")                

                    except Exception as error:
                        print("Error in Execution: ", error)

            view_name=list(view_registry.values())
            res = str(dict(zip(view_name, output_json)))
            print('********************Final Output after first run*********************')
            print('Output: ', res)
            print('Attempt Count after first run: ', first_increment_count) 



        except Exception as error:
            print("Error in Execution: ", error)

    print('**********************END OF FIRST RUN***********************************')

    print('**********************START OF SECOND RUN***********************************')

    print('***************retrieving records in access_request table with status failed and attempt count < 2*************************')
    failure_retrieve_records = bigquery_retrieve_failure_records()
    print('The Retrieved Failure Records: ', failure_retrieve_records)

        # for each record do the below
		# retrieve xml from access_request record and parse bp id, request id, request date/time.
    for record in failure_retrieve_records:
        print('failed_json: ', record)
        input_data = json.loads(record)
        print('*************current_input_record****************', record)
        UniqueTransactionID=input_data['APDataRequestTransaction']['TransactionMetadata']['UniqueTransactionID']
        TransactionDateTime=input_data['APDataRequestTransaction']['TransactionMetadata']['TransactionDateTime']
        CustomerRequestIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestIdentifier']
        CustomerRequestDateTime=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestDateTime']
        BusinessPartnerIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['BusinessPartnerIdentifier']
        print('**********************Retrieveing JSON Key Feilds for Second Run.....***********************')
        print("UniqueTransactionID: ",UniqueTransactionID)
        print("TransactionDateTime: ",TransactionDateTime)
        print("CustomerRequestIdentifier: ",CustomerRequestIdentifier)
        print("CustomerRequestDateTime: ",CustomerRequestDateTime)
        print("BusinessPartnerIdentifier: ",BusinessPartnerIdentifier)
        #insert a new record into access_request_attempt table with FK to access_request with created_at, updated_at.
        created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('created_at: ', created_at)
        updated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('updated_at: ', created_at)
        status = 'Inprogress'
        print('Status: ', status)

        ############################################
        write_access_request_attempt = [{'BusinessPartnerIdentifier': BusinessPartnerIdentifier, 'status': status, 'created_at': created_at, 'updated_at': updated_at}]
        
        try:
            project_id, dataset_id = bigquery_connection()
            client = bigquery.Client()
            config = bigquery.LoadJobConfig()
            config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            table_ref_access_request_attempt = client.dataset(dataset_id).table('access_request_attempt')

            print('********************inserting records into access_request_attemp table*******************')

            write_access_request_attempt_exec = client.load_table_from_json(write_access_request_attempt, table_ref_access_request_attempt, job_config=config)
            write_access_request_attempt_exec.result()

            print('********************write to access_request_attempt tablessuccessful**********************')
        
        except Exception as error :
            print("Error in Execution: ", error)

        # initialize output JSON structure 
        output_json = [] 
        print('initializing_output_json: ', output_json)
        print('count_from_previous_run: ', first_increment_count)
        # for each view in the view registry
        try:
            print(f'********************QueryingEachView in the Second RUN for {record}....********************')
            for database_name, view_name in view_registry.items():  
                print('current_database: ', database_name)
                print('current_view: ', view_name)
                try:   
                # retrieve records from the view using the bp id and requested date/time                        
                    project_id = bigquery_connection_db()
                    dataset_id = database_name
                    client = bigquery.Client()
                    view_name = view_name
                    request_query=f"select * from `{project_id}.{dataset_id}.{view_name}` where business_partner_no = {BusinessPartnerIdentifier} and customer_request_date <= '{CustomerRequestDateTime}'"
                    #request_query=f"select * from `{project_id}.{dataset_id}.{view_name}` where business_partner_no = {BusinessPartnerIdentifier}"
                    print(request_query)
                    query_job = client.query(request_query)  # API request
                    results = query_job.result()
                    result=[dict(row.items()) for row in results]
                    print(f'Retrived Records from the View for {BusinessPartnerIdentifier}: ', result)
                    # add all the retrieved records to the output JSON against a key which is the name of the view from which records were retrieved
                    output_json.append(result)

                    try:
                        print("**********************Updating Successful Status Information to access_request and access_request_attempt_tables**********************")
                        project_id, dataset_id = bigquery_connection()
                        client = bigquery.Client()
                        table_ref_access_request = 'access_request'
                        table_ref_access_request_attempt = 'access_request_attempt'
                        update_access_request_query = f"UPDATE `{project_id}.{dataset_id}.{table_ref_access_request}` SET status='Completed', attempt_count = {first_increment_count} where BusinessPartnerIdentifier = {BusinessPartnerIdentifier}"
                        print(update_access_request_query)
                        query_job = client.query(update_access_request_query)  # API request
                        query_job = query_job.result() 

                        update_access_request_attempt_query = f"UPDATE `{project_id}.{dataset_id}.{table_ref_access_request_attempt}` SET status='Completed' where BusinessPartnerIdentifier = {BusinessPartnerIdentifier}"
                        print(update_access_request_attempt_query)
                        query_job = client.query(update_access_request_attempt_query)  # API request
                        query_job = query_job.result() 
                        print("**********************Completion Update to access_request and access_request_attemp tables successful**********************")


                    except Exception as error:
                        print("Error in Execution: ", error)
                
                # update the record in access_request with status - failed/completed and increment attempt_count  
                except Exception as error:
                    print("Error in Execution: ", error)
                    print('Failed at second run, so increasing attempt count to ', second_increment_count)  
                    try:
                        print("**********************Updating Failure Status Information to access_request and access_request_attempt_tables**********************")
                        project_id, dataset_id = bigquery_connection()
                        client = bigquery.Client()
                        table_ref_access_request = 'access_request'
                        table_ref_access_request_attempt = 'access_request_attempt'
                        update_access_request_query = f"UPDATE `{project_id}.{dataset_id}.{table_ref_access_request}` SET status='Failed', attempt_count = {second_increment_count} where BusinessPartnerIdentifier = {BusinessPartnerIdentifier}"
                        print(update_access_request_query)
                        query_job = client.query(update_access_request_query)  # API request
                        query_job = query_job.result() 

                        update_access_request_attempt_query = f"UPDATE `{project_id}.{dataset_id}.{table_ref_access_request_attempt}` SET status='Failed' where BusinessPartnerIdentifier = {BusinessPartnerIdentifier}"
                        print(update_access_request_attempt_query)
                        query_job = client.query(update_access_request_attempt_query)  # API request
                        query_job = query_job.result() 
                        print("**********************Failure Update to access_request and access_request_attemp tables successful********************")                

                    except Exception as error:
                        print("Error in Execution: ", error)

            view_name=list(view_registry.values())
            res = str(dict(zip(view_name, output_json)))
            print('********************Final Output after first run*********************')
            print('Output: ', res)
            print('Attempt Count after second run: ', second_increment_count) 



        except Exception as error:
            print("Error in Execution: ", error)


    print('**********************END OF SECOND RUN***********************************')

#################################################################################################################

    print('**********************START OF FINAL RUN FOR TRIGGERING NOTIFICATION***********************************')
    # retrieve records in access_request table with status failed and attempt count < 2

    print('***************retrieving records in access_request table with status failed and attempt count = 2*************************')
    final_retrieve_records = bigquery_retrieve_final_records()
    print('The Retrieved Failure Records: ', final_retrieve_records)

        # for each record do the below
		# retrieve xml from access_request record and parse bp id, request id, request date/time.
    for record in final_retrieve_records:
        print('failed_json: ', record)
        input_data = json.loads(record)
        print('*************current_input_record****************', record)
        UniqueTransactionID=input_data['APDataRequestTransaction']['TransactionMetadata']['UniqueTransactionID']
        TransactionDateTime=input_data['APDataRequestTransaction']['TransactionMetadata']['TransactionDateTime']
        CustomerRequestIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestIdentifier']
        CustomerRequestDateTime=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestDateTime']
        BusinessPartnerIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['BusinessPartnerIdentifier']
        print('**********************Retrieveing JSON Key Feilds for Second Run.....***********************')
        print("UniqueTransactionID: ",UniqueTransactionID)
        print("TransactionDateTime: ",TransactionDateTime)
        print("CustomerRequestIdentifier: ",CustomerRequestIdentifier)
        print("CustomerRequestDateTime: ",CustomerRequestDateTime)
        print("BusinessPartnerIdentifier: ",BusinessPartnerIdentifier)
        # send an alert email with an email template
        print('Failure alert  to xyz@hm.com for BusinessPartnerIdentifier: ',{BusinessPartnerIdentifier})


    print('---------------------------------Failure Notification to be setup for BP IDs after Second RUN----------------------------------')
