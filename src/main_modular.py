import psycopg2
import json
import glob
import datetime
from config import config,config_db,config_env,bigquery_connection,bigquery_connection_db,connect_metadata,retrieve_views,retrieve_failure_records,retrieve_final_records,env_variables
from utils.logger import get_logger

logger = get_logger()

##################################################################################################
if __name__ == "__main__":

    # retrieve all the views from the view registry
    logger.info(f'*********************************Retrieving Views from View Registry*******************************')
    view_registry = retrieve_views()


    # retrieve all pub/sub messages i.e. xmls (create a few sample xmls)
    logger.info(f'*************************Retrieving Input JSON Files*************************************************')
    src = "input_request/"
    files = glob.glob('input_request/*json',recursive=True)
    logger.info(f'json_file_list: ', files) 
    
    logger.info(f'*****************Attempt Count Variables****************')
    attempt_count, first_increment_count, second_increment_count = env_variables()

    logger.info(f'**********************START OF FIRST RUN***********************************')
    #for each xml
    for single_json in files:
        logger.info(f'***************current_input_json_file: ', single_json)
        # parse xml to read bp id, request id, request date/time.
        with open(single_json, 'r') as f:
            input_data = json.load(f)
        UniqueTransactionID=input_data['APDataRequestTransaction']['TransactionMetadata']['UniqueTransactionID']
        TransactionDateTime=input_data['APDataRequestTransaction']['TransactionMetadata']['TransactionDateTime']
        CustomerRequestIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestIdentifier']
        CustomerRequestDateTime=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestDateTime']
        BusinessPartnerIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['BusinessPartnerIdentifier']
        logger.info(f'**********************Retrieveing JSON Key Feilds for {single_json}.....***********************')
        logger.info(f"UniqueTransactionID: ",UniqueTransactionID)
        logger.info(f"TransactionDateTime: ",TransactionDateTime)
        logger.info(f"CustomerRequestIdentifier: ",CustomerRequestIdentifier)
        logger.info(f"CustomerRequestDateTime: ",CustomerRequestDateTime)
        logger.info(f"BusinessPartnerIdentifier: ",BusinessPartnerIdentifier)
 
        # insert a record into access_request table with bp id, request id and requested date_time along with created_at, updated_at and attempt_count = 0 and the xml itself as a string/clob;
        # insert a new record into access_request_attempt table with FK to access_request with created_at, updated_at.
        logger.info(f'********************writing to access_request and access_request_attempt tables**********************')
        created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f'created_at: ', created_at)
        updated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f'updated_at: ', created_at)
        status = 'Inprogress'
        logger.info(f'Status: ', status)
        logger.info(f'attempt_count: ', attempt_count)
        text_json = json.dumps(input_data)
        connection = None
        write_access_request = """INSERT INTO access_request (UniqueTransactionID,TransactionDateTime,CustomerRequestIdentifier,CustomerRequestDateTime,BusinessPartnerIdentifier,status,attempt_count,created_at,updated_at,input_json) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        write_access_request_attempt = """INSERT INTO access_request_attempt (BusinessPartnerIdentifier,status,created_at,updated_at) VALUES(%s,%s,%s,%s)"""
        try:
            connection, cursor = connect_metadata()
            logger.info(f'********************inserting records into access_request and access_request_attemp tables*******************')
            write_access_request_exec = cursor.execute(write_access_request,(UniqueTransactionID,TransactionDateTime,CustomerRequestIdentifier,CustomerRequestDateTime,BusinessPartnerIdentifier,status,attempt_count,created_at,updated_at,text_json))
            write_access_request_attempt_exec = cursor.execute(write_access_request_attempt,(BusinessPartnerIdentifier,status,created_at,updated_at))
            connection.commit()
            cursor.close()
            logger.info(f'********************write to access_request and access_request_attempt tables successful**********************')
        except(Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            if connection is not None:
                connection.close()
        # initialize output JSON structure 
        output_json = [] 
        logger.info(f'initializing_output_json: ', output_json)
        # for each view in the view registry
        try:
            logger.info(f'********************QueryingEachView in the First RUN for {single_json}....********************')
            for database_name, view_name in view_registry.items():  
                logger.info(f'current_database: ', database_name)
                logger.info(f'current_view: ', view_name)
                connection = None                
                try:   
                # retrieve records from the view using the bp id and requested date/time                        
                    params=config_db()
                    connection=psycopg2.connect(**params, database=database_name)
                    logger.info(fconnection)
                    cursor=connection.cursor()
                    #request_query=f"select * from {view_name} where business_partner_no = {BusinessPartnerIdentifier}"
                    request_query=f"select * from {view_name} where business_partner_no = {BusinessPartnerIdentifier} and customer_request_date <= '{CustomerRequestDateTime}'"
                    logger.info(frequest_query)
                    cursor.execute(request_query)
                    query_result = [ dict(line) for line in [zip([ column[0] for column in cursor.description], row) for row in cursor.fetchall()] ]
                    logger.info(f'Retrived Records from the View for {BusinessPartnerIdentifier}: ', query_result)
                    connection.commit()
                    cursor.close()
                    # add all the retrieved records to the output JSON against a key which is the name of the view from which records were retrieved
                    output_json.append(query_result)
 
                    try:
                        logger.info(f"**********************Updating Successful Status Information to access_request and access_request_attempt_tables**********************")
                        connection, cursor = connect_metadata()
                        cursor = connection.cursor()
                        update_access_request_query = f"UPDATE access_request SET status='Completed', attempt_count = {first_increment_count} where BusinessPartnerIdentifier = {BusinessPartnerIdentifier} RETURNING *"
                        logger.info(fupdate_access_request_query)
                        cursor.execute(update_access_request_query)
                        connection.commit()

                        update_access_request_attempt_query = f"UPDATE access_request_attempt SET status='Completed' where BusinessPartnerIdentifier = {BusinessPartnerIdentifier} RETURNING *"
                        logger.info(fupdate_access_request_attempt_query)
                        cursor.execute(update_access_request_attempt_query)
                        connection.commit()
                        cursor.close()
                        logger.info(f"**********************Completion Update to access_request and access_request_attemp tables successful**********************")
                    except(Exception, psycopg2.DatabaseError) as error:
                        logger.error(error)
                    finally:
                        if connection is not None:
                            connection.close()

                # update the record in access_request with status - failed/completed and increment attempt_count  
                except(Exception, psycopg2.DatabaseError) as error:
                    logger.error(error)
                    logger.info(f"**********************Updating Failure Status Information to access_request and access_request_attempt_tables**********************")
                    logger.info(f'Failed at first run, so increasing attempt count to ', first_increment_count)                    
                    connection = None
                    try:
                        connection, cursor = connect_metadata()
                        cursor = connection.cursor()
                        update_access_request_query = f"UPDATE access_request SET status='Failed', attempt_count = {first_increment_count} where BusinessPartnerIdentifier = {BusinessPartnerIdentifier} RETURNING *"
                        logger.info(fupdate_access_request_query)
                        cursor.execute(update_access_request_query)
                        connection.commit()

                        update_access_request_attempt_query = f"UPDATE access_request_attempt SET status='Failed' where BusinessPartnerIdentifier = {BusinessPartnerIdentifier} RETURNING *"
                        logger.info(fupdate_access_request_attempt_query)
                        cursor.execute(update_access_request_attempt_query)
                        connection.commit()
                        cursor.close()
                        logger.info(f"**********************Failure Update to access_request and access_request_attemp tables successful********************")                
                    except(Exception, psycopg2.DatabaseError) as error:
                        logger.error(error)                        
                    finally:
                        if connection is not None:
                            connection.close()
                finally:
                    if connection is not None:
                        connection.close()
        
            view_name=list(view_registry.values())
            res = str(dict(zip(view_name, output_json)))
            logger.info(f'********************Final Output after first run*********************')
            logger.info(f'Output: ', res)
             

        except(Exception, psycopg2.DatabaseError) as error:
                logger.error(error)
        finally:
            if connection is not None:
                connection.close()
                logger.info(f'Attempt Count after first run: ', first_increment_count) 
    logger.info(f'**********************END OF FIRST RUN***********************************')

    logger.info(f'**********************START OF SECOND RUN***********************************')
    # retrieve records in access_request table with status failed and attempt count < 2
    logger.info(f'***************retrieving records in access_request table with status failed and attempt count < 2*************************')
    failure_retrieve_records = retrieve_failure_records()
    logger.info(f'The Retrieved Failure Records: ', failure_retrieve_records)


    # for each record do the below
		# retrieve xml from access_request record and parse bp id, request id, request date/time.
    for record in failure_retrieve_records:
        logger.info(f'failed_json: ', record)
        input_data = json.loads(record)
        logger.info(f'*************current_input_record****************', record)
        UniqueTransactionID=input_data['APDataRequestTransaction']['TransactionMetadata']['UniqueTransactionID']
        TransactionDateTime=input_data['APDataRequestTransaction']['TransactionMetadata']['TransactionDateTime']
        CustomerRequestIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestIdentifier']
        CustomerRequestDateTime=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestDateTime']
        BusinessPartnerIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['BusinessPartnerIdentifier']
        logger.info(f'**********************Retrieveing JSON Key Feilds for Second Run.....***********************')
        logger.info(f"UniqueTransactionID: ",UniqueTransactionID)
        logger.info(f"TransactionDateTime: ",TransactionDateTime)
        logger.info(f"CustomerRequestIdentifier: ",CustomerRequestIdentifier)
        logger.info(f"CustomerRequestDateTime: ",CustomerRequestDateTime)
        logger.info(f"BusinessPartnerIdentifier: ",BusinessPartnerIdentifier)
        #insert a new record into access_request_attempt table with FK to access_request with created_at, updated_at.
        created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f'created_at: ', created_at)
        updated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f'updated_at: ', created_at)
        status = 'Inprogress'
        logger.info(f'Status: ', status)
        write_access_request_attempt = """INSERT INTO access_request_attempt (BusinessPartnerIdentifier,status,created_at,updated_at) VALUES(%s,%s,%s,%s)"""
        connection = None
        try:
            connection, cursor = connect_metadata()
            logger.info(f'********************inserting records into access_request_attemp table*******************')
            write_access_request_attempt_exec = cursor.execute(write_access_request_attempt,(BusinessPartnerIdentifier,status,created_at,updated_at))
            connection.commit()
            cursor.close()
            logger.info(f'********************write to access_request_attempt table successful**********************')
        except(Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            if connection is not None:
                connection.close()
        #initialize output JSON structure 
        output_json = [] 
        logger.info(f'initializing_output_json: ', output_json)
        logger.info(f'count_from_previous_run: ', first_increment_count)
        # for each view in the view registry
        try:
            logger.info(f'********************QueryingEachView in the Second RUN...********************')
            for database_name, view_name in view_registry.items():  
                logger.info(f'current_database: ', database_name)
                logger.info(f'current_view: ', view_name)
                connection = None                
                # retrieve records from the view using the bp id and requested date/time                        
                try:   
                    params=config_db()
                    connection=psycopg2.connect(**params, database=database_name)
                    logger.info(fconnection)
                    cursor=connection.cursor()
                    #request_query=f"select * from {view_name} where business_partner_no = {BusinessPartnerIdentifier}"
                    request_query=f"select * from {view_name} where business_partner_no = {BusinessPartnerIdentifier} and customer_request_date <= '{CustomerRequestDateTime}'"
                    logger.info(frequest_query)
                    cursor.execute(request_query)
                    query_result = [ dict(line) for line in [zip([ column[0] for column in cursor.description], row) for row in cursor.fetchall()] ]
                    logger.info(f'Retrived Records from the View for {BusinessPartnerIdentifier}: ', query_result)
                    connection.commit()
                    cursor.close()
                    # add all the retrieved records to the output JSON against a key which is the name of the view from which records were retrieved
                    output_json.append(query_result)
 
                    try:
                        connection, cursor = connect_metadata()
                        cursor = connection.cursor()
                        update_access_request_query = f"UPDATE access_request SET status='Completed', attempt_count = {first_increment_count} where BusinessPartnerIdentifier = {BusinessPartnerIdentifier} RETURNING *"
                        logger.info(fupdate_access_request_query)
                        cursor.execute(update_access_request_query)
                        connection.commit()

                        update_access_request_attempt_query = f"UPDATE access_request_attempt SET status='Completed' where BusinessPartnerIdentifier = {BusinessPartnerIdentifier} RETURNING *"
                        logger.info(fupdate_access_request_attempt_query)
                        cursor.execute(update_access_request_attempt_query)
                        connection.commit()
                        cursor.close()
                        logger.info(f"**********************Completion Update to access_request and access_request_attemp tables tables successful**********************")
                    except(Exception, psycopg2.DatabaseError) as error:
                        logger.error(error)
                    finally:
                        if connection is not None:
                            connection.close()

                # update the record in access_request with status - failed/completed and increment attempt_count  
                except(Exception, psycopg2.DatabaseError) as error:
                    logger.error(error)
                    logger.info(f'Failed at second run, so increasing count..attempt count is: ', second_increment_count)                    
                    connection = None
                    try:
                        connection, cursor = connect_metadata()
                        cursor = connection.cursor()
                        update_access_request_query = f"UPDATE access_request SET status='Failed', attempt_count = {second_increment_count} where BusinessPartnerIdentifier = {BusinessPartnerIdentifier} RETURNING *"
                        logger.info(fupdate_access_request_query)
                        cursor.execute(update_access_request_query)
                        connection.commit()

                        update_access_request_attempt_query = f"UPDATE access_request_attempt SET status='Failed' where BusinessPartnerIdentifier = {BusinessPartnerIdentifier} RETURNING *"
                        logger.info(fupdate_access_request_attempt_query)
                        cursor.execute(update_access_request_attempt_query)
                        connection.commit()
                        cursor.close()
                        logger.info(f"**********************Failure Update to access_request and access_request_attemp tables tables successful********************")                
                    except(Exception, psycopg2.DatabaseError) as error:
                        logger.error(error)                        
                    finally:
                        if connection is not None:
                            connection.close()
                finally:
                    if connection is not None:
                        connection.close()
            logger.info(f'********************Final Output after Second run*********************')            
            view_name=list(view_registry.values())
            res = str(dict(zip(view_name, output_json)))
            logger.info(f'Output: ', res)
        except(Exception, psycopg2.DatabaseError) as error:
                logger.error(error)
        finally:
            if connection is not None:
                connection.close()
                logger.info(f'Attempt Count after second run: ', second_increment_count)

    logger.info(f'**********************END OF SECOND RUN***********************************')
    logger.info(f'**********************START OF FINAL RUN FOR TRIGGERING NOTIFICATION***********************************')
#################################################################################################################
    # retrieve records in access_request table with status failed and attempt count < 2
    logger.info(f'*******************retrieving records in access_request table with status failed and attempt count = 2********************')
    final_retrieve_records = retrieve_final_records()
    logger.info(f'The Retrieved Final Records: ', final_retrieve_records)


    # for each record do the below  
    for record in final_retrieve_records:
        logger.info(f'failed_json: ', record)
        input_data = json.loads(record)
        logger.info(f'current_input_json: ', record)
        UniqueTransactionID=input_data['APDataRequestTransaction']['TransactionMetadata']['UniqueTransactionID']
        TransactionDateTime=input_data['APDataRequestTransaction']['TransactionMetadata']['TransactionDateTime']
        CustomerRequestIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestIdentifier']
        CustomerRequestDateTime=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['CustomerRequestDateTime']
        BusinessPartnerIdentifier=input_data['APDataRequestTransaction']['Payload']['APDataRequest']['BusinessPartnerIdentifier']
        logger.info(f'**********************Retrieveing JSON Feilds.....***********************')
        logger.info(f"UniqueTransactionID: ",UniqueTransactionID)
        logger.info(f"TransactionDateTime: ",TransactionDateTime)
        logger.info(f"CustomerRequestIdentifier: ",CustomerRequestIdentifier)
        logger.info(f"CustomerRequestDateTime: ",CustomerRequestDateTime)
        logger.info(f"BusinessPartnerIdentifier: ",BusinessPartnerIdentifier)
        # send an alert email with an email template
        logger.info(f'Failure alert  to xyz@hm.com for BusinessPartnerIdentifier: ',{BusinessPartnerIdentifier})

    logger.info(f'---------------------------------Failure Notification to be setup for BP IDs after Second RUN----------------------------------')
    
        