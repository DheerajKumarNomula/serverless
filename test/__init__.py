import logging
import csv
import azure.functions as func
import pyodbc as po


username = "sqladminuser"
password = "increff@ADMIN123"

def initialize_synapse_db_connection(server, database, username, password):
    cnxn = po.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                      server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return cnxn

def initialize_storage_account(storage_account_name, storage_account_key):
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

        return service_client
    except Exception as e:
        print(e)
        raise e
        
def upload_file_to_directory_bulk(service_client, container, cloud_file_path, local_input):
    try:
        file_system_client = service_client.get_file_system_client(file_system=container)

        file_client = file_system_client.get_file_client(cloud_file_path)

        local_file = open(local_input,'r')

        file_contents = local_file.read()

        file_client.upload_data(file_contents, overwrite=True)
        file_client.flush_data(len(file_contents))

    except Exception as e:
        print(e)
        raise e

# need key : 'value1', 'value2', 'value3'
def parameterize_query(query, params):
    query = query.replace(";", " ")
    isWherePresent = "where" in query.lower()
    for key in params:
        if len(params[key]) == 0:
            continue
        if isWherePresent:
            query = query + " and "
        else:
            query = query + " where "
            isWherePresent = True
        query = query + key + " in (" + params[key] + ");"
    return query

def execute_sql(server, database, sql, params, local_file):
    cnxn = initialize_synapse_db_connection(server, database, username, password)
    cursor = cnxn.cursor()
    sql_query = parameterize_query(sql, params)
    print(sql_query)
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    field_names = [i[0] for i in cursor.description]
    fp = open(local_file, 'w')
    file = csv.writer(fp, delimiter='\t')
    file.writerow(field_names)
    file.writerows(rows)
    fp.close()
    cursor.close()
    del cursor
    cnxn.close()



def install(package):
    # This function will install a package if it is not present
    from importlib import import_module
    try:
        import_module(package)
    except:
        from sys import executable as se
        from subprocess import check_call
        check_call([se,'-m','pip','-q','install',package])

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        params = dict(req.get_json())
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse('Unsupported request format', status_code=400)

    # server = "foxie-poc-ondemand.sql.azuresynapse.net"
    server = params.get('server')

    # database = "test_db3"
    database = params.get('database')

    # sql = "select * from r_output_ow;"
    sql = params.get('query')
    
    dest_container = params.get("destionation_container")
    task_id = params.get("task_id")
    
    #params
    params = params.get('params')
    logging.info(sql)
    logging.info(params)
    logging.info(database)
    logging.info(server)
    logging.info(dest_container)

    install("pyodbc")
    install("csv")

    print("Dheeraj")
    
    local_file = "output.csv"
    execute_sql(server, database, sql, params, local_file)

    # with open(local_file, 'rb') as f:
    #         return func.HttpResponse(
    #         body=f.read(),
    #         mimetype='text/csv',
    #         status_code=200
    #     )
    
    cloud_file_path = task_id+"/output.tsv"
    service_client = initialize_storage_account("foxiepoc", "84a2MWWBq0r3nXpkznuPti8aENj2WI9tO9PFPkIV/ytuJISaDrWCPekcoBELxsK+e4ZF/Y7WeqL+9IC2tfLhYg==")
    upload_file_to_directory_bulk(service_client, dest_container, cloud_file_path, local_file)
    return func.HttpResponse(
             "File uploaded successfully to the destination container.",
             status_code=200
        )

