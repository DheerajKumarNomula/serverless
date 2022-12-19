import logging
import csv
import azure.functions as func
import pyodbc as po
from azure.storage.filedatalake import DataLakeServiceClient
from .data_lake_file_client import  initialize_storage_account
from .data_lake_file_client import  download_files_from_directory
from .data_lake_file_client import  upload_file_to_directory_bulk

from .data_lake_file_client import  write_file

username = "sqladminuser"
password = "increff@ADMIN123"

def initialize_synapse_db_connection(server, database, username, password):
    cnxn = po.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                      server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return cnxn


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

config_account_name = "foxiecommons"
config_account_key = "vP3zC4hFJEv/1JHktS5rQoeoIPoZRj4tmggXNNn6i/27NodwVHaThf1qubLXXEjewtC1BkRkplJq+AStztietQ=="
config_container = "commons"
config_sql_path = "common_files/export/"

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
    export_id = params.get("export_id")

    # sql = params.get('query')
    
    dest_container = params.get("destionation_container")
    task_id = params.get("task_id")
    
    #params
    params = params.get('params')



    print("Dheeraj")
    config_service_client = initialize_storage_account(config_account_name, config_account_key)
    sql = download_files_from_directory(config_service_client, config_container, config_sql_path+f"{export_id}"+".sql").decode()
    
    logging.info(sql)
    logging.info(params)
    logging.info(database)
    logging.info(server)
    logging.info(dest_container)

    local_file = f"{export_id}.csv"
    execute_sql(server, database, sql, params, local_file)

    cloud_file_path = task_id+f"/{export_id}.tsv"
    service_client = initialize_storage_account("foxiepoc", "84a2MWWBq0r3nXpkznuPti8aENj2WI9tO9PFPkIV/ytuJISaDrWCPekcoBELxsK+e4ZF/Y7WeqL+9IC2tfLhYg==")
    upload_file_to_directory_bulk(service_client, dest_container, cloud_file_path, local_file)
    return func.HttpResponse(
             "File uploaded successfully to the destination container.",
             status_code=200
        )
        # with open(local_file, 'rb') as f:
    #         return func.HttpResponse(
    #         body=f.read(),
    #         mimetype='text/csv',
    #         status_code=200
    #     )
    

