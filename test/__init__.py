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

    # server = "foxie-poc-ondemand.sql.azuresynapse.net"
    server = req_body.get('server')

    # database = "test_db3"
    database = req_body.get('database')

    # sql = "select * from r_output_ow;"
    sql = req_body.get('query')

    #params
    params = req_body.get('params')

    # name = req.params.get('name')
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    install("pyodbc")
    install("csv")

    print("Dheeraj")
    
    local_file = "output.csv"
    execute_sql(server, database, sql, params, local_file)
    with open(local_file, 'rb') as f:
            return func.HttpResponse(
            body=f.read(),
            mimetype='text/csv',
            status_code=200
        )

