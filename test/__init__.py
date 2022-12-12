import logging
import csv
import azure.functions as func
import temp

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
    print("Dheeraj")
    server = "foxie-poc-ondemand.sql.azuresynapse.net"
    database = "test_db3"
    username = "sqladminuser"
    password = "increff@ADMIN123"
    sql = "select * from valid_stores;"
    local_file = "output.csv"
    temp.execute_sql(server, database, username, password, sql, local_file)
    with open(local_file, 'rb') as f:
            return func.HttpResponse(
            body=f.read(),
            mimetype='text/csv',
            status_code=200
        )

