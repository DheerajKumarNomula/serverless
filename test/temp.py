import pyodbc as po
import csv




def initialize_synapse_db_connection(server, database, username, password):
    cnxn = po.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                      server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return cnxn


def execute_sql(server, database, username, password, sql, local_file):
    cnxn = initialize_synapse_db_connection(server, database, username, password)
    cursor = cnxn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    num_fields = len(cursor.description)
    field_names = [i[0] for i in cursor.description]
    fp = open(local_file, 'w')
    file = csv.writer(fp, delimiter='\t')
    file.writerow(field_names)
    file.writerows(rows)
    fp.close()
    cursor.close()
    del cursor
    cnxn.close()


print("yes")
