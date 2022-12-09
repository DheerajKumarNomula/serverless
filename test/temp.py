import pyodbc as po
import logging

class DBConnector(object):
   def __init__(self, server, database, user, password):
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        self.dbconn = None

    # creats new connection
   def create_connection(self):
       return po.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.user + ';PWD=' + self.password)

   # For explicitly opening database connection
   def __enter__(self):
       self.dbconn = self.create_connection()
       return self.dbconn

   def __exit__(self):
       if self.dbconn is None:
           logging.warning('Connection does not exist')
           return
       self.dbconn.close()

class AzureSQL():
    _con = None

    @classmethod
    def connection(cls, new=False):
        server = 'tcp:foxie-poc-ondemand.sql.azuresynapse.net'
        database = 'test_db5'
        username = 'dharani'
        password = 'Clay$123'
        """Creates return new Singleton database connection"""
        if new or cls._con is None:
            cls._con = DBConnector(server, database, user=username, password=password).create_connection()
        return cls._con

    @classmethod
    def cursor(cls):
        con = cls.connection()
        try:
            cursor = con.cursor()
        except po.ProgrammingError:
            con = cls.connection(new=True)
            cursor = con.cursor()
        return cursor

    @classmethod
    def execute_query(cls, query, first=False):
        """execute query on singleton db connection"""
        cursor = cls.cursor()
        cursor.execute(query)
        result = cursor.fetchone() if first else cursor.fetchall()
        cursor.close()
        return result

    @classmethod
    def execute_update(cls, query):
        csr = cls.cursor()
        csr.exectue(query)
        csr.commit()

    @classmethod
    def _select_query_builder(cls, src, cols=[], unique=False, limit=None):
        top_n = '' if limit is None or not isinstance(limit, int) else f'TOP {limit}'
        distinct = 'DISTINCT' if unique else ''
        to_pick = '*' if len(cols) == 0 else ','.join(cols)
        return f'''
        SELECT {distinct} {top_n} {to_pick}
        FROM {src}
        AS _
        '''

    # sep should be a raw string
    # user should have access to data_source
    @classmethod
    def load_from_file(cls, filename, data_source, sep=r'\t', **kwargs):
        src =  f'''OPENROWSET(
            BULK '{filename}',
            DATA_SOURCE = '{data_source}',
            FORMAT = 'csv',
            FIELDTERMINATOR = '{sep}',
            PARSER_VERSION = '2.0',
            HEADER_ROW=TRUE)'''

        query = cls._select_query_builder(src, **kwargs)
        logging.debug(query)
        return cls.execute_query(query)

    @classmethod
    def select(cls, table: str, **kwargs):

       '''
       :param table: could also be view
       '''
       query = cls._select_query_builder(table, **kwargs)
       logging.debug(query)
       return cls.execute_query(query)
