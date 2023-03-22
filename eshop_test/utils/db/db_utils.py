# Instructions:
# Download https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16
import pyodbc


class MSSQLConnector:
    def __init__(self, database='OrderingDb'):
        self.SERVER = '127.0.0.1,5433'
        self.USER = 'sa'
        self.PASSWORD = 'Pass@word'
        self.DATABASE = f'Microsoft.eShopOnContainers.Services.{database}'
        self.DRIVER = '{ODBC Driver 18 for SQL Server}'
        self.connection_str = f"Driver={self.DRIVER};Server={self.SERVER};Database={self.DATABASE};UID={self.USER};PWD={self.PASSWORD};TrustServerCertificate=yes"
        self.conn = None

    def __enter__(self):
        self.conn = pyodbc.connect(self.connection_str)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def select_query(self, query):
        """Executes a select query on the database and returns a list of dictionaries per row"""
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results

    def close(self):
        self.conn.close()




if __name__ == '__main__':
    import pprint
    with MSSQLConnector('CatalogDb') as conn:
        #pprint.pprint(conn.select_query('SELECT * from ordering.orders'))
        #result=conn.select_query('SELECT MAX(Id) FROM ordering.orders')
        #result=conn.select_query('SELECT COUNT(Id) from ordering.orders')
        #orderstatus = conn.select_query('select OrderStatusId from ordering.orders where Id = (select max(id) from ordering.orders)')
        #productid=1
        startingstock =conn.select_query('SELECT AvailableStock from dbo.Catalog where Id =1')
        print(startingstock[0]['AvailableStock'])