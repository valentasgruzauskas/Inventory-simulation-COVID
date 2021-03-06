import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import pandas.io.sql as sqlio
import numpy

class MyDatabase():

    def create_con(self, database_title, user="postgres", port = 5432, password = "admin", host = 'localhost'):
        self.conn = psycopg2.connect(database=database_title, user=user, port = port, password = password, host = host)
        self.cur = self.conn.cursor()

    def query(self, query):
        self.cur.execute(query)
        self.conn.commit()
    
    def table_to_df(self, table_name):
        sql = "SELECT * FROM " + table_name
        df = sqlio.read_sql_query(sql, self.conn)
        self.conn.commit()
        return df
        
    def select_where(self, query, values):
        self.cur.execute(query, values)
        df = self.cur.fetchall()
        self.conn.commit()

        return df

    def csv_to_table(self,file, table_name, sep):
        self.cur.copy_from(file, table_name, sep)
        self.conn.commit()
    
    def insert_to_table(self, query, values):
        self.cur.execute(query, values)
        self.conn.commit()
    
    def update_table_row(self, query, values):
        self.cur.execute(query, values)
        self.conn.commit()
            
    def commit(self):
        self.conn.commit()

    def close(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception:
            pass

#Register python specific types to allow saving to database
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)