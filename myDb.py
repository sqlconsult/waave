import mysql.connector

class myDb:
    def __init__(self):
        self.user='root'
        self.password='root'
        self.database='my_db'
        self.cxn = None


    def cxn_open(self):
        # open connection to d/b
        self.cxn = mysql.connector.connect(
            user=self.user, password=self.password, database=self.database)
        return self.cxn


    def cxn_close(self):
        # close connection to d/b
        self.cxn.close()


    def exec_qry(self, sql):
        # execute query that doesn't return results (insert, update or delete)
        cursor = self.cxn.cursor()
        cursor.execute(sql)
        cursor.close


    def exec_many_qry(self, sql, tuple_list):
        # execute query that doesn't return results (insert, update or delete)
        cursor = self.cxn.cursor()
        cursor.executemany(sql, tuple_list)
        affected_rows = cursor.rowcount
        cursor.close
        return affected_rows