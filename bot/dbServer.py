import os
import datetime
# import pyodbc
import mysql.connector
from mysql.connector import errorcode


class db_server:
    def __init__(self):
        self.connection = self
        self.cursor = self

    def get_sql_connection(self, server, db, trusted_connection, username="", password=""):
        try:

            if(trusted_connection):
                self.connection = pyodbc.connect(
                    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes;')
                print("Connection made local :(")
            else:
                self.connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                                                 server + ';DATABASE=' + db + ';UID = ' + username + ';PWD=' + password + ';')
                print("Connection made with secure username and password :(")

            self.cursor = self.connection.cursor()

        except Exception as ex:
            print(ex)
            self.cursor.close()

    def get_mysql_connection(self, server, db, username="", password=""):
        try:
            self.connection = mysql.connector.connect(
                user=username, password=password, host=server, database=db)
            self.cursor = self.connection.cursor()
        except Exception as ex:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            self.conneciton.close()

    def insert_rows(self, tablename, fields=[], data=[]):
        try:
            rows_fields = ','.join(fields)
            for item in data:
                print(f"Inserting: {item}")
                caractertArray = []
                for itemy in range(len(item)):
                    caractertArray.append("%s")

                query = "insert into {0} ({1}) values ({2});".format(
                    tablename, rows_fields, ','.join(caractertArray))
                self.cursor.execute(query, item)
                self.connection.commit()
                print(f"Inserted good...")

        except Exception as ex:
            print(ex)

    def get_rows_table(self, table):
        query = f"""select * from {table};"""
        rows = self.cursor.execute(query)
        self.connection.commit()
        for row in rows:
            print(row)

    def get_row_by_id(self, table, id):
        query = f"""select * from {table} where id='{id}'; """
        rows = self.cursor.execute(query)
        self.connection.commit()
        for row in rows:
            print(row)

    def get_profile_id_by_product(self, table, product):
        try:
            query = f"""select * from {table} where Product='{product}'; """
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            if(len(rows) == 0):
                return 0
            for row in rows:
                return row[0]
        except Exception as ex:
            print(ex)
            return 0

    def get_rows_by_First_Last_Name(self, table, first_name, last_name):
        query = f"""select * from {table} where FirstName='{first_name}' and LastName = '{last_name}'; """
        rows = self.cursor.execute(query)
        self.connection.commit()
        for row in rows:
            print(row)

    def get_data_query(self, str_query):
        try:
            query = f"""{str_query}"""
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            if(len(rows) == 0):
                return 0
            for row in rows:
                print(row)
                return row
        except Exception as ex:
            print("Error in get data quert function in db_server")
            print(ex)
            return 0

    def close_connection(self):
        self.cursor.close()
