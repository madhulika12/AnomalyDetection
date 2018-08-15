# -*- coding: utf-8 -*-
"""
Created on Sat Aug  4 19:10:56 2018

@author: madhu.hanlin
"""

import pandas as pd
import pyodbc
from configparser import ConfigParser


class ConnectToDB(object):

    def __init__(self, server_name, db_name, user_name, pwd):
        self.server_name = server_name
        self.db_name = db_name
        self.user_name = user_name
        self.pwd = pwd

        self._db_cur = self.connect_using_sql_account().cursor()

    def query(self, sent_query, params):
        return self._db_cur.execute(sent_query, params)

    def connect_using_sql_account(self):
        return pyodbc.connect(
            r'DRIVER={SQL Server};'
            r'SERVER=%s;'
            r'DATABASE=%s;'
            r'UID=%s;'
            r'PWD=%s'
            % (self.server_name, self.db_name, self.user_name, self.pwd))

    def __del__(self):
        print("Inside delete method")
        self.connect_using_sql_account().close()

    def connect_using_win_auth(self):
        return pyodbc.connect(
            r'DRIVER={SQL Server};'
            r'SERVER=%s;'
            r'DATABASE=%s;'
            r'Trusted_Connection=yes;'
            % (self.server_name, self.db_name))

# Testing the query function
# sql = db.query('select * from memberalert where memberid= ?', 27261628019507208)

# Testing connection closed
# db.__del__()
def configure():
    parser = ConfigParser()
    parser.read('appsettings.Dev.ini')
    
    server = parser.get('db','server')
    database = parser.get('db','database')
    user = parser.get('db','user')
    password = parser.get('db','password')
    query = parser.get('db','query')

configure()  

db = ConnectToDB(server, database, user, password)

df = pd.read_sql(query, db.connect_using_sql_account())
print(df.head())
