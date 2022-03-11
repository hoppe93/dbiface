

import sqlite3
from datetime import datetime

from . import Database, DatabaseCreator, DatabaseException


class SqliteDatabase(Database):
    

    def __init__(self, database):
        """
        Constructor.
        """
        connection = sqlite3.connect(database)

        super().__init__(connection=connection)


    def getcolumns(self, table):
        """
        Returns a list of columns for the named table.
        """
        self.cursor.execute(f'SELECT * FROM {table} WHERE 1=0')
        return [c[0] for c in self.cursor.description]


    def getcreator(self):
        """
        Returns a new instance of the database creator associated with
        this database type.
        """
        return SqliteDatabaseCreator()



class SqliteDatabaseCreator(DatabaseCreator):


    def __init__(self):
        """
        Constructor.
        """
        super().__init__()


    def add_column(self, name, ttype, primary=False, null=True):
        """
        Add a column to this creator.
        """
        sql = f"{name} "

        if type(ttype) == str:
            sql += ttype
        else:
            if ttype == str:
                sql += "TEXT"
            elif ttype == int:
                sql += "INTEGER"
            elif ttype == float:
                sql += "REAL"
            elif ttype == datetime:
                sql += "TEXT"
            else:
                raise DatabaseException(f"Unrecognized Python data type: {str(ttype)}. Cannot convert to Sqlite data type.")

        if not null:
            sql += " NOT NULL"

        if primary:
            sql += " PRIMARY KEY"
        
        self.columns.append(sql)


    def compile(self, table):
        """
        Compile the defined table to an Sqlite script.
        """
        sql = f"CREATE TABLE IF NOT EXISTS `{table}` ("
        
        for col in self.columns:
            sql += f"{col},\n"

        sql = sql[:-2] + ");"
        
        return sql


