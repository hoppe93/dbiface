# Database management routines


class Database:
    

    def __init__(self, connection):
        """
        Constructor.
        """
        self.connection = connection
        self.cursor = self.connection.cursor()


    def create_tables(self, *objs):
        """
        Create tables for the given classes.
        """
        c = self.getcreator()
        for obj in objs:
            obj.define_table(c)
            self.executescript(c.compile(obj._table))
            c.clear()


    def execute(self, sql, replace=None, commit=False):
        """
        Execute a single SQL command.
        """
        if replace is None:
            c = self.cursor.execute(sql)
        else:
            c = self.cursor.execute(sql, replace)

        if commit:
            self.connection.commit()

        return c


    def executescript(self, sql):
        """
        Execute an SQL script (one or more statements).
        """
        return self.cursor.executescript(sql)


    def getall(self, ttype):
        """
        Return all objects of the specified type in the database.
        """
        q = f'SELECT * FROM `{ttype._table}`'
        return [ttype(self, _row=r) for r in self.execute(q)]


    def getmany(self, ttype, sql):
        """
        Return all objects of the specified type in the database.
        """
        return [ttype(self, _row=r) for r in self.execute(sql)]


    def getcolumns(self, table):
        """
        Returns a list of columns of the named table.
        """
        raise Exception("The method 'getcolumns()' has not been implemented for this database.")
        #return self.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = `{table}`")


    def getlastid(self):
        """
        Returns the ID of the last inserted row.
        """
        return self.cursor.lastrowid


