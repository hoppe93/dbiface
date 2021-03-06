# Database management routines


class Database:
    

    def __init__(self, connection):
        """
        Constructor.
        """
        self.connection = connection
        self.cursor = self.connection.cursor()


    def __enter__(self):
        """
        Called when entering a "with" statement.
        """
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        """
        Called when leaving a "with" statement.
        """
        self.close()
        return (exc_type is None)


    def close(self):
        """
        Close the database connection.
        """
        self.connection.commit()
        self.connection.close()


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
        try:
            if replace is None:
                c = self.cursor.execute(sql)
            else:
                c = self.cursor.execute(sql, replace)

            if commit:
                self.connection.commit()
        except Exception as ex:
            print(f'{sql}')
            raise ex

        return [r for r in c]


    def executescript(self, sql):
        """
        Execute an SQL script (one or more statements).
        """
        try:
            v = self.cursor.executescript(sql)
        except Exception as ex:
            print(f'{sql}')
            raise ex

        return v


    def getall(self, ttype):
        """
        Return all objects of the specified type in the database.
        """
        q = f'SELECT * FROM `{ttype._table}`'
        return [ttype(self, _row=r) for r in self.execute(q)]


    def getcount(self, ttype, **kwargs):
        """
        Returns the number of entries of the given type. Keyword arguments
        can be specified to give conditions for the rows to select.
        """
        sql = f"SELECT COUNT(*) FROM `{ttype._table}`"
        if len(kwargs) > 0:
            sql += "WHERE "
            for k in kwargs.keys():
                sql += f"{k} = :{k} AND "
            sql = sql[:-5]

        nrows = self.execute(sql, kwargs)[0][0]
        return nrows


    def getlist(self, ttype, first=None, limit=20, sort='DESC', offset=0, where=None):
        """
        Return a list of objects of the specified type, limiting the
        result to 'limit' entries. If 'first' is given, the first
        entry will have less (greater) than or equal to this ID if
        'sort' is DESC (ASC).
        """
        d = {'limit':limit}

        if sort.lower() == 'asc':
            sortorder = 'ASC'
        else:
            sortorder = 'DESC'

        q = f'SELECT * FROM `{ttype._table}` '
        _where = []
        _offset = ''
        if first:
            w = 'id '
            if sort == 'DESC':
                w += f'<= :first'
            else:
                w += f'>= :first'
            _where.append(w)
            d['first'] = first
        elif offset > 0:
            _offset = ' OFFSET :offset'
            d['offset'] = offset

        if where or _where:
            q += ' WHERE '

            for w in _where:
                q += f'{w} AND '

            for w in where:
                q += f'{w} = :{w} AND '

            # Remove final ' AND '
            q = q[:-5]
            d = {**d, **where}

        q += f' ORDER BY `id` {sortorder} LIMIT :limit '+_offset

        l = self.getmany(ttype, q, d)
        return l


    def getmany(self, ttype, sql, params=None):
        """
        Return all objects of the specified type in the database.
        """
        return [ttype(self, _row=r) for r in self.execute(sql, params)]


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


    def hasid(self, table, id):
        """
        Check if the given table contains a row with id 'id'.
        """
        return self.hasrow(table=table, id=id)


    def hasrow(self, table, **kwargs):
        """
        Check if the given table contains one or more rows with the given
        column values.
        """
        raise DatabaseException("The method 'hasid()' has not been implemented for this database.")


