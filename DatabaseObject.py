# Base class for objects stored in database.

import sqlite3
from .DatabaseException import DatabaseException


class DatabaseObject:
    

    def __init__(self, db, id=None, _row=None, **kwargs):
        """
        Constructor.
        """
        self._db = db

        self._cols = self._db.getcolumns(self._table)

        for col in self._cols:
            setattr(self, col, None)

        # Load data for object?
        if id is not None and len(kwargs) == 0:
            self.get(id=id)
        elif len(kwargs) > 0:
            for col in self._cols:
                if col == 'id':
                    self.id = id
                elif col in kwargs:
                    setattr(self, col, kwargs[col])
        elif _row is not None:
            self.populate(_row)


    def create_table(self):
        """
        Create the table for this object.
        """
        self._db.executescript(self._sql)


    def delete(self):
        """
        Delete this object from the database.
        """
        self._db.execute(f'DELETE FROM `{self._table}` WHERE id = :id', {'id':self.id}, commit=True)


    @classmethod
    def deleteAll(cls, db, **kwargs):
        """
        Delete all objects for which the given list of keywords match.
        """
        sql = f'DELETE FROM `{cls._table}` WHERE '
        for k in kwargs:
            sql += f'{k} = :{k} AND '

        sql = sql[:-5]
        return db.execute(sql, kwargs)


    def get(self, limit=None, order=None, orderorder='DESC', **kwargs):
        """
        Load data from the table of this object and populate
        this object with the returned data.
        """
        if len(kwargs) == 0:
            raise DatabaseException("No selection arguments specified. Unable to load object.")

        q = f'SELECT * FROM {self._table} WHERE '
        for key in kwargs.keys():
            q += f"`{key}` = :{key} AND "

        # Remove final ' AND '
        q = q[:-5]

        # Order result?
        if order is not None:
            q += f" ORDER BY {order}"
        # Limit number of results?
        if limit is not None:
            q += f" LIMIT {limit}"

        rows = [r for r in self._db.execute(q, kwargs)]

        if len(rows) == 0:
            raise DatabaseException(f"Query returned no results. kwargs: {kwargs}")
        elif len(rows) > 1:
            raise DatabaseException(f"Query returned more than one result. kwargs: {kwargs}")
        else:
            self.populate(rows[0])

        return self


    def populate(self, row):
        """
        Populate the fields of this object with the given row.

        :param row: Result row from database query.
        """
        for col, val in zip(self._cols, row):
            setattr(self, col, val)


    def save(self, subset=None):
        """
        Save fields of this object to the database. If ``subset`` is a list
        of strings, only the fields listed in ``subset`` will be saved.
        """
        if subset is None:
            subset = self._cols

        if self.id != None:
            if self._db.hasid(table=self._table, id=self.id):
                self._update(subset)
            else:
                self._insert(subset, force_insert=True)
        else:
            self._insert(subset)


    def select(self, where):
        """
        Run a 'getmany' operation on the database with the given string
        as the 'WHERE' clause.
        """
        return self._db.getmany(f"SELECT * FROM `{self._table}` WHERE {where}")


    def todict(self, subset=None):
        """
        Return the data of this object as a dict.
        """
        if subset is None:
            subset = self._cols

        v = {}
        for col in subset:
            v[col] = getattr(self, col)

        return v


    def _insert(self, subset, force_insert=False):
        """
        Issue an SQL INSERT INTO query for this object.
        """
        q = f'INSERT INTO `{self._table}` '
        p, v = '', ''
        for col in subset:
            if col == 'id' and not force_insert: continue

            p += f'`{col}`, '
            v += f':{col}, '

        p = '('+p[:-2]+')'
        v = '('+v[:-2]+')'

        sql = q+p+' VALUES '+v
        self._db.execute(sql, self.todict(subset), commit=True)
        self.id = self._db.getlastid()


    def _update(self, subset):
        """
        Issue an SQL UPDATE query for this object.
        """
        q = f'UPDATE `{self._table}` SET '
        for col in subset:
            if col == 'id': continue

            q += f'`{col}` = :{col}, '

        q = q[:-2]
        q += ' WHERE id = :id'

        if 'id' not in subset:
            subset.append('id')

        self._db.execute(q, self.todict(subset=subset), commit=True)


