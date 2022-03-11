# Base class for creating databases


class DatabaseCreator:


    def __init__(self):
        """
        Constructor.
        """
        self.columns = []


    def ac(self, *args, **kwargs):
        """
        Alias for 'add_column()'.
        """
        self.add_column(*args, **kwargs)


    def add_column(self, name, type, primary=False, null=True):
        """
        Define a new column in this table.
        """
        raise DatabaseException("The 'add_column()' method has not been implemented for this database creator.")


    def add_id(self):
        """
        Add a integer column 'id' which is set as primary key and non-nullable.
        """
        self.add_column('id', int, primary=True, null=False)


    def clear(self):
        """
        Clear this creator and prepare to create a new table.
        """
        self.columns = []
    

    def compile(self, table):
        """
        Compile the defined table into an SQL script.
        """
        raise DatabaseException("The 'compile()' method has not been implemented for this database creator.")


