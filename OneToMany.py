# Helper base class for mapping an object to several children.


class OneToMany:
    

    def __init__(self, db, table):
        """
        Constructor.
        """
        self.db = db
        self.table = table


    @staticmethod
    def define_table(c):
        """
        Define the database table for this object.
        """
        c.add_id()
        c.ac('objid', int, null=False)
        c.ac('facilitatorid', int, null=False)


    @classmethod
    def getall(cls, db, facilitatorid):
        """
        Returns all "many" objects in a one-to-many relation.
        """
        q = f'SELECT * FROM `{obj._table}` (SELECT objid FROM `{cls._table}` WHERE facilitator = {facilitatorid}) AS obj WHERE id = obj.objid'
        return db.getmany(cls._many, q)


    @classmethod
    def map(cls, db, obj, facilitator):
        """
        Maps the given object (many) to the given facilitator (one).
        If either the object or facilitator does not have an ID, they are
        first saved.
        """
        if obj.id is None:
            obj.save()
        if facilitator.id is None:
            facilitator.save()

        db.execute(f"INSERT INTO `{cls._table}` (objid, facilitatorid) VALUES ({obj.id}, {facilitator.id})")


    @classmethod
    def unmap(cls, db, obj, facilitator):
        """
        Removes one MealProduct connecting 'obj' (many) to 'facilitator' (one).
        """
        db.execute(f"DELETE FROM `{cls._table}` WHERE id = (SELECT MAX(id) FROM `{cls._table}` WHERE objid = {obj.id} AND facilitatorid = {facilitator.id})")
        
    
