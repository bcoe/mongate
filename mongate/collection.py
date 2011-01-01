"""
Instances of individual collection objects within MongoDB.
"""

class Collection(object):
    """
    Instances of individual collection objects within MongoDB.
    """
    
    def __init__(self, name, connection, database):
        self.name = name
        self.database = database
        self.connection = connection