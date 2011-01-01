"""
A database within MongoDB.
"""

from mongate.collection import Collection

class Database(object):
    """
    A database within MongoDB.
    """
    
    def __init__(self, name, connection):
        self.__name = name
        self.__connection = connection
        
    def get_name(self):
        return self.__name
        
    def __getitem__(self, key):
        return self._return_collection(key)
        
    def __getattr__(self, key):
        return self._return_collection(key)
        
    def _return_collection(self, key):
        return Collection(key, self.__connection, self)