from mongate.database import Database

class Connection(object):
    
    def __init__(self, host='localhost', port=27080):
        self.__host = host
        self.__port = port
        
    def get_host(self):
        return self.__host
        
    def get_port(self):
        return self.__port
        
    def __getitem__(self, key):
        return self._return_database(key)
        
    def __getattr__(self, key):
        return self._return_database(key)
        
    def _return_database(self, key):
        return Database(key, self)
