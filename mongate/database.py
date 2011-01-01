class Database(object):
    
    def __init__(self, name, connection):
        self.__name = name
        self.__connection = connection
        
    def get_name(self):
        return self.__name