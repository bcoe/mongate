import unittest
from mongate.connection import Connection, ConnectionError

class TestConnection(unittest.TestCase):
    def setUp(self):
        pass
    
    def test_connection_initialized_properly(self):
        connection = Connection('localhost', 27080)
        self.assertEqual(27080, connection.get_port())
        self.assertEqual('localhost', connection.get_host())
        
    def test_connection_should_return_db_when_array_access_used(self):
        connection = Connection('localhost', '27080')
        db = connection['foo']
        self.assertEqual('foo', db.get_name())
        
    def test_connection_should_return_db_when_attribute_access_used(self):
        connection = Connection('localhost', '27080')
        db = connection.foo
        self.assertEqual('foo', db.get_name())

    def test_connect_to_mongo_with_invalid_host(self):
        connection = Connection('localhostgggg', 27080)
        
        error_occurred = False
        try:
            connection.connect_to_mongo(host='localhost', port=27017)
        except ConnectionError:
            error_occurred = True
        
        self.assertTrue(error_occurred)
        
    def test_connect_to_mongo_with_valid_info(self):
        connection = Connection('localhost', 27080)
        self.assertTrue(connection.connect_to_mongo(host='localhost', port=27017))
        
if __name__ == "__main__":
    unittest.main()
