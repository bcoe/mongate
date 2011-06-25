import unittest
from mongate.connection import Connection, ConnectionError
from tests import SLEEPY_HOST, SLEEPY_PORT, MONGO_HOST, MONGO_PORT

class TestConnection(unittest.TestCase):
    def setUp(self):
        pass
    
    def test_connection_initialized_properly(self):
        connection = Connection(SLEEPY_HOST, SLEEPY_PORT)
        self.assertEqual(SLEEPY_PORT, connection.get_port())
        self.assertEqual(SLEEPY_HOST, connection.get_host())
        
    def test_connection_should_return_db_when_array_access_used(self):
        connection = Connection(SLEEPY_HOST, SLEEPY_PORT)
        db = connection['foo']
        self.assertEqual('foo', db.get_name())
        
    def test_connection_should_return_db_when_attribute_access_used(self):
        connection = Connection(SLEEPY_HOST, SLEEPY_PORT)
        db = connection.foo
        self.assertEqual('foo', db.get_name())

    def test_connect_to_mongo_with_invalid_host(self):
        connection = Connection('localhostgggg', 27080)
        
        error_occurred = False
        try:
            connection.connect_to_mongo(host=MONGO_HOST, port=MONGO_PORT)
        except ConnectionError:
            error_occurred = True
        
        self.assertTrue(error_occurred)
        
    def test_connect_to_mongo_with_valid_info(self):
        connection = Connection(SLEEPY_HOST, SLEEPY_PORT)
        self.assertTrue(connection.connect_to_mongo(host=MONGO_HOST, port=MONGO_PORT))
        
if __name__ == "__main__":
    unittest.main()
