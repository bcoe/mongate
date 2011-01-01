import unittest
from mongate.connection import Connection

class TestConnection(unittest.TestCase):
    def setUp(self):
        pass
    
    def test_connection_string_initialized_properly(self):
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
                
if __name__ == "__main__":
    unittest.main()
