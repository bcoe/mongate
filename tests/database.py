import unittest
from mongate.connection import Connection
from mongate.database import Database

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection('localhost', '27080')
        
    def test_db_should_return_collection_when_array_access_used(self):
        db = self.connection['foo']
        collection = db['bar']
        self.assertEqual('bar', collection.name)
        
    def test_db_should_return_collection_when_attribute_access_used(self):
        db = self.connection.foo
        collection = db.bar
        self.assertEqual('bar', collection.name)
        
if __name__ == "__main__":
    unittest.main()
