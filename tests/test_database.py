import unittest
from mongate.connection import Connection
from mongate.database import Database
from tests import SLEEPY_HOST, SLEEPY_PORT

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(SLEEPY_HOST, SLEEPY_PORT)
        
    def test_db_should_return_collection_when_array_access_used(self):
        db = self.connection['foo']
        collection = db['bar']
        self.assertEqual('bar', collection.name)
        
    def test_db_should_return_collection_when_attribute_access_used(self):
        db = self.connection.foo
        collection = db.bar
        self.assertEqual('bar', collection.name)
        
    def test_drop_collection(self):
        db = self.connection.foo
        collection = db.test_collection
        
        collection.insert({
            'name': 'Benjamin & Company',
            'profession': 'Software Developer?'
        })
        
        db.drop_collection('test_collection')
        
        retrieved_collection = collection.find({
            'name': 'Benjamin & Company'
        })
        
        self.assertFalse(retrieved_collection)
        
if __name__ == "__main__":
    unittest.main()
