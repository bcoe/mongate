import unittest
from mongate.collection import Collection
from mongate.connection import Connection
from tests import SLEEPY_HOST, SLEEPY_PORT, MONGO_HOST, MONGO_PORT

class TestCollection(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(SLEEPY_HOST, SLEEPY_PORT)
        self.connection.connect_to_mongo(host=MONGO_HOST, port=MONGO_PORT)
        self.db = self.connection.mongate_test_db
        self.collection = self.db.test_collection

    def _reset_testing_db(self):
        pass
        
    def test_collection_insert(self):
        oid = self._insert_data()
        self.assertTrue(oid)
        
    def _insert_data(self):
        return self.collection.insert({
            'apple': 'tasty',
            'banana': 'phallic'
        })
        
    def test_collection_find_by_oid(self):
        oid = self._insert_data()
    
        retrieved_collection = self.collection.find({
            '_id': oid
        })
        
        self.assertEqual('tasty', retrieved_collection['apple'])
        
    def test_collection_find_by_key(self):
        self._insert_data()
    
        retrieved_collection = self.collection.find({
            'apple': 'tasty'
        })
        
        self.assertEqual('tasty', retrieved_collection['apple'])
        
    def test_collection_find_key_that_doesnt_exist(self):
        self._insert_data()
    
        retrieved_collection = self.collection.find({
            'apple': 'foobar'
        })
        
        self.assertFalse(retrieved_collection)
        
if __name__ == "__main__":
    unittest.main()
