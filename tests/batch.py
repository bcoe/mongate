import unittest
from mongate.batch import Batch
from mongate.collection import Collection
from mongate.connection import Connection
from tests import SLEEPY_HOST, SLEEPY_PORT, MONGO_HOST, MONGO_PORT

class TestCollection(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(SLEEPY_HOST, SLEEPY_PORT)
        self.connection.connect_to_mongo(host=MONGO_HOST, port=MONGO_PORT)
        self.db = self.connection.mongate_test_db
        self.collection = self.db.test_collection
        self._reset_testing_db()

    def _reset_testing_db(self):
        self.collection.remove()
        
    def test_batch_insert(self):
        
        self._perform_batch_insertion()        
        
        retrieved_document_1, retrieved_document_2 = self._retrieve_documents()
        
        self.assertEqual(2, retrieved_document_1['bar']) 
        self.assertEqual('apple', retrieved_document_2['banana'])
        
    def _perform_batch_insertion(self):
        batch = Batch(self.collection, self.connection)
        
        batch.add_insert({
            'batch_insert_1': 3,
            'bar': 2
        })
        
        batch.add_insert({
            'batch_insert_2': 'banana',
            'banana': 'apple'
        })
        
        batch.add_insert({
            'batch_insert_3': 29,
            'bar': 2
        })
        
        batch.execute()
        return batch
        
    def _retrieve_documents(self):
        retrieved_document_1 = self.collection.find_one({'batch_insert_1': 3})
        retrieved_document_2 = self.collection.find_one({'batch_insert_2': 'banana'})
        return retrieved_document_1, retrieved_document_2
        
    def test_batch_update(self):
        self._perform_batch_insertion()
        
        batch = Batch(self.collection, self.connection)
        
        batch.add_update(
            {
                'batch_insert_1': 3
            },
            {
                "$inc": {
                    "bar": 1
                }
            }
        )
        
        batch.add_update(
            {
                'batch_insert_2': 'banana'
            },
            {
                '$set': {
                    'banana': 'phalic'
                }
            }
        )
        
        batch.execute()
        
        retrieved_document_1, retrieved_document_2 = self._retrieve_documents()
        
        self.assertEqual(3, retrieved_document_1['bar']) 
        self.assertEqual('phalic', retrieved_document_2['banana'])
        
    def test_batch_find(self):
        self._perform_batch_insertion()
        batch = Batch(self.collection, self.connection)
        
        batch.add_find({
            'bar': 2
        })
        
        batch.add_find({
            'batch_insert_2': 'banana'
        })
        
        result = batch.find()
        self.assertEqual(2, len(result))
        self.assertEqual(2, len(result[0]))
        self.assertEqual('apple', result[1][0]['banana'])
        
    def test_batch_find_with_no_results(self):
        self._perform_batch_insertion()
        batch = Batch(self.collection, self.connection)
        
        batch.add_find({
            'bar': 23434
        })
        
        batch.add_find({
            'batch_insert_2': 'abananasdf'
        })
        
        result = batch.find()
        self.assertFalse(result[0])
        self.assertFalse(result[1])
        
    def test_batch_find_with_no_jobs(self):
        self._perform_batch_insertion()
        batch = Batch(self.collection, self.connection)        
        result = batch.find()
        self.assertFalse(result)
                    
if __name__ == "__main__":
    unittest.main()
