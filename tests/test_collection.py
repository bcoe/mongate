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
        self._reset_testing_db()

    def _reset_testing_db(self):
        self.collection.remove()
        
    def test_collection_insert(self):
        oid = self._insert_data()
        self.assertTrue(oid)
        
    def test_collection_insert_with_invalid_characters(self):
        oid = self.collection.insert({
            'name': 'Benjamin & Company=bar',
            'profession': 'Software Developer?'
        })
        self.assertTrue(oid)
        
    def _insert_data(self):
        self.collection.insert({
            'name': 'Benjamin',
            'age': 27,
            'profession': 'Software Developer'
        })
        
        return self.collection.insert({
            'apple': 'tasty',
            'banana': 'phallic'
        })
        
    def test_collection_find_by_oid(self):
        oid = self._insert_data()
    
        retrieved_collection = self.collection.find({
            '_id': oid
        })
        
        self.assertEqual('tasty', retrieved_collection[0]['apple'])
        
    def test_find_with_no_results(self):
        self.assertFalse(self.collection.find_one({'Elvis': True}))
        
    def test_find_with_invalid_characters(self):
        self.assertFalse(self.collection.find({'Elvis & Company': True}))
        self.assertFalse(self.collection.find_one({'Elvis & Company': True}))
        
    def test_collection_find_by_key(self):
        self._insert_data()
    
        retrieved_collection = self.collection.find({
            'apple': 'tasty'
        })
        
        self.assertEqual('tasty', retrieved_collection[0]['apple'])

    def test_collection_find_with_invalid_characters(self):
        oid = self.collection.insert({
            'name': 'Benjamin & Company',
            'profession': 'Software Developer?'
        })
        
        retrieved_collection = self.collection.find({
            'name': 'Benjamin & Company'
        })
        
        self.assertEqual('Software Developer?', retrieved_collection[0]['profession'])

    def test_collection_find_with_nonoid_id(self):
        self.collection.insert({
            '_id' : 100,
            'name' : 'Benjamin',
            'profession' : 'Software Developer'
        })
        
        retrieved_collection = self.collection.find({
            'name': 'Benjamin'
        })
        
        self.assertEqual(100, retrieved_collection[0]['_id'])
        
    def test_collection_find_key_that_doesnt_exist(self):
        self._insert_data()
    
        retrieved_collection = self.collection.find({
            'apple': 'foobar'
        })
        
        self.assertFalse(retrieved_collection)
        
    def test_remove_collection(self):
        self._insert_data()
        
        self.collection.remove({
            'apple': 'tasty'
        })

        retrieved_collection = self.collection.find({
            'apple': 'tasty'
        })
        
        self.assertFalse(retrieved_collection)
        
    def test_remove_only_removes_the_right_things(self):
        self._insert_data()
        
        self.collection.remove({
            'apple': 'tasty'
        })
        
        retrieved_collection = self.collection.find({
            'name': 'Benjamin'
        })
        
        self.assertTrue(retrieved_collection)
        
    def test_remove_with_no_criteria_removes_whole_collection(self):
        self._insert_data()
        
        self.collection.remove()
        
        retrieved_collection = self.collection.find({
            'name': 'Benjamin'
        })
        
        self.assertFalse(retrieved_collection)
        
    def test_update_with_key(self):
        self._insert_data()
        
        self.collection.update(
            {
                'name': 'Benjamin'
            },
            {
                "$inc": {
                    'age': 1
                }
            }
        )
        
        retrieved_collection = self.collection.find({
            'name': 'Benjamin'
        })
        
        self.assertEqual(28, retrieved_collection[0]['age'])
        self.assertEqual(1, len(retrieved_collection))
        
    def test_update_with_invalid_characters(self):
        self._insert_data()
        
        self.collection.update(
            {
                'name': 'Benjamin',
                'invalid': '&&&'
            },
            {
                "$inc": {
                    'age': 1
                }
            }
        )
        
        retrieved_collection = self.collection.find({
            'name': 'Benjamin'
        })
        
        self.assertEqual(27, retrieved_collection[0]['age'])
        
    def test_save_updates_document_if_it_already_exists(self):
        self._insert_data()
        
        retrieved_collection = self.collection.find({
            'name': 'Benjamin'
        })
        
        retrieved_collection[0]['name'] = 'Ben'
        self.collection.save(retrieved_collection[0])
        
        retrieved_collection = self.collection.find({
            'age': 27
        })

        self.assertEqual('Ben', retrieved_collection[0]['name'])
        self.assertEqual(1, len(retrieved_collection))
        
    def test_save_creates_document_if_it_does_not_exist(self):
        self.collection.save({
            'name': 'Bob',
            'age': 'old'
        })
        
        retrieved_collection = self.collection.find_one({
            'age': 'old'
        })
        
        self.assertEqual('Bob', retrieved_collection['name'])
        
    def test_count_with_zero_results(self):
        count = self.collection.count({'foo': 'bar'})
        self.assertEqual(0, count)
        
    def test_count_with_zero_results(self):
        self.collection.save({
            'name': 'Bob',
            'age': 'old'
        })
        self.collection.save({
            'name': 'Bob',
            'age': 'old'
        })
        self.collection.save({
            'name': 'Bob',
            'age': 'old'
        })
        
        count = self.collection.count({'name': 'Bob'})
        self.assertEqual(3, count)
            
if __name__ == "__main__":
    unittest.main()
