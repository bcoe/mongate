from mongate.collection import Collection
import simplejson as json
import urllib

class BatchError(Exception):
    pass
    
class AddError(Exception):
    pass

class Batch(Collection):
    """
    Perform a batch of operations on sleepy.mongoose.
    """
    
    batch_action = '_batch'
    modifying_action = '/insert/update/remove'
    
    def __init__(self, collection, connection):
        self.name = collection.get_name()
        self.connection = connection
        self.collection = collection
        self.database = connection.database
        self.requests = []
        self.batch_type = 'unknown'

    def add_insert(self, document):
        
        self._check_for_modify_exception()
        self.batch_type = self.modifying_action
        
        self.requests.append({
            'method': 'POST',
            'db': self.collection.database.get_name(),
            'collection': self.get_name(),
            'cmd': '_insert',
            'args': {
                'docs': "[%s]" % json.dumps(document)
            }
        })
    
    def _check_for_modify_exception(self):
        if self.batch_type == 'find':
            raise AddError("""
            You cannot add an insert or update operation
            to a batch with find operations in it.
            """)
        
    def add_update(self, criteria, document):
        self._check_for_modify_exception()
        self.batch_type = self.modifying_action
        
        self.requests.append({
            'method': 'POST',
            'db': self.collection.database.get_name(),
            'collection': self.get_name(),
            'cmd': '_update',
            'args': {
                'criteria': json.dumps(criteria),
                'newobj': json.dumps(document)
            }
        })
        
    def add_find(self, criteria):
        
        criteria = self._replace_id_with_object(criteria)
        self._check_for_find_exception()
        self.batch_type = 'find'
        
        self.requests.append({
            'method': 'GET',
            'db': self.collection.database.get_name(),
            'collection': self.get_name(),
            'cmd': '_find',
            'args': {
                'criteria': [json.dumps(criteria)],
                'batch_size': [99999999]
            }
        })
        
    def _check_for_find_exception(self):
        if self.batch_type == self.modifying_action:
            raise AddError("""
            You cannot add a find operation to a
            batch with find an insert or update operation.
            """)
            
    def add_remove(self, criteria):
        
        criteria = self._replace_id_with_object(criteria)
        self._check_for_modify_exception()
        self.batch_type = self.modifying_action
        
        self.requests.append({
            'method': 'POST',
            'db': self.collection.database.get_name(),
            'collection': self.get_name(),
            'cmd': '_remove',
            'args': {
                'criteria': json.dumps(criteria)
            }
        })
        
    def execute(self):
        """
        Insert a new document into MongoDB the 
        object id is returned.
        """
        url = self._create_url(self.batch_action)
        payload = self._create_batch_payload()
        response_object = self.connection.perform_request(
            url,
            payload,
            'POST',
            raise_error=self._raise_batch_error
        )
        return response_object
        
    def _create_batch_payload(self):
        return "requests=%s" % urllib.quote( json.dumps(self.requests) )
        
    def find(self):
        results = []
        response_object = self.execute()
        for response in response_object:
            results.append(self._process_response_object(response))
        return results
        
    def _raise_batch_error(self, exception_string=''):
        raise BatchError("""
        An error occurred while performing a
        batch operation. Because [%s]
        """ % (
            str(exception_string)
        ))
        