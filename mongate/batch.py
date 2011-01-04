from mongate.collection import Collection
import simplejson as json

class BatchError(Exception):
    pass

class Batch(Collection):
    """
    Perform a batch of operations on sleepy.mongoose.
    """
    
    batch_action = '_batch'
    
    def __init__(self, collection, connection):
        self.name = collection.get_name()
        self.connection = connection
        self.collection = collection
        self.database = connection.database
        self.requests = []

    def add_insert(self, document):
        self.requests.append({
            'method': 'POST',
            'db': self.collection.database.get_name(),
            'collection': self.get_name(),
            'cmd': '_insert',
            'args': {
                'docs': "[%s]" % json.dumps(document)
            }
        })
        
    def add_update(self, criteria, document):
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
        
    def _create_batch_payload(self):
        return "requests=%s" % json.dumps(self.requests)
        
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
        
    def _raise_batch_error(self, exception_string=''):
        raise BatchError("""
        An error occurred while performing a
        batch operation. Because [%s]
        """ % (
            str(exception_string)
        ))
        