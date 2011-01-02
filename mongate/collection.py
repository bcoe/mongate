"""
Instances of individual collection objects within MongoDB.
"""
import simplejson as json
import httplib2, urllib
from httplib2 import ServerNotFoundError

class CollectionError(Exception):
    """
    Default error raised by Collections
    """

class Collection(object):
    """
    Instances of individual collection objects within MongoDB.
    """
    insert_action = '_insert'
    find_action = '_find'
    remove_action = '_remove'
    update_action = '_update'
    
    def __init__(self, name, connection, database):
        self.name = name
        self.database = database
        self.connection = connection
        
    def save(self, document):
        if document.has_key('_id'):
            criteria = {
                '_id': document['_id']
            }
            
            del document['_id']
            
            document = {
                '$set': document
            }
            
            self.update(criteria, document)
        else:
            self.insert(document)
    
    def update(self, criteria, document):
        criteria = self._replace_id_with_object(criteria)
        url = self._create_url(self.update_action)
        payload = self._create_update_payload(criteria, document)
        return self._perform_request(url, payload=payload)
        
    def _create_update_payload(self, criteria, document):
        return "criteria=%s&newobj=%s" % (
            json.dumps(criteria),
            json.dumps(document)
        )
        
    def remove(self, criteria={}):
        criteria = self._replace_id_with_object(criteria)
        url = self._create_url(self.remove_action)
        payload = self._create_remove_payload(criteria)
        return self._perform_request(url, payload=payload)
    
    def _create_remove_payload(self, criteria):
        return "criteria=%s" % json.dumps(criteria)
        
    def find_one(self, criteria):
        results = self.find(criteria)
        if len(results) > 0:
            return results[0]
        else:
            return []
        
    def find(self, criteria={}):
        criteria = self._replace_id_with_object(criteria)
        url = self._create_url(self.find_action)
        get_params = self._create_find_get_params(criteria)
        response_object = self._perform_request(url, get_params=get_params, method='GET')
        return self._process_response_object(response_object)
        
    def _process_response_object(self, response_object):
        for result in response_object['results']:
            result['_id'] = result['_id']['$oid']
        return response_object['results']

    def _create_find_get_params(self, criteria):
        return "?batch_size=1&criteria=%s" % urllib.quote(
                json.dumps(criteria)
            )
        
    def _replace_id_with_object(self, criteria):
        if criteria.has_key('_id'):
            criteria['_id'] = {'$oid': criteria['_id']}
        return criteria
        
    def insert(self, document):
        url = self._create_url(self.insert_action)
        payload = self._create_insert_payload(document)
        response_object = self._perform_request(url, payload, 'POST')
        return response_object['oids'][0]['$oid']
        
    def _create_url(self, action):
        return "http://%s:%s/%s/%s/%s" % (
            self.connection.get_host(),
            self.connection.get_port(),
            self.database.get_name(),
            self.name,
            action
        )
        
    def _create_insert_payload(self, document):
        return "docs=[%s]" %  urllib.quote( json.dumps(document) )
        
    def _perform_request(self, url, payload='', method='POST', get_params=''):
        http = httplib2.Http()

        try:

            resp, content = http.request(
                "%s%s" % (url, get_params),
                method=method,
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                body=payload
            )

            response_object = json.loads(content)
            if response_object.has_key('ok') and not response_object['ok']:
                self._raise_collection_error()
                
            return response_object
            
        except ServerNotFoundError:
            self._raise_collection_error()
    
    def _raise_collection_error(self):
        raise CollectionError("""
        An error occurred while performing an
        operation on a collection.
        """)