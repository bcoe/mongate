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
        
    def get_name(self):
        return self.name
        
    def save(self, document):
        """
        Update a document if it exists, insert the
        document if it does not exist.
        """
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
        """
        criteria: a python structure representing the document to find.
        document: the update to perform, e.g., {'$set': '{'x': 'y'}}
        """
        criteria = self._replace_id_with_object(criteria)
        url = self._create_url(self.update_action)
        payload = self._create_update_payload(criteria, document)
        return self.connection.perform_request(
            url,
            payload=payload,
            raise_error=self._raise_collection_error
        )
        
    def _create_update_payload(self, criteria, document):
        return "criteria=%s&newobj=%s" % (
            urllib.quote( json.dumps(criteria) ),
            urllib.quote( json.dumps(document) )
        )
        
    def remove(self, criteria={}):
        """
        Remove all documents matching the criteria.
        """
        criteria = self._replace_id_with_object(criteria)
        url = self._create_url(self.remove_action)
        payload = self._create_remove_payload(criteria)
        return self.connection.perform_request(
            url,
            payload=payload,
            raise_error=self._raise_collection_error
        )
    
    def _create_remove_payload(self, criteria):
        return "criteria=%s" % json.dumps(criteria)
        
    def count(self, criteria):
        results = self.find(criteria, ['_id'])
        return len(results)
        
    def find_one(self, criteria={}):
        """
        Return the first result fetched by find.
        """
        results = self.find(criteria)
        if len(results) > 0:
            return results[0]
        else:
            return False
        
    def find(self, criteria={}, fields=False):
        """
        Find all documents matching the criteria, e.g.,
        {'foo': {'$lt': 5}}
        """
        criteria = self._replace_id_with_object(criteria)
        url = self._create_url(self.find_action)
        get_params = self._create_find_get_params(criteria, fields)
        response_object = self.connection.perform_request(
            url,
            get_params=get_params,
            method='GET',
            raise_error=self._raise_collection_error
        )
        return self._process_response_object(response_object)
        
    def _process_response_object(self, response_object):
        for result in response_object['results']:
            try:
                result['_id'] = result['_id']['$oid']
            except:
                # This document is not using an ObjectId _id
                pass
        return response_object['results']

    def _create_find_get_params(self, criteria, fields=False):
        params = "?batch_size=9999999&criteria=%s" % urllib.quote(
                json.dumps(criteria)
            )
        if fields:
            params = "%s&fields=%s" % (
                params,
                urllib.quote(
                    json.dumps(fields)
                )
            )
        return params
        
    def _replace_id_with_object(self, criteria):
        if criteria.has_key('_id'):
            criteria['_id'] = {'$oid': criteria['_id']}
        return criteria
        
    def insert(self, document):
        """
        Insert a new document into MongoDB the 
        object id is returned.
        """
        url = self._create_url(self.insert_action)
        payload = self._create_insert_payload(document)
        response_object = self.connection.perform_request(
            url,
            payload,
            'POST',
            raise_error=self._raise_collection_error
        )
        response = response_object['oids'][0]
        try:
            return response['$oid']
        except TypeError:
            return response
        
    def _create_url(self, action):
        if self.connection.is_https():
            prefix = 'https'
        else:
            prefix = 'http'
        
        return "%s://%s:%s/%s/%s/%s" % (
            prefix,
            self.connection.get_host(),
            self.connection.get_port(),
            self.database.get_name(),
            self.name,
            action
        )
        
    def _create_insert_payload(self, document):
        return "docs=[%s]" %  urllib.quote( json.dumps(document) )
    
    def _raise_collection_error(self, exception_string=''):
        raise CollectionError("""
        An error occurred while performing an
        operation on a collection. Because [%s] 
        """ % (
            exception_string
        ))