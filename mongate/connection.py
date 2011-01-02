"""
A connection to a specific MongoDB database.
"""

import httplib2, urllib
import simplejson as json
from mongate.database import Database
from httplib2 import ServerNotFoundError

class ConnectionError(Exception):
    """
    Errors raised by Connection.
    """
    pass

class Connection(object):
    """
    A connection to a specific MongoDB database.
    """
    
    __connect_endpoint = '_connect'
    
    def __init__(self, host='localhost', port=27080, https=False, auth=False, username='', password=''):
        self.__host = host
        self.__port = port
        self.__https = https
        self.__username = username
        self.__password = password
        
    def get_host(self):
        return self.__host
        
    def get_port(self):
        return self.__port
        
    def connect_to_mongo(self, host='localhost', port=27017):
        http = self.get_http()
        url = self._create_connect_url()
        payload = self._create_connect_payload(host, port)
        
        try:

            resp, content = http.request(
                url,
                method="POST",
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                body=payload
            )
            
            raise Exception(content)
            response_object = json.loads(content)
            if not response_object['ok']:
                self._raise_connection_error()
                
        except ServerNotFoundError:
            self._raise_connection_error()
        
        return True
        
    def get_http(self):
        http = httplib2.Http()
        
        if self.auth:
            http.add_credentials(self.__username, self.__password)
        
        return http
    
    def _create_connect_url(self):
        if self.is_https():
            prefix = 'https'
        else:
            prefix = 'http'
        
        return "%s://%s:%s/%s" % (
            prefix,
            self.__host,
            self.__port,
            self.__connect_endpoint
        )
        
    def is_https(self):
        return self.__https
        
    def _create_connect_payload(self, host, port):
        return urllib.urlencode({
            'server': "%s:%s" % (
                host,
                port
            )
        })
        
    def _raise_connection_error(self):
        raise ConnectionError("""
        Could not connect sleepy.mongoose to MongoDB.
        Make sure you have the sleepy.mongoose server running.
        """)
        
    def __getitem__(self, key):
        return self._return_database(key)
        
    def __getattr__(self, key):
        return self._return_database(key)
        
    def _return_database(self, key):
        return Database(key, self)
