"""
A connection to a specific MongoDB database.
"""

import httplib2, urllib
import simplejson as json
from simplejson import JSONDecodeError
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
        """
        Set auth to True and an 'https' connection will be made
        rather than an http connection.
        
        If you set auth to True HTTP Basic Authentication will be
        used with the username and password provided.
        """
        
        self.__host = host
        self.__port = port
        self.__https = https
        self.__username = username
        self.__password = password
        self.__auth = auth
        
    def get_host(self):
        return self.__host
        
    def get_port(self):
        return self.__port
        
    def connect_to_mongo(self, host='localhost', port=27017):
        """
        Used to tell Sleepy Mongoose about the
        MongoDB server it should connect to.
        """
        url = self._create_connect_url()
        payload = self._create_connect_payload(host, port)
        self.perform_request(
            url,
            payload,
            raise_error=self._raise_connection_error
        )
        return True
        
    def perform_request(self, url, payload='', method='POST', get_params='', raise_error=None, retries=0):
        """
        Wrapper for performing requests with httplib2
        """
        http = self.get_http()

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
            
            if not self._valid_response(response_object):
                if retries < 3:
                    self.perform_request(url, payload, method, get_params, raise_error, retries + 1)
                    return
                raise_error(content)
                             
            return response_object
            
        except ServerNotFoundError, exception:
            raise_error(str(exception))
        except JSONDecodeError, exception:
            raise_error("%s string = %s" % (
                str(exception),
                content
            ))
            
    def _valid_response(self, response_object):
        if isinstance(response_object, list):
            return self._batch_operation_ok(response_object)
        
        if not response_object.has_key('ok'):
            return True
            
        return response_object.has_key('ok') and response_object['ok']
        
    def _batch_operation_ok(self, response_object):
        if len(response_object) > 0 and response_object[0].has_key('ok') and not response_object[0]['ok']:
            return False
        return True
        
    def get_http(self):
        """
        Returns an http object, setting the basic auth
        credentials if they are provided.
        """
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
        
    def _raise_connection_error(self, exception_string=''):
        raise ConnectionError("""
        Could not connect sleepy.mongoose to MongoDB.
        Make sure you have the sleepy.mongoose server running.
        Because [%s]""" % (
            exception_string
        ))
        
    def __getitem__(self, key):
        return self._return_database(key)
        
    def __getattr__(self, key):
        return self._return_database(key)
        
    def _return_database(self, key):
        return Database(key, self)
