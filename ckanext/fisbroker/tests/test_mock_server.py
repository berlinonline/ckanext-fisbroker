import logging
import pytest
import requests

from ckanext.fisbroker.tests import MOCK_PORT

LOG = logging.getLogger(__name__)

class TestMockErrors(object):
    '''Tests for error messages from the mock FIS-Broker.'''

    def test_missing_id_error(self):
        '''
            A missing id parameter for a getrecordbyid-request should result in a
            bad request error.
        '''
        response = requests.get(f"http://127.0.0.1:{MOCK_PORT}/csw/?request=getrecordbyid&service=csw&version=2.0.2")
        assert response.status_code == requests.codes.bad_request
        assert "missing parameter Id" in response.content.decode('utf-8')
    
    def test_unknown_request_type(self):
        '''
            An unknown value for the 'request' parameter should result in a bad
            request error.
        '''
        bad_request_type = "getfood"
        response = requests.get(f"http://127.0.0.1:{MOCK_PORT}/csw/?request={bad_request_type}&service=csw&version=2.0.2")
        assert response.status_code == requests.codes.bad_request
        assert response.content.decode('utf-8') == f"unknown request '{bad_request_type}'."

    def test_request_parameter_missing(self):
        '''
            A missing 'request' parameter should result in a bad request error.
        '''
        response = requests.get(f"http://127.0.0.1:{MOCK_PORT}/csw/?service=csw&version=2.0.2")
        assert response.status_code == requests.codes.bad_request
        assert response.content.decode('utf-8') == "parameter 'request' is missing."

    def test_only_csw_can_be_found(self):
        '''
            All paths except /csw are disallowed and cannot be found (not found error).
        '''
        response = requests.get(f"http://127.0.0.1:{MOCK_PORT}/cswee/?request=getrecordbyid&service=csw&version=2.0.2")
        assert response.status_code == requests.codes.not_found


    def test_only_getrecords(self):
        '''
            For POST only getrecords is implemented, all other requests will be rejected (bad request).
        '''
        response = requests.post(f"http://127.0.0.1:{MOCK_PORT}/csw/?request=getrecords&service=csw&version=2.0.2", "<foo></foo>")
        assert response.status_code == requests.codes.bad_request

