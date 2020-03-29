# coding: utf-8
"""
    Code for mocking a FIS-Broker for testing.
"""


import logging
import os
import re
from urlparse import urlparse, parse_qs
from threading import Thread
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

import requests

PORT = 8999
CSW_PATH = "/csw"
VALID_GUID =   '65715c6e-bbaf-3def-982b-3b5156272da7'
INVALID_GUID = '65715c6e-bbaf-3def-982b-3b5156272da8'
METADATA_NOW = '2019-11-25T13:18:43'
METADATA_OLD = '2019-11-23T13:18:43'

LOG = logging.getLogger(__name__)

def read_responses():
    """Read canned mock responses into a dictionary."""

    responses = {}
    names = [
        'getcapabilities' ,
        'missing_id' ,
        'no_record_found' ,
        VALID_GUID ,
        INVALID_GUID
    ]
    responses['records'] = {}
    record_pattern = re.compile(r'^([a-z0-9]){8}-([a-z0-9]){4}-([a-z0-9]){4}-([a-z0-9]){4}-([a-z0-9]){12}$')
    folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'xml')
    for name in names:
        response_file = open(os.path.join(folder_path, "{}.xml".format(name)),"r")
        if re.match(record_pattern, name):
            responses['records'][name] = response_file.read()
        else:
            responses[name] = response_file.read()
    return responses

RESPONSES = read_responses()

class MockFISBroker(BaseHTTPRequestHandler):
    """A mock FIS-Broker for testing."""

    def do_GET(self):
        """Implementation of BaseHTTPRequestHandler.do_GET()."""

        parsed_url = urlparse(self.path)
        if parsed_url.path == CSW_PATH:
            query = parse_qs(parsed_url.query)

            csw_request = query.get('request')

            if csw_request:
                csw_request = csw_request[0].lower()
                if csw_request == "getcapabilities":
                    response_code = requests.codes.ok
                    content_type = 'text/xml; charset=utf-8'
                    response_content = RESPONSES['getcapabilities']
                    # TODO: how do I set the protocol dynamically?
                    base_url = "http://{}:{}{}".format(self.server.server_name, self.server.server_port, CSW_PATH)
                    response_content = response_content.replace("{BASE_URL}", base_url)
                elif csw_request == "getrecordbyid":
                    record_id = query.get('id')
                    if record_id:
                        record_id = record_id[0]
                        record = RESPONSES['records'].get(record_id)
                        if record:
                            response_code = requests.codes.ok
                            content_type = 'text/xml; charset=utf-8'
                            response_content = record
                        else:
                            response_code = requests.codes.ok
                            # /\ that really is the response code if id is not found...
                            content_type = 'text/xml; charset=utf-8'
                            response_content = RESPONSES['no_record_found']
                    else:
                        response_code = requests.codes.bad_request
                        content_type = 'text/xml; charset=utf-8'
                        response_content = RESPONSES['missing_id']
                else:
                    response_code = requests.codes.bad_request
                    content_type = 'text/plain; charset=utf-8'
                    response_content = "unknown request '{}'.".format(csw_request)
            else:
                response_code = requests.codes.bad_request
                content_type = 'text/plain; charset=utf-8'
                response_content = "parameter 'request' is missing."
        else:
            response_code = requests.codes.not_found
            content_type = 'text/plain; charset=utf-8'
            response_content = "This is not the response you are looking for."

        self.send_response(response_code)
        self.send_header('Content-Type', content_type)
        self.end_headers()
        self.wfile.write(response_content)
        return

def start_mock_server(port=PORT):
    """Start the mock FIS-Broker with some configuration."""

    mock_server = HTTPServer(('localhost', port), MockFISBroker)

    print('Serving mock FIS-Broker at port', port)

    mock_server_thread = Thread(target=mock_server.serve_forever)
    mock_server_thread.setDaemon(True)
    mock_server_thread.start()
