# coding: utf-8
"""
    Code for mocking a FIS-Broker for testing.
"""


import logging
import os
import re
from urllib.parse import urlparse, parse_qs
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer
from lxml import etree

import requests
from requests.exceptions import Timeout

PORT = 8999
CSW_PATH = "/csw"
VALID_GUID =   '65715c6e-bbaf-3def-982b-3b5156272da7'
INVALID_GUID = '65715c6e-bbaf-3def-982b-3b5156272da8'
METADATA_NOW = '2019-11-25T13:18:43'
METADATA_OLD = '2019-11-23T13:18:43'
RECORD_DUMMY_COUNT = 10

LOG = logging.getLogger(__name__)

def read_responses():
    """Read canned mock responses into a dictionary."""

    responses = {}
    names = [
        'getcapabilities' ,
        'missing_id' ,
        'no_record_found' ,
        'csw_getrecords_01' ,
        VALID_GUID ,
        INVALID_GUID ,
        "8a7ea996-7955-4fbb-8980-7be09be6f193_01" ,
        "aac23975-94e4-3707-96fa-e447e43d6013_01" ,
        "f2a8a483-74b9-3c7d-9b40-113c60a55c9e_01" ,
    ]
    responses['records'] = {}
    record_pattern = re.compile(r'^([a-z0-9]){8}-([a-z0-9]){4}-([a-z0-9]){4}-([a-z0-9]){4}-([a-z0-9]){12}(\_[0-9][0-9])?$')
    folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'xml')
    for name in names:
        response_file = open(os.path.join(folder_path, f"{name}.xml"), "r")
        if re.match(record_pattern, name):
            responses['records'][name] = response_file.read()
        else:
            responses[name] = response_file.read()

    # Add a bunch of extra records that are create dynamically by replacing placeholders in a template
    record_dummy_template_file = open(os.path.join(folder_path, f"record_dummy_template.xml"), "r")
    record_dummy_template = record_dummy_template_file.read()
    for index in range(RECORD_DUMMY_COUNT):
        guid = f"record_{index:02d}"
        title = f"Nährstoffversorgung des Oberbodens 2015 (Umweltatlas) {index:02d}"
        record = record_dummy_template.replace('[[GUID]]', guid)
        record = record.replace('[[TITLE]]', title)
        responses['records'][guid] = record

    return responses

RESPONSES = read_responses()
LOG.debug(f"responses: {RESPONSES['records'].keys()}")

class MockFISBroker(BaseHTTPRequestHandler):
    """A mock FIS-Broker for testing."""

    def do_GET(self):
        """Implementation of do_GET()"""

        parsed_url = urlparse(self.path)
        if parsed_url.path.rstrip('/') == CSW_PATH:
            query = parse_qs(parsed_url.query)

            csw_request = query.get('request')

            if csw_request:
                csw_request = csw_request[0].lower()
                if csw_request == "getcapabilities":
                    response_code = requests.codes.ok
                    content_type = 'text/xml; charset=utf-8'
                    response_content = RESPONSES['getcapabilities']
                    # TODO: how do I set the protocol dynamically?
                    base_url = f"http://{self.server.server_name}:{self.server.server_port}{CSW_PATH}"
                    response_content = response_content.replace("{BASE_URL}", base_url)
                elif csw_request == "getrecordbyid":
                    # for id X and getrecords count Y, the mock FIS-Broker will
                    # look for an entry "X_Y" in the RESPONSES dict. If the entry
                    # exists, it will be served. If it doesn't exist, the 'no_record_found'
                    # response will be served, leading to an error in the harvest job.
                    # This can be used for tests that somehow involve errored harvest jobs.
                    record_id = query.get('id')
                    LOG.info(f"this is a GetRecordById request: {MockFISBroker.count_get_records}")
                    if record_id:
                        record_id = record_id[0]
                        if record_id not in RESPONSES['records']:
                            record_id = f"{record_id}_{str(MockFISBroker.count_get_records).rjust(2, '0')}"
                        LOG.info(f"looking for {record_id}")
                        if record_id == "cannot_connect_00":
                            # mock a timeout happening during a GetRecordById request
                            raise Timeout()
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
                    response_content = f"unknown request '{csw_request}'."
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
        self.wfile.write(response_content.encode('utf-8'))

    def do_POST(self):
        """Implementation of do_POST()"""

        length = int(self.headers.get('content-length', 0))
        body = self.rfile.read(length)
        root = etree.fromstring(body)
        csw_request = root.tag
        content_type = "application/xml"
        response_content = "<foo></foo>"
        if csw_request == "{http://www.opengis.net/cat/csw/2.0.2}GetRecords":
            MockFISBroker.count_get_records += 1
            LOG.info(f"this is a GetRecords request: {MockFISBroker.count_get_records}")
            response_content = RESPONSES['csw_getrecords_01']
            response_code = 200
        else:
            response_code = requests.codes.bad_request
            content_type = 'text/plain; charset=utf-8'
            response_content = f"unknown request '{csw_request}'."

        self.send_response(response_code)
        self.send_header('Content-Type', content_type)
        self.end_headers()
        self.wfile.write(response_content.encode('utf-8'))


def start_mock_server(port=PORT):
    """Start the mock FIS-Broker with some configuration."""

    mock_server = HTTPServer(('localhost', port), MockFISBroker)
    MockFISBroker.count_get_records = 0

    print('Serving mock FIS-Broker at port', port)

    mock_server_thread = Thread(target=mock_server.serve_forever)
    mock_server_thread.setDaemon(True)
    mock_server_thread.start()

def reset_mock_server(counter=0):
    """Reset the mock FIS-Broker."""

    MockFISBroker.count_get_records = counter
