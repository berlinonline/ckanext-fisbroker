# -*- coding: UTF-8 -*-
"""Test for ckanext.fisbroker.controller.py"""

import json
import logging
from nose.tools import assert_raises, nottest
from urlparse import urlparse
from webtest import AppError

from ckan.common import c
from ckan.lib.base import config
from ckan.logic import get_action, NotAuthorized
from ckan.logic.action.update import package_update
from ckan.logic.schema import default_update_package_schema
from ckan import model
from ckan import plugins
from ckan.model import Session
from ckan.model.package import Package
from ckan.plugins import implements, SingletonPlugin
from ckan.tests import factories as ckan_factories


from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestSource
from ckanext.harvest.tests import factories
from ckanext.fisbroker import HARVESTER_ID
import ckanext.fisbroker.controller as controller
from ckanext.fisbroker.controller import get_error_dict, ERROR_MESSAGES
from ckanext.fisbroker.helper import is_fisbroker_package
from ckanext.fisbroker.tests import _assert_equal, _assert_not_equal, FisbrokerTestBase, FISBROKER_HARVESTER_CONFIG
from ckanext.fisbroker.tests.mock_fis_broker import start_mock_server, VALID_GUID

LOG = logging.getLogger(__name__)
FISBROKER_PLUGIN = u'fisbroker'

class TestControllerHelper:
    '''Tests for controller code not directly related to reimporting (i.e., helper stuff).'''

    def test_unknown_error_code_raises_error(self):
        '''Requesting an unknown error code should lead to a ValueError.'''
        with assert_raises(ValueError):
            get_error_dict(100)

    def test_known_error_code_returns_dict(self):
        '''Requesting a known error code should return a dict with members
           'message' and 'code', with the correct values.'''
        error_code = controller.ERROR_NOT_FOUND_IN_CKAN
        error_dict = get_error_dict(error_code)
        assert 'message' in error_dict
        assert 'code' in error_dict
        _assert_equal(error_dict['message'], ERROR_MESSAGES[error_code])
        _assert_equal(error_dict['code'], error_code)


class TestReimport(FisbrokerTestBase):
    '''Tests for controller code directly related to reimporting.'''
    _load_plugins = ('dummyharvest', )

    def setup(self):
        super(TestReimport, self).setup()
        # if not plugins.plugin_loaded(FISBROKER_PLUGIN):
        #     plugins.load(FISBROKER_PLUGIN)
        self.app = self._get_test_app()

    def test_reimport_api_must_request_json(self):
        '''If the request does not accept json content, the server should respond with an HTTP 400.'''
        response = self.app.get('/api/harvest/reimport?id=dunk', expect_errors=True)
        _assert_equal(response.status_int, 400)

    def test_reimport_api_requires_id(self):
        '''Requests to the reimport API require the presence of an 'id' parameter.'''
        response = self.app.get('/api/harvest/reimport', headers={'Accept':'application/json'}, expect_errors=True)
        _assert_equal(response.status_int, 400)

    def test_reimport_api_unknown_package_id_fails(self):
        '''If the reimport is triggered via the API and the requested package id does not exist,
           the server should respond with an HTTP 404, and the response should have content type
           application/json.'''
        response = self.app.get('/api/harvest/reimport?id=dunk', headers={'Accept':'application/json'}, expect_errors=True)
        _assert_equal(response.status_int, 404)
        _assert_equal(response.content_type, "application/json")

    def test_reimport_browser_triggers_redirect(self):
        '''If the reimport is triggered via the Browser (HTML is requested), the response should be a
           302 redirect to a specific URL.'''
        # unsuccessful request, /dataset/dunk does not exist:
        response = self.app.get(
            url='/dataset/dunk/reimport',
            headers={'Accept':'text/html'},
            expect_errors=True,
            extra_environ={'REMOTE_USER': self.context['user'].encode('ascii')}
        )
        url = urlparse(response.location)
        _assert_equal(response.status_int, 302)
        _assert_equal(url.path, "/dataset/dunk")

        # successful request:
        fb_dataset_dict, source, job = self._harvester_setup(FISBROKER_HARVESTER_CONFIG)
        job.status = u'Finished'
        job.save()
        # add the required guid extra
        fb_dataset_dict['extras'].append({'key': 'guid', 'value': VALID_GUID})
        package_update(self.context, fb_dataset_dict)
        package_id = fb_dataset_dict['id']
        response = self.app.get(
            url='/dataset/{}/reimport'.format(package_id),
            headers={'Accept': 'text/html'},
            expect_errors=True,
            extra_environ={'REMOTE_USER': self.context['user'].encode('ascii')}
        )
        url = urlparse(response.location)
        _assert_equal(response.status_int, 302)
        _assert_equal(url.path, "/dataset/{}".format(package_id))

    def test_can_only_reimport_harvested_packages(self):
        '''If we try to reimport an existing package that was not generated by a harvester, the response
           should be an HTTP 422, with an internal error code 5.'''
        non_fb_dataset_dict = ckan_factories.Dataset()
        package_id = non_fb_dataset_dict['id']
        response = self.app.get("/api/harvest/reimport?id={}".format(package_id), headers={'Accept':'application/json'}, expect_errors=True)
        _assert_equal(response.status_int, 422)
        content = json.loads(response.body)
        _assert_equal(content['error']['code'], controller.ERROR_NOT_HARVESTED)

    def test_can_only_reimport_fisbroker_packages(self):
        '''If we try to reimport an existing package that was generated by a harvester other than ckanext-fisbroker,
           the response should be an HTTP 422, with internal error code 6.'''
        harvester_config = {
            'title': 'Dummy Harvester' ,
            'name': 'dummy-harvester' ,
            'source_type': 'dummyharvest' ,
            'url' : "http://test.org/csw"
        }
        dataset_dict, source, job = self._harvester_setup(harvester_config)
        package_id = dataset_dict['id']
        response = self.app.get("/api/harvest/reimport?id={}".format(package_id), headers={'Accept':'application/json'}, expect_errors=True)
        _assert_equal(response.status_int, 422)
        content = json.loads(response.body)
        _assert_equal(content['error']['code'], controller.ERROR_NOT_HARVESTED_BY_FISBROKER)

    def test_can_only_reimport_with_guid(self):
        '''If we cannot determine a FIS-Broker guid for the package we try to reimport 
           return an HTTP 500 with internal error code 7.'''

        fb_dataset_dict, source, job = self._harvester_setup(FISBROKER_HARVESTER_CONFIG, fb_guid=None)
        # datasets created in this way have no extras set, so also no 'guid'
        package_id = fb_dataset_dict['id']
        response = self.app.get(
            "/api/harvest/reimport?id={}".format(package_id),
            headers={'Accept':'application/json'},
            expect_errors=True,
            extra_environ={'REMOTE_USER': self.context['user'].encode('ascii')}
        )
        _assert_equal(response.status_int, 500)
        content = json.loads(response.body)
        _assert_equal(content['error']['code'], controller.ERROR_NO_GUID)

    def test_handle_no_connection_to_fisbroker(self):
        '''If the FIS-Broker service cannot be reached, return an HTTP 500 with internal
           error code 8.'''

        unreachable_config = {
            'title': 'Unreachable FIS-Broker Harvest Source' ,
            'name': 'unreachable-fis-broker-harvest-source' ,
            'source_type': HARVESTER_ID ,
            'url' : "http://somewhere.over.the.ra.invalid/csw"
        }
        fb_dataset_dict, source, job = self._harvester_setup(unreachable_config)
        # add the required guid extra
        fb_dataset_dict['extras'].append({'key': 'guid', 'value': 'abcdef'})
        package_update(self.context, fb_dataset_dict)
        package_id = fb_dataset_dict['id']
        response = self.app.get("/api/harvest/reimport?id={}".format(package_id), headers={'Accept':'application/json'}, expect_errors=True)
        _assert_equal(response.status_int, 500)
        content = json.loads(response.body)
        _assert_equal(content['error']['code'], controller.ERROR_NO_CONNECTION)

    def test_handle_not_found_fisbroker(self):
        '''If FIS-Broker service replies that no record with the given guid exisits, return an
           HTTP 404 with internal error code 9.'''

        fb_dataset_dict, source, job = self._harvester_setup(FISBROKER_HARVESTER_CONFIG, fb_guid='invalid_guid')
        package_update(self.context, fb_dataset_dict)
        package_id = fb_dataset_dict['id']
        response = self.app.get(
            "/api/harvest/reimport?id={}".format(package_id),
            headers={'Accept':'application/json'},
            expect_errors=True,
            extra_environ={'REMOTE_USER': self.context['user'].encode('ascii')}
        )
        _assert_equal(response.status_int, 404)
        content = json.loads(response.body)
        _assert_equal(content['error']['code'], controller.ERROR_NOT_FOUND_IN_FISBROKER)

    def test_successful_reimport(self):
        '''If all is good and the FIS-Broker service returns a record,
           return an HTTP 200.'''

        fb_dataset_dict, source, job = self._harvester_setup(FISBROKER_HARVESTER_CONFIG)
        job.status = u'Finished'
        job.save()
        # add the required guid extra
        fb_dataset_dict['extras'].append({'key': 'guid', 'value': VALID_GUID})
        package_update(self.context, fb_dataset_dict)
        package_id = fb_dataset_dict['id']
        package = Package.get(package_id)
        old_title = package.title
        response = self.app.get(
            url="/api/harvest/reimport?id={}".format(package_id),
            headers={'Accept': 'application/json'},
            extra_environ={'REMOTE_USER': self.context['user'].encode('ascii')}
        )
        # assert successful HTTP response
        _assert_equal(response.status_int, 200)
        content = json.loads(response.body)
        # assert success marker in resonse JSON
        assert content['success']
        # assert that title has changed to the correct value (i.e., the reimport has actually happened)
        _assert_equal(package.title, u"Nährstoffversorgung des Oberbodens 2015 (Umweltatlas) - [WFS]")
        _assert_not_equal(package.title, old_title)

    def test_reimport_anonymously_fails(self):
        '''Only a logged in user can initiate a successful reimport, so anonymous access
           should raise an authorization error.'''

        fb_dataset_dict, source, job = self._harvester_setup(FISBROKER_HARVESTER_CONFIG)
        job.status = u'Finished'
        job.save()
        # add the required guid extra
        fb_dataset_dict['extras'].append({'key': 'guid', 'value': VALID_GUID})
        package_update(self.context, fb_dataset_dict)
        package_id = fb_dataset_dict['id']

        with assert_raises(NotAuthorized):
            self.app.get(
                url='/dataset/{}/reimport'.format(package_id),
                headers={'Accept': 'text/html'},
                expect_errors=True
            )


class DummyHarvester(SingletonPlugin):
    '''A dummy harvester for testing purposes.'''

    implements(IHarvester, inherit=True)

    def info(self):
        '''Implements ckanext.harvest.interfaces.IHarvester.info()'''

        return {
            'name': 'dummyharvest',
            'title': 'Dummy Harvester',
            'description': 'A dummy harvester for testing.'
        }
