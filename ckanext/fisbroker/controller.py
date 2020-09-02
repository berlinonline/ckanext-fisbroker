# coding: utf-8
"""
This module implements the main controller for ckanext-fisbroker.
"""

import datetime
import logging

# from ckan.common import OrderedDict, _, c, request, response, config
from ckan import model
from ckan.common import c, request, response
import ckan.lib.base as base
import ckan.lib.helpers as h
from ckan.model import Package, Session
from ckan.plugins import toolkit

from owslib.csw import CatalogueServiceWeb, namespaces
from requests.exceptions import RequestException

from ckanext.harvest.model import (
    HarvestJob,
    HarvestObject,
    HarvestObjectExtra
)

from ckanext.fisbroker import HARVESTER_ID
from ckanext.fisbroker.helper import (
    dataset_was_harvested,
    harvester_for_package,
    fisbroker_guid,
)
from ckanext.fisbroker.plugin import FisbrokerPlugin

LOG = logging.getLogger(__name__)
ERROR_WRONG_HTTP = 1
ERROR_WRONG_CONTENT_TYPE = 2
ERROR_MISSING_ID = 3
ERROR_NOT_FOUND_IN_CKAN = 4
ERROR_NOT_HARVESTED = 5
ERROR_NOT_HARVESTED_BY_FISBROKER = 6
ERROR_NO_GUID = 7
ERROR_NO_CONNECTION = 8
ERROR_NOT_FOUND_IN_FISBROKER = 9
ERROR_DURING_IMPORT = 10
ERROR_UNEXPECTED = 20


ERROR_MESSAGES = {
    ERROR_WRONG_HTTP: 'Wrong HTTP method, only GET is allowed.',
    ERROR_WRONG_CONTENT_TYPE: 'Wrong content type, only application/json is allowed.',
    ERROR_MISSING_ID: "Missing parameter 'id'.",
    ERROR_NOT_FOUND_IN_CKAN: 'No such package found.',
    ERROR_NOT_HARVESTED: 'Package could not be re-imported because it was not created by a harvester.',
    ERROR_NOT_HARVESTED_BY_FISBROKER: "Package could not be re-imported because it was not harvested by harvester '{}'.".format(HARVESTER_ID),
    ERROR_NO_GUID: "Package could not be re-imported because FIS-Broker GUID could not be determined.",
    ERROR_NO_CONNECTION: "Failed to establish connection to FIS-Broker service at {} ({}).",
    ERROR_NOT_FOUND_IN_FISBROKER: "Package could not be re-imported because GUID '{}' was not found on FIS-Broker.",
    ERROR_DURING_IMPORT: "Package could not be re-imported because the FIS-Broker data is no longer valid. Reason: {}. Package will be deactivated.",
    ERROR_UNEXPECTED: 'Unexpected error'
}

def get_error_dict(error_code):
    '''Return a dict for an error_code, raise ValueError if code doesn't exist.'''
    if error_code in ERROR_MESSAGES:
        return {
            "message": ERROR_MESSAGES[error_code] ,
            "code": error_code
        }
    raise ValueError("No error code {} exists, must be one of {}".format(error_code, ERROR_MESSAGES.keys()))

class FISBrokerController(base.BaseController):
    """
    Main controller class for ckanext-fisbroker.
    """

    def __call__(self, environ, start_response):
        # avoid status_code_redirect intercepting error responses
        environ['pylons.status_code_redirect'] = True
        return base.BaseController.__call__(self, environ, start_response)

    def reimport_browser(self, package_id):
        '''Initiate the reimport action through the browser (signified by
           the use of a /dataset/{name}/reimport pattern URL).'''

        # try to reimport through API
        response_data = self.reimport(package_id, direct_call=True)
        if response_data['success']:
            h.flash_success(response_data['message'])
        else:
            h.flash_error(response_data['error']['message'])

        # redirect to dataset page
        h.redirect_to(controller='package', action='read', id=package_id)

    def reimport_api(self):
        '''Initiate the reimport action through the api (signified by
           the use of an /api/harvest/reimport URL).'''

        def accepts_json(accept):
            if hasattr(accept, '_parsed_nonzero'):
                for element in accept._parsed_nonzero:
                    if element[0] == "application/json":
                        return True
            return False

        accept = request.accept
        response_code = 400
        response_data = {
            "success": "False"
        }
        if not accepts_json(accept):
            response_data['error'] = get_error_dict(ERROR_WRONG_CONTENT_TYPE)
            return self._finish(response_code, response_data)

        package_id = request.params.get('id')
        if not package_id:
            response_data['error'] = get_error_dict(ERROR_MISSING_ID)
            return self._finish(response_code, response_data)

        return self.reimport(package_id)

    def reimport(self, package_id, direct_call=False):
        '''Reimport package with `package_id` from the original harvest
           source.'''

        # initiate connector
        package = Package.get(package_id)
        response_data = {
            "success": False
        }
        if not package:
            response_data['error'] = get_error_dict(ERROR_NOT_FOUND_IN_CKAN)
            response_code = 404
        elif dataset_was_harvested(package):
            harvester = harvester_for_package(package)
            harvester_url = harvester.url
            harvester_type = harvester.type
            if harvester_type == HARVESTER_ID:
                fb_id = fisbroker_guid(package)
                if fb_id:
                    try:
                        csw = CatalogueServiceWeb(harvester_url)
                        # query connector to get resource document
                        csw.getrecordbyid([fb_id], outputschema=namespaces['gmd'])

                        # show resource document
                        record = csw.records.get(fb_id, None)
                        LOG.debug("got record: %s", record)
                        if record:
                            response_code = 200
                            response_data = {
                                "success": True ,
                                "message": "Package was successfully re-imported."
                            }
                            LOG.debug(record.xml)

                            # Create a job
                            context = {
                                'model': model,
                                'session': model.Session,
                                'user': c.user
                            }

                            job_dict = toolkit.get_action('harvest_job_create')(context,{'source_id':harvester.id})
                            harvest_job = HarvestJob.get(job_dict['id'])
                            harvest_job.gather_started = datetime.datetime.utcnow()
                            assert harvest_job

                            obj = HarvestObject(guid=fb_id,
                                                job=harvest_job,
                                                content=record.xml,
                                                package_id=package_id,
                                                extras=[HarvestObjectExtra(key='status',value='change')])
                            obj.save()

                            assert obj, obj.content

                            harvester = FisbrokerPlugin()
                            harvester.force_import = True
                            status = harvester.import_stage(obj)
                            rejection_reason = self._dataset_rejected(obj)
                            if rejection_reason:
                                response_code = 200
                                response_data = {
                                    "success": False,
                                    "error": get_error_dict(ERROR_DURING_IMPORT)
                                }
                                message = response_data['error']['message'].format(rejection_reason)
                                response_data['error']['message'] = message

                            harvester.force_import = False
                            Session.refresh(obj)

                            harvest_job.status = u'Finished'
                            harvest_job.finished = datetime.datetime.utcnow()
                            harvest_job.save()

                        else:
                            response_code = 404
                            response_data['error'] = get_error_dict(ERROR_NOT_FOUND_IN_FISBROKER)
                            message = response_data['error']['message'].format(fb_id)
                            response_data['error']['message'] = message
                    except RequestException as error:
                        response_code = 500
                        response_data['error'] = get_error_dict(ERROR_NO_CONNECTION)
                        message = response_data['error']['message'].format(harvester_url, str(error.__class__.__name__))
                        response_data['error']['message'] = message
                else:
                    response_code = 500
                    response_data['error'] = get_error_dict(ERROR_NO_GUID)
            else:
                response_code = 422
                response_data['error'] = get_error_dict(ERROR_NOT_HARVESTED_BY_FISBROKER)
        else:
            response_code = 422
            response_data['error'] = get_error_dict(ERROR_NOT_HARVESTED)

        response_data['package_id'] = package_id

        return self._finish(response_code, response_data, direct_call)

    def _dataset_rejected(self, harvest_object):
        """Look at harvest_object to see if the dataset was rejected during
           import. If rejected, return the reason, if not, return None."""

        for extra in harvest_object.extras:
            if extra.key == 'error':
                return extra.value

        return None


    def _finish(self, status_int, response_data=None, direct_call=False):
        '''When a controller method has completed, call this method
        to prepare the response.
        @return response message - return this value from the controller
                                   method
                 e.g. return self._finish(404, 'Package not found')

        Shortened version of from ckan/controllers/api._finish()
        '''
        assert isinstance(status_int, int)
        response.status_int = status_int
        if not direct_call:
            response_msg = ''
            if response_data is not None:
                response.headers['Content-Type'] = 'application/json;charset=utf-8'
                response.content_type = 'application/json'
                response_msg = h.json.dumps(
                    response_data,
                    for_json=True)  # handle objects with for_json methods
            return response_msg
        return response_data
