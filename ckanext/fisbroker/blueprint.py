# coding: utf-8
"""
This module implements the main controller for ckanext-fisbroker.
"""

import datetime
import logging

from flask import Blueprint, make_response

# from ckan.common import OrderedDict, _, c, request, response, config
from ckan import model
from ckan.common import c, request
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
from ckanext.fisbroker.exceptions import (
    ERROR_MESSAGES,
    ERROR_DURING_IMPORT,
    ERROR_MISSING_ID,
    ERROR_NO_CONNECTION,
    ERROR_NO_GUID,
    ERROR_NOT_FOUND_IN_CKAN,
    ERROR_NOT_FOUND_IN_FISBROKER,
    ERROR_NOT_HARVESTED,
    ERROR_NOT_HARVESTED_BY_FISBROKER,
    ERROR_UNEXPECTED,
    ERROR_WRONG_CONTENT_TYPE,
    ERROR_WRONG_HTTP,
    NoFBHarvesterDefined,
    PackageIdDoesNotExistError,
    PackageNotHarvestedError,
    PackageNotHarvestedInFisbrokerError,
    NoFisbrokerIdError,
    NoConnectionError,
    NotFoundInFisbrokerError,
    FBImportError,
)
from ckanext.fisbroker.helper import (
    dataset_was_harvested,
    harvester_for_package,
    fisbroker_guid,
    get_fisbroker_source,
    is_reimport_job,
)

LOG = logging.getLogger(__name__)

def get_error_dict(error_code):
    '''Return a dict for an error_code, raise ValueError if code doesn't exist.'''
    if error_code in ERROR_MESSAGES:
        return {
            "message": ERROR_MESSAGES[error_code] ,
            "code": error_code
        }
    raise ValueError(f"No error code {error_code} exists, must be one of {ERROR_MESSAGES.keys()}")

    # TODO: What to do with this?
    # def __call__(self, environ, start_response):
    #     # avoid status_code_redirect intercepting error responses
    #     environ['pylons.status_code_redirect'] = True
    #     return base.BaseController.__call__(self, environ, start_response)

def reimport_through_browser(package_id):
    '''Initiate the reimport action through the browser (signified by
        the use of a /dataset/{name}/reimport pattern URL).'''

    # try to reimport through API
    response_data = reimport(package_id, direct_call=True)
    if response_data['success']:
        h.flash_success(response_data['message'])
    else:
        h.flash_error(response_data['error']['message'])

    # redirect to dataset page
    return h.redirect_to(controller='dataset', action='read', id=package_id)

def reimport_through_api():
    '''Initiate the reimport action through the api (signified by
        the use of an /api/harvest/reimport URL).'''

    accept = request.accept_mimetypes
    response_code = 400
    response_data = {
        "success": "False"
    }
    if not accept.accept_json:
        response_data['error'] = get_error_dict(ERROR_WRONG_CONTENT_TYPE)
        return _finish(response_code, response_data)

    package_id = request.params.get('id')
    if not package_id:
        response_data['error'] = get_error_dict(ERROR_MISSING_ID)
        return _finish(response_code, response_data)

    return reimport(package_id)

def reimport_batch(package_ids, context):
    '''Batch-reimport all packages in `package_ids` from their original
        harvest source.'''

    ckan_fb_mapping = {}

    # first, do checks that can be done without connection to FIS-Broker
    for package_id in package_ids:
        package = Package.get(package_id)

        if not package:
            raise PackageIdDoesNotExistError(package_id)

        if not dataset_was_harvested(package):
            raise PackageNotHarvestedError(package_id)

        harvester = harvester_for_package(package)
        harvester_url = harvester.url
        harvester_type = harvester.type
        if not harvester_type == HARVESTER_ID:
            raise PackageNotHarvestedInFisbrokerError(package_id)

        fb_guid = fisbroker_guid(package)
        if not fb_guid:
            raise NoFisbrokerIdError(package_id)

        ckan_fb_mapping[package.id] = fb_guid

    # get the harvest source for FIS-Broker datasets
    fb_source = get_fisbroker_source()
    if not fb_source:
        raise NoFBHarvesterDefined()
    source_id = fb_source.get('id', None)

    # Create and start a new harvest job
    job_dict = toolkit.get_action('harvest_job_create')(context, {'source_id': source_id})
    harvest_job = HarvestJob.get(job_dict['id'])
    harvest_job.gather_started = datetime.datetime.utcnow()
    assert harvest_job

    # instatiate the CSW connector (on the reasonable assumption that harvester_url is
    # the same for all package_ids)
    package_id = None
    reimported_packages = {}
    try:
        csw = CatalogueServiceWeb(harvester_url)
        for package_id, fb_guid in ckan_fb_mapping.items():
            # query connector to get resource document
            csw.getrecordbyid([fb_guid], outputschema=namespaces['gmd'])

            # show resource document
            record = csw.records.get(fb_guid, None)
            if record:
                obj = HarvestObject(guid=fb_guid,
                                    job=harvest_job,
                                    content=(record.xml).decode('utf-8'),
                                    package_id=package_id,
                                    extras=[
                                        HarvestObjectExtra(key='status',value='change'),
                                        HarvestObjectExtra(key='type',value='reimport'),
                                    ])
                obj.save()

                assert obj, obj.content
                
                from ckanext.fisbroker.plugin import FisbrokerPlugin

                harvester = FisbrokerPlugin()
                harvester.force_import = True
                harvester.import_stage(obj)
                rejection_reason = _dataset_rejected(obj)
                harvester.force_import = False
                if rejection_reason:
                    raise FBImportError(package_id, rejection_reason)

                Session.refresh(obj)

                reimported_packages[package_id] = record

            else:
                raise NotFoundInFisbrokerError(package_id, fb_guid)

    except RequestException as error:
        raise NoConnectionError(package_id, harvester_url, str(error.__class__.__name__))


    # successfully finish harvest job
    harvest_job.status = u'Finished'
    harvest_job.finished = datetime.datetime.utcnow()
    harvest_job.save()

    return reimported_packages

def reimport(package_id, direct_call=False, context=None):
    '''Reimport package with `package_id` from the original harvest
        source.'''

    if not context:
        context = {
            'model': model,
            'session': model.Session,
            'user': c.user
        }
    response_data = {
        "success": False
    }
    response_code = 200
    try:
        reimport_batch([package_id], context)
    except PackageNotHarvestedInFisbrokerError:
        response_code = 422
        response_data['error'] = get_error_dict(ERROR_NOT_HARVESTED_BY_FISBROKER)
    except PackageNotHarvestedError:
        response_code = 422
        response_data['error'] = get_error_dict(ERROR_NOT_HARVESTED)
    except NoFisbrokerIdError:
        response_code = 500
        response_data['error'] = get_error_dict(ERROR_NO_GUID)
    except NoConnectionError as error:
        response_code = 500
        response_data['error'] = get_error_dict(ERROR_NO_CONNECTION)
        message = response_data['error']['message'].format(error.service_url, str(error.__class__.__name__))
        response_data['error']['message'] = message
    except NotFoundInFisbrokerError as error:
        response_code = 404
        response_data['error'] = get_error_dict(ERROR_NOT_FOUND_IN_FISBROKER)
        message = response_data['error']['message'].format(error.fb_guid)
        response_data['error']['message'] = message
    except PackageIdDoesNotExistError as error:
        response_code = 404
        response_data['error'] = get_error_dict(ERROR_NOT_FOUND_IN_CKAN)
        message = response_data['error']['message'].format(error.package_id)
        response_data['error']['message'] = message
    except FBImportError as error:
        from ckan.logic.action.delete import package_delete
        response_data['error'] =  get_error_dict(ERROR_DURING_IMPORT)
        message = response_data['error']['message'].format(error.reason)
        response_data['error']['message'] = f"{message} – Package will be deactivated."
        package_delete(context, { "id": package_id })
    else:
        response_data = {
            'success': True,
            'message': "Package was successfully re-imported."
        }

    response_data['package_id'] = package_id

    return _finish(response_code, response_data, direct_call)

def _dataset_rejected(harvest_object):
    """Look at harvest_object to see if the dataset was rejected during
        import. If rejected, return the reason, if not, return None."""

    for extra in harvest_object.extras:
        if extra.key == 'error':
            return extra.value

    return None


def _finish(status_int, response_data=None, direct_call=False):
    '''When a controller method has completed, call this method
    to prepare the response.
    @return response message - return this value from the controller
                                method
                e.g. return _finish(404, 'Package not found')

    Shortened version of from ckan/controllers/api._finish()
    '''
    assert isinstance(status_int, int)
    if not direct_call:
        response_msg = ''
        response_headers = {}
        if response_data is not None:
            response_headers['Content-Type'] = 'application/json;charset=utf-8'
            response_msg = h.json.dumps(
                response_data,
                for_json=True)  # handle objects with for_json methods
        return make_response(response_msg, status_int, response_headers)
    return response_data


reimportapi = Blueprint('reimportapi', __name__)
reimportapi.add_url_rule(u'/api/harvest/reimport',
                           methods=['GET', 'POST'], view_func=reimport_through_api)
reimportapi.add_url_rule(u'/dataset/<package_id>/reimport',
                           methods=['GET'], view_func=reimport_through_browser)
