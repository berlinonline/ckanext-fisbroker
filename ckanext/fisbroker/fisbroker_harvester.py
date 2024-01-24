'''
Subclass of ckanext-spatial's CSWHarvester tailored specifically for
Berlin's FIS-Broker GIS.
'''

from datetime import datetime, timedelta
import json
import logging
import re

import six

from time import sleep
import uuid
import hashlib
import dateutil

from owslib.fes import PropertyIsGreaterThanOrEqualTo
from sqlalchemy import exists

from ckan import logic, model
from ckan.lib.munge import munge_title_to_name
from ckan.lib.navl.validators import not_empty
from ckan.lib.search.index import PackageSearchIndex
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckantoolkit import config

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import (
    HarvestJob ,
    HarvestGatherError ,
    HarvestObject ,
    HarvestObjectExtra ,
)

from ckanext.spatial.interfaces import ISpatialHarvester
from ckanext.spatial.harvesters.base import text_traceback
from ckanext.spatial.model import ISODocument
from ckanext.spatial.harvesters.csw import CSWHarvester
from ckanext.spatial.validation.validation import BaseValidator

from ckanext.fisbroker import HARVESTER_ID
from ckanext.fisbroker.csw_client import CswService
from ckanext.fisbroker.fisbroker_resource_annotator import FISBrokerResourceAnnotator
import ckanext.fisbroker.helper as helpers


LOG = logging.getLogger(__name__)
TIMEDELTA_DEFAULT = 0
TIMEOUT_DEFAULT = 20


def marked_as_opendata(data_dict):
    '''Check if `data_dict` is marked as Open Data. If it is,
       return True, otherwise False.'''

    iso_values = data_dict['iso_values']

    # checking for 'opendata' tag
    tags = iso_values['tags']
    if not 'opendata' in tags:
        return False
    return True


def marked_as_service_resource(data_dict):
    '''Check if `data_dict` is marked as a service resource (as
       opposed to a dataset resource). If it is, return True,
       otherwise False.'''

    iso_values = data_dict['iso_values']
    return 'service' in iso_values['resource-type']


def filter_tags(tags, simple_tag_list, complex_tag_list):
    '''Check for the presence of all elements of `tags` in `simple_tag_list`
       (each element just a string), if present remove from `complex_tag_list`
       (each element a dict) and return `complex_tag_list`.
       All occurrences of a tag in `complex_tag_list` are removed, not just
       the first one.'''

    for tag in tags:
        if tag in simple_tag_list:
            complex_tag = {'name': tag}
            while complex_tag in complex_tag_list:
                complex_tag_list.remove(complex_tag)

    return complex_tag_list


def extract_contact_info(data_dict):
    '''Extract `author`, `maintainer` and `maintainer_email` dataset metadata from
       the CSW resource's ISO representation.'''

    contact_info = {}
    iso_values = data_dict['iso_values']

    if 'responsible-organisation' in iso_values and iso_values['responsible-organisation']:
        resp_org = iso_values['responsible-organisation'][0]
        if 'organisation-name' in resp_org and resp_org['organisation-name']:
            contact_info['author'] = resp_org['organisation-name']
        if 'contact-info' in resp_org and 'email' in resp_org['contact-info'] and resp_org['contact-info']['email']:
            contact_info['maintainer_email'] = resp_org['contact-info']['email']
        if 'individual-name' in resp_org:
            contact_info['maintainer'] = resp_org['individual-name']

    return contact_info


def extract_license_and_attribution(data_dict):
    '''Extract `license_id` and `attribution_text` dataset metadata from
       the CSW resource's ISO representation.'''

    license_and_attribution = {}
    iso_values = data_dict['iso_values']

    if 'limitations-on-public-access' in iso_values:
        for restriction in iso_values['limitations-on-public-access']:
            try:
                structured = json.loads(restriction)
                license_and_attribution['license_id'] = structured['id']
                license_and_attribution['attribution_text'] = structured['quelle']
            except ValueError:
                LOG.info(f"could not parse as JSON: {restriction}")

    # internally, we use 'dl-de-by-2.0' as the id for
    # Datenlizenz Deutschland – Namensnennung – Version 2.0
    # However, FIS-Broker uses 'dl-by-de/2.0' (as per https://www.dcat-ap.de/def/licenses/).
    # We could eventually also use dl-by-de/2.0, but for now we need to convert.
    if 'license_id' in license_and_attribution:
        old_license_id = license_and_attribution['license_id']
        new_license_id = "dl-de-by-2.0"
        if (old_license_id == "dl-de-by-2-0" or
            old_license_id == "dl-de-/by-2-0" or
            old_license_id == "dl-by-de/2.0"):
            license_and_attribution['license_id'] = new_license_id
        LOG.info(f"replace license_id '{old_license_id}' with '{new_license_id}'")

    return license_and_attribution


def extract_reference_dates(data_dict):
    '''Extract `date_released` and `date_updated` dataset metadata from
       the CSW resource's ISO representation.'''

    reference_dates = {}
    iso_values = data_dict['iso_values']

    if 'dataset-reference-date' in iso_values and iso_values['dataset-reference-date']:
        for date in iso_values['dataset-reference-date']:
            if date['type'] == 'revision':
                LOG.info('found revision date, using as date_updated')
                reference_dates['date_updated'] = date['value']
            if date['type'] == 'creation':
                LOG.info('found creation date, using as date_released')
                reference_dates['date_released'] = date['value']
            if date['type'] == 'publication' and 'date_released' not in reference_dates:
                LOG.info('found publication date and no prior date_released, using as date_released')
                reference_dates['date_released'] = date['value']

        if 'date_released' not in reference_dates and 'date_updated' in reference_dates:
            LOG.info('date_released not set, but date_updated is: using date_updated as date_released')
            reference_dates['date_released'] = reference_dates['date_updated']

        # we always want to set date_updated as well, to prevent confusing
        # Datenportal's ckan_import module:
        if 'date_updated' not in reference_dates:
            reference_dates['date_updated'] = reference_dates['date_released']

    return reference_dates

def extract_url(resources):
    '''Picks an URL from the list of resources best suited for the dataset's `url` metadatum.'''

    url = None

    for resource in resources:
        internal_function = resource.get('internal_function')
        if internal_function == 'web_interface':
            return resource['url']
        elif internal_function == 'api':
            url = resource['url']

    return url

def extract_preview_markup(data_dict):
    '''If the dataset's ISO values contain a preview image, generate markdown
       for that and return. Else return None.'''

    iso_values = data_dict['iso_values']
    package_dict = data_dict['package_dict']

    preview_graphics = iso_values.get("browse-graphic", [])
    for preview_graphic in preview_graphics:
        preview_graphic_title = preview_graphic.get('description', None)
        if preview_graphic_title == "Vorschaugrafik":
            preview_graphic_title = f"Vorschaugrafik zu Datensatz '{package_dict['title']}'"
            preview_graphic_path = preview_graphic.get('file', None)
            if preview_graphic_path:
                preview_markup = f"![{preview_graphic_title}]({preview_graphic_path})"
                return preview_markup

    return None

def generate_title(data_dict):
    ''' We can have different service datasets with the same
    name. We don't want that, so we add the service resource's
    format to make the title unique.'''

    package_dict = data_dict['package_dict']

    title = package_dict['title']
    resources = package_dict['resources']

    main_resources = [resource for resource in resources if resource.get('main', False)]
    if main_resources:
        main_resource = main_resources.pop()
        resource_format = main_resource.get('format', None)
        if resource_format is not None:
            title = f"{title} - [{resource_format}]"

    return title

def generate_name(data_dict):
    '''Generate a unique name based on the package's title and FIS-Broker
       guid.'''

    iso_values = data_dict['iso_values']
    package_dict = data_dict['package_dict']

    name = munge_title_to_name(package_dict['title'])
    name = re.sub('-+', '-', name)
    # ensure we don't exceed the allowed name length of 100:
    # (100-len(guid_part)-1)
    name = name[:91].strip('-')

    guid = iso_values['guid']
    guid_part = guid.split('-')[0]
    name = f"{name}-{guid_part}"
    return name

def extras_as_list(extras_dict):
    '''Convert a simple extras dict to a list of key/value dicts.
       Values that are themselves lists or dicts (as opposed to strings)
       will be converted to JSON-strings.'''

    extras_list = []
    for key, value in extras_dict.items():
        if isinstance(value, (list, dict)):
            extras_list.append({'key': key, 'value': json.dumps(value)})
        else:
            extras_list.append({'key': key, 'value': value})

    return extras_list


class FisbrokerHarvester(CSWHarvester):
    '''Main plugin class of the ckanext-fisbroker extension.'''

    plugins.implements(IHarvester, inherit=True)
    plugins.implements(ISpatialHarvester, inherit=True)

    import_since_keywords = ["last_error_free", "big_bang"]

    def extras_dict(self, extras_list):
        '''Convert input `extras_list` to a conventional extras dict.'''
        extras_dict = {}
        for item in extras_list:
            extras_dict[item['key']] = item['value']
        return extras_dict

    def get_import_since_date(self, harvest_job):
        '''Get the `import_since` config as a string (property of
           the query constraint). Handle special values such as
           `last_error_free` and `big bang`.'''

        if not 'import_since' in self.source_config:
            return None
        import_since = self.source_config['import_since']
        if import_since == 'last_error_free':
            last_error_free_job = self.last_error_free_job(harvest_job)
            LOG.debug('Last error-free job: %r', last_error_free_job)
            if last_error_free_job:
                gather_time = (last_error_free_job.gather_started +
                               timedelta(hours=self.get_timedelta()))
                return gather_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            else:
                return None
        elif import_since == 'big_bang':
            # looking since big bang means no date constraint
            return None
        return import_since

    def get_constraints(self, harvest_job):
        '''Compute and get the query constraint for requesting datasets from
           FIS-Broker.'''
        date = self.get_import_since_date(harvest_job)
        if date:
            LOG.info(f"date constraint: {date}")
            date_query = PropertyIsGreaterThanOrEqualTo('modified', date)
            return [date_query]
        else:
            LOG.info("no date constraint")
            return []

    def get_timeout(self):
        '''Get the `timeout` config as a string (timeout threshold for requests
           to FIS-Broker).'''
        if 'timeout' in self.source_config:
            return int(self.source_config['timeout'])
        return TIMEOUT_DEFAULT

    def get_timedelta(self):
        '''Get the `timedelta` config as a string (timezone difference between
           FIS-Broker server and harvester server).'''
        if 'timedelta' in self.source_config:
            return int(self.source_config['timedelta'])
        return TIMEDELTA_DEFAULT

    # IHarvester

    def info(self):
        '''Provides a dict with basic metadata about this plugin.
           Implementation of ckanext.harvest.interfaces.IHarvester.info()
           https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/interfaces.py
        '''
        return {
            'name': HARVESTER_ID,
            'title': "FIS Broker",
            'description': "A harvester specifically for Berlin's FIS Broker geo data CSW service."
        }

    def validate_config(self, config):
        '''Implementation of ckanext.harvest.interfaces.IHarvester.validate_config()
           https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/interfaces.py
        '''
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'import_since' in config_obj:
                import_since = config_obj['import_since']
                try:
                    if import_since not in self.import_since_keywords:
                        datetime.strptime(import_since, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(f"'import_since' is not a valid date: '{import_since}'. Use ISO8601: YYYY-MM-DD or one of {', '.join(self.import_since_keywords)}.")

            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                try:
                    config_obj['timeout'] = int(timeout)
                except ValueError:
                    raise ValueError(
                        f"\'timeout\' is not valid: '{timeout}'. Please use whole numbers to indicate seconds until timeout.")

            if 'timedelta' in config_obj:
                _timedelta = config_obj['timedelta']
                try:
                    config_obj['timedelta'] = int(_timedelta)
                except ValueError:
                    raise ValueError(
                        f"'\'timedelta\' is not valid: '{_timedelta}'. Please use whole numbers to indicate timedelta between UTC and harvest source timezone.")

            config = json.dumps(config_obj, indent=2)

        except ValueError as error:
            raise error

        return CSWHarvester.validate_config(self, config)

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.CSW.gather')
        log.debug(f"FisbrokerPlugin gather_stage for job: {harvest_job}")
        # Get source URL
        url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        try:
            self._setup_csw_client(url)
        except Exception as e:
            log.debug(f"Error contacting the CSW server: {e}")
            self._save_gather_error(f"Error contacting the CSW server: {e}", harvest_job)
            return None

        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).\
                                    filter(HarvestObject.current==True).\
                                    filter(HarvestObject.harvest_source_id==harvest_job.source.id)
        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = set(guid_to_package_id.keys())

        # extract cql filter if any
        cql = self.source_config.get('cql')

        def get_identifiers(constraints=[]):
            guid_set = set()
            for identifier in self.csw.getidentifiers(page=10, outputschema=self.output_schema(),
                                                      cql=cql, constraints=constraints):
                try:
                    log.info(f"Got identifier {identifier} from the CSW")
                    if identifier is None:
                        log.error(f"CSW returned identifier {identifier}, skipping...")
                        continue

                    guid_set.add(identifier)
                except Exception as e:
                    self._save_gather_error(f"Error for the identifier {identifier} [{e}]", harvest_job)
                    continue

            return guid_set

        # first get the (date)constrained set of identifiers,
        # to figure what was added and/or changed
        # only those identifiers will be fetched
        log.debug(f"Starting gathering for {url} (constrained)")

        try:
            constraints = self.get_constraints(harvest_job)
            guids_in_harvest_constrained = get_identifiers(constraints)

            # then get the complete set of identifiers, to figure out
            # what was deleted
            log.debug(f"Starting gathering for {url} (unconstrained)")
            guids_in_harvest_complete = set()
            if (constraints == []):
                log.debug("There were no constraints, so GUIDS(unconstrained) == GUIDs(constrained)")
                guids_in_harvest_complete = guids_in_harvest_constrained
            else:
                guids_in_harvest_complete = get_identifiers()
        except Exception as e:
            log.error(f"Exception: {text_traceback()}")
            self._save_gather_error(
                f"Error gathering the identifiers from the CSW server [{str(e)}]", harvest_job)
            return None

        # new datasets are those that were returned by the (constrained) harvest AND are not in
        # already in the database
        new = guids_in_harvest_constrained - guids_in_db

        # deleted datasets are those that were in the database AND were not included in the 
        # (unconstrained) harvest
        delete = guids_in_db - guids_in_harvest_complete

        # changed datasets are those that were in the database AND were also included in the
        # (constrained) harvest
        change = guids_in_db & guids_in_harvest_constrained

        log.debug(f"|new GUIDs|: {len(new)}")
        log.debug(f"|deleted GUIDs|: {len(delete)}")
        log.debug(f"|changed GUIDs|: {len(change)}")

        ids = []
        for guid in new:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                extras=[HarvestObjectExtra(key='status', value='new')])
            obj.save()
            ids.append(obj.id)
        for guid in change:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HarvestObjectExtra(key='status', value='change')])
            obj.save()
            ids.append(obj.id)
        for guid in delete:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HarvestObjectExtra(key='status', value='delete')])
            model.Session.query(HarvestObject).\
                  filter_by(guid=guid).\
                  update({'current': False}, False)
            obj.save()
            ids.append(obj.id)

        if len(ids) == 0:
            self._save_gather_error("No records received from the CSW server", harvest_job)
            return None

        return ids

    def fetch_stage(self,harvest_object, retries=3, wait_time=5.0):

        # Check harvest object status
        status = self._get_object_extra(harvest_object, 'status')

        if status == 'delete':
            # No need to fetch anything, just pass to the import stage
            return True

        log = logging.getLogger(__name__ + '.CSW.fetch')
        log.debug(f"CswHarvester fetch_stage for object: {harvest_object.id}")

        url = harvest_object.source.url
        for attempt in range(1, retries + 1):
            try:
                log.info(f"Setting up CSW client: Attempt #{attempt} of {retries}")
                self._setup_csw_client(url)
                break
            except Exception as e:
                err = f"Error setting up CSW client: {text_traceback()}"
                if attempt < retries:
                    log.info(err)
                    log.info(f"waiting {wait_time} seconds...")
                    sleep(wait_time)
                    log.info(f"Repeating request! (attempt #{(attempt + 1)})")
                    continue
                else:
                    self._save_object_error(f"Error setting up CSW client: {e}",
                                            harvest_object)
                    return False

        identifier = harvest_object.guid
        try:
            record = self.csw.getrecordbyid([identifier], outputschema=self.output_schema())
        except Exception as e:
            self._save_object_error(f"Error getting the CSW record with GUID {identifier}: {str(e)}", harvest_object)
            return False

        if record is None:
            self._save_object_error(f"Empty record for GUID {identifier}", harvest_object)
            return False

        try:
            # Save the fetch contents in the HarvestObject
            # Contents come from csw_client already declared and encoded as utf-8
            # Remove original XML declaration
            content = re.sub('<\?xml(.*)\?>', '', record['xml'])

            harvest_object.content = content.strip()
            harvest_object.save()
        except Exception as e:
            self._save_object_error(f"Error saving the harvest object for GUID {identifier} [{e}]", harvest_object)
            return False

        log.debug(f"XML content saved (len {len(record['xml'])})")
        return True

    def import_stage(self, harvest_object):
        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
        }

        log = logging.getLogger(__name__ + '.import')
        log.debug(f"Import stage for harvest object: {harvest_object.id}")

        if not harvest_object:
            log.error("No harvest object received")
            return False

        self._set_source_config(harvest_object.source.config)

        if self.force_import:
            status = 'change'
        else:
            status = self._get_object_extra(harvest_object, 'status')

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
                          .filter(HarvestObject.guid==harvest_object.guid) \
                          .filter(HarvestObject.current==True) \
                          .first()

        if status == 'delete':
            # Delete package
            context.update({
                'ignore_auth': True,
            })
            toolkit.get_action('package_delete')(context, {'id': harvest_object.package_id})
            log.info(f"Deleted package {harvest_object.package_id} with guid {harvest_object.guid}")

            return True

        # Check if it is a non ISO document
        original_document = self._get_object_extra(harvest_object, 'original_document')
        original_format = self._get_object_extra(harvest_object, 'original_format')
        if original_document and original_format:
            #DEPRECATED use the ISpatialHarvester interface method
            self.__base_transform_to_iso_called = False
            content = self.transform_to_iso(original_document, original_format, harvest_object)
            if not self.__base_transform_to_iso_called:
                log.warn("Deprecation warning: calling transform_to_iso directly is deprecated. " +
                         "Please use the ISpatialHarvester interface method instead.")

            for harvester in plugins.PluginImplementations(ISpatialHarvester):
                content = harvester.transform_to_iso(original_document, original_format, harvest_object)

            if content:
                harvest_object.content = content
            else:
                self._save_object_error("Transformation to ISO failed", harvest_object, 'Import')
                return False
        else:
            if harvest_object.content is None:
                self._save_object_error(f"Empty content for object {harvest_object.id}", harvest_object, 'Import')
                return False

            # Validate ISO document
            is_valid, profile, errors = self._validate_document(harvest_object.content, harvest_object)
            if not is_valid:
                # If validation errors were found, import will stop unless
                # configuration per source or per instance says otherwise
                continue_import = toolkit.asbool(config.get('ckanext.spatial.harvest.continue_on_validation_errors', False)) or \
                    self.source_config.get('continue_on_validation_errors')
                if not continue_import:
                    return False

        # Parse ISO document
        try:
            iso_parser = ISODocument(harvest_object.content)
            iso_values = iso_parser.read_values()
        except Exception as e:
            self._save_object_error(f"Error parsing ISO document for object {harvest_object.id}: {six.text_type(e)}", harvest_object, 'Import')
            return False

        # Flag previous object as not current anymore
        if previous_object and not self.force_import:
            previous_object.current = False
            previous_object.add()

        # Update GUID with the one on the document
        iso_guid = iso_values['guid']
        if iso_guid and harvest_object.guid != iso_guid:
            # First make sure there already aren't current objects
            # with the same guid
            existing_object = model.Session.query(HarvestObject.id) \
                            .filter(HarvestObject.guid==iso_guid) \
                            .filter(HarvestObject.current==True) \
                            .first()
            if existing_object:
                self._save_object_error(f"Object {existing_object.id} already has this guid {iso_guid}", harvest_object, 'Import')
                return False

            harvest_object.guid = iso_guid
            harvest_object.add()

        # Generate GUID if not present (i.e. it's a manual import)
        if not harvest_object.guid:
            m = hashlib.md5()
            m.update(harvest_object.content.encode('utf8', 'ignore'))
            harvest_object.guid = m.hexdigest()
            harvest_object.add()

        # Get document modified date
        try:
            metadata_modified_date = dateutil.parser.parse(iso_values['metadata-date'], ignoretz=True)
        except ValueError:
            self._save_object_error(f"Could not extract reference date for object {harvest_object.id} ({iso_values['metadata-date']})", harvest_object, 'Import')
            return False

        harvest_object.metadata_modified_date = metadata_modified_date
        harvest_object.add()


        # Build the package dict
        package_dict = self.get_package_dict(iso_values, harvest_object)
        for harvester in plugins.PluginImplementations(ISpatialHarvester):
            package_dict = harvester.get_package_dict(context, {
                'package_dict': package_dict,
                'iso_values': iso_values,
                'xml_tree': iso_parser.xml_tree,
                'harvest_object': harvest_object,
            })
        if not package_dict:
            log.error(f"No package dict returned, aborting import for object {harvest_object.id}")
            return False

        if package_dict == 'skip':
            log.info(f"Skipping import for object {harvest_object.id}")
            return 'unchanged'

        # Create / update the package
        context.update({
           'extras_as_string': True,
           'api_version': '2',
           'return_id_only': True})

        if self._site_user and context['user'] == self._site_user['name']:
            context['ignore_auth'] = True


        # The default package schema does not like Upper case tags
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, six.text_type]

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        package_name = package_dict['name']
        package = model.Package.get(package_name)

        # there are cases where a harvested dataset with a name identical to 
        # that of a record in FIS-Broker, but there is no previous harvest object with
        # that record's guid. (not sure why that happens. guids changed in FIS-Broker?)
        # In this case, gather_stage will set `status = 'new'` on the HO.
        # Here in import_stage, package_create will fail, because
        # 'Validation Error: That URL is already in use.' (URL is build from name).
        # So check if package_dict['name'] already exists. If so, change status to
        # 'change'.
        if status == 'new' and package:
            log.info(f"Resource with guid {harvest_object.guid} looks new, but there is a package with the same name: '{package_name}'. Changing that package instead of creating a new one.")
            status = 'change'
            harvest_object.package_id = package.as_dict()['id']
            harvest_object.add()

        # if we cannot find the package by name, maybe we can find it by id
        if not package:
            package = model.Package.get(harvest_object.package_id)

        # It can also happen that gather_stage set `status = 'change'`, because there
        # already is a previous HarvestObject with a guid identical to the one
        # encountered during gathering. If the dataset that was created through that
        # HarvestObject no longer exists (it was purged independently from the harvester),
        # we will get an error (trying to update a non-existing package). In that case,
        # we need to create a new one.
        if status == 'change' and not package:
            # apparently the package was purged, a new one has to be created
            log.info(f"There is no package named '{package_name}' for guid {harvest_object.guid}, creating a new one.")
            status = 'new'


        if status == 'new':
            package_schema = logic.schema.default_create_package_schema()
            package_schema['tags'] = tag_schema
            context['schema'] = package_schema

            # We need to explicitly provide a package ID, otherwise ckanext-spatial
            # won't be be able to link the extent to the package.
            package_dict['id'] = six.text_type(uuid.uuid4())
            package_schema['id'] = [six.text_type]

            # Save reference to the package on the object
            harvest_object.package_id = package_dict['id']
            harvest_object.add()
            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            try:
                package_id = toolkit.get_action('package_create')(context, package_dict)
                log.info('Created new package %s with guid %s', package_id, harvest_object.guid)
            except toolkit.ValidationError as e:
                self._save_object_error('Validation Error: %s' % six.text_type(e.error_summary), harvest_object, 'Import')
                return False

        elif status == 'change':

            # if the package was deleted, make it active again (state in FIS-Broker takes
            # precedence)
            if package.state == "deleted":
                log.info(f"The package named {package_dict['name']} was deleted, activating it again.")
                package.state = "active"

            # Check if the modified date is more recent
            if not self.force_import and previous_object and harvest_object.metadata_modified_date <= previous_object.metadata_modified_date:

                # Assign the previous job id to the new object to
                # avoid losing history
                harvest_object.harvest_job_id = previous_object.job.id
                harvest_object.add()

                # Delete the previous object to avoid cluttering the object table
                previous_object.delete()

                # Reindex the corresponding package to update the reference to the
                # harvest object
                if ((config.get('ckanext.spatial.harvest.reindex_unchanged', True) != 'False'
                    or self.source_config.get('reindex_unchanged') != 'False')
                    and harvest_object.package_id):
                    context.update({'validate': False, 'ignore_auth': True})
                    try:
                        package_dict = logic.get_action('package_show')(context,
                            {'id': harvest_object.package_id})
                    except toolkit.ObjectNotFound:
                        pass
                    else:
                        for extra in package_dict.get('extras', []):
                            if extra['key'] == 'harvest_object_id':
                                extra['value'] = harvest_object.id
                        if package_dict:
                            package_index = PackageSearchIndex()
                            package_index.index_package(package_dict)

                log.info(f"Document with GUID {harvest_object.guid} unchanged, skipping...")
            else:
                package_schema = logic.schema.default_update_package_schema()
                package_schema['tags'] = tag_schema
                context['schema'] = package_schema

                package_dict['id'] = harvest_object.package_id
                try:
                    package_id = toolkit.get_action('package_update')(context, package_dict)
                    log.info(f"Updated package {package_id} with guid {harvest_object.guid}")
                except toolkit.ValidationError as e:
                    self._save_object_error(f"Validation Error: {six.text_type(e.error_summary)}", harvest_object, 'Import')
                    return False

        model.Session.commit()

        return True

    def _setup_csw_client(self, url):
        self.csw = CswService(url, self.get_timeout())


    # ISpatialHarvester

    def get_validators(self):
        '''Implementation of ckanext.spatial.interfaces.ISpatialHarvester.get_validators().
        https://github.com/ckan/ckanext-spatial/blob/master/ckanext/spatial/interfaces.py
        '''
        LOG.debug("--------- get_validators ----------")
        return [AlwaysValid]

    def get_package_dict(self, context, data_dict):
        '''Implementation of ckanext.spatial.interfaces.ISpatialHarvester.get_package_dict().
        https://github.com/ckan/ckanext-spatial/blob/master/ckanext/spatial/interfaces.py
        '''

        def remember_error(harvest_object, error_dict):
            '''
            Add a HarvestObjectExtra to the harvest object to remember the reason for 
            a failed import.
            '''
            if harvest_object:
                harvest_object.extras.append(HarvestObjectExtra(key='error',value=json.dumps(error_dict)))

        LOG.debug("--------- get_package_dict ----------")

        if hasattr(data_dict, '__getitem__'):

            package_dict = data_dict['package_dict']
            iso_values = data_dict['iso_values']
            harvest_object = data_dict.get('harvest_object')

            LOG.debug(iso_values['title'])

            # checking if marked for Open Data
            if not marked_as_opendata(data_dict):
                LOG.debug("no 'opendata' tag, skipping dataset ...")
                remember_error(harvest_object, {'code': 1, 'description': 'not tagged as open data'})
                return 'skip'
            LOG.debug("this is tagged 'opendata', continuing ...")

            # we're only interested in service resources
            if not marked_as_service_resource(data_dict):
                LOG.debug("this is not a service resource, skipping dataset ...")
                remember_error(harvest_object, {'code': 2, 'description': 'not a service resource'})
                return 'skip'
            LOG.debug("this is a service resource, continuing ...")

            extras = self.extras_dict(package_dict['extras'])

            # filter out various tags
            to_remove = [u'äöü', u'opendata', u'open data']
            package_dict['tags'] = filter_tags(to_remove, iso_values['tags'], package_dict['tags'])

            # Veröffentlichende Stelle / author
            # Datenverantwortliche Stelle / maintainer
            # Datenverantwortliche Stelle Email / maintainer_email

            contact_info = extract_contact_info(data_dict)

            if 'author' in contact_info:
                package_dict['author'] = contact_info['author']
            else:
                LOG.error('could not determine responsible organisation name, skipping ...')
                remember_error(harvest_object, {'code': 3, 'description': 'no organisation name'})
                return 'skip'

            if 'maintainer_email' in contact_info:
                package_dict['maintainer_email'] = contact_info['maintainer_email']
            else:
                LOG.error('could not determine responsible organisation email, skipping ...')
                remember_error(harvest_object, {'code': 4, 'description': 'no responsible organisation email'})
                return 'skip'

            if 'maintainer' in contact_info:
                package_dict['maintainer'] = contact_info['maintainer']

            # Veröffentlichende Stelle Email / author_email
            # Veröffentlichende Person / extras.username

            # license_id

            license_and_attribution = extract_license_and_attribution(data_dict)

            if 'license_id' not in license_and_attribution:
                LOG.error('could not determine license code, skipping ...')
                remember_error(harvest_object, {'code': 5, 'description': 'could not determine license code'})
                return 'skip'

            package_dict['license_id'] = license_and_attribution['license_id']

            if 'attribution_text' in license_and_attribution:
                extras['attribution_text'] = license_and_attribution['attribution_text']

            # extras.date_released / extras.date_updated

            reference_dates = extract_reference_dates(data_dict)

            if 'date_released' not in reference_dates:
                LOG.error('could not get anything for date_released from ISO values, skipping ...')
                remember_error(harvest_object, {'code': 6, 'description': 'no release date'})
                return 'skip'

            extras['date_released'] = reference_dates['date_released']

            if 'date_updated' in reference_dates:
                extras['date_updated'] = reference_dates['date_updated']

            # resources

            annotator = FISBrokerResourceAnnotator()
            resources = annotator.annotate_all_resources(package_dict['resources'])
            package_dict['resources'] = helpers.uniq_resources_by_url(resources)

            # URL
            package_dict['url'] = extract_url(package_dict['resources'])

            # Preview graphic
            preview_markup = extract_preview_markup(data_dict)
            if preview_markup:
                preview_markup = "\n\n" + preview_markup
                package_dict['notes'] += preview_markup

            # title
            package_dict['title'] = generate_title(data_dict)

            # name
            package_dict['name'] = generate_name(data_dict)

            # internal dataset type:

            extras['berlin_type'] = 'datensatz'

            # source:

            extras['berlin_source'] = 'harvest-fisbroker'

            # always put in 'geo' group

            package_dict['groups'] = [{'name': 'geo'}]

            # geographical_granularity

            extras['geographical_granularity'] = "Berlin"
            # TODO: can we determine this from the ISO values?

            # geographical_coverage

            extras['geographical_coverage'] = "Berlin"
            # TODO: can we determine this from the ISO values?

            # temporal_granularity

            extras['temporal_granularity'] = "Keine"
            # TODO: can we determine this from the ISO values?

            # temporal_coverage_from
            if 'temporal-extent-begin' in extras:
                extras['temporal_coverage_from'] = extras['temporal-extent-begin']

            # temporal_coverage_to
            if 'temporal-extent-end' in extras:
                extras['temporal_coverage_to'] = extras['temporal-extent-end']

            # LOG.debug("----- data after get_package_dict -----")
            # LOG.debug(package_dict)

            # extras
            package_dict['extras'] = extras_as_list(extras)

            return package_dict
        else:
            LOG.debug('calling get_package_dict on CSWHarvester')
            return CSWHarvester.get_package_dict(self, context, data_dict)

    @classmethod
    def last_error_free_job(cls, harvest_job) -> HarvestJob:
        '''Override last_error_free_job() from
           ckanext.harvest.harvesters.base.HarvesterBase to filter out
           jobs that were created by a reimport action.'''

        jobs = \
            model.Session.query(HarvestJob) \
                 .filter(HarvestJob.source == harvest_job.source) \
                 .filter(HarvestJob.gather_started != None) \
                 .filter(HarvestJob.status == 'Finished') \
                 .filter(HarvestJob.id != harvest_job.id) \
                 .filter(
                     ~exists().where(
                         HarvestGatherError.harvest_job_id == HarvestJob.id)) \
                 .order_by(HarvestJob.gather_started.desc())

        # now check them until we find one with no fetch/import errors,
        # which isn't a reimport job
        for job in jobs:
            if helpers.is_reimport_job(job):
                continue
            for obj in job.objects:
                if obj.current is False and \
                        obj.report_status != 'not modified':
                    # unsuccessful, so go onto the next job
                    break
            else:
                return job


class AlwaysValid(BaseValidator):
    '''A validator that always validates. Needed because FIS-Broker-XML
       sometimes doesn't validate, but we don't want to break the harvest on
       that.'''

    name = 'always-valid'

    title = 'No validation performed'

    @classmethod
    def is_valid(cls, xml):
        '''Implements ckanext.spatial.validation.validation.BaseValidator.is_valid().'''

        return True, []
