# coding: utf-8

from datetime import datetime, timedelta
import json
import logging
import os
import re

from owslib.fes import PropertyIsGreaterThanOrEqualTo

from ckan.lib.munge import munge_title_to_name
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.spatial.interfaces import ISpatialHarvester
from ckanext.spatial.harvesters.csw import CSWHarvester
from ckanext.spatial.validation.validation import BaseValidator
from ckanext.fisbroker import HARVESTER_ID
from ckanext.fisbroker.fisbroker_resource_converter import FISBrokerResourceConverter
import ckanext.fisbroker.helper as helpers

LOG = logging.getLogger(__name__)

# https://fbinter.stadt-berlin.de/fb/csw


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
                LOG.info('could not parse as JSON: %s', restriction)

    # fix bad DL-DE-BY id, maybe remove eventually?
    if 'license_id' in license_and_attribution and license_and_attribution['license_id'] == "dl-de-by-2-0":
        license_and_attribution['license_id'] = "dl-de-by-2.0"
        LOG.info("fix bad DL-DE-BY id")

    return license_and_attribution


def extract_reference_dates(data_dict):
    '''Extract `date_released` and `date_updated` dataset metadata from
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
        if preview_graphic_title == u"Vorschaugrafik":
            preview_graphic_title = u"Vorschaugrafik zu Datensatz '{}'".format(
                package_dict['title'])
            preview_graphic_path = preview_graphic.get('file', None)
            if preview_graphic_path:
                preview_markup = u"![{}]({})".format(
                    preview_graphic_title, preview_graphic_path)
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
    main_resource = main_resources.pop()
    resource_format = main_resource.get('format', None)
    if resource_format is not None:
        title = u"{0} - [{1}]".format(title, resource_format)

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
    name = "{0}-{1}".format(name, guid_part)
    return name

def extras_as_list(extras_dict):
    '''Convert a simple extras dict to a list of key/value dicts.
       Values that are themselves lists or dicts (as opposed to strings)
       will be converted to JSON-strings.'''

    extras_list = []
    for key, value in extras_dict.iteritems():
        if isinstance(value, (list, dict)):
            extras_list.append({'key': key, 'value': json.dumps(value)})
        else:
            extras_list.append({'key': key, 'value': value})

    return extras_list


class FisbrokerPlugin(CSWHarvester):
    '''Main plugin class of the ckanext-fisbroker extension.'''

    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IRoutes, inherit=True)
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
            LOG.info("date constraint: %s", date)
            date_query = PropertyIsGreaterThanOrEqualTo('modified', date)
            return [date_query]
        else:
            LOG.info("no date constraint")
            return []

    def get_timeout(self):
        '''Get the `timeout` config as a string (timeout threshold for requests 
           to FIS-Broker).'''
        timeout = 20  # default
        if 'timeout' in self.source_config:
            timeout = int(self.source_config['timeout'])
        return timeout

    def get_timedelta(self):
        '''Get the `timedelta` config as a string (timezone difference between
           FIS-Broker server and harvester server).'''
        _timedelta = 0  # default
        if 'timedelta' in self.source_config:
            _timedelta = int(self.source_config['timedelta'])
        return _timedelta

    # IHarvester

    def info(self):
        '''Provides a dict with basic metadata about this plugin.
           Implementation of ckanext.harvest.interfaces.IHarvester.info()
           https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/interfaces.py
        '''
        return {
            'name': HARVESTER_ID,
            'title': 'FIS Broker',
            'description': 'A harvester specifically for Berlin\'s FIS Broker geo data CSW service.'
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
                    raise ValueError('\'import_since\' is not a valid date: \'%s\'. Use ISO8601: YYYY-MM-DD or one of %s.' % (
                        import_since, self.import_since_keywords))

            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                try:
                    config_obj['timeout'] = int(timeout)
                except ValueError:
                    raise ValueError(
                        '\'timeout\' is not valid: \'%s\'. Please use whole numbers to indicate seconds until timeout.' % timeout)

            if 'timedelta' in config_obj:
                timedelta = config_obj['timedelta']
                try:
                    config_obj['timedelta'] = int(timedelta)
                except ValueError:
                    raise ValueError(
                        '\'timedelta\' is not valid: \'%s\'. Please use whole numbers to indicate timedelta between UTC and harvest source timezone.' % timedelta)

            config = json.dumps(config_obj, indent=2)

        except ValueError as error:
            raise error

        return CSWHarvester.validate_config(self, config)

    # IConfigurer

    def update_config(self, config):
        '''
        Implementation of
        https://docs.ckan.org/en/latest/extensions/plugin-interfaces.html#ckan.plugins.interfaces.IConfigurer.update_config
        '''
        toolkit.add_template_directory(config, os.path.join('theme', 'templates'))
        toolkit.add_public_directory(config, 'public')
        toolkit.add_resource('fanstatic', 'fisbroker')
        config['ckan.spatial.validator.profiles'] = 'always-valid'

    # -------------------------------------------------------------------
    # Implementation ITemplateHelpers
    # -------------------------------------------------------------------

    def get_helpers(self):
        '''
        Implementation of
        https://docs.ckan.org/en/latest/extensions/plugin-interfaces.html#ckan.plugins.interfaces.ITemplateHelpers.get_helpers
        '''
        return {
            'berlin_is_fisbroker_package': helpers.is_fisbroker_package,
            'berlin_fisbroker_guid': helpers.fisbroker_guid,
        }

    # IRoutes:

    def before_map(self, map_):
        """
        Implementation of
        https://docs.ckan.org/en/latest/extensions/plugin-interfaces.html#ckan.plugins.interfaces.IRoutes.before_map
        """
        map_.connect(
            '/dataset/{package_id}/reimport',
            controller='ckanext.fisbroker.controller:FISBrokerController',
            action='reimport_browser')
        map_.connect(
            '/api/harvest/reimport',
            controller='ckanext.fisbroker.controller:FISBrokerController',
            action='reimport_api')

        return map_

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
        LOG.debug("--------- get_package_dict ----------")

        if hasattr(data_dict, '__getitem__'):

            package_dict = data_dict['package_dict']
            iso_values = data_dict['iso_values']

            LOG.debug(iso_values['title'])

            # checking if marked for Open Data
            if not marked_as_opendata(data_dict):
                LOG.debug("no 'opendata' tag, skipping dataset ...")
                return 'skip'
            LOG.debug("this is tagged 'opendata', continuing ...")

            # we're only interested in service resources
            if not marked_as_service_resource(data_dict):
                LOG.debug("this is not a service resource, skipping dataset ...")
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
                return 'skip'

            if 'maintainer_email' in contact_info:
                package_dict['maintainer_email'] = contact_info['maintainer_email']
            else:
                LOG.error('could not determine responsible organisation email, skipping ...')
                return 'skip'

            if 'maintainer' in contact_info:
                package_dict['maintainer'] = contact_info['maintainer']

            # Veröffentlichende Stelle Email / author_email
            # Veröffentlichende Person / extras.username

            # license_id

            license_and_attribution = extract_license_and_attribution(data_dict)

            if 'license_id' not in license_and_attribution:
                LOG.error('could not determine license code, skipping ...')
                return 'skip'

            package_dict['license_id'] = license_and_attribution['license_id']

            if 'attribution_text' in license_and_attribution:
                extras['attribution_text'] = license_and_attribution['attribution_text']

            # extras.date_released / extras.date_updated

            reference_dates = extract_reference_dates(data_dict)

            if 'date_released' not in reference_dates:
                LOG.error('could not get anything for date_released from ISO values, skipping ...')
                return 'skip'

            extras['date_released'] = reference_dates['date_released']

            if 'date_updated' in reference_dates:
                extras['date_updated'] = reference_dates['date_updated']

            converter = FISBrokerResourceConverter()
            resources = [converter.convert_resource(resource)
                         for resource in package_dict['resources']]
            resources = filter(None, resources)
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

            # temporal_coverage-from
            # TODO: can we determine this from the ISO values?
            # shold be iso_values['temporal-extent-begin']
            # which is derived from:
            # gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement
            # but that doesn't show up anywhere in FIS Broker...

            # temporal_coverage-to
            # TODO: can we determine this from the ISO values?
            # shold be iso_values['temporal-extent-end']

            # LOG.debug("----- data after get_package_dict -----")
            # LOG.debug(package_dict)

            # extras
            package_dict['extras'] = extras_as_list(extras)

            return package_dict
        else:
            LOG.debug('calling get_package_dict on CSWHarvester')
            return CSWHarvester.get_package_dict(self, context, data_dict)


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
