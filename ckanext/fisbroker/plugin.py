# coding: utf-8

import logging
from datetime import datetime
from datetime import timedelta
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.spatial.interfaces import ISpatialHarvester
from ckanext.spatial.harvesters.csw import CSWHarvester
from ckanext.spatial.validation.validation import BaseValidator
from owslib.fes import PropertyIsGreaterThanOrEqualTo

import json

log = logging.getLogger(__name__)

# http://fbinter.stadt-berlin.de/fb/csw

class FisbrokerPlugin(CSWHarvester):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(ISpatialHarvester, inherit=True)

    import_since_keywords = [ "last_error_free", "big_bang" ]

    def extras_dict(self, extras_list):
        extras_dict = {}
        for item in extras_list:
            extras_dict[item['key']] = item['value']
        return extras_dict

    def get_import_since_date(self, harvest_job):
        if not 'import_since' in self.source_config:
            return None
        import_since = self.source_config['import_since']
        if (import_since == 'last_error_free'):
            last_error_free_job = self.last_error_free_job(harvest_job)
            log.debug('Last error-free job: %r', last_error_free_job)
            if last_error_free_job:
                gather_time = (last_error_free_job.gather_started + timedelta(hours=self.get_timedelta()))
                return gather_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            else:
                return None
        elif (import_since == 'big_bang'):
            # looking since big bang means no date constraint
            return None
        return import_since

    def get_constraints(self, harvest_job):
        date = self.get_import_since_date(harvest_job)
        if date:
            log.info("date constraint: %s" % date)
            date_query = PropertyIsGreaterThanOrEqualTo('modified', date)
            return [date_query]
        else:
            log.info("no date constraint")
            return []


    def get_timeout(self):
        timeout = 20 # default
        if 'timeout' in self.source_config:
            timeout = int(self.source_config['timeout'])
        log.info("timeout: %s" % timeout)
        return timeout

    def get_timedelta(self):
        timedelta = 0 # default
        if 'timedelta' in self.source_config:
            timedelta = int(self.source_config['timedelta'])
        log.info("timedelta: %s" % timedelta)
        return timedelta

    # IHarvester

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'import_since' in config_obj:
                import_since = config_obj['import_since']
                try:
                    if (import_since not in self.import_since_keywords):
                        datetime.strptime(import_since, "%Y-%m-%d")
                except ValueError:
                    raise ValueError('\'import_since\' is not a valid date: \'%s\'. Use ISO8601: YYYY-MM-DD or one of %s.' % (import_since, self.import_since_keywords) )

            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                try:
                    config_obj['timeout'] = int(timeout)
                except ValueError:
                    raise ValueError('\'timeout\' is not valid: \'%s\'. Please use whole numbers to indicate seconds until timeout.' % timeout)

            if 'timedelta' in config_obj:
                timedelta = config_obj['timedelta']
                try:
                    config_obj['timedelta'] = int(timedelta)
                except ValueError:
                    raise ValueError('\'timedelta\' is not valid: \'%s\'. Please use whole numbers to indicate timedelta between UTC and harvest source timezone.' % timedelta)

            config = json.dumps(config_obj, indent=2)

        except ValueError, e:
            raise e

        return CSWHarvester.validate_config(self, config)

    # IConfigurer

    def update_config(self, config):
        toolkit.add_template_directory(config, 'templates')
        toolkit.add_public_directory(config, 'public')
        toolkit.add_resource('fanstatic', 'fisbroker')
        config['ckan.spatial.validator.profiles'] = 'always-valid'

    def info(self):
        return {
            'name': 'fisbroker',
            'title': 'FIS Broker',
            'description': 'A harvester specifically for Berlin\'s FIS Broker geo data CSW service.'
            }

    def get_validators(self):
        log.debug("--------- get_validators ----------")
        return [AlwaysValid]

    def get_package_dict(self, context, data_dict):
        log.debug("--------- get_package_dict ----------")

        if hasattr(data_dict, '__getitem__'):

            package_dict = data_dict['package_dict']
            iso_values = data_dict['iso_values']

            log.debug(iso_values['title'])

            # checking for 'opendata' tag
            tags = iso_values['tags']
            if not 'opendata' in tags:
                log.debug("no 'opendata' tag, skipping dataset ...")
                return 'skip'
            log.debug("this is tagged 'opendata', continuing ...")
            
            # we're only interested in service resources
            log.debug("resource type: {0}".format(iso_values['resource-type']))
            if not 'service' in iso_values['resource-type']:
                log.debug("this is not a service resource, skipping dataset ...")
                return 'skip'
            log.debug("this is a service resource, continuing ...")
            

            # log.debug(iso_values)
            # log.debug(package_dict)
            extras = self.extras_dict(package_dict['extras'])
            # log.debug(extras)

            # filter out 'äöü' tag
            if u'äöü' in tags:
                package_dict['tags'].remove({'name': u'äöü'})

            # filter out 'opendata' tags, we know it's open data
            if u'opendata' in tags:
                package_dict['tags'].remove({'name': u'opendata'})
            if u'open data' in tags:
                package_dict['tags'].remove({'name': u'open data'})


            # Veröffentlichende Stelle / author
            # Datenverantwortliche Stelle / maintainer
            # Datenverantwortliche Stelle Email / maintainer_email

            if 'responsible-organisation' in iso_values:
                resp_org = iso_values['responsible-organisation'][0]
                if 'organisation-name' in resp_org:
                    package_dict['author'] = resp_org['organisation-name']
                else:
                    log.error('could not determine responsible organisation name, skipping ...')
                    return 'skip'
                if 'contact-info' in resp_org:
                    package_dict['maintainer_email'] = resp_org['contact-info']['email']
                else:
                    log.error('could not determine responsible organisation email, skipping ...')
                    return 'skip'
                if 'individual-name' in resp_org:
                    package_dict['maintainer'] = resp_org['individual-name']
            else:
                log.error('could not determine responsible organisation, skipping ...')
                return 'skip'

            # Veröffentlichende Stelle Email / author_email
            # Veröffentlichende Person / extras.username
            
            # license_id

            if 'limitations-on-public-access' in iso_values:
                for restriction in iso_values['limitations-on-public-access']:
                    log.info(restriction)
                    try:
                        structured = json.loads(restriction)
                        package_dict['license_id'] = structured['id']
                        extras['attribution_text'] = structured['quelle']
                    except (ValueError) as e:
                        log.info('could not parse as JSON: %s' % restriction)

            if not 'license_id' in package_dict:
                log.error('could not determine license code, skipping ...')
                return 'skip'

            if package_dict['license_id'] is "dl-de-by-2-0":
                package_dict['license_id'] = "dl-de-by-2.0"
                log.info("fix bad DL-DE-BY id")

            # extras.date_released / extras.date_updated

            for date in iso_values['dataset-reference-date']:
                if date['type'] == 'revision':
                    log.info('found revision date, using as date_updated')
                    extras['date_updated'] = date['value']
                if date['type'] == 'creation':
                    log.info('found creation date, using as date_released')
                    extras['date_released'] = date['value']
                if date['type'] == 'publication' and not 'date_released' in extras:
                    log.info('found publication date and no prior date_released, using as date_released')
                    extras['date_released'] = date['value']

            if not 'date_released' in extras and 'date_updated' in extras:
                log.info('date_released not set, but date_updated is: using date_updated as date_released')
                extras['date_released'] = extras['date_updated']
                # extras.pop('date_updated', None)

            if not 'date_released' in extras:
                log.error('could not get anything for date_released from ISO values, skipping ...')
                return 'skip'

            # we always want to set date_updated as well, to prevent confusing 
            # Datenportal's ckan_import module:
            if not 'date_updated' in extras:
                extras['date_updated'] = extras['date_released']

            # URL - strange that this isn't set by default
            url = iso_values['url']
            package_dict['url'] = url

            # resources
            def convert_resource(resource):
                if "/feed/" in resource['url']:
                    resource['name'] = "Atom Feed"
                    resource['description'] = "Atom Feed"
                    resource['format'] = "Atom"
                    resource['main'] = True
                elif "/wfs/" in resource['url']:
                    resource['name'] = "WFS Service"
                    resource['description'] = "WFS Service"
                    resource['format'] = "WFS"
                    resource['url'] += "?service=wfs&request=GetCapabilities"
                    resource['main'] = True
                elif "/wms/" in resource['url']:
                    resource['name'] = "WMS Service"
                    resource['description'] = "WMS Service"
                    resource['format'] = "WMS"
                    resource['url'] += "?service=wms&request=GetCapabilities"
                    resource['main'] = True
                elif resource['url'].startswith('https://fbinter.stadt-berlin.de/fb'):
                    resource['name'] = "Serviceseite im FIS-Broker"
                    resource['format'] = "HTML"
                    resource['description'] = "Serviceseite im FIS-Broker"
                elif resource['description']:
                    resource['name'] = resource['description']
                    resource['main'] = False
                else:
                    resource = None
                return resource

            resources = [convert_resource(resource) for resource in package_dict['resources']]
            resources = filter(None, resources)
            package_dict['resources'] = resources

            # We can have different service datasets with the same
            # name. We don't want that, so we add the service resource's
            # format to make the title and name unique.
            resource_format = resources[0]['format']
            if (resource_format is not None):
                package_dict['title'] = u"{0} - [{1}]".format(package_dict['title'], resource_format)
                package_dict['name'] = self._gen_new_name("{0}-{1}".format(package_dict['name'], resource_format.lower()))
                log.info('package name set to: %s' % package_dict['name'])


            # internal dataset type:

            extras['berlin_type'] = 'datensatz'

            # source:

            extras['berlin_source'] = 'harvest-fisbroker'

            # always put in 'geo' group

            package_dict['groups'] = [ {'name': 'geo' } ] 

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

            # log.debug("----- data after get_package_dict -----")
            # log.debug(package_dict)

            extras_as_list = []
            for key, value in extras.iteritems():
                if isinstance(value, (list, dict)):
                    extras_as_list.append({'key': key, 'value': json.dumps(value)})
                else:
                    extras_as_list.append({'key': key, 'value': value})

            package_dict['extras'] = extras_as_list

            return package_dict
        else:
            log.debug('calling get_package_dict on CSWHarvester')
            return CSWHarvester.get_package_dict(self, context, data_dict)



class AlwaysValid(BaseValidator):

    name = 'always-valid'

    title = 'No validation performed'

    @classmethod
    def is_valid(cls, xml):

        return True, []


