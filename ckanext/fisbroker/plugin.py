# coding: utf-8

import logging
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.spatial.interfaces import ISpatialHarvester
from ckanext.spatial.harvesters.csw import CSWHarvester
from ckanext.spatial.validation.validation import BaseValidator

import json

log = logging.getLogger(__name__)

# http://fbinter.stadt-berlin.de/fb/csw

class FisbrokerPlugin(CSWHarvester):
    plugins.implements(plugins.IConfigurer)

    plugins.implements(ISpatialHarvester, inherit=True)

    def extras_dict(self, extras_list):
        extras_dict = {}
        for item in extras_list:
            extras_dict[item['key']] = item['value']
        return extras_dict

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
                    except (ValueError) as e:
                        log.info('could not parse as JSON: %s' % restriction)

            if not 'license_id' in package_dict:
                log.error('could not determine license code, skipping ...')
                return 'skip'

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
            resources = package_dict['resources']
            delete = None
            # set resource names and formats based on URLs
            # let's hope this is regular...
            for resource in resources:
                if "/feed/" in resource['url']:
                    resource['name'] = "Atom Feed"
                    resource['description'] = "Atom Feed"
                    resource['format'] = "Atom"
                elif "/wfs/" in resource['url']:
                    resource['name'] = "WFS Service"
                    resource['description'] = "WFS Service"
                    resource['format'] = "WFS"
                    resource['url'] += "?service=wfs&request=GetCapabilities"
                elif "/wms/" in resource['url']:
                    resource['name'] = "WMS Service"
                    resource['description'] = "WMS Service"
                    resource['format'] = "WMS"
                    resource['url'] += "?service=wms&request=GetCapabilities"
                else:
                    # If the resource is none of the above, it's just the 
                    # dataset page in FIS-Broker. We don't want that as
                    # a resource.
                    delete = resource

            if delete:
                resources.remove(delete)

            # We can have different service datasets with the same
            # name. We don't want that, so we add the service resource's
            # format to make the title and name unique.
            resource_format = resources[0]['format']
            if (resource_format is not None):
                package_dict['title'] = u"{0} - [{1}]".format(package_dict['title'], resource_format)
                package_dict['name'] = "{0}-{1}".format(package_dict['name'], resource_format.lower())


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


