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

        # log.debug(data_dict)
        # methods = [method for method in dir(data_dict) if callable(getattr(data_dict, method))]
        # log.debug(methods)

        # foo = data_dict.as_dict()

        if hasattr(data_dict, '__getitem__'):

            package_dict = data_dict['package_dict']
            iso_values = data_dict['iso_values']

            # log.debug("----- data before get_package_dict -----")
            # log.debug(package_dict)
            # log.debug(iso_values)

            # checking for 'opendata' tag
            tags = iso_values['tags']
            if not 'opendata' in tags:
                log.debug("no 'opendata' tag, skipping dataset ...")
                return False

            log.debug(iso_values)
            log.debug(package_dict)
            extras = self.extras_dict(package_dict['extras'])
            log.debug(extras)

            # Veröffentlichende Stelle / author
            # Datenverantwortliche Stelle / maintainer
            # Datenverantwortliche Stelle Email / maintainer_email

            if 'responsible-organisation' in iso_values:
                resp_org = iso_values['responsible-organisation'][0]
                if 'organisation-name' in resp_org:
                    package_dict['author'] = resp_org['organisation-name']
                else:
                    log.error('could not determine responsible organisation name')
                    return False
                if 'contact-info' in resp_org:
                    package_dict['maintainer_email'] = resp_org['contact-info']['email']
                else:
                    log.error('could not determine responsible organisation email')
                    return False
                if 'individual-name' in resp_org:
                    package_dict['maintainer'] = resp_org['individual-name']
            else:
                log.error('could not determine responsible organisation')
                return False

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
                log.error('could not determine license code')
                return False

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
                extras.pop('date_updated', None)

            if not 'date_released' in extras:
                log.error('could not get anything for date_released from ISO values')
                return False


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

            # temporal_coverage-to
            # TODO: can we determine this from the ISO values?

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


