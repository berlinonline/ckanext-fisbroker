# coding: utf-8
'''Code for annotating FIS-Broker resource objects.'''


import logging
from urlparse import urlparse, parse_qs
from ckanext.fisbroker.helper import normalize_url

LOG = logging.getLogger(__name__)
FORMAT_WFS = "WFS"
FORMAT_WMS = "WMS"
FORMAT_ATOM = "Atom"
FORMAT_HTML = "HTML"
FORMAT_PDF = "PDF"
FUNCTION_API_ENDPOINT = "api_endpoint"
FUNCTION_API_DESCRIPTION = "api_description"
FUNCTION_WEB_INTERFACE = "web_interface"
FUNCTION_DOCUMENTATION = "documentation"
VALID_SERVICE_TYPES = [FORMAT_WFS.lower(), FORMAT_WMS.lower()]

class FISBrokerResourceAnnotator:
    '''A class to assign meaningful metadata to FIS-Broker resource objects from a CKAN
       package_dict, based on their URLs.'''

    @staticmethod
    def getcapabilities_query(service):
        '''Build the query string for a GetCapabilities query to either a WMS or WFS service.
           Return None if `service` is not one of [ 'wms', 'wfs' ].'''
        if service not in VALID_SERVICE_TYPES:
            raise ValueError("Service must be one of [ {} ].".format(
                ', '.join(VALID_SERVICE_TYPES)))
        return "service={}&request=GetCapabilities&version={}".format(service, FISBrokerResourceAnnotator.service_version(service))

    @staticmethod
    def service_version(service):
        '''Return the service version for a GetCapabilities query to either a WMS or WFS service.
           Return None if `service` is not one of [ 'wms', 'wfs' ].'''
        if service == "wms":
            return "1.3.0"
        elif service == "wfs":
            return "2.0.0"
        raise ValueError("Service must be one of [ {} ].".format(
            ', '.join(VALID_SERVICE_TYPES)))

    def annotate_service_resource(self, resource):
        '''Convert wfs and wms service resources.'''

        if "/wfs/" in resource['url']:
            service_type = FORMAT_WFS
        elif "/wms/" in resource['url']:
            service_type = FORMAT_WMS
        else:
            raise ValueError("Resource type must be one of [ {} ].".format(
                ', '.join(VALID_SERVICE_TYPES)))

        resource['name'] = "Unspezifizierter {}-Service".format(service_type)
        resource['description'] = "Unspezifizierter {}-Service".format(service_type)

        parsed = urlparse(resource['url'])
        query = parse_qs(parsed.query)
        if not query:
            # this is the service endpoint
            resource['name'] = "API-Endpunkt des {}-Service".format(service_type)
            resource['description'] = "API-Endpunkt des {}-Service. Weitere Informationen unter https://www.ogc.org/standards/{}".format(service_type, service_type.lower())
            resource['internal_function'] = FUNCTION_API_ENDPOINT
            resource['weight'] = 15
        else:
            method = query.get('request')
            if method:
                if method.pop().lower() == "getcapabilities":
                    resource['name'] = "Endpunkt-Beschreibung des {}-Service".format(service_type)
                    resource['description'] = "Maschinenlesbare Endpunkt-Beschreibung des {}-Service. Weitere Informationen unter https://www.ogc.org/standards/{}".format(service_type, service_type.lower())
                    resource['main'] = True
                    resource['internal_function'] = FUNCTION_API_DESCRIPTION
                    resource['weight'] = 10

        resource['format'] = service_type

        return resource

    def is_fis_broker_service_page(self, url):
        '''Analyzes url to decide whether it is the service's entry page in FIS-Broker.
           Returns True or False accordingly.'''

        parsed = urlparse(url)
        if parsed.netloc == 'fbinter.stadt-berlin.de':
            if parsed.path.strip('/') == 'fb' or parsed.path.strip('/') == 'fb/index.jsp':
                if 'loginkey' in parse_qs(parsed.query):
                    return True

        return False


    def annotate_resource(self, resource):
        '''Assign meaningful metadata to FIS-Broker resource objects from a CKAN package_dict,
           based on their URLs.'''

        resource['main'] = False
        if "/feed/" in resource['url']:
            resource['name'] = "Atom Feed"
            resource['description'] = "Atom Feed"
            resource['format'] = "Atom"
            resource['main'] = True
            resource['internal_function'] = FUNCTION_API_ENDPOINT
            resource['weight'] = 15
        elif "/wfs/" in resource['url'] or "/wms/" in resource['url']:
            resource = self.annotate_service_resource(resource)
        elif self.is_fis_broker_service_page(resource['url']):
            resource['name'] = "Serviceseite im FIS-Broker"
            resource['format'] = "HTML"
            resource['description'] = "Serviceseite im FIS-Broker"
            resource['internal_function'] = FUNCTION_WEB_INTERFACE
            resource['weight'] = 20
        elif 'description' in resource:
            resource['name'] = resource['description']
            resource['internal_function'] = FUNCTION_DOCUMENTATION
            resource['weight'] = 30
        else:
            resource = None

        return resource

    def sort_resources(self, resources, default_weight=200):
        '''Sort resources in ascending order by `weight` field and and return.'''

        return sorted(resources, key=lambda resource: resource.get('weight', default_weight))

    def annotate_all_resources(self, resources):
        '''Assign meaningful metadata to all FIS-Broker resource objects. Ensure there is a 
           `getCapabilities` resource.'''

        resources = [self.annotate_resource(resource)
                     for resource in resources]
        resources = filter(None, resources)

        res_dict = { resource['internal_function']: resource for resource in resources }
        if 'api_endpoint' in res_dict and 'api_description' not in res_dict:
            res_format = res_dict['api_endpoint']['format'].lower()
            url = '{}?{}'.format(res_dict['api_endpoint']['url'],
                                 FISBrokerResourceAnnotator.getcapabilities_query(res_format))
            url = normalize_url(url)
            resource = {
                'url': url
            }
            resources.append(self.annotate_service_resource(resource))

        return self.sort_resources(resources)
