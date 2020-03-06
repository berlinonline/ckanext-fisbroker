# coding: utf-8

import logging
from urlparse import urlparse, parse_qs

LOG = logging.getLogger(__name__)

class FISBrokerResourceConverter:
    """A class to assign meaningful metadata to FIS-Broker resource objects from a CKAN package_dict, based on their URLs."""

    @staticmethod
    def getcapabilities_suffix():
        return "request=GetCapabilities&version=2.0.0"

    def convert_service_resource(self, resource):
        """Convert wfs and wms service resources."""

        if "/wfs/" in resource['url']:
            service_type = "WFS"
        elif "/wms/" in resource['url']:
            service_type = "WMS"
        else:
            return resource

        resource['name'] = "Unspezifizierter {}-Service".format(service_type)
        resource['description'] = "Unspezifizierter {}-Service".format(service_type)

        parsed = urlparse(resource['url'])
        query = parse_qs(parsed.query)
        if not query:
            # this is the service endpoint
            resource['name'] = "API-Endpunkt des {}-Service".format(service_type)
            resource['description'] = "API-Endpunkt des {}-Service. Weitere Informationen unter https://www.ogc.org/standards/{}".format(service_type, service_type.lower())
            resource['internal_function'] = 'api_endpoint'
            resource['weight'] = 15
        else:
            method = query.get('request')
            if method:
                if method.pop().lower() == "getcapabilities":
                    resource['name'] = "Endpunkt-Beschreibung des {}-Service".format(service_type)
                    resource['description'] = "Maschinenlesbare Endpunkt-Beschreibung des {}-Service. Weitere Informationen unter https://www.ogc.org/standards/{}".format(service_type, service_type.lower())
                    resource['main'] = True
                    resource['internal_function'] = 'api_description'
                    resource['weight'] = 10

        resource['format'] = service_type

        return resource

    def convert_resource(self, resource):
        """Assign meaningful metadata to FIS-Broker resource objects from a CKAN package_dict, based on their URLs."""

        resource['internal_function'] = 'unknown'
        resource['weight'] = 200
        if "/feed/" in resource['url']:
            resource['name'] = "Atom Feed"
            resource['description'] = "Atom Feed"
            resource['format'] = "Atom"
            resource['main'] = True
            resource['internal_function'] = 'api_endpoint'
            resource['weight'] = 15
        elif "/wfs/" in resource['url'] or "/wms/" in resource['url']:
            resource = self.convert_service_resource(resource)
        elif resource['url'].startswith('https://fbinter.stadt-berlin.de/fb?loginkey='):
            resource['name'] = "Serviceseite im FIS-Broker"
            resource['format'] = "HTML"
            resource['description'] = "Serviceseite im FIS-Broker"
            resource['main'] = False
            resource['internal_function'] = 'web_interface'
            resource['weight'] = 20
        elif 'description' in resource:
            resource['name'] = resource['description']
            resource['main'] = False
            resource['internal_function'] = 'documentation'
            resource['weight'] = 30
        else:
            resource = None

        return resource

    def sort_resources(self, resources):
        '''Sort resources in ascending order by `weight` field and and return.'''

        return sorted(resources, key=lambda resource: resource.get('weight', 200))


    def convert_all_resources(self, resources):
        '''Assign meaningful metadata to all FIS-Broker resource objects. Ensure there is a `getCapabilities`
           resource.'''

        resources = [self.convert_resource(resource)
                     for resource in resources]
        resources = filter(None, resources)

        res_dict = { resource['internal_function']: resource for resource in resources }
        if 'api_endpoint' in res_dict and 'api_description' not in res_dict:
            res_format = res_dict['api_endpoint']['format']
            if res_format == "WFS":
                resources.append({
                    'name': 'Endpunkt-Beschreibung des WFS-Service',
                    'description': 'Maschinenlesbare Endpunkt-Beschreibung des WFS-Service. Weitere Informationen unter https://www.ogc.org/standards/wfs',
                    'main': True ,
                    'format': 'WFS' ,
                    'internal_function': 'api_description' ,
                    'url': "{}?request=getcapabilities&service=wfs&version=2.0.0".format(res_dict['api_endpoint']['url']) ,
                    'weight': 10
                })
            elif res_format == "WMS":
                resources.append({
                    'name': 'Endpunkt-Beschreibung des WMS-Service',
                    'description': 'Maschinenlesbare Endpunkt-Beschreibung des WMS-Service. Weitere Informationen unter https://www.ogc.org/standards/wms',
                    'main': True ,
                    'format': 'WMS' ,
                    'internal_function': 'api_description' ,
                    'url': "{}?request=getcapabilities&service=wms&version=1.3.0".format(res_dict['api_endpoint']['url']),
                    'weight': 10
                })

        return self.sort_resources(resources)
