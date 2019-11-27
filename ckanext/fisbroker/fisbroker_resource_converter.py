# coding: utf-8

import logging
from urlparse import urlparse, urlunparse, parse_qs

LOG = logging.getLogger(__name__)

class FISBrokerResourceConverter:
    """A class to assign meaningful metadata to FIS-Broker resource objects from a CKAN package_dict, based on their URLs."""

    @staticmethod
    def getcapabilities_suffix():
        return "request=GetCapabilities&version=2.0.0"

    def normalize_url(self, url):
        """Normalize URL by sorting query parameters and lowercasing the values
           (because in parameter values are not case sensitive in WMS/WFS)."""

        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        normalized_query = []
        for parameter in sorted(query):
            normalized_query.append("{}={}".format(parameter, query[parameter][0].lower()))

        return urlunparse(parsed._replace(query="&".join(normalized_query)))

    def build_service_resource(self, resource):
        """Build either a WMS or WFS service resource from service URL."""
        
        if "/wfs/" in resource['url']:
            service_type = "WFS"
        elif "/wms/" in resource['url']:
            service_type = "WMS"
        else:
            return resource

        parsed = urlparse(resource['url'])
        query = parse_qs(parsed.query)
        if not query:
            resource['url'] += "?service={}&{}".format(service_type.lower(), FISBrokerResourceConverter.getcapabilities_suffix())

        resource['name'] = "{} Service".format(service_type)
        resource['description'] = "{} Service".format(service_type)
        resource['format'] = service_type

        return resource

    def convert_resource(self, resource):
        """Assign meaningful metadata to FIS-Broker resource objects from a CKAN package_dict, based on their URLs."""

        if "/feed/" in resource['url']:
            resource['name'] = "Atom Feed"
            resource['description'] = "Atom Feed"
            resource['format'] = "Atom"
            resource['main'] = True
        elif "/wfs/" in resource['url'] or "/wms/" in resource['url']:
            resource = self.build_service_resource(resource)
            resource['main'] = True
        elif resource['url'].startswith('https://fbinter.stadt-berlin.de/fb?loginkey='):
            resource['name'] = "Serviceseite im FIS-Broker"
            resource['format'] = "HTML"
            resource['description'] = "Serviceseite im FIS-Broker"
        elif 'description' in resource:
            resource['name'] = resource['description']
            resource['main'] = False
        else:
            resource = None
        return resource
