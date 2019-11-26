# coding: utf-8

import logging

log = logging.getLogger(__name__)

class FISBrokerResourceConverter:
    """A class to assign meaningful metadata to FIS-Broker resource objects from a CKAN package_dict, based on their URLs."""

    def convert_resource(self, resource):
        """Assign meaningful metadata to FIS-Broker resource objects from a CKAN package_dict, based on their URLs."""
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
        elif resource['url'].startswith('https://fbinter.stadt-berlin.de/fb?loginkey='):
            resource['name'] = "Serviceseite im FIS-Broker"
            resource['format'] = "HTML"
            resource['description'] = "Serviceseite im FIS-Broker"
        elif resource['description']:
            resource['name'] = resource['description']
            resource['main'] = False
        else:
            resource = None
        return resource
