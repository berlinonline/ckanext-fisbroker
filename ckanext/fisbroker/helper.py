# coding: utf-8
"""A collection of helper methods for the CKAN FIS-Broker harvester."""

import logging
from urlparse import urlparse, urlunparse, parse_qs

LOG = logging.getLogger(__name__)

def normalize_url(url):
    """Normalize URL by sorting query parameters and lowercasing the values
       (because parameter values are not case sensitive in WMS/WFS)."""

    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    normalized_query = []
    for parameter in sorted(query):
        normalized_query.append("{}={}".format(parameter, query[parameter][0].lower()))

    return urlunparse(parsed._replace(query="&".join(normalized_query)))

def uniq_resources_by_url(resources):
    """Consider resources with the same URL to be identical, remove duplicates
       by keeping only the first one."""

    uniq_resources = []

    for resource in resources:
        unique = True
        for uniq_resource in uniq_resources:
            if normalize_url(resource['url']) == normalize_url(uniq_resource['url']):
                unique = False
        if unique:
            uniq_resources.append(resource)

    return uniq_resources
