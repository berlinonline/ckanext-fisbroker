# coding: utf-8
"""A collection of helper methods for the CKAN FIS-Broker harvester."""

import logging
from urlparse import urlparse, urlunparse, parse_qs

from ckan import model
from ckan.model.package import Package
from ckan.plugins import toolkit

from ckanext.fisbroker import HARVESTER_ID

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

def is_fisbroker_package(package):
    """Return True if package was created by the FIS-Broker harvester,
       False if not."""

    if package:
        harvester = harvester_for_package(package)
        if harvester:
            return bool(harvester.type == HARVESTER_ID)
    return False

def fisbroker_guid(package):
    """Return the FIS-Broker GUID for this package, or None if
       there is none."""

    if package:
        if dataset_was_harvested(package):
            harvest_object = package.harvest_objects[0]
            if hasattr(harvest_object, 'guid'):
                return harvest_object.guid
    return None

def dataset_was_harvested(package):
    """Return True if package was harvested by a harvester,
       False if not."""
    return bool(len(package.harvest_objects) > 0)

def harvester_for_package(package):
    """Return the harvester object that harvested package, else None."""
    if dataset_was_harvested(package):
        harvest_object = package.harvest_objects[0]
        return harvest_object.source
    return None

def get_package_object(package_dict):
    """Return an instance of ckan.model.package.Package for
       `package_dict` or None if there isn't one."""

    return Package.get(package_dict.get('name'))

def get_fisbroker_source():
    """Return the HarvestSource object that is responsible for harvesting the FIS-Broker.
       Return None if no FIS-Broker source is found."""

    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    sources = toolkit.get_action('harvest_source_list')(context, {})

    for source in sources:
        if source.get('type', None) == HARVESTER_ID:
            return source

    return None
