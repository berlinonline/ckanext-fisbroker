# coding: utf-8
'''Tests for the FISBrokerAnnotator class.'''

import logging
from nose.tools import assert_raises
from ckanext.fisbroker.fisbroker_resource_annotator import (
    FISBrokerResourceAnnotator,
    FORMAT_WFS,
    FORMAT_WMS,
    FORMAT_HTML,
    FORMAT_PDF,
    FORMAT_ATOM,
    FUNCTION_API_ENDPOINT,
    FUNCTION_API_DESCRIPTION,
    FUNCTION_WEB_INTERFACE,
    FUNCTION_DOCUMENTATION,
)
from ckanext.fisbroker.helper import normalize_url
from ckanext.fisbroker.tests import _assert_equal

LOG = logging.getLogger(__name__)


class TestResourceAnnotator(object):
    """Various tests for the FISBrokerResourceAnnotator class"""

    def test_only_wms_and_wfs_allowed(self):
        '''Some methods only allow 'wms' or 'wfs' as the service parameter.
           Other values should raise an exception.'''
        with assert_raises(ValueError):
            FISBrokerResourceAnnotator.service_version('atom')
        with assert_raises(ValueError):
            FISBrokerResourceAnnotator.getcapabilities_query('atom')

        with assert_raises(ValueError):
            annotator = FISBrokerResourceAnnotator()
            url = 'https://fbinter.stadt-berlin.de/fb/feed/senstadt/a_SU_LOR'
            annotator.annotate_service_resource({'url': url})

    def test_annotate_service_endpoint_url(self):
        """An incoming FIS-Broker service resource without query strings is the
           service endpoint. Test that it is annotated correctly.
           Test both WFS and WMS."""

        url = 'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_boden_wfs1_2015'
        resource = {'url': url}
        annotator = FISBrokerResourceAnnotator()
        converted_resource = annotator.annotate_resource(resource)
        _assert_equal(converted_resource['url'], url)
        _assert_equal(converted_resource['format'], FORMAT_WFS)
        _assert_equal(converted_resource['internal_function'], FUNCTION_API_ENDPOINT)
        assert not converted_resource['main']

    def test_annotate_getcapabilities_url(self):
        """An incoming FIS-Broker service resource with URL parameters containing
           'request=GetCapabilities' should be annotated correctly. That means the URL
           should be unchanged, the internal function should be set etc.
           Case of 'getcapabilities' should not matter.
           Test both WFS and WMS."""

        wfs_urls = [
            'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_boden_wfs1_2015?request=getcapabilities&service=wfs&version=2.0.0',
            'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_boden_wfs1_2015?request=GetCapabilities&service=wfs&version=2.0.0',
        ]
        for url in wfs_urls:
            resource = {'url': url}
            annotator = FISBrokerResourceAnnotator()
            converted_resource = annotator.annotate_resource(resource)
            _assert_equal(converted_resource['url'], url)
            _assert_equal(converted_resource['format'], FORMAT_WFS)
            _assert_equal(converted_resource['internal_function'], FUNCTION_API_DESCRIPTION)
            assert converted_resource['main']

        url = 'https://fbinter.stadt-berlin.de/fb/wms/senstadt/wmsk_02_14_04gwtemp_60m?request=getcapabilities&service=wms&version=1.3.0'
        resource = {'url': url}
        annotator = FISBrokerResourceAnnotator()
        converted_resource = annotator.annotate_resource(resource)
        _assert_equal(converted_resource['url'], url)
        _assert_equal(converted_resource['format'], FORMAT_WMS)
        _assert_equal(converted_resource['internal_function'], FUNCTION_API_DESCRIPTION)
        assert converted_resource['main']

    def test_annotate_atom_feed(self):

        url = 'https://fbinter.stadt-berlin.de/fb/feed/senstadt/a_SU_LOR'
        resource = {'url': url}
        annotator = FISBrokerResourceAnnotator()
        converted_resource = annotator.annotate_resource(resource)

        _assert_equal(converted_resource['url'], url)
        _assert_equal(converted_resource['name'], "Atom Feed")
        _assert_equal(converted_resource['description'], "Atom Feed")
        _assert_equal(converted_resource['format'], FORMAT_ATOM)
        _assert_equal(converted_resource['internal_function'], FUNCTION_API_ENDPOINT)
        assert converted_resource['main']

    def test_annotate_service_page(self):
        service_urls = [
            "http://fbinter.stadt-berlin.de/fb?loginkey=showMap&mapId=nsg_lsg@senstadt",
            "http://fbinter.stadt-berlin.de/fb/index.jsp?loginkey=showMap&mapId=nsg_lsg@senstadt",
            "https://fbinter.stadt-berlin.de/fb?loginkey=showMap&mapId=nsg_lsg@senstadt",
            "https://fbinter.stadt-berlin.de/fb/index.jsp?loginkey=showMap&mapId=nsg_lsg@senstadt",
        ]

        for url in service_urls:
            resource = {'url': url}
            annotator = FISBrokerResourceAnnotator()
            converted_resource = annotator.annotate_resource(resource)

            _assert_equal(converted_resource['url'], url)
            _assert_equal(converted_resource['name'], "Serviceseite im FIS-Broker")
            _assert_equal(converted_resource['description'], "Serviceseite im FIS-Broker")
            _assert_equal(converted_resource['format'], FORMAT_HTML)
            _assert_equal(converted_resource['internal_function'], FUNCTION_WEB_INTERFACE)
            assert not converted_resource['main']

    def test_annotate_arbitrary_url_with_description(self):
        url = 'https://fbinter.stadt-berlin.de/fb_daten/beschreibung/umweltatlas/datenformatbeschreibung/Datenformatbeschreibung_kriterien_zur_bewertung_der_bodenfunktionen2015.pdf'
        description = 'Technische Beschreibung'
        res_format = FORMAT_PDF
        resource = {
            'url': url,
            'name': description,
            'description': description,
            'format': res_format
        }
        annotator = FISBrokerResourceAnnotator()
        converted_resource = annotator.annotate_resource(resource)

        _assert_equal(converted_resource['name'], description)
        _assert_equal(converted_resource['description'], description)
        _assert_equal(converted_resource['format'], FORMAT_PDF)
        _assert_equal(converted_resource['internal_function'], FUNCTION_DOCUMENTATION)
        _assert_equal(converted_resource['url'], url)
        assert not converted_resource['main']

    def test_arbitrary_url_without_description_is_ignored(self):
        url = 'https://fbinter.stadt-berlin.de/fb_daten/beschreibung/umweltatlas/datenformatbeschreibung/Datenformatbeschreibung_kriterien_zur_bewertung_der_bodenfunktionen2015.pdf'
        res_format = FORMAT_PDF
        resource = {
            'url': url,
            'name': 'Technische Beschreibung',
            'format': res_format
        }
        annotator = FISBrokerResourceAnnotator()
        converted_resource = annotator.annotate_resource(resource)

        _assert_equal(converted_resource, None)

    def test_sort_resources_by_weight(self):
        '''A list of resource dicts should be returned ordered in ascending
           order by the 'weight' member.'''

        resources = [
            {
                'name': 'foo',
                'weight': 20,
            },
            {
                'name': 'bar',
                'weight': 5,
            },
            {
                'name': 'daz',
                'weight': 10,
            },
            {
                'name': 'dingo',
                'weight': 15,
            },
            {
                'name': 'baz',
                'weight': 25,
            },
        ]

        annotator = FISBrokerResourceAnnotator()
        sorted_weights = [resource['weight'] for resource in annotator.sort_resources(resources)]
        _assert_equal([5, 10, 15, 20, 25], sorted_weights)

    def test_ensure_endpoint_description_is_present(self):
        '''When converting a set of resources for a WFS or WMS service, ensure there is an endpoint
           description (a GetCapabilities-URL), and all resources are annotated as expected.'''

        resources = [
            {
                'url': 'https://fbinter.stadt-berlin.de/fb?loginkey=showMap&mapId=nsg_lsg@senstadt'
            },
            {
                'url': 'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_boden_wfs1_2015'
            },
            {
                'url': 'https://fbinter.stadt-berlin.de/fb_daten/beschreibung/umweltatlas/datenformatbeschreibung/Datenformatbeschreibung_kriterien_zur_bewertung_der_bodenfunktionen2015.pdf',
                'description': 'Technische Beschreibung'
            }
        ]

        annotator = FISBrokerResourceAnnotator()
        annotated = annotator.annotate_all_resources(resources)
        expected = [
            {
                'name': 'Endpunkt-Beschreibung des WFS-Service',
                'weight': 10,
                'format': FORMAT_WFS,
                'url': normalize_url('https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_boden_wfs1_2015?request=getcapabilities&service=wfs&version=2.0.0'),
                'internal_function': FUNCTION_API_DESCRIPTION,
                'main': True,
                'description': 'Maschinenlesbare Endpunkt-Beschreibung des WFS-Service. Weitere Informationen unter https://www.ogc.org/standards/wfs'
            },
            {
                'name': 'API-Endpunkt des WFS-Service',
                'weight': 15,
                'format': FORMAT_WFS,
                'url': 'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_boden_wfs1_2015',
                'internal_function': FUNCTION_API_ENDPOINT,
                'main': False,
                'description': 'API-Endpunkt des WFS-Service. Weitere Informationen unter https://www.ogc.org/standards/wfs'
            },
            {
                'description': 'Serviceseite im FIS-Broker',
                'weight': 20,
                'format': FORMAT_HTML,
                'url': 'https://fbinter.stadt-berlin.de/fb?loginkey=showMap&mapId=nsg_lsg@senstadt', 'internal_function': 'web_interface',
                'main': False,
                'name': 'Serviceseite im FIS-Broker'
            },
            {
                'description': 'Technische Beschreibung',
                'weight': 30,
                'url': 'https://fbinter.stadt-berlin.de/fb_daten/beschreibung/umweltatlas/datenformatbeschreibung/Datenformatbeschreibung_kriterien_zur_bewertung_der_bodenfunktionen2015.pdf',
                'internal_function': 'documentation',
                'main': False,
                'name': 'Technische Beschreibung'
            }
        ]

        _assert_equal(annotated, expected)

        resources = [
            {
                'url': 'https://fbinter.stadt-berlin.de/fb/wms/senstadt/wmsk_02_14_04gwtemp_60m'
            }
        ]

        annotated = annotator.annotate_all_resources(resources)
        expected = [
            {
                'name': 'Endpunkt-Beschreibung des WMS-Service',
                'weight': 10,
                'format': FORMAT_WMS,
                'url': normalize_url('https://fbinter.stadt-berlin.de/fb/wms/senstadt/wmsk_02_14_04gwtemp_60m?request=getcapabilities&service=wms&version=1.3.0'),
                'internal_function': FUNCTION_API_DESCRIPTION,
                'main': True,
                'description': 'Maschinenlesbare Endpunkt-Beschreibung des WMS-Service. Weitere Informationen unter https://www.ogc.org/standards/wms'
            },
            {
                'url': 'https://fbinter.stadt-berlin.de/fb/wms/senstadt/wmsk_02_14_04gwtemp_60m',
                'name': 'API-Endpunkt des WMS-Service',
                'weight': 15,
                'format': FORMAT_WMS,
                'internal_function': FUNCTION_API_ENDPOINT,
                'main': False,
                'description': 'API-Endpunkt des WMS-Service. Weitere Informationen unter https://www.ogc.org/standards/wms'
            },
        ]

        _assert_equal(annotated, expected)
