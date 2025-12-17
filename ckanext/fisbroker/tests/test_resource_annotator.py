# coding: utf-8
'''Tests for the FISBrokerAnnotator class.'''

import logging
import pytest
from ckanext.fisbroker.fisbroker_resource_annotator import (
    FISBrokerResourceAnnotator,
    FORMAT_WFS,
    FORMAT_WMS,
    FORMAT_WMTS,
    FORMAT_HTML,
    FORMAT_PDF,
    FORMAT_ATOM,
    FUNCTION_API_ENDPOINT,
    FUNCTION_API_DESCRIPTION,
    FUNCTION_WEB_INTERFACE,
    FUNCTION_DOCUMENTATION,
)
from ckanext.fisbroker.helper import normalize_url

LOG = logging.getLogger(__name__)


class TestResourceAnnotator(object):
    """Various tests for the FISBrokerResourceAnnotator class"""

    def test_only_wms_wmts_wfs_allowed(self):
        '''Some methods only allow 'wms', 'wmts' or 'wfs' as the service parameter.
           Other values should raise an exception.'''
        with pytest.raises(ValueError) as e:
            FISBrokerResourceAnnotator.service_version('atom')
        with pytest.raises(ValueError) as e:
            FISBrokerResourceAnnotator.getcapabilities_query('atom')

        with pytest.raises(ValueError) as e:
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
        assert converted_resource['url'] == url
        assert converted_resource['format'] == FORMAT_WFS
        assert converted_resource['internal_function'] == FUNCTION_API_ENDPOINT
        assert not converted_resource['main']

    @pytest.mark.parametrize("service", [
            {'url': 'https://gdi.berlin.de/services/wfs/ua_srgk?REQUEST=GetCapabilities&SERVICE=wfs', 'format': FORMAT_WFS},
            {'url': 'https://gdi.berlin.de/services/wms/truedop_2013?REQUEST=GetCapabilities&SERVICE=wms', 'format': FORMAT_WMS},
            {'url': 'https://gdi.berlin.de/services/wmts/k5_farbe?REQUEST=GetCapabilities&SERVICE=wmts', 'format': FORMAT_WMTS},
    ])
    def test_annotate_getcapabilities_url(self, service):
        """An incoming FIS-Broker service resource with URL parameters containing
           'request=GetCapabilities' should be annotated correctly. That means the URL
           should be unchanged, the internal function should be set etc.
           Case of 'getcapabilities' should not matter.
           Test both WFS, WMS and WMTS."""

        resource = {'url': service['url']}
        annotator = FISBrokerResourceAnnotator()
        converted_resource = annotator.annotate_resource(resource)
        assert converted_resource['url'] == service['url']
        assert converted_resource['format'] == service['format']
        assert converted_resource['internal_function'] == FUNCTION_API_DESCRIPTION
        assert converted_resource['main']

    def test_annotate_atom_feed(self):

        url = 'https://fbinter.stadt-berlin.de/fb/feed/senstadt/a_SU_LOR'
        resource = {'url': url}
        annotator = FISBrokerResourceAnnotator()
        converted_resource = annotator.annotate_resource(resource)

        assert converted_resource['url'] ==  url
        assert converted_resource['name'] ==  "Atom Feed"
        assert converted_resource['description'] ==  "Atom Feed"
        assert converted_resource['format'] ==  FORMAT_ATOM
        assert converted_resource['internal_function'] ==  FUNCTION_API_ENDPOINT
        assert converted_resource['main']

    @pytest.mark.parametrize("url", [
            "http://fbinter.stadt-berlin.de/fb?loginkey=showMap&mapId=nsg_lsg@senstadt",
            "http://fbinter.stadt-berlin.de/fb/index.jsp?loginkey=showMap&mapId=nsg_lsg@senstadt",
            "https://fbinter.stadt-berlin.de/fb?loginkey=showMap&mapId=nsg_lsg@senstadt",
            "https://fbinter.stadt-berlin.de/fb/index.jsp?loginkey=showMap&mapId=nsg_lsg@senstadt",
    ])
    def test_annotate_service_page(self, url):

        resource = {'url': url}
        annotator = FISBrokerResourceAnnotator()
        converted_resource = annotator.annotate_resource(resource)

        assert converted_resource['url'] == url
        assert converted_resource['name'] == "Serviceseite im FIS-Broker"
        assert converted_resource['format'] == FORMAT_HTML
        assert converted_resource['internal_function'] == FUNCTION_WEB_INTERFACE
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

        assert converted_resource['name'] == description
        assert converted_resource['description'] == description
        assert converted_resource['format'] == FORMAT_PDF
        assert converted_resource['internal_function'] == FUNCTION_DOCUMENTATION
        assert converted_resource['url'] == url
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

        assert converted_resource == None

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
        assert [5, 10, 15, 20, 25] == sorted_weights

    @pytest.mark.parametrize('data', [
        {
            'resources': [
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
            ],
            'expected': [
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
                    'name': 'Serviceseite im FIS-Broker',
                    'weight': 20,
                    'format': FORMAT_HTML,
                    'url': 'https://fbinter.stadt-berlin.de/fb?loginkey=showMap&mapId=nsg_lsg@senstadt', 'internal_function': 'web_interface',
                    'main': False,
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
        },
        {
            'resources': [
                {
                    'url': 'https://fbinter.stadt-berlin.de/fb/wms/senstadt/wmsk_02_14_04gwtemp_60m'
                }
            ],
            'expected': [
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
        },
        {
            'resources': [
                {
                    'url': normalize_url('https://gdi.berlin.de/services/wmts/k5_farbe')
                },
            ],
            'expected': [
                {
                    'name': 'Endpunkt-Beschreibung des WMTS-Service',
                    'weight': 10,
                    'format': FORMAT_WMTS,
                    'url': normalize_url('https://gdi.berlin.de/services/wmts/k5_farbe?REQUEST=getcapabilities&SERVICE=wmts&VERSION=1.0.0'),
                    'internal_function': FUNCTION_API_DESCRIPTION,
                    'main': True,
                    'description': 'Maschinenlesbare Endpunkt-Beschreibung des WMTS-Service. Weitere Informationen unter https://www.ogc.org/standards/wmts'
                },
                {
                    'url': 'https://gdi.berlin.de/services/wmts/k5_farbe',
                    'name': 'API-Endpunkt des WMTS-Service',
                    'weight': 15,
                    'format': FORMAT_WMTS,
                    'internal_function': FUNCTION_API_ENDPOINT,
                    'main': False,
                    'description': 'API-Endpunkt des WMTS-Service. Weitere Informationen unter https://www.ogc.org/standards/wmts'
                },
            ]
        }
    ])
    def test_ensure_endpoint_description_is_present(self, data):
        '''When converting a set of resources for a WFS or WMS service, ensure there is an endpoint
           description (a GetCapabilities-URL), and all resources are annotated as expected.'''


        annotator = FISBrokerResourceAnnotator()
        annotated = annotator.annotate_all_resources(data['resources'])

        assert annotated == data['expected']

    def test_filename_as_fallback_for_empty_description(self):
        '''
            Documentation resources that have an empty description should 
            use the filename as a fallback.
        '''
        url = 'https://fbinter.stadt-berlin.de/fb_daten/beschreibung/umweltatlas/datenformatbeschreibung/Datenformatbeschreibung_05_04_forstbetriebskarte2014.html'
        resource = {
            'url': url,
            'name': '',
            'description': '',
            'format': FORMAT_HTML
        }
        annotator = FISBrokerResourceAnnotator()
        annotated = annotator.annotate_resource(resource)

        assert annotated['name'] == 'Datenformatbeschreibung_05_04_forstbetriebskarte2014.html'
