# coding: utf-8

import logging
import copy
from ckanext.fisbroker.helper import normalize_url, uniq_resources_by_url
from ckanext.fisbroker.tests.helper import _assert_equal

LOG = logging.getLogger(__name__)
GETCAPABILITIES_URL_1 = 'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s01_11_07naehr2015?request=getcapabilities&service=wfs&version=2.0.0'
GETCAPABILITIES_URL_2 = 'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s01_11_07naehr2015?service=wfs&version=2.0.0&request=GetCapabilities'

class TestHelper(object):
    """Test functionality of ckanext.fisbroker.helper"""

    def test_normalize_url(self):
        """Two URLs should be equal once their query strings have been normalized."""

        _assert_equal(normalize_url(GETCAPABILITIES_URL_1), normalize_url(GETCAPABILITIES_URL_2))

    def test_uniq_resources(self):
        """In a list of resources with two identical URLs, the second one should be removed."""

        resources = [
            {
                'url': GETCAPABILITIES_URL_1 ,
                'name': 'WFS Service',
                'format': 'WFS'
            } ,
            {
                'url': 'https://fbinter.stadt-berlin.de/fb?loginkey=alphaDataStart&alphaDataId=s01_11_07naehr2015@senstadt' ,
                'name': 'Serviceseite im FIS-Broker' ,
                'format': 'HTML'
            } ,
            {
                'url': 'https://www.stadtentwicklung.berlin.de/umwelt/umweltatlas/dd11107.htm' ,
                'name': 'Inhaltliche Beschreibung' ,
                'format': 'HTML'
            } ,
            {
                'url': 'https://fbinter.stadt-berlin.de/fb_daten/beschreibung/umweltatlas/datenformatbeschreibung/Datenformatbeschreibung_kriterien_zur_bewertung_der_bodenfunktionen2015.pdf' ,
                'name': 'Technische Beschreibung' ,
                'format': 'PDF'
            }
        ]

        duplicate =  {
            'url': GETCAPABILITIES_URL_2 ,
            'name': 'WFS Service' ,
            'format': 'WFS'
        }

        test_resources = copy.deepcopy(resources)
        test_resources.append(duplicate)
        uniq_resources = uniq_resources_by_url(test_resources)
        _assert_equal(uniq_resources, resources)
