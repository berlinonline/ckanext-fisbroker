"""Tests for testing the extension's helper functions."""


import logging
import copy

from ckan.logic import get_action
from ckan.model.package import Package
from ckan.tests import factories as ckan_factories

from ckanext.fisbroker.helper import (
    normalize_url,
    uniq_resources_by_url,
    is_fisbroker_package,
    dataset_was_harvested,
    harvester_for_package,
    fisbroker_guid,
    get_package_object,
)
from ckanext.fisbroker.tests import _assert_equal, FisbrokerTestBase, FISBROKER_HARVESTER_CONFIG

LOG = logging.getLogger(__name__)
GETCAPABILITIES_URL_1 = 'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s01_11_07naehr2015?request=getcapabilities&service=wfs&version=2.0.0'
GETCAPABILITIES_URL_2 = 'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s01_11_07naehr2015?service=wfs&version=2.0.0&request=GetCapabilities'

class TestHelper(FisbrokerTestBase):
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

    def test_is_fisbroker_package(self):
        fb_dataset_dict, source, job = self._harvester_setup(FISBROKER_HARVESTER_CONFIG)
        assert is_fisbroker_package(get_package_object(fb_dataset_dict))
        non_fb_dataset_dict = ckan_factories.Dataset()
        assert not is_fisbroker_package(get_package_object(non_fb_dataset_dict))

    def test_dataset_was_harvested(self):
        fb_dataset_dict, source, job = self._harvester_setup(FISBROKER_HARVESTER_CONFIG)
        fb_dataset = Package.get(fb_dataset_dict.get('name'))
        assert dataset_was_harvested(fb_dataset)
        non_fb_dataset_dict = ckan_factories.Dataset()
        non_fb_dataset = Package.get(non_fb_dataset_dict.get('name'))
        assert not dataset_was_harvested(non_fb_dataset)

    def test_harvester_for_package(self):
        fb_dataset_dict, source, job = self._harvester_setup(FISBROKER_HARVESTER_CONFIG)
        fb_dataset = Package.get(fb_dataset_dict.get('name'))
        assert harvester_for_package(fb_dataset) is source
        non_fb_dataset_dict = ckan_factories.Dataset()
        non_fb_dataset = Package.get(non_fb_dataset_dict.get('name'))
        assert harvester_for_package(non_fb_dataset) is None

    def test_fisbroker_guid(self):
        # Create source1
        fisbroker_fixture = {
            'title': 'FIS-Broker',
            'name': 'fisbroker',
            'url': u'http://127.0.0.1:8999/wfs-open-data.xml',
            'object_id': u'65715c6e-bbaf-3def-982b-3b5156272da7',
            'source_type': u'fisbroker'
        }

        source1, first_job = self._create_source_and_job(fisbroker_fixture)
        fb_dataset = self._run_job_for_single_document(first_job, fisbroker_fixture['object_id'])
        fb_dataset_dict = get_action('package_show')(self.context,{'id':fb_dataset.package_id})
        non_fb_dataset_dict = ckan_factories.Dataset()

        _assert_equal(fisbroker_guid(get_package_object(fb_dataset_dict)), fisbroker_fixture['object_id'])
        assert not fisbroker_guid(get_package_object(non_fb_dataset_dict))
