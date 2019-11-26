# coding: utf-8

from ckanext.fisbroker.fisbroker_resource_converter import FISBrokerResourceConverter

class TestResourceConverter(object):

    def test_convert_getcapabilities(self):
        resource = { 
            'url': 'https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_boden_wfs1_2015?request=getcapabilities&service=wfs&version=2.0.0'
        }
        converter = FISBrokerResourceConverter()
        converted_resource = converter.convert_resource(resource)
        assert converted_resource['url'] == resource['url']
