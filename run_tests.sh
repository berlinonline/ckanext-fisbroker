#! /bin/bash
nosetests -s --ckan --with-pylons=test.ini --with-coverage --cover-package=ckanext.fisbroker --cover-erase --cover-html ckanext/fisbroker/tests/
