#! /bin/bash
nosetests -s --verbosity=2 --ckan --with-pylons=test.ini --with-coverage --cover-package=ckanext.fisbroker --cover-erase --cover-html ckanext/fisbroker/tests/
