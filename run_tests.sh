#! /bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $DIR
nosetests -s --verbosity=2 --ckan --with-pylons=test.ini --with-coverage --cover-package=ckanext.fisbroker --cover-erase --cover-html ckanext/fisbroker/tests/ --cover-html-dir=$DIR/cover
