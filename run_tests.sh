#! /bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
nosetests -s --verbosity=2 --ckan --with-pylons=test.ini --with-coverage --cover-package=ckanext.fisbroker --cover-erase --cover-html ckanext/fisbroker/tests/$1 --cover-html-dir=$DIR/cover
