#! /bin/bash

export CKAN_INI="test_local.ini"

# delete .pyc-files to prevent the "import file mismatch" errors
find -name "*.pyc" -delete
FILTER=$1 CKAN_SQLALCHEMY_URL="postgresql://ckandbuser:ckandbpassword@db/ckan_test" CKAN_SOLR_URL="http://solr:8983/solr/ckan_test" bash -c 'coverage run --source=ckanext.fisbroker -m pytest -vv ckanext/fisbroker/tests -k "$FILTER" && coverage html'
