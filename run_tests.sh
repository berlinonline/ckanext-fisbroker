#! /bin/bash
# run a single test:
# nosetests -d --nocapture --verbosity=2 --ckan --with-pylons=test.ini ckanext/fisbroker/tests/test_controller.py:TestReimport.test_reimport_batch_raise_error_for_package_without_fb_guid

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
nosetests \
          --ckan \
          --with-pylons=test.ini \
          --with-coverage \
          --cover-package=ckanext.fisbroker \
          --cover-erase \
          --cover-html \
          --cover-html-dir=$DIR/cover \
          ckanext/fisbroker/tests/$1
          # -s \
          # --verbosity=2 \
