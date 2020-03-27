#! /bin/bash
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
