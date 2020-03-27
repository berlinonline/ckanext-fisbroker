.. You should enable this project on travis-ci.org and coveralls.io to make
   these badges work. The necessary Travis and Coverage config files have been
   generated for you.

.. image:: https://travis-ci.org/berlinonline/ckanext-fisbroker.svg?branch=develop
    :target: https://travis-ci.org/berlinonline/ckanext-fisbroker

.. image:: https://coveralls.io/repos/berlinonline/ckanext-fisbroker/badge.svg
  :target: https://coveralls.io/r/berlinonline/ckanext-fisbroker

=================
ckanext-fisbroker
=================

Developed by Knud Möller for [BerlinOnline](http://berlinonline.de)

This is an extension of the CWS Harvester from [ckanext-spatial](https://github.com/ckan/ckanext-spatial), intended to harvest the Berlin Geoportal [FIS-Broker](http://www.stadtentwicklung.berlin.de/geoinformation/fis-broker/). It mainly adapts the harvesting to extract metadata conforming to the [Berlin Open Data Schema](https://datenregister.berlin.de/schema/berlin_od_schema.json).

-------------
Configuration
-------------

* `import_since`: Sets a filter on the query to CSW to retrieve only records that were changed after a given date. Specified either as an ISO8601 date `YYYYMMDDTHH:MM:SS`, or as one of the following keywords:
  - `last_error_free`: replace with the date of the last error free harvest job
  - `big_bang`: no date constraint: retrieve all records
* `timeout`: Time in seconds to retry before allowing a timeout error. Default is `20`.
* `timedelta`: The harvest jobs' timestamps are logged in UTC, while the harvest source might use a different timezone. This setting specifies the delta in hours between UTC and the harvest source's timezone (will influence the timestamp retrieved by `last_error_free`). Default is `0`.


