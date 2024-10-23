# Changelog

## Development

## [1.4.5](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.4.5)

_(2024-10-23)_

- Change BerlinOnline company name to "BerlinOnline GmbH".
- Previously, `last_error_free_job()` would mark jobs with harvest objects with `report_status == 'deleted'` as having an error.
This is fixed now, `deleted` is accepted as well as `not_modified`.

## [1.4.4](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.4.4)

_(2024-03-22)_

- Add a button to open full CSW record in FIS-Broker.
- Add a link to the service's preview graphic in the newly introduced `preview_image` metadata field (if any), instead of adding the image as markup to the `notes`.
- Restructure output of the cli's `reimport-dataset` command to have `datasets` and `errors` fields.
- Change Solr image reference in github CI ([test.yml](.github/workflows/test.yml)) to the new naming scheme according to https://github.com/ckan/ckan-solr.

## [1.4.3](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.4.3)

_(2024-02-01)_

- Adjust log-levels.
- Remove `travis-requirements.txt`.

## [1.4.2](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.4.2)

_(2024-01-31)_

- Improved Exception handling in several places.
- More F-Strings.

## [1.4.1](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.4.1)

_(2024-01-30)_

- Refactor to make `ckanext-fisbroker` compatible with `ckanext-spatial@v2.1.1`.

## [1.4.0](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.4.0)

_(2024-01-24)_

- Extract information about temporal coverage from the CSW's `temporal-extent-begin` and `temporal-extent-end` fields.
- Map the license id for DL-DE-BY that FIS-Broker uses to the one we're using internally.
- Move `gather_stage()`, `fetch_stage()` and `import_stage()` here from [our fork of ckanext-spatial](https://github.com/berlinonline/ckanext-spatial/tree/bo_prs).
- Move the `CswService` class here from [our fork of ckanext-spatial](https://github.com/berlinonline/ckanext-spatial/tree/bo_prs).
- Refactor to separate the [ISpatialHarvester](https://docs.ckan.org/projects/ckanext-spatial/en/latest/harvesters.html#customizing-the-harvesters) implementation from the rest of the extension. It now lives in `ckanext.fisbroker.fisbroker_harvester` rather than `ckanext.fisbroker.plugin`.

## [1.3.2](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.3.2)

_(2023-12-18)_

- Fix a bug regarding case-sensitivity in the CSW interface.
- Fix problems in testing pipeline regarding versions of dependencies.


## [1.3.1](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.3.1)

_(2023-05-22)_

- Define extension's version string in [VERSION](VERSION), make it available as `ckanext.fisbroker.__version__` and in [setup.py](setup.py).


## [1.3.0](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.3.0)

_(2023-03-14)_

- Convert to Python 3.
- This is the first version that requires Python 3 / CKAN >= 2.9.
- Switch from RST to Markdown for Readme.
- Switch testing framework from Nose to Pytest.
- Switch command line tool from paster to click.
- Add Github CI.

## [1.2.1](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.2.1)

_(2021-09-29)_

- Add two additional sub-commands to the `fisbroker` command:
  - `harvest_objects`: List all harvest objects from a FIS-Broker source with their CSW guids and CKAN ids.
  - `list_datasets_berlin_source`: List all datasets which have a particular value for the extra `berlin_source` (e.g. all with `'berlin_source': 'harvest-fisbroker'`).
- Fix typos in log output.
- Fix version in setup.py.

## [1.1.1](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.1.1)

_(2020-10-23)_

- Add documentation for `-o`/`-l` options for paster command.

## [1.1.0](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.1.0)

_(2020-10-09)_

- Add paster command `fisbroker`.
- Add reimport functionality (fetch and import a set of datasets that have been previously harvested), with access through UI (button on dataset page) and paster command.

## [1.0.0](https://github.com/berlinonline/ckanext-fisbroker/releases/tag/1.0.0)

_(2020-02-25)_

- Add extensive unit tests.
- Add changelog and version numbers.
- Move repository from its old location [knudmoeller/fisbroker](https://github.com/knudmoeller/fisbroker) to [berlinonline/ckanext-fisbroker](https://github.com/berlinonline/ckanext-fisbroker).
