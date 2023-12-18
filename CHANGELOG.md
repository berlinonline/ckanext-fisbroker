# Changelog

## Development

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
