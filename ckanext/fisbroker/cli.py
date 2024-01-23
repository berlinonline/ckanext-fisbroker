'''Module to implement a click CLI for the FIS-Broker-Harvester'''

import json
import logging
import sys
import time

import click

from ckan import logic
from ckan import model

from ckanext.fisbroker import HARVESTER_ID
import ckanext.fisbroker.blueprint as blueprint
from ckanext.fisbroker.fisbroker_harvester import FisbrokerHarvester

from ckanext.harvest.model import HarvestObject, HarvestSource

LOG = logging.getLogger(__name__)
FISBROKER_SOURCE_NAME = 'harvest-fisbroker'
JSON_INDENT = 2

def _filter_dataset(dataset):
    '''Filter relevant information for an individual dataset.'''
    return {
        'id': dataset.get('id'),
        'name': dataset.get('name'),
        'title': dataset.get('title')
    }

def _filter_datasets(datasets):
    '''Generate a list of filtered datasets.'''
    return [_filter_dataset(dataset) for dataset in datasets]

def _list_sources():
    '''List all instances of the FIS-Broker harvester.
    '''
    context = {'model': model, 'session': model.Session}
    sources = logic.get_action('harvest_source_list')(context, {})

    return [source for source in sources if source['type'] == HARVESTER_ID]

def _list_packages(source_id: str, offset: int = 0, limit: int = -1):
    '''List the ids and titles of all datasets harvested by the
    FIS-Broker harvester. Either of all instances or of the
    one specified by {source-id}.
    '''
    click.echo(f"listing datasets for source {source_id} ...", err=True)

    filter_query = f'harvest_source_id: "{source_id}"'
    search_dict = {
        'fq': filter_query,
        'start': 0,
        'rows': 500,
        'sort': 'name asc',
    }
    context = {'model': model, 'session': model.Session}

    total_results = []
    while True:
        result = logic.get_action('package_search')(context, search_dict)
        length = len(result['results'])
        total_results += result['results']
        search_dict['start'] = search_dict['start'] + length
        if len(total_results) >= result['count']:
            break

    if limit < 0:
        limit = len(total_results)

    return total_results[offset:offset+limit]


def _reimport_dataset(dataset_ids, context):
    '''Reimport all datasets in dataset_ids.'''

    reimported_packages = blueprint.reimport_batch(dataset_ids, context)

    result = {}
    for package_id, record in reimported_packages.items():
        result[package_id] = {
            'fisbroker_guid': record.identifier ,
            'title': record.identification.title
        }

    return result

def get_commands():
    return [fisbroker]

@click.group()
def fisbroker():
    '''
    Command-line actions for the FIS-Broker harvester
    '''
    pass

@fisbroker.command()
def list_sources():
    '''
    List all instances of the FIS-Broker harvester.
    '''
    click.echo("listing all instances of FisbrokerHarvester ...", err=True)
    sources = _list_sources()

    click.echo(json.dumps(sources, indent=JSON_INDENT))

@fisbroker.command()
@click.option("-s",  "--source", help="The source id of the harvester")
@click.option("-o", "--offset", default=0, help="Index of the first dataset to reimport")
@click.option("-l", "--limit", default=-1, help="Max number of datasets to reimport")
def list_datasets(source: str, offset: int, limit: int):
    '''
    List the ids and titles of all datasets harvested by the
    FIS-Broker harvester. Either of all instances or of the
    one specified by {source-id}.
    '''
    click.echo("listing datasets harvested by FisbrokerPlugin ...", err=True)
    sources = [_source.get('id') for _source in _list_sources()]
    if source is not None:
        sources = [source]
    filtered_packages = {}
    for _source in sources:
        start = time.time()
        packages = _list_packages(_source, offset, limit)
        filtered_packages[_source] = _filter_datasets(packages)
        click.echo(f"there were {len(filtered_packages[_source])} results ...", err=True)
        end = time.time()
        click.echo(f"This took {end - start} seconds", err=True)

    click.echo(json.dumps(filtered_packages, indent=JSON_INDENT))


@fisbroker.command()
@click.option("-s",  "--source", help="The source id of the harvester")
def harvest_objects(source: str):
    '''
    Show all harvest objects with their CSW-guids and CKAN package ids, either
    of the harvester instance specified by {source-id}, or of all instances.
    '''
    sources = [_source.get('id') for _source in _list_sources()]
    if source is not None:
        sources = [str(source)]
    output = {}
    for _source in sources:
        harvest_job = MockHarvestJob()
        harvest_job.source = HarvestSource.get(_source)
        harvest_job.id = 'fakeid'
        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).\
            filter(HarvestObject.current == True).\
            filter(HarvestObject.harvest_source_id == harvest_job.source.id)
        harvest_objects = []

        for guid, package_id in query:
            harvest_objects.append({
                "csw_guid": guid,
                "package_id": package_id
            })

        output[str(_source)] = harvest_objects

    click.echo(json.dumps(output, indent=JSON_INDENT))

@fisbroker.command()
@click.option("-s",  "--source", help="The source id of the harvester")
def last_successful_job(source: str):
    '''
    Show the last successful job that was not a reimport job, either
    of the harvester instance specified by {source-id}, or of
    all instances.
    '''
    sources = []
    output = {}
    if source:
        click.echo(f"finding last successful job from a single source: {source} ...", err=True)
        sources = [source]
    else:
        sources = [source.get('id') for source in _list_sources()]
        click.echo("finding last successful job from all sources ...", err=True)
    for source in sources:
        harvest_job = MockHarvestJob()
        harvest_job.source = HarvestSource.get(source)
        harvest_job.id = 'fakeid'
        last_successful_job = FisbrokerHarvester.last_error_free_job(harvest_job)
        if last_successful_job:
            output[source] = last_successful_job.as_dict()

    click.echo(json.dumps(output, indent=JSON_INDENT))

@fisbroker.command()
@click.option("-b", "--berlinsource", default='harvest-fisbroker', help="The value for the 'berlin_source' extra we want to filter by.")
def list_datasets_berlin_source(berlinsource: str):
    '''
    Show all active datasets for which the 'berlin_source' extra is {berlin_source}
    (default is 'harvest-fisbroker').
    '''
    query = model.Session.query(model.Package).filter_by(
        state=model.State.ACTIVE)
    datasets = [
        {
            "name": pkg.name,
            "title": pkg.title,
            "id": pkg.id,
            "extras": {key: value for key, value in pkg.extras.items()}
        } for pkg in query ]

    datasets = [dataset for dataset in datasets if dataset["extras"].get("berlin_source", "_undefined") == berlinsource]
    filtered_packages = _filter_datasets(datasets)

    click.echo(json.dumps(filtered_packages, indent=JSON_INDENT))

@fisbroker.command()
@click.option("-s", "--source", help="The source id of the harvester")
@click.option("-d", "--datasetid", help="The id of the dataset")
@click.option("-o", "--offset", default=0, help="Index of the first dataset to reimport")
@click.option("-l", "--limit", default=-1, help="Max number of datasets to reimport")
@click.pass_context
def reimport_dataset(ctx: click.Context, source: str, datasetid: str, offset: int, limit: int):
    '''
    Reimport the specified datasets. The specified datasets are either
    all datasets by all instances of the FIS-Broker harvester (if no options
    are used), or all datasets by the FIS-Broker harvester instance with
    {source-id}, or the single dataset identified by {dataset-id}.
    To reimport only a subset or page through the complete set of datasets,
    use the --offset,-o and --limit,-l options.
    '''
    click.echo("reimporting datasets ...", err=True)
    package_ids = []
    if datasetid:
        click.echo("reimporting a single dataset ...", err=True)
        package_ids = [ datasetid ]
    else:
        sources = []
        if source:
            click.echo(f"reimporting all dataset from a single source: {source} ...", err=True)
            sources = [ source ]
        else:
            click.echo("reimporting all dataset from all sources ...", err=True)
            sources = [ source.get('id') for source in _list_sources() ]
        for source in sources:
            package_ids += [package['name'] for package in _list_packages(source, offset, limit)]

    start = time.time()
    site_user = logic.get_action(u'get_site_user')({
        u'model': model,
        u'ignore_auth': True},
        {}
    )
    context = {
        u'model': model,
        u'session': model.Session,
        u'ignore_auth': True,
        u'user': site_user['name'],
    }
    flask_app = ctx.meta['flask_app']
    with flask_app.test_request_context():
        output = _reimport_dataset(package_ids, context)

    click.echo(json.dumps(output, indent=JSON_INDENT))
    end = time.time()
    click.echo(f"This took {end - start} seconds", err=True)

class MockHarvestJob:
    pass
