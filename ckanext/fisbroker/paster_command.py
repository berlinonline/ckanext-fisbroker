'''Module to implement a paster action for the FIS-Broker-Harvester'''

import logging
import sys
import time
from ckan import logic
from ckan import model
from ckan.lib import cli

from ckanext.fisbroker import HARVESTER_ID
import ckanext.fisbroker.controller as controller
from ckanext.fisbroker.plugin import FisbrokerPlugin

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestSource

LOG = logging.getLogger(__name__)
FISBROKER_SOURCE_NAME = u'harvest-fisbroker'

class FISBrokerCommand(cli.CkanCommand):
    '''Actions for the FIS-Broker harvester

    Usage:

      fisbroker list_sources
        - List all instances of the FIS-Broker harvester.

      fisbroker [-s {source-id}] list_datasets
        - List the ids and titles of all datasets harvested by the
          FIS-Broker harvester. Either of all instances or of the
          one specified by {source-id}.

      fisbroker [-s|-d {source|dataset-id}] [-o {offset}] [-l {limit}] reimport_dataset
        - Reimport the specified datasets. The specified datasets are either
          all datasets by all instances of the FIS-Broker harvester (if no options
          are used), or all datasets by the FIS-Broker harvester instance with
          {source-id}, or the single dataset identified by {dataset-id}.
          To reimport only a subset or page through the complete set of datasets,
          use the --offset,-o and --limit,-l options.

      fisbroker [-s {source-id}] last_successful_job
        - Show the last successful job that was not a reimport job, either
          of the harvester instance specified by {source-id}, or of
          all instances.

      fisbroker [-s {source-id}] harvest_objects
        - Show all harvest objects with their CSW-guids and CKAN package ids, either
          of the harvester instance specified by {source-id}, or of all instances.

      fisbroker [-b {berlin_source}] list_datasets_berlin_source
        - Show all datasets for which the 'berlin_source' extra is {berlin_source}
          (default is 'harvest-fisbroker').
    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def __init__(self,name):

        super(FISBrokerCommand, self).__init__(name)

        self.parser.add_option('-s',
                               '--source-id',
                               dest='source_id',
                               default=False,
                               help='Id of the FIS-Broker instance to consider')

        self.parser.add_option('-d',
                               '--dataset-id',
                               dest='dataset_id',
                               default=False,
                               help='Id of dataset to reimport')

        self.parser.add_option('-l',
                               '--limit',
                               dest='limit',
                               default=False,
                               type='int',
                               help='Max number of datasets to reimport')

        self.parser.add_option('-o',
                               '--offset',
                               dest='offset',
                               default=False,
                               type='int',
                               help='Index of the first dataset to reimport')

        self.parser.add_option('-b',
                               '--berlinsource',
                               dest='berlin_source',
                               default=FISBROKER_SOURCE_NAME,
                               help='Value of the `berlin_source` extra to filter by')

    def print_dataset(self, dataset):
        '''Print an individual dataset.'''
        print(f"{dataset.get('id')},{dataset.get('name')},\"{dataset.get('title')}\"")

    def print_datasets(self, datasets):
        '''Print all datasets.'''
        print('id,name,title')
        for dataset in datasets:
            self.print_dataset(dataset)

    def print_harvest_sources(self, sources):
        '''Print all harvest sources (taken from ckanext-harvest).'''
        if sources:
            print()
        for source in sources:
            self.print_harvest_source(source)

    def print_harvest_source(self, source):
        '''Print an individual harvest source (taken from ckanext-harvest).'''
        print(f"Source id: {source.get('id')}")
        if 'name' in source:
            # 'name' is only there if the source comes from the Package
            print(f"     name: {source.get('name')}")
        print(f"      url: {source.get('url')}")
        # 'type' if source comes from HarvestSource, 'source_type' if it comes
        # from the Package
        print(f"   active: {source.get('active', source.get('state') == 'active')}")
        print(f"frequency: {source.get('frequency')}")
        print(f"     jobs: {source.get('status').get('job_count')}")
        print()

    def print_harvest_objects(self, harvest_objects):
        '''Print all harvest objects.'''
        print('source_id,csw_guid,package_id')
        for ho in harvest_objects:
            print(f"{ho['source_id']},{ho['csw_guid']},{ho['package_id']}")

    def list_sources(self):
        '''List all instances of the FIS-Broker harvester.
        '''
        context = {'model': model, 'session': model.Session}
        sources = logic.get_action('harvest_source_list')(context, {})

        return [source for source in sources if source['type'] == HARVESTER_ID]


    def list_packages(self, source_id):
        '''List the ids and titles of all datasets harvested by the
        FIS-Broker harvester. Either of all instances or of the
        one specified by {source-id}.
        '''

        LOG.debug(f"listing datasets for source {source_id} ...")

        filter_query = f'harvest_source_id: "{source_id}"'
        search_dict = {
            'fq': filter_query,
            'start': 0,
            'rows': 500,
            'sort': 'metadata_modified desc',
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

        offset = 0
        limit = len(total_results)
        if self.options.offset:
            offset = self.options.offset
        if self.options.limit:
            limit = self.options.limit

        return total_results[offset:offset+limit]

    def reimport_dataset(self, dataset_ids):
        '''Reimport all datasets in dataset_ids.'''

        fb_controller = controller.FISBrokerController()
        context = {'model': model, 'session': model.Session}
        result = fb_controller.reimport_batch(dataset_ids, context)

        return result


    def command(self):
        '''Implementation of the paster command
        '''

        class MockHarvestJob:
            pass

        self._load_config()

        if not self.args:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]

        if cmd == 'list_sources':
            LOG.debug("listing all instances of FisbrokerPlugin ...")
            sources = self.list_sources()
            self.print_harvest_sources(sources)
        elif cmd == 'list_datasets':
            LOG.debug("listing datasets harvested by FisbrokerPlugin ...")
            sources = [source.get('id') for source in self.list_sources()]
            if len(self.args) >= 2:
                sources = [str(self.args[1])]
            for source in sources:
                start = time.time()
                packages = self.list_packages(source)
                self.print_datasets(packages)
                LOG.debug(f"there were {len(packages)} results ...")
                end = time.time()
                LOG.debug(f"This took {end - start} seconds")
        elif cmd == 'reimport_dataset':
            LOG.debug("reimporting datasets ...")
            package_ids = []
            if self.options.dataset_id:
                LOG.debug("reimporting a single dataset ...")
                package_ids = [ str(self.options.dataset_id) ]
            else:
                sources = []
                if self.options.source_id:
                    LOG.debug(f"reimporting all dataset from a single source: {self.options.source_id} ...")
                    sources = [ str(self.options.source_id) ]
                else:
                    LOG.debug("reimporting all dataset from all sources ...")
                    sources = [ source.get('id') for source in self.list_sources() ]
                for source in sources:
                    package_ids += [package['name'] for package in self.list_packages(source)]
            start = time.time()
            self.reimport_dataset(package_ids)
            end = time.time()
            LOG.debug(f"This took {end - start} seconds")
        elif cmd == 'last_successful_job':
            sources = []
            if self.options.source_id:
                LOG.debug(f"finding last successful job from a single source: {self.options.source_id} ...")
                sources = [str(self.options.source_id)]
            else:
                LOG.debug("finding last successful job from all sources ...")
                sources = [source.get('id') for source in self.list_sources()]
            for source in sources:
                harvest_job = MockHarvestJob()
                harvest_job.source = HarvestSource.get(source)
                harvest_job.id = 'fakeid'
                last_successful_job = FisbrokerPlugin.last_error_free_job(harvest_job)
                LOG.debug(last_successful_job)
        elif cmd == 'harvest_objects':
            sources = []
            if self.options.source_id:
                LOG.debug(f"finding all harvest objects from a single source: {self.options.source_id} ...")
                sources = [str(self.options.source_id)]
            else:
                LOG.debug("finding all harvest objects from all sources ...")
                sources = [source.get('id') for source in self.list_sources()]
            for source in sources:
                harvest_job = MockHarvestJob()
                harvest_job.source = HarvestSource.get(source)
                harvest_job.id = 'fakeid'
                query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).\
                    filter(HarvestObject.current == True).\
                    filter(HarvestObject.harvest_source_id == harvest_job.source.id)
                harvest_objects = []

                for guid, package_id in query:
                    harvest_objects.append({
                        "source_id": source,
                        "csw_guid": guid,
                        "package_id": package_id
                    })

                self.print_harvest_objects(harvest_objects)
        elif cmd == 'list_datasets_berlin_source':
            LOG.debug(f"finding all packages with extra 'berlin_source' == '{self.options.berlin_source}' ...")
            query = model.Session.query(model.Package).filter_by(
                state=model.State.ACTIVE)
            packages = [
                {
                    "name": pkg.name,
                    "id": pkg.id,
                    "extras": {key: value for key, value in pkg.extras.items()}
                } for pkg in query ]
            print('package_name,package_id')
            for package in packages:
                if (package["extras"].get("berlin_source", "_undefined") == self.options.berlin_source):
                    print(f"{package['name']},{package['id']}")
        else:
            print(f'Command {cmd} not recognized')
