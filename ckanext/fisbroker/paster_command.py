'''Module to implement a paster action for the FIS-Broker-Harvester'''

import logging
import sys
import time
from ckan import logic
from ckan import model
from ckan.lib import cli

from ckanext.fisbroker import HARVESTER_ID
import ckanext.fisbroker.controller as controller

LOG = logging.getLogger(__name__)

class FISBrokerCommand(cli.CkanCommand):
    '''Actions for the FIS-Broker harvester

    Usage:

      fisbroker list_sources
        - List all instances of the FIS-Broker harvester.

        - List the ids and titles of all datasets harvested by the
      fisbroker [-s {source-id}] list_datasets
          FIS-Broker harvester. Either of all instances or of the
          one specified by {source-id}.

      fisbroker [-s|-d {source|dataset-id}] reimport_dataset
        - Reimport the specified datasets. The specified datasets are either
          all datasets by all instances of the FIS-Broker harvester (if no options
          are used), or all datasets by the FIS-Broker harvester instance with
          {source-id}, or the single dataset identified by {dataset-id}.
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

    def print_dataset(self, dataset):
        '''Print an individual dataset.'''
        print u'{},{},"{}"'.format(dataset.get('id'), dataset.get('name'),dataset.get('title')).encode('utf-8')

    def print_datasets(self, datasets):
        '''Print all datasets.'''
        print 'id,name,title'
        for dataset in datasets:
            self.print_dataset(dataset)

    def print_harvest_sources(self, sources):
        '''Print all harvest sources (taken from ckanext-harvest).'''
        if sources:
            print ''
        for source in sources:
            self.print_harvest_source(source)

    def print_harvest_source(self, source):
        '''Print an individual harvest source (taken from ckanext-harvest).'''
        print 'Source id: %s' % source.get('id')
        if 'name' in source:
            # 'name' is only there if the source comes from the Package
            print '     name: %s' % source.get('name')
        print '      url: %s' % source.get('url')
        # 'type' if source comes from HarvestSource, 'source_type' if it comes
        # from the Package
        print '   active: %s' % (source.get('active',
                                            source.get('state') == 'active'))
        print 'frequency: %s' % source.get('frequency')
        print '     jobs: %s' % source.get('status').get('job_count')
        print ''

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

        LOG.debug("listing datasets for source %s ...", source_id)

        filter_query = 'harvest_source_id:"{0}"'.format(source_id)
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
                sources = [unicode(self.args[1])]
            for source in sources:
                start = time.time()
                packages = self.list_packages(source)
                self.print_datasets(packages)
                LOG.debug("there were %i results ...", len(packages))
                end = time.time()
                LOG.debug("This took %f seconds", end - start)
        elif cmd == 'reimport_dataset':
            LOG.debug("reimporting datasets ...")
            package_ids = []
            if self.options.dataset_id:
                LOG.debug("reimporting a single dataset ...")
                package_ids = [ unicode(self.options.dataset_id) ]
            else:
                sources = []
                if self.options.source_id:
                    LOG.debug("reimporting all dataset from a single source: %s ...", self.options.source_id)
                    sources = [ unicode(self.options.source_id) ]
                else:
                    LOG.debug("reimporting all dataset from all sources ...")
                    sources = [ source.get('id') for source in self.list_sources() ]
                for source in sources:
                    package_ids += [package['name'] for package in self.list_packages(source)]
            LOG.debug(package_ids)
            self.reimport_dataset(package_ids)
        else:
            print 'Command %s not recognized' % cmd
