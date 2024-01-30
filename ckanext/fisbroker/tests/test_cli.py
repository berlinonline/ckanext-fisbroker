# coding: utf-8
'''Tests for the harvester's click CLI.'''

import copy
import json
import logging
import pytest

from ckan.cli.cli import ckan
from ckan.logic.action.update import package_update

from ckanext.harvest.queue import gather_stage, fetch_and_import_stages
from ckanext.harvest.model import HarvestObject

from ckanext.spatial.tests.conftest import harvest_setup

from ckanext.fisbroker import HARVESTER_ID
from ckanext.fisbroker.cli import fisbroker
from ckanext.fisbroker.fisbroker_harvester import FisbrokerHarvester
from ckanext.fisbroker.tests import FisbrokerTestBase, base_context, FISBROKER_HARVESTER_CONFIG, WFS_FIXTURE, FISBROKER_PLUGIN

LOG = logging.getLogger(__name__)

@pytest.mark.ckan_config('ckan.plugins', f"{FISBROKER_PLUGIN} {HARVESTER_ID} harvest dummyharvest")
@pytest.mark.usefixtures('with_plugins', 'clean_db', 'clean_index', 'harvest_setup')
class TestCli(FisbrokerTestBase):

    def test_list_sources_none(self, cli):
        cli.mix_stderr = False
        result = cli.invoke(ckan, ['fisbroker', 'list-sources'])
        assert result.exit_code == 0
        result_data = json.loads(result.stdout)
        assert len(result_data) == 0
    
    def test_list_sources_single(self, cli, base_context):
        self._create_source()
        cli.mix_stderr = False
        result = cli.invoke(ckan, ['fisbroker', 'list-sources'])
        
        assert result.exit_code == 0
        result_data = json.loads(result.stdout)
        assert len(result_data) == 1
        assert result_data[0]['title'] == FISBROKER_HARVESTER_CONFIG['title']
        assert result_data[0]['type'] == FISBROKER_HARVESTER_CONFIG['source_type']
        assert result_data[0]['url'] == FISBROKER_HARVESTER_CONFIG['url']

    def test_list_datasets_none(self, cli):
        cli.mix_stderr = False
        result = cli.invoke(ckan, ['fisbroker', 'list-datasets'])
        assert result.exit_code == 0
        result_data = json.loads(result.stdout)
        assert len(result_data) == 0

    @pytest.mark.parametrize("parameters", [
        'fisbroker list-datasets',
        'fisbroker list-datasets --source {}',
    ])
    def test_list_datasets_single(self, cli, base_context, parameters: str):
        # Create source1
        cli.mix_stderr = False
        source, job = self._create_source_and_job(WFS_FIXTURE)
        self._run_job_for_single_document(job, WFS_FIXTURE['object_id'])

        parameters = parameters.format(source.id)
        parameter_list = parameters.split()

        result = cli.invoke(ckan, parameter_list)
        assert result.exit_code == 0
        result_data = json.loads(result.stdout)
        assert source.id in result_data
        assert len(result_data[source.id]) == 1
        assert 'id' in result_data[source.id][0]
        assert 'name' in result_data[source.id][0]
        assert 'title' in result_data[source.id][0]
        assert result_data[source.id][0]['title'] == "Nährstoffversorgung des Oberbodens 2015 (Umweltatlas) - [WFS]"

    def test_list_datasets_wrong_source(self, cli, base_context):
        # Create source1
        source, job = self._create_source_and_job(WFS_FIXTURE)
        self._run_job_for_single_document(job, WFS_FIXTURE['object_id'])

        wrong_id = 'wrong_id'

        cli.mix_stderr = False
        result = cli.invoke(ckan, ['fisbroker', 'list-datasets', '--source', wrong_id])
        assert result.exit_code == 0
        result_data = json.loads(result.stdout)
        assert source.id not in result_data
        assert wrong_id in result_data
        assert len(result_data[wrong_id]) == 0

    def test_harvest_objects_none(self, cli):
        result = cli.invoke(ckan, ['fisbroker', 'harvest-objects'])
        assert result.exit_code == 0
        result_data = json.loads(result.output)
        assert len(result_data) == 0

    @pytest.mark.parametrize("parameters", [
        'fisbroker harvest-objects',
        'fisbroker harvest-objects --source {}',
    ])
    def test_harvest_objects_single(self, cli, base_context, parameters: str):
        # Create source1
        source, job = self._create_source_and_job(WFS_FIXTURE)
        self._run_job_for_single_document(job, WFS_FIXTURE['object_id'])

        parameters = parameters.format(source.id)
        parameter_list = parameters.split()

        result = cli.invoke(ckan, parameter_list)
        assert result.exit_code == 0
        result_data = json.loads(result.output)
        assert source.id in result_data
        assert len(result_data[source.id]) == 1
        assert result_data[source.id][0]['csw_guid'] == WFS_FIXTURE['object_id']

    def test_list_datasets_berlinsource_none(self, cli):
        result = cli.invoke(ckan, ['fisbroker', 'list-datasets-berlin-source'])
        assert result.exit_code == 0
        result_data = json.loads(result.output)
        assert len(result_data) == 0

    @pytest.mark.parametrize("parameters", [
        ['fisbroker', 'list-datasets-berlin-source'],
        ['fisbroker', 'list-datasets-berlin-source', '-b', 'harvest-fisbroker'],
    ])
    def test_list_datasets_berlinsource_single(self, cli, base_context, parameters: list):
        source, job = self._create_source_and_job(WFS_FIXTURE)
        self._run_job_for_single_document(job, WFS_FIXTURE['object_id'])

        result = cli.invoke(ckan, parameters)
        assert result.exit_code == 0
        result_data = json.loads(result.output)
        assert len(result_data) == 1

    @pytest.mark.parametrize("parameters", [
        'fisbroker last-successful-job',
        'fisbroker last-successful-job --source {}',
    ])
    def test_last_successful_job_exists(self, cli, base_context, parameters: list):
        source, job = self._create_source_and_job()
        object_ids = gather_stage(FisbrokerHarvester(), job)
        for object_id in object_ids:
            harvest_object = HarvestObject.get(object_id)
            fetch_and_import_stages(FisbrokerHarvester(), harvest_object)
        job.status = 'Finished'
        job.save()

        parameters = parameters.format(source.id)
        parameter_list = parameters.split()

        cli.mix_stderr = False
        result = cli.invoke(ckan, parameter_list)
        assert result.exit_code == 0

        result_data = json.loads(result.stdout)
        assert source.id in result_data
        assert result_data[source.id]['status'] == "Finished"
        assert result_data[source.id]['id'] == job.id

    def test_last_successful_job_wrong_source(self, cli, base_context):
        source, job = self._create_source_and_job()
        object_ids = gather_stage(FisbrokerHarvester(), job)
        for object_id in object_ids:
            harvest_object = HarvestObject.get(object_id)
            fetch_and_import_stages(FisbrokerHarvester(), harvest_object)
        job.status = 'Finished'
        job.save()

        cli.mix_stderr = False
        result = cli.invoke(ckan, ['fisbroker', 'last-successful-job', '--source', 'foobar'])
        assert result.exit_code == 0

        result_data = json.loads(result.stdout)
        assert len(result_data) == 0

    def test_reimport_cli_single_dataset(self, cli, base_context):
        source, job = self._create_source_and_job(FISBROKER_HARVESTER_CONFIG)
        datasets = self._create_mock_data(source, job, first=0, last=5)
        dataset_id = datasets[0]['id']

        cli.mix_stderr = False
        result = cli.invoke(ckan, ['fisbroker', 'reimport-dataset', '--datasetid', dataset_id])

        assert result.exit_code == 0
        result_data = json.loads(result.stdout)
        assert len(result_data) == 1
        assert list(result_data.keys())[0] == dataset_id
        assert 'fisbroker_guid' in result_data[dataset_id]
        assert 'title' in result_data[dataset_id]

    @pytest.mark.parametrize("num_datasets", [ 1, 3, 5])
    def test_reimport_cli_one_source(self, cli, base_context, num_datasets: int):
        source, job = self._create_source_and_job(FISBROKER_HARVESTER_CONFIG)
        self._create_mock_data(source, job, first=0, last=num_datasets-1)

        cli.mix_stderr = False
        result = cli.invoke(ckan, ['fisbroker', 'reimport-dataset'])

        assert result.exit_code == 0
        result_data = json.loads(result.stdout)
        assert len(result_data) == num_datasets
        for ckan_id, metadata in result_data.items():
            assert 'fisbroker_guid' in metadata
            assert 'title' in metadata

    @pytest.mark.parametrize("num_datasets", [ 1, 3, 5])
    def test_reimport_cli_two_sources(self, cli, base_context, num_datasets: int):
        source_1, job_1 = self._create_source_and_job(FISBROKER_HARVESTER_CONFIG)
        self._create_mock_data(source_1, job_1, first=0, last=num_datasets-1)
        
        config_2 = copy.copy(FISBROKER_HARVESTER_CONFIG)
        config_2['title'] = f"{FISBROKER_HARVESTER_CONFIG['title']} 2"
        config_2['name'] = f"{FISBROKER_HARVESTER_CONFIG['name']}_2"
        source_2, job_2 = self._create_source_and_job(config_2)
        self._create_mock_data(source_2, job_2, first=num_datasets, last=9)

        cli.mix_stderr = False
        result = cli.invoke(ckan, ['fisbroker', 'reimport-dataset', '--source', source_1.id])

        assert result.exit_code == 0
        result_data = json.loads(result.stdout)
        assert len(result_data) == num_datasets
        for ckan_id, metadata in result_data.items():
            assert 'fisbroker_guid' in metadata
            assert 'title' in metadata

