"""Tests for plugin.py."""

import logging

from lxml import etree

from ckan.lib.base import config
from ckan.logic import get_action
from ckan.logic.schema import default_update_package_schema
from ckan import model
from ckan.model import Session

from ckanext.harvest.model import (
    HarvestSource,
    HarvestJob,
    HarvestObject,
    HarvestObjectExtra
)
from ckanext.fisbroker.plugin import FisbrokerPlugin

from ckanext.fisbroker.tests.xml_file_server import serve

# Start simple HTTP server that serves XML test files
serve()

PLUGIN_NAME = 'fisbroker'
LOG = logging.getLogger(__name__)

class TestPlugin(object):

    def setup(self):
        # Add sysadmin user
        harvest_user = model.User(name=u'harvest', password=u'test', sysadmin=True)
        Session.add(harvest_user)
        Session.commit()
        package_schema = default_update_package_schema()
        self.context = {
            'model': model,
            'session': Session,
            'user': u'harvest',
            'schema': package_schema,
            'api_version': '2'
        }

    def teardown(self):
        model.repo.rebuild_db()

    def _create_job(self,source_id):
        # Create a job
        context = {
            'model': model,
            'session': Session,
            'user': u'harvest'
        }

        job_dict=get_action('harvest_job_create')(context,{'source_id':source_id})
        job = HarvestJob.get(job_dict['id'])
        assert job

        return job

    def _create_source_and_job(self, source_fixture):
        context = {
            'model': model,
            'session': Session,
            'user': u'harvest'
        }

        if config.get('ckan.harvest.auth.profile') == u'publisher' \
            and not 'publisher_id' in source_fixture:
            source_fixture['publisher_id'] = self.publisher.id

        source_dict=get_action('harvest_source_create')(context,source_fixture)
        source = HarvestSource.get(source_dict['id'])
        assert source

        job = self._create_job(source.id)

        return source, job

    def _run_job_for_single_document(self, harvest_job, object_id):

        harvester = FisbrokerPlugin()

        # we circumvent gather_stage() and fetch_stage() and just load the
        # content with a known object_id and create the harvest object:
        url = harvest_job.source.url
        content = harvester._get_content(url)
        obj = HarvestObject(guid=object_id,
                            job=harvest_job,
                            content=content,
                            extras=[HarvestObjectExtra(key='status',value='new')])
        obj.save()

        assert obj, obj.content

        harvester.import_stage(obj)
        Session.refresh(obj)

        harvest_job.status = u'Finished'
        harvest_job.save()

        return obj

    def test_open_data_wfs_service(self):
        # Create source1
        wfs_fixture = {
            'title': 'Test Source',
            'name': 'test-source',
            'url': u'http://127.0.0.1:8999/wfs-dataset.xml',
            'object_id': u'65715c6e-bbaf-3def-982b-3b5156272da7',
            'source_type': u'fisbroker'
        }

        source1, first_job = self._create_source_and_job(wfs_fixture)

        first_obj = self._run_job_for_single_document(first_job, wfs_fixture['object_id'])

        first_package_dict = get_action('package_show_rest')(self.context,{'id':first_obj.package_id})

        # Package was created
        assert first_package_dict
        assert first_package_dict['state'] == u'active'
        assert first_obj.current == True

        print(first_package_dict['extras'])

