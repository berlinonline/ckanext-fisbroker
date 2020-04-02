"""Common code for the various FIS-Broker test classes."""

from dateutil.parser import parse
import logging
import warnings

from lxml import etree
from sqlalchemy import exc as sa_exc

from ckan.lib.base import config
from ckan.logic import get_action
from ckan import model
from ckan.model import Session
from ckan.logic.schema import default_update_package_schema
from ckan.tests import helpers
from ckan.tests import factories as ckan_factories

from ckanext.harvest.model import (
    HarvestSource,
    HarvestJob,
    HarvestObject,
    HarvestObjectExtra
)
from ckanext.harvest.tests import factories as harvest_factories

from ckanext.fisbroker import HARVESTER_ID
from ckanext.fisbroker.plugin import FisbrokerPlugin
from ckanext.fisbroker.tests.mock_fis_broker import start_mock_server, VALID_GUID, METADATA_OLD
from ckanext.fisbroker.tests.xml_file_server import serve

LOG = logging.getLogger(__name__)
MOCK_PORT=8888
FISBROKER_HARVESTER_CONFIG = {
    'title': 'FIS-Broker Harvest Source' ,
    'name': 'fis-broker-harvest-source' ,
    'source_type': HARVESTER_ID ,
    'url' : "http://127.0.0.1:{}/csw".format(MOCK_PORT)
}


# Start simple HTTP server that serves XML test files
serve()
# Start mock CSW-Server
start_mock_server(MOCK_PORT)

warnings.filterwarnings("ignore", category=sa_exc.SAWarning)

def _assert_equal(actual, expected):
    """Wrapper for `assert expected == actual` that also logs the
       values for expected and actual."""

    LOG.debug("expected: %s", expected)
    LOG.debug("actual:   %s", actual)
    assert expected == actual

def _assert_not_equal(actual, not_expected):
    """Wrapper for `assert expected != actual` that also logs the
       values for expected and actual."""

    LOG.debug("not_expected: %s", not_expected)
    LOG.debug("actual:   %s", actual)
    assert not_expected != actual

class FisbrokerTestBase(helpers.FunctionalTestBase):

    def setup(self):
        super(FisbrokerTestBase, self).setup()
        # Add sysadmin user
        user_name = u'harvest'
        harvest_user = model.User(name=user_name, password=u'test', sysadmin=True)
        Session.add(harvest_user)
        Session.commit()
        package_schema = default_update_package_schema()
        self.context = {
            'model': model,
            'session': Session,
            'user': user_name,
            'schema': package_schema,
            'api_version': '2'
        }

    def teardown(self):
        model.repo.rebuild_db()

    def _create_source(self, source_fixture=FISBROKER_HARVESTER_CONFIG):
        context = {
            'model': model,
            'session': Session,
            'user': u'harvest'
        }

        source_dict = get_action('harvest_source_create')(context,source_fixture)
        source = HarvestSource.get(source_dict['id'])
        assert source

        return source

    def _create_job(self,source_id):
        # Create a job
        context = {
            'model': model,
            'session': Session,
            'user': u'harvest'
        }

        job_dict = get_action('harvest_job_create')(context,{'source_id':source_id})
        job = HarvestJob.get(job_dict['id'])
        assert job

        return job

    def _create_source_and_job(self, source_fixture=FISBROKER_HARVESTER_CONFIG):

        source = self._create_source(source_fixture)
        job = self._create_job(source.id)

        return source, job

    def _run_job_for_single_document(self, harvest_job, object_id):

        harvester = FisbrokerPlugin()

        # we circumvent gather_stage() and fetch_stage() and just load the
        # content with a known object_id and create the harvest object:
        url = harvest_job.source.url
        # _get_content() returns XML
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

    def _harvester_setup(self, source_config, fb_guid=VALID_GUID):
        # create a harvest source and matching job
        source, job = self._create_source_and_job(source_config)
        fb_dataset = ckan_factories.Dataset()
        # this makes sure that fb_dataset is marked as having been
        # harvested by source
        harvest_object = harvest_factories.HarvestObjectObj(guid=fb_guid,
                                                            job=job,
                                                            source=source,
                                                            package_id=fb_dataset['id'])
        harvest_object.current = True
        harvest_object.metadata_modified_date = parse(METADATA_OLD)

        job.status = u'Finished'
        job.save()

        return fb_dataset, source, job
