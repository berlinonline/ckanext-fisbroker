# coding: utf-8

import logging
import os

from ckan.plugins import IBlueprint, IClick, IConfigurer, ITemplateHelpers
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckanext.fisbroker import blueprint
import ckanext.fisbroker.helper as helpers
import ckanext.fisbroker.cli as cli

LOG = logging.getLogger(__name__)

class FisbrokerPlugin(plugins.SingletonPlugin):

    plugins.implements(IClick)
    plugins.implements(IConfigurer)
    plugins.implements(ITemplateHelpers)
    plugins.implements(IBlueprint, inherit=True)

    # IClick

    def get_commands(self):
        '''
        Implementation of IClick.get_commands(): https://docs.ckan.org/en/2.9/extensions/plugin-interfaces.html#ckan.plugins.interfaces.IClick.get_commands
        '''
        return cli.get_commands()

    # IConfigurer

    def update_config(self, config):
        '''
        Implementation of
        https://docs.ckan.org/en/latest/extensions/plugin-interfaces.html#ckan.plugins.interfaces.IConfigurer.update_config
        '''
        toolkit.add_template_directory(config, os.path.join('theme', 'templates'))
        toolkit.add_public_directory(config, 'public')
        toolkit.add_resource('fanstatic', 'fisbroker')
        config['ckan.spatial.validator.profiles'] = 'always-valid'

    # -------------------------------------------------------------------
    # Implementation ITemplateHelpers
    # -------------------------------------------------------------------

    def get_helpers(self):
        '''
        Implementation of
        https://docs.ckan.org/en/latest/extensions/plugin-interfaces.html#ckan.plugins.interfaces.ITemplateHelpers.get_helpers
        '''
        return {
            'berlin_is_fisbroker_package': helpers.is_fisbroker_package,
            'berlin_fisbroker_guid': helpers.fisbroker_guid,
            'berlin_package_object': helpers.get_package_object,
            'berlin_is_reimport_job': helpers.is_reimport_job,
        }

    # IBlueprint

    def get_blueprint(self):
        """
        Implementation of
        https://docs.ckan.org/en/latest/extensions/plugin-interfaces.html#ckan.plugins.interfaces.IBlueprint.get_blueprint
        """
        return blueprint.reimportapi

