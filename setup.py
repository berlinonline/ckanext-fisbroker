from setuptools import setup, find_packages  # Always prefer setuptools over distutils
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))
pluginname = 'fisbroker'

# Get the long description from the relevant file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'ckanext', pluginname, 'VERSION')) as version_file:
    version = version_file.read().strip()

setup(
    name=f'ckanext-{pluginname}',

    version=version,

    description='''Plugin to harvest Berlin's FIS-Broker geo information system into the datenregister.berlin.de schema''',
    long_description=long_description,

    # The project's main homepage.
    url=f'https://github.com/berlinonline/ckanext-{pluginname}',

    # Author details
    author='''Knud M\xc3\xb6ller''',
    author_email='''knud.moeller@berlinonline.de''',

    # Choose your license
    license='AGPL',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        # 3 - Alpha
        # 4 - Beta
        # 5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
    ],


    # What does your project relate to?
    keywords='''CKAN ISpatialHarvester IBlueprint IClick IConfigurer ITemplateHelpers geo harvesting berlin fisbroker''',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),

    # List run-time dependencies here.  These will be installed by pip when your
    # project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/technical.html#install-requires-vs-requirements-files
    install_requires=[],

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    include_package_data=True,

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points='''
        [ckan.plugins]
        fisbroker=ckanext.fisbroker.plugin:FisbrokerPlugin
        dummyharvest=ckanext.fisbroker.tests.test_blueprint:DummyHarvester

    ''',
)
