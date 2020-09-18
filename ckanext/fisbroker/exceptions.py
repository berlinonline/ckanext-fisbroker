# coding: utf-8
"""
This module defines exceptions for the ckanext-fisbroker plugin.
"""

from ckanext.fisbroker import HARVESTER_ID

ERROR_WRONG_HTTP = 1
ERROR_WRONG_CONTENT_TYPE = 2
ERROR_MISSING_ID = 3
ERROR_NOT_FOUND_IN_CKAN = 4
ERROR_NOT_HARVESTED = 5
ERROR_NOT_HARVESTED_BY_FISBROKER = 6
ERROR_NO_GUID = 7
ERROR_NO_CONNECTION = 8
ERROR_NO_CONNECTION_PACKAGE = 9
ERROR_NOT_FOUND_IN_FISBROKER = 10
ERROR_DURING_IMPORT = 11
ERROR_UNEXPECTED = 20


ERROR_MESSAGES = {
    ERROR_WRONG_HTTP: "Wrong HTTP method, only GET is allowed.",
    ERROR_WRONG_CONTENT_TYPE: "Wrong content type, only application/json is allowed.",
    ERROR_MISSING_ID: "Missing parameter 'id'.",
    ERROR_NOT_FOUND_IN_CKAN: "Package id '{}' does not exist. Cannot reimport package.",
    ERROR_NOT_HARVESTED: "Package could not be re-imported because it was not created by a harvester.",
    ERROR_NOT_HARVESTED_BY_FISBROKER: "Package could not be re-imported because it was not harvested by harvester '{}'.".format(HARVESTER_ID),
    ERROR_NO_GUID: "Package could not be re-imported because FIS-Broker GUID could not be determined.",
    ERROR_NO_CONNECTION: "Failed to establish connection to FIS-Broker service at {} ({}).",
    ERROR_NO_CONNECTION_PACKAGE: "Failed to establish connection to FIS-Broker service at {} ({}) while reimporting package '{}'.",
    ERROR_NOT_FOUND_IN_FISBROKER: "Package could not be re-imported because GUID '{}' was not found on FIS-Broker.",
    ERROR_DURING_IMPORT: "Package could not be re-imported because the FIS-Broker data is no longer valid. Reason: {}. Package will be deactivated.",
    ERROR_UNEXPECTED: "Unexpected error"
}

class NoFBHarvesterDefined(Exception):
    '''Exception for triggering reimport on a package id that doesn't exist.'''

    def __init__(self, msg="No FIS-Broker harvester found, cannot reimport."):
        super(NoFBHarvesterDefined, self).__init__(msg)

class ReimportError(Exception):
    '''Basic exception for reimporting datasets from FIS-Broker.'''

    def __init__(self, package_id, error_code=ERROR_UNEXPECTED, msg=None ):
        if msg is None:
            msg = "An error occured while trying to reimport package id {}".format(package_id)
        super(ReimportError, self).__init__(msg)
        self.package_id = package_id
        self.error_code = error_code

class PackageIdDoesNotExistError(ReimportError):
    '''Exception for triggering reimport on a package id that doesn't exist.'''

    def __init__(self, package_id):
        super(PackageIdDoesNotExistError, self).__init__(
            package_id,
            ERROR_NOT_FOUND_IN_CKAN,
            ERROR_MESSAGES[ERROR_NOT_FOUND_IN_CKAN].format(package_id)
        )

class PackageNotHarvestedError(ReimportError):
    '''Exception for triggering reimport on a package that wasn't harvested.'''

    def __init__(self, package_id):
        super(PackageNotHarvestedError, self).__init__(
            package_id,
            ERROR_NOT_HARVESTED,
            ERROR_MESSAGES[ERROR_NOT_HARVESTED]
        )

class PackageNotHarvestedInFisbrokerError(ReimportError):
    '''Exception for triggering reimport on a package that wasn't harvested by ckanext-fisbroker.'''

    def __init__(self, package_id):
        super(PackageNotHarvestedInFisbrokerError, self).__init__(
            package_id,
            ERROR_NOT_HARVESTED_BY_FISBROKER,
            ERROR_MESSAGES[ERROR_NOT_HARVESTED_BY_FISBROKER]
        )

class NoFisbrokerIdError(ReimportError):
    '''Exception for triggering reimport on a package that doesn't have a FIS-Broker guid.'''

    def __init__(self, package_id):
        super(NoFisbrokerIdError, self).__init__(
            package_id,
            ERROR_NO_GUID,
            ERROR_MESSAGES[ERROR_NO_GUID]
        )

class NoConnectionError(ReimportError):
    '''Exception raised when connection to FIS-Broker during reimport was not successful.'''

    def __init__(self, package_id, service_url, error_message):
        if package_id:
            message = ERROR_MESSAGES[ERROR_NO_CONNECTION_PACKAGE].format(service_url, error_message, package_id)
        else:
            message = ERROR_MESSAGES[ERROR_NO_CONNECTION].format(service_url, error_message)

        super(NoConnectionError, self).__init__(
            package_id,
            ERROR_NO_CONNECTION,
            message
        )
