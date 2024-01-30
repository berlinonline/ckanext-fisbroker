"""
This is a subclass of ckanext-spatial's CswService class,
adding the option to set the length of the `timeout` parameter
and retries for failed requests to the CSW endpoint.
"""
import six
import logging
from time import sleep

from owslib.etree import etree
from owslib.fes import PropertyIsEqualTo, SortBy, SortProperty

import ckanext.spatial.lib.csw_client as csw_client
from ckanext.spatial.harvesters.base import text_traceback

log = logging.getLogger(__name__)

class CswService(csw_client.CswService):
    """
    Perform various operations on a CSW service
    """
    from owslib.catalogue.csw2 import CatalogueServiceWeb as _Implementation

    def __init__(self, endpoint=None, timeout=10):
        if endpoint is not None:
            self._ows(endpoint, timeout)
        self.sortby = SortBy([SortProperty('dc:identifier')])

    def _ows(self, endpoint=None, timeout=10, **kw):
        if not hasattr(self, "_Implementation"):
            raise NotImplementedError("Needs an Implementation")
        if not hasattr(self, "__ows_obj__"):
            if endpoint is None:
                raise ValueError("Must specify a service endpoint")
            self.__ows_obj__ = self._Implementation(endpoint, timeout=timeout)
        return self.__ows_obj__

    def getidentifiers(self, qtype=None, typenames="csw:Record", esn="brief",
                       keywords=[], limit=None, page=10, outputschema="gmd",
                       startposition=0, cql=None, constraints=[], retries=3,
                       wait_time=5.0, **kw):
        from owslib.catalogue.csw2 import namespaces

        csw = self._ows(**kw)

        if qtype is not None:
           constraints.append(PropertyIsEqualTo("dc:type", qtype))

        kwa = {
            "constraints": constraints,
            "typenames": typenames,
            "esn": esn,
            "startposition": startposition,
            "maxrecords": page,
            "outputschema": namespaces[outputschema],
            "cql": cql,
            "sortby": self.sortby
            }
        i = 0
        matches = 0
        while True:
            # repeat the request up to [retries] times
            for attempt in range(1, retries + 1):
                log.info('Making CSW request: getrecords2 %r', kwa)

                try:
                    csw.getrecords2(**kwa)
                    if csw.exceptionreport:
                        err = 'Exceptionreport: %r' % csw.exceptionreport.exceptions
                        raise csw_client.CswError(err)
                    else:
                        break
                except Exception as e:
                    err = 'Error getting identifiers: %s' % text_traceback()
                    if attempt < retries:
                        log.info(err)
                        log.info('waiting %f seconds...' % wait_time)
                        sleep(wait_time)
                        log.info('Repeating request! (attempt #%d)' % (attempt + 1))
                        continue
                    else:
                        raise csw_client.CswError(err)

            if matches == 0:
                matches = csw.results['matches']

            identifiers = list(csw.records.keys())
            if limit is not None:
                identifiers = identifiers[:(limit-startposition)]
            for ident in identifiers:
                yield ident

            if len(identifiers) == 0:
                break

            i += len(identifiers)
            if limit is not None and i > limit:
                break

            startposition += page
            if startposition >= (matches + 1):
                break

            kwa["startposition"] = startposition

    def getrecordbyid(self, ids=[], esn="full", outputschema="gmd", retries=3, wait_time=5.0, **kw):
        from owslib.catalogue.csw2 import namespaces
        csw = self._ows(**kw)
        kwa = {
            "esn": esn,
            "outputschema": namespaces[outputschema],
            }
        # Ordinary Python version's don't support the metadata argument
        for attempt in range(1, retries + 1):
            log.info('Making CSW request: getrecordbyid %r %r', ids, kwa)

            try:
                csw.getrecordbyid(ids, **kwa)
                if csw.exceptionreport:
                    err = 'Exceptionreport: %r' % \
                        csw.exceptionreport.exceptions
                    raise csw_client.CswError(err)
                else:
                    break
            except Exception as e:
                err = 'Error getting record by id: %s' % text_traceback()
                if attempt < retries:
                    log.info(err)
                    log.info('Let them catch their breath - wait %f seconds...' % wait_time)
                    sleep(wait_time)
                    log.info('Repeating request! (attempt #%d)' % (attempt + 1))
                    continue
                else:
                    raise csw_client.CswError(err)

        if not csw.records:
            return
        record = self._xmd(list(csw.records.values())[0])

        ## strip off the enclosing results container, we only want the metadata
        #md = csw._exml.find("/gmd:MD_Metadata")#, namespaces=namespaces)
        # Ordinary Python version's don't support the metadata argument
        md = csw._exml.find("/{http://www.isotc211.org/2005/gmd}MD_Metadata")
        mdtree = etree.ElementTree(md)
        try:
            record["xml"] = etree.tostring(mdtree, pretty_print=True, encoding=str)
        except TypeError:
            # API incompatibilities between different flavours of elementtree
            try:
                record["xml"] = etree.tostring(mdtree, pretty_print=True, encoding=str)
            except AssertionError:
                record["xml"] = etree.tostring(md, pretty_print=True, encoding=str)

        record["xml"] = '<?xml version="1.0" encoding="UTF-8"?>\n' + record["xml"]
        record["tree"] = mdtree
        return record

    def records(self):
        '''Provide access the records attribute of the wrapped CatalogueServiceWeb object.'''
        return self._ows().records