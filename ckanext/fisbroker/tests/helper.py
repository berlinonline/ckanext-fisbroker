# coding: utf-8
"""Helper methods for testing"""

import logging

LOG = logging.getLogger(__name__)

def _assert_equal(actual, expected):
    """Wrapper for `assert expected == actual` that also logs the
       values for expected and actual."""

    LOG.debug("expected: %s", expected)
    LOG.debug("actual:   %s", actual)
    assert expected == actual
