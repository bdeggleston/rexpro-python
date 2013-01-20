__author__ = 'bdeggleston'

from unittest import TestCase

from rexpro.connection import RexProConnection

class BaseRexProTestCase(TestCase):
    """
    Base test case for rexpro tests
    """
    host = 'localhost'
    port = 8184
    graphname = 'rexpro_test'

    def get_connection(self):
        return RexProConnection(self.host, self.port, self.graphname)

