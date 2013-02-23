__author__ = 'bdeggleston'

from unittest import TestCase

from rexpro.connection import RexProConnection, RexProSocket

class BaseRexProTestCase(TestCase):
    """
    Base test case for rexpro tests
    """
    host = 'localhost'
    port = 8184
    graphname = 'emptygraph'

    def get_connection(self, host=None, port=None, graphname=None):
        return RexProConnection(
            host or self.host,
            port or self.port,
            graphname or self.graphname
        )

    def get_socket(self, host=None, port=None):
        conn = RexProSocket()
        conn.connect((
            host or self.host,
            port or self.port,
        ))
        return conn

