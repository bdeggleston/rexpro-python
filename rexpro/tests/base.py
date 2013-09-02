from rexpro.messages import ScriptRequest, MsgPackScriptResponse

__author__ = 'bdeggleston'

from unittest import TestCase

from rexpro.connection import RexProConnection, RexProSocket

def multi_graph(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as ex:
            #from http://stackoverflow.com/questions/6062576/adding-information-to-a-python-exception
            import sys
            message = '[Graph: {}] '.format(getattr(self, 'graphname', None)) + unicode(ex)
            raise type(ex), type(ex)(message), sys.exc_info()[2]

    # add the multi graph attribute to the function so
    # the test runner knows to run it multiple times
    setattr(wrapper, 'multi_graph', True)
    setattr(wrapper, 'func_name', func.func_name)

    return wrapper

class BaseRexProTestCase(TestCase):
    """
    Base test case for rexpro tests
    """
    host = 'localhost'
    port = 8184
    default_graphname = 'emptygraph'

    test_graphs = [
        'emptygraph', #Tinkergraph
#        'emptysailgraph', #in memory sail graph
#        'sailgraph', #sail graph
        'orientdbsample', #OrientDB
        'neo4jsample', #Neo4j
#        'dexsample', #DexGraph
#         'titangraph', #Titan
    ]

    def run(self, result=None):
        """
        extended to run a test multiple times with different graphs
        """
        method = getattr(self, self._testMethodName)

        if getattr(method, 'multi_graph', False) and not getattr(self, 'graphname', False):
            for graph in self.test_graphs:
                self.graphname = graph
                self.run(result=result)
            self.graphname = None
        else:
            super(BaseRexProTestCase, self).run(result=result)

    def get_connection(self, host=None, port=None, graphname=None):
        return RexProConnection(
            host or self.host,
            port or self.port,
            graphname or self.default_graphname
        )

    def get_socket(self, host=None, port=None):
        conn = RexProSocket()
        conn.connect((
            host or self.host,
            port or self.port,
        ))
        return conn

    def assertNotErrorResponse(self, response):
        from rexpro.messages import ErrorResponse
        self.assertNotIsInstance(response, ErrorResponse, getattr(response, 'message', ''))

    def assertErrorResponse(self, response):
        from rexpro.messages import ErrorResponse
        self.assertIsInstance(response, ErrorResponse, 'ErrorResponse was expected, got: {}'.format(type(response)))
