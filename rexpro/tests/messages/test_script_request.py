__author__ = 'bdeggleston'

from rexpro.messages import ScriptRequest, MsgPackScriptResponse
from rexpro.tests.base import BaseRexProTestCase, multi_graph_test

class TestRexProScriptRequestMessage(BaseRexProTestCase):

    @multi_graph_test
    def test_sessionless_message(self):
        conn = self.get_socket()
        out_msg = ScriptRequest(
            """
            v = g.addVertex([xyz:5])
            v
            """,
            in_session=False,
            graph_name=self.graphname
        )
        conn.send_message(out_msg)
        in_msg = conn.get_response()

        self.assertNotErrorResponse(in_msg)
        self.assertIsInstance(in_msg, MsgPackScriptResponse)
        self.assertIsInstance(in_msg.results, dict)
        assert '_properties' in in_msg.results
        assert 'xyz' in in_msg.results['_properties']
        assert in_msg.results['_properties']['xyz'] == 5

    def test_query_isolation(self):
        pass

    def test_disabled_query_isolation(self):
        pass

    def test_graph_definition(self):
        pass
