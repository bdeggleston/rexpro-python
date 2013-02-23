__author__ = 'bdeggleston'

from rexpro.messages import ScriptRequest, MsgPackScriptResponse
from rexpro.tests.base import BaseRexProTestCase

class TestRexProScriptRequestMessage(BaseRexProTestCase):

    def test_sessionless_message(self):
        conn = self.get_socket()
        out_msg = ScriptRequest(
            """
            v = g.addVertex([xyz:5])
            v
            """,
            in_session=False,
            graph_name='emptygraph'
        )
        conn.send_message(out_msg)
        in_msg = conn.get_response()

        assert isinstance(in_msg, MsgPackScriptResponse)
        assert isinstance(in_msg.results, dict)
        assert '_properties' in in_msg.results
        assert 'xyz' in in_msg.results['_properties']
        assert in_msg.results['_properties']['xyz'] == 5

    def test_query_isolation(self):
        pass

    def test_disabled_query_isolation(self):
        pass

    def test_graph_definition(self):
        pass
