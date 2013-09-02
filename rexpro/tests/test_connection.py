__author__ = 'bdeggleston'

from unittest import skip

from rexpro.tests.base import BaseRexProTestCase, multi_graph

from rexpro import exceptions

class TestConnection(BaseRexProTestCase):

    def test_connection_success(self):
        """ Development test to aid in debugging """
        conn = self.get_connection()

    def test_attempting_to_connect_to_an_invalid_graphname_raises_exception(self):
        """ Attempting to connect to a nonexistant graph should raise a RexProConnectionExeption """
        with self.assertRaises(exceptions.RexProConnectionException):
            self.get_connection(graphname='nothing')

    @skip
    def test_invalid_connection_info_raises_exception(self):
        pass

    @skip
    def test_call_close_transactions_without_an_open_transaction_fails(self):
        pass

    @skip
    def test_call_open_transaction_with_a_transaction_already_open_fails(self):
        pass


class TestQueries(BaseRexProTestCase):

    @multi_graph
    def test_data_integrity(self):
        """
        Tests that simply being passed through rexster comes unchanged
        """
        conn = self.get_connection(graphname=self.graphname)

        e = lambda p: conn.execute(
            script='values',
            params={'values':p}
        )

        #test string
        data = e('yea boyeeee')
        assert data== 'yea boyeeee'

        #test int
        data = e(1982)
        assert data == 1982

        #test float
        data = e(3.14)
        assert data == 3.14

        #test dict
        data = e({'blake':'eggleston'})
        assert data == {'blake':'eggleston'}

        #test none
        data = e(None)
        assert data is None

        #test list
        data = e([1,2])
        assert data == (1,2)

    def test_query_isolation(self):
        """ Test that variables defined in one query are not available in subsequent queries """
        conn = self.get_connection()

        conn.execute(
            """
            def one_val = 5
            one_val
            """,
            pretty=True
        )

        with self.assertRaises(exceptions.RexProScriptException):
            r = conn.execute(
                """
                one_val
                """
            )


    def test_element_creation(self):
        """ Tests that vertices and edges can be created and are serialized properly """

        conn = self.get_connection()
        elements = conn.execute(
            """
            def v1 = g.addVertex([prop:6])
            def v2 = g.addVertex([prop:8])
            def e = g.addEdge(v1, v2, 'connects', [prop:10])
            return [v1, v2, e]
            """
        )
        v1, v2, e = elements
        assert v1['_properties']['prop'] == 6
        assert v2['_properties']['prop'] == 8
        assert e['_properties']['prop'] == 10

        assert e['_outV'] == v1['_id']
        assert e['_inV'] == v2['_id']

class TestTransactions(BaseRexProTestCase):

    def test_transaction_isolation(self):
        """ Tests that operations between 2 transactions are isolated """
        conn1 = self.get_connection()
        conn2 = self.get_connection()

        if not conn1.graph_features['supportsTransactions']:
            return

        with conn1.transaction():
            v1, v2, v3 = conn1.execute(
                """
                def v1 = g.addVertex([val:1, str:"vertex 1"])
                def v2 = g.addVertex([val:2, str:"vertex 2"])
                def v3 = g.addVertex([val:3, str:"vertex 3"])
                [v1, v2, v3]
                """
            )

        conn1.open_transaction()
        conn2.open_transaction()

        v1_1 = conn1.execute(
            """
            def v1 = g.v(eid)
            v1.setProperty("str", "v1")
            v1
            """,
            params={'eid':v1['_id']}
        )

        v1_2 = conn2.execute(
            """
            g.v(eid)
            """,
            params={'eid':v1['_id']}
        )

        assert v1_2['_properties']['str'] == 'vertex 1'

