__author__ = 'bdeggleston'

from unittest import skip

from rexpro.tests.base import BaseRexProTestCase

from rexpro.connection import RexProConnection

class TestConnection(BaseRexProTestCase):

    @skip
    def test_connection_success(self):
        """
        Development test to aid in debugging
        """
        conn = self.get_connection()

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

    def test_data_integrity(self):
        """
        Tests that simply being passed through rexster comes unchanged
        """
        conn = self.get_connection()

        e = lambda p: conn.execute(
            script='values',
            params={'values':p}
        )

        test_data = {
#            'null_value': None,
            'int_value': 56,
#            'float_value': 3.14,
            'str_value': 'yea boyeeee',
#            'list_value': [1, None, 'blake'],
#            'dict_value': {'new':'name', 1:2}
        }

        #test string
        data = e('yea boyeeee')
        assert data== 'yea boyeeee'

        #test int
        data = e(1982)
        assert data == 1982

        #test float
        data = e(3.14)
        assert data == 3.14

        #test list
        #TODO: fix [ERROR] ScriptFilter - org.msgpack.MessageTypeException
#        data = e([1,2])
#        assert data == [1,2]

        #test dict
        #TODO: fix [ERROR] ScriptFilter - org.msgpack.MessageTypeException
#        data = e({'blake':'eggleston'})
#        assert data == {'blake':'eggleston'}

        #test none
        #TODO fix rexster's null handling exception here
        data = e(None)
        assert data is None

        #TODO: return error response when there's an exception internally
