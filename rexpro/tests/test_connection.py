__author__ = 'bdeggleston'

from unittest import skip

from rexpro.tests.base import BaseRexProTestCase

from rexpro.connection import RexProConnection

class TestConnection(BaseRexProTestCase):

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
