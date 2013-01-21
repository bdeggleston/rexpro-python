from rexpro.exceptions import RexProConnectionException, RexProScriptException
import rexpro.utils

__author__ = 'bdeggleston'

from contextlib import contextmanager
from socket import socket

from rexpro import exceptions
from rexpro import messages
from rexpro import utils

class RexProConnection(object):

    #determines which format rexster returns data in
    # 1 is a string format for consoles
    # 2 is a msgpack format, which we want
    CHANNEL = 2
    def __init__(self, host, port, graph_name, username='', password=''):
        """
        Connection constructor

        :param host: the rexpro server to connect to
        :type host: str (ip address)
        :param port: the rexpro server port to connect to
        :type port: int
        :param graph_name: the graph to connect to
        :type graph_name: str
        :param username: the username to use for authentication (optional)
        :type username: str
        :param password: the password to use for authentication (optional)
        :type password: str
        """
        self.host = host
        self.port = port
        self.graph_name = graph_name
        self.username = username
        self.password = password

        #connect to server
        self._socket = socket()
        self._socket.connect((host, port))

        #indicates that we're in a transaction
        self._in_transaction = False

        #stores the session key
        self._session_key = None
        self._open_session()


    def _open_session(self):
        """ Creates a session with rexster and creates the graph object """
        self._send_message(
            messages.SessionRequest(
                channel=self.CHANNEL,
                username=self.username,
                password=self.password
            )
        )
        session = self._get_response()
        self._session_key = session.session_key

        self.execute(
            script='g = rexster.getGraph(graphname)',
            params={'graphname': self.graph_name},
            isolate=False
        )


    def _send_message(self, msg):
        """
        Serializes the given message and sends it to rexster

        :param msg: the message instance to send to rexster
        :type msg: RexProMessage
        """
        self._socket.send(msg.serialize())

    def _get_response(self):
        """
        gets the message type and message from rexster

        :returns: RexProMessage
        """
        msg_type = self._socket.recv(1)
        if not msg_type:
            raise exceptions.RexProConnectionException('socket connection has been closed')
        msg_type = bytearray(msg_type)[0]
        msg_len = utils.int_from_32bit_array(self._socket.recv(4))
        response = self._socket.recv(msg_len)

        MessageTypes = messages.MessageTypes

        type_map = {
            MessageTypes.ERROR: messages.ErrorResponse,
            MessageTypes.SESSION_RESPONSE: messages.SessionResponse,
            MessageTypes.MSGPACK_SCRIPT_RESPONSE: messages.MsgPackScriptResponse
        }

        if msg_type not in type_map:
            raise RexProConnectionException("can't deserialize message type {}".format(msg_type))
        return type_map[msg_type].deserialize(response)

    def open_transaction(self):
        """ opens a transaction """
        if self._in_transaction:
            raise RexProScriptException("transaction is already open")
        self.execute(
            script='g.stopTransaction(FAILURE)',
            isolate=False
        )
        self._in_transaction = True

    def close_transaction(self, success=True):
        """
        closes an open transaction

        :param success: indicates which status to close the transaction with, True will commit the changes, False will roll them back
        :type success: bool
        """
        if self._in_transaction:
            raise RexProScriptException("transaction is not open")
        self.execute(
            script='g.stopTransaction({})'.format('SUCCESS' if success else 'FAILURE'),
            isolate=False
        )


    @contextmanager
    def transaction(self):
        """
        Context manager that opens a transaction and closes it at
        the end of it's code block, use with the 'with' statement
        """
        self.open_transaction()
        yield
        self.close_transaction()

    def execute(self, script, params={}, isolate=True):
        """
        executes the given gremlin script with the provided parameters

        :param script: the gremlin script to isolate
        :type script: string
        :param params: the parameters to execute the script with
        :type params: dictionary
        :param isolate: wraps the script in a closure so any variables set aren't persisted for the next execute call
        :type isolate: bool

        :rtype: list
        """

        self._send_message(
            messages.ScriptRequest(
                script=script,
                params=params,
                session_key=self._session_key
            )
        )
        response = self._get_response()
        if isinstance(response, messages.ErrorResponse):
            raise exceptions.RexProScriptException(response.message)

        return response.results
