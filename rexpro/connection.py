from rexpro.exceptions import RexProConnectionException
from rexpro.messages import ErrorResponse

from contextlib import contextmanager
from hashlib import md5
from Queue import Queue
import struct
from socket import socket
from textwrap import dedent

from rexpro import exceptions
from rexpro import messages
from rexpro import utils

class RexProSocket(socket):
    """ Subclass of python's socket that sends and received rexpro messages """

    def send_message(self, msg):
        """
        Serializes the given message and sends it to rexster

        :param msg: the message instance to send to rexster
        :type msg: RexProMessage
        """
        self.send(msg.serialize())

    def get_response(self):
        """
        gets the message type and message from rexster

        :returns: RexProMessage
        """
        msg_version = self.recv(1)
        if not msg_version:
            raise exceptions.RexProConnectionException('socket connection has been closed')
        if bytearray([msg_version])[0] != 1:
            raise exceptions.RexProConnectionException('unsupported protocol version: {}'.format())

        serializer_type = self.recv(1)
        if bytearray(serializer_type)[0] != 0:
            raise exceptions.RexProConnectionException('unsupported serializer version: {}'.format())

        #get padding
        self.recv(4)

        msg_type = self.recv(1)
        msg_type = bytearray(msg_type)[0]

        msg_len = struct.unpack('!I', self.recv(4))[0]

        response = ''
        while len(response) < msg_len:
            response += self.recv(msg_len)

        MessageTypes = messages.MessageTypes

        type_map = {
            MessageTypes.ERROR: messages.ErrorResponse,
            MessageTypes.SESSION_RESPONSE: messages.SessionResponse,
            MessageTypes.SCRIPT_RESPONSE: messages.MsgPackScriptResponse
        }

        if msg_type not in type_map:
            raise exceptions.RexProConnectionException("can't deserialize message type {}".format(msg_type))
        return type_map[msg_type].deserialize(response)

class RexProConnectionPool(object):

    def __init__(self, host, port, size):
        """
        Connection constructor

        :param host: the server to connect to
        :type host: str (ip address)
        :param port: the server port to connect to
        :type port: int
        :param size: the initial connection pool size
        :type size: int
        """

        self.host = host
        self.port = port
        self.size = size

        self.pool = Queue()
        for i in range(size):
            self.pool.put(self._new_conn())

    def _new_conn(self):
        """
        Creates and returns a new connection
        """
        conn = RexProSocket()
        conn.connect((self.host, self.port))
        return conn

    def get(self):
        """
        Returns a connection, creating a new one if the pool is empty
        """
        if self.pool.empty():
            return self._new_conn()
        return self.pool.get()

    def put(self, conn):
        """
        returns a connection to the pool, will close the connection if the pool is full
        """
        if self.pool.qsize() >= self.size:
            conn.close()
            return

        self.pool.put(conn)

    @contextmanager
    def contextual_connection(self):
        """
        context manager that will open, yield, and close a connection
        """
        conn = self.get()
        yield conn
        self.put(conn)

    def __del__(self):
        while not self.pool.empty():
            self.pool.get().close()


class RexProConnection(object):

    def __init__(self, host, port, graph_name, graph_obj_name='g', username='', password=''):
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

        self.graph_features = None

        #connect to server
        self._conn = RexProSocket()
        self._conn.connect((self.host, self.port))

        #indicates that we're in a transaction
        self._in_transaction = False

        #stores the session key
        self._session_key = None
        self._open_session()

    def _open_session(self):
        """ Creates a session with rexster and creates the graph object """
        self._conn.send_message(
            messages.SessionRequest(
                username=self.username,
                password=self.password,
                graph_name=self.graph_name
            )
        )
        response = self._conn.get_response()
        if isinstance(response, ErrorResponse):
            raise RexProConnectionException(response.message)
        self._session_key = response.session_key

        self.graph_features = self.execute('g.getFeatures().toMap()')

    def open_transaction(self):
        """ opens a transaction """
        if self._in_transaction:
            raise exceptions.RexProScriptException("transaction is already open")
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
        if not self._in_transaction:
            raise exceptions.RexProScriptException("transaction is not open")
        self.execute(
            script='g.stopTransaction({})'.format('SUCCESS' if success else 'FAILURE'),
            isolate=False
        )
        self._in_transaction = False

    @contextmanager
    def transaction(self):
        """
        Context manager that opens a transaction and closes it at
        the end of it's code block, use with the 'with' statement
        """
        self.open_transaction()
        yield
        self.close_transaction()

    def execute(self, script, params={}, isolate=True, transaction=True, pretty=False):
        """
        executes the given gremlin script with the provided parameters

        :param script: the gremlin script to isolate
        :type script: string
        :param params: the parameters to execute the script with
        :type params: dictionary
        :param isolate: wraps the script in a closure so any variables set aren't persisted for the next execute call
        :type isolate: bool
        :param transaction: query will be wrapped in a transaction if set to True (default)
        :type transaction: bool
        :param pretty: will dedent the script if set to True
        :type pretty: bool

        :rtype: list
        """
        self._conn.send_message(
            messages.ScriptRequest(
                script=script,
                params=params,
                session_key=self._session_key,
                isolate=isolate,
                in_transaction=transaction
            )
        )
        response = self._conn.get_response()

        if isinstance(response, messages.ErrorResponse):
            raise exceptions.RexProScriptException(response.message)

        return response.results
