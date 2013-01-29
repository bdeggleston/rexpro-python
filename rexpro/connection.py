__author__ = 'bdeggleston'

from contextlib import contextmanager
from hashlib import md5
from Queue import Queue
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
        msg_type = self.recv(1)
        if not msg_type:
            raise exceptions.RexProConnectionException('socket connection has been closed')
        msg_type = bytearray(msg_type)[0]
        msg_len = utils.int_from_32bit_array(self.recv(4))

        response = ''
        while len(response) < msg_len:
            response += self.recv(msg_len)

        MessageTypes = messages.MessageTypes

        type_map = {
            MessageTypes.ERROR: messages.ErrorResponse,
            MessageTypes.SESSION_RESPONSE: messages.SessionResponse,
            MessageTypes.MSGPACK_SCRIPT_RESPONSE: messages.MsgPackScriptResponse
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
                channel=self.CHANNEL,
                username=self.username,
                password=self.password
            )
        )
        session = self._conn.get_response()
        self._session_key = session.session_key

        results = self.execute(
            script='g = rexster.getGraph(graphname)',
            params={'graphname': self.graph_name},
            isolate=False
        )
        if not results:
            raise exceptions.RexProConnectionException("could not connect to graph '{}'".format(self.graph_name))
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

    def execute(self, script, params={}, isolate=True, pretty=False):
        """
        executes the given gremlin script with the provided parameters

        :param script: the gremlin script to isolate
        :type script: string
        :param params: the parameters to execute the script with
        :type params: dictionary
        :param isolate: wraps the script in a closure so any variables set aren't persisted for the next execute call
        :type isolate: bool
        :param pretty: will dedent the script if set to True
        :type pretty: bool

        :rtype: list
        """
        query_script = dedent(script) if pretty else script
        if isolate:
            closure_name = 'q_{}'.format(md5(query_script).hexdigest())
            query_script = '\n'.join([
                'def %s = {' % closure_name,
                query_script,
                '}',
                '%s()' % closure_name
            ])

        self._conn.send_message(
            messages.ScriptRequest(
                script=query_script,
                params=params,
                session_key=self._session_key
            )
        )
        response = self._conn.get_response()

        if isinstance(response, messages.ErrorResponse):
            raise exceptions.RexProScriptException(response.message)

        return response.results
