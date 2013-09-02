__author__ = 'bdeggleston'

import json
import re
import struct
from uuid import uuid1, uuid4

import msgpack

from rexpro import exceptions
from rexpro import utils

class MessageTypes(object):
    """
    Enumeration of RexPro send message types
    """
    ERROR = 0
    SESSION_REQUEST = 1
    SESSION_RESPONSE = 2
    SCRIPT_REQUEST = 3
    SCRIPT_RESPONSE = 5


class RexProMessage(object):
    """ Base class for rexpro message types """

    MESSAGE_TYPE = None

    def get_meta(self):
        """
        Returns a dictionary of message meta
        data depending on other set values
        """
        return {}

    def get_message_list(self):
        """
        Creates and returns the list containing the data to be serialized into a message
        """
        return [
            #session
            self.session,

            #unique request id
            uuid1().bytes,

            #meta
            self.get_meta()
        ]

    def serialize(self):
        """
        Serializes this message to send to rexster

        The format as far as I can tell is this:

        1B: Message type
        4B: message length
        nB: msgpack serialized message

        the actual message is just a list of values, all seem to start with version, session, and a unique request id
        the session and unique request id are uuid bytes, and the version and are each 1 byte unsigned integers
        """
        #msgpack list
        msg = self.get_message_list()
        bytes = msgpack.dumps(msg)

        #add protocol version
        message = bytearray([1])

        #add serializer type
        message += bytearray([0])

        #add padding
        message += bytearray([0, 0, 0, 0])

        #add message type
        message += bytearray([self.MESSAGE_TYPE])

        #add message length
        message += struct.pack('!I', len(bytes))

        #add message
        message += bytes

        return message

    @classmethod
    def deserialize(cls, data):
        """
        Constructs a message instance from the given data

        :param data: the raw data, minus the type and size info, from rexster
        :type data: str/bytearray

        :rtype: RexProMessage
        """
        #redefine in subclasses
        raise NotImplementedError

    @staticmethod
    def interpret_response(response):
        """
        interprets the response from rexster, returning the relevant response message object
        """

class ErrorResponse(RexProMessage):

    #meta flags
    INVALID_MESSAGE_ERROR = 0
    INVALID_SESSION_ERROR = 1
    SCRIPT_FAILURE_ERROR = 2
    AUTH_FAILURE_ERROR = 3
    GRAPH_CONFIG_ERROR = 4
    CHANNEL_CONFIG_ERROR = 5
    RESULT_SERIALIZATION_ERROR = 6

    def __init__(self, meta, message, **kwargs):
        super(ErrorResponse, self).__init__(**kwargs)
        self.meta = meta
        self.message = message

    @classmethod
    def deserialize(cls, data):
        message = msgpack.loads(data)
        session, request, meta, msg = message
        return cls(message=msg, meta=meta)

class SessionRequest(RexProMessage):
    """
    Message for creating a session with rexster
    """

    MESSAGE_TYPE = MessageTypes.SESSION_REQUEST

    def __init__(self, graph_name=None, graph_obj_name=None, username='', password='', session_key=None, kill_session=False, **kwargs):
        """
        :param graph_name: the name of the rexster graph to connect to
        :type graph_name: str
        :param graph_obj_name: the name of the variable to bind the graph object to (defaults to 'g')
        :type graph_obj_name: str
        :param username: the username to use for authentication (optional)
        :type username: str
        :param password: the password to use for authentication (optional)
        :type password: str
        :param session_key: the session key to reference (used only for killing existing session)
        :type session_key: str
        :param kill_session: sets this request to kill the server session referenced by the session key parameter, defaults to False
        :type kill_session: bool
        """
        super(SessionRequest, self).__init__(**kwargs)
        self.username = username
        self.password = password
        self.session = session_key
        self.graph_name = graph_name
        self.graph_obj_name = graph_obj_name
        self.kill_session = kill_session

    def get_meta(self):
        if self.kill_session:
            return {'killSession': True}

        meta = {}
        if self.graph_name:
            meta['graphName'] = self.graph_name
            if self.graph_obj_name:
                meta['graphObjName'] = self.graph_obj_name

        return meta

    def get_message_list(self):
        return super(SessionRequest, self).get_message_list() + [
            self.username,
            self.password
        ]


class SessionResponse(RexProMessage):

    def __init__(self, session_key, meta, languages, **kwargs):
        """
        """
        super(SessionResponse, self).__init__(**kwargs)
        self.session_key = session_key
        self.meta = meta
        self.languages = languages

    @classmethod
    def deserialize(cls, data):
        message = msgpack.loads(data)
        session, request, meta, languages = message
        return cls(
            session_key=session,
            meta=meta,
            languages=languages
        )

class ScriptRequest(RexProMessage):
    """
    Message that executes a gremlin script and returns the response
    """

    class Language(object):
        GROOVY = 'groovy'
        SCALA = 'scala'
        JAVA = 'java'

    MESSAGE_TYPE = MessageTypes.SCRIPT_REQUEST

    def __init__(self, script, params=None, session_key=None, graph_name=None, graph_obj_name=None, in_session=True,
                 isolate=True, in_transaction=True, language=Language.GROOVY, **kwargs):
        """
        :param script: script to execute
        :type script: str/unicode
        :param params: parameter values to bind to request
        :type params: dict (json serializable)
        :param session_key: the session key to execute the script with
        :type session_key: str
        :param graph_name: the name of the rexster graph to connect to
        :type graph_name: str
        :param graph_obj_name: the name of the variable to bind the graph object to (defaults to 'g')
        :type graph_obj_name: str
        :param in_session: indicates this message should be executed in the context of the included session
        :type in_session:bool
        :param isolate: indicates variables defined in this message should not be available to subsequent message
        :type isolate:bool
        :param in_transaction: indicates this message should be wrapped in a transaction
        :type in_transaction:bool
        :param language: the language used by the script (only groovy has been tested)
        :type language: ScriptRequest.Language
        """
        super(ScriptRequest, self).__init__(**kwargs)
        self.script = script
        self.params = params or {}
        self.session = session_key
        self.graph_name = graph_name
        self.graph_obj_name = graph_obj_name
        self.in_session = in_session
        self.isolate = isolate
        self.in_transaction = in_transaction
        self.language = language

    def get_meta(self):
        meta = {}

        if self.graph_name:
            meta['graphName'] = self.graph_name
            if self.graph_obj_name:
                meta['graphObjName'] = self.graph_obj_name

        #defaults to False
        if self.in_session:
            meta['inSession'] = True

        #defaults to True
        if not self.isolate:
            meta['isolate'] = False

        #defaults to True
        if not self.in_transaction:
            meta['transaction'] = False

        return meta

    def _validate_params(self):
        """
        Checks that the parameters are ok
        (no invalid types, no weird key names)
        """
        for k,v in self.params.items():

            if re.findall(r'^[0-9]', k):
                raise exceptions.RexProScriptException(
                    "parameter names can't begin with a number")
            if re.findall(r'[\s\.]', k):
                raise exceptions.RexProException(
                    "parameter names can't contain {}".format(
                        re.findall(r'^[0-9]', k)[0]
                    )
                )

            if not isinstance(v, (int,
                                  long,
                                  float,
                                  basestring,
                                  dict,
                                  list,
                                  tuple)):
                raise exceptions.RexProScriptException(
                    "{} is an unsupported type".format(type(v))
                )

    def serialize_parameters(self):
        """
        returns a serialization of the supplied parameters
        """
        data = bytearray()
        for k, v in self.params.items():
            key = k.encode('utf-8')
            val = json.dumps(v).encode('utf-8')
            data += utils.int_to_32bit_array(len(key))
            data += key
            data += utils.int_to_32bit_array(len(val))
            data += val
        return str(data)

    def get_message_list(self):
        return super(ScriptRequest, self).get_message_list() + [
            self.language,
            self.script.encode('utf-8'),
            self.params
        ]


class MsgPackScriptResponse(RexProMessage):

    def __init__(self, results, bindings, **kwargs):
        super(MsgPackScriptResponse, self).__init__(**kwargs)
        self.results = results
        self.bindings = bindings

    @classmethod
    def deserialize(cls, data):
        message = msgpack.loads(data)
        session, request, meta, results, bindings = message

        return cls(
            results=results,
            bindings=bindings
        )
