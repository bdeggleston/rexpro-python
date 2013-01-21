__author__ = 'bdeggleston'

import json
import re
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
    CONSOLE_SCRIPT_RESPONSE = 4
    MSGPACK_SCRIPT_RESPONSE = 5

class RexProMessage(object):
    """ Base class for rexpro message types """

    MESSAGE_TYPE = None

    def __init__(self, version=0, flag=0):
        """
        :param version:
        :type version:
        :param flag:
        :type flag:
        """
        self.version = version
        self.flag = flag

    def get_message_list(self):
        """
        Creates and returns the list containing the data to be serialized into a message
        """
        return [
            #version
            self.version,

            #flag
            self.flag,

            #session
            self.session,

            #unique request id
            uuid1().bytes
        ]

    def serialize(self):
        """
        Serializes this message to send to rexster

        The format as far as I can tell is this:

        1B: Message type
        4B: message length
        nB: msgpack serialized message

        the actual message is just a list of values, all seem to start with version, flag, session, and a unique request id
        the session and unique request id are uuid bytes, and the version and flag are each 1 byte unsigned integers
        """
        #msgpack list
        msg = self.get_message_list()
        bytes = msgpack.dumps(msg)

        #add meta data
        message = bytearray([self.MESSAGE_TYPE])
        message += utils.int_to_32bit_array(len(bytes))
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

    def __init__(self, message, **kwargs):
        super(ErrorResponse, self).__init__(**kwargs)
        self.message = message

    @classmethod
    def deserialize(cls, data):
        message = msgpack.loads(data)
        ver, flag, session, request, msg = message
        return cls(message=msg)

class SessionRequest(RexProMessage):
    """
    Message for creating a session with rexster
    """

    MESSAGE_TYPE = MessageTypes.SESSION_REQUEST

    def __init__(self, channel=1, username='', password='', **kwargs):
        """
        :param channel: the channel to open the session on
        :type channel: int
        :param username: the username to use for authentication (optional)
        :type username: str
        :param password: the password to use for authentication (optional)
        :type password: str
        """
        super(SessionRequest, self).__init__(**kwargs)
        self.channel = channel
        self.username = username
        self.password = password
        self.session = uuid4().bytes

    def get_message_list(self):
        return super(SessionRequest, self).get_message_list() + [
            self.channel,
            self.username,
            self.password
        ]

class SessionResponse(RexProMessage):

    def __init__(self, session_key, languages, **kwargs):
        """
        """
        super(SessionResponse, self).__init__(**kwargs)
        self.session_key = session_key
        self.languages = languages

    @classmethod
    def deserialize(cls, data):
        message = msgpack.loads(data)
        version, flag, session, request, languages = message
        return cls(
            version=version,
            flag=flag,
            session_key=session,
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

    def __init__(self, script, params, session_key, language=Language.GROOVY, **kwargs):
        """
        :param script: script to execute
        :type script: str/unicode
        :param params: parameter values to bind to request
        :type params: dict (json serializable)
        :param session_key: the session key to execute the script with
        :type session_key: str
        :param language: the language used by the script (only groovy has been tested)
        :type language: ScriptRequest.Language
        """
        super(ScriptRequest, self).__init__(**kwargs)
        self.script = script
        self.params = params
        self.session = session_key
        self.language = language

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
        for k,v in self.params.items():
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
            msgpack.dumps(self.params)
        ]

class MsgPackScriptResponse(RexProMessage):

    def __init__(self, results, bindings, **kwargs):
        super(MsgPackScriptResponse, self).__init__(**kwargs)
        self.results = results
        self.bindings = bindings

    @classmethod
    def deserialize(cls, data):
        message = msgpack.loads(data)
        version, flag, session, request, results, bindings = message

        #deserialize the results
        results = msgpack.loads(results)

        return cls(
            version=version,
            flag=flag,
            results=results,
            bindings=bindings
        )
