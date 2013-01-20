__author__ = 'bdeggleston'

class RexProException(Exception):
    """ Base RexProException """
    pass

class RexProConnectionException(RexProException):
    """ Raised when there are problems with the rexster connection """
    pass

class RexProScriptException(RexProException):
    """
    Raised when there's an error with a script request
    """
