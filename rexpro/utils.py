__author__ = 'bdeggleston'


def int_to_32bit_array(val):
    """
    Converts an integer to 32 bit bytearray
    :param val: the value to convert to bytes
    :return: bytearray
    """
    value = val
    bytes = bytearray()
    for i in range(4):
        bytes.insert(0, (value & 0xff))
        value >>= 8
    return str(bytes)

def int_from_32bit_array(val):
    """
    Converts an integer from a 32 bit bytearray
    :param val: the value to convert to an int
    :return: int
    """
    rval = 0
    for fragment in bytearray(val):
        rval <<= 8
        rval |= fragment
    return rval

